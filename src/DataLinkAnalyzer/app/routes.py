"""
路由处理模块
"""

import os
import json
from datetime import datetime
from pathlib import Path
from starlette.responses import HTMLResponse, JSONResponse
from starlette.requests import Request

from core.excel_reader import create_reader
from core.query_executor import create_executor
from core.chart_generator import create_chart_generator
from core.script_generator import create_script_generator
from utils.file_scanner import scan_folder, find_similar_files


# 存储调试日志
debug_logs = []


def _log_debug(message: str, level: str = 'info') -> None:
    """记录调试日志"""
    from datetime import datetime
    log_entry = {
        'time': datetime.now().strftime('%H:%M:%S'),
        'level': level,
        'message': message
    }
    debug_logs.append(log_entry)
    # 只保留最近100条
    if len(debug_logs) > 100:
        debug_logs.pop(0)
    print(f"[{level.upper()}] {message}")


# ==================== 页面路由 ====================

async def home_page(request: Request) -> HTMLResponse:
    """首页"""
    static_dir = os.path.join(os.path.dirname(__file__), 'static')
    index_path = os.path.join(static_dir, 'index.html')
    
    if os.path.exists(index_path):
        with open(index_path, 'r', encoding='utf-8') as f:
            return HTMLResponse(f.read())
    else:
        return HTMLResponse("<h1>请先创建静态文件</h1>", status_code=404)


# ==================== API路由 ====================

async def scan_folder(request: Request) -> JSONResponse:
    """扫描文件夹"""
    try:
        data = await request.json()
        folder_path = data.get('folder_path', '')
        
        if not folder_path or not os.path.exists(folder_path):
            return JSONResponse({
                'success': False,
                'error': '文件夹不存在'
            })
        
        from utils.file_scanner import scan_folder as do_scan_folder
        files = do_scan_folder(folder_path)
        
        # 获取上一级目录
        parent_dir = os.path.dirname(folder_path) if os.path.isabs(folder_path) else os.path.dirname(folder_path)
        
        # 列出当前目录的子目录
        sub_dirs = []
        if os.path.exists(folder_path):
            try:
                sub_dirs = [d for d in os.listdir(folder_path) 
                           if os.path.isdir(os.path.join(folder_path, d)) and not d.startswith('.')]
            except:
                pass
        
        _log_debug(f"扫描文件夹 {folder_path}: 找到 {len(files)} 个文件")
        
        return JSONResponse({
            'success': True,
            'files': files,
            'parent_dir': parent_dir,
            'current_dir': folder_path,
            'sub_dirs': sub_dirs
        })
    except Exception as e:
        _log_debug(f"扫描文件夹失败: {e}", 'error')
        return JSONResponse({
            'success': False,
            'error': str(e)
        })


async def scan_file(request: Request) -> JSONResponse:
    """扫描Excel文件"""
    try:
        data = await request.json()
        file_path = data.get('file_path', '')
        
        # 处理相对路径
        if not os.path.isabs(file_path):
            file_path = os.path.abspath(file_path)
        
        if not file_path or not os.path.exists(file_path):
            return JSONResponse({
                'success': False,
                'error': f'文件不存在: {file_path}'
            })
        
        reader = create_reader(file_path)
        summary = reader.get_summary()
        
        _log_debug(f"扫描文件 {summary['filename']}: {summary['sheet_count']} 个Sheet")
        
        return JSONResponse({
            'success': True,
            'filename': summary['filename'],
            'path': summary['path'],
            'size_mb': summary['size_mb'],
            'is_large': summary['is_large'],
            'sheets': summary['sheets']
        })
    except Exception as e:
        _log_debug(f"扫描文件失败: {e}", 'error')
        return JSONResponse({
            'success': False,
            'error': str(e)
        })


async def get_sheets(request: Request) -> JSONResponse:
    """获取文件的所有Sheet"""
    try:
        data = await request.json()
        file_path = data.get('file_path', '')
        
        reader = create_reader(file_path)
        sheets = reader.get_sheets()
        
        return JSONResponse({
            'success': True,
            'sheets': sheets
        })
    except Exception as e:
        return JSONResponse({
            'success': False,
            'error': str(e)
        })


async def get_headers(request: Request) -> JSONResponse:
    """获取表头"""
    try:
        data = await request.json()
        file_path = data.get('file_path', '')
        sheet_name = data.get('sheet_name', '')
        header_rows = data.get('header_rows', 1)
        
        # 处理相对路径
        if not os.path.isabs(file_path):
            file_path = os.path.abspath(file_path)
        
        reader = create_reader(file_path)
        headers = reader.get_headers(sheet_name, header_rows)
        fields = reader.get_field_info(sheet_name)
        
        # 转换nullable为Python原生bool
        for f in fields:
            if 'nullable' in f:
                f['nullable'] = bool(f['nullable'])
        
        _log_debug(f"获取 {sheet_name} 表头: {len(headers)} 个字段")
        
        return JSONResponse({
            'success': True,
            'headers': headers,
            'fields': fields
        })
    except Exception as e:
        _log_debug(f"获取表头失败: {e}", 'error')
        return JSONResponse({
            'success': False,
            'error': str(e)
        })


async def load_tables(request: Request) -> JSONResponse:
    """加载表数据"""
    try:
        data = await request.json()
        tables = data.get('tables', [])
        
        executor = create_executor(debug=True)
        result = executor.load_tables(tables)
        
        if result['success']:
            _log_debug(f"加载 {len(result['tables'])} 个表成功", 'success')
        
        return JSONResponse(result)
    except Exception as e:
        _log_debug(f"加载表失败: {e}", 'error')
        return JSONResponse({
            'success': False,
            'error': str(e)
        })


import traceback

async def execute_query(request: Request) -> JSONResponse:
    """执行查询"""
    try:
        print("[execute_query] 收到请求")
        data = await request.json()
        print(f"[execute_query] data keys: {data.keys()}")
        
        # 修复：tables可能是dict（config.json格式），需要转换为list
        tables_data = data.get('tables', [])
        if isinstance(tables_data, dict):
            # dict格式转换为list
            tables = []
            for file_path, file_info in tables_data.items():
                if 'sheets' in file_info:
                    for sheet in file_info['sheets']:
                        tables.append({
                            'name': file_info.get('filename', file_path.split('/')[-1]),
                            'file_path': file_path,
                            'sheet_name': sheet.get('name')
                        })
        else:
            tables = tables_data
            
        links = data.get('links', [])
        output_fields = data.get('output_fields', {})
        output_file = data.get('output_file', 'output/query_result.xlsx')
        
        _log_debug(f"开始查询: {len(tables)} 表, {len(links)} 关联")
        
        # 检查文件大小，对大文件使用特殊处理
        for table in tables:
            file_path = table.get('file_path', '')
            if os.path.exists(file_path):
                size_mb = os.path.getsize(file_path) / (1024 * 1024)
                if size_mb > 100:
                    _log_debug(f"警告: 大文件 {table.get('name')} ({size_mb:.1f}MB)", 'warning')
        
        # 加载表（传递output_fields和links，只加载需要的列）
        executor = create_executor(debug=True)
        load_result = executor.load_tables(tables, output_fields, links)
        
        if not load_result['success']:
            return JSONResponse(load_result)
        
        # 执行查询
        try:
            print("[execute_query] 开始执行关联...")
            result_df = executor.execute(links, output_fields)
            print(f"[execute_query] 关联完成: {len(result_df)} 行")
        except MemoryError as me:
            print(f"[execute_query] 内存不足: {me}")
            _log_debug(f"内存不足: {me}", 'error')
            return JSONResponse({
                'success': False,
                'error': f'内存不足，请减少数据量或选择较小文件: {str(me)}'
            })
        except Exception as eq:
            print(f"[execute_query] 查询失败: {eq}")
            traceback.print_exc()
            _log_debug(f"查询执行失败: {eq}", 'error')
            return JSONResponse({
                'success': False,
                'error': f'查询失败: {str(eq)}'
            })
        
        # 导出
        export_path = executor.export_to_excel(result_df, output_file)
        
        # 转换统计信息中的numpy类型为Python原生类型
        stats = executor.get_statistics(result_df)
        stats['total_rows'] = int(stats.get('rows', len(result_df)))
        stats['total_columns'] = int(stats.get('columns', len(result_df.columns)))
        stats['memory_usage'] = 0
        stats['null_counts'] = {}
        
        _log_debug(f"查询完成: {stats['total_rows']} 行, 保存到 {export_path}", 'success')
        
        return JSONResponse({
            'success': True,
            'rows': stats['total_rows'],
            'columns': stats['total_columns'],
            'output_file': export_path,
            'statistics': stats
        })
    except Exception as e:
        _log_debug(f"查询失败: {e}", 'error')
        return JSONResponse({
            'success': False,
            'error': str(e)
        })


async def export_data(request: Request) -> JSONResponse:
    """导出数据（独立接口）"""
    try:
        data = await request.json()
        # 与execute_query类似但更简单
        return await execute_query(request)
    except Exception as e:
        return JSONResponse({
            'success': False,
            'error': str(e)
        })


async def generate_chart(request: Request) -> JSONResponse:
    """生成图表"""
    try:
        data = await request.json()
        # 获取已查询的数据（这里简化处理，实际需要状态管理）
        return JSONResponse({
            'success': False,
            'error': '请先执行查询'
        })
    except Exception as e:
        return JSONResponse({
            'success': False,
            'error': str(e)
        })


CONFIG_DIR = 'config'

async def save_config(request: Request) -> JSONResponse:
    """保存配置"""
    try:
        data = await request.json()
        config = data.get('config', {})
        output_path = data.get('output_path', 'config.json')
        
        # 默认保存到config目录
        if '/' not in output_path and '\\' not in output_path:
            output_path = os.path.join(CONFIG_DIR, output_path)
        
        os.makedirs(CONFIG_DIR, exist_ok=True)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(config, f, ensure_ascii=False, indent=2)
        
        _log_debug(f"配置已保存: {output_path}", 'success')
        
        return JSONResponse({
            'success': True,
            'output_path': output_path
        })
    except Exception as e:
        _log_debug(f"保存配置失败: {e}", 'error')
        return JSONResponse({
            'success': False,
            'error': str(e)
        })


async def load_config(request: Request) -> JSONResponse:
    """加载配置"""
    try:
        data = await request.json()
        config_path = data.get('config_path', 'config.json')
        
        # 默认从config目录读取
        if '/' not in config_path and '\\' not in config_path:
            config_path = os.path.join(CONFIG_DIR, config_path)
        
        if not os.path.exists(config_path):
            return JSONResponse({
                'success': False,
                'error': f'配置文件不存在: {config_path}'
            })
        
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        
        _log_debug(f"配置已加载: {config_path}", 'success')
        
        return JSONResponse({
            'success': True,
            'config': config
        })
    except Exception as e:
        _log_debug(f"加载配置失败: {e}", 'error')
        return JSONResponse({
            'success': False,
            'error': str(e)
        })


async def generate_script(request: Request) -> JSONResponse:
    """生成查询脚本"""
    try:
        data = await request.json()
        tables = data.get('tables', [])
        links = data.get('links', [])
        output_fields = data.get('output_fields', {})
        output_file = data.get('output_file', 'output/query_script.py')
        
        generator = create_script_generator()
        script_path = generator.generate_script(
            tables, links, output_fields, output_file
        )
        
        _log_debug(f"脚本已生成: {script_path}", 'success')
        
        return JSONResponse({
            'success': True,
            'script_path': script_path
        })
    except Exception as e:
        _log_debug(f"生成脚本失败: {e}", 'error')
        return JSONResponse({
            'success': False,
            'error': str(e)
        })


async def debug_log(request: Request) -> JSONResponse:
    """获取调试日志"""
    if request.method == 'POST':
        data = await request.json()
        message = data.get('message', '')
        level = data.get('level', 'info')
        _log_debug(message, level)
        return JSONResponse({'success': True})
    else:
        return JSONResponse({
            'success': True,
            'logs': debug_logs
        })

# ==================== 文件选择对话框 ====================

async def select_folder_dialog(request: Request) -> JSONResponse:
    """打开文件夹选择对话框（后端调用tkinter）"""
    try:
        import tkinter as tk
        from tkinter import filedialog
        
        # 创建隐藏的tk窗口
        root = tk.Tk()
        root.withdraw()
        
        # 弹出选择对话框
        folder_selected = filedialog.askdirectory(title="选择文件夹")
        root.destroy()
        
        if folder_selected:
            return JSONResponse({
                'success': True,
                'folder_path': folder_selected
            })
        else:
            return JSONResponse({
                'success': False,
                'error': '未选择文件夹'
            })
    except Exception as e:
        _log_debug(f"选择文件夹失败: {e}", 'error')
        return JSONResponse({
            'success': False,
            'error': str(e)
        })


# ==================== 配置管理API ====================

async def save_named_config(request: Request) -> JSONResponse:
    """保存命名配置"""
    try:
        data = await request.json()
        config_name = data.get('config_name', 'default')
        tables = data.get('tables', {})
        links = data.get('links', [])
        output_fields = data.get('output_fields', {})
        
        config_data = {
            'tables': tables,
            'links': links,
            'output_fields': output_fields,
            'saved_at': str(datetime.now())
        }
        
        # 保存到configs目录
        config_dir = 'configs'
        os.makedirs(config_dir, exist_ok=True)
        
        # 清理文件名
        safe_name = "".join(c for c in config_name if c.isalnum() or c in ('-', '_'))
        config_path = os.path.join(config_dir, f'{safe_name}.json')
        
        with open(config_path, 'w', encoding='utf-8') as f:
            json.dump(config_data, f, ensure_ascii=False, indent=2)
        
        return JSONResponse({
            'success': True,
            'config_path': config_path,
            'config_name': config_name
        })
    except Exception as e:
        _log_debug(f"保存配置失败: {e}", 'error')
        return JSONResponse({'success': False, 'error': str(e)})


async def load_named_config(request: Request) -> JSONResponse:
    """加载命名配置"""
    try:
        data = await request.json()
        config_name = data.get('config_name', '')
        
        if not config_name:
            # 列出所有配置
            config_dir = 'configs'
            if not os.path.exists(config_dir):
                return JSONResponse({'success': True, 'configs': []})
            
            configs = []
            for f in os.listdir(config_dir):
                if f.endswith('.json'):
                    configs.append(f.replace('.json', ''))
            return JSONResponse({'success': True, 'configs': configs})
        
        # 加载指定配置
        safe_name = "".join(c for c in config_name if c.isalnum() or c in ('-', '_'))
        config_path = os.path.join('configs', f'{safe_name}.json')
        
        if not os.path.exists(config_path):
            return JSONResponse({'success': False, 'error': f'配置不存在: {config_name}'})
        
        with open(config_path, 'r', encoding='utf-8') as f:
            config_data = json.load(f)
        
        # 验证文件是否存在
        validation = []
        tables = config_data.get('tables', {})
        for file_path, info in tables.items():
            exists = os.path.exists(file_path)
            status = '✅' if exists else '❌'
            validation.append({
                'file': os.path.basename(file_path),
                'path': file_path,
                'exists': exists,
                'status': status
            })
        
        return JSONResponse({
            'success': True,
            'config': config_data,
            'validation': validation
        })
    except Exception as e:
        _log_debug(f"加载配置失败: {e}", 'error')
        return JSONResponse({'success': False, 'error': str(e)})


async def delete_named_config(request: Request) -> JSONResponse:
    """删除命名配置"""
    try:
        data = await request.json()
        config_name = data.get('config_name', '')
        
        safe_name = "".join(c for c in config_name if c.isalnum() or c in ('-', '_'))
        config_path = os.path.join('configs', f'{safe_name}.json')
        
        if os.path.exists(config_path):
            os.remove(config_path)
            return JSONResponse({'success': True})
        else:
            return JSONResponse({'success': False, 'error': '配置不存在'})
    except Exception as e:
        return JSONResponse({'success': False, 'error': str(e)})


async def list_configs(request: Request) -> JSONResponse:
    """列出所有配置文件"""
    try:
        configs = []
        if os.path.exists(CONFIG_DIR):
            for f in os.listdir(CONFIG_DIR):
                if f.endswith('.json') and not f.startswith('output'):
                    configs.append(f)
        configs.sort()
        return JSONResponse({'success': True, 'configs': configs})
    except Exception as e:
        return JSONResponse({'success': False, 'error': str(e)})
