"""
查询执行器 - Subprocess版本
解决大文件OOM问题：fork子进程执行，主进程不崩溃
"""

import os
import sys
import json
import subprocess
import tempfile
import pandas as pd
from typing import List, Dict, Any

from core.linker import create_linker
from core.excel_reader import create_reader
from utils.cleaner import clean_dataframe_values, normalize_field_for_link


# 子进程执行的查询脚本
QUERY_SCRIPT = '''
import sys
import os
import json
import pandas as pd

# 添加项目路径
sys.path.insert(0, os.getcwd())

from utils.cleaner import clean_dataframe_values

def execute_query(tables, links, output_fields):
    """在子进程中执行查询"""
    results = {}
    
    print(f"[子进程] output_fields keys: {list(output_fields.keys())[:3]}", flush=True)
    
    # 读取所有表
    for i, table in enumerate(tables):
        name = table["name"]
        file_path = table["file_path"]
        sheet_name = table["sheet_name"]
        
        # 获取需要的字段 - 尝试多种key匹配
        needed_fields = []
        
        # 直接匹配
        if name in output_fields:
            needed_fields = output_fields[name]
        else:
            # 尝试文件名匹配
            file_name = name.split('/')[-1]
            for key, fields in output_fields.items():
                if file_name in key or key in file_name:
                    needed_fields = fields
                    break
        
        # 从links获取关联字段
        for link in links:
            left_name = link.get("left_name", link.get("left_table", ""))
            right_name = link.get("right_name", link.get("right_table", ""))
            
            if left_name and (left_name in name or name in left_name):
                f = link.get("left_field")
                if f and f not in needed_fields:
                    needed_fields.append(f)
            if right_name and (right_name in name or name in right_name):
                f = link.get("right_field")
                if f and f not in needed_fields:
                    needed_fields.append(f)
        
        print(f"[子进程] {name[:30]}: 需要字段 {needed_fields[:5]}...", flush=True)
        
        # 从links获取关联字段
        for link in links:
            left_name = link.get("left_name", link.get("left_table", ""))
            right_name = link.get("right_name", link.get("right_table", ""))
            
            if left_name and (left_name in name or name in left_name):
                f = link.get("left_field")
                if f and f not in needed_fields:
                    needed_fields.append(f)
            if right_name and (right_name in name or name in right_name):
                f = link.get("right_field")
                if f and f not in needed_fields:
                    needed_fields.append(f)
        
        usecols = needed_fields if needed_fields else None
        
        print(f"[子进程] 读取 {name}...", flush=True)
        df = pd.read_excel(file_path, sheet_name=sheet_name, usecols=usecols)
        df = clean_dataframe_values(df)
        
        results[name] = df
        print(f"[子进程] {name}: {len(df)}行", flush=True)
    
    # 执行关联
    print(f"[子进程] 开始关联...", flush=True)
    
    # 取第一个表作为主表
    table_names = list(results.keys())
    result = results[table_names[0]]
    
    # 依次关联
    for link in links:
        left_table = link.get("left_name", link.get("left_table", ""))
        right_table = link.get("right_name", link.get("right_table", ""))
        left_field = link.get("left_field", "")
        right_field = link.get("right_field", "")
        join_type = link.get("join_type", "inner")
        
        # 找到右表
        right_df = None
        right_name = None
        for name, df in results.items():
            if right_table in name or name in right_table:
                right_df = df
                right_name = name
                break
        
        if right_df is not None and left_field and right_field:
            # 检查字段是否存在
            if left_field not in result.columns:
                print(f"[子进程] 警告: 左表缺少字段 {left_field}, 可用: {list(result.columns)[:5]}...", flush=True)
                continue
            if right_field not in right_df.columns:
                print(f"[子进程] 警告: 右表缺少字段 {right_field}", flush=True)
                continue
            
            # 标准化关联字段（创建临时列用于关联）
            result['_link_key'] = result[left_field].astype(str).str.strip().str.upper()
            right_df['_link_key'] = right_df[right_field].astype(str).str.strip().str.upper()
            
            # 不去重！保留所有数据，让pandas自己处理关联
            # 关联
            result = result.merge(right_df, left_on='_link_key', right_on='_link_key', how=join_type, suffixes=('', '_y'))
            
            # 删除临时列
            result = result.drop(columns=['_link_key'], errors='ignore')
            
            print(f"[子进程] 关联后: {len(result)}行, {len(result.columns)}列", flush=True)
    
    # 保存结果
    output_path = sys.argv[1]
    result.to_excel(output_path, index=False)
    print(f"[子进程] 结果已保存: {output_path}", flush=True)
    print(f"[子进程] 完成: {len(result)}行, {len(result.columns)}列", flush=True)

if __name__ == "__main__":
    data = json.loads(sys.stdin.read())
    execute_query(data["tables"], data["links"], data["output_fields"])
'''


class QueryExecutor:
    """查询执行器 - Subprocess版本"""
    
    def __init__(self, debug: bool = False):
        self.debug = debug
        self.linker = create_linker(debug=debug)
    
    def _log(self, msg: str):
        if self.debug:
            print(f"[QueryExecutor] {msg}")
    
    def load_tables(self, tables: List[Dict[str, Any]], 
                    output_fields: Dict[str, List[str]] = None,
                    links: List[Dict] = None) -> Dict[str, Any]:
        """加载表（暂存配置，实际执行在子进程）"""
        loaded = {}
        
        for table in tables:
            name = table['name']
            file_path = table['file_path']
            sheet_name = table['sheet_name']
            
            if os.path.exists(file_path):
                size_mb = os.path.getsize(file_path) / (1024 * 1024)
                loaded[name] = {
                    'rows': '?',
                    'columns': '?',
                    'sheet': sheet_name,
                    'file_path': file_path,
                    'file_size': size_mb
                }
        
        self.linker.dataframes = loaded
        
        return {
            'success': True,
            'tables': loaded,
            'errors': []
        }
    
    def execute(self, links: List[Dict], output_fields: Dict[str, List[str]] = None) -> pd.DataFrame:
        """
        执行查询（fork子进程）
        """
        self._log("[Subprocess] 开始fork子进程执行查询...")
        
        if not self.linker.dataframes:
            raise ValueError("没有加载任何表")
        
        # 关键：按links中的顺序确定表的主次
        # 第1个link的left_table是主表，后续link的left_table是上一轮的结果
        tables = []
        processed_tables = set()
        
        # 按links顺序处理，确保主表在前
        for link in links:
            left_name = link.get('left_name', link.get('left_table', ''))
            right_name = link.get('right_name', link.get('right_table', ''))
            
            # 添加左表
            if left_name not in processed_tables:
                for name, info in self.linker.dataframes.items():
                    if left_name in name or name in left_name:
                        tables.append({
                            'name': name,
                            'file_path': info.get('file_path', ''),
                            'sheet_name': info.get('sheet', '')
                        })
                        processed_tables.add(left_name)
                        break
            
            # 添加右表
            if right_name not in processed_tables:
                for name, info in self.linker.dataframes.items():
                    if right_name in name or name in right_name:
                        tables.append({
                            'name': name,
                            'file_path': info.get('file_path', ''),
                            'sheet_name': info.get('sheet', '')
                        })
                        processed_tables.add(right_name)
                        break
        
        # 添加任何遗漏的表
        for name, info in self.linker.dataframes.items():
            if name not in [t['name'] for t in tables]:
                tables.append({
                    'name': name,
                    'file_path': info.get('file_path', ''),
                    'sheet_name': info.get('sheet', '')
                })
        
        self._log(f"[Subprocess] 表顺序: {[t['name'][:20] for t in tables]}")
        
        # 创建输出文件
        os.makedirs('output', exist_ok=True)
        output_path = "output/query_result.xlsx"
        
        try:
            # 准备输入数据
            input_data = json.dumps({
                'tables': tables,
                'links': links,
                'output_fields': output_fields or {}
            }).encode('utf-8')
            
            self._log(f"[Subprocess] 启动子进程...")
            
            # Fork子进程执行查询
            # 使用subprocess.run而不是fork（Python跨平台）
            result = subprocess.run(
                [sys.executable, '-c', QUERY_SCRIPT, output_path],
                input=input_data,
                capture_output=True,
                text=False,
                timeout=600  # 10分钟超时
            )
            
            if result.returncode != 0:
                error_msg = result.stderr.decode('utf-8', errors='ignore')
                self._log(f"[Subprocess] 错误: {error_msg}")
                raise Exception(f"子进程执行失败: {error_msg}")
            
            self._log(f"[Subprocess] 输出: {result.stdout.decode('utf-8', errors='ignore')}")
            
            # 读取结果
            if os.path.exists(output_path):
                df = pd.read_excel(output_path)
                self._log(f"[Subprocess] 读取结果: {len(df)}行")
                
                # 清理临时文件
                os.remove(output_path)
                
                return df
            else:
                raise Exception("结果文件不存在")
                
        except subprocess.TimeoutExpired:
            raise Exception("查询超时（10分钟）")
        except Exception as e:
            # 清理临时文件
            if os.path.exists(output_path):
                os.remove(output_path)
            raise e


    def export_to_excel(self, df: pd.DataFrame, output_path: str) -> str:
        """导出结果到Excel"""
        os.makedirs(os.path.dirname(output_path) or '.', exist_ok=True)
        df.to_excel(output_path, index=False)
        return output_path

    def get_statistics(self, df: pd.DataFrame) -> Dict[str, Any]:
        """获取查询统计信息"""
        return {
            'rows': len(df),
            'columns': len(df.columns),
            'fields': list(df.columns)
        }


def create_executor(debug: bool = False) -> QueryExecutor:
    """创建查询执行器"""
    return QueryExecutor(debug=debug)
