"""
Web服务器
"""

import os
import json
from pathlib import Path
from starlette.applications import Starlette
from starlette.routing import Route, Mount
from starlette.staticfiles import StaticFiles
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware
from starlette.responses import HTMLResponse, JSONResponse, FileResponse
from starlette.staticfiles import StaticFiles

from app.routes import (
    home_page,
    scan_folder,
    scan_file,
    get_headers,
    get_sheets,
    load_tables,
    execute_query,
    export_data,
    generate_chart,
    save_config,
    load_config,
    generate_script,
    debug_log,
    select_folder_dialog,
    save_named_config,
    load_named_config,
    list_configs,
    delete_named_config
)


# 下载结果文件
async def download_file(request):
    """下载结果文件"""
    file_path = request.query_params.get('path', 'output/query_result.xlsx')
    filename = file_path.split('/')[-1]
    
    if os.path.exists(file_path):
        return FileResponse(file_path, filename=filename, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    else:
        return JSONResponse({'error': '文件不存在'}, status_code=404)


def create_app(debug: bool = False) -> Starlette:
    """
    创建ASGI应用
    
    Args:
        debug: 调试模式
        
    Returns:
        Starlette应用
    """
    # 获取静态文件目录
    static_dir = os.path.join(os.path.dirname(__file__), 'static')
    
    # 确保静态目录存在
    os.makedirs(static_dir, exist_ok=True)
    
    # 检查index.html是否存在
    index_path = os.path.join(static_dir, 'index.html')
    if not os.path.exists(index_path):
        # 创建默认的index.html
        create_default_index(static_dir)
    
    # 定义路由
    routes = [
        Route('/', home_page),
        Route('/api/scan-folder', scan_folder, methods=['POST']),
        Route('/api/scan-file', scan_file, methods=['POST']),
        Route('/api/get-headers', get_headers, methods=['POST']),
        Route('/api/get-sheets', get_sheets, methods=['POST']),
        Route('/api/load-tables', load_tables, methods=['POST']),
        Route('/api/execute-query', execute_query, methods=['POST']),
        Route('/api/export-data', export_data, methods=['POST']),
        Route('/api/generate-chart', generate_chart, methods=['POST']),
        Route('/api/save-config', save_config, methods=['POST']),
        Route('/api/load-config', load_config, methods=['POST']),
        Route('/api/save-named-config', save_named_config, methods=['POST']),
        Route('/api/load-named-config', load_named_config, methods=['POST']),
        Route('/api/delete-named-config', delete_named_config, methods=['POST']),
        Route('/api/list-configs', list_configs, methods=['GET', 'POST']),
        Route('/api/generate-script', generate_script, methods=['POST']),
        Route('/api/debug-log', debug_log, methods=['GET', 'POST']),
        Route('/api/select-folder', select_folder_dialog, methods=['POST']),
        Route('/api/download', download_file, methods=['GET']),
        Mount('/static', StaticFiles(directory=static_dir), name='static'),
    ]
    
    # 中间件
    middleware = [
        Middleware(
            CORSMiddleware,
            allow_origins=['*'],
            allow_methods=['*'],
            allow_headers=['*'],
        )
    ]
    
    app = Starlette(
        debug=debug,
        routes=routes,
        middleware=middleware
    )
    
    return app


def create_default_index(static_dir: str) -> None:
    """
    创建默认的index.html
    
    Args:
        static_dir: 静态文件目录
    """
    html = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>数据关联分析平台</title>
    <link rel="stylesheet" href="/static/styles.css">
</head>
<body>
    <div class="container">
        <header>
            <h1>🔗 数据关联分析平台</h1>
            <p>支持多表关联、灵活查询、可视化分析</p>
        </header>
        
        <main>
            <!-- 模块一：文件选择 -->
            <section class="module" id="module-file">
                <h2>📂 文件选择</h2>
                <div class="form-group">
                    <label>选择文件夹：</label>
                    <input type="text" id="folder-path" placeholder="点击选择文件夹" readonly>
                    <button onclick="selectFolder()">选择文件夹</button>
                </div>
                <div class="file-list" id="file-list">
                    <p class="hint">请先选择文件夹</p>
                </div>
            </section>
            
            <!-- 模块二：关联配置 -->
            <section class="module" id="module-link">
                <h2>🔗 多表关联</h2>
                <div class="form-group">
                    <label>主表：</label>
                    <select id="main-table"></select>
                </div>
                <div class="form-group">
                    <button onclick="addLink()">+ 添加关联</button>
                </div>
                <div class="link-list" id="link-list">
                    <p class="hint">未配置关联</p>
                </div>
            </section>
            
            <!-- 模块三：输出字段 -->
            <section class="module" id="module-output">
                <h2>📤 输出字段</h2>
                <div class="form-group">
                    <select id="output-table">
                        <option value="">选择表</option>
                    </select>
                    <select id="output-sheet">
                        <option value="">选择Sheet</option>
                    </select>
                </div>
                <div class="field-list" id="field-list">
                    <p class="hint">选择表后显示字段</p>
                </div>
                <div class="selected-fields" id="selected-fields">
                    <h4>已选字段：</h4>
                    <div class="tags" id="selected-tags"></div>
                </div>
            </section>
            
            <!-- 模块四：功能按钮 -->
            <section class="module" id="module-actions">
                <h2>⚡ 功能操作</h2>
                <div class="button-group">
                    <button class="primary" onclick="saveConfig()">💾 保存设置</button>
                    <button class="primary" onclick="executeQuery()">🔍 查询输出</button>
                    <button class="secondary" onclick="generateScript()">🐍 生成脚本</button>
                    <button class="secondary" onclick="showChartDialog()">📊 生成图表</button>
                </div>
            </section>
            
            <!-- 调试输出 -->
            <section class="debug-panel" id="debug-panel">
                <h3>🔧 调测信息</h3>
                <div class="debug-content" id="debug-content">
                    <p>等待操作...</p>
                </div>
            </section>
        </main>
    </div>
    
    <script src="/static/app.js"></script>
</body>
</html>
"""
    
    with open(os.path.join(static_dir, 'index.html'), 'w', encoding='utf-8') as f:
        f.write(html)
    
    # 创建默认CSS
    css = """/* 基础样式 */
* {
    margin: 0;
    padding: 0;
    box-sizing: border-box;
}

body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    min-height: 100vh;
    padding: 20px;
}

.container {
    max-width: 1200px;
    margin: 0 auto;
}

header {
    text-align: center;
    color: white;
    padding: 30px;
    margin-bottom: 20px;
}

header h1 {
    font-size: 2.5em;
    margin-bottom: 10px;
}

main {
    display: flex;
    flex-direction: column;
    gap: 20px;
}

.module {
    background: white;
    border-radius: 12px;
    padding: 24px;
    box-shadow: 0 4px 6px rgba(0,0,0,0.1);
}

.module h2 {
    color: #333;
    margin-bottom: 20px;
    padding-bottom: 10px;
    border-bottom: 2px solid #667eea;
}

.form-group {
    margin-bottom: 15px;
    display: flex;
    align-items: center;
    gap: 10px;
    flex-wrap: wrap;
}

.form-group label {
    font-weight: 500;
    color: #555;
    min-width: 80px;
}

.form-group input,
.form-group select {
    flex: 1;
    min-width: 200px;
    padding: 10px 15px;
    border: 2px solid #e0e0e0;
    border-radius: 8px;
    font-size: 14px;
    transition: border-color 0.3s;
}

.form-group input:focus,
.form-group select:focus {
    outline: none;
    border-color: #667eea;
}

button {
    padding: 10px 20px;
    border: none;
    border-radius: 8px;
    cursor: pointer;
    font-size: 14px;
    transition: all 0.3s;
}

button.primary {
    background: #667eea;
    color: white;
}

button.primary:hover {
    background: #5568d3;
}

button.secondary {
    background: #764ba2;
    color: white;
}

button.secondary:hover {
    background: #6a4190;
}

.file-list,
.link-list,
.field-list {
    margin-top: 15px;
    max-height: 300px;
    overflow-y: auto;
    border: 1px solid #e0e0e0;
    border-radius: 8px;
    padding: 10px;
}

.hint {
    color: #999;
    text-align: center;
    padding: 20px;
}

.button-group {
    display: flex;
    gap: 15px;
    flex-wrap: wrap;
}

.debug-panel {
    background: #1e1e1e;
    color: #d4d4d4;
    border-radius: 12px;
    padding: 20px;
}

.debug-panel h3 {
    color: #4ec9b0;
    margin-bottom: 10px;
}

.debug-content {
    font-family: 'Consolas', 'Monaco', monospace;
    font-size: 13px;
    max-height: 200px;
    overflow-y: auto;
    background: #2d2d2d;
    padding: 10px;
    border-radius: 8px;
}

/* 响应式设计 */
@media (max-width: 768px) {
    header h1 {
        font-size: 1.8em;
    }
    
    .form-group {
        flex-direction: column;
        align-items: stretch;
    }
    
    .form-group input,
    .form-group select {
        min-width: 100%;
    }
    
    .button-group {
        flex-direction: column;
    }
    
    .button-group button {
        width: 100%;
    }
}

@media (max-width: 480px) {
    body {
        padding: 10px;
    }
    
    .module {
        padding: 15px;
    }
}
"""
    
    with open(os.path.join(static_dir, 'styles.css'), 'w', encoding='utf-8') as f:
        f.write(css)
    
    # 创建默认JS
    js = """// 数据关联分析平台 - 前端脚本

// 调试日志
function debugLog(message, type = 'info') {
    const debugContent = document.getElementById('debug-content');
    const time = new Date().toLocaleTimeString();
    const prefix = type === 'error' ? '❌' : type === 'success' ? '✅' : 'ℹ️';
    debugContent.innerHTML += `<p>[${time}] ${prefix} ${message}</p>`;
    debugContent.scrollTop = debugContent.scrollHeight;
    console.log(`[Debug] ${message}`);
}

// API调用
async function apiCall(url, data) {
    try {
        const response = await fetch(url, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(data)
        });
        const result = await response.json();
        if (!result.success) {
            throw new Error(result.error || '操作失败');
        }
        return result;
    } catch (error) {
        debugLog(error.message, 'error');
        throw error;
    }
}

// 选择文件夹
async function selectFolder() {
    // 由于浏览器安全限制，这里需要后端支持
    // 暂时使用输入框手动输入
    const path = prompt('请输入文件夹路径：');
    if (path) {
        document.getElementById('folder-path').value = path;
        await scanFolder(path);
    }
}

// 扫描文件夹
async function scanFolder(folderPath) {
    debugLog(`扫描文件夹: ${folderPath}`);
    try {
        const result = await apiCall('/api/scan-folder', {folder_path: folderPath});
        displayFileList(result.files);
        debugLog(`找到 ${result.files.length} 个Excel文件`, 'success');
    } catch (error) {
        debugLog(`扫描失败: ${error.message}`, 'error');
    }
}

// 显示文件列表
function displayFileList(files) {
    const container = document.getElementById('file-list');
    if (files.length === 0) {
        container.innerHTML = '<p class="hint">未找到Excel文件</p>';
        return;
    }
    
    container.innerHTML = files.map((f, i) => `
        <div class="file-item">
            <input type="checkbox" id="file-${i}" value="${f.path}">
            <label for="file-${i}">${f.name} (${f.size_mb} MB)</label>
        </div>
    `).join('');
}

// 扫描选中的文件
async function scanSelectedFiles() {
    const checkboxes = document.querySelectorAll('#file-list input[type="checkbox"]:checked');
    const files = Array.from(checkboxes).map(cb => cb.value);
    
    if (files.length === 0) {
        alert('请先选择文件');
        return;
    }
    
    debugLog(`扫描 ${files.length} 个文件...`);
    
    for (const filePath of files) {
        try {
            const result = await apiCall('/api/scan-file', {file_path: filePath});
            updateTableSelect(result);
            debugLog(`文件 ${result.filename} 扫描完成: ${result.sheets.length} 个Sheet`, 'success');
        } catch (error) {
            debugLog(`扫描失败: ${error.message}`, 'error');
        }
    }
}

// 更新表选择下拉框
function updateTableSelect(fileInfo) {
    const select = document.getElementById('main-table');
    const option = document.createElement('option');
    option.value = fileInfo.path;
    option.textContent = `${fileInfo.filename}`;
    select.appendChild(option);
}

console.log('数据关联分析平台已加载');
"""
    
    with open(os.path.join(static_dir, 'app.js'), 'w', encoding='utf-8') as f:
        f.write(js)

# 添加下载路由
async def download_file(request):
    """下载结果文件"""
    file_path = request.query_params.get('path', 'output/query_result.xlsx')
    filename = file_path.split('/')[-1]
    
    if os.path.exists(file_path):
        return FileResponse(file_path, filename=filename, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    else:
        return JSONResponse({'error': '文件不存在'}, status_code=404)


# 在routes列表中添加
# 找到routes定义的位置并添加
