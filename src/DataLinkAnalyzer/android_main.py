#!/usr/bin/env python3
"""
DataLinkAnalyzer - Android 主入口
使用 BeeWare Toga + WebView 封装 Python Web 应用
"""

import asyncio
import threading
import os
import sys
from pathlib import Path

# 添加应用代码目录到Python路径
APP_DIR = Path(__file__).parent
sys.path.insert(0, str(APP_DIR))

# ==================== 后台服务管理 ====================

class StarletteServer:
    """后台运行的Starlette服务器管理"""
    
    def __init__(self, host='127.0.0.1', port=8080):
        self.host = host
        self.port = port
        self.server = None
        self.thread = None
        self.running = False
    
    def _run_server(self):
        """在独立线程中运行服务器"""
        import uvicorn
        from app.server import create_app
        
        # 创建ASGI应用
        app = create_app(debug=False)
        
        # 配置并运行服务器
        config = uvicorn.Config(
            app,
            host=self.host,
            port=self.port,
            log_level="info",
            timeout_keep_alive=300,
            limit_concurrency=50,
            access_log=False,
        )
        self.server = uvicorn.Server(config)
        self.running = True
        
        # 运行服务器（阻塞直到停止）
        asyncio.run(self.server.serve())
        self.running = False
    
    def start(self):
        """启动服务器线程"""
        if self.running:
            return
        
        self.thread = threading.Thread(target=self._run_server, daemon=True)
        self.thread.start()
        
        # 等待服务器启动
        import time
        for _ in range(30):  # 最多等3秒
            time.sleep(0.1)
            if self.running:
                break
    
    def stop(self):
        """停止服务器"""
        if self.server:
            self.server.should_exit = True
        self.running = False


# 全局服务器实例
_server_instance = None


def get_server() -> StarletteServer:
    """获取或创建服务器实例"""
    global _server_instance
    if _server_instance is None:
        _server_instance = StarletteServer(host='127.0.0.1', port=8080)
    return _server_instance


# ==================== Toga 应用 ====================

def create_app():
    """创建Toga应用"""
    import toga
    from toga import WebView
    from toga.style import Pack
    from toga.style.pack import COLUMN, ROW, CENTER
    
    # 服务器URL
    SERVER_URL = "http://127.0.0.1:8080"
    
    class DataLinkAnalyzerApp(toga.App):
        def startup(self):
            """应用启动"""
            # 启动后台服务器
            server = get_server()
            server.start()
            
            # 创建主窗口
            self.main_window = toga.MainWindow(
                title="数据关联分析平台",
                size=(800, 600),
                resizable=True,
            )
            
            # 创建WebView
            self.webview = WebView(
                url=SERVER_URL,
                style=Pack(flex=1),
            )
            
            # 添加到主窗口
            self.main_window.content = self.webview
            
            # 显示窗口
            self.main_window.show()
        
        def shutdown(self):
            """应用关闭"""
            server = get_server()
            server.stop()
    
    return DataLinkAnalyzerApp("DataLinkAnalyzer", "com.datalink.analyzer")


# ==================== 入口点 ====================

def main():
    """主入口点 - 被Toga调用"""
    app = create_app()
    return app


if __name__ == "__main__":
    # 直接运行时的调试入口
    app = main()
    app.run()