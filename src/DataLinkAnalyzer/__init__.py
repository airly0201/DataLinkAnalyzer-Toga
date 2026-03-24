"""
DataLinkAnalyzer - 数据关联分析平台 Android App

使用 BeeWare Toga + WebView 封装 Python Web 应用
"""

__version__ = "1.0.0"
__app_name__ = "DataLinkAnalyzer"

from .android_main import main, create_app

__all__ = ["main", "create_app", "__version__", "__app_name__"]