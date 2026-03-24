"""
配置管理模块
"""

import os

# 项目根目录
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# 静态文件目录
STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'app', 'static')

# 默认端口
DEFAULT_PORT = 8765

# Excel支持的扩展名
EXCEL_EXTENSIONS = ['.xlsx', '.xls']

# 大文件阈值（MB）
LARGE_FILE_THRESHOLD = 50

# 分块读取大小
CHUNK_SIZE = 10000

# 最大关联表数
MAX_LINK_TABLES = 5

# 调试模式
DEBUG = False

# 日志级别
LOG_LEVEL = 'INFO'


class Config:
    """配置类"""
    
    def __init__(self):
        self.project_root = PROJECT_ROOT
        self.static_dir = STATIC_DIR
        self.port = DEFAULT_PORT
        self.debug = DEBUG
        
    def update(self, **kwargs):
        """更新配置"""
        for key, value in kwargs.items():
            if hasattr(self, key):
                setattr(self, key, value)


# 全局配置实例
config = Config()