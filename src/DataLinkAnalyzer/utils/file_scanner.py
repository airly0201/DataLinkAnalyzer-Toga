"""
文件扫描工具
负责扫描文件夹、查找Excel文件、识别相似文件等
"""

import os
import re
from typing import List, Dict, Any, Optional
from pathlib import Path

# 支持的Excel扩展名
EXCEL_EXTENSIONS = ['.xlsx', '.xls']


def scan_folder(folder_path: str) -> List[Dict[str, Any]]:
    """
    扫描文件夹，返回Excel文件列表
    
    Args:
        folder_path: 文件夹路径
        
    Returns:
        文件信息列表 [{name, path, size, modified_time}, ...]
    """
    files = []
    
    if not os.path.exists(folder_path):
        return files
    
    if not os.path.isdir(folder_path):
        return files
    
    for filename in os.listdir(folder_path):
        filepath = os.path.join(folder_path, filename)
        
        # 检查是否是文件
        if not os.path.isfile(filepath):
            continue
        
        # 检查扩展名
        ext = os.path.splitext(filename)[1].lower()
        if ext not in EXCEL_EXTENSIONS:
            continue
        
        # 获取文件信息
        try:
            stat = os.stat(filepath)
            files.append({
                'name': filename,
                'path': filepath,
                'size': stat.st_size,
                'size_mb': round(stat.st_size / (1024 * 1024), 2),
                'modified_time': stat.st_mtime
            })
        except Exception as e:
            print(f"Error getting file info: {e}")
            continue
    
    # 按修改时间排序（最新的在前）
    files.sort(key=lambda x: x['modified_time'], reverse=True)
    
    return files


def find_similar_files(base_filename: str, folder_path: str) -> List[Dict[str, Any]]:
    """
    查找相似名称的文件（用于识别不同日期版本）
    
    识别规则：
    - 去除日期前后缀后比较
    - 日期格式：YYYYMMDD, YYYY-MM-DD, YYYY_MM_DD, YYYYMMDDHHMMSS等
    
    Args:
        base_filename: 基准文件名
        folder_path: 要搜索的文件夹
        
    Returns:
        相似文件列表
    """
    # 提取文件名（不含扩展名）
    base_name = os.path.splitext(base_filename)[0]
    
    # 去除日期模式
    patterns_to_remove = [
        r'\d{8}',           # YYYYMMDD
        r'\d{4}-\d{2}-\d{2}',  # YYYY-MM-DD
        r'\d{4}_\d{2}_\d{2}',  # YYYY_MM_DD
        r'\d{12}',          # YYYYMMDDHHMM
        r'\d{14}',          # YYYYMMDDHHMMSS
        r'v\d+',            # v1, v2等版本号
        r'_\d+$',           # 末尾的数字
    ]
    
    # 生成核心名称（去除日期和版本号）
    core_name = base_name
    for pattern in patterns_to_remove:
        core_name = re.sub(pattern, '', core_name, flags=re.IGNORECASE)
    
    # 清理多余的分隔符
    core_name = re.sub(r'[_-]+', '_', core_name).strip('_')
    
    # 扫描文件夹
    all_files = scan_folder(folder_path)
    
    # 查找相似文件
    similar_files = []
    
    for file_info in all_files:
        filename = os.path.splitext(file_info['name'])[0]
        test_name = filename
        
        # 对每个候选文件去除日期模式
        for pattern in patterns_to_remove:
            test_name = re.sub(pattern, '', test_name, flags=re.IGNORECASE)
        
        test_name = re.sub(r'[_-]+', '_', test_name).strip('_')
        
        # 比较核心名称
        if test_name == core_name and filename != base_name:
            file_info['core_name'] = core_name
            file_info['date_suffix'] = extract_date_suffix(filename)
            similar_files.append(file_info)
    
    return similar_files


def extract_date_suffix(filename: str) -> Optional[str]:
    """
    提取文件名中的日期后缀
    
    Args:
        filename: 文件名
        
    Returns:
        日期字符串，如果未找到返回None
    """
    # 匹配各种日期格式
    patterns = [
        r'(\d{8})',           # YYYYMMDD
        r'(\d{4}-\d{2}-\d{2})',  # YYYY-MM-DD
        r'(\d{4}_\d{2}_\d{2})',  # YYYY_MM_DD
        r'(\d{12})',          # YYYYMMDDHHMM
        r'(\d{14})',          # YYYYMMDDHHMMSS
    ]
    
    for pattern in patterns:
        match = re.search(pattern, filename)
        if match:
            return match.group(1)
    
    return None


def is_large_file(file_path: str, threshold_mb: int = 50) -> bool:
    """
    判断是否是大文件
    
    Args:
        file_path: 文件路径
        threshold_mb: 阈值（MB）
        
    Returns:
        是否为大文件
    """
    try:
        size = os.path.getsize(file_path)
        size_mb = size / (1024 * 1024)
        return size_mb > threshold_mb
    except Exception:
        return False


def format_file_size(size_bytes: int) -> str:
    """
    格式化文件大小
    
    Args:
        size_bytes: 字节数
        
    Returns:
        格式化后的大小字符串
    """
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.2f} TB"