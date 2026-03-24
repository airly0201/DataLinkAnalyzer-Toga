"""
数据清洗工具
负责字段名标准化、去重、空值处理等
"""

import re
import pandas as pd
from typing import List, Dict, Any, Optional


def clean_field_name(field_name: Any) -> str:
    """
    清洗字段名
    
    处理内容：
    - 去除前后空格
    - 去除换行符
    - 去除多余空格
    - 转换为字符串
    
    Args:
        field_name: 原始字段名
        
    Returns:
        清洗后的字段名
    """
    if field_name is None:
        return ""
    
    # 转换为字符串
    field_str = str(field_name)
    
    # 去除前后空格
    field_str = field_str.strip()
    
    # 去除换行符
    field_str = field_str.replace('\n', '').replace('\r', '')
    
    # 去除多余空格（多个空格合并为一个）
    field_str = re.sub(r'\s+', ' ', field_str)
    
    return field_str


def clean_dataframe_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    清洗DataFrame的列名
    
    Args:
        df: 原始DataFrame
        
    Returns:
        列名清洗后的DataFrame
    """
    # 清洗列名
    df.columns = [clean_field_name(col) for col in df.columns]
    return df


def clean_dataframe_values(df: pd.DataFrame, columns: Optional[List[str]] = None) -> pd.DataFrame:
    """
    清洗DataFrame指定列的值（去除空格）
    
    Args:
        df: 原始DataFrame
        columns: 需要清洗的列名列表，None表示所有列
        
    Returns:
        清洗后的DataFrame
    """
    df = df.copy()
    
    if columns is None:
        columns = df.columns.tolist()
    
    for col in columns:
        if col in df.columns:
            # 字符串列去除空格
            if df[col].dtype == 'object':
                df[col] = df[col].apply(lambda x: clean_field_name(x) if isinstance(x, str) else x)
    
    return df


def remove_duplicates(df: pd.DataFrame, subset: Optional[List[str]] = None) -> pd.DataFrame:
    """
    去除重复行
    
    Args:
        df: 原始DataFrame
        subset: 用于判断重复的列，None表示所有列
        
    Returns:
        去重后的DataFrame
    """
    return df.drop_duplicates(subset=subset)


def handle_null_values(df: pd.DataFrame, strategy: str = 'keep') -> pd.DataFrame:
    """
    处理空值
    
    Args:
        df: 原始DataFrame
        strategy: 处理策略
            - 'keep': 保留空值
            - 'drop': 删除空值行
            - 'fill_empty': 用空字符串填充
            - 'fill_na': 用NaN填充
            
    Returns:
        处理后的DataFrame
    """
    df = df.copy()
    
    if strategy == 'drop':
        df = df.dropna()
    elif strategy == 'fill_empty':
        df = df.fillna('')
    elif strategy == 'fill_na':
        df = df.fillna(pd.NA)
    # 'keep' 什么都不做
    
    return df


def normalize_field_for_link(field_value: Any) -> str:
    """
    标准化关联字段值（用于关联匹配）
    
    Args:
        field_value: 字段值
        
    Returns:
        标准化后的值（用于关联比较）
    """
    if pd.isna(field_value) or field_value is None:
        return '__NULL__'
    
    # 转换为字符串并清洗
    return clean_field_name(str(field_value)).upper()


def build_field_mapping(field_names: List[str]) -> Dict[str, str]:
    """
    建立字段名映射（处理同一字段不同表头名的情况）
    
    Args:
        field_names: 字段名列表
        
    Returns:
        字段名映射字典 {标准化名: 原名}
    """
    mapping = {}
    seen = {}
    
    for name in field_names:
        cleaned = clean_field_name(name)
        if cleaned:
            # 如果已存在，添加序号区分
            if cleaned in seen:
                seen[cleaned] += 1
                mapping[f"{cleaned}_{seen[cleaned]}"] = name
            else:
                seen[cleaned] = 0
                mapping[cleaned] = name
    
    return mapping