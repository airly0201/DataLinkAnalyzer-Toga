"""
脚本生成模块
生成可独立运行的Python查询脚本
"""

import os
from typing import List, Dict, Any, Optional
from datetime import datetime


class ScriptGenerator:
    """独立查询脚本生成器"""
    
    def __init__(self):
        """初始化生成器"""
        pass
    
    def generate_script(self,
                        tables: List[Dict[str, Any]],
                        links: List[Dict[str, Any]],
                        output_fields: Dict[str, List[str]],
                        output_file: str,
                        debug: bool = False) -> str:
        """
        生成独立运行的Python脚本
        
        Args:
            tables: 表配置列表
            links: 关联配置列表
            output_fields: 输出字段配置
            output_file: 输出文件路径
            debug: 是否调试模式
            
        Returns:
            脚本内容
        """
        script_lines = []
        
        # 文件头
        script_lines.append('#!/usr/bin/env python3')
        script_lines.append('"""')
        script_lines.append('数据关联查询脚本')
        script_lines.append(f'生成时间: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        script_lines.append('"""')
        script_lines.append('')
        
        # 导入
        script_lines.append('import os')
        script_lines.append('import sys')
        script_lines.append('import pandas as pd')
        script_lines.append('from pathlib import Path')
        script_lines.append('')
        
        # 配置部分
        script_lines.append('# ==================== 配置 ====================')
        script_lines.append('')
        script_lines.append('# 表配置')
        tables_config = []
        for i, table in enumerate(tables):
            tables_config.append(f"    {{")
            tables_config.append(f"        'name': '{table['name']}',")
            tables_config.append(f"        'file_path': r'{table['file_path']}',")
            tables_config.append(f"        'sheet_name': '{table['sheet_name']}',")
            tables_config.append(f"    }}")
        
        script_lines.append('TABLES = [')
        script_lines.append(',\n'.join(tables_config))
        script_lines.append(']')
        script_lines.append('')
        
        # 关联配置
        script_lines.append('# 关联配置')
        links_config = []
        for link in links:
            links_config.append(f"    {{")
            links_config.append(f"        'left_table': '{link['left_table']}',")
            links_config.append(f"        'right_table': '{link['right_table']}',")
            links_config.append(f"        'left_field': '{link['left_field']}',")
            links_config.append(f"        'right_field': '{link['right_field']}',")
            links_config.append(f"        'join_type': '{link.get('join_type', 'left')}',")
            links_config.append(f"    }}")
        
        script_lines.append('LINKS = [')
        script_lines.append(',\n'.join(links_config))
        script_lines.append(']')
        script_lines.append('')
        
        # 输出字段配置
        script_lines.append('# 输出字段配置')
        output_fields_str = '{'
        for table_name, fields in output_fields.items():
            if output_fields_str != '{':
                output_fields_str += ', '
            output_fields_str += f"'{table_name}': {fields}"
        output_fields_str += '}'
        script_lines.append(f'OUTPUT_FIELDS = {output_fields_str}')
        script_lines.append('')
        
        # 输出文件
        script_lines.append(f'OUTPUT_FILE = r"{output_file}"')
        script_lines.append('')
        
        # 函数定义
        script_lines.append('# ==================== 核心函数 ====================')
        script_lines.append('')
        
        script_lines.append('''
def clean_field_name(field_name):
    """清洗字段名"""
    if field_name is None:
        return ""
    field_str = str(field_name).strip()
    field_str = field_str.replace('\\n', '').replace('\\r', '')
    return field_str

def normalize_field(value):
    """标准化关联字段"""
    if pd.isna(value) or value is None:
        return '__NULL__'
    return clean_field_name(str(value)).upper()

def load_table(table_config):
    """加载单个表"""
    print(f"加载表: {table_config['name']}")
    df = pd.read_excel(
        table_config['file_path'],
        sheet_name=table_config['sheet_name'],
        engine='openpyxl'
    )
    # 清洗列名
    df.columns = [clean_field_name(col) for col in df.columns]
    # 清洗数据
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].apply(lambda x: clean_field_name(x) if isinstance(x, str) else x)
    print(f"  - {len(df)} 行, {len(df.columns)} 列")
    return df

def link_tables(df_left, df_right, left_field, right_field, join_type='left'):
    """关联两个表"""
    print(f"关联表: {left_field} = {right_field}")
    df_left = df_left.copy()
    df_right = df_right.copy()
    df_left['_link_key'] = df_left[left_field].apply(normalize_field)
    df_right['_link_key'] = df_right[right_field].apply(normalize_field)
    cols_to_add = [c for c in df_right.columns if c != '_link_key']
    df_right_renamed = df_right[['_link_key'] + cols_to_add].copy()
    prefix = f"{df_right.columns[0]}_right" if '_link_key' in df_right.columns else "right"
    df_right_renamed.columns = ['_link_key'] + [f"{prefix}.{c}" for c in cols_to_add]
    result = pd.merge(df_left, df_right_renamed, on='_link_key', how=join_type)
    result = result.drop(columns=['_link_key'], errors='ignore')
    print(f"  - 结果: {len(result)} 行")
    return result

def execute_query():
    """执行查询"""
    print("=" * 50)
    print("开始执行查询")
    print("=" * 50)
    
    # 加载所有表
    dataframes = {}
    for table in TABLES:
        dataframes[table['name']] = load_table(table)
    
    if len(dataframes) == 1:
        result = list(dataframes.values())[0]
    else:
        # 链式关联
        first_table = TABLES[0]['name']
        result = dataframes[first_table]
        processed = {first_table}
        
        for link in LINKS:
            left = link['left_table']
            right = link['right_table']
            
            if left in processed and right not in processed:
                result = link_tables(
                    result, dataframes[right],
                    link['left_field'], link['right_field'],
                    link.get('join_type', 'left')
                )
                processed.add(right)
            elif right in processed and left not in processed:
                result = link_tables(
                    result, dataframes[left],
                    link['right_field'], link['left_field'],
                    link.get('join_type', 'left')
                )
                processed.add(left)
    
    # 选择输出字段
    if OUTPUT_FIELDS:
        output_cols = []
        for table_name, fields in OUTPUT_FIELDS.items():
            for f in fields:
                matching = [c for c in result.columns if c == f or c.endswith(f'.{f}')]
                output_cols.extend(matching)
        if output_cols:
            seen = set()
            unique_cols = []
            for c in output_cols:
                if c not in seen:
                    seen.add(c)
                    unique_cols.append(c)
            result = result[unique_cols]
    
    print(f"\\n查询完成: {len(result)} 行, {len(result.columns)} 列")
    return result

def main():
    """主函数"""
    # 执行查询
    result = execute_query()
    
    # 保存结果
    print(f"\\n保存结果到: {OUTPUT_FILE}")
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    result.to_excel(OUTPUT_FILE, sheet_name='结果', index=False, engine='openpyxl')
    print("完成!")
    
    return result

if __name__ == '__main__':
    main()
''')
        
        # 写入文件
        script_content = '\n'.join(script_lines)
        
        # 确保目录存在
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(script_content)
        
        return output_file
    
    def generate_config_json(self,
                             tables: List[Dict[str, Any]],
                             links: List[Dict[str, Any]],
                             output_fields: Dict[str, List[str]],
                             output_file: str) -> str:
        """
        生成配置JSON文件
        
        Args:
            tables: 表配置
            links: 关联配置
            output_fields: 输出字段
            output_file: 输出文件路径
            
        Returns:
            文件路径
        """
        import json
        
        config = {
            'tables': tables,
            'links': links,
            'output_fields': output_fields,
            'output_file': output_file,
            'generated_at': datetime.now().isoformat()
        }
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(config, f, ensure_ascii=False, indent=2)
        
        return output_file


def create_script_generator() -> ScriptGenerator:
    """
    工厂函数 - 创建ScriptGenerator实例
    
    Returns:
        ScriptGenerator实例
    """
    return ScriptGenerator()