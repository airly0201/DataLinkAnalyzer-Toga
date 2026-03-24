"""
多表关联引擎
支持1-5个表的链式关联查询
"""

import pandas as pd
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass, field

from utils.cleaner import clean_dataframe_values, normalize_field_for_link


@dataclass
class LinkConfig:
    """关联配置"""
    table_name: str           # 表名（文件名）
    sheet_name: str           # Sheet名
    link_field: str           # 关联字段
    join_type: str = 'left'   # 连接类型：left, inner, outer
    

@dataclass
class TableConfig:
    """表配置"""
    name: str                 # 显示名称
    file_path: str            # 文件路径
    sheet_name: str           # Sheet名称
    fields: List[str] = field(default_factory=list)  # 输出字段
    link_config: Optional[LinkConfig] = None  # 关联配置


class Linker:
    """多表关联引擎"""
    
    def __init__(self, debug: bool = False):
        """
        初始化关联引擎
        
        Args:
            debug: 是否输出调试信息
        """
        self.debug = debug
        self.tables: List[TableConfig] = []
        self.dataframes: Dict[str, pd.DataFrame] = {}
        
    def add_table(self, table: TableConfig) -> None:
        """
        添加表配置
        
        Args:
            table: 表配置
        """
        self.tables.append(table)
        self._log(f"添加表: {table.name}")
    
    def remove_table(self, table_name: str) -> None:
        """
        移除表配置
        
        Args:
            table_name: 表名
        """
        self.tables = [t for t in self.tables if t.name != table_name]
        self._log(f"移除表: {table_name}")
    
    def set_output_fields(self, table_name: str, fields: List[str]) -> None:
        """
        设置输出字段
        
        Args:
            table_name: 表名
            fields: 字段列表
        """
        for table in self.tables:
            if table.name == table_name:
                table.fields = fields
                break
    
    def link_tables(self, 
                    left_table: str, 
                    right_table: str,
                    left_field: str, 
                    right_field: str,
                    join_type: str = 'left',
                    output_fields: Optional[Dict[str, List[str]]] = None) -> pd.DataFrame:
        """
        关联两个表（优化内存版本）
        
        Args:
            left_table: 左表名
            right_table: 右表名
            left_field: 左表关联字段
            right_field: 右表关联字段
            join_type: 连接类型
            output_fields: 用户选择的输出字段
            
        Returns:
            关联后的DataFrame
        """
        # 获取数据
        df_left = self.dataframes.get(left_table)
        df_right = self.dataframes.get(right_table)
        
        if df_left is None:
            raise ValueError(f"表 {left_table} 未加载")
        if df_right is None:
            raise ValueError(f"表 {right_table} 未加载")
        
        # 内存检查 - 如果太大，使用inner join减少内存
        total_rows = len(df_left) + len(df_right)
        # 更保守的内存保护
        if total_rows > 100000:
            self._log(f"警告：数据量较大({total_rows}行)，使用inner join优化性能")
            join_type = 'inner'
        
        # 修复：保留用户选择的所有字段，而不仅仅是前10列
        left_selected = []
        right_selected = []
        
        if output_fields:
            for table_key, fields in output_fields.items():
                table_name = table_key
                if '/' in table_key or '\\' in table_key:
                    table_name = table_key.split('/')[-1].split('\\')[-1]
                    table_name = table_name.replace('.xlsx', '').replace('.xls', '')
                
                if table_name in left_table or left_table in table_name:
                    left_selected.extend(fields)
                if table_name in right_table or right_table in table_name:
                    right_selected.extend(fields)
        
        # 左表：关联字段 + 用户选择的字段
        left_cols_to_keep = [left_field] + left_selected
        left_cols_to_keep = list(dict.fromkeys(left_cols_to_keep))
        left_cols_to_keep = [c for c in left_cols_to_keep if c in df_left.columns]
        if len(left_cols_to_keep) <= 1:  # 没选字段时用默认
            left_cols_to_keep = [left_field] + list(df_left.columns[:10])
            left_cols_to_keep = [c for c in left_cols_to_keep if c in df_left.columns]
        df_left = df_left[left_cols_to_keep].copy()
        
        # 右表：关联字段 + 用户选择的字段
        right_cols_to_keep = [right_field] + right_selected
        right_cols_to_keep = list(dict.fromkeys(right_cols_to_keep))
        right_cols_to_keep = [c for c in right_cols_to_keep if c in df_right.columns]
        if len(right_cols_to_keep) <= 1:  # 没选字段时用默认
            right_cols_to_keep = [right_field] + list(df_right.columns[:10])
            right_cols_to_keep = [c for c in right_cols_to_keep if c in df_right.columns]
        df_right = df_right[right_cols_to_keep].copy()
        
        # 预处理：清洗关联字段（向量化操作，避免apply）
        df_left[left_field] = df_left[left_field].astype(str).str.strip().str.upper()
        df_right[right_field] = df_right[right_field].astype(str).str.strip().str.upper()
        
        # 处理空值
        df_left[left_field] = df_left[left_field].replace('NAN', '__NULL__').replace('NONE', '__NULL__').replace('', '__NULL__')
        df_right[right_field] = df_right[right_field].replace('NAN', '__NULL__').replace('NONE', '__NULL__').replace('', '__NULL__')
        
        # 创建关联键（直接赋值，不使用apply）
        df_left['_link_key'] = df_left[left_field]
        df_right['_link_key'] = df_right[right_field]
        
        # 为右表字段添加前缀（避免列名冲突）
        # 直接修改列名，不复制数据
        right_fields = [c for c in df_right.columns if c != '_link_key']
        rename_dict = {c: f"{right_table}.{c}" for c in right_fields}
        df_right = df_right.rename(columns=rename_dict)
        
        # 执行关联
        self._log(f"执行关联: left={left_table}, right={right_table}, join_type={join_type}")
        
        try:
            if join_type == 'left':
                result = pd.merge(
                    df_left, 
                    df_right, 
                    on='_link_key', 
                    how='left'
                )
            elif join_type == 'inner':
                result = pd.merge(
                    df_left, 
                    df_right, 
                    on='_link_key', 
                    how='inner'
                )
            elif join_type == 'outer':
                result = pd.merge(
                    df_left, 
                    df_right, 
                    on='_link_key', 
                    how='outer'
                )
            else:
                raise ValueError(f"不支持的连接类型: {join_type}")
            
            # 清理临时字段
            result = result.drop(columns=['_link_key'], errors='ignore')
            
            self._log(f"关联完成: {left_table} 和 {right_table} -> {len(result)} 行")
            
            return result
            
        except MemoryError:
            self._log(f"内存不足，尝试减少数据...")
            # 内存不足时，只保留关键列
            left_cols = [left_field] + [c for c in df_left.columns if 'id' in c.lower() or '编号' in c.lower()][:5]
            right_cols = [right_field] + [c for c in df_right.columns if 'id' in c.lower() or '编号' in c.lower()][:5]
            
            df_left_small = df_left[left_cols].copy()
            df_right_small = df_right[right_cols].copy()
            
            # 重新创建关联键
            df_left_small['_link_key'] = df_left_small[left_field].astype(str).str.upper()
            df_right_small['_link_key'] = df_right_small[right_field].astype(str).str.upper()
            
            result = pd.merge(df_left_small, df_right_small, on='_link_key', how='inner')
            result = result.drop(columns=['_link_key'], errors='ignore')
            
            self._log(f"简化关联完成: {len(result)} 行")
            return result
    
    def execute_chain(self, 
                      links: List[Dict[str, Any]], 
                      output_fields: Optional[Dict[str, List[str]]] = None) -> pd.DataFrame:
        """
        执行链式关联查询
        
        Args:
            links: 关联配置列表
                [{left_table, right_table, left_field, right_field, join_type}, ...]
            output_fields: 输出字段配置 {表名: [字段列表]}, None表示全部字段
            
        Returns:
            查询结果DataFrame
        """
        if not self.tables:
            raise ValueError("没有配置表")
        
        if len(self.tables) == 1:
            # 单表查询
            df = self.dataframes[self.tables[0].name]
            table_name = self.tables[0].name
            
            # 选择输出字段
            if output_fields and table_name in output_fields:
                fields = output_fields[table_name]
                available_fields = [c for c in fields if c in df.columns]
                if available_fields:
                    # 拼接表名前缀
                    output_cols = []
                    for f in available_fields:
                        if '.' in f:
                            output_cols.append(f)
                        else:
                            output_cols.append(f"{table_name}.{f}")
                    # 选择匹配的列
                    cols = [c for c in df.columns if any(f.endswith(c.split('.')[-1]) for f in output_cols)]
                    if cols:
                        df = df[cols]
            
            return df
        
        # 多表关联
        # 构建关联顺序
        link_order = []
        processed = set()
        
        # 首先处理第一个表
        first_table = self.tables[0].name
        self._log(f"第一个表: {first_table}, dataframes keys: {list(self.dataframes.keys())}")
        processed.add(first_table)
        link_order.append({
            'table': first_table,
            'source': 'initial'
        })
        
        # 按照links配置添加关联表
        for link in links:
            left = link['left_table']
            right = link['right_table']
            self._log(f"处理关联: left={left}, right={right}, processed={processed}")
            
            # 确定哪个是已处理的表
            if left in processed and right not in processed:
                self._log(f"  关联 {left} -> {right}")
                link_order.append({
                    'table': right,
                    'source': 'link',
                    'left_table': left,
                    'left_field': link['left_field'],
                    'right_field': link['right_field'],
                    'join_type': link.get('join_type', 'left')
                })
                processed.add(right)
            elif right in processed and left not in processed:
                self._log(f"  关联 {right} -> {left}")
                link_order.append({
                    'table': left,
                    'source': 'link',
                    'left_table': right,
                    'left_field': link['right_field'],
                    'right_field': link['left_field'],
                    'join_type': link.get('join_type', 'left')
                })
                processed.add(left)
            else:
                self._log(f"  警告: left={left} in processed={left in processed}, right={right} in processed={right in processed}")
        
        # 依次关联
        result = self.dataframes[link_order[0]['table']]
        
        for i in range(1, len(link_order)):
            link_info = link_order[i]
            
            if link_info['source'] == 'link':
                result = self.link_tables(
                    link_info['left_table'],
                    link_info['table'],
                    link_info['left_field'],
                    link_info['right_field'],
                    link_info['join_type'],
                    output_fields  # 传递用户选择的输出字段
                )
        
        # 选择输出字段
        if output_fields:
            self._log(f"开始选择输出字段, output_fields: {output_fields}")
            self._log(f"当前结果列: {list(result.columns)}")  # 打印所有列
            output_cols = []
            for table_key, fields in output_fields.items():
                # table_key可能是文件路径或表名，需要提取表名
                table_name = table_key
                if '/' in table_key or '\\' in table_key:
                    table_name = table_key.split('/')[-1].split('\\')[-1]
                    # 去掉扩展名
                    table_name = table_name.replace('.xlsx', '').replace('.xls', '')
                
                self._log(f"处理表: {table_name}, 字段: {fields}")
                
                for f in fields:
                    # 安全的匹配逻辑
                    matching = [c for c in result.columns 
                               if c == f 
                               or c == f'{table_name}.{f}'
                               or c.endswith(f'.{f}')
                               or ('.' in c and c.split('.')[-1] == f)]
                    self._log(f"  字段 '{f}' 匹配到: {matching}")
                    output_cols.extend(matching)
            
            if output_cols:
                # 去重并保持顺序
                seen = set()
                unique_cols = []
                for c in output_cols:
                    if c not in seen:
                        seen.add(c)
                        unique_cols.append(c)
                result = result[unique_cols]
        
        return result
    
    

    def check_memory_usage(self) -> Dict[str, Any]:
        """检查当前内存使用情况"""
        import psutil
        
        process = psutil.Process()
        memory_info = process.memory_info()
        
        memory_mb = memory_info.rss / 1024 / 1024
        
        # 获取所有dataframe的内存占用
        df_memory = {}
        for name, df in self.dataframes.items():
            if hasattr(df, 'index'):
                df_memory[name] = df.memory_usage(deep=True).sum() / 1024 / 1024
        
        return {
            'process_memory_mb': memory_mb,
            'dataframes_memory_mb': df_memory,
            'total_memory_mb': memory_mb + sum(df_memory.values())
        }

    def _log(self, message: str) -> None:
        """输出日志"""
        if self.debug:
            print(f"[Linker] {message}")


def create_linker(debug: bool = False) -> Linker:
    """
    工厂函数 - 创建Linker实例
    
    Args:
        debug: 是否调试模式
        
    Returns:
        Linker实例
    """
    return Linker(debug=debug)