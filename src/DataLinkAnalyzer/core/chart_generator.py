"""
图表生成模块
"""

import os
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.io import to_html
from typing import List, Dict, Any, Optional


class ChartGenerator:
    """图表生成器"""
    
    CHART_TYPES = {
        'bar': '柱状图',
        'line': '折线图',
        'pie': '饼图',
        'scatter': '散点图',
        'histogram': '直方图'
    }
    
    def __init__(self, debug: bool = False):
        """
        初始化生成器
        
        Args:
            debug: 是否调试模式
        """
        self.debug = debug
    
    def generate_chart(self,
                       df: pd.DataFrame,
                       chart_type: str,
                       x_field: str,
                       y_field: Optional[str] = None,
                       title: str = '数据图表',
                       color_field: Optional[str] = None) -> str:
        """
        生成图表
        
        Args:
            df: 数据DataFrame
            chart_type: 图表类型
            x_field: X轴字段
            y_field: Y轴字段
            title: 图表标题
            color_field: 颜色字段
            
        Returns:
            HTML字符串
        """
        if chart_type not in self.CHART_TYPES:
            raise ValueError(f"不支持的图表类型: {chart_type}")
        
        fig = None
        
        try:
            if chart_type == 'bar':
                if y_field:
                    fig = px.bar(df, x=x_field, y=y_field, title=title, color=color_field)
                else:
                    # 计数柱状图
                    fig = px.bar(df, x=x_field, title=title, color=color_field)
                    
            elif chart_type == 'line':
                if y_field:
                    fig = px.line(df, x=x_field, y=y_field, title=title, color=color_field, markers=True)
                else:
                    fig = px.line(df, x=x_field, title=title, color=color_field, markers=True)
                    
            elif chart_type == 'pie':
                if y_field:
                    fig = px.pie(df, values=y_field, names=x_field, title=title)
                else:
                    # 自动计数
                    fig = px.pie(df, names=x_field, title=title)
                    
            elif chart_type == 'scatter':
                if y_field:
                    fig = px.scatter(df, x=x_field, y=y_field, title=title, color=color_field)
                else:
                    fig = px.scatter(df, x=x_field, title=title, color=color_field)
                    
            elif chart_type == 'histogram':
                fig = px.histogram(df, x=x_field, title=title, color=color_field)
            
            if fig is None:
                raise ValueError("图表生成失败")
            
            # 美化图表
            fig.update_layout(
                template='plotly_white',
                font=dict(family='Arial, sans-serif', size=12),
                title_font_size=18,
                legend=dict(
                    orientation='h',
                    yanchor='bottom',
                    y=1.02,
                    xanchor='right',
                    x=1
                )
            )
            
            # 生成HTML
            html = to_html(fig, full_html=True, include_plotlyjs='cdn')
            
            self._log(f"生成{self.CHART_TYPES[chart_type]}: {title}")
            
            return html
            
        except Exception as e:
            self._log(f"图表生成错误: {e}")
            raise
    
    def save_chart(self, html: str, output_path: str) -> str:
        """
        保存图表为HTML文件
        
        Args:
            html: HTML内容
            output_path: 输出路径
            
        Returns:
            输出路径
        """
        # 确保目录存在
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html)
        
        self._log(f"图表已保存: {output_path}")
        
        return output_path
    
    def get_available_fields(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        获取可用于图表的字段
        
        Args:
            df: DataFrame
            
        Returns:
            字段信息列表
        """
        fields = []
        
        for col in df.columns:
            dtype = str(df[col].dtype)
            
            # 判断字段类型
            if 'int' in dtype or 'float' in dtype:
                field_type = 'numeric'
            elif 'object' in dtype or 'string' in dtype:
                field_type = 'categorical'
            elif 'datetime' in dtype:
                field_type = 'datetime'
            else:
                field_type = 'other'
            
            # 样本值
            sample_values = df[col].dropna().head(5).tolist()
            
            fields.append({
                'name': col,
                'type': field_type,
                'dtype': dtype,
                'unique_count': df[col].nunique(),
                'sample': sample_values[:3]
            })
        
        return fields
    
    def suggest_chart_config(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        建议图表配置
        
        Args:
            df: DataFrame
            
        Returns:
            建议配置列表
        """
        fields = self.get_available_fields(df)
        
        suggestions = []
        
        # 获取分类字段和数值字段
        categorical = [f for f in fields if f['type'] == 'categorical']
        numeric = [f for f in fields if f['type'] == 'numeric']
        
        # 柱状图建议
        if categorical and numeric:
            for cat in categorical[:3]:
                for num in numeric[:3]:
                    suggestions.append({
                        'chart_type': 'bar',
                        'x_field': cat['name'],
                        'y_field': num['name'],
                        'title': f"{num['name']} 按 {cat['name']} 分布"
                    })
        
        # 饼图建议
        if categorical:
            for cat in categorical[:2]:
                suggestions.append({
                    'chart_type': 'pie',
                    'x_field': cat['name'],
                    'title': f"{cat['name']} 占比分布"
                })
        
        return suggestions[:5]  # 最多5个建议
    
    def _log(self, message: str) -> None:
        """输出日志"""
        if self.debug:
            print(f"[ChartGenerator] {message}")


def create_chart_generator(debug: bool = False) -> ChartGenerator:
    """
    工厂函数 - 创建ChartGenerator实例
    
    Args:
        debug: 是否调试模式
        
    Returns:
        ChartGenerator实例
    """
    return ChartGenerator(debug=debug)