"""
空值分析检查器
"""
import logging
import numpy as np
import pandas as pd
from . import BaseChecker

class NullChecker(BaseChecker):
    """空值分析检查器"""
    
    def __init__(self, config):
        """
        初始化空值检查器
        
        Args:
            config (Config): 配置对象
        """
        super().__init__(config)
        self.null_threshold = config.get('quality_checks.null_threshold')
    
    def check(self, series, column_info=None):
        """
        执行空值分析
        
        Args:
            series (Series): 列数据
            column_info (dict, optional): 列信息
            
        Returns:
            dict: 空值分析结果
        """
        if series.empty:
            return {
                'null_count': 0,
                'null_rate': 0,
                'issues': [],
                'above_threshold': False
            }
        
        # 计算基本空值统计
        total_count = len(series)
        null_count = series.isna().sum()
        null_rate = null_count / total_count if total_count > 0 else 0
        
        # 检查是否超过阈值
        above_threshold = null_rate > self.null_threshold
        
        # 识别问题
        issues = []
        if above_threshold:
            issues.append({
                'type': 'high_null_rate',
                'description': f'空值率 ({null_rate:.2%}) 超过阈值 ({self.null_threshold:.2%})'
            })
        
        # 如果这是一个不应该有空值的列（例如主键），但却有空值
        if column_info and column_info.get('is_primary', False) and null_count > 0:
            issues.append({
                'type': 'primary_key_nulls',
                'description': '主键列包含空值'
            })
        
        # 对于非空约束的列，检查是否有空值
        if column_info and column_info.get('is_nullable', '').lower() == 'no' and null_count > 0:
            issues.append({
                'type': 'not_null_constraint_violation',
                'description': '非空约束列包含空值'
            })
        
        # 分析空值模式
        null_pattern = self._analyze_null_pattern(series)
        
        return {
            'null_count': int(null_count),
            'null_rate': float(null_rate),
            'above_threshold': above_threshold,
            'issues': issues,
            'null_pattern': null_pattern
        }
    
    def _analyze_null_pattern(self, series):
        """
        分析空值模式
        
        Args:
            series (Series): 列数据
            
        Returns:
            dict: 空值模式分析
        """
        # 创建空值标志序列
        is_null = series.isna()
        
        if is_null.sum() == 0:
            return {
                'pattern_type': 'no_nulls',
                'description': '没有空值'
            }
        
        if is_null.all():
            return {
                'pattern_type': 'all_nulls',
                'description': '全部为空值'
            }
        
        # 检查是否存在连续空值块
        consecutive_nulls = self._find_consecutive_nulls(is_null)
        
        # 检查空值是否在特定位置集中
        position_concentration = self._check_position_concentration(is_null)
        
        # 返回模式结果
        if consecutive_nulls['max_consecutive'] > 10:
            return {
                'pattern_type': 'consecutive_blocks',
                'description': f'存在连续空值块，最大连续空值长度为{consecutive_nulls["max_consecutive"]}',
                'details': consecutive_nulls
            }
        elif position_concentration['is_concentrated']:
            return {
                'pattern_type': 'position_concentrated',
                'description': position_concentration['description'],
                'details': position_concentration
            }
        else:
            return {
                'pattern_type': 'random',
                'description': '空值呈随机分布'
            }
    
    def _find_consecutive_nulls(self, is_null):
        """
        查找连续空值
        
        Args:
            is_null (Series): 空值标志序列
            
        Returns:
            dict: 连续空值分析
        """
        # 计算连续空值块
        diff = np.diff(np.concatenate(([0], is_null.astype(int), [0])))
        starts = np.where(diff == 1)[0]
        ends = np.where(diff == -1)[0]
        lengths = ends - starts
        
        if len(lengths) == 0:
            return {
                'max_consecutive': 0,
                'avg_consecutive': 0,
                'block_count': 0
            }
        
        return {
            'max_consecutive': int(max(lengths)),
            'avg_consecutive': float(np.mean(lengths)),
            'block_count': len(lengths)
        }
    
    def _check_position_concentration(self, is_null):
        """
        检查空值在位置上是否集中
        
        Args:
            is_null (Series): 空值标志序列
            
        Returns:
            dict: 位置集中分析
        """
        # 将数据分为三部分：开始、中间和结束
        length = len(is_null)
        first_third = is_null[:length//3]
        middle_third = is_null[length//3:2*length//3]
        last_third = is_null[2*length//3:]
        
        # 计算各部分的空值率
        first_rate = first_third.mean()
        middle_rate = middle_third.mean()
        last_rate = last_third.mean()
        
        # 判断是否存在位置集中
        concentration_threshold = 0.7  # 如果一部分的空值占比超过70%，则视为集中
        
        if first_rate > concentration_threshold:
            return {
                'is_concentrated': True,
                'position': 'start',
                'description': '空值集中在数据开始部分',
                'rates': {
                    'start': float(first_rate),
                    'middle': float(middle_rate),
                    'end': float(last_rate)
                }
            }
        elif last_rate > concentration_threshold:
            return {
                'is_concentrated': True,
                'position': 'end',
                'description': '空值集中在数据结束部分',
                'rates': {
                    'start': float(first_rate),
                    'middle': float(middle_rate),
                    'end': float(last_rate)
                }
            }
        elif middle_rate > concentration_threshold:
            return {
                'is_concentrated': True,
                'position': 'middle',
                'description': '空值集中在数据中间部分',
                'rates': {
                    'start': float(first_rate),
                    'middle': float(middle_rate),
                    'end': float(last_rate)
                }
            }
        else:
            return {
                'is_concentrated': False,
                'description': '空值分布均匀',
                'rates': {
                    'start': float(first_rate),
                    'middle': float(middle_rate),
                    'end': float(last_rate)
                }
            }
    
    def check_table(self, table_name, df, columns_info=None):
        """
        对表执行空值分析
        
        Args:
            table_name (str): 表名
            df (DataFrame): 表数据样本
            columns_info (dict, optional): 列信息
            
        Returns:
            dict: 表空值分析结果
        """
        logging.info(f"对表 {table_name} 执行空值分析")
        
        # 对整个表的空值进行总体分析
        null_summary = df.isna().sum()
        total_cells = df.size
        null_cells = null_summary.sum()
        overall_null_rate = null_cells / total_cells if total_cells > 0 else 0
        
        columns_null_rates = {}
        columns_above_threshold = []
        columns_with_issues = []
        
        # 对每一列执行空值分析
        for column in df.columns:
            column_info = columns_info.get(column) if columns_info else None
            result = self.check(df[column], column_info)
            
            columns_null_rates[column] = result['null_rate']
            
            if result['above_threshold']:
                columns_above_threshold.append(column)
                
            if result['issues']:
                columns_with_issues.append({
                    'column': column,
                    'issues': result['issues']
                })
        
        table_result = {
            'overall_null_rate': float(overall_null_rate),
            'columns_null_rates': columns_null_rates,
            'columns_above_threshold': columns_above_threshold,
            'columns_with_issues': columns_with_issues,
            'null_pattern_correlated': self._check_correlated_nulls(df),
            'column_results': {column: self.check(df[column], columns_info.get(column) if columns_info else None)
                              for column in df.columns}
        }
        
        self.results[table_name] = table_result
        return table_result
    
    def _check_correlated_nulls(self, df):
        """
        检查表中的空值相关性
        
        Args:
            df (DataFrame): 表数据
            
        Returns:
            list: 相关性空值列对
        """
        # 如果列太少，不检查相关性
        if len(df.columns) < 2:
            return []
        
        # 创建空值指示矩阵
        null_matrix = df.isna()
        
        # 计算空值列对的相关性
        correlated_pairs = []
        
        for i, col1 in enumerate(df.columns[:-1]):
            for col2 in df.columns[i+1:]:
                # 如果两列都有空值
                if null_matrix[col1].sum() > 0 and null_matrix[col2].sum() > 0:
                    # 计算空值coincidence率
                    both_null = (null_matrix[col1] & null_matrix[col2]).sum()
                    either_null = (null_matrix[col1] | null_matrix[col2]).sum()
                    
                    if either_null > 0:
                        coincidence_rate = both_null / either_null
                        
                        # 如果coincidence率超过阈值，认为存在相关性
                        if coincidence_rate > 0.8:
                            correlated_pairs.append({
                                'columns': [col1, col2],
                                'coincidence_rate': float(coincidence_rate),
                                'description': f'列 {col1} 和 {col2} 的空值高度相关 ({coincidence_rate:.2%})'
                            })
        
        return correlated_pairs
