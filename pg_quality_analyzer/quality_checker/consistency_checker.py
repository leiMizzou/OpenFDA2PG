"""
一致性检查器，负责检查数据一致性和参照完整性
"""
import logging
import pandas as pd
import numpy as np
from .base_checker import BaseChecker

class ConsistencyChecker(BaseChecker):
    """数据一致性检查器"""
    
    def __init__(self, config):
        """
        初始化一致性检查器
        
        Args:
            config (Config): 配置对象
        """
        super().__init__(config)
        self.correlation_threshold = config.get('quality_checks.correlation_threshold')
    
    def check(self, series, column_info=None):
        """
        执行单列一致性检查
        
        Args:
            series (Series): 列数据
            column_info (dict, optional): 列信息
            
        Returns:
            dict: 检查结果
        """
        # 单列一致性检查主要关注数据格式一致性
        if series.empty:
            return {
                'issues': [],
                'format_consistency': 1.0
            }
        
        # 检查数据格式一致性
        format_consistency = self._check_format_consistency(series)
        
        # 检查是否为惟一约束列（如主键）
        uniqueness_check = None
        if column_info and (column_info.get('is_primary', False) or 
                           column_info.get('is_unique', False)):
            uniqueness_check = self._check_uniqueness(series)
        
        # 检查取值范围一致性（对于结构良好的列）
        range_consistency = None
        if pd.api.types.is_numeric_dtype(series) or pd.api.types.is_datetime64_dtype(series):
            range_consistency = self._check_range_consistency(series)
        
        # 识别问题
        issues = []
        
        if format_consistency['consistency_score'] < 0.9:
            issues.append({
                'type': 'inconsistent_format',
                'description': f'数据格式不一致，一致性分数为 {format_consistency["consistency_score"]:.2f}',
                'details': format_consistency['format_groups']
            })
        
        if uniqueness_check and not uniqueness_check['is_unique']:
            issues.append({
                'type': 'uniqueness_violation',
                'description': f'唯一性约束违反，有 {uniqueness_check["duplicate_count"]} 个重复值',
                'details': uniqueness_check['examples']
            })
        
        if range_consistency and range_consistency['has_outliers']:
            issues.append({
                'type': 'range_inconsistency',
                'description': f'数据范围不一致，有 {range_consistency["outlier_count"]} 个离群值',
                'details': range_consistency
            })
        
        return {
            'issues': issues,
            'format_consistency': format_consistency,
            'uniqueness_check': uniqueness_check,
            'range_consistency': range_consistency
        }
    
    def _check_format_consistency(self, series):
        """
        检查数据格式一致性
        
        Args:
            series (Series): 列数据
            
        Returns:
            dict: 格式一致性检查结果
        """
        # 对于非字符串类型，认为格式是一致的
        if not pd.api.types.is_string_dtype(series) and not pd.api.types.is_object_dtype(series):
            return {
                'consistency_score': 1.0,
                'format_groups': {'consistent': len(series)}
            }
        
        try:
            # 将数据转换为字符串
            str_series = series.astype(str)
            
            # 识别不同的格式模式
            patterns = {}
            
            # 数字格式
            numeric_pattern = str_series.str.match(r'^-?\d+(\.\d+)?$')
            patterns['numeric'] = int(numeric_pattern.sum())
            
            # 日期格式 (简化版)
            date_pattern = str_series.str.match(r'^\d{4}-\d{2}-\d{2}')
            patterns['date'] = int(date_pattern.sum())
            
            # 电子邮件格式
            email_pattern = str_series.str.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
            patterns['email'] = int(email_pattern.sum())
            
            # URL格式
            url_pattern = str_series.str.match(r'^(https?|ftp)://')
            patterns['url'] = int(url_pattern.sum())
            
            # JSON格式
            json_pattern = str_series.str.match(r'^\s*[\{\[]')
            patterns['json'] = int(json_pattern.sum())
            
            # 纯字母格式
            alpha_pattern = str_series.str.match(r'^[a-zA-Z\s]+$')
            patterns['alpha'] = int(alpha_pattern.sum())
            
            # 字母数字混合格式
            alphanum_pattern = str_series.str.match(r'^[a-zA-Z0-9\s]+$')
            patterns['alphanum'] = int(alphanum_pattern.sum())
            
            # 计算已识别格式的总数
            identified = sum(patterns.values())
            
            # 计算一致性分数
            top_format = max(patterns.items(), key=lambda x: x[1])
            consistency_score = top_format[1] / len(series) if len(series) > 0 else 1.0
            
            # 整理格式分组
            format_groups = {k: v for k, v in patterns.items() if v > 0}
            format_groups['other'] = len(series) - identified
            
            return {
                'consistency_score': float(consistency_score),
                'dominant_format': top_format[0],
                'format_groups': format_groups
            }
        except Exception as e:
            logging.warning(f"检查格式一致性时出错: {str(e)}")
            return {
                'consistency_score': 0.0,
                'dominant_format': 'unknown',
                'format_groups': {'error': len(series)}
            }
    
    def _check_uniqueness(self, series):
        """
        检查列唯一性
        
        Args:
            series (Series): 列数据
            
        Returns:
            dict: 唯一性检查结果
        """
        try:
            # 计算每个值的出现次数
            value_counts = series.value_counts()
            
            # 找出重复值
            duplicates = value_counts[value_counts > 1]
            duplicate_count = duplicates.sum() - len(duplicates)
            
            # 收集重复值示例
            examples = {}
            for value, count in duplicates.head(5).items():
                examples[str(value)] = int(count)
            
            return {
                'is_unique': duplicate_count == 0,
                'duplicate_count': int(duplicate_count),
                'unique_count': int(len(value_counts)),
                'examples': examples
            }
        except Exception as e:
            logging.warning(f"检查唯一性时出错: {str(e)}")
            return {
                'is_unique': False,
                'duplicate_count': 0,
                'unique_count': 0,
                'examples': {'error': str(e)}
            }
    
    def _check_range_consistency(self, series):
        """
        检查数据范围一致性
        
        Args:
            series (Series): 列数据
            
        Returns:
            dict: 范围一致性检查结果
        """
        try:
            # 计算标准范围（使用四分位范围）
            q1 = series.quantile(0.25)
            q3 = series.quantile(0.75)
            iqr = q3 - q1
            
            # 避免IQR为零导致的问题
            if iqr <= 0:
                iqr = series.std() * 0.5 if series.std() > 0 else 1.0
            
            lower_bound = q1 - (1.5 * iqr)
            upper_bound = q3 + (1.5 * iqr)
            
            # 检测离群值（使用逻辑运算符而不是算术运算符）
            outliers_below = series < lower_bound
            outliers_above = series > upper_bound
            outliers_mask = outliers_below | outliers_above  # 使用逻辑或运算符
            outliers = series[outliers_mask]
            
            outlier_count = len(outliers)
            outlier_percentage = outlier_count / len(series) if len(series) > 0 else 0
            
            # 根据数据类型处理结果值
            if pd.api.types.is_datetime64_dtype(series):
                # 时间戳类型 - 转换为字符串表示
                return {
                    'has_outliers': outlier_count > 0,
                    'outlier_count': int(outlier_count),
                    'outlier_percentage': float(outlier_percentage),
                    'range': {
                        'min': str(series.min()),
                        'q1': str(q1),
                        'median': str(series.median()),
                        'q3': str(q3),
                        'max': str(series.max())
                    },
                    'examples': [str(x) for x in outliers.head(5).tolist()] if not outliers.empty else []
                }
            else:
                # 数值类型 - 使用float转换
                return {
                    'has_outliers': outlier_count > 0,
                    'outlier_count': int(outlier_count),
                    'outlier_percentage': float(outlier_percentage),
                    'range': {
                        'min': float(series.min()),
                        'q1': float(q1),
                        'median': float(series.median()),
                        'q3': float(q3),
                        'max': float(series.max())
                    },
                    'examples': outliers.head(5).tolist() if not outliers.empty else []
                }
        except Exception as e:
            logging.warning(f"检查范围一致性时出错: {str(e)}")
            return {
                'has_outliers': False,
                'outlier_count': 0,
                'outlier_percentage': 0.0,
                'range': {'error': str(e)},
                'examples': []
            }
    
    def check_table(self, table_name, df, columns_info=None, relationships=None):
        """
        对表执行一致性检查
        
        Args:
            table_name (str): 表名
            df (DataFrame): 表数据样本
            columns_info (dict, optional): 列信息
            relationships (list, optional): 表关系信息
            
        Returns:
            dict: 表一致性检查结果
        """
        logging.info(f"对表 {table_name} 执行一致性分析")
        
        column_results = {}
        correlation_findings = []
        semantic_groups = []
        
        # 检查每列的一致性
        for column in df.columns:
            column_info = columns_info.get(column) if columns_info else None
            try:
                result = self.check(df[column], column_info)
                column_results[column] = result
            except Exception as e:
                logging.error(f"检查列 {column} 一致性时失败: {str(e)}")
                column_results[column] = {'issues': [], 'error': str(e)}
        
        # 检查列之间的相关性（对于数值列）
        numeric_columns = df.select_dtypes(include=np.number).columns
        if len(numeric_columns) > 1:
            try:
                correlation_matrix = df[numeric_columns].corr()
                
                # 寻找高相关性的列对
                high_corr_pairs = []
                
                for i, col1 in enumerate(correlation_matrix.columns[:-1]):
                    for col2 in correlation_matrix.columns[i+1:]:
                        corr = correlation_matrix.loc[col1, col2]
                        if abs(corr) > self.correlation_threshold:
                            high_corr_pairs.append({
                                'columns': [col1, col2],
                                'correlation': float(corr),
                                'type': 'positive' if corr > 0 else 'negative'
                            })
                
                if high_corr_pairs:
                    correlation_findings = high_corr_pairs
            except Exception as e:
                logging.error(f"计算列相关性时失败: {str(e)}")
        
        # 尝试识别语义相关的列组（例如名称、描述等）
        if len(df.columns) > 1:
            try:
                semantic_groups = self._identify_semantic_groups(df)
            except Exception as e:
                logging.error(f"识别语义组时失败: {str(e)}")
        
        # 检查主键完整性（如果有主键信息）
        primary_key_check = None
        if columns_info:
            primary_keys = [col for col, info in columns_info.items() 
                           if info.get('is_primary', False)]
            
            if primary_keys:
                try:
                    primary_key_check = self._check_primary_key_integrity(df, primary_keys)
                except Exception as e:
                    logging.error(f"检查主键完整性时失败: {str(e)}")
                    primary_key_check = {'error': str(e)}
        
        # 检查外键完整性（需要关系信息和其他表数据）
        foreign_key_checks = []
        if relationships:
            # 外键检查需要在main方法中实现，因为需要访问多个表的数据
            pass
        
        # 汇总结果
        table_result = {
            'column_consistency': column_results,
            'correlation_findings': correlation_findings,
            'semantic_groups': semantic_groups,
            'primary_key_check': primary_key_check,
            'foreign_key_checks': foreign_key_checks
        }
        
        self.results[table_name] = table_result
        return table_result
    
    def _identify_semantic_groups(self, df):
        """
        识别语义相关的列组
        
        Args:
            df (DataFrame): 表数据
            
        Returns:
            list: 语义组列表
        """
        semantic_groups = []
        
        try:
            # 根据列名寻找常见的语义组模式
            name_columns = [col for col in df.columns if 'name' in col.lower()]
            if len(name_columns) > 1:
                semantic_groups.append({
                    'type': 'name_group',
                    'columns': name_columns,
                    'description': '名称相关列'
                })
            
            date_columns = [col for col in df.columns if any(term in col.lower() for term in ['date', 'time', 'day', 'month', 'year'])]
            if len(date_columns) > 1:
                semantic_groups.append({
                    'type': 'date_group',
                    'columns': date_columns,
                    'description': '日期时间相关列'
                })
            
            address_columns = [col for col in df.columns if any(term in col.lower() for term in ['address', 'city', 'state', 'zip', 'postal', 'country'])]
            if len(address_columns) > 1:
                semantic_groups.append({
                    'type': 'address_group',
                    'columns': address_columns,
                    'description': '地址相关列'
                })
            
            amount_columns = [col for col in df.columns if any(term in col.lower() for term in ['amount', 'price', 'cost', 'fee', 'total', 'sum'])]
            if len(amount_columns) > 1:
                semantic_groups.append({
                    'type': 'amount_group',
                    'columns': amount_columns,
                    'description': '金额相关列'
                })
        except Exception as e:
            logging.warning(f"识别语义组时出错: {str(e)}")
        
        return semantic_groups
    
    def _check_primary_key_integrity(self, df, primary_keys):
        """
        检查主键完整性
        
        Args:
            df (DataFrame): 表数据
            primary_keys (list): 主键列列表
            
        Returns:
            dict: 主键完整性检查结果
        """
        try:
            # 检查主键列中是否有空值
            nulls_in_pk = df[primary_keys].isna().any(axis=1).sum()
            
            # 检查主键是否唯一
            if len(primary_keys) == 1:
                # 单列主键
                unique_count = len(df[primary_keys[0]].unique())
                is_unique = unique_count == len(df)
                duplicate_count = len(df) - unique_count
            else:
                # 复合主键
                unique_count = len(df.drop_duplicates(subset=primary_keys))
                is_unique = unique_count == len(df)
                duplicate_count = len(df) - unique_count
            
            issues = []
            if not is_unique:
                issues.append({
                    'type': 'pk_not_unique', 
                    'description': f'主键不唯一，有{duplicate_count}个重复值'
                })
            
            if nulls_in_pk > 0:
                issues.append({
                    'type': 'pk_has_nulls', 
                    'description': f'主键包含{nulls_in_pk}个空值'
                })
            
            return {
                'primary_keys': primary_keys,
                'is_unique': is_unique,
                'has_nulls': nulls_in_pk > 0,
                'null_count': int(nulls_in_pk),
                'duplicate_count': int(duplicate_count),
                'issues': [issue for issue in issues if issue]
            }
        except Exception as e:
            logging.warning(f"检查主键完整性时出错: {str(e)}")
            return {
                'primary_keys': primary_keys,
                'is_unique': False,
                'has_nulls': False,
                'null_count': 0,
                'duplicate_count': 0,
                'issues': [{'type': 'pk_check_error', 'description': f'检查主键时出错: {str(e)}'}]
            }
    
    def check_relationships(self, tables_data, relationships, schema_info):
        """
        检查表间关系的一致性
        
        Args:
            tables_data (dict): 表名到DataFrame的映射
            relationships (list): 表关系信息
            schema_info (dict): Schema信息
            
        Returns:
            dict: 关系一致性检查结果
        """
        logging.info("检查表间关系一致性")
        
        relationship_results = {}
        
        for table_name, table_relationships in relationships.items():
            if table_name not in tables_data:
                continue
                
            table_result = []
            
            for rel in table_relationships:
                # 排除反向关系
                if rel.get('is_reverse'):
                    continue
                    
                from_table = rel['from_table']
                from_column = rel['from_column']
                to_table = rel['to_table']
                to_column = rel['to_column']
                
                # 如果没有目标表的数据，跳过
                if to_table not in tables_data:
                    continue
                    
                # 检查外键完整性
                try:
                    result = self._check_referential_integrity(
                        tables_data[from_table], from_column,
                        tables_data[to_table], to_column,
                        rel
                    )
                    
                    if result:
                        table_result.append(result)
                except Exception as e:
                    logging.error(f"检查关系 {from_table}.{from_column} -> {to_table}.{to_column} 失败: {str(e)}")
                    table_result.append({
                        'relationship': rel,
                        'is_valid': False,
                        'error': str(e),
                        'violation_count': 0,
                        'examples': []
                    })
            
            if table_result:
                relationship_results[table_name] = table_result
        
        return relationship_results
    
    def _check_referential_integrity(self, df_from, from_column, df_to, to_column, relationship_info):
        """
        检查外键参照完整性
        
        Args:
            df_from (DataFrame): 外键表数据
            from_column (str): 外键列
            df_to (DataFrame): 主键表数据
            to_column (str): 主键列
            relationship_info (dict): 关系信息
            
        Returns:
            dict: 参照完整性检查结果
        """
        # 如果列不存在于DataFrame中，返回错误
        if from_column not in df_from.columns or to_column not in df_to.columns:
            return {
                'relationship': relationship_info,
                'is_valid': False,
                'error': '列不存在',
                'violation_count': 0,
                'examples': []
            }
        
        try:
            # 获取目标表中唯一的主键值
            pk_values = set(df_to[to_column].dropna().unique())
            
            # 检查外键值是否全部存在于主键值中
            fk_values = df_from[from_column].dropna()
            
            # 使用列表推导式查找无效值
            invalid_values = [value for value in fk_values.unique() if value not in pk_values]
            
            # 使用布尔索引计算违反参照完整性的行数
            violation_mask = fk_values.isin(invalid_values)
            violation_count = violation_mask.sum()
            
            # 收集违反示例
            examples = invalid_values[:5]
            
            return {
                'relationship': {
                    'from_table': relationship_info['from_table'],
                    'from_column': from_column,
                    'to_table': relationship_info['to_table'],
                    'to_column': to_column
                },
                'is_valid': len(invalid_values) == 0,
                'violation_count': int(violation_count),
                'violation_percentage': float(violation_count / len(fk_values)) if len(fk_values) > 0 else 0,
                'examples': examples
            }
        except Exception as e:
            logging.warning(f"检查参照完整性时出错: {str(e)}")
            return {
                'relationship': relationship_info,
                'is_valid': False,
                'error': str(e),
                'violation_count': 0,
                'examples': []
            }
        
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
        
        try:
            # 创建空值指示矩阵
            null_matrix = df.isna()
            
            # 计算空值列对的相关性
            correlated_pairs = []
            
            for i, col1 in enumerate(df.columns[:-1]):
                for col2 in df.columns[i+1:]:
                    # 如果两列都有空值
                    null_count_col1 = null_matrix[col1].sum()
                    null_count_col2 = null_matrix[col2].sum()
                    
                    if null_count_col1 > 0 and null_count_col2 > 0:
                        # 计算空值coincidence率
                        both_null = (null_matrix[col1] & null_matrix[col2]).sum()
                        
                        # 使用逻辑或运算符而不是算术运算符
                        either_null_mask = null_matrix[col1] | null_matrix[col2]
                        either_null = either_null_mask.sum()
                        
                        if either_null > 0:
                            coincidence_rate = both_null / either_null
                            
                            # 如果coincidence率超过阈值，认为存在相关性
                            if coincidence_rate > 0.8:
                                correlated_pairs.append({
                                    'columns': [col1, col2],
                                    'coincidence_rate': float(coincidence_rate),
                                    'description': f'列 {col1} 和 {col2} 的空值高度相关 ({coincidence_rate:.2%})'
                                })
        except Exception as e:
            logging.warning(f"检查空值相关性时出错: {str(e)}")
        
        return correlated_pairs