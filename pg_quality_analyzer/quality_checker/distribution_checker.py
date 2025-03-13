"""
数据分布分析检查器
"""
import logging
import numpy as np
import pandas as pd
from scipy import stats
from .base_checker import BaseChecker

class DistributionChecker(BaseChecker):
    """数据分布分析检查器"""
    
    def __init__(self, config):
        """
        初始化分布检查器
        
        Args:
            config (Config): 配置对象
        """
        super().__init__(config)
        self.outlier_threshold = config.get('quality_checks.outlier_threshold')
        self.cardinality_threshold = config.get('quality_checks.cardinality_threshold')
    
    def check(self, series, column_info=None):
        """
        执行数据分布分析
        
        Args:
            series (Series): 列数据
            column_info (dict, optional): 列信息
            
        Returns:
            dict: 分布分析结果
        """
        if series.empty:
            return {
                'distribution_type': 'empty',
                'issues': [],
                'outliers': {'count': 0, 'percentage': 0, 'values': []}
            }
        
        # 去除空值
        non_null = series.dropna()
        
        if non_null.empty:
            return {
                'distribution_type': 'all_null',
                'issues': [{
                    'type': 'all_null',
                    'description': '所有值都为空'
                }],
                'outliers': {'count': 0, 'percentage': 0, 'values': []}
            }
        
        # 确定数据类型
        data_type = self._determine_data_type(non_null)
        
        # 根据数据类型进行不同的分析
        if data_type == 'boolean':  # 新增布尔类型处理分支
            return self._analyze_boolean(non_null)
        elif data_type == 'numeric':
            return self._analyze_numeric(non_null)
        elif data_type == 'categorical':
            return self._analyze_categorical(non_null)
        elif data_type == 'datetime':
            return self._analyze_datetime(non_null)
        else:  # text or other
            return self._analyze_text(non_null)
    
    def _determine_data_type(self, series):
        """
        确定数据类型
        
        Args:
            series (Series): 列数据
            
        Returns:
            str: 数据类型
        """
        # 检查是否为布尔类型 - 新增的特殊检查
        if pd.api.types.is_bool_dtype(series):
            return 'boolean'
        
        # 检查是否为数值类型
        if pd.api.types.is_numeric_dtype(series):
            return 'numeric'
        
        # 检查是否为日期时间类型
        if pd.api.types.is_datetime64_dtype(series):
            return 'datetime'
        
        # 检查是否为分类数据
        # 对于字符串类型，如果唯一值占比低于阈值，视为分类数据
        if pd.api.types.is_string_dtype(series) or pd.api.types.is_object_dtype(series):
            unique_ratio = len(series.unique()) / len(series)
            if unique_ratio < 0.1:  # 如果唯一值比例小于10%，则视为分类数据
                return 'categorical'
        
        # 默认为文本
        return 'text'
    
    def _analyze_boolean(self, series):
        """
        分析布尔型数据
        
        Args:
            series (Series): 列数据
            
        Returns:
            dict: 分析结果
        """
        # 计算值分布
        true_count = series.sum()
        false_count = len(series) - true_count
        
        # 构建结果
        return {
            'distribution_type': 'boolean',
            'true_count': int(true_count),
            'false_count': int(false_count),
            'true_percentage': float(true_count / len(series)) if len(series) > 0 else 0,
            'issues': []
        }
    
    def _analyze_numeric(self, series):
        """
        分析数值型数据
        
        Args:
            series (Series): 列数据
            
        Returns:
            dict: 分析结果
        """
        try:
            # 基本统计量
            stats_dict = {
                'count': int(len(series)),
                'min': float(series.min()),
                'max': float(series.max()),
                'mean': float(series.mean()),
                'median': float(series.median()),
                'std': float(series.std()),
                'skewness': float(stats.skew(series)),
                'kurtosis': float(stats.kurtosis(series))
            }
            
            # 百分位数
            percentiles = [0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99]
            for p in percentiles:
                stats_dict[f'percentile_{int(p*100)}'] = float(series.quantile(p))
            
            # 唯一值分析
            unique_count = len(series.unique())
            unique_ratio = unique_count / len(series)
            
            # 异常值检测
            try:
                q1 = series.quantile(0.25)
                q3 = series.quantile(0.75)
                iqr = q3 - q1
                
                lower_bound = q1 - (self.outlier_threshold * iqr)
                upper_bound = q3 + (self.outlier_threshold * iqr)
                
                outliers = series[(series < lower_bound) | (series > upper_bound)]
                outlier_count = len(outliers)
                outlier_percentage = outlier_count / len(series)
            except Exception as e:
                logging.warning(f"检测异常值时出错: {str(e)}")
                outliers = pd.Series([])
                outlier_count = 0
                outlier_percentage = 0
                lower_bound = float('nan')
                upper_bound = float('nan')
            
            # 分布类型推断
            distribution_type = self._infer_distribution_type(series)
            
            # 识别问题
            issues = []
            
            if outlier_percentage > 0.01:  # 超过1%的异常值
                issues.append({
                    'type': 'high_outlier_percentage',
                    'description': f'异常值比例较高 ({outlier_percentage:.2%})',
                    'severity': 'medium' if outlier_percentage > 0.05 else 'low'
                })
            
            if abs(stats_dict['skewness']) > 1:  # 显著偏斜
                skew_direction = '右偏' if stats_dict['skewness'] > 0 else '左偏'
                issues.append({
                    'type': 'skewed_distribution',
                    'description': f'分布显著{skew_direction} (偏度={stats_dict["skewness"]:.2f})',
                    'severity': 'low'
                })
            
            if unique_ratio < 0.01 and unique_count > 1:  # 低基数但不是常量
                issues.append({
                    'type': 'low_cardinality',
                    'description': f'唯一值比例较低 ({unique_ratio:.2%}), 可能是离散编码数据',
                    'severity': 'info'
                })
            
            # 可能存在的量化（精度）问题
            if self._check_quantization(series):
                issues.append({
                    'type': 'quantization',
                    'description': '数据可能存在量化/精度问题',
                    'severity': 'low'
                })
            
            # 计算直方图
            histogram = self._calculate_histogram(series)
            
            return {
                'distribution_type': distribution_type,
                'basic_stats': stats_dict,
                'unique_count': unique_count,
                'unique_ratio': float(unique_ratio),
                'outliers': {
                    'count': int(outlier_count),
                    'percentage': float(outlier_percentage),
                    'lower_bound': float(lower_bound) if not pd.isna(lower_bound) else None,
                    'upper_bound': float(upper_bound) if not pd.isna(upper_bound) else None,
                    'values': outliers.head(10).tolist() if not outliers.empty else []
                },
                'histogram': histogram,
                'issues': issues
            }
        except Exception as e:
            logging.warning(f"数值分析失败: {str(e)}")
            return {
                'distribution_type': 'numeric',
                'issues': [{
                    'type': 'analysis_error',
                    'description': f'数值分析失败: {str(e)}',
                    'severity': 'medium'
                }],
                'outliers': {'count': 0, 'percentage': 0, 'values': []}
            }
    
    def _analyze_categorical(self, series):
        """
        分析分类数据
        
        Args:
            series (Series): 列数据
            
        Returns:
            dict: 分析结果
        """
        # 计算值分布
        value_counts = series.value_counts()
        
        # 唯一值分析
        unique_count = len(value_counts)
        unique_ratio = unique_count / len(series)
        
        # 计算分类分布的信息熵
        probabilities = value_counts / len(series)
        entropy = float(-(probabilities * np.log2(probabilities)).sum())
        
        # 最常见值和最不常见值
        most_common = value_counts.head(5).to_dict()
        least_common = value_counts.tail(5).to_dict()
        
        # 类别不平衡检测
        max_frequency = value_counts.max()
        min_frequency = value_counts.min()
        imbalance_ratio = max_frequency / min_frequency if min_frequency > 0 else float('inf')
        
        # 识别问题
        issues = []
        
        if unique_ratio > self.cardinality_threshold:
            issues.append({
                'type': 'high_cardinality',
                'description': f'唯一值比例很高 ({unique_ratio:.2%})，可能不是真正的分类变量',
                'severity': 'medium'
            })
        
        if imbalance_ratio > 100:  # 类别严重不平衡
            issues.append({
                'type': 'class_imbalance',
                'description': f'类别严重不平衡，最多类与最少类的比例为 {imbalance_ratio:.2f}',
                'severity': 'medium'
            })
        
        if unique_count == 1:
            issues.append({
                'type': 'constant_value',
                'description': '该列只有一个值',
                'severity': 'high'
            })
        
        return {
            'distribution_type': 'categorical',
            'unique_count': int(unique_count),
            'unique_ratio': float(unique_ratio),
            'entropy': entropy,
            'most_common': most_common,
            'least_common': least_common,
            'imbalance_ratio': float(imbalance_ratio),
            'value_counts': value_counts.head(20).to_dict(),
            'issues': issues
        }
    
    def _analyze_datetime(self, series):
        """
        分析日期时间数据
        
        Args:
            series (Series): 列数据
            
        Returns:
            dict: 分析结果
        """
        # 基本统计量
        min_date = series.min()
        max_date = series.max()
        range_days = (max_date - min_date).days
        
        # 时间戳分布
        if range_days > 0:
            timestamps_per_day = len(series) / range_days
        else:
            timestamps_per_day = len(series)
        
        # 按年月日聚合分析
        year_counts = series.dt.year.value_counts().to_dict()
        month_counts = series.dt.month.value_counts().to_dict()
        day_counts = series.dt.day.value_counts().to_dict()
        weekday_counts = series.dt.weekday.value_counts().to_dict()
        
        # 序列完整性和规律性检查
        time_diffs = series.sort_values().diff().dropna()
        
        if not time_diffs.empty:
            median_diff = time_diffs.median().total_seconds()
            mean_diff = time_diffs.mean().total_seconds()
            min_diff = time_diffs.min().total_seconds()
            max_diff = time_diffs.max().total_seconds()
            
            # 检查是否存在规律间隔
            is_regular = (max_diff / min_diff) < 2 if min_diff > 0 else False
            
            # 检查是否缺少时间点（针对规律间隔的时间序列）
            missing_points = []
            if is_regular and len(time_diffs) > 10:
                expected_interval = median_diff
                large_gaps = time_diffs[time_diffs.dt.total_seconds() > expected_interval * 1.5]
                missing_points = [
                    {
                        'before': str(series.sort_values().iloc[i]),
                        'after': str(series.sort_values().iloc[i+1]),
                        'expected_points': int((time_diffs.iloc[i].total_seconds() / expected_interval) - 1)
                    }
                    for i in large_gaps.index
                ]
        else:
            median_diff = 0
            mean_diff = 0
            min_diff = 0
            max_diff = 0
            is_regular = False
            missing_points = []
        
        # 识别问题
        issues = []
        
        if len(series.unique()) == 1:
            issues.append({
                'type': 'constant_date',
                'description': '该列只有一个日期值',
                'severity': 'high'
            })
        
        # 检查未来日期
        now = pd.Timestamp.now()
        future_dates = series[series > now]
        if not future_dates.empty:
            issues.append({
                'type': 'future_dates',
                'description': f'包含{len(future_dates)}个未来日期',
                'severity': 'medium' if len(future_dates) / len(series) > 0.01 else 'low'
            })
        
        # 检查非常早的日期（可能是错误）
        ancient_dates = series[series.dt.year < 1900]
        if not ancient_dates.empty:
            issues.append({
                'type': 'ancient_dates',
                'description': f'包含{len(ancient_dates)}个早于1900年的日期',
                'severity': 'medium'
            })
        
        # 检查工作日/周末分布（如果有足够数据）
        if len(weekday_counts) > 3:
            weekday_sum = sum(weekday_counts.get(d, 0) for d in range(0, 5))  # 0-4 是周一到周五
            weekend_sum = sum(weekday_counts.get(d, 0) for d in range(5, 7))  # 5-6 是周六日
            
            if weekend_sum == 0 and weekday_sum > 100:
                issues.append({
                    'type': 'no_weekend_dates',
                    'description': '数据只包含工作日日期，没有周末日期',
                    'severity': 'info'
                })
            elif weekday_sum == 0 and weekend_sum > 100:
                issues.append({
                    'type': 'no_weekday_dates',
                    'description': '数据只包含周末日期，没有工作日日期',
                    'severity': 'info'
                })
        
        # 如果是规律间隔但有明显缺失
        if is_regular and len(missing_points) > 0:
            issues.append({
                'type': 'missing_time_points',
                'description': f'时间序列中可能缺少了{sum(p["expected_points"] for p in missing_points)}个数据点',
                'severity': 'medium'
            })
        
        return {
            'distribution_type': 'datetime',
            'min_date': str(min_date),
            'max_date': str(max_date),
            'range_days': int(range_days),
            'unique_dates': int(len(series.unique())),
            'timestamps_per_day': float(timestamps_per_day),
            'year_distribution': year_counts,
            'month_distribution': month_counts,
            'day_distribution': day_counts,
            'weekday_distribution': weekday_counts,
            'time_intervals': {
                'median_seconds': float(median_diff),
                'mean_seconds': float(mean_diff),
                'min_seconds': float(min_diff),
                'max_seconds': float(max_diff),
                'is_regular': is_regular,
                'missing_points': missing_points[:10]  # 限制显示数量
            },
            'issues': issues
        }
    
    def _analyze_text(self, series):
        """
        分析文本数据
        
        Args:
            series (Series): 列数据
            
        Returns:
            dict: 分析结果
        """
        # 字符串长度分析
        lengths = series.astype(str).str.len()
        
        length_stats = {
            'min': int(lengths.min()),
            'max': int(lengths.max()),
            'mean': float(lengths.mean()),
            'median': float(lengths.median()),
            'std': float(lengths.std())
        }
        
        # 唯一值分析
        unique_count = len(series.unique())
        unique_ratio = unique_count / len(series)
        
        # 常见字符和模式分析
        starts_with = series.astype(str).str[:1].value_counts().head(5).to_dict()
        ends_with = series.astype(str).str[-1:].value_counts().head(5).to_dict()
        
        # 检查是否包含数字
        contains_digits = series.astype(str).str.contains(r'\d').mean()
        
        # 检查是否包含特殊字符
        contains_special = series.astype(str).str.contains(r'[^a-zA-Z0-9\s]').mean()
        
        # 检查是否全部大写或全部小写
        all_upper = series.astype(str).str.isupper().mean()
        all_lower = series.astype(str).str.islower().mean()
        
        # 识别问题
        issues = []
        
        if unique_count == 1:
            issues.append({
                'type': 'constant_text',
                'description': '该列只有一个文本值',
                'severity': 'high'
            })
        
        if unique_ratio > self.cardinality_threshold:
            # 高基数可能表示这是ID列或唯一标识符
            if lengths.std() < 1 and lengths.mean() > 8:
                issues.append({
                    'type': 'possible_id',
                    'description': '该列可能是ID或唯一标识符（高基数，固定长度）',
                    'severity': 'info'
                })
            else:
                issues.append({
                    'type': 'high_cardinality',
                    'description': f'唯一值比例很高 ({unique_ratio:.2%})',
                    'severity': 'info'
                })
        
        # 检查是否存在混合数据格式
        if 0.1 < contains_digits < 0.9:
            issues.append({
                'type': 'mixed_formats',
                'description': f'部分值包含数字 ({contains_digits:.2%})，数据格式可能不一致',
                'severity': 'low'
            })
        
        if 0.1 < contains_special < 0.9:
            issues.append({
                'type': 'mixed_special_chars',
                'description': f'部分值包含特殊字符 ({contains_special:.2%})，格式可能不一致',
                'severity': 'low'
            })
        
        # 检查长度变化大的情况
        if length_stats['std'] > length_stats['mean'] * 0.5 and length_stats['max'] > length_stats['min'] * 3:
            issues.append({
                'type': 'variable_length',
                'description': f'文本长度变化很大 (std={length_stats["std"]:.2f}, min={length_stats["min"]}, max={length_stats["max"]})',
                'severity': 'low'
            })
        
        return {
            'distribution_type': 'text',
            'length_stats': length_stats,
            'unique_count': int(unique_count),
            'unique_ratio': float(unique_ratio),
            'character_stats': {
                'starts_with': starts_with,
                'ends_with': ends_with,
                'contains_digits': float(contains_digits),
                'contains_special': float(contains_special),
                'all_upper': float(all_upper),
                'all_lower': float(all_lower)
            },
            'issues': issues
        }
    
    def _infer_distribution_type(self, series):
        """
        推断数值分布类型
        
        Args:
            series (Series): 列数据
            
        Returns:
            str: 分布类型
        """
        # 检查是否为常量
        if len(series.unique()) == 1:
            return 'constant'
        
        # 检查是否为二进制/布尔值
        if len(series.unique()) == 2:
            return 'binary'
        
        # 检查是否为整数分布
        is_integer = np.all(series.dropna() == series.dropna().astype(int))
        
        # 计算描述性统计
        try:
            skewness = stats.skew(series)
            kurtosis = stats.kurtosis(series)
            
            # 正态分布检验
            try:
                # 对于大数据集，只检验一个样本
                sample_size = min(1000, len(series))
                sample = series.sample(sample_size) if sample_size > 0 else series
                _, shapiro_p = stats.shapiro(sample)
            except:
                shapiro_p = 0  # 如果检验失败，假设不是正态分布
            
            # 根据统计特征推断分布类型
            if shapiro_p > 0.05:
                return 'normal'
            elif is_integer and series.min() >= 0:
                # 可能是计数或离散分布
                if abs(skewness) > 1 and kurtosis > 3:
                    return 'poisson_like'
                else:
                    return 'discrete'
            elif skewness > 1 and series.min() >= 0:
                # 右偏分布
                if kurtosis > 3:
                    return 'exponential_like'
                else:
                    return 'right_skewed'
            elif skewness < -1:
                # 左偏分布
                return 'left_skewed'
            elif abs(skewness) <= 1 and abs(kurtosis - 3) <= 1:
                # 接近正态但不完全符合
                return 'near_normal'
            elif kurtosis > 5:
                # 重尾分布
                return 'heavy_tailed'
            else:
                # 默认为其他类型
                return 'other'
        except Exception as e:
            logging.warning(f"推断分布类型失败: {str(e)}")
            return 'unknown'
    
    def _calculate_histogram(self, series):
        """
        计算直方图数据
        
        Args:
            series (Series): 列数据
            
        Returns:
            dict: 直方图数据
        """
        try:
            # 处理特殊类型
            if pd.api.types.is_datetime64_dtype(series):
                # 转换时间序列为数值
                numeric_series = series.astype(np.int64)
            else:
                numeric_series = pd.to_numeric(series, errors='coerce')
                
            # 移除NaN值
            numeric_series = numeric_series.dropna()
            
            if len(numeric_series) <= 1:
                return {
                    'counts': [],
                    'bins': [],
                    'bin_labels': []
                }
                
            # 根据数据范围和大小确定bin数量
            data_range = numeric_series.max() - numeric_series.min()
            n_bins = min(30, max(10, int(np.sqrt(len(numeric_series)))))
            
            # 计算直方图
            hist, bin_edges = np.histogram(numeric_series, bins=n_bins)
            
            # 对于时间类型，转换bin边界
            if pd.api.types.is_datetime64_dtype(series):
                bin_labels = [
                    f'{pd.Timestamp(bin_edges[i]):%Y-%m-%d %H:%M:%S} - {pd.Timestamp(bin_edges[i+1]):%Y-%m-%d %H:%M:%S}'
                    for i in range(len(bin_edges)-1)
                ]
            else:
                # 将bin_edges转换为区间标签
                bin_labels = [f'{bin_edges[i]:.2f} - {bin_edges[i+1]:.2f}' for i in range(len(bin_edges)-1)]
            
            return {
                'counts': hist.tolist(),
                'bins': bin_edges.tolist(),
                'bin_labels': bin_labels
            }
        except Exception as e:
            logging.warning(f"计算直方图失败: {str(e)}")
            return {
                'counts': [],
                'bins': [],
                'bin_labels': []
            }
    
    def _check_quantization(self, series):
        """
        检查数据是否存在量化/精度问题
        
        Args:
            series (Series): 列数据
            
        Returns:
            bool: 是否存在量化问题
        """
        # 如果数据是整数类型，不考虑量化问题
        if np.all(series.dropna() == series.dropna().astype(int)):
            return False
        
        # 计算小数部分
        decimal_parts = series.dropna() - series.dropna().astype(int)
        unique_decimals = decimal_parts.unique()
        
        # 如果小数部分的种类很少，可能存在量化
        if len(unique_decimals) < 10 and len(series) > 100:
            return True
            
        return False
        
    def _find_consecutive_nulls(self, is_null):
        """
        查找连续空值
        
        Args:
            is_null (Series): 空值标志序列
            
        Returns:
            dict: 连续空值分析
        """
        try:
            # 计算连续空值块
            # 注意：这里使用XOR而不是减法
            diff = np.diff(np.concatenate(([0], is_null.astype(int), [0])))
            starts = np.where(diff == 1)[0]
            ends = np.where(diff == -1)[0]
            
            # 使用数值运算而不是布尔运算
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
        except Exception as e:
            logging.warning(f"查找连续空值时出错: {str(e)}")
            return {
                'max_consecutive': 0,
                'avg_consecutive': 0,
                'block_count': 0
            }
    
    def check_table(self, table_name, df, columns_info=None):
        """
        对表执行数据分布分析
        
        Args:
            table_name (str): 表名
            df (DataFrame): 表数据样本
            columns_info (dict, optional): 列信息
            
        Returns:
            dict: 表分布分析结果
        """
        logging.info(f"对表 {table_name} 执行分布分析")
        
        column_results = {}
        issues = []
        
        # 检查表是否为空
        if df.empty:
            return {
                'column_results': {},
                'issues': [{
                    'type': 'empty_table',
                    'description': '表为空',
                    'severity': 'high'
                }]
            }
        
        # 对每列执行分布分析
        for column in df.columns:
            try:
                if columns_info and column in columns_info:
                    column_info = columns_info[column]
                else:
                    column_info = None
                
                # 执行分布分析
                result = self.check(df[column], column_info)
                
                # 添加列结果
                column_results[column] = result
                
                # 收集列级别的问题
                if 'issues' in result:
                    for issue in result['issues']:
                        issues.append({
                            'column': column,
                            'type': issue['type'],
                            'description': issue['description'],
                            'severity': issue.get('severity', 'medium')
                        })
            except Exception as e:
                logging.error(f"对表 {table_name} 列 {column} 执行分布分析失败: {str(e)}")
                issues.append({
                    'column': column,
                    'type': 'analysis_error',
                    'description': f'分析失败: {str(e)}',
                    'severity': 'medium'
                })
        
        # 构建表分布分析结果
        table_result = {
            'column_results': column_results,
            'issues': issues
        }
        
        self.results[table_name] = table_result
        return table_result