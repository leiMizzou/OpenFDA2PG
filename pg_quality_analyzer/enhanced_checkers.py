# enhanced_checkers.py
import logging
import numpy as np
import pandas as pd
from scipy import stats
from quality_checker import DistributionChecker

class EnhancedDistributionChecker(DistributionChecker):
    """增强版分布检查器，可以处理复杂数据类型和数值稳定性问题"""
    
    def _determine_data_type(self, series):
        """
        确定数据类型 - 增强版，可以识别复杂数据类型
        """
        # 检查是否包含复杂数据类型（列表、字典等）
        try:
            sample = series.dropna().head(10)
            if any(isinstance(x, (list, tuple, dict)) for x in sample):
                return 'complex'
        except Exception as e:
            logging.warning(f"检查复杂类型时出错: {str(e)}")
        
        # 使用父类方法检查其他类型
        return super()._determine_data_type(series)
    
    def check(self, series, column_info=None):
        """执行数据分布分析 - 增强版"""
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
        
        # 安全地确定数据类型
        try:
            data_type = self._determine_data_type(non_null)
        except Exception as e:
            logging.warning(f"数据类型检测失败: {str(e)}")
            data_type = 'unknown'
        
        # 为复杂数据类型提供特殊处理
        if data_type == 'complex':
            # 获取样本值作为字符串以避免序列化问题
            samples = []
            for val in non_null.head(5):
                try:
                    samples.append(str(val))
                except:
                    samples.append("<无法表示的值>")
            
            return {
                'distribution_type': 'complex',
                'issues': [{
                    'type': 'complex_data_type',
                    'description': '数据包含复杂类型如列表或字典，无法进行标准分布分析',
                    'severity': 'info'
                }],
                'sample_values': samples
            }
        
        # 对于其他数据类型使用原始方法
        return super().check(series, column_info)
    
    def _analyze_numeric(self, series):
        """分析数值型数据 - 增强版，处理数值稳定性问题"""
        try:
            # 基本统计量计算
            stats_dict = {
                'count': int(len(series)),
                'min': float(series.min()) if not pd.isna(series.min()) else None,
                'max': float(series.max()) if not pd.isna(series.max()) else None,
                'mean': float(series.mean()) if not pd.isna(series.mean()) else None,
                'median': float(series.median()) if not pd.isna(series.median()) else None,
                'std': float(series.std()) if not pd.isna(series.std()) else None
            }
            
            # 安全计算偏度和峰度
            try:
                # 检查数据是否几乎恒定
                range_value = stats_dict['max'] - stats_dict['min'] if stats_dict['max'] is not None and stats_dict['min'] is not None else 0
                if range_value < 1e-10:  # 实际上是常数
                    stats_dict['skewness'] = 0.0
                    stats_dict['kurtosis'] = 0.0
                else:
                    with np.errstate(all='ignore'):  # 抑制numpy警告
                        stats_dict['skewness'] = float(stats.skew(series))
                        stats_dict['kurtosis'] = float(stats.kurtosis(series))
            except Exception as e:
                logging.warning(f"计算偏度和峰度失败: {str(e)}")
                stats_dict['skewness'] = 0.0
                stats_dict['kurtosis'] = 0.0
            
            # 百分位数计算
            percentiles = [0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99]
            for p in percentiles:
                try:
                    stats_dict[f'percentile_{int(p*100)}'] = float(series.quantile(p))
                except Exception as e:
                    logging.warning(f"计算百分位数失败: {str(e)}")
                    stats_dict[f'percentile_{int(p*100)}'] = None
            
            # 继续使用父类方法的剩余部分
            # (这里我们简化，实际使用时可能需要更多调整)
            
            # 继续执行原始方法的其余部分...
            unique_count = len(series.unique())
            unique_ratio = unique_count / len(series)
            
            # 异常值检测 - 添加安全保护
            try:
                q1 = series.quantile(0.25)
                q3 = series.quantile(0.75)
                iqr = q3 - q1
                
                if iqr > 0:  # 防止除以零或极小值
                    lower_bound = q1 - (self.outlier_threshold * iqr)
                    upper_bound = q3 + (self.outlier_threshold * iqr)
                    
                    outliers = series[(series < lower_bound) | (series > upper_bound)]
                    outlier_count = len(outliers)
                    outlier_percentage = outlier_count / len(series)
                else:
                    outliers = pd.Series([])
                    outlier_count = 0
                    outlier_percentage = 0
                    lower_bound = float('nan')
                    upper_bound = float('nan')
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
            
            if abs(stats_dict.get('skewness', 0)) > 1:  # 显著偏斜
                skew_direction = '右偏' if stats_dict['skewness'] > 0 else '左偏'
                issues.append({
                    'type': 'skewed_distribution',
                    'description': f'分布显著{skew_direction} (偏度={stats_dict["skewness"]:.2f})',
                    'severity': 'low'
                })
            
            # 计算直方图
            try:
                histogram = self._calculate_histogram(series)
            except Exception as e:
                logging.warning(f"计算直方图失败: {str(e)}")
                histogram = {'counts': [], 'bins': [], 'bin_labels': []}
            
            # 返回完整的分析结果
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
