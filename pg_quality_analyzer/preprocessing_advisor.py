"""
预处理策略建议模块，为不同类型的数据提供预处理建议
"""
import logging
import pandas as pd
import json
import re

class PreprocessingAdvisor:
    """预处理策略推荐器，为数据提供处理建议"""
    
    def __init__(self, config, gemini_integrator=None):
        """
        初始化预处理策略推荐器
        
        Args:
            config (Config): 配置对象
            gemini_integrator (GeminiIntegrator, optional): Gemini集成器
        """
        self.config = config
        self.gemini = gemini_integrator
    
    def recommend_strategies(self, column_info, analysis_results, data_samples=None):
        """
        基于分析结果推荐预处理策略
        
        Args:
            column_info (dict): 列信息
            analysis_results (dict): 分析结果
            data_samples (list, optional): 数据样本
            
        Returns:
            dict: 预处理策略建议
        """
        # 确定数据类型
        data_type = self._determine_data_type(column_info, analysis_results)
        
        # 根据数据类型生成不同的预处理建议
        if data_type == 'json':
            strategies = self._recommend_json_strategies(column_info, analysis_results)
        elif data_type == 'text':
            strategies = self._recommend_text_strategies(column_info, analysis_results)
        elif data_type == 'numeric':
            strategies = self._recommend_numeric_strategies(column_info, analysis_results)
        elif data_type == 'categorical':
            strategies = self._recommend_categorical_strategies(column_info, analysis_results)
        elif data_type == 'datetime':
            strategies = self._recommend_datetime_strategies(column_info, analysis_results)
        elif data_type == 'mixed':
            strategies = self._recommend_mixed_strategies(column_info, analysis_results)
        else:
            strategies = self._recommend_general_strategies(column_info, analysis_results)
        
        # 如果启用了Gemini，获取AI定制建议
        ai_recommendations = None
        if self.gemini and self.config.get('gemini.unstructured_analysis'):
            try:
                ai_recommendations = self._get_ai_recommendations(column_info, analysis_results, data_samples, data_type)
                
                # 合并AI建议
                if ai_recommendations and 'strategies' in ai_recommendations:
                    # 添加AI策略，确保不重复
                    ai_strategies = ai_recommendations['strategies']
                    existing_strategy_names = {s['name'] for s in strategies}
                    
                    for ai_strategy in ai_strategies:
                        if ai_strategy['name'] not in existing_strategy_names:
                            strategies.append(ai_strategy)
            except Exception as e:
                logging.error(f"获取AI预处理建议失败: {str(e)}")
        
        return {
            'data_type': data_type,
            'strategies': strategies,
            'ai_recommendations': ai_recommendations
        }
    
    def _determine_data_type(self, column_info, analysis_results):
        """
        确定数据类型
        
        Args:
            column_info (dict): 列信息
            analysis_results (dict): 分析结果
            
        Returns:
            str: 数据类型
        """
        # 检查列信息中的数据类型
        db_type = column_info.get('data_type', '').lower() if column_info else ''
        
        # 检查非结构化分析结果
        unstructured_type = None
        if 'unstructured' in analysis_results:
            unstructured_type = analysis_results['unstructured'].get('data_type')
        
        # 检查分布分析结果
        distribution_type = None
        if 'distribution' in analysis_results:
            distribution_type = analysis_results['distribution'].get('distribution_type')
        
        # 确定综合数据类型
        if unstructured_type == 'json' or (db_type and 'json' in db_type):
            return 'json'
        elif unstructured_type == 'text' or (db_type and db_type in ('text', 'character varying', 'varchar')):
            return 'text'
        elif distribution_type == 'categorical' or (db_type and db_type in ('enum', 'boolean')):
            return 'categorical'
        elif distribution_type in ('numeric', 'integer', 'normal', 'right_skewed', 'left_skewed') or \
             (db_type and db_type in ('integer', 'bigint', 'smallint', 'numeric', 'decimal', 'real', 'double precision')):
            return 'numeric'
        elif distribution_type == 'datetime' or (db_type and db_type in ('date', 'timestamp', 'time')):
            return 'datetime'
        elif unstructured_type or distribution_type == 'text':
            return 'text'
        elif column_info and column_info.get('is_structured') is False:
            return 'mixed'
        
        # 默认为一般类型
        return 'general'
    
    def _recommend_json_strategies(self, column_info, analysis_results):
        """
        为JSON数据推荐预处理策略
        
        Args:
            column_info (dict): 列信息
            analysis_results (dict): 分析结果
            
        Returns:
            list: 策略列表
        """
        strategies = []
        
        # 基本JSON解析策略
        strategies.append({
            'name': 'json_parsing',
            'description': '将JSON字符串解析为结构化数据',
            'priority': 'high',
            'code': '''
# 解析JSON字符串为Python对象
import json

def parse_json(value):
    if pd.isna(value):
        return None
    try:
        return json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return None

df['{column}_parsed'] = df['{column}'].apply(parse_json)
'''
        })
        
        # 检查分析结果中的JSON结构
        json_schema = None
        if 'unstructured' in analysis_results:
            unstructured = analysis_results['unstructured']
            if unstructured.get('data_type') == 'json' and 'analysis' in unstructured:
                json_analysis = unstructured['analysis']
                
                # 检查JSON是否有共同字段
                if 'field_consistency' in json_analysis:
                    field_consistency = json_analysis['field_consistency']
                    common_fields = field_consistency.get('common_fields', [])
                    
                    if common_fields:
                        # 提取共同字段策略
                        field_extract_code = '\n'.join([
                            f"# 提取字段: {field}",
                            f"df['{column}_{field}'] = df['{column}_parsed'].apply(lambda x: x.get('{field}') if isinstance(x, dict) else None)"
                        ] for field in common_fields[:5])
                        
                        strategies.append({
                            'name': 'extract_common_fields',
                            'description': f'从JSON中提取常见字段 ({", ".join(common_fields[:5])})',
                            'priority': 'medium',
                            'code': f'''
# 提取JSON中的常见字段
{field_extract_code}
'''
                        })
                
                # 检查是否有嵌套层级深的问题
                if 'depth_analysis' in json_analysis:
                    depth = json_analysis['depth_analysis'].get('avg_depth', 0)
                    if depth > 3:
                        strategies.append({
                            'name': 'flatten_json',
                            'description': f'将深度嵌套的JSON ({depth:.1f}层) 扁平化为单层结构',
                            'priority': 'medium',
                            'code': '''
# 将嵌套JSON扁平化
from pandas.io.json import json_normalize

def flatten_json(value):
    if pd.isna(value) or not isinstance(value, dict):
        return None
    try:
        return json_normalize(value).to_dict(orient='records')[0]
    except Exception:
        return None

flattened = df['{column}_parsed'].apply(flatten_json).dropna()
if not flattened.empty:
    flat_df = pd.DataFrame(flattened.tolist())
    # 重命名列以避免冲突
    flat_df = flat_df.add_prefix('{column}_flat_')
    # 将扁平化结果合并回原数据
    df = pd.concat([df, flat_df], axis=1)
'''
                        })
        
        # 添加JSON数组展开策略
        strategies.append({
            'name': 'explode_json_arrays',
            'description': '将JSON数组展开为多行',
            'priority': 'low',
            'code': '''
# 检查是否包含数组
def contains_array(value):
    if not isinstance(value, dict):
        return False
    return any(isinstance(v, list) and len(v) > 0 for v in value.values())

# 找出包含数组的列
array_columns = [col for col in df.columns if col.startswith('{column}_') and 
                df[col].apply(contains_array).any()]

# 对每个包含数组的列进行展开
for col in array_columns:
    array_field = None
    # 查找第一个数组字段
    sample = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
    if sample and isinstance(sample, dict):
        for key, value in sample.items():
            if isinstance(value, list) and len(value) > 0:
                array_field = key
                break
    
    if array_field:
        # 创建一个临时列用于展开
        df[f'{col}_{array_field}_exploded'] = df[col].apply(
            lambda x: x.get(array_field) if isinstance(x, dict) else None
        )
        # 展开数组
        exploded_df = df.explode(f'{col}_{array_field}_exploded')
'''
        })
        
        return strategies
    
    def _recommend_text_strategies(self, column_info, analysis_results):
        """
        为文本数据推荐预处理策略
        
        Args:
            column_info (dict): 列信息
            analysis_results (dict): 分析结果
            
        Returns:
            list: 策略列表
        """
        strategies = []
        
        # 文本清洗策略
        strategies.append({
            'name': 'text_cleaning',
            'description': '基本文本清洗 (去除多余空格、特殊字符等)',
            'priority': 'high',
            'code': '''
# 基本文本清洗
import re

def clean_text(text):
    if pd.isna(text):
        return text
    # 转为字符串
    text = str(text)
    # 去除多余空格
    text = re.sub(r'\\s+', ' ', text)
    # 去除前后空格
    text = text.strip()
    # 去除特殊字符
    text = re.sub(r'[^\\w\\s.,;:!?\'"-]', '', text)
    return text

df['{column}_cleaned'] = df['{column}'].apply(clean_text)
'''
        })
        
        # 检查文本长度统计
        text_length_stats = None
        if 'unstructured' in analysis_results:
            unstructured = analysis_results['unstructured']
            if 'analysis' in unstructured and 'length_stats' in unstructured['analysis']:
                text_length_stats = unstructured['analysis']['length_stats']
        
        # 如果文本较长，添加文本分段策略
        if text_length_stats and text_length_stats.get('avg', 0) > 200:
            strategies.append({
                'name': 'text_segmentation',
                'description': '将长文本分割为段落或句子',
                'priority': 'medium',
                'code': '''
# 文本分段
def segment_text(text):
    if pd.isna(text):
        return []
    # 转为字符串
    text = str(text)
    # 分割为段落
    paragraphs = [p.strip() for p in text.split('\\n\\n') if p.strip()]
    # 如果没有段落，则分割为句子
    if len(paragraphs) <= 1:
        import nltk
        try:
            return nltk.sent_tokenize(text)
        except Exception:
            # 简单句子分割
            return [s.strip() for s in re.split(r'[.!?]', text) if s.strip()]
    return paragraphs

df['{column}_segments'] = df['{column}'].apply(segment_text)
# 展开为多行
df_exploded = df.explode('{column}_segments')
'''
            })
        
        # 添加文本标准化策略
        strategies.append({
            'name': 'text_normalization',
            'description': '文本标准化 (小写转换、标点规范化等)',
            'priority': 'medium',
            'code': '''
# 文本标准化
def normalize_text(text):
    if pd.isna(text):
        return text
    # 转为字符串
    text = str(text)
    # 转为小写
    text = text.lower()
    # 标点规范化
    text = re.sub(r'[\\"\\\']', '"', text)
    # 多个标点替换为单个
    text = re.sub(r'([.,!?;:])+', r'\\1', text)
    return text

df['{column}_normalized'] = df['{column}'].apply(normalize_text)
'''
        })
        
        # 添加文本特征提取策略
        strategies.append({
            'name': 'text_feature_extraction',
            'description': '提取文本特征 (字数、句子数、词频等)',
            'priority': 'medium',
            'code': '''
# 提取文本特征
import nltk

def extract_text_features(text):
    if pd.isna(text):
        return {'word_count': 0, 'sentence_count': 0, 'avg_word_length': 0}
    # 转为字符串
    text = str(text)
    # 分词
    words = nltk.word_tokenize(text)
    # 分句
    sentences = nltk.sent_tokenize(text)
    # 计算特征
    word_count = len(words)
    sentence_count = len(sentences)
    avg_word_length = sum(len(word) for word in words) / max(1, word_count)
    
    return {
        'word_count': word_count,
        'sentence_count': sentence_count,
        'avg_word_length': avg_word_length
    }

# 应用函数并扩展为多列
features = df['{column}'].apply(extract_text_features).apply(pd.Series)
df = pd.concat([df, features.add_prefix('{column}_')], axis=1)
'''
        })
        
        return strategies
    
    def _recommend_numeric_strategies(self, column_info, analysis_results):
        """
        为数值数据推荐预处理策略
        
        Args:
            column_info (dict): 列信息
            analysis_results (dict): 分析结果
            
        Returns:
            list: 策略列表
        """
        strategies = []
        
        # 检查分布分析结果
        distribution_info = None
        if 'distribution' in analysis_results:
            distribution_info = analysis_results['distribution']
        
        # 缺失值处理策略
        strategies.append({
            'name': 'missing_value_imputation',
            'description': '使用均值/中位数填充缺失的数值',
            'priority': 'high',
            'code': '''
# 缺失值填充
# 使用均值填充
df['{column}_mean_filled'] = df['{column}'].fillna(df['{column}'].mean())
# 使用中位数填充
df['{column}_median_filled'] = df['{column}'].fillna(df['{column}'].median())
'''
        })
        
        # 异常值处理策略
        if distribution_info and 'outliers' in distribution_info:
            outliers = distribution_info['outliers']
            if outliers.get('count', 0) > 0:
                strategies.append({
                    'name': 'outlier_treatment',
                    'description': f'处理异常值 (检测到{outliers.get("count", 0)}个异常值)',
                    'priority': 'high',
                    'code': '''
# 异常值处理
# 使用IQR方法识别异常值
Q1 = df['{column}'].quantile(0.25)
Q3 = df['{column}'].quantile(0.75)
IQR = Q3 - Q1
lower_bound = Q1 - 1.5 * IQR
upper_bound = Q3 + 1.5 * IQR

# 方法1: 截断异常值
df['{column}_clipped'] = df['{column}'].clip(lower_bound, upper_bound)

# 方法2: 删除异常值
df_no_outliers = df[(df['{column}'] >= lower_bound) & (df['{column}'] <= upper_bound)]

# 方法3: 将异常值替换为空值，然后用中位数填充
df['{column}_outliers_removed'] = df['{column}'].copy()
df.loc[(df['{column}'] < lower_bound) | (df['{column}'] > upper_bound), '{column}_outliers_removed'] = np.nan
df['{column}_outliers_removed'] = df['{column}_outliers_removed'].fillna(df['{column}'].median())
'''
                })
        
        # 数据标准化/归一化策略
        strategies.append({
            'name': 'normalization',
            'description': '标准化/归一化数值特征',
            'priority': 'medium',
            'code': '''
# 数据标准化/归一化
from sklearn.preprocessing import StandardScaler, MinMaxScaler

# Z-score标准化 (均值为0，标准差为1)
scaler = StandardScaler()
df['{column}_standardized'] = scaler.fit_transform(df[['{column}']]).flatten()

# Min-Max归一化 (缩放至0-1区间)
min_max_scaler = MinMaxScaler()
df['{column}_normalized'] = min_max_scaler.fit_transform(df[['{column}']]).flatten()
'''
        })
        
        # 分箱(离散化)策略
        strategies.append({
            'name': 'binning',
            'description': '将连续数值分割为离散区间',
            'priority': 'low',
            'code': '''
# 数值型特征分箱
# 等宽分箱
df['{column}_bins_equal_width'] = pd.cut(df['{column}'], bins=5, labels=False)

# 等频分箱
df['{column}_bins_equal_freq'] = pd.qcut(df['{column}'], q=5, labels=False, duplicates='drop')

# 自定义分箱边界
# 根据数据分布确定边界
# 示例: 使用百分位数作为边界
quantiles = [0, 0.2, 0.4, 0.6, 0.8, 1.0]
bins = [df['{column}'].quantile(q) for q in quantiles]
df['{column}_bins_custom'] = pd.cut(df['{column}'], bins=bins, labels=False, include_lowest=True)
'''
        })
        
        # 如果分布偏斜，添加转换策略
        if distribution_info and distribution_info.get('distribution_type') in ('right_skewed', 'left_skewed'):
            strategies.append({
                'name': 'skewed_distribution_transformation',
                'description': '对偏斜分布应用转换以接近正态分布',
                'priority': 'medium',
                'code': '''
# 偏斜分布转换
import numpy as np

# 对数转换 (适用于右偏分布)
# 注意：仅适用于正值
df['{column}_log'] = df['{column}'].apply(lambda x: np.log1p(x) if x > 0 else np.nan)

# 平方根转换 (适用于中等右偏)
# 注意：仅适用于非负值
df['{column}_sqrt'] = df['{column}'].apply(lambda x: np.sqrt(x) if x >= 0 else np.nan)

# Box-Cox变换 (需要scipy库)
from scipy import stats
# 确保所有值都为正
min_val = df['{column}'].min()
if min_val <= 0:
    shifted_values = df['{column}'] - min_val + 1
else:
    shifted_values = df['{column}']
    
# 应用Box-Cox变换
transformed_values, lambda_value = stats.boxcox(shifted_values.dropna())
# 创建一个与原始数据相同大小的结果数组
boxcox_result = np.empty(df['{column}'].shape)
boxcox_result.fill(np.nan)
# 将变换后的值填入结果数组
boxcox_result[~df['{column}'].isna()] = transformed_values
df['{column}_boxcox'] = boxcox_result
'''
            })
        
        return strategies
    
    def _recommend_categorical_strategies(self, column_info, analysis_results):
        """
        为分类数据推荐预处理策略
        
        Args:
            column_info (dict): 列信息
            analysis_results (dict): 分析结果
            
        Returns:
            list: 策略列表
        """
        strategies = []
        
        # 编码策略
        strategies.append({
            'name': 'categorical_encoding',
            'description': '对分类变量进行编码',
            'priority': 'high',
            'code': '''
# 分类变量编码
# One-Hot编码
one_hot = pd.get_dummies(df['{column}'], prefix='{column}')
df = pd.concat([df, one_hot], axis=1)

# 标签编码
from sklearn.preprocessing import LabelEncoder
le = LabelEncoder()
df['{column}_label'] = le.fit_transform(df['{column}'].astype(str))

# 目标编码 (适用于有监督学习，需要目标变量)
# 示例: 假设'target'是目标变量
# target_encoding = df.groupby('{column}')['target'].mean()
# df['{column}_target_encoded'] = df['{column}'].map(target_encoding)
'''
        })
        
        # 稀有类别合并策略
        strategies.append({
            'name': 'rare_category_handling',
            'description': '合并出现频率低的稀有类别',
            'priority': 'medium',
            'code': '''
# 合并稀有类别
# 计算每个类别的频率
value_counts = df['{column}'].value_counts()
# 识别频率低于阈值的类别
threshold = 0.01  # 1%
rare_categories = value_counts[value_counts / len(df) < threshold].index.tolist()

# 将稀有类别替换为'Other'
df['{column}_grouped'] = df['{column}'].copy()
df.loc[df['{column}'].isin(rare_categories), '{column}_grouped'] = 'Other'
'''
        })
        
        # 检查分布分析结果
        distribution_info = None
        if 'distribution' in analysis_results:
            distribution_info = analysis_results['distribution']
        
        # 如果存在类别不平衡问题，添加平衡策略
        if distribution_info and 'imbalance_ratio' in distribution_info:
            imbalance_ratio = distribution_info.get('imbalance_ratio', 1)
            if imbalance_ratio > 10:  # 严重不平衡
                strategies.append({
                    'name': 'class_imbalance_handling',
                    'description': f'处理类别不平衡问题 (不平衡比例: {imbalance_ratio:.1f})',
                    'priority': 'medium',
                    'code': '''
# 处理类别不平衡
# 方法1: 对低频类别过采样
from sklearn.utils import resample

# 获取每个类别的数据
categories = df['{column}'].unique()
max_size = df['{column}'].value_counts().max()

# 对每个小类别进行过采样
resampled_dfs = []
for category in categories:
    category_df = df[df['{column}'] == category]
    # 如果该类别的样本量小于最大类别的样本量
    if len(category_df) < max_size:
        # 过采样至与最大类别相同的样本量
        category_resampled = resample(
            category_df,
            replace=True,
            n_samples=max_size,
            random_state=42
        )
        resampled_dfs.append(category_resampled)
    else:
        resampled_dfs.append(category_df)

# 合并过采样后的数据
df_oversampled = pd.concat(resampled_dfs)

# 方法2: 对高频类别欠采样
# 找出最小类别的样本量
min_size = df['{column}'].value_counts().min()

# 对每个大类别进行欠采样
resampled_dfs = []
for category in categories:
    category_df = df[df['{column}'] == category]
    # 如果该类别的样本量大于最小类别的样本量
    if len(category_df) > min_size:
        # 欠采样至与最小类别相同的样本量
        category_resampled = resample(
            category_df,
            replace=False,
            n_samples=min_size,
            random_state=42
        )
        resampled_dfs.append(category_resampled)
    else:
        resampled_dfs.append(category_df)

# 合并欠采样后的数据
df_undersampled = pd.concat(resampled_dfs)
'''
                })
        
        # 缺失值处理策略
        strategies.append({
            'name': 'missing_categorical_handling',
            'description': '处理分类变量中的缺失值',
            'priority': 'high',
            'code': '''
# 处理分类变量中的缺失值
# 方法1: 用最频繁值填充
most_frequent = df['{column}'].mode()[0]
df['{column}_mode_filled'] = df['{column}'].fillna(most_frequent)

# 方法2: 将缺失值视为单独的类别
df['{column}_missing_as_category'] = df['{column}'].fillna('Missing')
'''
        })
        
        return strategies
    
    def _recommend_datetime_strategies(self, column_info, analysis_results):
        """
        为日期时间数据推荐预处理策略
        
        Args:
            column_info (dict): 列信息
            analysis_results (dict): 分析结果
            
        Returns:
            list: 策略列表
        """
        strategies = []
        
        # 日期时间解析策略
        strategies.append({
            'name': 'datetime_parsing',
            'description': '将字符串解析为标准日期时间格式',
            'priority': 'high',
            'code': '''
# 日期时间解析
import pandas as pd

# 如果列不是日期时间类型，则尝试转换
if not pd.api.types.is_datetime64_any_dtype(df['{column}']):
    # 尝试自动解析
    df['{column}_parsed'] = pd.to_datetime(df['{column}'], errors='coerce')
else:
    df['{column}_parsed'] = df['{column}']
'''
        })
        
        # 日期特征提取策略
        strategies.append({
            'name': 'datetime_feature_extraction',
            'description': '从日期时间中提取有用特征',
            'priority': 'high',
            'code': '''
# 提取日期时间特征
datetime_col = '{column}_parsed' if '{column}_parsed' in df.columns else '{column}'

# 确保是日期时间类型
if not pd.api.types.is_datetime64_any_dtype(df[datetime_col]):
    df[datetime_col] = pd.to_datetime(df[datetime_col], errors='coerce')

# 提取年、月、日
df['{column}_year'] = df[datetime_col].dt.year
df['{column}_month'] = df[datetime_col].dt.month
df['{column}_day'] = df[datetime_col].dt.day

# 提取周相关特征
df['{column}_weekday'] = df[datetime_col].dt.weekday  # 0=周一, 6=周日
df['{column}_is_weekend'] = df['{column}_weekday'].isin([5, 6]).astype(int)
df['{column}_week_of_year'] = df[datetime_col].dt.isocalendar().week

# 提取时间特征
if df[datetime_col].dt.hour.nunique() > 1:  # 检查是否包含时间信息
    df['{column}_hour'] = df[datetime_col].dt.hour
    df['{column}_minute'] = df[datetime_col].dt.minute
    # 一天中的时段
    df['{column}_time_of_day'] = pd.cut(
        df[datetime_col].dt.hour,
        bins=[0, 6, 12, 18, 24],
        labels=['Night', 'Morning', 'Afternoon', 'Evening'],
        right=False
    )

# 提取季节
df['{column}_quarter'] = df[datetime_col].dt.quarter
df['{column}_season'] = pd.cut(
    df[datetime_col].dt.month,
    bins=[0, 3, 6, 9, 12],
    labels=['Winter', 'Spring', 'Summer', 'Fall'],
    right=False
)

# 是否月初/月末
df['{column}_is_month_start'] = df[datetime_col].dt.is_month_start.astype(int)
df['{column}_is_month_end'] = df[datetime_col].dt.is_month_end.astype(int)

# 是否季初/季末
df['{column}_is_quarter_start'] = df[datetime_col].dt.is_quarter_start.astype(int)
df['{column}_is_quarter_end'] = df[datetime_col].dt.is_quarter_end.astype(int)

# 是否年初/年末
df['{column}_is_year_start'] = df[datetime_col].dt.is_year_start.astype(int)
df['{column}_is_year_end'] = df[datetime_col].dt.is_year_end.astype(int)
'''
        })
        
        # 时间差计算策略
        strategies.append({
            'name': 'datetime_elapsed_time',
            'description': '计算相对于参考日期的时间差',
            'priority': 'medium',
            'code': '''
# 计算时间差
import datetime

datetime_col = '{column}_parsed' if '{column}_parsed' in df.columns else '{column}'

# 确保是日期时间类型
if not pd.api.types.is_datetime64_any_dtype(df[datetime_col]):
    df[datetime_col] = pd.to_datetime(df[datetime_col], errors='coerce')

# 计算距今天数
today = pd.Timestamp.now().normalize()
df['{column}_days_from_today'] = (today - df[datetime_col]).dt.days

# 计算距离月初的天数
df['{column}_days_from_month_start'] = df[datetime_col].dt.day - 1

# 计算距离年初的天数
df['{column}_days_from_year_start'] = df[datetime_col].dt.dayofyear - 1

# 如果有另一个日期列，计算两者时间差
# 示例：假设有另一个日期列'other_date'
# df['{column}_to_other_days'] = (df['other_date'] - df[datetime_col]).dt.days
'''
        })
        
        # 处理缺失和异常日期
        strategies.append({
            'name': 'datetime_cleaning',
            'description': '清理日期数据，处理缺失值和异常值',
            'priority': 'medium',
            'code': '''
# 清理日期数据
datetime_col = '{column}_parsed' if '{column}_parsed' in df.columns else '{column}'

# 确保是日期时间类型
if not pd.api.types.is_datetime64_any_dtype(df[datetime_col]):
    df[datetime_col] = pd.to_datetime(df[datetime_col], errors='coerce')

# 检查未来日期
today = pd.Timestamp.now().normalize()
future_dates_mask = df[datetime_col] > today
if future_dates_mask.any():
    print(f"检测到 {future_dates_mask.sum()} 个未来日期")
    # 处理未来日期 (根据业务需求决定)
    # 选项1: 设置为今天
    # df.loc[future_dates_mask, datetime_col] = today
    # 选项2: 设置为空值
    # df.loc[future_dates_mask, datetime_col] = pd.NaT

# 检查过去太远的日期
ancient_date = pd.Timestamp('1900-01-01')
ancient_dates_mask = df[datetime_col] < ancient_date
if ancient_dates_mask.any():
    print(f"检测到 {ancient_dates_mask.sum()} 个早于1900年的日期")
    # 处理太早的日期 (根据业务需求决定)
    # 选项1: 设置为某个合理的早期日期
    # df.loc[ancient_dates_mask, datetime_col] = ancient_date
    # 选项2: 设置为空值
    # df.loc[ancient_dates_mask, datetime_col] = pd.NaT

# 填充缺失日期
# 选项1: 用中位数填充
median_date = df[datetime_col].median()
df[f'{datetime_col}_filled'] = df[datetime_col].fillna(median_date)
# 选项2: 用众数填充
# mode_date = df[datetime_col].mode()[0] if not df[datetime_col].mode().empty else median_date
# df[f'{datetime_col}_filled'] = df[datetime_col].fillna(mode_date)
'''
        })
        
        return strategies
    
    def _recommend_mixed_strategies(self, column_info, analysis_results):
        """
        为混合类型数据推荐预处理策略
        
        Args:
            column_info (dict): 列信息
            analysis_results (dict): 分析结果
            
        Returns:
            list: 策略列表
        """
        strategies = []
        
        # 数据类型分割策略
        strategies.append({
            'name': 'mixed_type_splitting',
            'description': '将混合类型数据分割为不同的类型列',
            'priority': 'high',
            'code': '''
# 分割混合类型数据
import re

# 识别不同类型的数据
def identify_data_type(value):
    if pd.isna(value):
        return 'missing'
    
    value_str = str(value)
    
    # 检查是否为数字
    if re.match(r'^-?\\d+(\\.\\d+)?$', value_str):
        return 'numeric'
    
    # 检查是否为日期格式
    date_patterns = [
        r'^\\d{4}-\\d{2}-\\d{2}$',  # yyyy-mm-dd
        r'^\\d{2}/\\d{2}/\\d{4}$',  # mm/dd/yyyy
        r'^\\d{2}-\\d{2}-\\d{4}$'   # dd-mm-yyyy
    ]
    if any(re.match(pattern, value_str) for pattern in date_patterns):
        return 'date'
    
    # 检查是否为JSON
    if (value_str.startswith('{') and value_str.endswith('}')) or \
       (value_str.startswith('[') and value_str.endswith(']')):
        try:
            json.loads(value_str)
            return 'json'
        except:
            pass
    
    # 默认为文本
    return 'text'

# 应用类型识别
df['{column}_type'] = df['{column}'].apply(identify_data_type)

# 分割为不同类型的列
for data_type in ['numeric', 'date', 'json', 'text']:
    # 创建mask
    mask = df['{column}_type'] == data_type
    if mask.any():
        # 创建特定类型的列
        df[f'{column}_{data_type}'] = df['{column}'].copy()
        # 将非此类型的值设为空
        df.loc[~mask, f'{column}_{data_type}'] = None
        
        # 对每种类型应用适当的转换
        if data_type == 'numeric':
            df[f'{column}_{data_type}'] = pd.to_numeric(df[f'{column}_{data_type}'], errors='coerce')
        elif data_type == 'date':
            df[f'{column}_{data_type}'] = pd.to_datetime(df[f'{column}_{data_type}'], errors='coerce')
'''
        })
        
        # 格式清洗和标准化策略
        strategies.append({
            'name': 'mixed_format_cleaning',
            'description': '清洗并标准化混合格式数据',
            'priority': 'high',
            'code': '''
# 清洗混合格式数据
import re

def clean_mixed_format(value):
    if pd.isna(value):
        return value
        
    value_str = str(value)
    
    # 去除多余空格
    value_str = re.sub(r'\\s+', ' ', value_str).strip()
    
    # 标准化引号
    value_str = re.sub(r'["""]', '"', value_str)
    
    # 标准化连字符和破折号
    value_str = re.sub(r'[–—]', '-', value_str)
    
    # 标准化空格字符
    value_str = re.sub(r'[\\xa0\\u2002-\\u200a]', ' ', value_str)
    
    return value_str

df['{column}_cleaned'] = df['{column}'].apply(clean_mixed_format)
'''
        })
        
        # 结构提取策略
        strategies.append({
            'name': 'structure_extraction',
            'description': '从半结构化数据中提取结构',
            'priority': 'medium',
            'code': '''
# 从半结构化数据中提取结构
import re
import json

def extract_structure(value):
    if pd.isna(value):
        return {}
        
    value_str = str(value)
    result = {}
    
    # 尝试提取键值对 (格式如 key=value 或 key:value)
    kv_pairs = re.findall(r'([\\w\\s]+)[=:](\\s*[^;,=:]+)', value_str)
    for key, val in kv_pairs:
        key = key.strip()
        val = val.strip()
        result[key] = val
    
    # 尝试提取JSON
    json_matches = re.search(r'(\\{.*\\})', value_str)
    if json_matches:
        try:
            json_data = json.loads(json_matches.group(1))
            # 将JSON键添加到结果中
            for key, val in json_data.items():
                result[key] = val
        except:
            pass
    
    # 尝试提取电子邮件
    email_match = re.search(r'([\\w.-]+@[\\w.-]+\\.[\\w]+)', value_str)
    if email_match:
        result['email'] = email_match.group(1)
    
    # 尝试提取电话号码
    phone_match = re.search(r'((?:\\+\\d{1,3}[\\s-]?)?\\(?\\d{3}\\)?[\\s.-]?\\d{3}[\\s.-]?\\d{4})', value_str)
    if phone_match:
        result['phone'] = phone_match.group(1)
    
    # 尝试提取日期
    date_match = re.search(r'(\\d{4}-\\d{2}-\\d{2}|\\d{2}/\\d{2}/\\d{4}|\\d{2}-\\d{2}-\\d{4})', value_str)
    if date_match:
        result['date'] = date_match.group(1)
    
    return result

# 应用提取函数并展开结果
extracted = df['{column}'].apply(extract_structure).apply(pd.Series)
if not extracted.empty:
    # 为提取的列添加前缀并合并回原始DataFrame
    extracted = extracted.add_prefix('{column}_')
    df = pd.concat([df, extracted], axis=1)
'''
        })
        
        return strategies
    
    def _recommend_general_strategies(self, column_info, analysis_results):
        """
        推荐通用预处理策略
        
        Args:
            column_info (dict): 列信息
            analysis_results (dict): 分析结果
            
        Returns:
            list: 策略列表
        """
        strategies = []
        
        # 缺失值处理策略
        strategies.append({
            'name': 'general_missing_value_handling',
            'description': '通用缺失值处理策略',
            'priority': 'high',
            'code': '''
# 通用缺失值处理
# 检查缺失值数量
missing_count = df['{column}'].isna().sum()
total_count = len(df['{column}'])
missing_percentage = missing_count / total_count if total_count > 0 else 0

print(f"列 '{column}' 有 {missing_count} 个缺失值 ({missing_percentage:.2%})")

# 处理策略取决于缺失值比例
if missing_percentage < 0.05:  # 少于5%缺失
    # 选项1: 删除缺失行
    df_clean = df.dropna(subset=['{column}'])
    
    # 选项2: 根据数据类型填充
    if pd.api.types.is_numeric_dtype(df['{column}']):
        # 数值型用均值填充
        df['{column}_filled'] = df['{column}'].fillna(df['{column}'].mean())
    elif pd.api.types.is_datetime64_dtype(df['{column}']):
        # 日期型用中位数填充
        df['{column}_filled'] = df['{column}'].fillna(df['{column}'].median())
    else:
        # 字符串/分类型用众数填充
        most_common = df['{column}'].mode()[0] if not df['{column}'].mode().empty else "Unknown"
        df['{column}_filled'] = df['{column}'].fillna(most_common)
        
elif missing_percentage < 0.3:  # 5%-30%缺失
    # 选项1: 创建指示缺失的标志列
    df['{column}_is_missing'] = df['{column}'].isna().astype(int)
    
    # 选项2: 使用更高级的填充方法
    if pd.api.types.is_numeric_dtype(df['{column}']):
        # 针对数值型，创建预测模型填充缺失值
        # 这里只是示例，实际应用中需要更复杂的模型
        from sklearn.impute import KNNImputer
        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
        if len(numeric_cols) > 1:  # 需要至少有其他一个数值列
            imputer = KNNImputer(n_neighbors=5)
            df[numeric_cols] = imputer.fit_transform(df[numeric_cols])
    else:
        # 非数值型，用 'Missing' 显式标记
        df['{column}_filled'] = df['{column}'].fillna('Missing')
        
else:  # 超过30%缺失
    # 当缺失太多时，考虑删除此列或将缺失标记为一个特殊类别
    print(f"警告: 列 '{column}' 缺失值比例很高 ({missing_percentage:.2%})，可能需要考虑删除")
    # 选项1: 删除列
    # df = df.drop(columns=['{column}'])
    
    # 选项2: 标记为特殊类别
    if not pd.api.types.is_numeric_dtype(df['{column}']):
        df['{column}_filled'] = df['{column}'].fillna('High_Missing_Rate')
    else:
        # 对于数值型，可以用一个特殊值（如-999）标记
        df['{column}_filled'] = df['{column}'].fillna(-999)
'''
        })
        
        # 数据类型转换策略
        strategies.append({
            'name': 'data_type_conversion',
            'description': '将数据转换为最适合的类型',
            'priority': 'high',
            'code': '''
# 数据类型转换
# 检查当前类型
current_type = df['{column}'].dtype
print(f"列 '{column}' 当前类型: {current_type}")

# 尝试转换为更适合的类型
# 数值转换
try:
    numeric_values = pd.to_numeric(df['{column}'], errors='coerce')
    # 检查转换后的非空值比例
    non_null_ratio = numeric_values.notna().mean()
    if non_null_ratio > 0.8:  # 如果80%以上的值可以转为数值
        # 判断是否为整数
        if (numeric_values.dropna() % 1 == 0).all():
            df['{column}_int'] = numeric_values.astype('Int64')  # 使用pandas的可空整数类型
            print(f"列 '{column}' 转换为整数类型")
        else:
            df['{column}_float'] = numeric_values.astype('float')
            print(f"列 '{column}' 转换为浮点数类型")
except Exception as e:
    print(f"数值转换失败: {str(e)}")

# 日期时间转换
try:
    datetime_values = pd.to_datetime(df['{column}'], errors='coerce')
    # 检查转换后的非空值比例
    non_null_ratio = datetime_values.notna().mean()
    if non_null_ratio > 0.8:  # 如果80%以上的值可以转为日期
        df['{column}_date'] = datetime_values
        print(f"列 '{column}' 转换为日期时间类型")
except Exception as e:
    print(f"日期转换失败: {str(e)}")

# 分类转换
if df['{column}'].nunique() < 0.2 * len(df):  # 如果唯一值少于20%
    df['{column}_cat'] = df['{column}'].astype('category')
    print(f"列 '{column}' 转换为分类类型")
'''
        })
        
        # 异常值检测策略
        strategies.append({
            'name': 'general_outlier_detection',
            'description': '通用异常值检测',
            'priority': 'medium',
            'code': '''
# 通用异常值检测
# 针对数值型列
if pd.api.types.is_numeric_dtype(df['{column}']):
    # Z-score方法
    from scipy import stats
    import numpy as np
    
    z_scores = stats.zscore(df['{column}'].dropna())
    abs_z_scores = np.abs(z_scores)
    z_outliers = abs_z_scores > 3  # 通常3个标准差之外视为异常
    
    # 计算异常值数量
    has_outliers = np.sum(z_outliers)
    if has_outliers > 0:
        print(f"基于Z-score检测到 {has_outliers} 个异常值")
        
        # 创建不包含异常值的副本
        df_no_outliers = df.copy()
        outlier_indices = np.where(abs_z_scores > 3)[0]
        df_no_outliers.iloc[outlier_indices, df.columns.get_loc('{column}')] = np.nan
    
    # IQR方法
    Q1 = df['{column}'].quantile(0.25)
    Q3 = df['{column}'].quantile(0.75)
    IQR = Q3 - Q1
    
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    
    iqr_outliers = ((df['{column}'] < lower_bound) | (df['{column}'] > upper_bound))
    iqr_outlier_count = iqr_outliers.sum()
    
    if iqr_outlier_count > 0:
        print(f"基于IQR检测到 {iqr_outlier_count} 个异常值")
        # 创建截断异常值的副本
        df['{column}_clipped'] = df['{column}'].clip(lower_bound, upper_bound)

# 针对分类型列
elif pd.api.types.is_object_dtype(df['{column}']) or pd.api.types.is_categorical_dtype(df['{column}']):
    # 对于分类，检查超低频类别
    value_counts = df['{column}'].value_counts()
    total_count = len(df)
    
    # 识别频率低于0.1%的类别
    rare_categories = value_counts[value_counts / total_count < 0.001].index.tolist()
    
    if rare_categories:
        print(f"检测到 {len(rare_categories)} 个罕见类别，可能是异常值")
        # 可选：将罕见类别替换为"其他"
        df['{column}_grouped'] = df['{column}'].copy()
        df.loc[df['{column}'].isin(rare_categories), '{column}_grouped'] = 'Other'
'''
        })
        
        return strategies
    
    def _get_ai_recommendations(self, column_info, analysis_results, data_samples, data_type):
        """
        使用Gemini API获取AI定制预处理建议
        
        Args:
            column_info (dict): 列信息
            analysis_results (dict): 分析结果
            data_samples (list): 数据样本
            data_type (str): 数据类型
            
        Returns:
            dict: AI预处理建议
        """
        if not self.gemini:
            return {"error": "Gemini API not available"}
        
        try:
            # 准备上下文信息
            column_name = column_info.get('column_name') if column_info else 'unknown'
            db_type = column_info.get('data_type') if column_info else 'unknown'
            
            # 提取分析结果摘要
            analysis_summary = {
                'data_type': data_type,
                'db_type': db_type,
                'column_name': column_name
            }
            
            # 添加特定类型的分析摘要
            if 'distribution' in analysis_results:
                dist = analysis_results['distribution']
                if 'basic_stats' in dist:
                    analysis_summary['stats'] = dist['basic_stats']
                analysis_summary['distribution_type'] = dist.get('distribution_type')
                
            if 'unstructured' in analysis_results:
                unstruct = analysis_results['unstructured']
                analysis_summary['unstructured_type'] = unstruct.get('data_type')
                if 'issues' in unstruct:
                    analysis_summary['issues'] = [issue['description'] for issue in unstruct.get('issues', [])]
            
            # 准备样本数据
            sample_data = []
            if data_samples:
                sample_data = data_samples[:5]  # 只使用前5个样本
            
            # 调用Gemini API
            result = self.gemini.recommend_preprocessing(column_name, data_type, analysis_summary, sample_data)
            return result
            
        except Exception as e:
            logging.error(f"获取AI预处理建议失败: {str(e)}")
            return {"error": str(e)}
