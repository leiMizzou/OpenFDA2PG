"""
数据类型检测模块，负责识别数据类型以及结构化/非结构化字段
"""
import logging
import re
import json
import pandas as pd
import numpy as np
from collections import Counter
import nltk
import os
import ssl

def ensure_nltk_resources():
    """确保所需的NLTK资源已下载"""
    # 要下载的资源列表
    resources = [
        ('tokenizers/punkt', 'punkt'),
        ('corpora/stopwords', 'stopwords')
    ]
    
    # 创建NLTK数据目录（如果不存在）
    nltk_data_dir = os.path.expanduser('~/nltk_data')
    os.makedirs(nltk_data_dir, exist_ok=True)
    
    for resource_path, resource_name in resources:
        try:
            # 首先尝试查找资源
            nltk.data.find(resource_path)
            logging.info(f"NLTK资源已存在: {resource_name}")
        except LookupError:
            # 如果资源不存在，尝试下载
            try:
                # 处理SSL证书问题（在某些环境中可能需要）
                try:
                    _create_unverified_https_context = ssl._create_unverified_context
                except AttributeError:
                    pass
                else:
                    ssl._create_default_https_context = _create_unverified_https_context
                
                # 下载资源到用户目录
                nltk.download(resource_name, quiet=True, download_dir=nltk_data_dir)
                logging.info(f"已成功下载NLTK资源: {resource_name}")
            except Exception as e:
                logging.warning(f"无法下载NLTK资源 {resource_name}: {str(e)}，将使用备用方法")

def safe_word_tokenize(text):
    """安全的分词函数，避免NLTK错误"""
    if not isinstance(text, str):
        return []
        
    try:
        return nltk.word_tokenize(text)
    except Exception as e:
        logging.debug(f"NLTK分词失败: {str(e)}，使用简单分词")
        # 简单分词：按空白字符分割
        return text.split()

def safe_sent_tokenize(text):
    """安全的分句函数，避免NLTK错误"""
    if not isinstance(text, str):
        return []
        
    try:
        return nltk.sent_tokenize(text)
    except Exception as e:
        logging.debug(f"NLTK分句失败: {str(e)}，使用简单分句")
        # 简单分句：按句号分割
        return [s.strip() + '.' for s in text.split('.') if s.strip()]

# 初始化时确保资源可用
ensure_nltk_resources()

class DataTypeDetector:
    """数据类型检测器，识别结构化和非结构化字段"""
    
    def __init__(self, config):
        """
        初始化数据类型检测器
        
        Args:
            config (Config): 配置对象
        """
        self.config = config
        
        # 用于检测JSON格式的正则表达式
        self.json_pattern = re.compile(r'^(\s*\{.*\}\s*|\s*\[.*\]\s*)$', re.DOTALL)
        
        # 用于检测XML/HTML格式的正则表达式
        self.xml_pattern = re.compile(r'^\s*<[^>]+>.*</[^>]+>\s*$', re.DOTALL)
        
        # 用于检测URL的正则表达式
        self.url_pattern = re.compile(
            r'^(https?|ftp):\/\/[^\s/$.?#].[^\s]*$'
        )
        
        # 用于检测电子邮件的正则表达式
        self.email_pattern = re.compile(
            r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        )
        
        # 结构化数据类型集合
        self.structured_types = {
            'integer', 'bigint', 'smallint', 'numeric', 'decimal', 'real', 'double precision',
            'boolean', 'date', 'time', 'timestamp', 'interval', 'uuid', 'inet'
        }
    
    def analyze_column(self, df, column_name, db_type=None, sample_size=1000):
        """
        分析列数据类型并判断是结构化还是非结构化
        
        Args:
            df (DataFrame): 包含列数据的DataFrame
            column_name (str): 列名
            db_type (str, optional): 数据库中的数据类型
            sample_size (int, optional): 分析的样本大小
            
        Returns:
            dict: 数据类型分析结果
        """
        # 如果列不存在于DataFrame中，返回空结果
        if column_name not in df.columns:
            return {
                'column_name': column_name,
                'is_structured': True,  # 默认为结构化
                'data_type': 'unknown',
                'detection_confidence': 0,
                'format_type': 'unknown',
                'contains_pattern': False,
                'avg_length': 0,
                'sample_values': []
            }
        
        # 获取列数据
        column_data = df[column_name]
        
        # 如果所有值都是NaN，返回默认结果
        if column_data.isna().all():
            return {
                'column_name': column_name,
                'is_structured': True,  # 默认为结构化
                'data_type': 'null',
                'detection_confidence': 1.0,
                'format_type': 'null',
                'contains_pattern': False,
                'avg_length': 0,
                'sample_values': []
            }
        
        # 获取非空值的样本
        non_null_data = column_data.dropna()
        if len(non_null_data) > sample_size:
            sample_data = non_null_data.sample(sample_size)
        else:
            sample_data = non_null_data
        
        # 检查数据库类型是否明确为结构化类型
        if db_type and db_type.lower() in self.structured_types:
            return {
                'column_name': column_name,
                'is_structured': True,
                'data_type': db_type.lower(),
                'detection_confidence': 1.0,
                'format_type': 'basic',
                'contains_pattern': False,
                'avg_length': column_data.astype(str).str.len().mean() if not column_data.empty else 0,
                'sample_values': sample_data.head(5).tolist()
            }
        
        # 基于pandas数据类型的初步判断
        pandas_type = str(column_data.dtype)
        is_numeric = pd.api.types.is_numeric_dtype(column_data)
        is_datetime = pd.api.types.is_datetime64_dtype(column_data)
        is_boolean = pd.api.types.is_bool_dtype(column_data)
        
        # 对于明确的基本类型，直接返回结构化
        if is_numeric or is_datetime or is_boolean:
            return {
                'column_name': column_name,
                'is_structured': True,
                'data_type': pandas_type,
                'detection_confidence': 1.0,
                'format_type': 'basic',
                'contains_pattern': False,
                'avg_length': column_data.astype(str).str.len().mean() if not column_data.empty else 0,
                'sample_values': sample_data.head(5).tolist()
            }
        
        # 对于字符串类型，进行深入分析
        result = self._analyze_string_column(sample_data)
        result['column_name'] = column_name
        result['sample_values'] = sample_data.head(5).tolist()
        result['avg_length'] = column_data.astype(str).str.len().mean() if not column_data.empty else 0
        
        return result
    
    def _analyze_string_column(self, sample_data):
        """
        分析字符串类型的列
        
        Args:
            sample_data (Series): 样本数据
            
        Returns:
            dict: 分析结果
        """
        # 将所有数据转换为字符串
        string_data = sample_data.astype(str)
        
        # 检查样本中的JSON格式占比
        json_count = sum(1 for value in string_data if self._is_json(value))
        json_ratio = json_count / len(string_data) if len(string_data) > 0 else 0
        
        # 检查样本中的XML/HTML格式占比
        xml_count = sum(1 for value in string_data if self._is_xml(value))
        xml_ratio = xml_count / len(string_data) if len(string_data) > 0 else 0
        
        # 检查样本中的URL占比
        url_count = sum(1 for value in string_data if self._is_url(value))
        url_ratio = url_count / len(string_data) if len(string_data) > 0 else 0
        
        # 检查样本中的电子邮件占比
        email_count = sum(1 for value in string_data if self._is_email(value))
        email_ratio = email_count / len(string_data) if len(string_data) > 0 else 0
        
        # 检查文本复杂性
        text_complexity = self._analyze_text_complexity(string_data)
        
        # 确定最可能的格式类型
        format_types = {
            'json': json_ratio,
            'xml/html': xml_ratio,
            'url': url_ratio,
            'email': email_ratio,
            'text': text_complexity['text_ratio']
        }
        
        most_likely_format = max(format_types.items(), key=lambda x: x[1])
        format_type = most_likely_format[0]
        confidence = most_likely_format[1]
        
        # 决定是结构化还是非结构化
        is_structured = True
        
        # 特定格式的数据（JSON, XML）被视为半结构化
        if format_type in ('json', 'xml/html') and confidence > 0.5:
            is_structured = False
        # 复杂文本被视为非结构化
        elif format_type == 'text' and text_complexity['complex_ratio'] > 0.5:
            is_structured = False
        # URL和Email被视为结构化
        elif format_type in ('url', 'email') and confidence > 0.5:
            is_structured = True
        # 其他情况，根据文本长度和词数判断
        elif text_complexity['avg_length'] > 100 or text_complexity['avg_words'] > 15:
            is_structured = False
        
        return {
            'is_structured': is_structured,
            'data_type': 'string',
            'detection_confidence': confidence,
            'format_type': format_type,
            'contains_pattern': json_ratio > 0.2 or xml_ratio > 0.2,
            'text_complexity': text_complexity
        }
    
    def _is_json(self, value):
        """检查字符串是否为JSON格式"""
        if not isinstance(value, str):
            return False
            
        if not self.json_pattern.match(value):
            return False
            
        try:
            json.loads(value)
            return True
        except:
            return False
    
    def _is_xml(self, value):
        """检查字符串是否为XML/HTML格式"""
        if not isinstance(value, str):
            return False
            
        return bool(self.xml_pattern.match(value))
    
    def _is_url(self, value):
        """检查字符串是否为URL"""
        if not isinstance(value, str):
            return False
            
        return bool(self.url_pattern.match(value))
    
    def _is_email(self, value):
        """检查字符串是否为电子邮件"""
        if not isinstance(value, str):
            return False
            
        return bool(self.email_pattern.match(value))
    
    def _analyze_text_complexity(self, string_data):
        """
        分析文本复杂性
        
        Args:
            string_data (Series): 字符串数据
            
        Returns:
            dict: 文本复杂性指标
        """
        # 计算文本长度
        lengths = string_data.str.len()
        avg_length = lengths.mean()
        max_length = lengths.max()
        
        # 计算单词数 - 使用安全分词函数
        word_counts = string_data.apply(lambda x: len(safe_word_tokenize(x)) if isinstance(x, str) else 0)
        
        avg_words = word_counts.mean()
        max_words = word_counts.max()
        
        # 检测句子数 - 使用安全分句函数
        sentence_counts = string_data.apply(lambda x: len(safe_sent_tokenize(x)) if isinstance(x, str) else 0)
        
        avg_sentences = sentence_counts.mean()
        
        # 判断是否存在复杂文本
        complex_count = sum(1 for count in word_counts if count > 10)
        complex_ratio = complex_count / len(string_data) if len(string_data) > 0 else 0
        
        # 判断是否主要为文本（超过一定字符的非结构化内容）
        text_count = sum(1 for length in lengths if length > 30)
        text_ratio = text_count / len(string_data) if len(string_data) > 0 else 0
        
        return {
            'avg_length': avg_length,
            'max_length': max_length,
            'avg_words': avg_words,
            'max_words': max_words,
            'avg_sentences': avg_sentences,
            'complex_ratio': complex_ratio,
            'text_ratio': text_ratio
        }
    
    def analyze_table_columns(self, df, db_column_types=None):
        """
        分析表中所有列的数据类型
        
        Args:
            df (DataFrame): 表数据
            db_column_types (dict, optional): 列名到数据库类型的映射
            
        Returns:
            dict: 列名到类型分析结果的映射
        """
        results = {}
        
        for column in df.columns:
            db_type = None
            if db_column_types and column in db_column_types:
                db_type = db_column_types[column]
            
            results[column] = self.analyze_column(df, column, db_type)
            
        return results