"""
非结构化数据检查器，负责检查文本、JSON等非结构化内容
"""
import logging
import json
import pandas as pd
import numpy as np
import re
from collections import Counter
import nltk
from .base_checker import BaseChecker

def ensure_nltk_resources():
    """确保所需的NLTK资源已下载"""
    resources = ['punkt']
    for resource in resources:
        try:
            nltk.data.find(f'tokenizers/{resource}')
            logging.info(f"NLTK资源已存在: {resource}")
        except LookupError:
            try:
                nltk.download(resource, quiet=True)
                logging.info(f"已下载NLTK资源: {resource}")
            except Exception as e:
                logging.warning(f"无法下载NLTK资源 {resource}: {str(e)}")

# 确保所需NLTK资源可用
ensure_nltk_resources()

class UnstructuredChecker(BaseChecker):
    """非结构化数据检查器"""
    
    def __init__(self, config, gemini_integrator=None):
        """
        初始化非结构化数据检查器
        
        Args:
            config (Config): 配置对象
            gemini_integrator (GeminiIntegrator, optional): Gemini集成器
        """
        super().__init__(config)
        self.gemini = gemini_integrator
    
    def check(self, series, column_info=None):
        """
        执行非结构化数据检查
        
        Args:
            series (Series): 列数据
            column_info (dict, optional): 列信息
            
        Returns:
            dict: 检查结果
        """
        if series.empty:
            return {
                'issues': [],
                'data_type': 'unknown',
                'analysis': {}
            }
        
        # 判断数据类型
        data_type = self._detect_data_type(series)
        
        # 根据数据类型执行不同的检查
        if data_type == 'json':
            analysis = self._analyze_json(series)
        elif data_type == 'xml':
            analysis = self._analyze_xml(series)
        elif data_type == 'text':
            analysis = self._analyze_text(series)
        else:
            analysis = self._analyze_other(series)
        
        # 识别问题
        issues = self._identify_issues(analysis, data_type)
        
        # 如果启用了Gemini API，调用AI增强分析
        if self.gemini and self.config.get('gemini.unstructured_analysis'):
            ai_analysis = self._get_ai_analysis(series, data_type, column_info)
            analysis['ai_insights'] = ai_analysis
        
        return {
            'data_type': data_type,
            'analysis': analysis,
            'issues': issues
        }
    
    def _detect_data_type(self, series):
        """
        检测非结构化数据类型
        
        Args:
            series (Series): 列数据
            
        Returns:
            str: 数据类型
        """
        # 转换为字符串并获取样本
        string_data = series.astype(str)
        sample = string_data.dropna().head(100)
        
        if sample.empty:
            return 'unknown'
        
        # 统计各种数据类型的计数
        json_count = sum(1 for value in sample if self._is_json(value))
        xml_count = sum(1 for value in sample if self._is_xml(value))
        
        # 判断主要数据类型
        if json_count > len(sample) * 0.7:
            return 'json'
        elif xml_count > len(sample) * 0.7:
            return 'xml'
        else:
            return 'text'
    
    def _is_json(self, value):
        """检查字符串是否为JSON格式"""
        if not isinstance(value, str):
            return False
            
        # 预判断：检查是否以'{' 或 '[' 开头
        value = value.strip()
        if not (value.startswith('{') or value.startswith('[')):
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
            
        # 预判断：检查是否包含XML标签
        value = value.strip()
        if not (value.startswith('<') and '>' in value):
            return False
            
        # 简单检查是否有闭合标签
        xml_pattern = re.compile(r'<([a-zA-Z][a-zA-Z0-9]*)[^>]*>.*?</\1>', re.DOTALL)
        return bool(xml_pattern.search(value))
    
    def _analyze_json(self, series):
        """
        分析JSON数据
        
        Args:
            series (Series): 列数据
            
        Returns:
            dict: 分析结果
        """
        # 获取有效的JSON样本
        valid_jsons = []
        invalid_count = 0
        
        for value in series.dropna().head(100):
            try:
                if isinstance(value, str):
                    json_obj = json.loads(value)
                    valid_jsons.append(json_obj)
                else:
                    invalid_count += 1
            except Exception:
                invalid_count += 1
        
        if not valid_jsons:
            return {
                'valid_ratio': 0,
                'structure': 'unknown',
                'fields': [],
                'depth': 0,
                'sample_size': len(series.dropna().head(100)),
                'invalid_count': invalid_count
            }
        
        # 分析JSON结构
        structure_type = 'object' if isinstance(valid_jsons[0], dict) else 'array'
        
        # 收集所有顶级字段
        fields = []
        if structure_type == 'object':
            field_counter = Counter()
            for json_obj in valid_jsons:
                if isinstance(json_obj, dict):
                    field_counter.update(json_obj.keys())
            
            # 计算每个字段的频率
            total = len(valid_jsons)
            fields = [{'name': field, 'frequency': count / total} 
                     for field, count in field_counter.most_common()]
        
        # 计算JSON深度
        max_depth = max(self._calculate_json_depth(json_obj) for json_obj in valid_jsons)
        
        # 提取schema结构
        sample_schema = self._extract_json_schema(valid_jsons[0]) if valid_jsons else {}
        
        return {
            'valid_ratio': len(valid_jsons) / (len(valid_jsons) + invalid_count) if (len(valid_jsons) + invalid_count) > 0 else 0,
            'structure': structure_type,
            'fields': fields,
            'depth': max_depth,
            'sample_size': len(series.dropna().head(100)),
            'invalid_count': invalid_count,
            'sample_schema': sample_schema
        }
    
    def _calculate_json_depth(self, obj, current_depth=0):
        """
        计算JSON对象的嵌套深度
        
        Args:
            obj: JSON对象
            current_depth (int): 当前深度
            
        Returns:
            int: 最大深度
        """
        if isinstance(obj, dict):
            if not obj:  # 空字典
                return current_depth
            return max(self._calculate_json_depth(v, current_depth + 1) for v in obj.values())
        elif isinstance(obj, list):
            if not obj:  # 空列表
                return current_depth
            return max(self._calculate_json_depth(item, current_depth + 1) for item in obj)
        else:
            return current_depth
    
    def _extract_json_schema(self, obj, path='$'):
        """
        提取JSON对象的schema结构
        
        Args:
            obj: JSON对象
            path (str): 当前路径
            
        Returns:
            dict: Schema结构
        """
        if isinstance(obj, dict):
            schema = {'type': 'object', 'properties': {}}
            for key, value in obj.items():
                schema['properties'][key] = self._extract_json_schema(value, f"{path}.{key}")
            return schema
        elif isinstance(obj, list):
            if not obj:  # 空列表
                return {'type': 'array', 'items': {}}
            # 取第一个元素作为代表
            return {'type': 'array', 'items': self._extract_json_schema(obj[0], f"{path}[0]")}
        elif isinstance(obj, str):
            return {'type': 'string', 'path': path}
        elif isinstance(obj, bool):
            return {'type': 'boolean', 'path': path}
        elif isinstance(obj, int):
            return {'type': 'integer', 'path': path}
        elif isinstance(obj, float):
            return {'type': 'number', 'path': path}
        elif obj is None:
            return {'type': 'null', 'path': path}
        else:
            return {'type': 'unknown', 'path': path}
    
    def _analyze_xml(self, series):
        """
        分析XML数据
        
        Args:
            series (Series): 列数据
            
        Returns:
            dict: 分析结果
        """
        # 简单分析XML结构
        samples = series.dropna().head(100).astype(str)
        
        if samples.empty:
            return {
                'tag_statistics': {},
                'valid_structure': 0
            }
        
        # 提取所有XML标签
        tag_pattern = re.compile(r'<([a-zA-Z][a-zA-Z0-9]*).*?>', re.DOTALL)
        all_tags = []
        
        for xml in samples:
            tags = tag_pattern.findall(xml)
            all_tags.extend(tags)
        
        # 标签统计
        tag_counter = Counter(all_tags)
        
        # 检查XML结构是否有效（开标签=闭标签）
        valid_structure_count = 0
        
        for xml in samples:
            open_tags = re.findall(r'<([a-zA-Z][a-zA-Z0-9]*)[^>]*>', xml)
            close_tags = re.findall(r'</([a-zA-Z][a-zA-Z0-9]*)[^>]*>', xml)
            
            # 简单检查：开标签和闭标签数量是否匹配
            if Counter(open_tags) == Counter(close_tags):
                valid_structure_count += 1
        
        return {
            'tag_statistics': {tag: count for tag, count in tag_counter.most_common(10)},
            'valid_structure_ratio': valid_structure_count / len(samples) if len(samples) > 0 else 0,
            'sample_size': len(samples)
        }
    
    def _analyze_text(self, series):
        """
        分析文本数据
        
        Args:
            series (Series): 列数据
            
        Returns:
            dict: 分析结果
        """
        # 获取文本样本 - 限制样本大小
        samples = series.dropna().head(100).astype(str)
        
        if samples.empty:
            return {
                'length_stats': {},
                'word_stats': {},
                'character_stats': {}
            }
        
        # 限制文本长度处理，避免OOM
        MAX_TEXT_LENGTH = 10000  # 设置单个文本最大处理长度
        truncated_samples = samples.apply(lambda x: x[:MAX_TEXT_LENGTH] if len(x) > MAX_TEXT_LENGTH else x)
        
        # 计算长度统计 - 使用原始样本长度
        lengths = samples.str.len()
        length_stats = {
            'min': int(lengths.min()),
            'max': int(lengths.max()),
            'mean': float(lengths.mean()),
            'median': float(lengths.median()),
            'std': float(lengths.std()) if len(lengths) > 1 else 0
        }
        
        # 分词并统计单词 - 使用截断的样本
        all_words = []
        for text in truncated_samples:
            try:
                # 限制单个文本的处理长度，进一步防止大文本导致内存问题
                words = nltk.word_tokenize(text)
                all_words.extend(words[:1000])  # 限制每个文本贡献的单词数
            except Exception as e:
                logging.warning(f"分词失败: {str(e)}")
                # 使用简单的分词方法作为备用
                words = text.split()
                all_words.extend(words[:1000])
        
        # 限制总单词数，防止内存爆炸
        MAX_WORDS = 50000
        if len(all_words) > MAX_WORDS:
            all_words = all_words[:MAX_WORDS]
            
        word_counter = Counter(all_words)
        
        # 计算句子统计 - 使用截断的样本
        sentence_counts = []
        for text in truncated_samples:
            try:
                sentences = nltk.sent_tokenize(text)
                sentence_counts.append(len(sentences))
            except Exception as e:
                logging.warning(f"分句失败: {str(e)}")
                # 使用简单的分句方法作为备用
                sentences = [s for s in text.split('.') if s.strip()]
                sentence_counts.append(len(sentences))
        
        sentence_stats = {
            'min': min(sentence_counts) if sentence_counts else 0,
            'max': max(sentence_counts) if sentence_counts else 0,
            'mean': sum(sentence_counts) / len(sentence_counts) if sentence_counts else 0
        }
        
        # 字符统计 - 限制处理的总字符数
        MAX_CHARS = 100000
        all_chars = ''.join(truncated_samples)[:MAX_CHARS]
        char_counter = Counter(all_chars)
        
        # 特殊字符检测
        special_chars = sum(1 for c in all_chars if not c.isalnum() and not c.isspace())
        special_ratio = special_chars / len(all_chars) if all_chars else 0
        
        return {
            'length_stats': length_stats,
            'sentence_stats': sentence_stats,
            'word_stats': {
                'unique_words': len(word_counter),
                'total_words': len(all_words),
                'top_words': {word: count for word, count in word_counter.most_common(10)}
            },
            'character_stats': {
                'total_chars': len(all_chars),
                'special_chars': special_chars,
                'special_ratio': special_ratio,
                'top_chars': {char: count for char, count in char_counter.most_common(10) if not char.isspace()}
            }
        }
    
    def _analyze_other(self, series):
        """
        分析其他类型的非结构化数据
        
        Args:
            series (Series): 列数据
            
        Returns:
            dict: 分析结果
        """
        # 获取样本
        samples = series.dropna().head(100).astype(str)
        
        if samples.empty:
            return {
                'length_stats': {},
                'pattern_consistency': 0
            }
        
        # 计算长度统计
        lengths = samples.str.len()
        length_stats = {
            'min': int(lengths.min()),
            'max': int(lengths.max()),
            'mean': float(lengths.mean()),
            'median': float(lengths.median()),
            'std': float(lengths.std())
        }
        
        # 检查是否存在一致的模式（例如，所有值都以相同的字符开始）
        starts_with = samples.str[:1].value_counts()
        ends_with = samples.str[-1:].value_counts()
        
        max_start_ratio = starts_with.max() / len(samples) if not starts_with.empty else 0
        max_end_ratio = ends_with.max() / len(samples) if not ends_with.empty else 0
        
        # 计算模式一致性分数
        pattern_consistency = (max_start_ratio + max_end_ratio) / 2
        
        return {
            'length_stats': length_stats,
            'pattern_consistency': float(pattern_consistency),
            'common_prefixes': starts_with.head(3).to_dict(),
            'common_suffixes': ends_with.head(3).to_dict()
        }
    
    def _identify_issues(self, analysis, data_type):
        """
        识别非结构化数据问题
        
        Args:
            analysis (dict): 分析结果
            data_type (str): 数据类型
            
        Returns:
            list: 问题列表
        """
        issues = []
        
        if data_type == 'json':
            # JSON特有问题
            if analysis.get('valid_ratio', 1) < 0.9:
                issues.append({
                    'type': 'invalid_json',
                    'description': f"有 {analysis.get('invalid_count', 0)} 个无效JSON格式",
                    'severity': 'high'
                })
            
            if analysis.get('depth', 0) > 5:
                issues.append({
                    'type': 'complex_json',
                    'description': f"JSON嵌套深度为 {analysis.get('depth', 0)}，结构较复杂",
                    'severity': 'medium'
                })
            
            # 检查字段一致性
            fields = analysis.get('fields', [])
            if fields:
                inconsistent_fields = [f['name'] for f in fields if f['frequency'] < 0.9]
                if inconsistent_fields:
                    issues.append({
                        'type': 'inconsistent_fields',
                        'description': f"JSON字段不一致，以下字段不是在所有对象中都存在: {', '.join(inconsistent_fields[:5])}",
                        'severity': 'medium'
                    })
        
        elif data_type == 'xml':
            # XML特有问题
            if analysis.get('valid_structure_ratio', 1) < 0.9:
                issues.append({
                    'type': 'invalid_xml',
                    'description': f"有 {(1 - analysis.get('valid_structure_ratio', 0)) * analysis.get('sample_size', 0):.0f} 个XML结构不完整",
                    'severity': 'high'
                })
        
        elif data_type == 'text':
            # 文本特有问题
            length_stats = analysis.get('length_stats', {})
            
            # 检查长度变化大的情况
            if length_stats.get('std', 0) > length_stats.get('mean', 0) * 0.5:
                issues.append({
                    'type': 'variable_length',
                    'description': f"文本长度变化很大 (mean={length_stats.get('mean', 0):.1f}, std={length_stats.get('std', 0):.1f})",
                    'severity': 'low'
                })
            
            # 检查特殊字符比例高的情况
            char_stats = analysis.get('character_stats', {})
            if char_stats.get('special_ratio', 0) > 0.3:
                issues.append({
                    'type': 'high_special_chars',
                    'description': f"特殊字符比例较高 ({char_stats.get('special_ratio', 0):.1%})",
                    'severity': 'medium'
                })
        
        else:
            # 其他类型的通用问题
            length_stats = analysis.get('length_stats', {})
            if length_stats.get('std', 0) > 0 and length_stats.get('mean', 0) > 0:
                cv = length_stats['std'] / length_stats['mean']  # 变异系数
                if cv > 1:
                    issues.append({
                        'type': 'inconsistent_format',
                        'description': f"数据格式不一致，长度变异系数为 {cv:.2f}",
                        'severity': 'medium'
                    })
        
        return issues
    
    def _get_ai_analysis(self, series, data_type, column_info):
        """
        使用Gemini API进行增强分析
        
        Args:
            series (Series): 列数据
            data_type (str): 数据类型
            column_info (dict): 列信息
            
        Returns:
            dict: AI分析结果
        """
        if not self.gemini:
            return {"error": "Gemini API not available"}
        
        # 准备样本数据
        samples = series.dropna().head(5).astype(str).tolist()
        
        # 构建上下文信息
        context = {
            'column_name': column_info.get('column_name') if column_info else 'unknown',
            'data_type': data_type,
            'sample_count': len(samples)
        }
        
        # 调用Gemini API进行分析
        try:
            result = self.gemini.analyze_unstructured_content(samples, context)
            return result
        except Exception as e:
            logging.error(f"Gemini API调用失败: {str(e)}")
            return {"error": str(e)}