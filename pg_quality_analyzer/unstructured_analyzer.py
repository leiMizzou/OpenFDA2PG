"""
非结构化数据分析模块，用于分析文本、JSON等非结构化内容
"""
import logging
import json
import pandas as pd
import numpy as np
import re
from collections import Counter
import nltk
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.cluster import KMeans
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
    except LookupError as e:
        if 'punkt_tab' in str(e):
            # 如果错误与punkt_tab有关，尝试下载punkt并再次调用
            try:
                nltk.download('punkt', quiet=True)
                return nltk.sent_tokenize(text)
            except:
                pass
        
        # 如果上述方法失败，使用简单的基于句点的分割
        sentences = []
        for potential_sentence in text.split('.'):
            if potential_sentence.strip():
                sentences.append(potential_sentence.strip() + '.')
        return sentences if sentences else [text]
    except Exception as e:
        logging.debug(f"NLTK分句失败: {str(e)}，使用简单分句")
        # 简单分句：按句号分割
        return [s.strip() + '.' for s in text.split('.') if s.strip()]

# 初始化时确保资源可用
ensure_nltk_resources()

class UnstructuredAnalyzer:
    """非结构化数据分析器"""
    
    def __init__(self, config, gemini_integrator=None):
        """
        初始化非结构化数据分析器
        
        Args:
            config (Config): 配置对象
            gemini_integrator (GeminiIntegrator, optional): Gemini集成器
        """
        self.config = config
        self.gemini = gemini_integrator
    
    def analyze_text(self, text_samples, column_context=None):
        """
        分析文本类型的非结构化数据
        
        Args:
            text_samples (list): 文本样本列表
            column_context (dict, optional): 列上下文信息
            
        Returns:
            dict: 文本分析结果
        """
        if not text_samples:
            return {"error": "No text samples provided"}
        
        # 基本文本统计
        basic_stats = self._get_text_basic_stats(text_samples)
        
        # 语言检测
        language_info = self._detect_language(text_samples)
        
        # 主题分析
        topic_analysis = self._analyze_topics(text_samples)
        
        # 词频分析
        word_freq = self._analyze_word_frequency(text_samples)
        
        # 文本聚类
        clusters = self._cluster_texts(text_samples)
        
        # 实体识别
        entities = self._extract_entities(text_samples)
        
        # 结构化元素分析
        structure_analysis = self._analyze_text_structure(text_samples)
        
        # 如果启用了Gemini API，获取高级语义分析
        semantic_analysis = None
        if self.gemini and self.config.get('gemini.unstructured_analysis'):
            try:
                semantic_analysis = self.gemini.analyze_text_semantics(text_samples, column_context)
            except Exception as e:
                logging.error(f"Gemini API文本语义分析失败: {str(e)}")
        
        return {
            "basic_stats": basic_stats,
            "language": language_info,
            "topics": topic_analysis,
            "word_frequency": word_freq,
            "clusters": clusters,
            "entities": entities,
            "structure": structure_analysis,
            "semantic_analysis": semantic_analysis
        }
    
    def _get_text_basic_stats(self, text_samples):
        """
        获取文本基本统计信息
        
        Args:
            text_samples (list): 文本样本列表
            
        Returns:
            dict: 基本统计信息
        """
        # 计算文本长度
        lengths = [len(text) for text in text_samples]
        
        # 计算单词数 - 使用安全分词函数
        word_counts = [len(safe_word_tokenize(text)) for text in text_samples]
        
        # 计算句子数 - 使用安全分句函数
        sentence_counts = [len(safe_sent_tokenize(text)) for text in text_samples]
        
        # 计算平均单词长度
        all_words = []
        for text in text_samples:
            all_words.extend(safe_word_tokenize(text))
        
        avg_word_length = sum(len(word) for word in all_words) / len(all_words) if all_words else 0
        
        return {
            "character_count": {
                "min": min(lengths),
                "max": max(lengths),
                "mean": sum(lengths) / len(lengths),
                "total": sum(lengths)
            },
            "word_count": {
                "min": min(word_counts),
                "max": max(word_counts),
                "mean": sum(word_counts) / len(word_counts),
                "total": sum(word_counts)
            },
            "sentence_count": {
                "min": min(sentence_counts),
                "max": max(sentence_counts),
                "mean": sum(sentence_counts) / len(sentence_counts),
                "total": sum(sentence_counts)
            },
            "avg_word_length": avg_word_length,
            "avg_sentence_length": sum(word_counts) / sum(sentence_counts) if sum(sentence_counts) > 0 else 0
        }
    
    def _detect_language(self, text_samples):
        """
        检测文本语言
        
        Args:
            text_samples (list): 文本样本列表
            
        Returns:
            dict: 语言检测结果
        """
        # 简单的语言检测基于常用词
        english_common_words = {"the", "be", "to", "of", "and", "a", "in", "that", "have", "is"}
        chinese_common_words = {"的", "是", "在", "有", "和", "上", "不", "了", "我", "这"}
        spanish_common_words = {"el", "la", "de", "que", "y", "a", "en", "un", "ser", "se"}
        
        language_scores = {"english": 0, "chinese": 0, "spanish": 0, "other": 0}
        
        for text in text_samples:
            # 使用安全分词
            words = set(safe_word_tokenize(text.lower()))
            
            # 计算每种语言的匹配分数
            english_score = len(words.intersection(english_common_words))
            chinese_score = len(words.intersection(chinese_common_words))
            spanish_score = len(words.intersection(spanish_common_words))
            
            # 确定最可能的语言
            max_score = max(english_score, chinese_score, spanish_score)
            
            if max_score == 0:
                language_scores["other"] += 1
            elif max_score == english_score:
                language_scores["english"] += 1
            elif max_score == chinese_score:
                language_scores["chinese"] += 1
            elif max_score == spanish_score:
                language_scores["spanish"] += 1
        
        # 确定主要语言
        total = sum(language_scores.values())
        primary_language = max(language_scores.items(), key=lambda x: x[1])[0]
        language_distribution = {lang: count / total for lang, count in language_scores.items()}
        
        return {
            "primary_language": primary_language,
            "distribution": language_distribution
        }
    
    def _analyze_topics(self, text_samples):
        """
        分析文本主题
        
        Args:
            text_samples (list): 文本样本列表
            
        Returns:
            dict: 主题分析结果
        """
        try:
            # 使用简单的TF-IDF特征提取
            vectorizer = CountVectorizer(stop_words='english', max_features=50)
            X = vectorizer.fit_transform(text_samples)
            
            # 获取词汇
            terms = vectorizer.get_feature_names_out()
            
            # 计算每个词的频率
            frequencies = X.sum(axis=0).A1
            
            # 将词汇和频率组合
            topic_terms = [{"term": term, "frequency": freq} for term, freq in zip(terms, frequencies)]
            
            # 按频率排序
            topic_terms = sorted(topic_terms, key=lambda x: x["frequency"], reverse=True)
            
            return {
                "key_terms": topic_terms[:20],
                "total_terms": len(terms)
            }
            
        except Exception as e:
            logging.error(f"主题分析失败: {str(e)}")
            return {"error": str(e)}
    
    def _analyze_word_frequency(self, text_samples):
        """
        分析词频
        
        Args:
            text_samples (list): 文本样本列表
            
        Returns:
            dict: 词频分析结果
        """
        # 获取所有单词 - 使用安全分词
        all_words = []
        for text in text_samples:
            all_words.extend(safe_word_tokenize(text.lower()))
        
        # 过滤掉标点符号
        words = [word for word in all_words if word.isalnum()]
        
        # 计算词频
        word_counter = Counter(words)
        
        # 提取n-gram
        bigrams = []
        trigrams = []
        
        for text in text_samples:
            tokens = safe_word_tokenize(text.lower())
            
            # 生成二元语法
            for i in range(len(tokens) - 1):
                bigrams.append((tokens[i], tokens[i+1]))
            
            # 生成三元语法
            for i in range(len(tokens) - 2):
                trigrams.append((tokens[i], tokens[i+1], tokens[i+2]))
        
        bigram_counter = Counter(bigrams)
        trigram_counter = Counter(trigrams)
        
        return {
            "unigrams": {word: count for word, count in word_counter.most_common(20)},
            "bigrams": {" ".join(bigram): count for bigram, count in bigram_counter.most_common(10)},
            "trigrams": {" ".join(trigram): count for trigram, count in trigram_counter.most_common(10)},
            "unique_words": len(word_counter),
            "total_words": len(words)
        }
    
    def _cluster_texts(self, text_samples):
        """
        对文本进行聚类
        
        Args:
            text_samples (list): 文本样本列表
            
        Returns:
            dict: 聚类结果
        """
        try:
            if len(text_samples) < 5:
                return {"error": "Not enough samples for clustering"}
            
            # 使用向量化表示文本
            vectorizer = CountVectorizer(stop_words='english')
            X = vectorizer.fit_transform(text_samples)
            
            # 确定聚类数量
            n_clusters = min(3, len(text_samples) // 2)
            
            # 应用K-means聚类
            kmeans = KMeans(n_clusters=n_clusters, random_state=42)
            clusters = kmeans.fit_predict(X)
            
            # 计算每个聚类的样本数
            cluster_sizes = {}
            for cluster_id in range(n_clusters):
                cluster_sizes[f"cluster_{cluster_id}"] = int((clusters == cluster_id).sum())
            
            # 为每个聚类找出最有代表性的文本
            cluster_examples = {}
            for cluster_id in range(n_clusters):
                cluster_indices = np.where(clusters == cluster_id)[0]
                if len(cluster_indices) > 0:
                    # 简单地取第一个示例
                    example_idx = cluster_indices[0]
                    cluster_examples[f"cluster_{cluster_id}"] = text_samples[example_idx][:100] + "..."
            
            # 找出每个聚类的关键词
            cluster_keywords = {}
            for cluster_id in range(n_clusters):
                cluster_indices = np.where(clusters == cluster_id)[0]
                if len(cluster_indices) > 0:
                    # 计算该聚类中的词频
                    cluster_texts = [text_samples[i] for i in cluster_indices]
                    all_words = []
                    for text in cluster_texts:
                        all_words.extend(safe_word_tokenize(text.lower()))
                    
                    # 过滤掉标点符号和常见停用词
                    stopwords = {"the", "a", "an", "and", "to", "of", "is", "in", "that", "for"}
                    words = [word for word in all_words if word.isalnum() and word not in stopwords]
                    
                    # 计算词频
                    word_counter = Counter(words)
                    cluster_keywords[f"cluster_{cluster_id}"] = [word for word, _ in word_counter.most_common(5)]
            
            return {
                "n_clusters": n_clusters,
                "cluster_sizes": cluster_sizes,
                "cluster_examples": cluster_examples,
                "cluster_keywords": cluster_keywords
            }
            
        except Exception as e:
            logging.error(f"文本聚类失败: {str(e)}")
            return {"error": str(e)}
    
    def _extract_entities(self, text_samples):
        """
        提取实体
        
        Args:
            text_samples (list): 文本样本列表
            
        Returns:
            dict: 实体提取结果
        """
        # 使用简单的正则表达式提取常见实体
        
        # 电子邮件
        email_pattern = re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')
        emails = []
        
        # URL
        url_pattern = re.compile(r'https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+')
        urls = []
        
        # 日期
        date_pattern = re.compile(r'\d{1,4}[-/]\d{1,2}[-/]\d{1,4}')
        dates = []
        
        # 电话号码
        phone_pattern = re.compile(r'\(?(?:\+?(\d{1,3}))?[-. (]*(\d{3})[-. )]*(\d{3})[-. ]*(\d{4})(?: *x(\d+))?\)?')
        phones = []
        
        # IP地址
        ip_pattern = re.compile(r'\b(?:\d{1,3}\.){3}\d{1,3}\b')
        ips = []
        
        for text in text_samples:
            emails.extend(email_pattern.findall(text))
            urls.extend(url_pattern.findall(text))
            dates.extend(date_pattern.findall(text))
            phones.extend(phone_pattern.findall(text))
            ips.extend(ip_pattern.findall(text))
        
        # 去重并限制数量
        emails = list(set(emails))[:10]
        urls = list(set(urls))[:10]
        dates = list(set(dates))[:10]
        phones = list(set(phones))[:10]
        ips = list(set(ips))[:10]
        
        return {
            "emails": emails,
            "urls": urls,
            "dates": dates,
            "phones": phones,
            "ips": ips,
            "entity_counts": {
                "emails": len(emails),
                "urls": len(urls),
                "dates": len(dates),
                "phones": len(phones),
                "ips": len(ips)
            }
        }
    
    def _analyze_text_structure(self, text_samples):
        """
        分析文本结构
        
        Args:
            text_samples (list): 文本样本列表
            
        Returns:
            dict: 文本结构分析结果
        """
        # 分析文本是否遵循特定结构
        
        # 检查是否为JSON格式
        json_like = 0
        for text in text_samples:
            if text.strip().startswith('{') and text.strip().endswith('}'):
                try:
                    json.loads(text)
                    json_like += 1
                except:
                    pass
        
        # 检查是否为CSV格式
        csv_like = 0
        for text in text_samples:
            lines = text.strip().split('\n')
            if len(lines) > 1:
                # 检查每行是否有相似数量的逗号
                commas_per_line = [line.count(',') for line in lines]
                if len(set(commas_per_line)) <= 2 and commas_per_line[0] > 0:
                    csv_like += 1
        
        # 检查是否为列表格式
        list_like = 0
        list_patterns = [
            re.compile(r'^\s*\d+\.\s+'),  # 数字列表
            re.compile(r'^\s*-\s+'),       # 破折号列表
            re.compile(r'^\s*\*\s+')       # 星号列表
        ]
        
        for text in text_samples:
            lines = text.strip().split('\n')
            if len(lines) > 1:
                list_matches = 0
                for line in lines:
                    if any(pattern.match(line) for pattern in list_patterns):
                        list_matches += 1
                
                if list_matches / len(lines) > 0.5:
                    list_like += 1
        
        # 分析是否包含标题结构
        heading_like = 0
        heading_pattern = re.compile(r'^#+\s+|\b[A-Z][A-Z0-9 ]{2,}\b')
        
        for text in text_samples:
            lines = text.strip().split('\n')
            heading_count = 0
            
            for line in lines:
                if heading_pattern.search(line) and len(line) < 100:
                    heading_count += 1
            
            if heading_count > 0 and heading_count < len(lines) / 3:
                heading_like += 1
                
        # 确定最可能的结构类型
        total_samples = len(text_samples)
        structure_types = {
            "json": json_like / total_samples if total_samples > 0 else 0,
            "csv": csv_like / total_samples if total_samples > 0 else 0,
            "list": list_like / total_samples if total_samples > 0 else 0,
            "headings": heading_like / total_samples if total_samples > 0 else 0,
            "free_text": 1 - max(json_like, csv_like, list_like, heading_like) / total_samples if total_samples > 0 else 1
        }
        
        primary_structure = max(structure_types.items(), key=lambda x: x[1])[0]
        
        return {
            "primary_structure": primary_structure,
            "structure_types": structure_types
        }
    
    def analyze_json(self, json_samples, column_context=None):
        """
        分析JSON类型的半结构化数据
        
        Args:
            json_samples (list): JSON样本列表
            column_context (dict, optional): 列上下文信息
            
        Returns:
            dict: JSON分析结果
        """
        if not json_samples:
            return {"error": "No JSON samples provided"}
        
        # 解析JSON样本
        parsed_samples = []
        parse_errors = []
        
        for idx, sample in enumerate(json_samples):
            try:
                if isinstance(sample, str):
                    parsed_json = json.loads(sample)
                    parsed_samples.append(parsed_json)
                else:
                    # 如果已经是字典或列表形式，直接添加
                    parsed_samples.append(sample)
            except Exception as e:
                parse_errors.append({
                    "index": idx,
                    "error": str(e),
                    "sample": sample[:100] + "..." if len(sample) > 100 else sample
                })
        
        if not parsed_samples:
            return {
                "error": "No valid JSON samples",
                "parse_errors": parse_errors
            }
        
        # 分析JSON结构
        schema = self._extract_json_schema(parsed_samples)
        
        # 分析字段一致性
        field_consistency = self._analyze_json_field_consistency(parsed_samples)
        
        # 分析嵌套层级
        depth_analysis = self._analyze_json_depth(parsed_samples)
        
        # 分析数组大小
        array_analysis = self._analyze_json_arrays(parsed_samples)
        
        # 分析值分布
        value_analysis = self._analyze_json_values(parsed_samples)
        
        # 如果启用了Gemini API，获取高级JSON分析
        advanced_analysis = None
        if self.gemini and self.config.get('gemini.unstructured_analysis'):
            try:
                # 将解析后的JSON转换回字符串，但格式化以便阅读
                json_str_samples = [json.dumps(json_obj, indent=2)[:500] for json_obj in parsed_samples[:3]]
                advanced_analysis = self.gemini.analyze_json_structure(json_str_samples, column_context)
            except Exception as e:
                logging.error(f"Gemini API JSON分析失败: {str(e)}")
        
        return {
            "schema": schema,
            "field_consistency": field_consistency,
            "depth_analysis": depth_analysis,
            "array_analysis": array_analysis,
            "value_analysis": value_analysis,
            "parse_errors": parse_errors,
            "advanced_analysis": advanced_analysis
        }
    
    def _extract_json_schema(self, json_samples):
        """
        提取JSON样本的通用schema
        
        Args:
            json_samples (list): 解析后的JSON对象列表
            
        Returns:
            dict: JSON schema
        """
        if not json_samples:
            return {}
        
        # 确定基本类型
        first_sample = json_samples[0]
        base_type = "array" if isinstance(first_sample, list) else "object" if isinstance(first_sample, dict) else "primitive"
        
        # 如果是基本类型，返回简单schema
        if base_type == "primitive":
            return {
                "type": base_type,
                "value_type": type(first_sample).__name__
            }
        
        # 如果是数组类型
        if base_type == "array":
            # 检查数组元素类型
            element_schemas = []
            for sample in json_samples:
                if isinstance(sample, list) and sample:
                    # 取第一个元素作为示例
                    element_schemas.append(self._extract_element_schema(sample[0]))
            
            # 合并所有元素模式
            array_schema = self._merge_schemas(element_schemas) if element_schemas else {}
            
            return {
                "type": "array",
                "items": array_schema
            }
        
        # 如果是对象类型
        if base_type == "object":
            # 收集所有样本中的字段
            all_fields = set()
            for sample in json_samples:
                if isinstance(sample, dict):
                    all_fields.update(sample.keys())
            
            # 为每个字段构建schema
            properties = {}
            for field in all_fields:
                field_values = []
                for sample in json_samples:
                    if isinstance(sample, dict) and field in sample:
                        field_values.append(sample[field])
                
                # 计算字段出现率
                occurrence = sum(1 for sample in json_samples if isinstance(sample, dict) and field in sample)
                occurrence_rate = occurrence / len(json_samples)
                
                # 提取字段schema
                if field_values:
                    field_schema = self._extract_element_schema(field_values[0])
                    field_schema["occurrence_rate"] = occurrence_rate
                    properties[field] = field_schema
            
            return {
                "type": "object",
                "properties": properties
            }
        
        return {}
    
    def _extract_element_schema(self, element):
        """
        提取单个元素的schema
        
        Args:
            element: JSON元素
            
        Returns:
            dict: 元素schema
        """
        if element is None:
            return {"type": "null"}
        elif isinstance(element, bool):
            return {"type": "boolean"}
        elif isinstance(element, int):
            return {"type": "integer"}
        elif isinstance(element, float):
            return {"type": "number"}
        elif isinstance(element, str):
            return {"type": "string"}
        elif isinstance(element, list):
            if not element:
                return {"type": "array", "items": {}}
            return {"type": "array", "items": self._extract_element_schema(element[0])}
        elif isinstance(element, dict):
            properties = {}
            for key, value in element.items():
                properties[key] = self._extract_element_schema(value)
            return {"type": "object", "properties": properties}
        else:
            return {"type": "unknown"}
    
    def _merge_schemas(self, schemas):
        """
        合并多个schema
        
        Args:
            schemas (list): schema列表
            
        Returns:
            dict: 合并后的schema
        """
        if not schemas:
            return {}
        
        # 简单合并：使用第一个schema
        return schemas[0]
    
    def _analyze_json_field_consistency(self, json_samples):
        """
        分析JSON字段一致性
        
        Args:
            json_samples (list): 解析后的JSON对象列表
            
        Returns:
            dict: 字段一致性分析结果
        """
        if not json_samples:
            return {}
        
        # 仅分析对象类型
        object_samples = [sample for sample in json_samples if isinstance(sample, dict)]
        
        if not object_samples:
            return {"error": "No object samples for field consistency analysis"}
        
        # 收集所有字段
        all_fields = set()
        for sample in object_samples:
            all_fields.update(sample.keys())
        
        # 统计每个字段的出现次数
        field_counts = {}
        for field in all_fields:
            count = sum(1 for sample in object_samples if field in sample)
            field_counts[field] = {
                "count": count,
                "rate": count / len(object_samples)
            }
        
        # 计算全局一致性分数
        if not all_fields:
            consistency_score = 1.0
        else:
            total_occurrences = sum(data["count"] for data in field_counts.values())
            max_possible = len(all_fields) * len(object_samples)
            consistency_score = total_occurrences / max_possible if max_possible > 0 else 1.0
        
        # 找出常见字段和稀有字段
        common_fields = [field for field, data in field_counts.items() if data["rate"] > 0.9]
        rare_fields = [field for field, data in field_counts.items() if data["rate"] < 0.1]
        
        return {
            "consistency_score": consistency_score,
            "total_fields": len(all_fields),
            "common_fields": common_fields,
            "rare_fields": rare_fields,
            "field_counts": field_counts
        }
    
    def _analyze_json_depth(self, json_samples):
        """
        分析JSON嵌套深度
        
        Args:
            json_samples (list): 解析后的JSON对象列表
            
        Returns:
            dict: 嵌套深度分析结果
        """
        if not json_samples:
            return {}
        
        # 计算每个样本的深度
        depths = []
        for sample in json_samples:
            depth = self._calculate_depth(sample)
            depths.append(depth)
        
        # 计算深度统计
        if not depths:
            return {"error": "Could not calculate depths"}
        
        return {
            "min_depth": min(depths),
            "max_depth": max(depths),
            "avg_depth": sum(depths) / len(depths),
            "depth_distribution": {f"level_{d}": depths.count(d) for d in range(min(depths), max(depths) + 1)}
        }
    
    def _calculate_depth(self, element, current_depth=0):
        """
        计算JSON元素的嵌套深度
        
        Args:
            element: JSON元素
            current_depth (int): 当前深度
            
        Returns:
            int: 元素深度
        """
        if isinstance(element, dict):
            if not element:
                return current_depth
            return max((self._calculate_depth(v, current_depth + 1) for v in element.values()), default=current_depth)
        elif isinstance(element, list):
            if not element:
                return current_depth
            return max((self._calculate_depth(item, current_depth + 1) for item in element), default=current_depth)
        else:
            return current_depth
    
    def _analyze_json_arrays(self, json_samples):
        """
        分析JSON数组大小
        
        Args:
            json_samples (list): 解析后的JSON对象列表
            
        Returns:
            dict: 数组大小分析结果
        """
        # 收集所有数组
        all_arrays = []
        
        def collect_arrays(element, path="$"):
            if isinstance(element, dict):
                for key, value in element.items():
                    new_path = f"{path}.{key}"
                    if isinstance(value, list):
                        all_arrays.append({"path": new_path, "size": len(value)})
                    collect_arrays(value, new_path)
            elif isinstance(element, list):
                for i, item in enumerate(element):
                    new_path = f"{path}[{i}]"
                    collect_arrays(item, new_path)
        
        for sample in json_samples:
            collect_arrays(sample)
        
        if not all_arrays:
            return {"error": "No arrays found"}
        
        # 按路径分组
        arrays_by_path = {}
        for arr in all_arrays:
            path = arr["path"]
            if path not in arrays_by_path:
                arrays_by_path[path] = []
            arrays_by_path[path].append(arr["size"])
        
        # 计算每个路径的数组大小统计
        array_stats = {}
        for path, sizes in arrays_by_path.items():
            array_stats[path] = {
                "min_size": min(sizes),
                "max_size": max(sizes),
                "avg_size": sum(sizes) / len(sizes),
                "count": len(sizes)
            }
        
        return {
            "total_arrays": len(all_arrays),
            "unique_array_paths": len(arrays_by_path),
            "array_stats": array_stats
        }
    
    def _analyze_json_values(self, json_samples):
        """
        分析JSON值分布
        
        Args:
            json_samples (list): 解析后的JSON对象列表
            
        Returns:
            dict: 值分布分析结果
        """
        # 收集所有值
        all_values = []
        
        def collect_values(element):
            if isinstance(element, dict):
                for value in element.values():
                    if not isinstance(value, (dict, list)):
                        all_values.append(value)
                    collect_values(value)
            elif isinstance(element, list):
                for item in element:
                    if not isinstance(item, (dict, list)):
                        all_values.append(item)
                    collect_values(item)
        
        for sample in json_samples:
            collect_values(sample)
        
        if not all_values:
            return {"error": "No primitive values found"}
        
        # 按类型分组
        values_by_type = {}
        for value in all_values:
            value_type = type(value).__name__
            if value_type not in values_by_type:
                values_by_type[value_type] = []
            values_by_type[value_type].append(value)
        
        # 计算每种类型的统计信息
        type_stats = {}
        for value_type, values in values_by_type.items():
            type_stats[value_type] = {"count": len(values), "percentage": len(values) / len(all_values)}
            
            # 对字符串类型计算额外统计
            if value_type == "str":
                lengths = [len(v) for v in values]
                type_stats[value_type]["length_stats"] = {
                    "min": min(lengths),
                    "max": max(lengths),
                    "avg": sum(lengths) / len(lengths)
                }
                
                # 频率统计
                if len(values) > 1:
                    counter = Counter(values)
                    type_stats[value_type]["value_frequencies"] = {str(k): v for k, v in counter.most_common(5)}
            
            # 对数值类型计算额外统计
            elif value_type in ("int", "float"):
                if values:
                    type_stats[value_type]["value_stats"] = {
                        "min": min(values),
                        "max": max(values),
                        "avg": sum(values) / len(values)
                    }
        
        return {
            "total_values": len(all_values),
            "type_distribution": {t: stats["percentage"] for t, stats in type_stats.items()},
            "type_stats": type_stats
        }
    
    def analyze_binary(self, binary_samples, column_context=None):
        """
        分析二进制类型数据
        
        Args:
            binary_samples (list): 二进制数据样本列表
            column_context (dict, optional): 列上下文信息
            
        Returns:
            dict: 二进制数据分析结果
        """
        # 注意：真正的二进制数据分析需要专门的库和工具
        # 这里实现简单的分析，主要基于二进制数据转换为字符串后的特征
        
        if not binary_samples:
            return {"error": "No binary samples provided"}
        
        # 将二进制样本转换为字符串进行分析
        str_samples = []
        for sample in binary_samples:
            try:
                if isinstance(sample, bytes):
                    # 尝试解码为UTF-8
                    try:
                        str_sample = sample.decode('utf-8')
                    except UnicodeDecodeError:
                        # 如果无法解码，使用十六进制表示
                        str_sample = sample.hex()
                else:
                    str_sample = str(sample)
                str_samples.append(str_sample)
            except Exception:
                str_samples.append("")
        
        # 猜测文件类型
        file_types = self._guess_file_types(str_samples)
        
        # 长度分析
        length_stats = {}
        if binary_samples:
            lengths = [len(sample) if isinstance(sample, (bytes, str)) else 0 for sample in binary_samples]
            length_stats = {
                "min": min(lengths),
                "max": max(lengths),
                "avg": sum(lengths) / len(lengths),
                "total": sum(lengths)
            }
        
        # 字符分布分析
        char_distribution = {}
        if str_samples:
            all_chars = ''.join(str_samples)
            char_counter = Counter(all_chars)
            char_distribution = {char: count for char, count in char_counter.most_common(10)}
        
        # 如果启用了Gemini API，获取高级二进制分析
        advanced_analysis = None
        if self.gemini and self.config.get('gemini.unstructured_analysis'):
            try:
                # 使用前100个字符作为样本
                preview_samples = [sample[:100] + "..." if len(sample) > 100 else sample for sample in str_samples[:3]]
                advanced_analysis = self.gemini.analyze_binary_content(preview_samples, column_context)
            except Exception as e:
                logging.error(f"Gemini API二进制分析失败: {str(e)}")
        
        return {
            "file_types": file_types,
            "length_stats": length_stats,
            "char_distribution": char_distribution,
            "advanced_analysis": advanced_analysis
        }
    
    def _guess_file_types(self, str_samples):
        """
        猜测文件类型
        
        Args:
            str_samples (list): 字符串样本列表
            
        Returns:
            dict: 文件类型猜测结果
        """
        # 定义文件类型特征
        type_signatures = {
            "json": ["{", "["],
            "xml": ["<?xml", "<"],
            "csv": [","],
            "html": ["<!DOCTYPE", "<html", "<HTML"],
            "base64": ["=", "/"]
        }
        
        # 计数每种类型的匹配数
        type_matches = {file_type: 0 for file_type in type_signatures}
        
        for sample in str_samples:
            for file_type, signatures in type_signatures.items():
                for sig in signatures:
                    if sample.strip().startswith(sig):
                        type_matches[file_type] += 1
                        break
        
        # 计算匹配率
        total_samples = len(str_samples) if str_samples else 1
        type_probabilities = {file_type: count / total_samples 
                             for file_type, count in type_matches.items()}
        
        # 确定最可能的类型
        most_likely_type = max(type_probabilities.items(), key=lambda x: x[1])
        
        return {
            "most_likely": most_likely_type[0] if most_likely_type[1] > 0.5 else "unknown",
            "probabilities": type_probabilities
        }