"""
Gemini API集成模块，用于与Google Gemini API交互
"""
import logging
import json
import google.generativeai as genai
import time

class GeminiIntegrator:
    """Gemini API集成器"""
    
    def __init__(self, config):
        """
        初始化Gemini API集成器
        
        Args:
            config (Config): 配置对象
        """
        self.config = config
        self.api_key = config.get('gemini.api_key')
        self.model = config.get('gemini.model')
        self.temperature = config.get('gemini.temperature')
        self.max_tokens = config.get('gemini.max_tokens')
        
        # 初始化API
        if self.api_key:
            try:
                genai.configure(api_key=self.api_key)
                logging.info(f"Gemini API初始化成功，使用模型: {self.model}")
                self._is_initialized = True
            except Exception as e:
                logging.error(f"Gemini API初始化失败: {str(e)}")
                self._is_initialized = False
        else:
            logging.warning("未提供Gemini API密钥，AI增强分析将不可用")
            self._is_initialized = False
    
    def is_available(self):
        """
        检查Gemini API是否可用
        
        Returns:
            bool: 是否可用
        """
        return self._is_initialized and self.config.get('gemini.enable')
    
    def analyze_schema(self, schema_info):
        """
        分析数据库schema设计
        
        Args:
            schema_info (dict): Schema信息
            
        Returns:
            dict: 分析结果
        """
        if not self.is_available():
            return {"error": "Gemini API not available"}
        
        # 构建提示
        prompt = f"""
        你是一位专业的数据库设计分析师，请分析以下PostgreSQL数据库schema并提供设计评估和优化建议：
        
        Schema名称: {schema_info.get('name')}
        表数量: {schema_info.get('table_count')}
        关系数量: {schema_info.get('relationship_count')}
        
        表列表: {', '.join(schema_info.get('tables', [])[:20])}
        
        请提供以下分析:
        1. Schema设计的整体评估
        2. 数据建模模式的识别(如星型模式、雪花模式等)
        3. 可能的设计问题
        4. 优化建议
        
        请以JSON格式返回结果，包含以下字段:
        {{
            "overall_assessment": "整体评估",
            "data_modeling_patterns": ["识别的数据建模模式"],
            "potential_issues": ["可能的设计问题"],
            "optimization_suggestions": ["优化建议"]
        }}
        """
        
        return self._call_gemini_api(prompt, parse_json=True)
    

    def analyze_text_semantics(self, text_samples, column_context=None):
        """
        分析文本语义
        
        Args:
            text_samples (list): 文本样本列表
            column_context (dict, optional): 列上下文信息
            
        Returns:
            dict: 分析结果
        """
        if not self.is_available():
            return {"error": "Gemini API not available"}
        
        # 准备样本文本
        samples_text = "\n\n".join([
            f"样本 {i+1}: {sample[:500]}..." if len(sample) > 500 else f"样本 {i+1}: {sample}"
            for i, sample in enumerate(text_samples[:3])
        ])
        
        column_name = column_context.get('column', 'unknown') if column_context else 'unknown'
        
        # 构建提示
        prompt = f"""
        你是一位专业的文本分析师，请对下列文本样本进行深度语义分析。这些文本来自名为"{column_name}"的数据库列：
        
        {samples_text}
        
        请提供以下分析:
        1. 主题识别：这些文本主要讨论什么主题？
        2. 情感分析：文本的主要情感倾向是什么？
        3. 内容分类：这些文本可以分为哪些类别？
        4. 关键实体：文本中提到了哪些重要的实体（人物、组织、地点等）？
        5. 格式特点：文本是否遵循特定格式或结构？
        6. 写作风格：文本的语言风格、正式程度如何？
        
        请以JSON格式返回结果，包含以下字段:
        {
            "topics": ["识别的主题"],
            "sentiment": {"overall": "整体情感", "details": "详细分析"},
            "categories": ["内容类别"],
            "key_entities": ["关键实体"],
            "format_characteristics": ["格式特点"],
            "writing_style": "写作风格分析"
        }
        """
        
        return self._call_gemini_api(prompt, parse_json=True)
    
    def analyze_json_structure(self, json_samples, column_context=None):
        """
        分析JSON结构
        
        Args:
            json_samples (list): JSON样本列表
            column_context (dict, optional): 列上下文信息
            
        Returns:
            dict: 分析结果
        """
        if not self.is_available():
            return {"error": "Gemini API not available"}
        
        # 准备样本文本
        samples_text = "\n\n".join([
            f"样本 {i+1}:\n{sample}" for i, sample in enumerate(json_samples[:2])
        ])
        
        column_name = column_context.get('column', 'unknown') if column_context else 'unknown'
        
        # 构建提示
        prompt = f"""
        你是一位专业的JSON数据分析师，请分析以下JSON样本，这些JSON数据来自名为"{column_name}"的数据库列：

        {samples_text}

        请提供以下分析:
        1. JSON Schema: 推断这些JSON的通用Schema结构
        2. 字段分析: 对主要字段的数据类型和用途进行分析
        3. 嵌套层级: 评估JSON的嵌套复杂度
        4. 标准化建议: 提供将这些JSON标准化为结构化表的建议
        5. 索引策略: 如果需要对这些JSON数据进行查询，建议的索引策略

        请以JSON格式返回结果，包含以下字段:
        {{
            "schema": {{"type": "object", "properties": {{}}}},
            "field_analysis": [{{"field": "字段名", "type": "类型", "purpose": "用途"}}],
            "nesting_complexity": {{"level": "复杂度评级", "description": "描述"}},
            "normalization_suggestions": ["标准化建议"],
            "indexing_strategy": ["索引策略"]
        }}
        """

        
        return self._call_gemini_api(prompt, parse_json=True)
    
    def analyze_binary_content(self, binary_samples, column_context=None):
        """
        分析二进制内容
        
        Args:
            binary_samples (list): 二进制样本列表
            column_context (dict, optional): 列上下文信息
            
        Returns:
            dict: 分析结果
        """
        if not self.is_available():
            return {"error": "Gemini API not available"}
        
        # 准备样本文本（二进制数据用字符串表示）
        samples_text = "\n\n".join([
            f"样本 {i+1}: {sample}" for i, sample in enumerate(binary_samples[:3])
        ])
        
        column_name = column_context.get('column', 'unknown') if column_context else 'unknown'
        
        # 构建提示
        prompt = f"""
        你是一位专业的数据分析师，请分析以下可能是二进制内容的文本表示，这些数据来自名为"{column_name}"的数据库列：
        
        {samples_text}
        
        请提供以下分析:
        1. 数据类型识别: 这些数据可能代表什么类型的内容？
        2. 文件格式推测: 如果是文件数据，可能的文件格式是什么？
        3. 数据特征: 从这些样本中观察到的特征模式
        4. 处理建议: 如何最佳地处理和分析这类数据
        
        请以JSON格式返回结果，包含以下字段:
        {
            "content_type": "识别的内容类型",
            "possible_file_formats": ["可能的文件格式"],
            "data_characteristics": ["数据特征"],
            "processing_recommendations": ["处理建议"]
        }
        """
        
        return self._call_gemini_api(prompt, parse_json=True)
    
    
    
    def recommend_preprocessing(self, column_name, data_type, analysis_summary, sample_data):
        """
        推荐数据预处理策略
        
        Args:
            column_name (str): 列名
            data_type (str): 数据类型
            analysis_summary (dict): 分析摘要
            sample_data (list): 数据样本
            
        Returns:
            dict: 预处理建议
        """
        if not self.is_available():
            return {"error": "Gemini API not available"}
        
        # 准备分析摘要
        summary_text = json.dumps(analysis_summary, indent=2)
        
        # 准备样本数据
        samples_text = "\n".join([f"样本 {i+1}: {str(sample)[:100]}" for i, sample in enumerate(sample_data)])
        
        # 构建提示
        prompt = f"""
        你是一位数据预处理专家，请为以下数据列推荐预处理策略：
        
        列名: {column_name}
        数据类型: {data_type}
        
        分析摘要:
        {summary_text}
        
        数据样本:
        {samples_text}
        
        请推荐3-5个针对此类数据的预处理策略，这些策略应该解决数据质量问题并为后续分析准备数据。
        
        对于每项策略，请提供：
        1. 策略名称
        2. 策略描述
        3. 优先级（高/中/低）
        4. 实现策略的Python代码（使用pandas）
        5. 预期结果
        
        Python代码应该：
        - 假设数据在名为'df'的pandas DataFrame中，列名为'{column_name}'
        - 代码应该是完整的、可以直接执行的
        
        请以JSON格式返回结果，包含以下字段:
        {
            "strategies": [
                {
                    "name": "策略名称",
                    "description": "策略描述",
                    "priority": "high/medium/low",
                    "code": "Python代码",
                    "expected_outcome": "预期结果"
                }
            ],
            "overall_recommendation": "整体建议"
        }
        """
        
        return self._call_gemini_api(prompt, parse_json=True)
    
    # gemini_integrator.py文件中修复格式化字符串的问题

    def analyze_unstructured_content(self, text_samples, column_context):
        """
        分析非结构化内容
        
        Args:
            text_samples (list): 文本样本列表
            column_context (dict): 列上下文信息
            
        Returns:
            dict: 分析结果
        """
        if not self.is_available():
            return {"error": "Gemini API not available"}
        
        # 准备样本文本
        samples_text = "\n\n".join([
            f"样本 {i+1}: {sample[:500]}..." if len(sample) > 500 else f"样本 {i+1}: {sample}"
            for i, sample in enumerate(text_samples[:3])
        ])
        
        # 构建提示 - 修复JSON格式
        prompt = f"""
        你是一位专业的数据分析师，请分析以下PostgreSQL数据库中"{column_context.get('table')}.{column_context.get('column')}"列的文本样本:
        
        {samples_text}
        
        请提供以下分析:
        1. 这些数据是什么类型（纯文本、JSON、XML、混合内容等）？
        2. 数据中包含哪些关键信息结构？
        3. 如何最佳地解析和预处理这些数据？
        4. 建议的数据质量检查标准是什么？
        5. 是否需要将这些非结构化数据转换为结构化存储？
        
        请以JSON格式返回结果，包含以下字段:
        {{
            "data_type": "识别的数据类型",
            "key_information_structures": ["关键信息结构"],
            "parsing_preprocessing_methods": ["建议的解析和预处理方法"],
            "quality_check_standards": ["建议的质量检查标准"],
            "structuring_recommendation": "是否需要结构化转换及建议"
        }}
        """
        
        return self._call_gemini_api(prompt, parse_json=True)

    def generate_custom_checks(self, sample_data, context):
        """
        生成自定义检查规则
        
        Args:
            sample_data (list): 数据样本
            context (dict): 列上下文信息
            
        Returns:
            dict: 检查规则
        """
        if not self.is_available():
            return {"error": "Gemini API not available"}
        
        # 限制样本数据大小
        limited_samples = sample_data[:10]
        samples_text = "\n".join([f"样本 {i+1}: {str(sample)[:100]}" for i, sample in enumerate(limited_samples)])
        
        # 准备统计信息
        stats_text = json.dumps(context.get('basic_stats', {}), indent=2)
        
        # 构建提示 - 修复JSON格式
        prompt = f"""
        你是一位数据质量专家，请为以下数据列生成自定义质量检查规则：
        
        列名: {context.get('column_name')}
        数据类型: {context.get('data_type')}
        列描述: {context.get('column_description', '无')}
        
        基本统计信息:
        {stats_text}
        
        数据样本:
        {samples_text}
        
        请生成5-8个针对性的数据质量检查规则，这些规则应该能够检测出潜在的质量问题。
        
        对于每项检查，请提供：
        1. 唯一的检查ID
        2. 检查描述
        3. 问题的严重程度（高/中/低）
        4. 实现检查的Python代码（使用pandas）
        5. 创建此检查的理由
        
        Python代码应该：
        - 假设数据在名为'data'的pandas Series中
        - 将检查结果存储在名为'result'的字典中，包含'passed'字段（布尔值）和可选的'details'字段
        - 代码应该是完整的、可以直接执行的
        
        请以JSON格式返回结果，包含以下字段:
        {{
            "checks": [
                {{
                    "id": "check_id",
                    "description": "检查描述",
                    "severity": "high/medium/low",
                    "code": "Python代码",
                    "rationale": "创建理由"
                }}
            ]
        }}
        """
        
        return self._call_gemini_api(prompt, parse_json=True)

    def generate_table_checks(self, context):
        """
        生成表级检查规则
        
        Args:
            context (dict): 表上下文信息
            
        Returns:
            dict: 检查规则
        """
        if not self.is_available():
            return {"error": "Gemini API not available"}
        
        # 准备列信息
        columns_info = "列名和类型:\n"
        for col, dtype in context.get('column_types', {}).items():
            columns_info += f"- {col}: {dtype}\n"
        
        # 构建提示 - 修复JSON格式
        prompt = f"""
        你是一位数据质量专家，请为以下数据表生成自定义的表级质量检查规则：
        
        表名: {context.get('table_name')}
        列数: {context.get('column_count')}
        行数: {context.get('row_count')}
        
        {columns_info}
        
        请生成3-5个表级数据质量检查规则，这些规则应该关注跨列的关系和表级完整性。
        
        对于每项检查，请提供：
        1. 唯一的检查ID
        2. 检查描述
        3. 问题的严重程度（高/中/低）
        4. 实现检查的Python代码（使用pandas）
        5. 创建此检查的理由
        
        Python代码应该：
        - 假设数据在名为'data'的pandas DataFrame中
        - 将检查结果存储在名为'result'的字典中，包含'passed'字段（布尔值）和可选的'details'字段
        - 代码应该是完整的、可以直接执行的
        
        请以JSON格式返回结果，包含以下字段:
        {{
            "checks": [
                {{
                    "id": "check_id",
                    "description": "检查描述",
                    "severity": "high/medium/low",
                    "code": "Python代码",
                    "rationale": "创建理由"
                }}
            ]
        }}
        """
        
        return self._call_gemini_api(prompt, parse_json=True)

    def analyze_optimization_opportunities(self, table_info, quality_results):
        """
        分析优化机会
        
        Args:
            table_info (dict): 表信息
            quality_results (dict): 质量检查结果
            
        Returns:
            dict: 优化建议
        """
        if not self.is_available():
            return {"error": "Gemini API not available"}
        
        # 准备表信息
        table_name = table_info.get('name', 'unknown')
        table_stats = json.dumps({
            "row_count": table_info.get('info', {}).get('approximate_row_count', 'unknown'),
            "total_size": table_info.get('info', {}).get('total_size', 'unknown'),
            "column_count": table_info.get('column_count', 'unknown')
        }, indent=2)
        
        # 准备质量检查结果摘要
        issues_summary = []
        for checker_type, results in quality_results.items():
            if isinstance(results, dict) and 'issues' in results:
                for issue in results.get('issues', []):
                    issues_summary.append(f"- {checker_type}: {issue.get('description', 'No description')}")
        
        issues_text = "\n".join(issues_summary[:10])  # 限制为前10个问题
        
        # 构建提示 - 修复JSON格式
        prompt = f"""
        你是一位PostgreSQL数据库优化专家，请分析以下表的信息并提供优化建议：
        
        表名: {table_name}
        
        表统计信息:
        {table_stats}
        
        发现的质量问题:
        {issues_text}
        
        请提供以下分析:
        1. 存储优化：如何优化这个表的存储（分区、压缩等）
        2. 查询优化：如何优化对这个表的查询（索引、物化视图等）
        3. 维护建议：维护此表的最佳实践
        4. 架构优化：可能的架构改进（列拆分、标准化等）
        5. 性能优化建议的优先级和预期收益
        
        请以JSON格式返回结果，包含以下字段:
        {{
            "storage_optimization": ["存储优化建议"],
            "query_optimization": ["查询优化建议"],
            "maintenance_recommendations": ["维护建议"],
            "schema_improvements": ["架构改进"],
            "priority_recommendations": ["按优先级排序的建议，包含预期收益"]
        }}
        """
        
        return self._call_gemini_api(prompt, parse_json=True)

    def generate_report_insights(self, summary_data):
        """
        为报告生成见解
        
        Args:
            summary_data (dict): 分析摘要数据
            
        Returns:
            dict: 见解和建议
        """
        if not self.is_available():
            return {"error": "Gemini API not available"}
        
        # 准备摘要数据
        summary_text = json.dumps(summary_data, indent=2)
        
        # 构建提示
        prompt = f"""
        你是一位数据质量和数据库优化专家，请基于以下分析摘要提供高级见解和建议：
        
        分析摘要:
        {summary_text}
        
        请提供以下见解:
        1. 数据质量评估：基于发现的问题评估整体数据质量
        2. 关键问题：识别最需要关注的数据质量问题，按优先级排序
        3. 趋势和模式：发现的数据问题趋势和模式
        4. 改进建议：针对主要问题的具体改进步骤
        5. 最佳实践：针对此数据库的数据质量最佳实践建议
        
        请以JSON格式返回结果，包含以下字段:
        {{
            "quality_assessment": {{"score": "评分（1-10）", "summary": "整体质量评估"}},
            "key_issues": [{{"issue": "问题描述", "impact": "影响", "priority": "优先级"}}],
            "patterns": ["识别的趋势和模式"],
            "improvement_steps": [{{"issue": "问题", "steps": ["改进步骤"]}}],
            "best_practices": ["最佳实践建议"]
        }}
        """
        
        return self._call_gemini_api(prompt, parse_json=True)
    
    def _call_gemini_api(self, prompt, parse_json=False, retry_count=2):
        """
        调用Gemini API
        
        Args:
            prompt (str): 提示文本
            parse_json (bool, optional): 是否解析返回的JSON
            retry_count (int, optional): 重试次数
            
        Returns:
            dict or str: API响应
        """
        if not self.is_available():
            return {"error": "Gemini API not available"}
        
        for attempt in range(retry_count + 1):
            try:
                # 创建模型实例
                model = genai.GenerativeModel(
                    model_name=self.model,
                    generation_config={
                        "temperature": self.temperature,
                        "max_output_tokens": self.max_tokens
                    }
                )
                
                # 调用API
                response = model.generate_content(prompt)
                text_response = response.text
                
                # 如果需要解析JSON
                if parse_json:
                    try:
                        # 提取JSON部分
                        json_text = self._extract_json(text_response)
                        return json.loads(json_text)
                    except json.JSONDecodeError as je:
                        logging.error(f"JSON解析失败: {str(je)}")
                        if attempt < retry_count:
                            logging.info(f"尝试重新请求，第{attempt+1}次重试...")
                            time.sleep(1)  # 等待1秒后重试
                            continue
                        return {"error": "Failed to parse JSON from response", "text_response": text_response}
                
                return text_response
                
            except Exception as e:
                logging.error(f"Gemini API调用失败: {str(e)}")
                if attempt < retry_count:
                    logging.info(f"尝试重新请求，第{attempt+1}次重试...")
                    time.sleep(1)  # 等待1秒后重试
                else:
                    return {"error": str(e)}
        
        return {"error": "Maximum retry attempts reached"}
    
    def _extract_json(self, text):
        """
        从文本中提取JSON
        
        Args:
            text (str): 含有JSON的文本
            
        Returns:
            str: 提取的JSON文本
        """
        # 尝试提取被```json ```或```包围的JSON
        import re
        json_pattern = r'```(?:json)?\s*([\s\S]*?)\s*```'
        matches = re.findall(json_pattern, text)
        
        if matches:
            return matches[0]
        
        # 如果没有找到被```包围的JSON，尝试提取{开头}结尾的部分
        curly_pattern = r'(\{[\s\S]*\})'
        matches = re.findall(curly_pattern, text)
        
        if matches:
            # 选择最长的匹配，这可能是完整的JSON
            return max(matches, key=len)
        
        # 如果都没找到，返回原文本
        return text
