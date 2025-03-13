"""
优化分析器模块，负责分析并提供数据库优化建议
"""
import logging
import pandas as pd

class OptimizationAnalyzer:
    """优化分析器，提供数据库优化建议"""
    
    def __init__(self, config, gemini_integrator=None):
        """
        初始化优化分析器
        
        Args:
            config (Config): 配置对象
            gemini_integrator (GeminiIntegrator, optional): Gemini集成器
        """
        self.config = config
        self.gemini = gemini_integrator
    
    def analyze_optimization(self, tables_info, quality_results, schema_info=None):
        """
        分析并提供优化建议
        
        Args:
            tables_info (dict): 表信息
            quality_results (dict): 质量检查结果
            schema_info (dict, optional): Schema信息
            
        Returns:
            dict: 优化建议
        """
        logging.info("开始生成优化建议")
        
        # 存储优化建议
        optimization_results = {}
        
        # 对每个表生成优化建议
        for table_name, table_info in tables_info.items():
            table_optimization = self._analyze_table_optimization(
                table_name, 
                table_info, 
                quality_results.get(table_name, {})
            )
            
            if table_optimization:
                optimization_results[table_name] = table_optimization
        
        # 添加整体Schema优化建议
        if schema_info:
            schema_optimization = self._analyze_schema_optimization(schema_info, tables_info)
            if schema_optimization:
                optimization_results['schema_overall'] = schema_optimization
        
        return optimization_results
    
    def _analyze_table_optimization(self, table_name, table_info, quality_results):
        """
        分析表级优化机会
        
        Args:
            table_name (str): 表名
            table_info (dict): 表信息
            quality_results (dict): 质量检查结果
            
        Returns:
            dict: 表优化建议
        """
        # 检查是否启用了Gemini API，且是否具有AI优化建议
        if self.gemini and self.config.get('gemini.enable'):
            try:
                # 使用Gemini API生成优化建议
                ai_suggestions = self.gemini.analyze_optimization_opportunities(table_info, quality_results)
                if ai_suggestions and not 'error' in ai_suggestions:
                    return ai_suggestions
            except Exception as e:
                logging.error(f"使用Gemini API生成优化建议失败: {str(e)}")
        
        # 如果没有AI建议或AI建议失败，使用基于规则的建议
        return self._generate_rule_based_suggestions(table_name, table_info, quality_results)
    
    def _generate_rule_based_suggestions(self, table_name, table_info, quality_results):
        """
        生成基于规则的优化建议
        
        Args:
            table_name (str): 表名
            table_info (dict): 表信息
            quality_results (dict): 质量检查结果
            
        Returns:
            dict: 优化建议
        """
        storage_suggestions = []
        query_suggestions = []
        maintenance_suggestions = []
        schema_suggestions = []
        
        # 表大小分析
        row_count = table_info.get('info', {}).get('approximate_row_count', 0)
        total_size = table_info.get('info', {}).get('total_bytes', 0)
        
        # 索引分析
        indexes = table_info.get('info', {}).get('indexes', [])
        
        # 空值分析
        high_null_columns = table_info.get('high_null_columns', [])
        
        # 质量问题分析
        consistency_issues = []
        distribution_issues = []
        null_issues = []
        unstructured_issues = []
        
        for checker_type, results in quality_results.items():
            if isinstance(results, dict) and 'issues' in results:
                issues = results.get('issues', [])
                
                if 'consistency' in checker_type:
                    consistency_issues.extend(issues)
                elif 'distribution' in checker_type:
                    distribution_issues.extend(issues)
                elif 'null' in checker_type:
                    null_issues.extend(issues)
                elif 'unstructured' in checker_type:
                    unstructured_issues.extend(issues)
        
        # 生成存储优化建议
        if row_count > 10000000:  # 超过1000万行
            storage_suggestions.append(f"考虑对表 {table_name} 进行分区，按适当的字段(如日期)进行分区以提高性能")
        
        if total_size > 10 * 1024 * 1024 * 1024:  # 超过10GB
            storage_suggestions.append(f"表 {table_name} 体积较大({total_size/(1024*1024*1024):.1f}GB)，考虑实施表压缩策略")
            
        # 有很多索引但总体积很大
        if len(indexes) > 5 and total_size > 1024 * 1024 * 1024:
            storage_suggestions.append(f"表 {table_name} 索引数量多({len(indexes)}个)且表较大，审查索引使用情况，去除不必要的索引")
        
        # 空值过多的大表
        if len(high_null_columns) > 3 and row_count > 1000000:
            storage_suggestions.append(f"表 {table_name} 有{len(high_null_columns)}个高空值率列，考虑拆分这些列到单独的表以节省存储空间")
        
        # 生成查询优化建议
        
        # 表没有主键或唯一索引
        if not any(idx.get('is_primary', False) or idx.get('is_unique', False) for idx in indexes):
            query_suggestions.append(f"表 {table_name} 缺少主键或唯一索引，建议添加以提高查询效率")
        
        # 表大但索引少
        if row_count > 1000000 and len(indexes) < 2:
            query_suggestions.append(f"表 {table_name} 较大但索引较少，考虑添加适当的索引以提高查询性能")
        
        # 类型不一致问题可能影响查询性能
        if any('data_type' in issue.get('type', '') for issue in consistency_issues):
            query_suggestions.append(f"检测到数据类型不一致问题，可能导致查询无法使用索引，建议统一数据类型")
        
        # 生成维护建议
        
        # 表非常大时的维护建议
        if row_count > 5000000 or total_size > 5 * 1024 * 1024 * 1024:
            maintenance_suggestions.append(f"定期对表 {table_name} 执行VACUUM和ANALYZE以回收空间并更新统计信息")
        
        # 有空值问题时的维护建议
        if null_issues:
            maintenance_suggestions.append(f"定期检查和清理表 {table_name} 中的空值数据，特别是在被用于连接的列中")
        
        # 有分布异常时的维护建议
        if any('outlier' in issue.get('type', '') for issue in distribution_issues):
            maintenance_suggestions.append(f"定期监控表 {table_name} 中的数据分布，尤其是检测到异常值的列")
        
        # 非结构化数据的维护建议
        if unstructured_issues:
            maintenance_suggestions.append(f"对表 {table_name} 中的非结构化数据实施规范化策略，并定期验证其格式一致性")
        
        # 生成架构优化建议
        
        # 高空值率列建议
        if high_null_columns:
            high_null_column_names = [col['name'] for col in high_null_columns[:3]]
            schema_suggestions.append(f"考虑将表 {table_name} 中的高空值率列({', '.join(high_null_column_names)}等)移至单独的表")
        
        # 一致性问题的架构改进
        if consistency_issues:
            schema_suggestions.append(f"审查表 {table_name} 的架构设计，解决数据一致性问题，考虑添加约束或触发器")
        
        # 非结构化数据的架构改进
        if unstructured_issues:
            schema_suggestions.append(f"考虑将表 {table_name} 中的非结构化数据正规化为专用表或使用JSONB类型")
        
        # 如果表大且有很多列，可能需要垂直分区
        if row_count > 1000000 and table_info.get('column_count', 0) > 20:
            schema_suggestions.append(f"表 {table_name} 有大量行({row_count})和列({table_info.get('column_count', 0)})，考虑垂直分区")
        
        # 整合所有建议
        return {
            'storage_optimization': storage_suggestions,
            'query_optimization': query_suggestions,
            'maintenance_recommendations': maintenance_suggestions,
            'schema_improvements': schema_suggestions
        }
    
    def _analyze_schema_optimization(self, schema_info, tables_info):
        """
        分析Schema级优化机会
        
        Args:
            schema_info (dict): Schema信息
            tables_info (dict): 所有表信息
            
        Returns:
            dict: Schema优化建议
        """
        storage_suggestions = []
        query_suggestions = []
        maintenance_suggestions = []
        schema_suggestions = []
        
        # 表数量分析
        table_count = len(tables_info)
        
        # 关系分析
        relationships = schema_info.get('relationships', {})
        relationship_count = schema_info.get('relationship_count', 0)
        
        # 分析表大小分布
        table_sizes = []
        for table_name, table_info in tables_info.items():
            row_count = table_info.get('info', {}).get('approximate_row_count', 0)
            total_size = table_info.get('info', {}).get('total_bytes', 0)
            table_sizes.append((table_name, row_count, total_size))
        
        # 按行数排序
        table_sizes.sort(key=lambda x: x[1], reverse=True)
        
        # 大表占比分析
        large_tables = [t for t in table_sizes if t[1] > 1000000]  # 超过100万行的表
        large_table_percentage = len(large_tables) / table_count if table_count > 0 else 0
        
        # 关系完整性分析
        tables_with_relationships = len([t for t in relationships if relationships[t]])
        relationship_coverage = tables_with_relationships / table_count if table_count > 0 else 0
        
        # 检查是否启用了Gemini API，且是否具有AI优化建议
        if self.gemini and self.config.get('gemini.enable'):
            try:
                # 使用Gemini API生成Schema优化建议
                ai_suggestions = self.gemini.analyze_schema(schema_info)
                if ai_suggestions and not 'error' in ai_suggestions:
                    return ai_suggestions
            except Exception as e:
                logging.error(f"使用Gemini API生成Schema优化建议失败: {str(e)}")
        
        # 生成Schema级存储优化建议
        if large_table_percentage > 0.3:  # 超过30%的表是大表
            storage_suggestions.append("数据库中有较高比例的大表，考虑实施分区策略和定期归档历史数据")
        
        if table_count > 50:
            storage_suggestions.append(f"Schema中表数量较多({table_count}个)，考虑按业务领域或功能拆分为多个Schema")
        
        # 生成Schema级查询优化建议
        if relationship_coverage < 0.5 and table_count > 10:
            query_suggestions.append("许多表缺少定义的关系，建议添加外键约束以提高查询优化器效率")
        
        if large_tables:
            large_table_names = [t[0] for t in large_tables[:3]]
            query_suggestions.append(f"对大表({', '.join(large_table_names)}等)创建适当的物化视图以加速常用查询")
        
        # 生成Schema级维护建议
        maintenance_suggestions.append("定期重建索引并更新统计信息，特别是在大量数据变更后")
        maintenance_suggestions.append("实施定期数据质量检查流程，确保数据一致性和完整性")
        
        if table_count > 20:
            maintenance_suggestions.append("设置自动化监控以跟踪表增长和查询性能，及早发现潜在问题")
        
        # 生成Schema级架构改进建议
        if relationship_count < table_count / 2 and table_count > 5:
            schema_suggestions.append("数据库关系较少，可能表明部分表之间的逻辑连接尚未在数据库层面定义")
        
        if table_count > 30:
            schema_suggestions.append("考虑审查Schema设计，是否可以通过合并相似表或规范化设计来简化架构")
        
        schema_suggestions.append("审查数据类型一致性，确保在整个数据库中对相似数据使用一致的类型")
        
        # 整合所有建议
        return {
            'storage_optimization': storage_suggestions,
            'query_optimization': query_suggestions,
            'maintenance_recommendations': maintenance_suggestions,
            'schema_improvements': schema_suggestions,
            'priority_recommendations': self._get_priority_recommendations(
                storage_suggestions, query_suggestions, 
                maintenance_suggestions, schema_suggestions
            )
        }
    
    def _get_priority_recommendations(self, storage, query, maintenance, schema):
        """
        获取优先级排序的建议
        
        Args:
            storage (list): 存储优化建议
            query (list): 查询优化建议
            maintenance (list): 维护建议
            schema (list): 架构改进建议
            
        Returns:
            list: 优先级排序的建议
        """
        # 建议加上类型标签
        tagged_suggestions = []
        
        for suggestion in storage:
            tagged_suggestions.append({
                'type': 'storage',
                'suggestion': suggestion,
                'priority': 'high'
            })
        
        for suggestion in query:
            tagged_suggestions.append({
                'type': 'query',
                'suggestion': suggestion,
                'priority': 'high'
            })
        
        for suggestion in maintenance:
            tagged_suggestions.append({
                'type': 'maintenance',
                'suggestion': suggestion,
                'priority': 'medium'
            })
        
        for suggestion in schema:
            tagged_suggestions.append({
                'type': 'schema',
                'suggestion': suggestion,
                'priority': 'medium'
            })
        
        # 按优先级和类型排序
        priority_order = {'high': 0, 'medium': 1, 'low': 2}
        type_order = {'query': 0, 'storage': 1, 'schema': 2, 'maintenance': 3}
        
        return sorted(
            tagged_suggestions, 
            key=lambda x: (priority_order[x['priority']], type_order[x['type']])
        )
