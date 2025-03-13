"""
Schema分析模块，负责分析数据库schema、表结构和关系
"""
import logging
import pandas as pd
from collections import defaultdict
from tqdm import tqdm

class SchemaAnalyzer:
    """PostgreSQL数据库Schema分析器"""
    
    def __init__(self, db_connector, config):
        """
        初始化Schema分析器
        
        Args:
            db_connector (DBConnector): 数据库连接器
            config (Config): 配置对象
        """
        self.db = db_connector
        self.config = config
        self.schema = config.get('database.schema')
        self.schema_info = {}
        self.tables_info = {}
        self.relationship_graph = None
        self.large_table_threshold = config.get('analysis.large_table_threshold', 100000)
        self.very_large_table_threshold = config.get('analysis.very_large_table_threshold', 1000000)
    
    def analyze_schema(self):
        """
        分析数据库schema
        
        Returns:
            dict: schema分析结果
        """
        logging.info(f"开始分析schema: {self.schema}")
        
        # 获取表列表
        tables = self.db.get_tables()
        
        # 应用表过滤
        filtered_tables = self._filter_tables(tables)
        
        # 获取表大小信息
        table_sizes = {}
        for table_name in tqdm(filtered_tables, desc="评估表大小"):
            try:
                size_info = self.db.get_table_size_estimate(table_name)
                table_sizes[table_name] = size_info
            except Exception as e:
                logging.error(f"评估表 {table_name} 大小失败: {str(e)}")
        
        # 分析每个表，对大表应用不同的分析策略
        tables_info = {}
        for table_name in tqdm(filtered_tables, desc="分析表结构"):
            logging.info(f"分析表: {table_name}")
            
            # 获取表大小信息
            size_info = table_sizes.get(table_name, {'row_estimate': 0})
            row_estimate = size_info.get('row_estimate', 0)
            
            # 根据表大小选择分析策略
            if row_estimate > self.very_large_table_threshold:
                # 超大表使用轻量级分析
                table_info = self._analyze_very_large_table(table_name, size_info)
                logging.info(f"对超大表 {table_name} ({row_estimate} 行) 完成轻量级分析")
            elif row_estimate > self.large_table_threshold:
                # 大表使用中等级别分析
                table_info = self._analyze_large_table(table_name, size_info)
                logging.info(f"对大表 {table_name} ({row_estimate} 行) 完成中等级别分析")
            else:
                # 普通表使用完整分析
                table_info = self._analyze_table(table_name)
            
            tables_info[table_name] = table_info
        
        # 构建表关系图
        relationship_graph = self._build_relationship_graph(tables_info)
        
        # 构建schema信息
        schema_info = {
            'name': self.schema,
            'table_count': len(filtered_tables),
            'tables': filtered_tables,
            'relationship_count': sum(len(relations) for relations in relationship_graph.values()),
            'tables_with_relationships': len([t for t in relationship_graph if relationship_graph[t]])
        }
        
        self.schema_info = schema_info
        self.tables_info = tables_info
        self.relationship_graph = relationship_graph
        
        return {
            'schema': schema_info,
            'tables': tables_info,
            'relationships': relationship_graph
        }
    
    def _filter_tables(self, tables):
        """
        根据配置过滤表列表
        
        Args:
            tables (list): 原始表列表
            
        Returns:
            list: 过滤后的表列表
        """
        max_tables = self.config.get('analysis.max_tables')
        exclude_tables = self.config.get('analysis.exclude_tables')
        include_tables = self.config.get('analysis.include_tables')
        
        # 首先应用包含过滤
        if include_tables:
            tables = [t for t in tables if t in include_tables]
        
        # 然后应用排除过滤
        if exclude_tables:
            tables = [t for t in tables if t not in exclude_tables]
        
        # 最后应用最大表数量限制
        if max_tables and len(tables) > max_tables:
            tables = tables[:max_tables]
            
        return tables
    
    def _analyze_table(self, table_name):
        """
        分析单个普通表
        
        Args:
            table_name (str): 表名
            
        Returns:
            dict: 表分析结果
        """
        # 获取表基本信息
        table_info = self.db.get_table_info(table_name)
        
        # 获取列信息
        columns = self.db.get_columns(table_name)
        column_info = []
        
        for column in columns:
            # 获取列统计信息
            stats = self.db.get_column_statistics(table_name, column['column_name'])
            
            # 合并列信息和统计信息
            column_data = dict(column)
            column_data['statistics'] = stats
            
            # 计算空值率
            if stats.get('total_count', 0) > 0:
                column_data['null_rate'] = stats.get('null_count', 0) / stats['total_count']
            else:
                column_data['null_rate'] = 0
                
            column_info.append(column_data)
        
        # 获取外键关系
        foreign_keys = self.db.get_foreign_keys(table_name)
        
        # 构建表分析结果
        table_analysis = {
            'name': table_name,
            'info': table_info,
            'columns': column_info,
            'foreign_keys': foreign_keys,
            'column_count': len(column_info)
        }
        
        # 计算高空值率的列
        high_null_columns = []
        null_threshold = self.config.get('quality_checks.null_threshold')
        
        for column in column_info:
            if column['null_rate'] > null_threshold:
                high_null_columns.append({
                    'name': column['column_name'],
                    'null_rate': column['null_rate']
                })
        
        table_analysis['high_null_columns'] = high_null_columns
        
        return table_analysis
    
    def _analyze_large_table(self, table_name, size_info):
        """
        分析大表（10万-100万行）- 使用中等级别分析
        
        Args:
            table_name (str): 表名
            size_info (dict): 表大小信息
            
        Returns:
            dict: 表分析结果
        """
        # 获取表基本信息
        table_info = self.db.get_table_info(table_name)
        
        # 合并大小信息
        table_info.update(size_info)
        
        # 获取列信息 - 不获取详细统计
        columns = self.db.get_columns(table_name)
        column_info = []
        
        # 仅为前10列获取基本统计信息
        priority_columns = columns[:10]
        
        for column in columns:
            column_data = dict(column)
            
            # 只为优先列获取统计信息
            if column in priority_columns:
                try:
                    # 获取简化的列统计信息
                    stats = self.db.get_column_statistics(table_name, column['column_name'])
                    column_data['statistics'] = stats
                    
                    # 计算空值率
                    if stats.get('total_count', 0) > 0:
                        column_data['null_rate'] = stats.get('null_count', 0) / stats['total_count']
                    else:
                        column_data['null_rate'] = 0
                except Exception as e:
                    logging.warning(f"获取大表 {table_name} 列 {column['column_name']} 统计信息失败: {str(e)}")
                    column_data['statistics'] = {}
                    column_data['null_rate'] = 0
            else:
                column_data['statistics'] = {}
                column_data['null_rate'] = 0
                
            column_info.append(column_data)
        
        # 获取外键关系
        foreign_keys = self.db.get_foreign_keys(table_name)
        
        # 构建表分析结果
        table_analysis = {
            'name': table_name,
            'info': table_info,
            'columns': column_info,
            'foreign_keys': foreign_keys,
            'column_count': len(column_info),
            'is_large_table': True
        }
        
        # 计算高空值率的列 - 仅对获取了统计信息的列
        high_null_columns = []
        null_threshold = self.config.get('quality_checks.null_threshold')
        
        for column in column_info:
            if column.get('null_rate', 0) > null_threshold:
                high_null_columns.append({
                    'name': column['column_name'],
                    'null_rate': column['null_rate']
                })
        
        table_analysis['high_null_columns'] = high_null_columns
        
        return table_analysis
    
    def _analyze_very_large_table(self, table_name, size_info):
        """
        分析超大表（>100万行）- 使用轻量级分析
        
        Args:
            table_name (str): 表名
            size_info (dict): 表大小信息
            
        Returns:
            dict: 表分析结果
        """
        # 获取表基本信息 - 不获取索引和约束信息以减少查询
        try:
            query = """
                SELECT 
                    c.reltuples::bigint AS approximate_row_count,
                    pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size,
                    pg_total_relation_size(c.oid) AS total_bytes,
                    obj_description(c.oid) AS description
                FROM 
                    pg_class c
                JOIN 
                    pg_namespace n ON n.oid = c.relnamespace
                WHERE 
                    n.nspname = %s
                    AND c.relname = %s
                    AND c.relkind = 'r'
            """
            result = self.db.execute_query(query, (self.schema, table_name))
            table_info = dict(result[0]) if result else {}
        except Exception as e:
            logging.error(f"获取超大表 {table_name} 基本信息失败: {str(e)}")
            table_info = {}
        
        # 合并大小信息
        table_info.update(size_info)
        
        # 获取列信息 - 不获取统计
        columns = self.db.get_columns(table_name)
        column_info = []
        
        for column in columns:
            column_data = dict(column)
            column_data['statistics'] = {}
            column_data['null_rate'] = 0  # 不计算真实空值率
            column_info.append(column_data)
        
        # 获取外键关系
        foreign_keys = self.db.get_foreign_keys(table_name)
        
        # 构建表分析结果
        table_analysis = {
            'name': table_name,
            'info': table_info,
            'columns': column_info,
            'foreign_keys': foreign_keys,
            'column_count': len(column_info),
            'is_very_large_table': True,
            'high_null_columns': []  # 不分析高空值率列
        }
        
        return table_analysis
    
    def _build_relationship_graph(self, tables_info):
        """
        构建表关系图
        
        Args:
            tables_info (dict): 表信息字典
            
        Returns:
            dict: 关系图
        """
        graph = defaultdict(list)
        
        # 添加所有表作为节点
        for table_name in tables_info:
            if table_name not in graph:
                graph[table_name] = []
        
        # 添加外键关系
        for table_name, table_info in tables_info.items():
            for fk in table_info.get('foreign_keys', []):
                if fk['foreign_table_schema'] == self.schema:  # 只考虑同一schema中的关系
                    relationship = {
                        'from_table': table_name,
                        'from_column': fk['column_name'],
                        'to_table': fk['foreign_table_name'],
                        'to_column': fk['foreign_column_name'],
                        'constraint_name': fk['constraint_name']
                    }
                    
                    graph[table_name].append(relationship)
                    
                    # 添加反向引用关系
                    reverse_relationship = {
                        'from_table': fk['foreign_table_name'],
                        'from_column': fk['foreign_column_name'],
                        'to_table': table_name,
                        'to_column': fk['column_name'],
                        'constraint_name': fk['constraint_name'],
                        'is_reverse': True
                    }
                    
                    graph[fk['foreign_table_name']].append(reverse_relationship)
        
        return dict(graph)
    
    def get_schema_summary(self):
        """
        获取schema摘要信息
        
        Returns:
            DataFrame: schema摘要
        """
        if not self.tables_info:
            return pd.DataFrame()
        
        # 准备表摘要数据
        data = []
        for table_name, table_info in self.tables_info.items():
            row_count = table_info['info'].get('approximate_row_count', 0)
            total_size = table_info['info'].get('total_bytes', 0)
            
            # 计算平均空值率
            null_rates = [col['null_rate'] for col in table_info['columns'] if 'null_rate' in col]
            avg_null_rate = sum(null_rates) / len(null_rates) if null_rates else 0
            
            data.append({
                'table_name': table_name,
                'row_count': row_count,
                'column_count': table_info['column_count'],
                'total_size': total_size,
                'avg_null_rate': avg_null_rate,
                'relationship_count': len(self.relationship_graph.get(table_name, [])),
                'high_null_columns': len(table_info['high_null_columns']),
                'is_large_table': table_info.get('is_large_table', False),
                'is_very_large_table': table_info.get('is_very_large_table', False)
            })
        
        # 创建DataFrame并排序
        df = pd.DataFrame(data)
        if not df.empty:
            df = df.sort_values('row_count', ascending=False)
        
        return df
    
    def get_column_types_summary(self):
        """
        获取列数据类型分布摘要
        
        Returns:
            DataFrame: 数据类型分布
        """
        if not self.tables_info:
            return pd.DataFrame()
        
        # 收集所有列的数据类型
        data_types = []
        for table_info in self.tables_info.values():
            for column in table_info['columns']:
                data_types.append({
                    'data_type': column['data_type'],
                    'column_name': column['column_name'],
                    'table_name': table_info['name']
                })
        
        # 创建DataFrame
        df = pd.DataFrame(data_types)
        
        # 计算每种数据类型的计数
        type_counts = df['data_type'].value_counts().reset_index()
        type_counts.columns = ['data_type', 'count']
        
        return type_counts
        
    def detect_potential_issues(self):
        """
        检测schema中的潜在问题
        
        Returns:
            list: 潜在问题列表
        """
        issues = []
        
        # 检查缺少主键的表
        for table_name, table_info in self.tables_info.items():
            # 跳过超大表，因为可能没有获取索引信息
            if table_info.get('is_very_large_table'):
                continue
                
            has_primary_key = False
            for index in table_info['info'].get('indexes', []):
                if index.get('is_primary'):
                    has_primary_key = True
                    break
            
            if not has_primary_key:
                issues.append({
                    'table': table_name,
                    'issue_type': 'missing_primary_key',
                    'description': f"表 {table_name} 缺少主键"
                })
        
        # 检查没有关系的表
        tables_without_relations = []
        for table_name in self.tables_info:
            relations = self.relationship_graph.get(table_name, [])
            if not relations:
                tables_without_relations.append(table_name)
        
        if tables_without_relations:
            issues.append({
                'tables': tables_without_relations,
                'issue_type': 'isolated_tables',
                'description': f"有 {len(tables_without_relations)} 个表没有与其他表的关系"
            })
        
        # 检查空值率高的表
        high_null_tables = []
        for table_name, table_info in self.tables_info.items():
            if len(table_info['high_null_columns']) > 3:
                high_null_tables.append({
                    'table': table_name,
                    'null_columns': len(table_info['high_null_columns']),
                    'column_count': table_info['column_count']
                })
        
        if high_null_tables:
            issues.append({
                'tables': high_null_tables,
                'issue_type': 'high_null_rate',
                'description': f"有 {len(high_null_tables)} 个表存在多个高空值率列"
            })
        
        return issues