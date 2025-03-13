"""
Schema分析模块，负责分析数据库schema、表结构和关系
"""
import logging
import pandas as pd
from collections import defaultdict

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
        
        # 分析每个表
        tables_info = {}
        for table_name in filtered_tables:
            logging.info(f"分析表: {table_name}")
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
        分析单个表
        
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
                'high_null_columns': len(table_info['high_null_columns'])
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
