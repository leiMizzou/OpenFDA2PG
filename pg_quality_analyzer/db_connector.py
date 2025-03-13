"""
数据库连接模块，负责管理PostgreSQL连接和查询执行
"""
import logging
import psycopg2
import pandas as pd
import numpy as np
import time
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
from sqlalchemy import create_engine

class DBConnector:
    """PostgreSQL数据库连接器"""
    
    def __init__(self, config):
        """
        初始化数据库连接器
        
        Args:
            config (Config): 配置对象
        """
        self.config = config
        self.conn = None
        self.cursor = None
        self.engine = None
        self.schema = config.get('database.schema')
        self.max_retries = 3
        self.retry_delay = 1  # 初始重试延迟(秒)
    
    def connect(self):
        """
        连接到PostgreSQL数据库
        
        Returns:
            bool: 连接成功返回True，否则返回False
        """
        db_config = self.config.get('database')
        try:
            # 创建psycopg2连接
            self.conn = psycopg2.connect(
                host=db_config['host'],
                port=db_config['port'],
                user=db_config['user'],
                password=db_config['password'],
                dbname=db_config['dbname'],
                # 添加超时设置
                connect_timeout=120,  # 连接超时，单位秒
                options="-c statement_timeout=120000"  # 查询超时，单位毫秒
            )
            self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)
            
            # 创建SQLAlchemy引擎，用于pandas查询
            db_url = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
            self.engine = create_engine(db_url)
            
            logging.info(f"成功连接到数据库 {db_config['dbname']} 在 {db_config['host']}:{db_config['port']}")
            return True
        except Exception as e:
            logging.error(f"数据库连接失败: {str(e)}")
            return False
    
    def disconnect(self):
        """关闭数据库连接"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        if self.engine:
            self.engine.dispose()
        logging.info("数据库连接已关闭")
    
    def check_connection(self):
        """
        检查连接是否有效，如失效则重连
        
        Returns:
            bool: 连接有效返回True，否则返回False
        """
        try:
            # 执行简单查询测试连接
            if self.conn and not self.conn.closed:
                self.cursor.execute("SELECT 1")
                return True
            else:
                return self.connect()
        except Exception:
            # 连接失效，尝试重连
            logging.warning("数据库连接已失效，尝试重连")
            self.disconnect()
            return self.connect()
    
    def set_timeout(self, seconds):
        """
        安全设置查询超时
        
        Args:
            seconds (int): 超时秒数
            
        Returns:
            bool: 设置成功返回True，否则返回False
        """
        for attempt in range(self.max_retries):
            try:
                if not self.check_connection():
                    continue
                
                # 使用SQL命令设置超时，可以在事务内执行
                self.cursor.execute(f"SET statement_timeout = {seconds * 1000}")
                return True
            except Exception as e:
                logging.warning(f"设置超时失败(尝试{attempt+1}/{self.max_retries}): {str(e)}")
                time.sleep(self.retry_delay)  # 延迟重试
        
        logging.error("设置查询超时失败")
        return False

    def reset_timeout(self):
        """
        重置查询超时设置
        
        Returns:
            bool: 重置成功返回True，否则返回False
        """
        for attempt in range(self.max_retries):
            try:
                if not self.check_connection():
                    continue
                
                # 使用SQL命令重置超时，可以在事务内执行
                self.cursor.execute("SET statement_timeout = 0")
                return True
            except Exception as e:
                logging.warning(f"重置超时失败(尝试{attempt+1}/{self.max_retries}): {str(e)}")
                time.sleep(self.retry_delay)  # 延迟重试
        
        logging.error("重置查询超时失败")
        return False
    
    def execute_query(self, query, params=None, timeout=60, max_retries=3):
        """
        执行SQL查询并返回结果
        
        Args:
            query (str): SQL查询语句
            params (tuple, optional): 查询参数
            timeout (int, optional): 查询超时秒数
            max_retries (int, optional): 最大重试次数
            
        Returns:
            list: 查询结果列表
        """
        # 使用指数退避重试策略
        retry_count = 0
        retry_delay = self.retry_delay
        
        while retry_count < max_retries:
            try:
                # 先检查连接
                if not self.check_connection():
                    retry_count += 1
                    retry_delay *= 2  # 增加重试延迟
                    logging.warning(f"连接检查失败，重试({retry_count}/{max_retries})")
                    time.sleep(retry_delay)
                    continue
                
                # 设置超时
                if not self.set_timeout(timeout):
                    retry_count += 1
                    retry_delay *= 2
                    logging.warning(f"设置超时失败，重试({retry_count}/{max_retries})")
                    time.sleep(retry_delay)
                    continue
                
                # 执行查询
                self.cursor.execute(query, params)
                result = self.cursor.fetchall()
                
                # 重置超时
                self.reset_timeout()
                
                return result
            except Exception as e:
                retry_count += 1
                # 记录错误并回滚
                logging.error(f"查询执行失败: {str(e)}")
                logging.error(f"查询: {query}")
                
                # 回滚事务以避免 "transaction is aborted" 错误
                if self.conn:
                    self.conn.rollback()
                
                if retry_count < max_retries:
                    # 增加延迟并重试
                    logging.info(f"将在 {retry_delay}秒后重试 ({retry_count}/{max_retries})")
                    time.sleep(retry_delay)
                    retry_delay *= 2  # 指数退避
                else:
                    logging.error("已达最大重试次数，查询失败")
                    break
        
        return []
    
    def execute_query_df(self, query, params=None, timeout=60, max_retries=3):
        """
        执行SQL查询并返回pandas DataFrame
        
        Args:
            query (str): SQL查询语句
            params (tuple, optional): 查询参数
            timeout (int, optional): 查询超时秒数
            max_retries (int, optional): 最大重试次数
            
        Returns:
            DataFrame: 查询结果DataFrame
        """
        # 使用指数退避重试策略
        retry_count = 0
        retry_delay = self.retry_delay
        
        while retry_count < max_retries:
            try:
                # 先检查连接
                if not self.check_connection():
                    retry_count += 1
                    retry_delay *= 2
                    logging.warning(f"连接检查失败，重试({retry_count}/{max_retries})")
                    time.sleep(retry_delay)
                    continue
                
                # 设置超时（使用连接参数设置）
                if timeout > 0:
                    self.set_timeout(timeout)
                
                # 使用SQLAlchemy引擎执行查询
                if self.engine:
                    df = pd.read_sql_query(query, self.engine, params=params)
                else:
                    # 降级为直接使用psycopg2连接
                    df = pd.read_sql_query(query, self.conn, params=params)
                
                # 重置超时
                if timeout > 0:
                    self.reset_timeout()
                
                return df
            except Exception as e:
                retry_count += 1
                # 记录错误并回滚
                logging.error(f"查询执行失败: {str(e)}")
                logging.error(f"查询: {query}")
                
                # 回滚事务以避免 "transaction is aborted" 错误
                if self.conn:
                    self.conn.rollback()
                
                if retry_count < max_retries:
                    # 增加延迟并重试
                    logging.info(f"将在 {retry_delay}秒后重试 ({retry_count}/{max_retries})")
                    time.sleep(retry_delay)
                    retry_delay *= 2  # 指数退避
                else:
                    logging.error("已达最大重试次数，查询失败")
                    break
        
        return pd.DataFrame()
    
    def transaction(self):
        """
        返回事务上下文管理器
        
        Returns:
            Transaction: 事务上下文管理器
        """
        class Transaction:
            def __init__(self, conn):
                self.conn = conn
                
            def __enter__(self):
                return self.conn
                
            def __exit__(self, exc_type, exc_val, exc_tb):
                if exc_type is not None:
                    self.conn.rollback()
                    logging.info("事务已回滚")
                else:
                    self.conn.commit()
                    logging.info("事务已提交")
                    
        return Transaction(self.conn)
    
    def get_tables(self):
        """
        获取指定schema中的所有表
        
        Returns:
            list: 表名列表
        """
        query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = %s 
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """
        result = self.execute_query(query, (self.schema,))
        return [record['table_name'] for record in result]
    
    def get_table_size_estimate(self, table_name):
        """
        获取表大小估计
        
        Args:
            table_name (str): 表名
            
        Returns:
            dict: 包含行数和大小信息的字典
        """
        query = """
            SELECT 
                reltuples::bigint AS row_estimate,
                pg_size_pretty(pg_total_relation_size(%s)) AS total_size,
                pg_total_relation_size(%s) AS size_bytes
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s AND c.relname = %s
        """
        full_table_name = f"{self.schema}.{table_name}"
        result = self.execute_query(query, (full_table_name, full_table_name, self.schema, table_name))
        
        if result:
            return {
                'row_estimate': result[0]['row_estimate'],
                'total_size': result[0]['total_size'],
                'size_bytes': result[0]['size_bytes']
            }
        return {
            'row_estimate': 0,
            'total_size': '0 bytes',
            'size_bytes': 0
        }

    def get_table_info(self, table_name):
        """
        获取表的详细信息
        
        Args:
            table_name (str): 表名
            
        Returns:
            dict: 表信息
        """
        # 获取表基本信息
        query = """
            SELECT 
                c.reltuples::bigint AS approximate_row_count,
                pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size,
                pg_size_pretty(pg_relation_size(c.oid)) AS table_size,
                pg_size_pretty(pg_total_relation_size(c.oid) - pg_relation_size(c.oid)) AS index_size,
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
        result = self.execute_query(query, (self.schema, table_name))
        if not result:
            return {}
        
        table_info = dict(result[0])
        
        # 获取索引信息
        query = """
            SELECT
                i.relname AS index_name,
                array_to_string(array_agg(a.attname ORDER BY k.indnatts), ', ') AS column_names,
                ix.indisunique AS is_unique,
                ix.indisprimary AS is_primary
            FROM
                pg_class t
            JOIN
                pg_namespace n ON n.oid = t.relnamespace
            JOIN
                pg_index ix ON t.oid = ix.indrelid
            JOIN
                pg_class i ON i.oid = ix.indexrelid
            LEFT JOIN
                pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
            LEFT JOIN
                pg_catalog.pg_constraint c ON c.conindid = ix.indexrelid
            CROSS JOIN
                LATERAL unnest(ix.indkey) WITH ORDINALITY AS k(key, indnatts)
            WHERE
                t.relkind = 'r'
                AND n.nspname = %s
                AND t.relname = %s
            GROUP BY
                i.relname, ix.indisunique, ix.indisprimary
            ORDER BY
                i.relname
        """
        indexes = self.execute_query(query, (self.schema, table_name))
        table_info['indexes'] = indexes
        
        # 获取约束信息 - 使用简化的查询
        query = """
            SELECT
                c.conname AS constraint_name,
                c.contype AS constraint_type,
                pg_get_constraintdef(c.oid) AS definition
            FROM
                pg_constraint c
            JOIN
                pg_namespace n ON n.oid = c.connamespace
            JOIN
                pg_class t ON t.oid = c.conrelid
            WHERE
                n.nspname = %s
                AND t.relname = %s
            ORDER BY
                c.contype, c.conname
        """
        constraints = self.execute_query(query, (self.schema, table_name))
        table_info['constraints'] = constraints
        
        return table_info

    def get_columns(self, table_name):
        """
        获取表的列信息
        
        Args:
            table_name (str): 表名
            
        Returns:
            list: 列信息列表
        """
        query = """
            SELECT 
                c.column_name,
                c.data_type,
                c.character_maximum_length,
                c.numeric_precision,
                c.numeric_scale,
                c.is_nullable,
                c.column_default,
                pgd.description AS column_description,
                c.ordinal_position
            FROM 
                information_schema.columns c
            LEFT JOIN 
                pg_catalog.pg_statio_all_tables st ON st.schemaname = c.table_schema AND st.relname = c.table_name
            LEFT JOIN 
                pg_catalog.pg_description pgd ON pgd.objoid = st.relid AND pgd.objsubid = c.ordinal_position
            WHERE 
                c.table_schema = %s 
                AND c.table_name = %s
            ORDER BY 
                c.ordinal_position
        """
        return self.execute_query(query, (self.schema, table_name))
    
    def get_primary_key(self, table_name):
        """
        获取表的主键
        
        Args:
            table_name (str): 表名
            
        Returns:
            list: 主键列名列表
        """
        query = """
            SELECT a.attname as column_name
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = %s::regclass
            AND i.indisprimary
        """
        full_table_name = f"{self.schema}.{table_name}"
        result = self.execute_query(query, (full_table_name,))
        
        return [col['column_name'] for col in result]
        
    def get_column_statistics(self, table_name, column_name):
        """
        获取列的统计信息 - 优化版本，适用于大表
        
        Args:
            table_name (str): 表名
            column_name (str): 列名
            
        Returns:
            dict: 统计信息
        """
        try:
            # 首先获取列的数据类型
            column_info = self.get_column_type(table_name, column_name)
            data_type = column_info.get('data_type', '').lower()
            
            # 获取表大小估计
            size_info = self.get_table_size_estimate(table_name)
            row_estimate = size_info['row_estimate']
            
            # 对大表使用不同的采样策略
            sample_clause = ""
            if row_estimate > 100000:
                sample_percent = min(10.0, 1000000 / row_estimate * 100)
                sample_clause = f"TABLESAMPLE SYSTEM({sample_percent})"
                logging.info(f"表 {table_name} 较大 ({row_estimate} 行)，使用 {sample_percent:.2f}% 采样")
            
            # 计算基本的计数统计信息
            count_query = sql.SQL("""
                SELECT 
                    COUNT(*) AS total_count,
                    COUNT({}) AS non_null_count,
                    COUNT(*) - COUNT({}) AS null_count
                FROM {}.{} {}
            """).format(
                sql.Identifier(column_name),
                sql.Identifier(column_name),
                sql.Identifier(self.schema),
                sql.Identifier(table_name),
                sql.SQL(sample_clause)
            )
            
            # 使用更短的超时
            self.set_timeout(15)  # 15秒
            result = self.execute_query(count_query.as_string(self.conn))
            self.reset_timeout()
            
            if not result:
                return {}
            
            stats = dict(result[0])
            
            # 检查是否是文本类型
            is_text = data_type in ('text', 'varchar', 'character varying')
            
            # 对文本类型，获取长度统计信息
            if is_text:
                text_query = sql.SQL("""
                    SELECT 
                        MIN(LENGTH({})) AS min_length,
                        MAX(LENGTH({})) AS max_length,
                        AVG(LENGTH({})) AS avg_length,
                        COUNT(DISTINCT {}) AS distinct_count
                    FROM (
                        SELECT {} FROM {}.{} {}
                        WHERE {} IS NOT NULL
                        LIMIT 1000
                    ) AS sample
                """).format(
                    sql.Identifier(column_name),
                    sql.Identifier(column_name),
                    sql.Identifier(column_name),
                    sql.Identifier(column_name),
                    sql.Identifier(column_name),
                    sql.Identifier(self.schema),
                    sql.Identifier(table_name),
                    sql.SQL(sample_clause),
                    sql.Identifier(column_name)
                )
                
                self.set_timeout(15)
                text_stats = self.execute_query(text_query.as_string(self.conn))
                self.reset_timeout()
                
                if text_stats:
                    stats.update(dict(text_stats[0]))
            
            # 根据数据类型执行特定的统计分析
            if data_type in ('integer', 'bigint', 'smallint', 'numeric', 'decimal', 'real', 'double precision'):
                # 对于大表，使用近似统计
                if row_estimate > 100000:
                    num_query = sql.SQL("""
                        SELECT 
                            MIN({}) AS min_value,
                            MAX({}) AS max_value,
                            AVG({}) AS avg_value,
                            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {}) AS median_value
                        FROM {}.{} {}
                        WHERE {} IS NOT NULL
                    """).format(
                        sql.Identifier(column_name),
                        sql.Identifier(column_name),
                        sql.Identifier(column_name),
                        sql.Identifier(column_name),
                        sql.Identifier(self.schema),
                        sql.Identifier(table_name),
                        sql.SQL(sample_clause),
                        sql.Identifier(column_name)
                    )
                else:
                    # 对于小表，使用完整统计
                    num_query = sql.SQL("""
                        SELECT 
                            MIN({}) AS min_value,
                            MAX({}) AS max_value,
                            AVG({}) AS avg_value,
                            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {}) AS median_value,
                            STDDEV({}) AS stddev_value
                        FROM {}.{} {}
                        WHERE {} IS NOT NULL
                        LIMIT 10000
                    """).format(
                        sql.Identifier(column_name),
                        sql.Identifier(column_name),
                        sql.Identifier(column_name),
                        sql.Identifier(column_name),
                        sql.Identifier(column_name),
                        sql.Identifier(self.schema),
                        sql.Identifier(table_name),
                        sql.SQL(sample_clause),
                        sql.Identifier(column_name)
                    )
                
                self.set_timeout(20)
                num_stats = self.execute_query(num_query.as_string(self.conn))
                self.reset_timeout()
                
                if num_stats:
                    stats.update(dict(num_stats[0]))
            
            # 计算基数率（唯一值比例）- 使用限制行数或TABLESAMPLE
            if row_estimate > 100000:
                # 对于大表，使用近似唯一值计数
                cardinality_query = sql.SQL("""
                    SELECT COUNT(DISTINCT {}) AS distinct_count
                    FROM {}.{} {}
                    WHERE {} IS NOT NULL
                """).format(
                    sql.Identifier(column_name),
                    sql.Identifier(self.schema),
                    sql.Identifier(table_name),
                    sql.SQL(sample_clause),
                    sql.Identifier(column_name)
                )
            else:
                # 对于小表，使用完整唯一值计数
                cardinality_query = sql.SQL("""
                    SELECT COUNT(DISTINCT {}) AS distinct_count
                    FROM (
                        SELECT {} FROM {}.{} 
                        WHERE {} IS NOT NULL
                        LIMIT 10000
                    ) AS limited_data
                """).format(
                    sql.Identifier(column_name),
                    sql.Identifier(column_name),
                    sql.Identifier(self.schema),
                    sql.Identifier(table_name),
                    sql.Identifier(column_name)
                )
            
            self.set_timeout(20)
            distinct_result = self.execute_query(cardinality_query.as_string(self.conn))
            self.reset_timeout()
            
            if distinct_result and distinct_result[0]['distinct_count'] is not None and stats.get('non_null_count', 0) > 0:
                sample_size = min(stats['non_null_count'], 10000)  # 限制为样本大小或者实际非空值数量
                if sample_size > 0:
                    stats['cardinality_ratio'] = distinct_result[0]['distinct_count'] / sample_size
            
            return stats
                
        except Exception as e:
            logging.error(f"获取列统计信息失败: {str(e)}")
            # 回滚事务以避免 "transaction is aborted" 错误
            if self.conn:
                self.conn.rollback()
            return {}

    def get_column_type(self, table_name, column_name):
        """
        获取列的数据类型
        
        Args:
            table_name (str): 表名
            column_name (str): 列名
            
        Returns:
            dict: 列类型信息
        """
        query = """
            SELECT 
                data_type,
                character_maximum_length,
                numeric_precision,
                numeric_scale,
                is_nullable
            FROM 
                information_schema.columns 
            WHERE 
                table_schema = %s 
                AND table_name = %s 
                AND column_name = %s
        """
        result = self.execute_query(query, (self.schema, table_name, column_name))
        return dict(result[0]) if result else {}
    
    def get_sample_data(self, table_name, columns=None, sample_size=1000, method='random'):
        """
        从表中获取采样数据，针对大表优化
        
        Args:
            table_name (str): 表名
            columns (list, optional): 列名列表，如果为None则获取所有列
            sample_size (int, optional): 采样大小
            method (str, optional): 采样方法，'random', 'sequential', 'system'
            
        Returns:
            DataFrame: 采样数据
        """
        try:
            # 获取表大小信息
            size_info = self.get_table_size_estimate(table_name)
            row_estimate = size_info['row_estimate']
            
            # 针对大小确定采样策略
            if row_estimate > 1000000:  # 超过100万行
                return self._sample_very_large_table(table_name, columns, sample_size, method)
            elif row_estimate > 100000:  # 超过10万行
                return self._sample_large_table(table_name, columns, sample_size, method)
            else:
                return self._sample_regular_table(table_name, columns, sample_size, method)
                
        except Exception as e:
            logging.error(f"获取采样数据失败: {str(e)}")
            # 回滚事务以避免 "transaction is aborted" 错误
            if self.conn:
                self.conn.rollback()
            return pd.DataFrame()
    
    def _sample_very_large_table(self, table_name, columns=None, sample_size=100, method='system'):
        """
        从超大表中获取采样数据
        
        Args:
            table_name (str): 表名
            columns (list, optional): 列名列表
            sample_size (int, optional): 采样大小
            method (str, optional): 采样方法
            
        Returns:
            DataFrame: 采样数据
        """
        logging.info(f"使用超大表采样策略采样表 {table_name}")
        
        # 确定采样百分比，确保不会返回太多行
        size_info = self.get_table_size_estimate(table_name)
        row_estimate = size_info['row_estimate']
        
        # 根据表大小计算采样百分比，确保获取合理数量的样本
        sample_percent = min(0.1, (sample_size / row_estimate) * 100)
        logging.info(f"表 {table_name} 估计有 {row_estimate} 行，使用 {sample_percent:.4f}% 系统采样")
        
        # 构建选择的列
        if columns:
            columns_sql = sql.SQL(', ').join(sql.Identifier(col) for col in columns)
        else:
            columns_sql = sql.SQL('*')
        
        # 使用TABLESAMPLE系统采样
        query = sql.SQL("""
            SELECT {}
            FROM {}.{}
            TABLESAMPLE SYSTEM({})
            LIMIT %s
        """).format(
            columns_sql,
            sql.Identifier(self.schema),
            sql.Identifier(table_name),
            sql.SQL(str(sample_percent))
        )
        
        # 使用较短的超时并执行查询
        return self.execute_query_df(query.as_string(self.conn), params=(sample_size,), timeout=30)
    
    def _sample_large_table(self, table_name, columns=None, sample_size=100, method='system'):
        """
        从大表中获取采样数据
        
        Args:
            table_name (str): 表名
            columns (list, optional): 列名列表
            sample_size (int, optional): 采样大小
            method (str, optional): 采样方法
            
        Returns:
            DataFrame: 采样数据
        """
        logging.info(f"使用大表采样策略采样表 {table_name}")
        
        # 构建选择的列
        if columns:
            columns_sql = sql.SQL(', ').join(sql.Identifier(col) for col in columns)
        else:
            columns_sql = sql.SQL('*')
        
        if method == 'system':
            # 使用TABLESAMPLE系统采样
            query = sql.SQL("""
                SELECT {}
                FROM {}.{}
                TABLESAMPLE SYSTEM(1)
                LIMIT %s
            """).format(
                columns_sql,
                sql.Identifier(self.schema),
                sql.Identifier(table_name)
            )
        elif method == 'random':
            # 随机采样
            query = sql.SQL("""
                SELECT {}
                FROM {}.{}
                ORDER BY random()
                LIMIT %s
            """).format(
                columns_sql,
                sql.Identifier(self.schema),
                sql.Identifier(table_name)
            )
        else:
            # 顺序采样
            query = sql.SQL("""
                SELECT {}
                FROM {}.{}
                LIMIT %s
            """).format(
                columns_sql,
                sql.Identifier(self.schema),
                sql.Identifier(table_name)
            )
        
        # 使用较短的超时并执行查询
        return self.execute_query_df(query.as_string(self.conn), params=(sample_size,), timeout=30)
    
    def _sample_regular_table(self, table_name, columns=None, sample_size=1000, method='random'):
        """
        从普通大小的表中获取采样数据
        
        Args:
            table_name (str): 表名
            columns (list, optional): 列名列表
            sample_size (int, optional): 采样大小
            method (str, optional): 采样方法
            
        Returns:
            DataFrame: 采样数据
        """
        # 构建选择的列
        if columns:
            columns_sql = sql.SQL(', ').join(sql.Identifier(col) for col in columns)
        else:
            columns_sql = sql.SQL('*')
        
        # 对于普通表使用原来的方法
        if method == 'random':
            query = sql.SQL("""
                SELECT {}
                FROM {}.{}
                ORDER BY RANDOM()
                LIMIT %s
            """).format(
                columns_sql,
                sql.Identifier(self.schema),
                sql.Identifier(table_name)
            )
        else:  # 顺序采样
            query = sql.SQL("""
                SELECT {}
                FROM {}.{}
                LIMIT %s
            """).format(
                columns_sql,
                sql.Identifier(self.schema),
                sql.Identifier(table_name)
            )
        
        # 使用较短的超时
        self.set_timeout(30)  # 30秒
        
        # 使用SQLAlchemy引擎执行查询
        if self.engine:
            df = pd.read_sql(query.as_string(self.conn), self.engine, params=(sample_size,))
        else:
            # 降级为直接使用psycopg2连接
            df = pd.read_sql_query(query.as_string(self.conn), self.conn, params=(sample_size,))
        
        # 重置超时
        self.reset_timeout()
                
        if df.empty:
            logging.warning(f"表 {table_name} 采样返回空DataFrame")
        return df
        
    def get_foreign_keys(self, table_name):
        """
        获取表的外键关系
        
        Args:
            table_name (str): 表名
            
        Returns:
            list: 外键关系列表
        """
        query = """
            SELECT
                tc.constraint_name,
                kcu.column_name,
                ccu.table_schema AS foreign_table_schema,
                ccu.table_name AS foreign_table_name,
                ccu.column_name AS foreign_column_name
            FROM
                information_schema.table_constraints tc
            JOIN
                information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            JOIN
                information_schema.constraint_column_usage ccu ON ccu.constraint_name = tc.constraint_name
                AND ccu.table_schema = tc.table_schema
            WHERE
                tc.constraint_type = 'FOREIGN KEY'
                AND tc.table_schema = %s
                AND tc.table_name = %s
        """
        return self.execute_query(query, (self.schema, table_name))