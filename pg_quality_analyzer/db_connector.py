"""
数据库连接模块，负责管理PostgreSQL连接和查询执行
"""
import logging
import time
import psycopg2
import pandas as pd
import numpy as np
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
        self.max_retries = 3  # 最大重试次数
        self.retry_delay = 2  # 重试延迟（秒）
        self.is_connected = False
        
    def connect(self):
        """
        连接到PostgreSQL数据库
        
        Returns:
            bool: 连接成功返回True，否则返回False
        """
        db_config = self.config.get('database')
        retry_count = 0
        
        while retry_count < self.max_retries:
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
                self.conn.autocommit = False  # 明确设置非自动提交模式
                self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)
                
                # 测试连接是否有效
                self.cursor.execute("SELECT 1")
                result = self.cursor.fetchone()
                if not result or result.get('?column?', None) != 1:
                    raise Exception("Connection test failed")
                
                # 创建SQLAlchemy引擎，用于pandas查询
                db_url = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
                self.engine = create_engine(db_url)
                
                self.is_connected = True
                logging.info(f"成功连接到数据库 {db_config['dbname']} 在 {db_config['host']}:{db_config['port']}")
                return True
                
            except Exception as e:
                retry_count += 1
                if retry_count >= self.max_retries:
                    logging.error(f"数据库连接失败，已达最大重试次数: {str(e)}")
                    self.is_connected = False
                    return False
                
                logging.warning(f"数据库连接失败，将在{self.retry_delay}秒后重试 ({retry_count}/{self.max_retries}): {str(e)}")
                time.sleep(self.retry_delay)
                # 指数退避增加延迟
                self.retry_delay *= 1.5
    
    def disconnect(self):
        """关闭数据库连接"""
        try:
            if self.cursor:
                self.cursor.close()
                self.cursor = None
                
            if self.conn:
                self.conn.close()
                self.conn = None
                
            if self.engine:
                self.engine.dispose()
                self.engine = None
                
            self.is_connected = False
            logging.info("数据库连接已关闭")
        except Exception as e:
            logging.error(f"关闭数据库连接时出错: {str(e)}")
    
    def check_connection(self):
        """
        检查连接是否有效，如果无效则尝试重新连接
        
        Returns:
            bool: 连接有效返回True，否则返回False
        """
        if not self.is_connected or not self.conn or not self.cursor:
            logging.warning("数据库连接不存在或未初始化，尝试重新连接")
            return self.connect()
        
        try:
            # 检查连接是否还有效
            self.cursor.execute("SELECT 1")
            result = self.cursor.fetchone()
            return result is not None and result.get('?column?', None) == 1
        except Exception as e:
            logging.warning(f"数据库连接已断开，尝试重新连接: {str(e)}")
            # 关闭旧连接
            self.disconnect()
            # 重新连接
            return self.connect()
    
    def execute_query(self, query, params=None, timeout=60, fetch_all=True):
        """
        执行SQL查询并返回结果
        
        Args:
            query (str): SQL查询语句
            params (tuple, optional): 查询参数
            timeout (int, optional): 查询超时秒数
            fetch_all (bool, optional): 是否获取所有结果
            
        Returns:
            list: 查询结果列表
        """
        retry_count = 0
        
        while retry_count < self.max_retries:
            try:
                # 检查连接是否有效
                if not self.check_connection():
                    raise Exception("无法建立数据库连接")
                
                # 设置查询超时
                self.cursor.execute(f"SET statement_timeout = '{timeout * 1000}'")
                
                # 执行查询
                self.cursor.execute(query, params)
                
                # 获取结果
                if fetch_all:
                    result = self.cursor.fetchall()
                else:
                    result = self.cursor.fetchone()
                
                # 重置超时设置
                self.cursor.execute("RESET statement_timeout")
                self.conn.commit()  # 提交事务，确保超时设置生效
                
                return result
                
            except Exception as e:
                retry_count += 1
                logging.error(f"查询执行失败: {str(e)}")
                logging.error(f"查询: {query}")
                
                # 回滚事务以避免 "transaction is aborted" 错误
                if self.conn:
                    try:
                        self.conn.rollback()
                    except:
                        pass
                
                if retry_count >= self.max_retries:
                    logging.error(f"已达最大重试次数，查询失败")
                    return [] if fetch_all else None
                
                # 如果是连接问题，尝试重新连接
                if "connection" in str(e).lower() or "terminating" in str(e).lower():
                    self.check_connection()
                    
                time.sleep(self.retry_delay)
                # 指数退避增加延迟
                self.retry_delay *= 1.5
    
    def execute_query_df(self, query, params=None, timeout=60):
        """
        执行SQL查询并返回pandas DataFrame
        
        Args:
            query (str): SQL查询语句
            params (tuple, optional): 查询参数
            timeout (int, optional): 查询超时秒数
            
        Returns:
            DataFrame: 查询结果DataFrame
        """
        retry_count = 0
        
        while retry_count < self.max_retries:
            try:
                # 检查连接是否有效
                if not self.check_connection():
                    raise Exception("无法建立数据库连接")
                
                # 设置查询超时 (直接执行SQL，不使用cursor)
                if self.engine:
                    with self.engine.connect() as conn:
                        conn.execute(f"SET statement_timeout = '{timeout * 1000}'")
                        
                        # 使用SQLAlchemy引擎执行查询
                        df = pd.read_sql_query(query, conn, params=params)
                        
                        # 重置超时设置
                        conn.execute("RESET statement_timeout")
                        return df
                else:
                    # 降级为直接使用psycopg2连接
                    self.cursor.execute(f"SET statement_timeout = '{timeout * 1000}'")
                    self.conn.commit()  # 确保设置生效
                    
                    df = pd.read_sql_query(query, self.conn, params=params)
                    
                    self.cursor.execute("RESET statement_timeout")
                    self.conn.commit()  # 确保重置生效
                    return df
                    
            except Exception as e:
                retry_count += 1
                logging.error(f"查询执行失败: {str(e)}")
                logging.error(f"查询: {query}")
                
                # 回滚事务以避免 "transaction is aborted" 错误
                if self.conn:
                    try:
                        self.conn.rollback()
                    except:
                        pass
                
                if retry_count >= self.max_retries:
                    logging.error(f"已达最大重试次数，查询失败")
                    return pd.DataFrame()
                
                # 如果是连接问题，尝试重新连接
                if "connection" in str(e).lower() or "terminating" in str(e).lower():
                    self.check_connection()
                    
                time.sleep(self.retry_delay)
                # 指数退避增加延迟
                self.retry_delay *= 1.5
    
    def transaction(self):
        """
        返回事务上下文管理器
        
        Returns:
            Transaction: 事务上下文管理器
        """
        class Transaction:
            def __init__(self, connector):
                self.connector = connector
                self.conn = connector.conn
                
            def __enter__(self):
                # 确保连接有效
                if not self.connector.check_connection():
                    raise Exception("无法建立数据库连接")
                return self.conn
                
            def __exit__(self, exc_type, exc_val, exc_tb):
                if exc_type is not None:
                    logging.info(f"事务出错，回滚: {str(exc_val)}")
                    try:
                        self.conn.rollback()
                        logging.info("事务已回滚")
                    except Exception as e:
                        logging.error(f"回滚事务失败: {str(e)}")
                else:
                    try:
                        self.conn.commit()
                        logging.info("事务已提交")
                    except Exception as e:
                        logging.error(f"提交事务失败: {str(e)}")
                        try:
                            self.conn.rollback()
                        except:
                            pass
                    
        return Transaction(self)
    
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
    
    def get_column_statistics(self, table_name, column_name):
        """
        获取列的统计信息
        
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
            
            # 检查是否是文本类型, 以便区别处理
            is_text = data_type in ('text', 'varchar', 'character varying')
            
            # 计算基本的计数统计信息
            query = sql.SQL("""
                SELECT 
                    COUNT(*) AS total_count,
                    COUNT({}) AS non_null_count,
                    COUNT(*) - COUNT({}) AS null_count
                FROM {}.{}
            """).format(
                sql.Identifier(column_name),
                sql.Identifier(column_name),
                sql.Identifier(self.schema),
                sql.Identifier(table_name)
            )
            
            with self.transaction():
                self.cursor.execute("SET statement_timeout = '30000'")  # 30秒
                result = self.execute_query(query.as_string(self.conn))
                self.cursor.execute("RESET statement_timeout")
            
            if not result:
                return {}
            
            stats = dict(result[0])
            
            # 对文本类型，获取长度统计信息
            if is_text:
                text_query = sql.SQL("""
                    SELECT 
                        MIN(LENGTH({})) AS min_length,
                        MAX(LENGTH({})) AS max_length,
                        AVG(LENGTH({})) AS avg_length,
                        COUNT(DISTINCT {}) AS distinct_count
                    FROM (
                        SELECT {} FROM {}.{} 
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
                    sql.Identifier(column_name)
                )
                
                with self.transaction():
                    self.cursor.execute("SET statement_timeout = '30000'")
                    text_stats = self.execute_query(text_query.as_string(self.conn))
                    self.cursor.execute("RESET statement_timeout")
                
                if text_stats:
                    stats.update(dict(text_stats[0]))
            
            # 根据数据类型执行特定的统计分析
            if data_type in ('integer', 'bigint', 'smallint', 'numeric', 'decimal', 'real', 'double precision'):
                # 数值类型
                num_query = sql.SQL("""
                    SELECT 
                        MIN({}) AS min_value,
                        MAX({}) AS max_value,
                        AVG({}) AS avg_value,
                        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {}) AS median_value,
                        STDDEV({}) AS stddev_value
                    FROM {}.{}
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
                    sql.Identifier(column_name)
                )
                
                with self.transaction():
                    self.cursor.execute("SET statement_timeout = '30000'")
                    num_stats = self.execute_query(num_query.as_string(self.conn))
                    self.cursor.execute("RESET statement_timeout")
                
                if num_stats:
                    stats.update(dict(num_stats[0]))
                    
            elif data_type == 'boolean':
                # 布尔类型特殊处理
                bool_query = sql.SQL("""
                    SELECT 
                        CAST(MIN(CAST({} AS INT)) AS BOOLEAN) AS min_value,
                        CAST(MAX(CAST({} AS INT)) AS BOOLEAN) AS max_value,
                        AVG(CAST({} AS INT)) AS avg_value,
                        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CAST({} AS INT)) AS median_value,
                        STDDEV(CAST({} AS INT)) AS stddev_value
                    FROM {}.{}
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
                    sql.Identifier(column_name)
                )
                
                with self.transaction():
                    self.cursor.execute("SET statement_timeout = '30000'")
                    bool_stats = self.execute_query(bool_query.as_string(self.conn))
                    self.cursor.execute("RESET statement_timeout")
                
                if bool_stats:
                    stats.update(dict(bool_stats[0]))
            
            elif data_type in ('timestamp', 'timestamp without time zone', 'timestamp with time zone', 'date', 'time'):
                # 日期时间类型
                date_query = sql.SQL("""
                    SELECT 
                        MIN({})::TEXT AS min_value,
                        MAX({})::TEXT AS max_value,
                        COUNT(DISTINCT {}) AS distinct_count
                    FROM {}.{}
                    WHERE {} IS NOT NULL
                    LIMIT 10000
                """).format(
                    sql.Identifier(column_name),
                    sql.Identifier(column_name),
                    sql.Identifier(column_name),
                    sql.Identifier(self.schema),
                    sql.Identifier(table_name),
                    sql.Identifier(column_name)
                )
                
                with self.transaction():
                    self.cursor.execute("SET statement_timeout = '30000'")
                    date_stats = self.execute_query(date_query.as_string(self.conn))
                    self.cursor.execute("RESET statement_timeout")
                
                if date_stats:
                    stats.update(dict(date_stats[0]))
            
            # 计算基数率（唯一值比例）- 使用限制行数
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
            
            with self.transaction():
                self.cursor.execute("SET statement_timeout = '30000'")
                distinct_result = self.execute_query(cardinality_query.as_string(self.conn))
                self.cursor.execute("RESET statement_timeout")
            
            if distinct_result and distinct_result[0]['distinct_count'] is not None and stats.get('non_null_count', 0) > 0:
                sample_size = min(stats['non_null_count'], 10000)  # 限制为样本大小或者实际非空值数量
                if sample_size > 0:
                    stats['cardinality_ratio'] = distinct_result[0]['distinct_count'] / sample_size
            
            return stats
                
        except Exception as e:
            logging.error(f"获取列统计信息失败: {str(e)}")
            # 回滚事务以避免 "transaction is aborted" 错误
            if self.conn:
                try:
                    self.conn.rollback()
                except:
                    pass
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
        从表中获取采样数据
        
        Args:
            table_name (str): 表名
            columns (list, optional): 列名列表，如果为None则获取所有列
            sample_size (int, optional): 采样大小
            method (str, optional): 采样方法，'random'或'sequential'
            
        Returns:
            DataFrame: 采样数据
        """
        try:
            # 检查连接是否有效
            if not self.check_connection():
                raise Exception("无法建立数据库连接")
                
            # 构建选择的列
            if columns:
                columns_sql = sql.SQL(', ').join(sql.Identifier(col) for col in columns)
            else:
                columns_sql = sql.SQL('*')
            
            # 首先检查表的大小，以决定是否使用限制的采样方法
            size_query = sql.SQL("""
                SELECT reltuples::bigint AS estimate_rows
                FROM pg_class
                WHERE relname = {}
            """).format(sql.Literal(table_name))
            
            size_result = self.execute_query(size_query.as_string(self.conn))
            estimated_rows = size_result[0]['estimate_rows'] if size_result else 0
            
            # 对于大表使用更高效的采样
            if estimated_rows > 1000000:  # 百万行以上的表
                if method == 'random':
                    query = sql.SQL("""
                        SELECT {}
                        FROM {}.{}
                        TABLESAMPLE SYSTEM(0.1)  
                        LIMIT %s
                    """).format(
                        columns_sql,
                        sql.Identifier(self.schema),
                        sql.Identifier(table_name)
                    )
                else:
                    query = sql.SQL("""
                        SELECT {}
                        FROM {}.{}
                        LIMIT %s
                    """).format(
                        columns_sql,
                        sql.Identifier(self.schema),
                        sql.Identifier(table_name)
                    )
            else:
                # 对于小表使用原来的方法
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
            
            # 使用事务确保语句超时设置生效
            with self.transaction():
                # 设置较长的超时
                self.cursor.execute("SET statement_timeout = '60000'")  # 60秒
                
                # 使用SQLAlchemy引擎执行查询
                if self.engine:
                    df = pd.read_sql(query.as_string(self.conn), self.engine, params=(sample_size,))
                else:
                    df = pd.read_sql_query(query.as_string(self.conn), self.conn, params=(sample_size,))
                
                # 重置超时
                self.cursor.execute("RESET statement_timeout")
                    
            if df.empty:
                logging.warning(f"表 {table_name} 采样返回空DataFrame")
            
            return df
                
        except Exception as e:
            logging.error(f"获取采样数据失败: {str(e)}")
            # 回滚事务以避免 "transaction is aborted" 错误
            if self.conn:
                try:
                    self.conn.rollback()
                except:
                    pass
            return pd.DataFrame()
    
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