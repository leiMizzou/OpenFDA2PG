"""
数据采样模块，负责从表中提取代表性数据样本进行分析
"""
import logging
import pandas as pd
from tqdm import tqdm

class DataSampler:
    """数据采样器，负责从表中采样数据"""
    
    def __init__(self, db_connector, config):
        """
        初始化数据采样器
        
        Args:
            db_connector (DBConnector): 数据库连接器
            config (Config): 配置对象
        """
        self.db = db_connector
        self.config = config
        self.samples = {}
    
    def sample_tables(self, tables):
        """
        从多个表中采样数据
        
        Args:
            tables (list): 表名列表
            
        Returns:
            dict: 表名到样本DataFrame的映射
        """
        sample_size = self.config.get('analysis.sample_size')
        sample_method = self.config.get('analysis.sample_method')
        
        logging.info(f"开始从{len(tables)}个表中采样数据，每表采样{sample_size}行")
        
        samples = {}
        
        for table_name in tqdm(tables, desc="采样表数据"):
            try:
                df = self.sample_table(table_name, sample_size, sample_method)
                if not df.empty:
                    samples[table_name] = df
                    logging.info(f"表 {table_name} 采样成功，获取了 {len(df)} 行")
                else:
                    logging.warning(f"表 {table_name} 采样返回空DataFrame")
            except Exception as e:
                logging.error(f"采样表 {table_name} 失败: {str(e)}")
        
        self.samples = samples
        return samples
    
    def sample_table(self, table_name, sample_size=None, method=None):
        """
        从单个表中采样数据
        
        Args:
            table_name (str): 表名
            sample_size (int, optional): 采样大小，如果为None则使用配置中的值
            method (str, optional): 采样方法，如果为None则使用配置中的值
            
        Returns:
            DataFrame: 采样数据
        """
        if sample_size is None:
            sample_size = self.config.get('analysis.sample_size')
            
        if method is None:
            method = self.config.get('analysis.sample_method')
        
        # First check if table exists and has data
        try:
            count_query = f"""
                SELECT COUNT(*) FROM {self.db.schema}."{table_name}" LIMIT 1
            """
            result = self.db.execute_query(count_query)
            if not result or result[0]['count'] == 0:
                logging.warning(f"表 {table_name} 是空表或不存在")
                return pd.DataFrame()
        except Exception as e:
            logging.error(f"检查表 {table_name} 失败: {str(e)}")
            # Try to continue with sampling anyway
        
        try:
            # Set a shorter timeout for sampling queries
            self.db.execute_query("SET statement_timeout = '30s'")
            
            df = self.db.get_sample_data(table_name, sample_size=sample_size, method=method)
            
            # Reset timeout
            self.db.execute_query("RESET statement_timeout")
            
            if df.empty:
                # Try alternative approach with direct SQL for small sample
                try:
                    alt_query = f"""
                        SELECT * FROM {self.db.schema}."{table_name}" LIMIT 10
                    """
                    df = self.db.execute_query_df(alt_query)
                    if not df.empty:
                        logging.info(f"使用替代方法成功获取表 {table_name} 的少量样本")
                except Exception as inner_e:
                    logging.warning(f"替代采样方法也失败: {str(inner_e)}")
            
            return df
        except Exception as e:
            logging.error(f"从表 {table_name} 采样失败: {str(e)}")
            # Rollback to clear any pending transactions
            if hasattr(self.db, 'conn') and self.db.conn:
                self.db.conn.rollback()
            return pd.DataFrame()
    
    def get_sample(self, table_name):
        """
        获取表的采样数据
        
        Args:
            table_name (str): 表名
            
        Returns:
            DataFrame: 采样数据，如果表未采样则返回None
        """
        return self.samples.get(table_name)
    
    def get_samples(self):
        """
        获取所有采样数据
        
        Returns:
            dict: 表名到样本DataFrame的映射
        """
        return self.samples