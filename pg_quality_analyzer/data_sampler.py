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
        
        # 获取表大小阈值配置
        self.large_table_threshold = config.get('analysis.large_table_threshold', 100000)
        self.very_large_table_threshold = config.get('analysis.very_large_table_threshold', 1000000)
        self.large_table_sample_percent = config.get('analysis.large_table_sample_percent', 1.0)
        self.very_large_table_sample_percent = config.get('analysis.very_large_table_sample_percent', 0.1)
    
    def filter_tables_by_size(self, tables, max_rows=None):
        """
        根据表大小过滤表列表
        
        Args:
            tables (list): 表名列表
            max_rows (int, optional): 表行数上限，如果为None则使用配置中的值
            
        Returns:
            tuple: (表名到大小信息的映射, 过滤后的表列表, 被排除的表列表)
        """
        if max_rows is None:
            max_rows = self.config.get('analysis.max_table_rows', 10000000)
        
        table_sizes = {}
        filtered_tables = []
        excluded_tables = []
        
        logging.info(f"正在评估 {len(tables)} 个表的大小...")
        for table_name in tqdm(tables, desc="评估表大小"):
            try:
                # 获取表大小估计
                size_info = self.db.get_table_size_estimate(table_name)
                row_estimate = size_info['row_estimate']
                
                # 存储大小信息
                table_sizes[table_name] = size_info
                
                # 根据大小过滤
                if row_estimate <= max_rows:
                    filtered_tables.append(table_name)
                else:
                    excluded_tables.append({
                        'name': table_name,
                        'row_estimate': row_estimate,
                        'size': size_info['total_size']
                    })
                    logging.warning(f"表 {table_name} 太大 ({row_estimate} 行, {size_info['total_size']}), 将使用轻量级分析或跳过")
            except Exception as e:
                logging.error(f"评估表 {table_name} 大小失败: {str(e)}")
                # 如果无法评估大小，保守起见将其添加到过滤后的列表中
                filtered_tables.append(table_name)
        
        logging.info(f"过滤表完成: {len(filtered_tables)}/{len(tables)} 个表将被完整分析")
        
        return table_sizes, filtered_tables, excluded_tables
    
    def sample_tables(self, tables):
        """
        从多个表中采样数据，针对表大小采用不同策略
        
        Args:
            tables (list): 表名列表
            
        Returns:
            dict: 表名到样本DataFrame的映射
        """
        # 首先过滤过大的表
        table_sizes, filtered_tables, excluded_tables = self.filter_tables_by_size(tables)
        
        # 获取配置
        sample_size = self.config.get('analysis.sample_size')
        sample_method = self.config.get('analysis.sample_method')
        
        logging.info(f"开始从{len(filtered_tables)}个表中采样数据，每表采样{sample_size}行")
        
        samples = {}
        
        # 对过滤后的表采样
        for table_name in tqdm(filtered_tables, desc="采样表数据"):
            try:
                # 获取表大小信息
                size_info = table_sizes.get(table_name)
                if size_info:
                    row_estimate = size_info['row_estimate']
                else:
                    # 如果没有大小信息，再次获取
                    size_info = self.db.get_table_size_estimate(table_name)
                    row_estimate = size_info['row_estimate']
                
                # 根据表大小确定采样策略
                if row_estimate > self.very_large_table_threshold:
                    # 超大表使用系统采样
                    method = 'system'
                    actual_sample_size = min(sample_size, 100)  # 限制样本大小
                    logging.info(f"表 {table_name} 非常大 ({row_estimate} 行)，使用系统采样，限制为 {actual_sample_size} 行")
                    
                    df = self.sample_table(table_name, actual_sample_size, method)
                elif row_estimate > self.large_table_threshold:
                    # 大表使用系统采样
                    method = 'system'
                    actual_sample_size = min(sample_size, 500)  # 适当限制样本大小
                    logging.info(f"表 {table_name} 较大 ({row_estimate} 行)，使用系统采样，限制为 {actual_sample_size} 行")
                    
                    df = self.sample_table(table_name, actual_sample_size, method)
                else:
                    # 普通表使用配置的方法
                    df = self.sample_table(table_name, sample_size, sample_method)
                
                if not df.empty:
                    samples[table_name] = df
                    logging.info(f"表 {table_name} 采样成功，获取了 {len(df)} 行")
                else:
                    logging.warning(f"表 {table_name} 采样返回空DataFrame")
            except Exception as e:
                logging.error(f"采样表 {table_name} 失败: {str(e)}")
        
        # 对排除的超大表使用轻量级采样
        for table_info in excluded_tables:
            table_name = table_info['name']
            logging.info(f"对超大表 {table_name} ({table_info['row_estimate']} 行) 进行轻量级采样")
            
            try:
                # 使用非常有限的系统采样
                micro_sample_size = min(50, sample_size)
                df = self.db.get_sample_data(table_name, sample_size=micro_sample_size, method='system')
                
                if not df.empty:
                    samples[table_name] = df
                    logging.info(f"超大表 {table_name} 轻量级采样成功，获取了 {len(df)} 行")
                else:
                    logging.warning(f"超大表 {table_name} 轻量级采样返回空DataFrame")
            except Exception as e:
                logging.error(f"轻量级采样超大表 {table_name} 失败: {str(e)}")
        
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
            # 获取表大小信息
            size_info = self.db.get_table_size_estimate(table_name)
            row_estimate = size_info['row_estimate']
            
            if row_estimate == 0:
                logging.warning(f"表 {table_name} 是空表或不存在")
                return pd.DataFrame()
            
            # 对于大表，使用不同的采样方法
            if method != 'system' and row_estimate > self.large_table_threshold:
                method = 'system'
                logging.info(f"表 {table_name} 较大 ({row_estimate} 行)，切换为系统采样")
        except Exception as e:
            logging.error(f"检查表 {table_name} 失败: {str(e)}")
            # Try to continue with sampling anyway
        
        try:
            # 使用改进的get_sample_data方法
            df = self.db.get_sample_data(table_name, sample_size=sample_size, method=method)
            
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
    
    def sample_large_table_in_chunks(self, table_name, chunk_size=10000, max_chunks=5):
        """
        分块采样大表，适用于需要扫描整个表的情况
        
        Args:
            table_name (str): 表名
            chunk_size (int): 每个数据块的大小
            max_chunks (int): 最大数据块数量
            
        Returns:
            DataFrame: 合并后的采样数据
        """
        logging.info(f"开始分块采样表 {table_name}, 块大小: {chunk_size}, 最大块数: {max_chunks}")
        
        # 获取主键或唯一索引
        primary_keys = self.db.get_primary_key(table_name)
        
        if not primary_keys:
            logging.warning(f"表 {table_name} 没有主键，使用标准采样方法")
            return self.sample_table(table_name)
        
        primary_key = primary_keys[0]  # 使用第一个主键列
        
        # 分块获取数据
        chunks = []
        offset = 0
        
        for i in range(max_chunks):
            logging.info(f"正在获取表 {table_name} 的第 {i+1} 块数据...")
            
            try:
                query = f"""
                    SELECT * FROM {self.db.schema}."{table_name}"
                    ORDER BY {primary_key}
                    LIMIT {chunk_size} OFFSET {offset}
                """
                
                chunk_df = self.db.execute_query_df(query, timeout=30)
                
                if chunk_df.empty:
                    logging.info(f"表 {table_name} 已无更多数据")
                    break
                
                chunks.append(chunk_df)
                logging.info(f"获取到表 {table_name} 的第 {i+1} 块数据: {len(chunk_df)} 行")
                
                offset += chunk_size
                
                # 如果获取的行数小于块大小，表示已到达表末尾
                if len(chunk_df) < chunk_size:
                    break
                
            except Exception as e:
                logging.error(f"获取表 {table_name} 的第 {i+1} 块数据失败: {str(e)}")
                break
        
        # 合并所有块
        if not chunks:
            logging.warning(f"表 {table_name} 没有获取到任何数据")
            return pd.DataFrame()
        
        combined_df = pd.concat(chunks, ignore_index=True)
        logging.info(f"表 {table_name} 分块采样完成，总共 {len(combined_df)} 行")
        
        return combined_df
    
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
    
    def analyze_excluded_tables(self, excluded_tables):
        """
        分析被排除的超大表（使用轻量级分析）
        
        Args:
            excluded_tables (list): 超大表信息列表
            
        Returns:
            dict: 表名到轻量级分析结果的映射
        """
        results = {}
        
        for table_info in excluded_tables:
            table_name = table_info['name']
            logging.info(f"执行超大表 {table_name} 的轻量级分析")
            
            try:
                # 获取表结构信息
                table_metadata = self.db.get_table_info(table_name)
                columns = self.db.get_columns(table_name)
                
                # 获取小样本用于基本质量检查 - 增加样本大小
                sample_size = 100  # 增加样本大小以提供更好的分析
                small_sample = self.db.get_sample_data(
                    table_name, 
                    sample_size=sample_size, 
                    method='system'
                )
                
                if not small_sample.empty:
                    logging.info(f"成功获取超大表 {table_name} 的 {len(small_sample)} 行样本")
                else:
                    logging.warning(f"超大表 {table_name} 采样返回空DataFrame，尝试备用方法")
                    # 备用方法：使用直接SQL查询获取更小样本
                    try:
                        backup_query = f"""
                            SELECT * FROM {self.db.schema}."{table_name}" LIMIT 20
                        """
                        small_sample = self.db.execute_query_df(backup_query, timeout=10)
                        if not small_sample.empty:
                            logging.info(f"使用备用方法成功获取表 {table_name} 的 {len(small_sample)} 行样本")
                    except Exception as inner_e:
                        logging.warning(f"备用采样方法也失败: {str(inner_e)}")
                
                # 创建轻量级分析结果
                results[table_name] = {
                    'is_large_table': True,
                    'row_estimate': table_info['row_estimate'],
                    'size': table_info['size'],
                    'metadata': table_metadata,
                    'columns': columns,
                    'sample_size': len(small_sample),
                    'sample': small_sample
                }
                
                # 添加列统计信息 (限于基本信息)
                if not small_sample.empty:
                    column_stats = {}
                    priority_columns = [col['column_name'] for col in columns[:5]]  # 优先分析前5列
                    
                    for column_name in small_sample.columns:
                        if column_name in priority_columns:
                            try:
                                # 获取简化的列统计信息
                                stats = self._calculate_basic_stats(small_sample[column_name])
                                column_stats[column_name] = stats
                            except Exception as e:
                                logging.warning(f"计算列 {column_name} 基本统计信息失败: {str(e)}")
                    
                    results[table_name]['column_stats'] = column_stats
                
                logging.info(f"超大表 {table_name} 轻量级分析完成")
                
            except Exception as e:
                logging.error(f"轻量级分析超大表 {table_name} 失败: {str(e)}")
        
        return results

    def _calculate_basic_stats(self, series):
        """
        计算列的基本统计信息
        
        Args:
            series (Series): 列数据
            
        Returns:
            dict: 基本统计信息
        """
        stats = {
            'count': len(series),
            'null_count': series.isna().sum(),
            'null_percentage': float(series.isna().mean())
        }
        
        # 对于非空值计算进一步的统计
        non_null = series.dropna()
        if len(non_null) > 0:
            # 获取唯一值信息
            stats['unique_count'] = len(non_null.unique())
            stats['unique_percentage'] = float(stats['unique_count'] / len(non_null)) if len(non_null) > 0 else 0
            
            # 对于数值类型获取基本统计量
            if pd.api.types.is_numeric_dtype(series) and not pd.api.types.is_bool_dtype(series):
                try:
                    stats['min'] = float(non_null.min())
                    stats['max'] = float(non_null.max())
                    stats['mean'] = float(non_null.mean())
                    stats['median'] = float(non_null.median())
                except:
                    pass
            # 对于布尔类型
            elif pd.api.types.is_bool_dtype(series):
                try:
                    stats['true_count'] = int(non_null.sum())
                    stats['false_count'] = int(len(non_null) - non_null.sum())
                    stats['true_percentage'] = float(stats['true_count'] / len(non_null)) if len(non_null) > 0 else 0
                except:
                    pass
            # 对于字符串类型
            elif pd.api.types.is_string_dtype(series) or pd.api.types.is_object_dtype(series):
                try:
                    lengths = non_null.astype(str).str.len()
                    stats['min_length'] = int(lengths.min())
                    stats['max_length'] = int(lengths.max())
                    stats['mean_length'] = float(lengths.mean())
                except:
                    pass
        
        return stats