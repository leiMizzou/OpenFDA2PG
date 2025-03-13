"""
基础质量检查器
"""
import logging
from abc import ABC, abstractmethod

class BaseChecker(ABC):
    """所有质量检查器的基类"""
    
    def __init__(self, config):
        """
        初始化基础检查器
        
        Args:
            config (Config): 配置对象
        """
        self.config = config
        self.results = {}
        
    @abstractmethod
    def check(self, df, column_info=None):
        """
        执行质量检查
        
        Args:
            df (DataFrame): 数据样本
            column_info (dict, optional): 列信息
            
        Returns:
            dict: 检查结果
        """
        pass
    
    def check_table(self, table_name, df, columns_info):
        """
        对表执行质量检查
        
        Args:
            table_name (str): 表名
            df (DataFrame): 表数据样本
            columns_info (dict): 列信息
            
        Returns:
            dict: 表检查结果
        """
        logging.info(f"使用 {self.__class__.__name__} 检查表 {table_name}")
        
        table_results = {}
        
        # 对每一列执行检查
        for column_name, column_info in columns_info.items():
            if column_name in df.columns:
                column_result = self.check(df[column_name], column_info)
                table_results[column_name] = column_result
        
        self.results[table_name] = table_results
        return table_results
    
    def get_results(self):
        """
        获取所有检查结果
        
        Returns:
            dict: 所有检查结果
        """
        return self.results
    
    def clear_results(self):
        """清空检查结果"""
        self.results = {}
    
    def is_enabled(self):
        """
        检查器是否启用
        
        Returns:
            bool: 是否启用
        """
        checker_name = self.__class__.__name__.lower().replace('checker', '')
        checker_config = f"quality_checks.checks.{checker_name}_analysis"
        
        # 首先检查特定检查是否启用
        specific_enabled = self.config.get(checker_config)
        
        # 然后检查全局设置
        all_enabled = self.config.get('quality_checks.enable_all')
        
        # 特定设置优先于全局设置
        if specific_enabled is not None:
            return specific_enabled
        
        return all_enabled
