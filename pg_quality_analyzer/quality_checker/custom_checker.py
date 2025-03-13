"""
自定义检查器，使用Gemini API生成定制化检查
"""
import logging
import pandas as pd
import json
from . import BaseChecker

class CustomChecker(BaseChecker):
    """自定义检查器，使用Gemini API生成检查规则"""
    
    def __init__(self, config, gemini_integrator=None):
        """
        初始化自定义检查器
        
        Args:
            config (Config): 配置对象
            gemini_integrator (GeminiIntegrator, optional): Gemini集成器
        """
        super().__init__(config)
        self.gemini = gemini_integrator
        self.cached_checks = {}  # 缓存生成的检查规则
    
    def check(self, series, column_info=None):
        """
        执行自定义检查
        
        Args:
            series (Series): 列数据
            column_info (dict, optional): 列信息
            
        Returns:
            dict: 检查结果
        """
        if not self.gemini or not self.config.get('gemini.custom_check_generation'):
            return {
                'issues': [],
                'gemini_enabled': False
            }
        
        if series.empty:
            return {
                'issues': [],
                'gemini_enabled': True
            }
        
        # 获取列名
        column_name = column_info.get('column_name') if column_info else 'unknown'
        
        # 检查缓存中是否有此列的检查规则
        if column_name in self.cached_checks:
            checks = self.cached_checks[column_name]
        else:
            # 生成自定义检查规则
            checks = self._generate_checks(series, column_info)
            if checks:
                self.cached_checks[column_name] = checks
        
        # 执行检查
        issues = []
        
        for check in checks:
            try:
                # 执行检查规则
                result = self._execute_check(check, series)
                
                if not result['passed']:
                    issues.append({
                        'type': check['id'],
                        'description': check['description'],
                        'severity': check['severity'],
                        'details': result.get('details', {})
                    })
            except Exception as e:
                logging.error(f"执行自定义检查 {check['id']} 失败: {str(e)}")
        
        return {
            'issues': issues,
            'generated_checks': checks,
            'gemini_enabled': True
        }
    
    def _generate_checks(self, series, column_info):
        """
        使用Gemini API生成检查规则
        
        Args:
            series (Series): 列数据
            column_info (dict): 列信息
            
        Returns:
            list: 检查规则列表
        """
        if not self.gemini:
            return []
        
        try:
            # 准备样本数据
            sample_count = min(100, len(series))
            sample_data = series.sample(sample_count).tolist() if sample_count > 0 else []
            
            # 获取列数据类型
            dtype = str(series.dtype)
            
            # 分析样本数据以提供更多上下文
            basic_stats = {}
            
            if pd.api.types.is_numeric_dtype(series):
                basic_stats = {
                    "min": float(series.min()) if not series.empty else None,
                    "max": float(series.max()) if not series.empty else None,
                    "mean": float(series.mean()) if not series.empty else None,
                    "null_count": int(series.isna().sum())
                }
            elif pd.api.types.is_string_dtype(series) or pd.api.types.is_object_dtype(series):
                non_null = series.dropna()
                if not non_null.empty:
                    basic_stats = {
                        "min_length": int(non_null.astype(str).str.len().min()),
                        "max_length": int(non_null.astype(str).str.len().max()),
                        "mean_length": float(non_null.astype(str).str.len().mean()),
                        "null_count": int(series.isna().sum()),
                        "unique_count": int(series.nunique())
                    }
            
            # 构建Gemini API请求
            context = {
                "column_name": column_info.get('column_name') if column_info else 'unknown',
                "data_type": dtype,
                "basic_stats": basic_stats,
                "sample_count": sample_count,
                "column_description": column_info.get('column_description') if column_info else None
            }
            
            # 调用Gemini API生成检查规则
            result = self.gemini.generate_custom_checks(sample_data, context)
            
            # 解析并返回检查规则
            return self._parse_checks(result)
            
        except Exception as e:
            logging.error(f"生成自定义检查规则失败: {str(e)}")
            return []
    
    def _parse_checks(self, gemini_response):
        """
        解析Gemini API返回的检查规则
        
        Args:
            gemini_response (dict): Gemini API响应
            
        Returns:
            list: 检查规则列表
        """
        if not gemini_response or 'checks' not in gemini_response:
            return []
        
        checks = []
        
        for check_data in gemini_response.get('checks', []):
            if not all(key in check_data for key in ['id', 'description', 'code']):
                continue
                
            check = {
                'id': check_data['id'],
                'description': check_data['description'],
                'severity': check_data.get('severity', 'medium'),
                'code': check_data['code'],
                'rationale': check_data.get('rationale', '')
            }
            
            checks.append(check)
        
        return checks
    
    def _execute_check(self, check, series):
        """
        执行单个检查规则
        
        Args:
            check (dict): 检查规则
            series (Series): 列数据
            
        Returns:
            dict: 检查结果
        """
        try:
            # 创建局部变量
            data = series
            result = {'passed': True, 'details': {}}
            
            # 执行检查代码
            code = check['code']
            exec(code, {'pd': pd, 'np': pd.np, 'data': data, 'result': result})
            
            return result
            
        except Exception as e:
            logging.error(f"执行检查代码失败: {str(e)}")
            return {'passed': True, 'error': str(e)}
    
    def check_table(self, table_name, df, columns_info=None, schema=None):
        """
        对表执行自定义检查
        
        Args:
            table_name (str): 表名
            df (DataFrame): 表数据样本
            columns_info (dict, optional): 列信息
            schema (dict, optional): 表schema信息
            
        Returns:
            dict: 表检查结果
        """
        logging.info(f"对表 {table_name} 执行自定义检查")
        
        if not self.gemini or not self.config.get('gemini.custom_check_generation'):
            return {}
        
        # 表级检查规则
        table_checks = self._generate_table_checks(df, table_name, schema)
        
        # 列级检查
        column_results = {}
        
        for column in df.columns:
            column_info = columns_info.get(column) if columns_info else None
            
            # 为列信息添加列名
            if column_info:
                column_info['column_name'] = column
                
            result = self.check(df[column], column_info)
            if result['issues']:
                column_results[column] = result
        
        # 执行表级检查
        table_issues = []
        
        for check in table_checks:
            try:
                # 执行检查规则
                result = self._execute_table_check(check, df)
                
                if not result['passed']:
                    table_issues.append({
                        'type': check['id'],
                        'description': check['description'],
                        'severity': check['severity'],
                        'details': result.get('details', {})
                    })
            except Exception as e:
                logging.error(f"执行表级自定义检查 {check['id']} 失败: {str(e)}")
        
        table_result = {
            'table_issues': table_issues,
            'column_results': column_results,
            'table_checks': table_checks
        }
        
        self.results[table_name] = table_result
        return table_result
    
    def _generate_table_checks(self, df, table_name, schema=None):
        """
        生成表级检查规则
        
        Args:
            df (DataFrame): 表数据
            table_name (str): 表名
            schema (dict, optional): 表schema信息
            
        Returns:
            list: 表级检查规则
        """
        if not self.gemini:
            return []
        
        try:
            # 提取表的基本信息
            column_names = df.columns.tolist()
            column_types = {col: str(dtype) for col, dtype in df.dtypes.items()}
            
            # 构建上下文信息
            context = {
                "table_name": table_name,
                "column_count": len(column_names),
                "row_count": len(df),
                "column_names": column_names,
                "column_types": column_types,
                "has_schema": schema is not None
            }
            
            # 调用Gemini API生成表级检查
            result = self.gemini.generate_table_checks(context)
            
            # 解析并返回检查规则
            return self._parse_checks(result)
            
        except Exception as e:
            logging.error(f"生成表级自定义检查规则失败: {str(e)}")
            return []
    
    def _execute_table_check(self, check, df):
        """
        执行表级检查
        
        Args:
            check (dict): 检查规则
            df (DataFrame): 表数据
            
        Returns:
            dict: 检查结果
        """
        try:
            # 创建局部变量
            data = df
            result = {'passed': True, 'details': {}}
            
            # 执行检查代码
            code = check['code']
            exec(code, {'pd': pd, 'np': pd.np, 'data': data, 'result': result})
            
            return result
            
        except Exception as e:
            logging.error(f"执行表级检查代码失败: {str(e)}")
            return {'passed': True, 'error': str(e)}
