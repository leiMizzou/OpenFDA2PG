"""
配置处理模块，负责加载和管理程序配置
"""
import os
import yaml
import logging
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

class Config:
    """配置处理类"""
    
    def __init__(self, config_file=None):
        """
        初始化配置
        
        Args:
            config_file (str, optional): 配置文件路径
        """
        self.config = {}
        
        # 默认配置
        self.default_config = {
            'database': {
                'host': 'localhost',
                'port': 5432,
                'user': '',
                'password': '',
                'dbname': '',
                'schema': 'public'
            },
            'analysis': {
                'sample_size': 1000,
                'sample_method': 'random',
                'max_tables': 100,
                'exclude_tables': [],
                'include_tables': []
            },
            'quality_checks': {
                'null_threshold': 0.3,  # 空值率阈值
                'cardinality_threshold': 0.9,  # 基数比率阈值
                'outlier_threshold': 3.0,  # 异常值标准差倍数
                'correlation_threshold': 0.8,  # 相关性阈值
                'enable_all': True,  # 启用所有检查
                'checks': {
                    'null_analysis': True,
                    'distribution_analysis': True,
                    'consistency_analysis': True,
                    'unstructured_analysis': True
                }
            },
            'gemini': {
                'api_key': '',
                'model': 'gemini-pro',
                'temperature': 0.2,
                'max_tokens': 1024,
                'enable': True,
                'custom_check_generation': True,
                'unstructured_analysis': True
            },
            'output': {
                'format': 'html',
                'path': './reports',
                'filename': 'pgsql_quality_report',
                'show_plots': True
            },
            'logging': {
                'level': 'INFO',
                'file': 'pgsql_analyzer.log'
            }
        }
        
        # 合并默认配置
        self.config = self.default_config.copy()
        
        # 如果提供了配置文件，则加载它
        if config_file and os.path.exists(config_file):
            self.load_config(config_file)
            
        # 从环境变量中加载关键配置
        self._load_from_env()
        
        # 设置日志级别
        self._setup_logging()
        
    def load_config(self, config_file):
        """
        从YAML文件加载配置
        
        Args:
            config_file (str): 配置文件路径
        """
        try:
            with open(config_file, 'r') as f:
                loaded_config = yaml.safe_load(f)
                
            # 递归更新配置
            self._update_dict(self.config, loaded_config)
            logging.info(f"配置已从 {config_file} 加载")
        except Exception as e:
            logging.error(f"加载配置文件失败: {str(e)}")
    
    def _update_dict(self, d, u):
        """递归更新字典"""
        for k, v in u.items():
            if isinstance(v, dict):
                d[k] = self._update_dict(d.get(k, {}), v)
            else:
                d[k] = v
        return d
    
    def _load_from_env(self):
        """从环境变量加载配置"""
        # 数据库配置
        if os.getenv('PGHOST'):
            self.config['database']['host'] = os.getenv('PGHOST')
        if os.getenv('PGPORT'):
            self.config['database']['port'] = int(os.getenv('PGPORT'))
        if os.getenv('PGUSER'):
            self.config['database']['user'] = os.getenv('PGUSER')
        if os.getenv('PGPASSWORD'):
            self.config['database']['password'] = os.getenv('PGPASSWORD')
        if os.getenv('PGDATABASE'):
            self.config['database']['dbname'] = os.getenv('PGDATABASE')
        if os.getenv('PGSCHEMA'):
            self.config['database']['schema'] = os.getenv('PGSCHEMA')
        
        # Gemini API配置
        if os.getenv('GEMINI_API_KEY'):
            self.config['gemini']['api_key'] = os.getenv('GEMINI_API_KEY')
    
    def _setup_logging(self):
        """设置日志配置"""
        log_level = getattr(logging, self.config['logging']['level'].upper())
        log_file = self.config['logging']['file']
        
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
    
    def get(self, key=None, default=None):
        """
        获取配置项
        
        Args:
            key (str, optional): 配置键，如果为None则返回全部配置
            default: 当键不存在时返回的默认值
                
        Returns:
            任意类型: 配置值
        """
        if key is None:
            return self.config
        
        keys = key.split('.')
        result = self.config
        for k in keys:
            if k in result:
                result = result[k]
            else:
                return default
        return result
    
    def set(self, key, value):
        """
        设置配置项
        
        Args:
            key (str): 配置键
            value: 配置值
        """
        keys = key.split('.')
        config = self.config
        for k in keys[:-1]:
            if k not in config:
                config[k] = {}
            config = config[k]
        config[keys[-1]] = value
