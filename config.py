"""
Configuration settings for FDA device data import
"""
import os

# 数据库连接配置 - 修改为您的实际连接信息
DB_CONFIG = {
    'host': '192.168.2.2',
    'port': 5432,
    'dbname': 'fda_device',
    'user': 'postgres',
    'password': '12345687'
}

# 配置文件路径 - 根据实际存放文件结构进行设置
BASE_DIR = '../datafiles/unzip/device'  # 根目录
DATA_DIRS = {
    'classification_dir': os.path.join(BASE_DIR, 'classification'),
    'enforcement_dir': os.path.join(BASE_DIR, 'enforcement'),
    'event_dir': os.path.join(BASE_DIR, 'event'),
    'recall_dir': os.path.join(BASE_DIR, 'recall'),
    'udi_dir': os.path.join(BASE_DIR, 'udi'),
}
