"""
Logging utilities for FDA device data import
"""
import logging
import datetime
from IPython.display import display, HTML

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger("device_importer")

# 在Jupyter中美化输出日志的函数
def log_info(message):
    print(f"ℹ️ {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - INFO - {message}")

def log_error(message):
    print(f"❌ {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - ERROR - {message}")

def log_success(message):
    print(f"✅ {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - SUCCESS - {message}")
    
def log_warning(message):
    print(f"⚠️ {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - WARNING - {message}")

def show_header():
    """显示应用程序标题"""
    display(HTML("<h2>FDA 医疗设备数据库建立与数据导入 (改进版)</h2>"))
    
def show_version_info():
    """显示版本信息"""
    display(HTML(f"<p><i>FDA Device Importer - 改进版 v1.0.0 - {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</i></p>"))
