"""
Utility functions for FDA device data import
"""
import re
import json
import datetime
from logger import log_warning

def parse_date(date_str):
    """
    解析FDA日期格式，支持多种格式
    
    Args:
        date_str: 日期字符串
        
    Returns:
        datetime.date 或 None
    """
    if not date_str:
        return None
    try:
        # 处理多种可能的日期格式
        formats = ['%Y%m%d', '%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y', '%Y']
        for fmt in formats:
            try:
                return datetime.datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue
        
        # 如果没有匹配到，尝试解析特殊格式
        if re.match(r'^\d{4}-\d{2}$', date_str):  # YYYY-MM
            return datetime.datetime.strptime(date_str + "-01", '%Y-%m-%d').date()
        
        log_warning(f"无法解析日期格式: {date_str}")
        return None
    except Exception as e:
        log_warning(f"日期解析错误: {date_str} - {str(e)}")
        return None

def parse_boolean(value):
    """
    标准化解析布尔值
    
    Args:
        value: 输入值
        
    Returns:
        bool 或 None
    """
    if isinstance(value, bool):
        return value
    
    if value is None:
        return None
        
    if isinstance(value, str):
        return value.upper() == 'Y' or value.upper() == 'YES' or value.upper() == 'TRUE'
        
    return bool(value)

def convert_to_array(value):
    """
    将各种输入转换为Python数组
    
    Args:
        value: 输入值
        
    Returns:
        list 或 None
    """
    if value is None:
        return None
        
    if isinstance(value, list):
        return value
        
    if isinstance(value, str):
        # 尝试作为JSON数组解析
        if value.startswith('[') and value.endswith(']'):
            try:
                return json.loads(value)
            except:
                pass
                
        # 尝试分隔字符串
        return [item.strip() for item in re.split(r'[,;]\s*', value) if item.strip()]
        
    # 单个项目转为数组
    return [value]

def parse_code_info(code_info):
    """
    解析召回代码信息为结构化数据
    
    Args:
        code_info: 代码信息字符串
        
    Returns:
        list: 包含结构化代码信息的列表
    """
    if not code_info:
        return []
        
    result = []
    
    # 尝试识别常见的代码类型模式
    patterns = [
        # 批号
        (r'Lot(?:\s+Number)?s?:?\s+([\w\s,\-\.;/]+)', 'lot'),
        # 序列号
        (r'Serial(?:\s+Number)?s?:?\s+([\w\s,\-\.;/]+)', 'serial'),
        # UDI代码
        (r'UDI(?:\-DI)?(?:\s+code)?:?\s+([\w\s,\-\.;/]+)', 'udi'),
        # GTIN代码
        (r'GTIN:?\s+([\w\s,\-\.;/]+)', 'gtin'),
        # 过期日期
        (r'Expiration Date:?\s+([\w\s,\-\.;/]+)', 'expiration')
    ]
    
    for pattern, item_type in patterns:
        matches = re.finditer(pattern, code_info, re.IGNORECASE)
        for match in matches:
            value_str = match.group(1).strip()
            # 处理可能的多个值（用逗号、分号等分隔）
            values = re.split(r'[,;]\s*', value_str)
            for value in values:
                if value.strip():
                    result.append({
                        'item_type': item_type,
                        'item_value': value.strip()
                    })
    
    # 如果没有找到任何匹配，保存整个字符串
    if not result and code_info.strip():
        result.append({
            'item_type': 'unknown',
            'item_value': code_info.strip()
        })
        
    return result
