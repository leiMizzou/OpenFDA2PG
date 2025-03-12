"""
File handling utilities for FDA device data import
"""
import os
import json
import glob
from logger import log_error

class FileHandler:
    """处理FDA医疗设备文件的辅助类"""
    
    @staticmethod
    def get_classification_files(classification_dir):
        """获取所有设备分类文件路径"""
        return glob.glob(os.path.join(classification_dir, 'device-classification-*.json'))
    
    @staticmethod
    def get_enforcement_files(enforcement_dir):
        """获取所有执法行动文件路径"""
        return glob.glob(os.path.join(enforcement_dir, 'device-enforcement-*.json'))
    
    @staticmethod
    def get_event_files(event_dir):
        """获取所有不良事件报告文件路径，包括子目录"""
        event_files = []
        # 遍历event目录下的所有子目录
        for root, dirs, files in os.walk(event_dir):
            # 在每个目录中查找匹配的JSON文件
            for file in files:
                if file.startswith('device-event-') and file.endswith('.json'):
                    event_files.append(os.path.join(root, file))
        return event_files
    
    @staticmethod
    def get_recall_files(recall_dir):
        """获取所有召回文件路径"""
        return glob.glob(os.path.join(recall_dir, 'device-recall-*.json'))
    
    @staticmethod
    def get_udi_files(udi_dir):
        """获取所有UDI文件路径"""
        return glob.glob(os.path.join(udi_dir, 'device-udi-*.json'))
    
    @staticmethod
    def load_json(filename):
        """加载JSON文件"""
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                data = json.load(f)
            return data
        except Exception as e:
            log_error(f"加载JSON文件失败 {filename}: {str(e)}")
            return None
    
    @staticmethod
    def sample_data(filename, record_count=3):
        """采样JSON文件中的数据查看结构"""
        try:
            data = FileHandler.load_json(filename)
            if data and 'results' in data and len(data['results']) > 0:
                sample = data['results'][:min(record_count, len(data['results']))]
                return sample
            return None
        except Exception as e:
            log_error(f"采样数据失败 {filename}: {str(e)}")
            return None

    @staticmethod
    def extract_meta_data(filename):
        """提取JSON文件的元数据"""
        try:
            data = FileHandler.load_json(filename)
            if data and 'meta' in data:
                return data['meta']
            return None
        except Exception as e:
            log_error(f"提取元数据失败 {filename}: {str(e)}")
            return None
