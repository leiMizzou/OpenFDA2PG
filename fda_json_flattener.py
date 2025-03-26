#!/usr/bin/env python3
"""
通用FDA JSON结构分析工具

处理同类多个JSON文件，分析所有可能的嵌套路径，并生成完整的表结构关系。
特点:
- 自动发现并处理所有数组和嵌套对象，无需预定义特殊字段
- 为所有发现的数组创建适当的关系表或枚举表
- 智能数据类型推断
- 支持任意深度的嵌套结构

用法:
    python universal_json_analyzer.py --input_dir /path/to/data/dir --data_type event --recursive --output_dir ./output
"""

import os
import sys
import json
import re
import glob
import argparse
import pandas as pd
from collections import defaultdict, Counter
from datetime import datetime
import logging
import hashlib

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('fda_json_analyzer.log')
    ]
)
logger = logging.getLogger('universal_json_analyzer')


class BatchJsonAnalyzer:
    """用于批量分析JSON嵌套结构并生成表关系的工具类"""
    
    def __init__(self, output_dir="./output"):
        """初始化分析器"""
        self.output_dir = output_dir
        
        # 确保输出目录存在
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            logger.info(f"创建输出目录: {output_dir}")
        
        # 存储所有发现的字段路径
        self.all_paths = set()
        
        # 存储嵌套字段路径
        self.nested_paths = []
        
        # 存储对象和数组路径
        self.object_paths = set()
        self.array_paths = set()
        
        # 存储数组值字段内容，用于生成枚举表
        # 键为数组路径，值为该路径下的唯一值集合
        self.array_value_contents = defaultdict(set)
        
        # 标记数组类型 - 简单数组(True)或对象数组(False)
        self.simple_array_flags = {}
        
        # 存储字段样本值
        self.field_samples = {}
        
        # 表结构映射
        self.tables = {}
        
        # 路径计数器
        self.path_counter = Counter()
        
        # 已处理文件计数
        self.processed_files = 0
        self.total_records = 0
        
        # 特殊嵌套对象字段 - 需要展平处理
        self.flatten_objects = set([])  # 移除所有特殊处理
        
        # 不保留原始对象字段，所有嵌套对象都按通用逻辑处理
        self.preserve_original_objects = set([])
        
        # 追踪需要作为中间表处理的嵌套对象
        self.flatten_objects_with_tables = {}
        
        # 日期字段检测模式
        self.date_field_patterns = [
            re.compile(r'date[_]?', re.IGNORECASE),
            re.compile(r'.*_dt$', re.IGNORECASE),
            re.compile(r'.*_date$', re.IGNORECASE),
            re.compile(r'^dt_.*', re.IGNORECASE),
        ]
        
        # 日期样本值检测模式
        self.date_value_patterns = [
            re.compile(r'^[0-9]{8}$'),  # 20250121
            re.compile(r'^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$'),  # 01/23/2025
            re.compile(r'^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}$'),  # 2025-01-23
        ]
        
        # 路径到表名的映射缓存
        self.path_to_table_map = {}
    
    def find_json_files(self, input_dir, pattern, recursive=False):
        """查找匹配的JSON文件
        
        Args:
            input_dir: 输入目录路径
            pattern: 文件名匹配模式
            recursive: 是否递归搜索子目录
            
        Returns:
            list: 匹配的文件路径列表
        """
        matched_files = []
        
        if recursive:
            # 递归搜索子目录
            for root, dirs, files in os.walk(input_dir):
                for file in files:
                    if self._match_pattern(file, pattern):
                        matched_files.append(os.path.join(root, file))
        else:
            # 仅搜索当前目录
            search_pattern = os.path.join(input_dir, pattern)
            matched_files = glob.glob(search_pattern)
        
        return matched_files
    
    def _match_pattern(self, filename, pattern):
        """检查文件名是否匹配模式
        
        Args:
            filename: 文件名
            pattern: 匹配模式（支持通配符）
            
        Returns:
            bool: 是否匹配
        """
        # 将通配符模式转换为正则表达式
        regex_pattern = pattern.replace(".", "\\.").replace("*", ".*").replace("?", ".")
        return re.match(regex_pattern, filename) is not None
    
    def process_directory(self, input_dir, data_type, pattern=None, max_files=10, 
                         max_records_per_file=100, recursive=False):
        """处理目录中的多个JSON文件
        
        Args:
            input_dir: 输入目录路径
            data_type: 数据类型标识
            pattern: 文件名匹配模式
            max_files: 最大处理文件数
            max_records_per_file: 每个文件最大处理记录数
            recursive: 是否递归搜索子目录
            
        Returns:
            dict: 处理结果
        """
        if pattern is None:
            pattern = f"*.json"  # 默认处理所有JSON文件
        
        # 查找匹配的文件
        files = self.find_json_files(input_dir, pattern, recursive)
        
        if not files:
            logger.warning(f"未找到匹配的文件: {os.path.join(input_dir, pattern)}")
            logger.info(f"注意: {'启用' if recursive else '未启用'}递归搜索子目录")
            return None
        
        # 限制文件数量
        files = files[:min(len(files), max_files)]
        
        logger.info(f"找到 {len(files)} 个匹配文件，将处理其中 {len(files)} 个")
        
        # 处理每个文件
        for i, file_path in enumerate(files):
            logger.info(f"处理文件 {i+1}/{len(files)}: {os.path.basename(file_path)} ({file_path})")
            self.process_file(file_path, data_type, max_records_per_file)
        
        # 预处理阶段 - 分析需要创建中间表的特殊嵌套对象
        self._preprocess_special_nested_objects()
        
        # 分析路径计数器，识别表结构
        self.identify_tables(data_type)
        
        # 不再创建枚举表
        
        return {
            'data_type': data_type,
            'processed_files': self.processed_files,
            'total_records': self.total_records,
            'unique_paths': len(self.all_paths),
            'nested_paths': len(self.nested_paths),
            'tables': len(self.tables),
            'array_paths': len(self.array_paths),
            'simple_arrays': sum(1 for is_simple in self.simple_array_flags.values() if is_simple)
        }
    
    def process_file(self, file_path, data_type, max_records=100):
        """处理单个JSON文件
        
        Args:
            file_path: 文件路径
            data_type: 数据类型标识
            max_records: 最大处理记录数
            
        Returns:
            int: 处理的记录数
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # 获取记录列表
            records = []
            if 'results' in data and isinstance(data['results'], list):
                # FDA标准格式
                records = data['results'][:max_records]
            elif isinstance(data, list):
                # 纯数组格式
                records = data[:max_records]
            else:
                # 单个对象
                records = [data]
            
            # 处理每条记录
            record_count = 0
            for record in records:
                paths = self.find_all_paths(record, data_type)
                
                # 更新路径计数器
                for path in paths:
                    self.path_counter[path] += 1
                    self.all_paths.add(path)
                
                # 处理所有数组值收集
                self._collect_all_array_values(record, data_type)
                
                record_count += 1
            
            self.processed_files += 1
            self.total_records += record_count
            
            return record_count
            
        except Exception as e:
            logger.error(f"处理文件 {file_path} 时出错: {str(e)}")
            return 0
    
    def _collect_all_array_values(self, record, data_type, current_path=None):
        """递归收集所有数组字段的值用于生成枚举表
        
        Args:
            record: JSON记录或其子对象
            data_type: 数据类型
            current_path: 当前路径（递归用）
        """
        if current_path is None:
            current_path = data_type
            
        if isinstance(record, dict):
            # 处理字典对象中的所有字段
            for key, value in record.items():
                field_path = f"{current_path}.{key}"
                
                if isinstance(value, list):
                    # 找到数组字段
                    if value and all(not isinstance(item, (dict, list)) for item in value):
                        # 简单数组（原始值数组）- 收集所有唯一值
                        self.simple_array_flags[field_path] = True
                        for item in value:
                            if item:  # 避免空值
                                self.array_value_contents[field_path].add(str(item))
                    else:
                        # 对象数组或空数组 - 标记为复杂数组
                        self.simple_array_flags[field_path] = False
                    
                    # 递归处理数组中的每个对象
                    for i, item in enumerate(value):
                        if isinstance(item, (dict, list)):
                            self._collect_all_array_values(item, data_type, f"{field_path}[{i}]")
                
                # 递归处理嵌套对象
                elif isinstance(value, dict):
                    self._collect_all_array_values(value, data_type, field_path)
                    
        elif isinstance(record, list):
            # 处理数组
            for i, item in enumerate(record):
                item_path = f"{current_path}[{i}]"
                if isinstance(item, (dict, list)):
                    self._collect_all_array_values(item, data_type, item_path)
    
    def find_all_paths(self, json_obj, prefix="", path=""):
        """递归查找所有路径，包括嵌套路径
        
        Args:
            json_obj: JSON对象
            prefix: 字段前缀
            path: 当前路径
            
        Returns:
            list: 路径列表
        """
        paths = []
        current_path = f"{path}.{prefix}" if path else prefix
        
        if isinstance(json_obj, dict):
            # 记录对象路径
            if current_path:
                self.object_paths.add(current_path)
            
            for key, value in json_obj.items():
                new_path = f"{current_path}.{key}" if current_path else key
                paths.append(new_path)
                
                # 保存样本值 - 确保每个路径都有样本
                sample_value = self._get_sample_value(value)
                self.field_samples[new_path] = sample_value
                
                # 确保同样的值也可以通过不带数组索引的路径访问
                if '[' in new_path:
                    base_path = new_path.split('[')[0]
                    if base_path not in self.field_samples:
                        self.field_samples[base_path] = sample_value
                
                # 处理嵌套结构
                if isinstance(value, (dict, list)):
                    nested = self.find_all_paths(value, key, current_path)
                    paths.extend(nested)
                    
                    # 记录嵌套路径
                    if nested and new_path not in self.nested_paths:
                        self.nested_paths.append(new_path)
        
        elif isinstance(json_obj, list):
            # 记录数组路径
            array_path = f"{current_path}[]"
            self.array_paths.add(array_path)
            paths.append(array_path)
            
            # 为数组路径保存样本值
            if array_path not in self.field_samples and json_obj:
                # 提供数组第一个元素作为样本
                self.field_samples[array_path] = self._get_sample_value(json_obj[0])
            
            # 处理数组元素（最多3个）
            for i, item in enumerate(json_obj[:3]):
                if isinstance(item, (dict, list)):
                    item_path = f"{current_path}[{i}]"
                    item_paths = self.find_all_paths(item, f"{prefix}[{i}]", path)
                    paths.extend(item_paths)
        
        return paths
    
    def _get_sample_value(self, value):
        """获取字段的样本值，简化复杂结构
        
        Args:
            value: 字段值
            
        Returns:
            str: 样本值的字符串表示
        """
        try:
            if isinstance(value, (dict, list)):
                # 对于复杂类型，返回类型信息和长度
                if isinstance(value, dict):
                    # 对于字典，提供第一个键值对作为示例
                    if value:
                        first_key = next(iter(value))
                        first_val = value[first_key]
                        if isinstance(first_val, (dict, list)):
                            return f"[Object with {len(value)} keys]"
                        else:
                            return f"[Object] 示例键'{first_key}': '{str(first_val)[:50]}'"
                    return f"[Object with {len(value)} keys]"
                else:
                    # 对于数组，提供第一个元素作为示例
                    if value:
                        first_item = value[0]
                        if isinstance(first_item, (dict, list)):
                            return f"[Array with {len(value)} items]"
                        else:
                            return f"[Array] 示例: '{str(first_item)[:50]}'"
                    return f"[Array with {len(value)} items]"
            elif value is None:
                return "null"
            elif isinstance(value, bool):
                return str(value).lower()  # 返回"true"或"false"
            elif isinstance(value, (int, float)):
                return str(value)
            else:
                # 简单类型直接返回字符串表示，截断长字符串
                str_val = str(value)
                if len(str_val) > 100:
                    return str_val[:100] + "..."
                return str_val
        except Exception as e:
            # 确保即使出错也返回某种值
            logger.warning(f"获取样本值时出错: {str(e)[:30]}")
            return f"[值解析错误: {str(e)[:30]}]"
    
    def _preprocess_special_nested_objects(self):
        """预处理特殊嵌套对象，识别需要中间表的对象"""
        
        # 检查所有路径，寻找特殊嵌套对象的子字段
        for path in self.all_paths:
            parts = path.split('.')
            
            # 检查是否包含特殊嵌套对象
            for special_obj in self.flatten_objects:
                if special_obj in parts:
                    # 找到特殊对象在路径中的位置
                    obj_index = parts.index(special_obj)
                    if obj_index > 0 and obj_index < len(parts) - 1:
                        # 构建完整的对象路径
                        obj_path = '.'.join(parts[:obj_index+1])
                        parent_path = '.'.join(parts[:obj_index])
                        
                        # 确保有子路径
                        if obj_index + 1 < len(parts):
                            # 记录该对象需要中间表
                            if obj_path not in self.flatten_objects_with_tables:
                                self.flatten_objects_with_tables[obj_path] = {
                                    'parent_path': parent_path,
                                    'fields': set(),
                                    'array_fields': set()
                                }
                            
                            # 记录字段名
                            field_name = parts[obj_index + 1]
                            self.flatten_objects_with_tables[obj_path]['fields'].add(field_name)
                            
                            # 检查是否为数组字段
                            if path in self.array_paths:
                                self.flatten_objects_with_tables[obj_path]['array_fields'].add(field_name)
        
        # 记录调试信息
        logger.debug(f"特殊对象中间表预处理结果: {self.flatten_objects_with_tables}")
    
    def identify_tables(self, main_type):
        """基于所有路径识别完整表结构
        
        Args:
            main_type: 主表类型
            
        Returns:
            dict: 表结构映射
        """
        # 主表
        main_table = f"{main_type}"
        self.tables[main_table] = {
            'name': main_table,
            'type': 'main',
            'parent': None,
            'fields': [],
            'has_arrays': False
        }
        
        # 按路径深度排序，先处理浅层路径
        sorted_paths = sorted(
            [(path, count) for path, count in self.path_counter.items()],
            key=lambda x: (x[0].count('.'), -x[1])  # 按深度和计数排序
        )
        
        # 处理所有路径
        for path, count in sorted_paths:
            # 分割路径
            parts = path.split('.')
            
            # 确定表和字段
            if len(parts) <= 2:
                # 根级字段，属于主表
                if len(parts) == 2:  # 跳过第一部分(类型名称)
                    field_name = parts[1]
                    
                    # 检查是否为数组路径
                    array_path = f"{main_type}.{field_name}"
                    if array_path in self.array_paths:
                        # 标记主表包含数组
                        self.tables[main_table]['has_arrays'] = True
                        
                        # 所有数组都作为JSON数组直接添加到主表
                        self._add_field_to_table(main_table, field_name, path, count)
                    else:
                        # 非数组字段，直接添加到主表
                        self._add_field_to_table(main_table, field_name, path, count)
            else:
                # 嵌套字段，需要确定所属表
                self._assign_field_to_table(main_type, path, parts, count)
        
        # 创建特殊对象的中间表
        self._create_intermediate_tables_for_special_objects()
        
        return self.tables
    
    def _create_intermediate_tables_for_special_objects(self):
        """为特殊嵌套对象创建中间表"""
        
        for obj_path, obj_info in self.flatten_objects_with_tables.items():
            parent_path = obj_info['parent_path']
            fields = obj_info['fields']
            
            if not fields:
                continue
                
            # 根据路径确定表名
            table_name = self._derive_table_name_from_path(obj_path)
            parent_table = self._derive_table_name_from_path(parent_path)
            
            # 检查父表是否存在
            if parent_table not in self.tables:
                logger.warning(f"父表 {parent_table} 不存在，无法创建中间表 {table_name}")
                continue
            
            # 创建中间表
            if table_name not in self.tables:
                self.tables[table_name] = {
                    'name': table_name,
                    'type': 'object',  # 特殊对象中间表类型为object
                    'parent': parent_table,
                    'fields': [],
                    'has_arrays': len(obj_info['array_fields']) > 0,
                    'object_path': obj_path
                }
                
                # 为所有子字段添加到中间表
                for field_name in fields:
                    # 构建完整字段路径
                    field_path = f"{obj_path}.{field_name}"
                    
                    # 获取计数
                    field_count = 0
                    for p, c in self.path_counter.items():
                        if p.startswith(field_path):
                            field_count = max(field_count, c)
                    
                    # 添加字段到中间表
                    is_array = field_path in self.array_paths
                    self.tables[table_name]['fields'].append({
                        'name': field_name,
                        'path': field_path,
                        'count': field_count,
                        'is_array': is_array,
                        'sample': self.field_samples.get(field_path, '')
                    })
                
                logger.info(f"为特殊对象 {obj_path} 创建中间表 {table_name} 关联到 {parent_table}")
            
            # 更新路径到表的映射
            self.path_to_table_map[obj_path] = table_name
    
    def _create_all_enum_tables(self, main_type):
        """为所有简单数组创建枚举表
        
        Args:
            main_type: 主表类型
        """
        # 处理收集到的所有简单数组
        for array_path, is_simple in self.simple_array_flags.items():
            # 仅处理简单数组
            if not is_simple:
                continue
                
            # 确保有收集到值
            if array_path not in self.array_value_contents or not self.array_value_contents[array_path]:
                continue
            
            # 生成表名
            table_name = self._derive_table_name_from_path(array_path)
            
            # 获取数组上下文信息
            parts = array_path.split('.')
            field_name = parts[-1]
            parent_path = '.'.join(parts[:-1])
            
            # 检查是否在特殊对象中
            special_parent = None
            for special_path, special_info in self.flatten_objects_with_tables.items():
                if array_path.startswith(special_path):
                    special_parent = special_path
                    break
            
            # 确定父表名
            if special_parent:
                # 使用特殊对象的中间表作为父表
                parent_table = self._derive_table_name_from_path(special_parent)
            else:
                # 正常处理
                parent_table = self._derive_table_name_from_path(parent_path)
            
            # 从路径分析确定值字段名
            value_field_name = f"{field_name}_value"
            
            # 创建枚举表
            self.tables[table_name] = {
                'name': table_name,
                'type': 'enum',
                'parent': parent_table,
                'fields': [
                    {
                        'name': value_field_name,
                        'path': array_path + '.value',
                        'count': len(self.array_value_contents[array_path]),
                        'is_array': False,
                        'sample': next(iter(self.array_value_contents[array_path])) if self.array_value_contents[array_path] else ''
                    }
                ],
                'has_arrays': False,
                'array_path': array_path
            }
            
            # 将该路径映射到表名
            self.path_to_table_map[array_path] = table_name
    
    def _derive_table_name_from_path(self, path):
        """从路径派生表名
        
        Args:
            path: JSON路径
            
        Returns:
            str: 派生的表名
        """
        # 检查缓存
        if path in self.path_to_table_map:
            return self.path_to_table_map[path]
            
        parts = path.split('.')
        
        # 移除数组索引
        clean_parts = []
        for part in parts:
            if '[' in part:
                clean_parts.append(part.split('[')[0])
            else:
                clean_parts.append(part)
        
        # 根据路径深度和结构构建表名
        if len(clean_parts) == 1:
            # 主表
            table_name = clean_parts[0]
        elif len(clean_parts) == 2:
            # 直接子表
            table_name = f"{clean_parts[0]}_{clean_parts[1]}"
        else:
            # 多级嵌套 - 使用父对象和当前对象名
            # 避免表名过长，只使用最后两部分
            table_name = f"{clean_parts[0]}_{clean_parts[-2]}_{clean_parts[-1]}"
            
            # 如果中间有多个对象，可能会导致冲突
            # 使用哈希值确保唯一性
            if len(clean_parts) > 3:
                hash_str = '_'.join(clean_parts[1:-2])
                hash_val = hashlib.md5(hash_str.encode()).hexdigest()[:4]
                table_name = f"{clean_parts[0]}_{hash_val}_{clean_parts[-2]}_{clean_parts[-1]}"
        
        # 保存到缓存
        self.path_to_table_map[path] = table_name
        return table_name
    
    def _add_field_to_table(self, table_name, field_name, path, count):
        """添加字段到表
        
        Args:
            table_name: 表名
            field_name: 字段名
            path: 字段路径
            count: 出现次数
        """
        # 检查字段是否已存在
        if any(f['name'] == field_name for f in self.tables[table_name]['fields']):
            return
        
        # 处理数组字段
        is_array = path in self.array_paths
        
        # 添加字段
        self.tables[table_name]['fields'].append({
            'name': field_name,
            'path': path,
            'count': count,
            'is_array': is_array,
            'sample': self.field_samples.get(path, '')
        })
    
    def _assign_field_to_table(self, main_type, path, parts, count):
        """将嵌套字段分配到适当的表
        
        Args:
            main_type: 主表类型
            path: 字段路径
            parts: 路径部分
            count: 出现次数
        """
        # 确定字段所属表
        if '[' in path:
            # 数组元素，创建子表
            array_path = path.split('[')[0]
            array_parts = array_path.split('.')
            
            if len(array_parts) >= 2:
                # 获取数组名称和所属对象
                array_name = array_parts[-1]
                
                # 获取父对象路径
                parent_path = '.'.join(array_parts[:-1])
                
                # 创建数组表名和父表名
                table_name = self._derive_table_name_from_path(array_path)
                parent_table = self._derive_table_name_from_path(parent_path)
                
                # 创建表（如果不存在）
                if table_name not in self.tables:
                    self.tables[table_name] = {
                        'name': table_name,
                        'type': 'array',
                        'parent': parent_table,
                        'fields': [],
                        'has_arrays': False,
                        'array_path': array_path
                    }
                
                # 获取叶子节点名称
                leaf_name = parts[-1]
                if '[' in leaf_name:
                    leaf_name = leaf_name.split('[')[0]
                
                # 检查是否为特殊对象需要展平
                if leaf_name in self.flatten_objects:
                    # 特殊对象路径
                    special_path = f"{array_path}.{leaf_name}"
                    
                    # 如果该特殊对象有中间表，则不展平
                    if special_path in self.flatten_objects_with_tables:
                        # 该对象将创建中间表，不直接添加到当前表
                        pass
                    else:
                        # 展平处理（简单情况）
                        self._flatten_nested_object(table_name, path, leaf_name, count)
                else:
                    # 所有叶子节点都直接添加到表中，不管是否是数组
                    self._add_field_to_table(table_name, leaf_name, path, count)
                
                # 标记表是否包含嵌套数组
                if path in self.array_paths:
                    self.tables[table_name]['has_arrays'] = True
        
        elif len(parts) > 2:
            # 嵌套对象
            parent_obj = parts[1]  # 第一级嵌套对象
            
            # 如果是多级嵌套，可能需要创建中间表
            if len(parts) > 3:
                # 检查是否需要中间表
                intermediate_path = '.'.join(parts[:3])  # 例如 event.device.openfda
                
                if intermediate_path in self.object_paths:
                    # 中间对象路径
                    mid_path = '.'.join(parts[:-1])
                    
                    # 创建中间表名
                    mid_table_name = self._derive_table_name_from_path(mid_path)
                    parent_path = '.'.join(parts[:-2])
                    parent_table = self._derive_table_name_from_path(parent_path)
                    
                    # 确保父表存在
                    if parent_table not in self.tables:
                        # 父对象路径
                        parent_obj_path = '.'.join(parts[:2])
                        
                        self.tables[parent_table] = {
                            'name': parent_table,
                            'type': 'object',
                            'parent': main_type,
                            'fields': [],
                            'has_arrays': False,
                            'object_path': parent_obj_path
                        }
                    
                    # 检查是否为特殊对象（如openfda）
                    object_name = parts[-2]  # 例如 openfda
                    leaf_name = parts[-1]    # 例如 fei_number
                    special_path = '.'.join(parts[:-1])  # 例如 event.device[0].openfda
                    
                    if object_name in self.flatten_objects:
                        # 检查是否应该创建中间表
                        if special_path in self.flatten_objects_with_tables:
                            # 该对象将创建中间表，不直接添加到父表
                            pass
                        else:
                            # 展平到父表
                            parent_obj_table = self._derive_table_name_from_path(parent_path)
                            self._flatten_nested_object(parent_obj_table, path, object_name, count, leaf_name)
                    else:
                        # 创建中间表
                        if mid_table_name not in self.tables:
                            self.tables[mid_table_name] = {
                                'name': mid_table_name,
                                'type': 'object',
                                'parent': parent_table,
                                'fields': [],
                                'has_arrays': False,
                                'object_path': mid_path
                            }
                        
                        # 添加字段到中间表
                        self._add_field_to_table(mid_table_name, leaf_name, path, count)
                    return
            
            # 创建直接子表
            obj_path = '.'.join(parts[:-1])
            table_name = self._derive_table_name_from_path(obj_path)
            parent_path = '.'.join(parts[:1])  # 主表路径
            
            # 创建表（如果不存在）
            if table_name not in self.tables:
                self.tables[table_name] = {
                    'name': table_name,
                    'type': 'object',
                    'parent': main_type,
                    'fields': [],
                    'has_arrays': False,
                    'object_path': obj_path
                }
            
            # 添加字段到表
            leaf_name = parts[-1]
            
            # 检查是否为特殊对象（如openfda）
            if leaf_name in self.flatten_objects:
                # 特殊对象路径
                special_path = obj_path + '.' + leaf_name
                
                # 检查是否应该创建中间表
                if special_path in self.flatten_objects_with_tables:
                    # 该对象将创建中间表，不直接添加到当前表
                    pass
                else:
                    # 展平处理
                    self._flatten_nested_object(table_name, path, leaf_name, count)
            else:
                # 添加常规字段，不管是否是数组
                self._add_field_to_table(table_name, leaf_name, path, count)
            
            # 标记表是否包含数组
            if path in self.array_paths:
                self.tables[table_name]['has_arrays'] = True
    
    def _flatten_nested_object(self, table_name, path, object_name, count, leaf_name=None):
        """将嵌套对象展平为表字段
        
        Args:
            table_name: 表名
            path: 对象路径
            object_name: 对象名
            count: 出现次数
            leaf_name: 如果提供，仅添加该叶子节点
        """
        # 寻找该对象的所有子字段
        object_prefix = path if leaf_name is None else path.rsplit('.', 1)[0]
        
        # 查找所有相关路径
        object_paths = [p for p in self.all_paths if p.startswith(object_prefix)]
        
        # 如果指定了叶子节点，只处理该叶子节点
        if leaf_name:
            for obj_path in object_paths:
                if obj_path.endswith(f".{leaf_name}"):
                    # 创建展平字段名
                    flat_field_name = f"{object_name}_{leaf_name}"
                    self._add_field_to_table(table_name, flat_field_name, obj_path, count)
                    break
        else:
            # 遍历所有子路径，创建展平字段
            for obj_path in object_paths:
                if obj_path == path:
                    continue  # 跳过对象本身的路径
                
                # 从路径中提取叶子节点名称
                suffix = obj_path[len(object_prefix) + 1:]
                if '.' in suffix:
                    continue  # 跳过更深层次的嵌套
                
                # 创建展平字段名
                flat_field_name = f"{object_name}_{suffix}"
                
                # 获取该路径的计数
                obj_count = self.path_counter.get(obj_path, 0)
                
                # 添加到表中
                self._add_field_to_table(table_name, flat_field_name, obj_path, obj_count)
    
    def export_table_structure(self, prefix):
        """导出表结构到CSV和SQL文件
        
        Args:
            prefix: 文件前缀
            
        Returns:
            dict: 输出文件路径
        """
        if not self.tables:
            logger.warning("没有表结构可导出")
            return None
        
        # 1. 生成表结构CSV
        tables_data = []
        for table_name, table_info in self.tables.items():
            tables_data.append({
                'table_name': table_info['name'],
                'table_type': table_info['type'],
                'parent_table': table_info['parent'] or '',
                'field_count': len(table_info['fields']),
                'has_arrays': 'Yes' if table_info['has_arrays'] else 'No',
                'path': table_info.get('array_path', table_info.get('object_path', ''))
            })
        
        # 输出表结构CSV
        tables_csv = os.path.join(self.output_dir, f"{prefix}_tables.csv")
        pd.DataFrame(tables_data).to_csv(tables_csv, index=False)
        logger.info(f"已生成表结构CSV: {tables_csv}")
        
        # 2. 生成字段映射CSV
        all_fields = []
        for table_name, table_info in self.tables.items():
            for field in table_info['fields']:
                # Debug: print sample value information
                path = field['path']
                sample = self.field_samples.get(path, '')
                if not sample:
                    # Try with alternate path format
                    alt_path = path.replace('[]', '')
                    sample = self.field_samples.get(alt_path, '')
                
                # 推断数据类型
                data_type = self._infer_data_type(field['name'], sample)
                
                field_data = {
                    'table_name': table_name,
                    'field_name': field['name'],
                    'original_path': field['path'],
                    'is_array': 'Yes' if field['is_array'] else 'No',
                    'occurrence_count': field['count'],
                    'occurrence_percent': f"{(field['count'] / self.total_records) * 100:.1f}%",
                    'sample_value': sample or '[No sample available]',  # Ensure a non-empty value
                    'data_type': data_type  # 添加推荐的数据类型
                }
                all_fields.append(field_data)
        
        # 输出字段CSV
        fields_csv = os.path.join(self.output_dir, f"{prefix}_fields.csv")
        pd.DataFrame(all_fields).to_csv(fields_csv, index=False)
        logger.info(f"已生成字段映射CSV: {fields_csv}")
        
        # 3. 生成表关系CSV
        relationships = []
        for table_name, table_info in self.tables.items():
            if table_info['parent']:
                # 确定外键字段名
                if table_info['type'] == 'enum':
                    # 枚举表使用更具体的外键命名
                    array_name = table_name.split('_')[-1]
                    parent_short = table_info['parent'].split('_')[-1]
                    foreign_key = f"{parent_short}_id"
                    relationship_type = '多对多'
                else:
                    # 常规子表
                    parent_short = table_info['parent'].split('_')[-1]
                    foreign_key = f"{parent_short}_id"
                    relationship_type = '一对多'
                
                relationships.append({
                    'child_table': table_name,
                    'parent_table': table_info['parent'],
                    'relationship_type': relationship_type,
                    'foreign_key_field': foreign_key,
                    'path': table_info.get('array_path', table_info.get('object_path', '')),
                    'description': f"{table_name}是{table_info['parent']}的{table_info['type']}表"
                })
        
        # 输出关系CSV
        relationships_csv = os.path.join(self.output_dir, f"{prefix}_relationships.csv")
        pd.DataFrame(relationships).to_csv(relationships_csv, index=False)
        logger.info(f"已生成表关系CSV: {relationships_csv}")
        
        # 4. 生成建议DDL
        ddl = self.generate_ddl(prefix)
        ddl_file = os.path.join(self.output_dir, f"{prefix}_tables.sql")
        with open(ddl_file, 'w', encoding='utf-8') as f:
            f.write(ddl)
        logger.info(f"已生成表DDL: {ddl_file}")
        
        # 5. 生成路径统计CSV
        paths_csv = os.path.join(self.output_dir, f"{prefix}_paths.csv")
        paths_data = []
        for path, count in sorted(self.path_counter.items(), key=lambda x: (-x[1], x[0])):
            paths_data.append({
                'path': path,
                'count': count,
                'percent': f"{(count / self.total_records) * 100:.1f}%",
                'is_array': 'Yes' if path in self.array_paths else 'No',
                'is_object': 'Yes' if path in self.object_paths else 'No',
                'depth': path.count('.'),
                'sample': self.field_samples.get(path, '')
            })
        
        pd.DataFrame(paths_data).to_csv(paths_csv, index=False)
        logger.info(f"已生成路径统计CSV: {paths_csv}")
        
        # 6. 生成枚举值参考CSV
        enum_csv = os.path.join(self.output_dir, f"{prefix}_enum_values.csv")
        enum_data = []
        for array_path, values in sorted(self.array_value_contents.items()):
            # 跳过非简单数组
            if array_path not in self.simple_array_flags or not self.simple_array_flags[array_path]:
                continue
                
            # 获取表名
            table_name = self._derive_table_name_from_path(array_path)
                
            for value in sorted(values):
                enum_data.append({
                    'array_path': array_path,
                    'enum_table': table_name,
                    'value': value,
                    'occurrence_count': '-'  # 暂不统计每个值的出现次数
                })
        
        pd.DataFrame(enum_data).to_csv(enum_csv, index=False)
        logger.info(f"已生成枚举值参考CSV: {enum_csv}")
        
        return {
            'tables_structure': tables_csv,
            'fields_mapping': fields_csv,
            'table_relationships': relationships_csv,
            'paths_statistics': paths_csv,
            'enum_values': enum_csv,
            'ddl_file': ddl_file
        }
    
    def generate_ddl(self, prefix):
        """生成建表SQL语句
        
        Args:
            prefix: 前缀
            
        Returns:
            str: SQL语句
        """
        ddl_lines = [
            f"-- 自动生成的{prefix}数据表结构",
            f"-- 生成日期: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"-- 分析文件数: {self.processed_files}",
            f"-- 分析记录数: {self.total_records}",
            f"-- 唯一字段路径: {len(self.all_paths)}",
            f"-- 数组路径: {len(self.array_paths)}",
            f"-- 简单数组: {sum(1 for is_simple in self.simple_array_flags.values() if is_simple)}",
            ""
        ]
        
        # 按表类型和关系排序：先主表，后子表，再孙表
        def table_sort_key(table_item):
            table_name, info = table_item
            if info['type'] == 'main':
                return 0, table_name
            elif info['parent'] and info['parent'] == prefix:
                return 1, table_name  # 直接子表
            else:
                return 2, table_name  # 孙表
                
        sorted_tables = sorted(self.tables.items(), key=table_sort_key)
        
        # 生成每个表的DDL
        for table_name, table_info in sorted_tables:
            # 所有表都使用标准表生成
            self._generate_standard_table_ddl(ddl_lines, table_name, table_info)
        
        return "\n".join(ddl_lines)
    
    def _generate_standard_table_ddl(self, ddl_lines, table_name, table_info):
        """生成标准表DDL
        
        Args:
            ddl_lines: DDL行列表
            table_name: 表名
            table_info: 表信息
        """
        ddl_lines.append(f"-- 添加UUID扩展 (如果尚未添加)")
        ddl_lines.append(f"CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";")
        ddl_lines.append(f"")
        ddl_lines.append(f"CREATE TABLE {table_name} (")
        ddl_lines.append(f"    id VARCHAR(128) PRIMARY KEY DEFAULT uuid_generate_v4(),")
        
        # 如果有父表，添加外键
        if table_info['parent']:
            parent_id = f"{table_info['parent'].split('_')[-1]}_id"
            ddl_lines.append(f"    {parent_id} VARCHAR(128) REFERENCES {table_info['parent']}(id),")
        
        # 添加字段
        processed_fields = set()  # 追踪已处理的字段以避免重复
        for field in table_info['fields']:
            field_name = field['name']
            
            # 跳过数组字段（它们将成为独立的表）和已处理的字段
            if field['is_array'] or field_name in processed_fields:
                continue
            
            processed_fields.add(field_name)
                
            # 推断数据类型
            sample = field['sample']
            data_type = self._infer_data_type(field_name, sample)
            
            # 添加字段定义
            ddl_lines.append(f"    {field_name} {data_type},")
        
        # 添加通用字段
        ddl_lines.append(f"    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,")
        ddl_lines.append(f"    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        ddl_lines.append(");")
        ddl_lines.append("")
        
        # 添加索引
        if table_info['parent']:
            parent_id = f"{table_info['parent'].split('_')[-1]}_id"
            ddl_lines.append(f"CREATE INDEX idx_{table_name}_{parent_id} ON {table_name}({parent_id});")
            ddl_lines.append("")
    
    def _generate_enum_table_ddl(self, ddl_lines, table_name, table_info):
        """生成枚举表DDL
        
        Args:
            ddl_lines: DDL行列表
            table_name: 表名
            table_info: 表信息
        """
        # 获取字段名和路径
        field_name = table_info['fields'][0]['name'] if table_info['fields'] else 'value'
        array_path = table_info.get('array_path', '')
        
        # 生成表注释
        ddl_lines.append(f"-- 枚举表: {table_name}")
        ddl_lines.append(f"-- 对应JSON路径: {array_path}")
        ddl_lines.append(f"CREATE TABLE {table_name} (")
        ddl_lines.append(f"    id VARCHAR(128) PRIMARY KEY DEFAULT uuid_generate_v4(),")
        
        # 添加父表外键
        if table_info['parent']:
            parent_id = f"{table_info['parent'].split('_')[-1]}_id"
            ddl_lines.append(f"    {parent_id} VARCHAR(128) REFERENCES {table_info['parent']}(id),")
        
        # 添加值字段
        ddl_lines.append(f"    {field_name} VARCHAR(255) NOT NULL,")
        
        # 添加通用字段
        ddl_lines.append(f"    occurrence_count VARCHAR(128) DEFAULT '1',")
        ddl_lines.append(f"    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,")
        ddl_lines.append(f"    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        ddl_lines.append(");")
        ddl_lines.append("")
        
        # 添加索引
        if table_info['parent']:
            parent_id = f"{table_info['parent'].split('_')[-1]}_id"
            ddl_lines.append(f"CREATE INDEX idx_{table_name}_{parent_id} ON {table_name}({parent_id});")
            ddl_lines.append(f"CREATE INDEX idx_{table_name}_value ON {table_name}({field_name});")
            ddl_lines.append("")
    
    def _is_date_field(self, field_name):
        """检查字段名是否为日期字段
        
        Args:
            field_name: 字段名
        
        Returns:
            bool: 是否为日期字段
        """
        lower_name = field_name.lower()
        
        # 检查常见的日期字段命名模式
        for pattern in self.date_field_patterns:
            if pattern.search(lower_name):
                return True
        
        # 检查特定名称
        date_keywords = ['date', 'dt', 'timestamp', 'time', 'year', 'month', 'day']
        for keyword in date_keywords:
            if keyword in lower_name:
                return True
        
        return False
    
    def _is_date_value(self, value):
        """检查值是否为日期格式
        
        Args:
            value: 样本值
        
        Returns:
            bool: 是否为日期格式
        """
        if not isinstance(value, str):
            return False
            
        # 检查常见的日期格式
        for pattern in self.date_value_patterns:
            if pattern.match(value):
                return True
                
        # 检查其他可能的日期格式
        date_formats = [
            '%Y-%m-%d',
            '%m/%d/%Y',
            '%d/%m/%Y',
            '%Y%m%d',
            '%b %d, %Y',
            '%B %d, %Y',
            '%d %b %Y',
            '%d %B %Y'
        ]
        
        for fmt in date_formats:
            try:
                datetime.strptime(value, fmt)
                return True
            except ValueError:
                continue
                
        return False
    
    def _infer_data_type(self, field_name, sample_value):
        """推断字段数据类型 (改进版3.0)
        
        Args:
            field_name: 字段名
            sample_value: 样本值
            
        Returns:
            str: SQL数据类型
        """
        lower_name = field_name.lower()
        
        # 特别处理已知的长文本字段 - 直接判定为TEXT
        known_text_fields = [
            'distribution_pattern', 'code_info', 'reason_for_recall', 'definition','special_conditions', 'udi_public','lot_number',
            'product_description', 'recall_reason', 'distribution','recalling_firm','action','registration_number','fei_number','pma_number','k_number',
            'recall_information', 'product_info', 'initial_firm_notification','product_quantity'
        ]
        if lower_name in known_text_fields:
            return "TEXT"
        
        # 检查是否为日期字段
        if self._is_date_field(field_name) or (isinstance(sample_value, str) and self._is_date_value(sample_value)):
            return "DATE"
        
        # 检查是否为布尔字段
        if (lower_name.endswith('_flag') or lower_name.startswith('is_') 
            or lower_name.startswith('has_') or 'flag' in lower_name):
            return "BOOLEAN"
        
        # 检查是否为ID或编号字段 - 但要验证样本值是否为纯数字
        if (lower_name.endswith('_id') or lower_name.endswith('_count') 
            or lower_name.endswith('_number') or lower_name.endswith('_key')):
            # 首先检查样本值是否存在
            if sample_value:
                # 尝试将样本值转换为整数，如果成功则使用VARCHAR(128)类型
                try:
                    if isinstance(sample_value, str):
                        int(sample_value)
                        return "VARCHAR(128)"
                except ValueError:
                    # 包含非数字字符的ID应该使用VARCHAR
                    return "VARCHAR(128)"
            return "VARCHAR(128)"  # 默认使用VARCHAR类型来处理混合格式的ID和编号
        
        # 检查是否为电话号码字段
        if ('phone' in lower_name or 'area_code' in lower_name 
            or 'extension' in lower_name or 'exchange' in lower_name):
            return "VARCHAR(128)"
        
        # 检查是否为邮政编码字段
        if ('zip' in lower_name or 'postal' in lower_name):
            return "VARCHAR(128)"
        
        # 检查是否为金额字段
        if ('price' in lower_name or 'cost' in lower_name 
            or 'amount' in lower_name or 'fee' in lower_name):
            return "DECIMAL(12,2)"
        
        # 扩展的文本字段识别 - 添加更多可能包含长文本的关键词
        if ('name' in lower_name or 'description' in lower_name 
            or 'text' in lower_name or 'narrative' in lower_name 
            or 'problem' in lower_name or 'comment' in lower_name
            or 'brand' in lower_name or 'generic' in lower_name
            or 'report' in lower_name or 'reason' in lower_name 
            or 'pattern' in lower_name or 'distribution' in lower_name
            or 'code' in lower_name or 'info' in lower_name
            or 'detail' in lower_name or 'note' in lower_name
            or 'summary' in lower_name or 'content' in lower_name):
            return "TEXT"  # 使用TEXT避免长度限制
        
        # 检查是否为地址字段 - 考虑使用TEXT而非VARCHAR防止长地址溢出
        if ('address' in lower_name or 'street' in lower_name):
            return "TEXT"  # 改为TEXT以适应长地址
        
        # 其他地址相关字段保持VARCHAR(255)
        if ('city' in lower_name or 'state' in lower_name 
            or 'country' in lower_name or 'location' in lower_name):
            return "VARCHAR(255)"
        
        # UDI相关字段
        if ('udi' in lower_name):
            return "VARCHAR(128)"
        
        # 设备相关代码
        if ('product_code' in lower_name or 'catalog' in lower_name or 'model' in lower_name):
            return "VARCHAR(128)"
        
        # 检查是否为枚举类型字段
        enum_fields = [
            'type', 'status', 'code', 'category', 'level', 'priority', 
            'severity', 'stage', 'phase', 'mode', 'method'
        ]
        for enum_field in enum_fields:
            if enum_field in lower_name:
                return "VARCHAR(128)"
        
        # 基于样本值判断 - 降低文本长度阈值
        if isinstance(sample_value, str):
            if sample_value.startswith('[Object') or sample_value.startswith('[Array'):
                return "TEXT"  # 复杂对象序列化为JSON
            
            # 尝试转换为数字
            try:
                float_val = float(sample_value)
                if float_val.is_integer():
                    return "VARCHAR(128)"
                return "DECIMAL(12,2)"
            except (ValueError, TypeError):
                pass
                
            # 尝试判断为布尔值
            if sample_value.lower() in ('true', 'false', 'yes', 'no', 'y', 'n', 't', 'f'):
                return "BOOLEAN"
            
            # 检查长度 - 降低阈值，使更多字段转为TEXT
            if len(sample_value) > 200:  # 从255降低到200
                return "TEXT"
            elif len(sample_value) > 100:  # 从128降低到100
                return "VARCHAR(255)"
            else:
                return "VARCHAR(128)"
        
        # 默认类型 - 保守起见，可以考虑从VARCHAR(255)改为TEXT
        return "VARCHAR(255)"

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='通用FDA JSON结构分析工具')
    parser.add_argument('--input_dir', required=True, help='输入目录路径')
    parser.add_argument('--data_type', required=True, help='数据类型标识(例如:event,recall,udi)')
    parser.add_argument('--pattern', help='文件名匹配模式(例如:device-event-*.json)')
    parser.add_argument('--output_dir', default='./output', help='输出目录')
    parser.add_argument('--max_files', type=int, default=10, help='最大处理文件数')
    parser.add_argument('--max_records', type=int, default=100000, help='每个文件最大处理记录数')
    parser.add_argument('--recursive', action='store_true', help='是否递归搜索子目录')
    parser.add_argument('--log_level', default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], help='日志级别')
    
    args = parser.parse_args()
    
    # 设置日志级别
    logger.setLevel(getattr(logging, args.log_level))
    
    # 设置默认模式
    if not args.pattern:
        args.pattern = f"device-{args.data_type}-*.json"
    
    # 创建分析器
    analyzer = BatchJsonAnalyzer(args.output_dir)
    
    # 处理目录
    result = analyzer.process_directory(
        args.input_dir, args.data_type, args.pattern, 
        args.max_files, args.max_records, args.recursive
    )
    
    if result:
        logger.info("\n批量分析结果:")
        logger.info(f"  数据类型: {result['data_type']}")
        logger.info(f"  处理文件数: {result['processed_files']}")
        logger.info(f"  处理记录数: {result['total_records']}")
        logger.info(f"  唯一路径数: {result['unique_paths']}")
        logger.info(f"  嵌套路径数: {result['nested_paths']}")
        logger.info(f"  数组路径数: {result['array_paths']}")
        logger.info(f"  简单数组数: {result['simple_arrays']}")
        logger.info(f"  识别表数: {result['tables']}")
        
        # 导出表结构
        output_files = analyzer.export_table_structure(args.data_type)
        
        logger.info("\n输出文件:")
        for file_type, file_path in output_files.items():
            logger.info(f"  {file_type}: {file_path}")
    else:
        logger.error("分析失败!")
        logger.info("提示: 尝试添加 --recursive 参数以搜索子目录")


if __name__ == "__main__":
    main()