#!/usr/bin/env python3

import os
import sys
import json
import re
import glob
import argparse
import pandas as pd
import random
from collections import defaultdict, Counter
import itertools
import logging
import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('fda_analyzer.log', mode='w')
    ]
)
logger = logging.getLogger(__name__)


class JsonFileAnalyzer:
    
    def __init__(self):
        self.field_samples = {}
        self.primary_key_candidates = set()
    
    def find_all_paths(self, json_obj, prefix="", path="", object_paths=None, array_paths=None):
        paths = []
        current_path = f"{path}.{prefix}" if path else prefix
        
        if isinstance(json_obj, dict):
            if current_path and object_paths is not None:
                object_paths.add(current_path)
            
            for key, value in json_obj.items():
                new_path = f"{current_path}.{key}" if current_path else key
                paths.append(new_path)
                
                if new_path not in self.field_samples:
                    self.field_samples[new_path] = self._get_sample_value(value)
                    
                    if self._is_potential_key_field(key):
                        self.primary_key_candidates.add(new_path)
                
                if isinstance(value, (dict, list)):
                    nested = self.find_all_paths(value, key, current_path, object_paths, array_paths)
                    paths.extend(nested)
        
        elif isinstance(json_obj, list):
            array_path = f"{current_path}[]"
            if array_paths is not None:
                array_paths.add(array_path)
            paths.append(array_path)
            
            for i, item in enumerate(json_obj[:3]):
                if isinstance(item, (dict, list)):
                    item_path = f"{current_path}[{i}]"
                    item_paths = self.find_all_paths(item, f"{prefix}[{i}]", path, object_paths, array_paths)
                    paths.extend(item_paths)
        
        return paths
    
    def _is_potential_key_field(self, key):
        lower_key = key.lower()
        if lower_key == 'id' or lower_key == 'uuid':
            return True
        
        key_suffixes = ['_id', '_key', '_uuid', '_number', '_code']
        for suffix in key_suffixes:
            if lower_key.endswith(suffix):
                non_key_patterns = ['version_id', 'parent_id', 'related_id', 'reference_id']
                if not any(pattern in lower_key for pattern in non_key_patterns):
                    return True
        
        return False
    
    def _get_sample_value(self, value):
        if isinstance(value, (dict, list)):
            if isinstance(value, dict):
                return f"[Object with {len(value)} keys]"
            else:
                return f"[Array with {len(value)} items]"
        elif value is None:
            return "null"
        else:
            return value


class BatchJsonAnalyzer:
    
    def __init__(self, output_dir="./output", data_type=None):
        self.output_dir = output_dir
        self.data_type = data_type
        
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            logger.info(f"创建输出目录: {output_dir}")
        
        self.all_paths = set()
        self.nested_paths = []
        self.object_paths = set()
        self.array_paths = set()
        self.field_samples = {}
        self.tables = {}
        self.path_counter = Counter()
        self.processed_files = 0
        self.total_records = 0
        self.primary_key_candidates = set()
        self.unique_identifiers = defaultdict(set)
        self.field_cardinality = {}
        self.field_value_distribution = defaultdict(Counter)
        self.field_max_length = {}
    
    def guess_data_type_from_file(self, file_path):
        file_name = os.path.basename(file_path)
        file_dir = os.path.basename(os.path.dirname(file_path))
        
        logger.debug(f"尝试从文件名 {file_name} 和目录 {file_dir} 推断数据类型")
        
        valid_types = {
            'k510', 'classification', 'covid19serology', 'enforcement', 
            'event', 'pma', 'recall', 'registrationlisting', 'udi',
            'adverse', 'drug', 'device', 'food', 'animalandveterinary'
        }
        
        if file_dir.lower() in valid_types:
            logger.info(f"从目录名 {file_dir} 推断出数据类型")
            return file_dir.lower()
        
        device_patterns = [
            r'device-(\w+)-\d+',
            r'(\w+)-\d+',
        ]
        
        for pattern in device_patterns:
            match = re.search(pattern, file_name)
            if match:
                data_type = match.group(1).lower()
                if data_type not in ['of', 'part', 'section']:
                    if data_type == "510k":
                        data_type = "k510"
                    logger.info(f"从文件名 {file_name} 使用模式 {pattern} 推断出数据类型: {data_type}")
                    return data_type
        
        for type_name in valid_types:
            if type_name in file_name.lower():
                logger.info(f"从文件名 {file_name} 中发现类型名: {type_name}")
                return type_name
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data_sample = f.read(10240)
                try:
                    data = json.loads(data_sample)
                    
                    if isinstance(data, dict):
                        if 'type' in data:
                            type_value = data['type']
                            if type_value == "510k":
                                return "k510"
                            return type_value
                        
                        if 'meta' in data and isinstance(data['meta'], dict):
                            if 'data_type' in data['meta']:
                                data_type = data['meta']['data_type']
                                if data_type == "510k":
                                    return "k510"
                                return data_type
                            if 'type' in data['meta']:
                                type_value = data['meta']['type']
                                if type_value == "510k":
                                    return "k510"
                                return type_value
                        
                        for key in ['results', 'data', 'records']:
                            if key in data and isinstance(data[key], list) and len(data[key]) > 0:
                                if isinstance(data[key][0], dict):
                                    record = data[key][0]
                                    for type_key in ['report_type', 'type', 'data_type', 'category', 'submission_type']:
                                        if type_key in record:
                                            type_value = record[type_key]
                                            if type_value == "510k":
                                                return "k510"
                                            return type_value
                except json.JSONDecodeError:
                    pass
        except Exception as e:
            logger.warning(f"无法从内容推断文件 {file_path} 的类型: {str(e)}")
        
        dir_parts = os.path.dirname(file_path).split(os.sep)
        for part in reversed(dir_parts):
            part_lower = part.lower()
            if part_lower == "510k":
                logger.info(f"从目录路径 {part} 推断出数据类型: k510")
                return "k510"
            if part_lower in valid_types:
                logger.info(f"从目录路径 {part} 推断出数据类型")
                return part_lower
        
        logger.warning(f"无法确定 {file_path} 的数据类型，使用默认类型 'unknown'")
        return "unknown"
    
    def find_json_files(self, input_dir, pattern, recursive=False):
        matched_files = []
        
        if recursive:
            for root, dirs, files in os.walk(input_dir):
                for file in files:
                    if self._match_pattern(file, pattern):
                        matched_files.append(os.path.join(root, file))
        else:
            search_pattern = os.path.join(input_dir, pattern)
            matched_files = glob.glob(search_pattern)
        
        return matched_files
    
    def _match_pattern(self, filename, pattern):
        regex_pattern = pattern.replace(".", "\\.").replace("*", ".*").replace("?", ".")
        return re.match(regex_pattern, filename) is not None
    
    def process_directory(self, input_dir, pattern=None, max_files=10, 
                         max_records_per_file=100, recursive=False):
        data_type = self.data_type
        
        if pattern is None:
            if data_type:
                if data_type == "k510":
                    pattern = f"*510k*.json"
                else:
                    pattern = f"*{data_type}*.json"
            else:
                pattern = f"*.json"
        
        files = self.find_json_files(input_dir, pattern, recursive)
        
        if not files:
            logger.warning(f"未找到匹配的文件: {os.path.join(input_dir, pattern)}")
            logger.warning(f"注意: {'启用' if recursive else '未启用'}递归搜索子目录")
            return None
        
        files = files[:min(len(files), max_files)]
        
        logger.info(f"找到 {len(files)} 个匹配文件，将处理其中 {len(files)} 个")
        
        if not data_type and files:
            self.data_type = data_type = self.guess_data_type_from_file(files[0])
            logger.info(f"从文件推断数据类型: {data_type}")
        
        for i, file_path in enumerate(files):
            logger.info(f"处理文件 {i+1}/{len(files)}: {os.path.basename(file_path)} ({file_path})")
            self.process_file(file_path, data_type, max_records_per_file)
        
        if data_type:
            self.identify_tables(data_type)
            self._calculate_field_statistics()
        
        return {
            'data_type': data_type,
            'processed_files': self.processed_files,
            'total_records': self.total_records,
            'unique_paths': len(self.all_paths),
            'nested_paths': len(self.nested_paths),
            'tables': len(self.tables)
        }
    
    def process_file(self, file_path, data_type, max_records=100):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            records = []
            if 'results' in data and isinstance(data['results'], list):
                records = data['results'][:max_records]
            elif isinstance(data, list):
                records = data[:max_records]
            else:
                records = [data]
            
            record_count = 0
            file_analyzer = JsonFileAnalyzer()
            
            for record in records:
                paths = file_analyzer.find_all_paths(record, data_type, "", self.object_paths, self.array_paths)
                
                for path in paths:
                    self.path_counter[path] += 1
                    self.all_paths.add(path)
                
                for path, value in file_analyzer.field_samples.items():
                    if path not in self.field_samples:
                        self.field_samples[path] = value
                    
                    if isinstance(value, str):
                        current_max = self.field_max_length.get(path, 0)
                        self.field_max_length[path] = max(current_max, len(value))
                    
                    if not isinstance(value, (dict, list)) and value is not None:
                        str_value = str(value)
                        if len(self.field_value_distribution[path]) < 50:
                            self.field_value_distribution[path][str_value] += 1
                    
                    for candidate in file_analyzer.primary_key_candidates:
                        if candidate == path and isinstance(value, (str, int, float)):
                            self.unique_identifiers[candidate].add(str(value))
                
                self.primary_key_candidates.update(file_analyzer.primary_key_candidates)
                
                record_count += 1
            
            self.processed_files += 1
            self.total_records += record_count
            
            return record_count
            
        except Exception as e:
            logger.error(f"处理文件 {file_path} 时出错: {str(e)}", exc_info=True)
            return 0
    
    def _calculate_field_statistics(self):
        for path, values in self.unique_identifiers.items():
            if path in self.path_counter and self.path_counter[path] > 0:
                cardinality = len(values) / self.path_counter[path]
                self.field_cardinality[path] = cardinality
    
    def identify_tables(self, main_type):
        if main_type == "k510":
            table_main_type = "fda_k510"
        else:
            table_main_type = main_type
            
        main_table = f"{table_main_type}"
        self.tables[main_table] = {
            'name': main_table,
            'type': 'main',
            'parent': None,
            'fields': [],
            'has_arrays': False,
            'primary_keys': [],
            'recommended_unique_keys': []
        }
        
        sorted_paths = sorted(
            [(path, count) for path, count in self.path_counter.items()],
            key=lambda x: (x[0].count('.'), -x[1])
        )
        
        for path, count in sorted_paths:
            parts = path.split('.')
            
            if len(parts) <= 2:
                if len(parts) == 2:
                    field_name = parts[1]
                    self._add_field_to_table(main_table, field_name, path, count)
                    
                    if path in self.array_paths:
                        self.tables[main_table]['has_arrays'] = True
                    
                    if path in self.primary_key_candidates:
                        cardinality = self.field_cardinality.get(path, 0)
                        if cardinality > 0.9:
                            if field_name not in self.tables[main_table]['primary_keys']:
                                self.tables[main_table]['primary_keys'].append(field_name)
                        elif cardinality > 0.5:
                            if field_name not in self.tables[main_table]['recommended_unique_keys']:
                                self.tables[main_table]['recommended_unique_keys'].append(field_name)
            else:
                self._assign_field_to_table(table_main_type, path, parts, count)
        
        for table_name in self.tables:
            if not self.tables[table_name]['primary_keys'] and 'recommended_unique_keys' in self.tables[table_name]:
                if self.tables[table_name]['recommended_unique_keys']:
                    self.tables[table_name]['primary_keys'] = ['id']
                    self.tables[table_name]['unique_constraints'] = self.tables[table_name]['recommended_unique_keys']
                else:
                    self.tables[table_name]['primary_keys'] = ['id']
            elif len(self.tables[table_name]['primary_keys']) > 3:
                unique_fields = self.tables[table_name]['primary_keys'][3:]
                self.tables[table_name]['primary_keys'] = ['id'] + self.tables[table_name]['primary_keys'][:2]
                self.tables[table_name]['unique_constraints'] = unique_fields
            self.tables[table_name]['primary_keys'] = list(dict.fromkeys(self.tables[table_name]['primary_keys']))
        
        self._optimize_table_structure()
        
        return self.tables
    
    def _optimize_table_structure(self):
        tables_to_remove = []
        
        for table_name, table_info in self.tables.items():
            if (table_info['type'] != 'main' and 
                len(table_info['fields']) <= 2 and 
                '_' in table_name and 
                table_name.count('_') >= 2):
                
                parent_table = table_info['parent']
                if parent_table in self.tables:
                    if table_info['type'] == 'array' and all(not f.get('is_array') for f in table_info['fields']):
                        field_name = table_name.split('_')[-1]
                        array_field = {
                            'name': f"{field_name}_array",
                            'path': f"{parent_table}.{field_name}",
                            'count': max(f['count'] for f in table_info['fields']),
                            'is_array': True,
                            'is_array_type': True,
                            'sample': f"ARRAY[values of {field_name}]",
                            'original_table': table_name
                        }
                        self.tables[parent_table]['fields'].append(array_field)
                        tables_to_remove.append(table_name)
                    elif len(table_info['fields']) == 1:
                        field = table_info['fields'][0]
                        parent_field = {
                            'name': f"{table_name.split('_')[-1]}_{field['name']}",
                            'path': field['path'],
                            'count': field['count'],
                            'is_array': field.get('is_array', False),
                            'sample': field.get('sample', ''),
                            'from_merged_table': table_name
                        }
                        self.tables[parent_table]['fields'].append(parent_field)
                        tables_to_remove.append(table_name)
        
        for table in tables_to_remove:
            if table in self.tables:
                del self.tables[table]
    
    def _add_field_to_table(self, table_name, field_name, path, count):
        if table_name not in self.tables:
            return
            
        if any(f['name'] == field_name for f in self.tables[table_name]['fields']):
            return
        
        if field_name[0].isdigit() or field_name.startswith('_'):
            field_name = f"fld_{field_name}"
        
        use_jsonb = False
        if path in self.object_paths and field_name.lower() not in ('id', 'key', 'code', 'number'):
            child_count = sum(1 for p in self.all_paths if p.startswith(f"{path}.") and p.count('.') == path.count('.')+1)
            if child_count >= 5:
                use_jsonb = True
        
        field = {
            'name': field_name,
            'path': path,
            'count': count,
            'is_array': path in self.array_paths,
            'sample': self.field_samples.get(path, ''),
            'is_primary_key': path in self.primary_key_candidates,
            'unique_values': len(self.unique_identifiers.get(path, set())),
            'max_length': self.field_max_length.get(path, 0),
            'value_distribution': dict(self.field_value_distribution.get(path, Counter())),
            'cardinality': self.field_cardinality.get(path, 0),
            'use_jsonb': use_jsonb
        }
        
        self.tables[table_name]['fields'].append(field)
    
    def _assign_field_to_table(self, main_type, path, parts, count):
        if len(parts) > 3 and not any('[' in part for part in parts):
            parent_path = '.'.join(parts[:-1])
            siblings = sum(1 for p in self.all_paths if p.startswith(f"{parent_path}.") and p.count('.') == parent_path.count('.')+1)
            if siblings <= 3:
                table_path = '.'.join(parts[:2])
                if main_type == "k510":
                    table_name = f"fda_k510_{parts[1]}"
                else:
                    table_name = f"{main_type}_{parts[1]}"
                
                if table_name in self.tables:
                    object_name = '_'.join(parts[2:-1])
                    field_name = f"{object_name}_{parts[-1]}"
                    
                    if field_name[0].isdigit() or field_name.startswith('_'):
                        field_name = f"fld_{field_name}"
                    
                    self._add_field_to_table(table_name, field_name, path, count)
                    return
        
        if '[' in path:
            array_path = path.split('[')[0]
            array_parts = array_path.split('.')
            
            if len(array_parts) >= 2:
                array_name = array_parts[-1]
                parent_obj = array_parts[1] if len(array_parts) > 2 else main_type
                
                if parent_obj == main_type:
                    if main_type == "k510":
                        table_name = f"fda_k510_{array_name}"
                        parent_table = "fda_k510"
                    else:
                        table_name = f"{main_type}_{array_name}"
                        parent_table = main_type
                else:
                    if len(array_parts) > 3:
                        if main_type == "k510":
                            table_name = f"fda_k510_{parent_obj}_{array_name}"
                        else:
                            table_name = f"{main_type}_{parent_obj}_{array_name}"
                    else:
                        if main_type == "k510":
                            table_name = f"fda_k510_{parent_obj}_{array_name}"
                        else:
                            table_name = f"{main_type}_{parent_obj}_{array_name}"
                    
                    if main_type == "k510":
                        parent_table = "fda_k510"
                    else:
                        parent_table = main_type
                
                if table_name not in self.tables:
                    self.tables[table_name] = {
                        'name': table_name,
                        'type': 'array',
                        'parent': parent_table,
                        'fields': [],
                        'has_arrays': False,
                        'array_path': array_path,
                        'primary_keys': [],
                        'recommended_unique_keys': []
                    }
                
                leaf_name = parts[-1]
                if '[' in leaf_name:
                    leaf_name = leaf_name.split('[')[0]
                
                if leaf_name[0].isdigit() or leaf_name.startswith('_'):
                    leaf_name = f"fld_{leaf_name}"
                
                self._add_field_to_table(table_name, leaf_name, path, count)
                
                if path in self.array_paths:
                    self.tables[table_name]['has_arrays'] = True
                    
                if path in self.primary_key_candidates:
                    cardinality = self.field_cardinality.get(path, 0)
                    if cardinality > 0.9:
                        if table_name in self.tables and leaf_name not in self.tables[table_name]['primary_keys']:
                            self.tables[table_name]['primary_keys'].append(leaf_name)
                    elif cardinality > 0.5:
                        if table_name in self.tables and leaf_name not in self.tables[table_name]['recommended_unique_keys']:
                            self.tables[table_name]['recommended_unique_keys'].append(leaf_name)
        
        elif len(parts) > 2:
            parent_obj = parts[1]
            
            if len(parts) > 3:
                object_path = '.'.join(parts[:3])
                siblings = sum(1 for p in self.all_paths if p.startswith(f"{object_path}.") and p not in self.object_paths)
                
                if siblings <= 5 and object_path in self.object_paths:
                    if main_type == "k510":
                        table_name = f"fda_k510_{parent_obj}"
                    else:
                        table_name = f"{main_type}_{parent_obj}"
                    
                    if table_name not in self.tables:
                        if main_type == "k510":
                            parent_table_name = "fda_k510"
                        else:
                            parent_table_name = main_type
                            
                        self.tables[table_name] = {
                            'name': table_name,
                            'type': 'object',
                            'parent': parent_table_name,
                            'fields': [],
                            'has_arrays': False,
                            'object_path': f"{main_type}.{parent_obj}",
                            'primary_keys': [],
                            'recommended_unique_keys': []
                        }
                    
                    jsonb_field_name = parts[2]
                    jsonb_field_path = object_path
                    
                    if jsonb_field_name[0].isdigit() or jsonb_field_name.startswith('_'):
                        jsonb_field_name = f"fld_{jsonb_field_name}"
                    
                    if not any(f['name'] == jsonb_field_name and f.get('use_jsonb') for f in self.tables[table_name]['fields']):
                        jsonb_field = {
                            'name': jsonb_field_name,
                            'path': jsonb_field_path,
                            'count': count,
                            'is_array': False,
                            'sample': f"[Object with nested fields]",
                            'is_primary_key': False,
                            'use_jsonb': True,
                            'jsonb_subfields': []
                        }
                        self.tables[table_name]['fields'].append(jsonb_field)
                    
                    for field in self.tables[table_name]['fields']:
                        if field['name'] == jsonb_field_name and field.get('use_jsonb'):
                            if 'jsonb_subfields' not in field:
                                field['jsonb_subfields'] = []
                            field['jsonb_subfields'].append(parts[-1])
                    
                    return
            
            if main_type == "k510":
                table_name = f"fda_k510_{parent_obj}"
            else:
                table_name = f"{main_type}_{parent_obj}"
            
            if table_name not in self.tables:
                if main_type == "k510":
                    parent_table_name = "fda_k510"
                else:
                    parent_table_name = main_type
                
                self.tables[table_name] = {
                    'name': table_name,
                    'type': 'object',
                    'parent': parent_table_name,
                    'fields': [],
                    'has_arrays': False,
                    'object_path': f"{main_type}.{parent_obj}",
                    'primary_keys': [],
                    'recommended_unique_keys': []
                }
            
            if len(parts) > 3:
                prefix = '_'.join(parts[2:-1])
                leaf_name = f"{prefix}_{parts[-1]}"
            else:
                leaf_name = parts[-1]
            
            if leaf_name[0].isdigit() or leaf_name.startswith('_'):
                leaf_name = f"fld_{leaf_name}"
                
            self._add_field_to_table(table_name, leaf_name, path, count)
            
            if path in self.array_paths:
                self.tables[table_name]['has_arrays'] = True
                
            if path in self.primary_key_candidates:
                cardinality = self.field_cardinality.get(path, 0)
                if cardinality > 0.9:
                    if table_name in self.tables and leaf_name not in self.tables[table_name]['primary_keys']:
                        self.tables[table_name]['primary_keys'].append(leaf_name)
                elif cardinality > 0.5:
                    if table_name in self.tables and leaf_name not in self.tables[table_name]['recommended_unique_keys']:
                        self.tables[table_name]['recommended_unique_keys'].append(leaf_name)
    
    def export_table_structure(self, prefix):
        if not self.tables:
            logger.warning("没有表结构可导出")
            return None
        
        if prefix == "k510":
            export_prefix = "fda_k510"
        else:
            export_prefix = prefix
        
        tables_data = []
        for table_name, table_info in self.tables.items():
            tables_data.append({
                'table_name': table_info['name'],
                'table_type': table_info['type'],
                'parent_table': table_info['parent'] or '',
                'field_count': len(table_info['fields']),
                'has_arrays': 'Yes' if table_info['has_arrays'] else 'No',
                'path': table_info.get('array_path', table_info.get('object_path', '')),
                'primary_keys': ','.join(table_info.get('primary_keys', [])),
                'unique_constraints': ','.join(table_info.get('unique_constraints', []))
            })
        
        tables_csv = os.path.join(self.output_dir, f"{export_prefix}_tables.csv")
        pd.DataFrame(tables_data).to_csv(tables_csv, index=False)
        logger.info(f"已生成表结构CSV: {tables_csv}")
        
        all_fields = []
        for table_name, table_info in self.tables.items():
            for field in table_info['fields']:
                field_data = {
                    'table_name': table_name,
                    'field_name': field['name'],
                    'original_path': field['path'],
                    'is_array': 'Yes' if field.get('is_array', False) else 'No',
                    'is_jsonb': 'Yes' if field.get('use_jsonb', False) else 'No',
                    'occurrence_count': field['count'],
                    'occurrence_percent': f"{(field['count'] / self.total_records) * 100:.1f}%",
                    'sample_value': field['sample'],
                    'is_primary_key': 'Yes' if field.get('is_primary_key', False) else 'No',
                    'unique_values': field.get('unique_values', 0),
                    'max_length': field.get('max_length', 0),
                    'cardinality': f"{field.get('cardinality', 0):.2f}"
                }
                all_fields.append(field_data)
        
        fields_csv = os.path.join(self.output_dir, f"{export_prefix}_fields.csv")
        pd.DataFrame(all_fields).to_csv(fields_csv, index=False)
        logger.info(f"已生成字段映射CSV: {fields_csv}")
        
        relationships = []
        for table_name, table_info in self.tables.items():
            if table_info['parent']:
                parent_name = table_info['parent']
                foreign_key = f"{parent_name}_id"
                
                relationships.append({
                    'child_table': table_name,
                    'parent_table': parent_name,
                    'relationship_type': '一对多',
                    'foreign_key_field': foreign_key,
                    'path': table_info.get('array_path', table_info.get('object_path', '')),
                    'description': f"{table_name}是{parent_name}的子表"
                })
        
        relationships_csv = os.path.join(self.output_dir, f"{export_prefix}_relationships.csv")
        pd.DataFrame(relationships).to_csv(relationships_csv, index=False)
        logger.info(f"已生成表关系CSV: {relationships_csv}")
        
        ddl = self.generate_ddl(export_prefix)
        ddl_file = os.path.join(self.output_dir, f"{export_prefix}_tables_optimized.sql")
        with open(ddl_file, 'w', encoding='utf-8') as f:
            f.write(ddl)
        logger.info(f"已生成优化后的表DDL: {ddl_file}")
        
        paths_csv = os.path.join(self.output_dir, f"{export_prefix}_paths.csv")
        paths_data = []
        for path, count in sorted(self.path_counter.items(), key=lambda x: (-x[1], x[0])):
            paths_data.append({
                'path': path,
                'count': count,
                'percent': f"{(count / self.total_records) * 100:.1f}%",
                'is_array': 'Yes' if path in self.array_paths else 'No',
                'is_object': 'Yes' if path in self.object_paths else 'No',
                'depth': path.count('.'),
                'sample': self.field_samples.get(path, ''),
                'is_primary_key_candidate': 'Yes' if path in self.primary_key_candidates else 'No',
                'cardinality': f"{self.field_cardinality.get(path, 0):.2f}",
                'unique_values': len(self.unique_identifiers.get(path, set()))
            })
        
        pd.DataFrame(paths_data).to_csv(paths_csv, index=False)
        logger.info(f"已生成路径统计CSV: {paths_csv}")
        
        return {
            'tables_structure': tables_csv,
            'fields_mapping': fields_csv,
            'table_relationships': relationships_csv,
            'paths_statistics': paths_csv,
            'ddl_file': ddl_file
        }
    
    def generate_ddl(self, prefix):
        ddl_lines = [
            f"-- 自动生成的{prefix}数据表结构 (优化版)",
            f"-- 生成日期: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"-- 分析文件数: {self.processed_files}",
            f"-- 分析记录数: {self.total_records}",
            f"-- 唯一字段路径: {len(self.all_paths)}",
            ""
        ]
        
        ddl_lines.extend(self._generate_domain_definitions())
        
        def table_sort_key(table_item):
            table_name, info = table_item
            if info['type'] == 'main':
                return 0, table_name
            elif info['parent'] and info['parent'] == prefix:
                return 1, table_name
            else:
                return 2, table_name
                
        sorted_tables = sorted(self.tables.items(), key=table_sort_key)
        
        text_search_indexes = []
        enums_to_create = {}
        
        for table_name, table_info in sorted_tables:
            field_types = {}
            
            for field in table_info['fields']:
                if not field.get('use_jsonb') and not field.get('is_array') and 'value_distribution' in field:
                    distribution = field.get('value_distribution', {})
                    if (5 <= len(distribution) <= 15 and 
                        all(isinstance(v, str) for v in distribution.keys()) and 
                        max(len(v) for v in distribution.keys()) < 50):
                        if field['name'].lower() not in enums_to_create:
                            enums_to_create[field['name'].lower()] = sorted(distribution.keys())
            
            ddl_lines.append(f"CREATE TABLE {table_name} (")
            
            ddl_lines.append(f"    id SERIAL,")
            
            if table_info['parent']:
                parent_id = f"{table_info['parent']}_id"
                ddl_lines.append(f"    {parent_id} INTEGER,")
            
            processed_fields = set()
            field_defs = []
            
            for field in table_info['fields']:
                field_name = field['name']
                if field_name[0].isdigit() or field_name.startswith('_'):
                    field_name = f"fld_{field_name}"
                
                if field_name in processed_fields:
                    continue
                
                processed_fields.add(field_name)
                
                if field.get('use_jsonb', False):
                    data_type = "JSONB"
                elif field.get('is_array_type', False):
                    element_type = self._infer_array_element_type(field)
                    data_type = f"{element_type}[]"
                else:
                    data_type = self._infer_data_type(field_name, field['sample'], field)
                
                field_types[field_name] = data_type
                
                field_defs.append(f"    {field_name} {data_type},")
                
                if data_type == "TEXT" and field_name.lower() in [
                    'description', 'reason', 'text', 'summary', 'notes', 'comments',
                    'distribution_pattern', 'reason_for_recall', 'product_description'
                ]:
                    text_search_indexes.append((table_name, field_name))
            
            ddl_lines.extend(field_defs)
            
            ddl_lines.append(f"    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,")
            ddl_lines.append(f"    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
            
            if table_info.get('primary_keys'):
                primary_keys = table_info['primary_keys'][:3]
                
                if 'id' not in primary_keys:
                    primary_keys = ['id'] + primary_keys
                
                pk_constraint = f"    PRIMARY KEY ({', '.join(primary_keys)})"
                ddl_lines[-1] += ","
                ddl_lines.append(pk_constraint)
            else:
                ddl_lines[-1] += ","
                ddl_lines.append(f"    PRIMARY KEY (id)")
            
            ddl_lines.append(");")
            ddl_lines.append("")
            
            ddl_lines.append(f"COMMENT ON TABLE {table_name} IS '{self._get_table_description(table_name, table_info)}';")
            ddl_lines.append("")
            
            for field in table_info['fields']:
                field_name = field['name']
                if field_name[0].isdigit() or field_name.startswith('_'):
                    field_name = f"fld_{field_name}"
                
                if field_name in processed_fields:
                    field_desc = self._get_field_description(field)
                    ddl_lines.append(f"COMMENT ON COLUMN {table_name}.{field_name} IS '{field_desc}';")
            ddl_lines.append("")
            
            if 'unique_constraints' in table_info and table_info['unique_constraints']:
                for i, uc_field in enumerate(table_info['unique_constraints']):
                    if uc_field[0].isdigit() or uc_field.startswith('_'):
                        uc_field = f"fld_{uc_field}"
                        
                    if uc_field in processed_fields:
                        uc_name = f"uq_{table_name}_{uc_field}"
                        ddl_lines.append(f"ALTER TABLE {table_name} ADD CONSTRAINT {uc_name} UNIQUE ({uc_field});")
                ddl_lines.append("")
            
            for field in table_info['fields']:
                field_name = field['name']
                if field_name[0].isdigit() or field_name.startswith('_'):
                    field_name = f"fld_{field_name}"
                    
                if field_name in processed_fields:
                    constraints = self._generate_check_constraints(table_name, field, field_name)
                    if constraints:
                        ddl_lines.extend(constraints)
                        ddl_lines.append("")
        
        if enums_to_create:
            ddl_lines.append("-- 创建枚举类型")
            for enum_name, values in enums_to_create.items():
                safe_values = []
                for v in values:
                    escaped_value = str(v).replace("'", "''")
                    safe_values.append(f"'{escaped_value}'")
                ddl_lines.append(f"CREATE TYPE {enum_name}_enum AS ENUM ({', '.join(safe_values)});")
            ddl_lines.append("")
        
        ddl_lines.append("-- 添加父子表关系外键")
        for table_name, table_info in sorted_tables:
            if table_info['parent']:
                parent_table = table_info['parent']
                parent_id = f"{parent_table}_id"
                constraint_name = f"fk_{table_name}_{parent_id}"
                
                ddl_lines.append(f"ALTER TABLE {table_name} ADD CONSTRAINT {constraint_name}")
                ddl_lines.append(f"    FOREIGN KEY ({parent_id}) REFERENCES {parent_table}(id);")
                ddl_lines.append("")
        
        ddl_lines.append("-- 添加索引")
        
        fk_indexes = set()
        for table_name, table_info in sorted_tables:
            if table_info['parent']:
                parent_id = f"{table_info['parent']}_id"
                index_name = f"idx_{table_name}_{parent_id}"
                if index_name not in fk_indexes:
                    fk_indexes.add(index_name)
                    ddl_lines.append(f"CREATE INDEX {index_name} ON {table_name}({parent_id});")
        ddl_lines.append("")
        
        ddl_lines.append("-- 为重要查询字段添加索引")
        indexed_fields = set()
        for table_name, table_info in sorted_tables:
            for field in table_info['fields']:
                field_name = field['name']
                if field_name[0].isdigit() or field_name.startswith('_'):
                    field_name = f"fld_{field_name}"
                
                if field.get('is_primary_key') and field_name != 'id':
                    index_key = f"{table_name}.{field_name}"
                    if index_key not in indexed_fields:
                        indexed_fields.add(index_key)
                        ddl_lines.append(f"CREATE INDEX idx_{table_name}_{field_name} ON {table_name}({field_name});")
                
                lower_name = field_name.lower()
                if any(kw in lower_name for kw in ['date', 'time', 'status', 'type', 'code', 'number']) and not field.get('use_jsonb'):
                    if any(kw in lower_name for kw in ['date', 'time']):
                        index_key = f"{table_name}.{field_name}"
                        if index_key not in indexed_fields:
                            indexed_fields.add(index_key)
                            ddl_lines.append(f"CREATE INDEX idx_{table_name}_{field_name}_brin ON {table_name} USING BRIN ({field_name});")
                    elif 'status' in lower_name:
                        index_key = f"{table_name}.{field_name}"
                        if index_key not in indexed_fields:
                            indexed_fields.add(index_key)
                            ddl_lines.append(f"CREATE INDEX idx_{table_name}_{field_name} ON {table_name}({field_name});")
                            ddl_lines.append(f"CREATE INDEX idx_{table_name}_{field_name}_active ON {table_name}({field_name}) WHERE {field_name} IN ('active', 'open', 'pending');")
        ddl_lines.append("")
        
        ddl_lines.append("-- 添加JSONB字段索引")
        for table_name, table_info in sorted_tables:
            for field in table_info['fields']:
                field_name = field['name']
                if field_name[0].isdigit() or field_name.startswith('_'):
                    field_name = f"fld_{field_name}"
                    
                if field.get('use_jsonb'):
                    index_key = f"{table_name}.{field_name}"
                    if index_key not in indexed_fields:
                        indexed_fields.add(index_key)
                        ddl_lines.append(f"CREATE INDEX idx_{table_name}_{field_name}_gin ON {table_name} USING GIN ({field_name});")
        ddl_lines.append("")
        
        ddl_lines.append("-- 添加全文搜索索引")
        for table_name, field_name in text_search_indexes:
            if field_name[0].isdigit() or field_name.startswith('_'):
                field_name = f"fld_{field_name}"
                
            index_key = f"{table_name}.{field_name}"
            if index_key not in indexed_fields:
                indexed_fields.add(index_key)
                ddl_lines.append(f"CREATE INDEX idx_{table_name}_{field_name}_tsvector ON {table_name} USING GIN (to_tsvector('english', {field_name}));")
        ddl_lines.append("")
        
        ddl_lines.append("-- 添加自动更新时间戳触发器")
        ddl_lines.append("""
CREATE OR REPLACE FUNCTION update_modified_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
        """)
        
        for table_name, _ in sorted_tables:
            ddl_lines.append(f"""
CREATE TRIGGER update_timestamp
BEFORE UPDATE ON {table_name}
FOR EACH ROW EXECUTE FUNCTION update_modified_column();
            """)
        
        return "\n".join(ddl_lines)
    
    def _generate_domain_definitions(self):
        domains = [
            "-- 创建通用域定义",
            "-- 这些类型定义可以重用，确保一致性",
            "CREATE DOMAIN postal_code_type AS VARCHAR(20);",
            "CREATE DOMAIN phone_number_type AS VARCHAR(20);",
            "CREATE DOMAIN fda_id_type AS VARCHAR(30);",
            "CREATE DOMAIN product_code_type AS VARCHAR(10);",
            "CREATE DOMAIN registration_number_type AS VARCHAR(20);",
            "CREATE DOMAIN status_code_type AS VARCHAR(50);",
            ""
        ]
        return domains
    
    def _generate_check_constraints(self, table_name, field, field_name):
        constraints = []
        
        lower_name = field_name.lower()
        
        if 'status' in lower_name and 'value_distribution' in field:
            values = list(field['value_distribution'].keys())
            if 5 <= len(values) <= 15:
                safe_values = []
                for v in values:
                    escaped_value = str(v).replace("'", "''")
                    safe_values.append(f"'{escaped_value}'")
                constraint_name = f"chk_{table_name}_{field_name}_valid"
                constraints.append(f"ALTER TABLE {table_name} ADD CONSTRAINT {constraint_name}")
                constraints.append(f"    CHECK ({field_name} IS NULL OR {field_name} IN ({', '.join(safe_values)}));")
        
        elif any(kw in lower_name for kw in ['date', 'time']) and not lower_name.endswith('_flag'):
            constraint_name = f"chk_{table_name}_{field_name}_range"
            constraints.append(f"ALTER TABLE {table_name} ADD CONSTRAINT {constraint_name}")
            constraints.append(f"    CHECK ({field_name} IS NULL OR {field_name} > '1970-01-01' AND {field_name} < '2100-01-01');")
        
        elif lower_name.startswith('is_') or lower_name.startswith('has_') or lower_name.endswith('_flag'):
            pass
        
        elif lower_name.endswith('_count') or lower_name.endswith('_number'):
            constraint_name = f"chk_{table_name}_{field_name}_positive"
            constraints.append(f"ALTER TABLE {table_name} ADD CONSTRAINT {constraint_name}")
            constraints.append(f"    CHECK ({field_name} IS NULL OR {field_name} >= 0);")
        
        return constraints
    
    def _infer_array_element_type(self, field):
        return "VARCHAR"  
    
    def _infer_data_type(self, field_name, sample_value, field_info=None):
        lower_name = field_name.lower()
        
        if lower_name in ['postal_code', 'zip_code', 'zip']:
            return "postal_code_type"
        elif lower_name in ['phone_number', 'phone_num', 'phone']:
            return "phone_number_type"
        elif lower_name in ['product_code']:
            return "product_code_type"
        elif lower_name in ['registration_number', 'fei_number']:
            return "registration_number_type"
        elif lower_name in ['status_code', 'status']:
            return "status_code_type"
        
        if lower_name.endswith('[]') or '_array' in lower_name:
            base_type = "VARCHAR"
            return f"{base_type}[]"
        
        date_time_keywords = ['date', 'created', 'updated', 'modified', 'timestamp']
        if any(keyword in lower_name for keyword in date_time_keywords):
            if 'time' in lower_name or 'timestamp' in lower_name:
                return "TIMESTAMP"
            else:
                return "DATE"
        
        if (lower_name.startswith('is_') or lower_name.startswith('has_') 
            or lower_name.endswith('_flag') or 'flag' in lower_name):
            return "BOOLEAN"
        
        if field_info and 'value_distribution' in field_info:
            values = list(field_info['value_distribution'].keys())
            if 5 <= len(values) <= 15:
                return f"{lower_name}_enum"
        
        id_suffixes = ['_id', '_key', '_uuid', '_code', '_number']
        if lower_name == 'id' or any(lower_name.endswith(suffix) for suffix in id_suffixes):
            if isinstance(sample_value, int):
                return "INTEGER"
            elif field_info and field_info.get('max_length', 0) <= 40:
                return "VARCHAR(40)"
            else:
                return "VARCHAR(100)"
        
        if isinstance(sample_value, int):
            return "INTEGER"
        elif isinstance(sample_value, float):
            return "NUMERIC(15,5)"
        elif isinstance(sample_value, bool):
            return "BOOLEAN"
        elif isinstance(sample_value, str):
            if sample_value.startswith('[Object') or sample_value.startswith('[Array'):
                return "JSONB"
            
            if field_info and 'max_length' in field_info:
                max_len = field_info['max_length']
                if max_len > 1000:
                    return "TEXT"
                elif max_len > 255:
                    return f"VARCHAR({min(1000, max_len + 50)})"
                elif max_len > 50:
                    return f"VARCHAR({min(255, max_len + 30)})"
                else:
                    return f"VARCHAR({min(100, max_len + 20)})"
            
            str_len = len(str(sample_value))
            if str_len > 1000:
                return "TEXT"
            elif str_len > 255:
                return "VARCHAR(1000)"
            elif str_len > 50:
                return "VARCHAR(255)"
            else:
                return "VARCHAR(100)"
        
        return "VARCHAR(255)"
    
    def _get_table_description(self, table_name, table_info):
        if table_info['type'] == 'main':
            return f"FDA {table_name} 主数据表"
        elif table_info['type'] == 'array':
            return f"{table_info['parent']} 的子表，存储 {table_name.split('_')[-1]} 数组数据"
        else:
            return f"{table_info['parent']} 的子表，存储 {table_name.split('_')[-1]} 嵌套对象数据"
    
    def _get_field_description(self, field):
        desc_parts = []
        
        if field.get('is_primary_key'):
            desc_parts.append("主键字段")
        
        if field.get('path'):
            path = field.get('path')
            desc_parts.append(f"路径:{path}")
        
        if field.get('use_jsonb'):
            desc_parts.append("JSONB类型")
            
        if field.get('is_array_type'):
            desc_parts.append("数组类型")
        
        sample = field.get('sample')
        if sample and not str(sample).startswith('[Object') and not str(sample).startswith('[Array'):
            sample_str = str(sample).replace("'", "''")
            if len(sample_str) > 30:
                sample_str = sample_str[:27] + "..."
            desc_parts.append(f"示例:{sample_str}")
        
        if 'cardinality' in field and field['cardinality'] > 0:
            desc_parts.append(f"基数:{field['cardinality']:.2f}")
        
        return " | ".join(desc_parts)


class CrossTypeAnalyzer:
    
    def __init__(self, output_dir="./output"):
        self.output_dir = output_dir
        
        self.analyzers = {}
        self.all_tables = {}
        self.cross_type_relations = []
        
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            logger.info(f"创建输出目录: {output_dir}")
    
    def discover_data_types(self, input_dir, recursive=False):
        json_files = []
        
        if recursive:
            for root, dirs, files in os.walk(input_dir):
                for file in files:
                    if file.endswith('.json'):
                        json_files.append(os.path.join(root, file))
        else:
            json_files = glob.glob(os.path.join(input_dir, '*.json'))
            for item in os.listdir(input_dir):
                item_path = os.path.join(input_dir, item)
                if os.path.isdir(item_path):
                    json_files.extend(glob.glob(os.path.join(item_path, '*.json')))
        
        if not json_files:
            logger.warning(f"未找到JSON文件，请检查路径: {input_dir}")
            return []
        
        direct_types = set()
        for root, dirs, files in os.walk(input_dir):
            base_dir = os.path.basename(root)
            if any(file.endswith('.json') for file in files):
                if base_dir.lower() not in ['data', 'json', 'files', 'docs', 'documents']:
                    if base_dir.lower() == "510k":
                        direct_types.add("k510")
                    else:
                        direct_types.add(base_dir.lower())
        
        file_pattern_types = set()
        pattern = re.compile(r'device-(\w+)-\d+')
        
        for file in json_files[:50]:
            match = pattern.search(os.path.basename(file))
            if match:
                file_type = match.group(1).lower()
                if file_type not in ['of', 'part', 'section']:
                    if file_type == "510k":
                        file_pattern_types.add("k510")
                    else:
                        file_pattern_types.add(file_type)
        
        content_types = set()
        analyzer = BatchJsonAnalyzer()
        
        sample_files = random.sample(json_files, min(20, len(json_files)))
        for file in sample_files:
            data_type = analyzer.guess_data_type_from_file(file)
            if data_type and data_type != "unknown":
                content_types.add(data_type)
        
        all_types = direct_types.union(file_pattern_types).union(content_types)
        
        logger.info(f"从目录结构发现的类型: {', '.join(direct_types)}")
        logger.info(f"从文件名模式发现的类型: {', '.join(file_pattern_types)}")
        logger.info(f"从文件内容发现的类型: {', '.join(content_types)}")
        logger.info(f"最终发现的所有数据类型: {', '.join(all_types)}")
        
        return list(all_types)
    
    def analyze_all_types(self, input_dir, recursive=False, max_files=10, max_records=1000):
        data_types = self.discover_data_types(input_dir, recursive)
        
        if not data_types:
            logger.warning("未能发现任何数据类型")
            return None
        
        for data_type in data_types:
            logger.info(f"开始分析数据类型: {data_type}")
            analyzer = BatchJsonAnalyzer(self.output_dir, data_type)
            
            if data_type == "k510":
                pattern = f"*510k*.json"
            else:
                pattern = f"*{data_type}*.json"
            
            result = analyzer.process_directory(
                input_dir, pattern, max_files, max_records, recursive
            )
            
            if result:
                analyzer.export_table_structure(data_type)
                
                self.analyzers[data_type] = analyzer
                
                for table_name, table_info in analyzer.tables.items():
                    self.all_tables[table_name] = table_info
        
        if len(self.analyzers) > 1:
            self.analyze_cross_type_relations()
            
            self.export_cross_type_relations()
            
            self.generate_combined_schema()
        
        return {
            'data_types': list(self.analyzers.keys()),
            'total_tables': len(self.all_tables),
            'cross_type_relations': len(self.cross_type_relations)
        }
    
    def analyze_cross_type_relations(self):
        logger.info("开始分析跨类型关系 (优化版)")
        
        all_primary_keys = {}
        all_identifiers = {}
        all_field_patterns = {}
        
        fda_id_patterns = {
            'k_number': r'^K\d{6,}$',
            'pma_number': r'^P\d{6,}$',
            'regulation_number': r'^\d{3}\.\d{4}$',
            'registration_number': r'^\d{7,10}$',
            'product_code': r'^[A-Z]{3}$',
            'fei_number': r'^\d{7,10}$'
        }
        
        field_importance = {
            'k_number': 1.5,
            'pma_number': 1.5,
            'regulation_number': 1.3,
            'product_code': 1.2,
            'registration_number': 1.2,
            'fei_number': 1.1,
            'device_name': 0.9,
            'manufacturer_name': 0.9
        }
        
        for data_type, analyzer in self.analyzers.items():
            for path in analyzer.primary_key_candidates:
                if path in analyzer.field_samples:
                    parts = path.split('.')
                    key_name = parts[-1]
                    all_primary_keys[f"{data_type}.{key_name}"] = path
                    
                    id_values = analyzer.unique_identifiers.get(path, set())
                    all_identifiers[f"{data_type}.{key_name}"] = id_values
                    
                    patterns = self._analyze_value_patterns(id_values)
                    all_field_patterns[f"{data_type}.{key_name}"] = patterns
        
        self._add_fda_specific_relations()
        
        relations = []
        
        for (type1, key1), (type2, key2) in itertools.combinations(all_primary_keys.items(), 2):
            if type1.split('.')[0] != type2.split('.')[0]:
                relation_info = self._detect_relationship(
                    type1, key1, type2, key2, 
                    all_identifiers, all_field_patterns, 
                    fda_id_patterns, field_importance
                )
                
                if relation_info:
                    relations.append(relation_info)
        
        business_rule_relations = self._apply_business_rules()
        relations.extend(business_rule_relations)
        
        main_table_relations = self._analyze_main_table_relations()
        relations.extend(main_table_relations)
        
        unique_relations = []
        seen = set()
        
        for relation in sorted(relations, key=lambda x: x['confidence'], reverse=True):
            primary_table = relation['primary_table']
            foreign_table = relation['foreign_table']
            
            if primary_table == "510k":
                primary_table = "k510"
                relation['primary_table'] = "k510"
            
            if foreign_table == "510k":
                foreign_table = "k510" 
                relation['foreign_table'] = "k510"
                
            relation_key = f"{primary_table}.{relation['primary_key']}-{foreign_table}.{relation['foreign_key']}"
            if relation_key not in seen:
                seen.add(relation_key)
                unique_relations.append(relation)
        
        self.cross_type_relations = unique_relations
        logger.info(f"发现 {len(self.cross_type_relations)} 个跨类型关系")
        
        top_relations = sorted(self.cross_type_relations, key=lambda x: x['confidence'], reverse=True)[:10]
        logger.info("最可能的跨类型关系:")
        for rel in top_relations:
            logger.info(f"  {rel['foreign_table']}.{rel['foreign_key']} -> {rel['primary_table']}.{rel['primary_key']} (可信度: {rel['confidence']:.2f})")
        
        return self.cross_type_relations
    
    def _detect_relationship(self, type1, key1, type2, key2, identifiers, patterns, id_patterns, importance):
        type1_name = type1.split('.')[0]
        key1_name = key1.split('.')[-1]
        
        type2_name = type2.split('.')[0]
        key2_name = key2.split('.')[-1]
        
        if key1_name == "id" or key2_name == "id":
            return None
        
        score = 0.0
        reasons = []
        
        if key1_name == key2_name:
            name_score = 1.0
            reasons.append(f"字段名完全匹配: {key1_name}")
        elif key1_name.lower() in key2_name.lower() or key2_name.lower() in key1_name.lower():
            name_score = 0.7
            reasons.append(f"字段名部分匹配: {key1_name}/{key2_name}")
        else:
            name_score = self._name_similarity(key1_name, key2_name)
            if name_score > 0.6:
                reasons.append(f"字段名相似 ({name_score:.2f}): {key1_name}/{key2_name}")
        
        if key1_name in importance:
            name_score *= importance[key1_name]
        if key2_name in importance:
            name_score *= importance[key2_name]
        
        score += name_score
        
        id_set1 = identifiers.get(type1, set())
        id_set2 = identifiers.get(type2, set())
        
        if not id_set1 or not id_set2:
            return None
        
        overlap = len(id_set1.intersection(id_set2))
        min_size = min(len(id_set1), len(id_set2))
        
        if min_size > 0:
            overlap_score = overlap / min_size
            if overlap_score > 0.05:
                reasons.append(f"值重叠 ({overlap_score:.2f}): {overlap}/{min_size}")
                score += overlap_score * 3
        
        pattern1 = patterns.get(type1, {})
        pattern2 = patterns.get(type2, {})
        
        pattern_score = self._compare_value_patterns(pattern1, pattern2)
        if pattern_score > 0:
            reasons.append(f"值模式相似 ({pattern_score:.2f})")
            score += pattern_score
        
        for pattern_name, pattern in id_patterns.items():
            if (key1_name.lower() == pattern_name.lower() or 
                key2_name.lower() == pattern_name.lower()):
                
                sample1 = next(iter(id_set1)) if id_set1 else ""
                sample2 = next(iter(id_set2)) if id_set2 else ""
                
                if (re.match(pattern, str(sample1)) or re.match(pattern, str(sample2))):
                    reasons.append(f"符合FDA {pattern_name} 格式")
                    score += 1.0
        
        if self._check_structural_similarity(type1, key1, type2, key2):
            reasons.append("字段在API结构中位置相似")
            score += 0.8
        
        final_score = score / max(4, len(reasons))
        
        if len(id_set1) >= len(id_set2):
            relation = {
                'primary_table': type1_name,
                'primary_key': key1_name,
                'foreign_table': type2_name,
                'foreign_key': key2_name,
                'confidence': round(final_score, 2),
                'reasons': reasons
            }
        else:
            relation = {
                'primary_table': type2_name,
                'primary_key': key2_name,
                'foreign_table': type1_name,
                'foreign_key': key1_name,
                'confidence': round(final_score, 2),
                'reasons': reasons
            }
        
        return relation if final_score >= 0.4 else None
    
    def _name_similarity(self, name1, name2):
        if name1[0].isdigit() or name1.startswith('_'):
            name1 = f"fld_{name1}"
        if name2[0].isdigit() or name2.startswith('_'):
            name2 = f"fld_{name2}"
            
        name1 = name1.lower()
        name2 = name2.lower()
        
        related_pairs = [
            ('number', 'num'),
            ('id', 'identifier'),
            ('code', 'number'),
            ('regulation', 'reg'),
            ('registration', 'reg'),
            ('pma', 'premarket'),
            ('application', 'app'),
            ('fei', 'facility'),
            ('k510', '510k'),
            ('k510', 'k_number')
        ]
        
        for a, b in related_pairs:
            if (a in name1 and b in name2) or (b in name1 and a in name2):
                return 0.8
        
        if abs(len(name1) - len(name2)) > max(len(name1), len(name2)) / 2:
            return 0
        
        m, n = len(name1), len(name2)
        dp = [[0] * (n + 1) for _ in range(m + 1)]
        
        for i in range(m + 1):
            dp[i][0] = i
        for j in range(n + 1):
            dp[0][j] = j
        
        for i in range(1, m + 1):
            for j in range(1, n + 1):
                if name1[i-1] == name2[j-1]:
                    dp[i][j] = dp[i-1][j-1]
                else:
                    dp[i][j] = min(dp[i-1][j-1] + 1, dp[i][j-1] + 1, dp[i-1][j] + 1)
        
        max_len = max(m, n)
        if max_len == 0:
            return 0
        
        return 1 - (dp[m][n] / max_len)
    
    def _analyze_value_patterns(self, values):
        if not values:
            return {}
        
        patterns = {
            'length': {},
            'char_types': {},
            'prefix': {},
            'suffix': {}
        }
        
        str_values = [str(v) for v in values]
        
        samples = random.sample(str_values, min(100, len(str_values)))
        
        for val in samples:
            length = len(val)
            patterns['length'][length] = patterns['length'].get(length, 0) + 1
        
        for val in samples:
            if val.isdigit():
                char_type = 'numeric'
            elif val.isalpha():
                char_type = 'alpha'
            else:
                char_type = 'mixed'
            patterns['char_types'][char_type] = patterns['char_types'].get(char_type, 0) + 1
        
        for val in samples:
            if len(val) >= 3:
                prefix = val[:3]
                patterns['prefix'][prefix] = patterns['prefix'].get(prefix, 0) + 1
        
        for val in samples:
            if len(val) >= 3:
                suffix = val[-3:]
                patterns['suffix'][suffix] = patterns['suffix'].get(suffix, 0) + 1
        
        result = {}
        
        if patterns['length']:
            result['common_length'] = max(patterns['length'].items(), key=lambda x: x[1])[0]
        
        if patterns['char_types']:
            result['common_char_type'] = max(patterns['char_types'].items(), key=lambda x: x[1])[0]
        
        if patterns['prefix']:
            top_prefixes = sorted(patterns['prefix'].items(), key=lambda x: x[1], reverse=True)[:3]
            result['top_prefixes'] = [p[0] for p in top_prefixes]
        
        if patterns['suffix']:
            top_suffixes = sorted(patterns['suffix'].items(), key=lambda x: x[1], reverse=True)[:3]
            result['top_suffixes'] = [s[0] for s in top_suffixes]
        
        return result
    
    def _compare_value_patterns(self, pattern1, pattern2):
        if not pattern1 or not pattern2:
            return 0
        
        score = 0
        factors = 0
        
        if 'common_length' in pattern1 and 'common_length' in pattern2:
            len1 = pattern1['common_length']
            len2 = pattern2['common_length']
            length_sim = 1 - min(abs(len1 - len2) / max(len1, len2, 1), 1)
            score += length_sim
            factors += 1
        
        if 'common_char_type' in pattern1 and 'common_char_type' in pattern2:
            if pattern1['common_char_type'] == pattern2['common_char_type']:
                score += 1
            factors += 1
        
        if 'top_prefixes' in pattern1 and 'top_prefixes' in pattern2:
            common_prefixes = set(pattern1['top_prefixes']).intersection(pattern2['top_prefixes'])
            if common_prefixes:
                score += len(common_prefixes) / max(len(pattern1['top_prefixes']), len(pattern2['top_prefixes']))
            factors += 1
        
        if 'top_suffixes' in pattern1 and 'top_suffixes' in pattern2:
            common_suffixes = set(pattern1['top_suffixes']).intersection(pattern2['top_suffixes'])
            if common_suffixes:
                score += len(common_suffixes) / max(len(pattern1['top_suffixes']), len(pattern2['top_suffixes']))
            factors += 1
        
        return score / max(1, factors)
    
    def _check_structural_similarity(self, type1, key1, type2, key2):
        path1 = type1.split('.')
        key1_parts = key1.split('.')
        
        path2 = type2.split('.')
        key2_parts = key2.split('.')
        
        if ('openfda' in key1_parts and 'openfda' in key2_parts):
            return True
        
        if ('identifiers' in key1_parts and 'identifiers' in key2_parts):
            return True
        
        if len(key1_parts) == len(key2_parts):
            return True
        
        return False
    
    def _add_fda_specific_relations(self):
        known_relations = [
            {'primary': 'registrationlisting', 'primary_key': 'registration_number',
             'foreign': 'pma', 'foreign_key': 'registration_number', 'confidence': 0.9},
            {'primary': 'registrationlisting', 'primary_key': 'registration_number',
             'foreign': 'k510', 'foreign_key': 'registration_number', 'confidence': 0.9},
             
            {'primary': 'classification', 'primary_key': 'regulation_number',
             'foreign': 'pma', 'foreign_key': 'regulation_number', 'confidence': 0.95},
            {'primary': 'classification', 'primary_key': 'regulation_number',
             'foreign': 'k510', 'foreign_key': 'regulation_number', 'confidence': 0.95},
            {'primary': 'classification', 'primary_key': 'regulation_number',
             'foreign': 'udi', 'foreign_key': 'regulation_number', 'confidence': 0.9},
             
            {'primary': 'pma', 'primary_key': 'pma_number',
             'foreign': 'recall', 'foreign_key': 'pma_number', 'confidence': 0.9},
            {'primary': 'k510', 'primary_key': 'k_number',
             'foreign': 'recall', 'foreign_key': 'k_number', 'confidence': 0.9},
             
            {'primary': 'udi', 'primary_key': 'public_device_record_key',
             'foreign': 'event', 'foreign_key': 'device_report_product_code', 'confidence': 0.7},
        ]
        
        for relation in known_relations:
            if relation['primary'] in self.analyzers and relation['foreign'] in self.analyzers:
                self.cross_type_relations.append({
                    'primary_table': relation['primary'],
                    'primary_key': relation['primary_key'],
                    'foreign_table': relation['foreign'],
                    'foreign_key': relation['foreign_key'],
                    'confidence': relation['confidence'],
                    'reasons': ['FDA业务规则']
                })
    
    def _apply_business_rules(self):
        business_relations = []
        
        if 'device' in self.analyzers and 'classification' in self.analyzers:
            business_relations.append({
                'primary_table': 'classification',
                'primary_key': 'device_class',
                'foreign_table': 'device', 
                'foreign_key': 'device_class',
                'confidence': 0.9,
                'reasons': ['FDA业务规则: 设备分类关系']
            })
        
        if 'registrationlisting' in self.analyzers:
            for data_type in self.analyzers:
                if data_type != 'registrationlisting' and data_type in ['device', 'drug', 'food']:
                    business_relations.append({
                        'primary_table': 'registrationlisting',
                        'primary_key': 'registration_number',
                        'foreign_table': data_type,
                        'foreign_key': 'registration_number',
                        'confidence': 0.85,
                        'reasons': ['FDA业务规则: 注册与产品关系']
                    })
        
        return business_relations
    
    def _analyze_main_table_relations(self):
        relations = []
        
        stages = [
            ('registrationlisting', '注册和列表'),
            ('classification', '设备分类'),
            ('k510', '510(k)申请'),
            ('pma', 'PMA批准'),
            ('udi', 'UDI标识'),
            ('event', '不良事件'),
            ('recall', '召回')
        ]
        
        available_stages = [stage for stage in stages if stage[0] in self.analyzers]
        
        for i in range(len(available_stages) - 1):
            for j in range(i + 1, len(available_stages)):
                stage1 = available_stages[i]
                stage2 = available_stages[j]
                
                a1 = self.analyzers[stage1[0]]
                a2 = self.analyzers[stage2[0]]
                
                common_ids = []
                for field1 in a1.tables.get(stage1[0], {}).get('fields', []):
                    for field2 in a2.tables.get(stage2[0], {}).get('fields', []):
                        if field1['name'] == field2['name'] and self._is_id_field(field1['name']):
                            common_ids.append(field1['name'])
                
                for field_name in common_ids:
                    relations.append({
                        'primary_table': stage1[0],
                        'primary_key': field_name,
                        'foreign_table': stage2[0],
                        'foreign_key': field_name,
                        'confidence': 0.75,
                        'reasons': [f'FDA流程关系: {stage1[1]} -> {stage2[1]}']
                    })
        
        return relations
    
    def _is_id_field(self, field_name):
        lower_name = field_name.lower()
        return (
            lower_name == 'id' or
            lower_name.endswith('_id') or
            lower_name.endswith('_number') or
            lower_name.endswith('_code') or
            lower_name.endswith('_key') or
            lower_name == 'uuid'
        )
    
    def export_cross_type_relations(self):
        if not self.cross_type_relations:
            logger.info("没有跨类型关系可导出")
            return
        
        sorted_relations = sorted(
            self.cross_type_relations, 
            key=lambda x: x['confidence'], 
            reverse=True
        )
        
        relations_csv = os.path.join(self.output_dir, "cross_type_relations.csv")
        
        relations_data = []
        for relation in sorted_relations:
            reasons = "; ".join(relation.get('reasons', []))
            
            relations_data.append({
                'primary_table': relation['primary_table'],
                'primary_key': relation['primary_key'],
                'foreign_table': relation['foreign_table'],
                'foreign_key': relation['foreign_key'],
                'confidence': relation['confidence'],
                'reasons': reasons,
                'sql_constraint': self._generate_constraint_sql(relation),
                'relation_type': self._get_relation_type(relation)
            })
        
        pd.DataFrame(relations_data).to_csv(relations_csv, index=False)
        logger.info(f"已生成跨类型关系CSV: {relations_csv}")
        
        groups_csv = os.path.join(self.output_dir, "relation_groups.csv")
        
        table_pairs = defaultdict(list)
        for relation in sorted_relations:
            key = f"{relation['primary_table']}-{relation['foreign_table']}"
            table_pairs[key].append(relation)
        
        groups_data = []
        for pair, relations in table_pairs.items():
            tables = pair.split('-')
            avg_confidence = sum(r['confidence'] for r in relations) / len(relations)
            
            groups_data.append({
                'primary_table': tables[0],
                'foreign_table': tables[1],
                'relation_count': len(relations),
                'avg_confidence': round(avg_confidence, 2),
                'max_confidence': max(r['confidence'] for r in relations),
                'related_fields': ", ".join(f"{r['primary_key']}->{r['foreign_key']}" for r in relations[:3])
            })
        
        groups_data.sort(key=lambda x: x['relation_count'], reverse=True)
        pd.DataFrame(groups_data).to_csv(groups_csv, index=False)
        logger.info(f"已生成关系分组CSV: {groups_csv}")
        
        dot_file = os.path.join(self.output_dir, "relation_graph.dot")
        
        high_confidence_relations = [r for r in sorted_relations if r['confidence'] >= 0.7]
        
        dot_content = ['digraph FDA_Relations {', 
                      '  rankdir=LR;',
                      '  node [shape=box, style=filled, fillcolor=lightblue];']
        
        tables = set()
        for relation in high_confidence_relations:
            tables.add(relation['primary_table'])
            tables.add(relation['foreign_table'])
        
        for table in sorted(tables):
            dot_content.append(f'  "{table}" [label="{table}"];')
        
        for relation in high_confidence_relations:
            edge = f'  "{relation["foreign_table"]}" -> "{relation["primary_table"]}" '
            edge += f'[label="{relation["foreign_key"]}->{relation["primary_key"]}", '
            edge += f'penwidth={relation["confidence"]+0.5}, '
            
            if relation['confidence'] >= 0.9:
                edge += 'color=darkgreen];'
            elif relation['confidence'] >= 0.8:
                edge += 'color=green];'
            elif relation['confidence'] >= 0.7:
                edge += 'color=blue];'
            else:
                edge += 'color=gray, style=dashed];'
                
            dot_content.append(edge)
        
        dot_content.append('}')
        
        with open(dot_file, 'w') as f:
            f.write('\n'.join(dot_content))
        
        logger.info(f"已生成关系图DOT文件: {dot_file}")
        logger.info("可使用Graphviz工具将DOT文件转换为可视化图表")
        
        sql_file = os.path.join(self.output_dir, "cross_type_constraints.sql")
        
        sql_content = [
            f"-- FDA跨类型关系约束 (优化版)",
            f"-- 生成日期: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"-- 数据类型: {', '.join(self.analyzers.keys())}",
            f"-- 关系数量: {len(sorted_relations)}",
            f"",
            f"-- 高可信度关系 (>= 0.8)",
            f"-- 这些关系非常可能是正确的外键关系"
        ]
        
        high_confidence = [r for r in sorted_relations if r['confidence'] >= 0.8]
        for relation in high_confidence:
            sql = self._generate_constraint_sql(relation)
            sql_content.append(f"{sql};")
            sql_content.append("")
        
        sql_content.append(f"-- 中等可信度关系 (0.6-0.8)")
        sql_content.append(f"-- 这些关系可能是外键关系，但建议先验证")
        
        medium_confidence = [r for r in sorted_relations if 0.6 <= r['confidence'] < 0.8]
        for relation in medium_confidence:
            sql = self._generate_constraint_sql(relation)
            sql_content.append(f"-- {sql};")
            sql_content.append("")
        
        with open(sql_file, 'w') as f:
            f.write('\n'.join(sql_content))
        
        logger.info(f"已生成SQL约束文件: {sql_file}")
        
        return {
            'relations_csv': relations_csv,
            'groups_csv': groups_csv,
            'dot_file': dot_file,
            'sql_file': sql_file
        }
    
    def _generate_constraint_sql(self, relation):
        if relation['primary_table'] == "k510":
            primary_table = "fda_k510"
        else:
            primary_table = relation['primary_table']
            
        if relation['foreign_table'] == "k510":
            foreign_table = "fda_k510"
        else:
            foreign_table = relation['foreign_table']
            
        primary_key = relation['primary_key']
        foreign_key = relation['foreign_key']
        
        if primary_key[0].isdigit() or primary_key.startswith('_'):
            primary_key = f"fld_{primary_key}"
        if foreign_key[0].isdigit() or foreign_key.startswith('_'):
            foreign_key = f"fld_{foreign_key}"
        
        constraint_name = f"fk_{foreign_table}_{foreign_key}_to_{primary_table}"
        
        if len(constraint_name) > 63:
            constraint_name = f"fk_{foreign_table[:6]}_{foreign_key[:6]}_{primary_table[:6]}"
        
        sql = f"ALTER TABLE {foreign_table} ADD CONSTRAINT {constraint_name}"
        sql += f"\n    FOREIGN KEY ({foreign_key}) REFERENCES {primary_table}({primary_key})"
        
        return sql
    
    def _get_relation_type(self, relation):
        primary_key = relation['primary_key'].lower()
        if primary_key.startswith('fld_'):
            primary_key = primary_key[4:]
            
        foreign_key = relation['foreign_key'].lower()
        if foreign_key.startswith('fld_'):
            foreign_key = foreign_key[4:]
        
        if primary_key == foreign_key:
            if primary_key.endswith('_id') or primary_key == 'id':
                return '标准ID关系'
            elif primary_key.endswith('_number'):
                return '编号关系'
            elif primary_key.endswith('_code'):
                return '代码关系'
        
        if ('regulation' in primary_key or 'regulation' in foreign_key):
            return '法规关系'
        elif ('pma' in primary_key or 'pma' in foreign_key):
            return 'PMA关系'
        elif ('k_number' in primary_key or 'k_number' in foreign_key or 'k510' in primary_key or 'k510' in foreign_key):
            return '510K关系'
        elif ('registration' in primary_key or 'registration' in foreign_key):
            return '注册关系'
        elif ('product' in primary_key or 'product' in foreign_key):
            return '产品关系'
        
        return '一般关系'
    
    def generate_combined_schema(self):
        analyzer_keys = list(self.analyzers.keys())
        for i, key in enumerate(analyzer_keys):
            if key == "510k":
                analyzer_keys[i] = "k510"
        
        ddl_lines = [
            f"-- FDA数据综合模式 (优化版)",
            f"-- 生成日期: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"-- 数据类型: {', '.join(analyzer_keys)}",
            f"-- 总表数: {len(self.all_tables)}",
            f"-- 跨类型关系: {len(self.cross_type_relations)}",
            ""
        ]
        
        ddl_lines.extend([
            "-- 创建通用数据域",
            "CREATE DOMAIN postal_code_type AS VARCHAR(20);",
            "CREATE DOMAIN phone_number_type AS VARCHAR(20);",
            "CREATE DOMAIN fda_id_type AS VARCHAR(30);",
            "CREATE DOMAIN product_code_type AS VARCHAR(10);",
            ""
        ])
        
        table_dependencies = self._build_dependency_graph()
        sorted_tables = self._topological_sort(table_dependencies)
        
        ddl_lines.extend([
            "-- 创建常用枚举类型",
            "CREATE TYPE recall_status_enum AS ENUM ('Open', 'Closed', 'Open, Classified', 'Terminated');",
            "CREATE TYPE device_class_enum AS ENUM ('1', '2', '3', 'U', 'N');",
            ""
        ])
        
        for table_name in sorted_tables:
            if table_name not in self.all_tables:
                continue
                
            table_info = self.all_tables[table_name]
            
            simplified_name = self._simplify_table_name(table_name)
            
            ddl_lines.append(f"CREATE TABLE {simplified_name} (")
            
            ddl_lines.append(f"    id SERIAL,")
            
            if table_info.get('parent'):
                parent_table = self._simplify_table_name(table_info['parent'])
                parent_id = f"{parent_table}_id"
                ddl_lines.append(f"    {parent_id} INTEGER,")
            
            processed_fields = set()
            field_defs = []
            
            analyzer = None
            for data_type, a in self.analyzers.items():
                if table_name in a.tables:
                    analyzer = a
                    break
            
            if analyzer and table_name in analyzer.tables:
                for field in analyzer.tables[table_name]['fields']:
                    if field.get('name') in processed_fields:
                        continue
                    
                    field_name = field.get('name')
                    if field_name[0].isdigit() or field_name.startswith('_'):
                        field_name = f"fld_{field_name}"
                        
                    processed_fields.add(field_name)
                    
                    data_type = self._infer_optimized_data_type(field, table_name)
                    
                    field_defs.append(f"    {field_name} {data_type},")
            
            ddl_lines.extend(field_defs)
            
            ddl_lines.append(f"    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,")
            ddl_lines.append(f"    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
            
            if 'primary_keys' in table_info and table_info['primary_keys']:
                primary_keys = ['id']
                
                business_keys = [k for k in table_info['primary_keys'] if k != 'id'][:1]
                if business_keys:
                    primary_keys.extend(business_keys)
                    
                pk_constraint = f"    PRIMARY KEY ({', '.join(primary_keys)})"
                ddl_lines[-1] += ","
                ddl_lines.append(pk_constraint)
            else:
                pk_constraint = f"    PRIMARY KEY (id)"
                ddl_lines[-1] += ","
                ddl_lines.append(pk_constraint)
            
            ddl_lines.append(");")
            ddl_lines.append("")
            
            table_description = self._get_optimized_table_description(table_name, table_info)
            ddl_lines.append(f"COMMENT ON TABLE {simplified_name} IS '{table_description}';")
            ddl_lines.append("")
            
            if analyzer and table_name in analyzer.tables:
                for field in analyzer.tables[table_name]['fields']:
                    field_name = field.get('name')
                    if field_name[0].isdigit() or field_name.startswith('_'):
                        field_name = f"fld_{field_name}"
                        
                    if field_name in processed_fields:
                        field_desc = analyzer._get_field_description(field)
                        ddl_lines.append(f"COMMENT ON COLUMN {simplified_name}.{field_name} IS '{field_desc}';")
                ddl_lines.append("")
            
            if 'primary_keys' in table_info:
                remaining_keys = [k for k in table_info['primary_keys'] if k != 'id'][1:]
                if remaining_keys:
                    for key in remaining_keys:
                        if key[0].isdigit() or key.startswith('_'):
                            key = f"fld_{key}"
                            
                        if key in processed_fields:
                            ddl_lines.append(f"ALTER TABLE {simplified_name} ADD CONSTRAINT uk_{simplified_name}_{key}")
                            ddl_lines.append(f"    UNIQUE ({key});")
                    ddl_lines.append("")
        
        ddl_lines.append("-- 内部表关系")
        for data_type, analyzer in self.analyzers.items():
            for table_name, table_info in analyzer.tables.items():
                if table_info.get('parent'):
                    parent_table = table_info['parent']
                    simplified_table = self._simplify_table_name(table_name)
                    simplified_parent = self._simplify_table_name(parent_table)
                    parent_id = f"{simplified_parent}_id"
                    ddl_lines.append(f"ALTER TABLE {simplified_table} ADD CONSTRAINT fk_{simplified_table}_{parent_id}")
                    ddl_lines.append(f"    FOREIGN KEY ({parent_id}) REFERENCES {simplified_parent}(id);")
                    ddl_lines.append("")
        
        ddl_lines.append("-- 高可信度跨类型关系 (>= 0.8)")
        high_conf_relations = [r for r in self.cross_type_relations if r['confidence'] >= 0.8]
        
        for relation in high_conf_relations:
            primary_table = relation['primary_table']
            primary_key = relation['primary_key']
            foreign_table = relation['foreign_table']
            foreign_key = relation['foreign_key']
            
            simplified_primary = self._simplify_table_name(primary_table)
            simplified_foreign = self._simplify_table_name(foreign_table)
            
            if primary_key[0].isdigit() or primary_key.startswith('_'):
                primary_key = f"fld_{primary_key}"
            if foreign_key[0].isdigit() or foreign_key.startswith('_'):
                foreign_key = f"fld_{foreign_key}"
                
            constraint_name = f"fk_{simplified_foreign}_{foreign_key[:6]}"
            
            ddl_lines.append(f"-- 可信度: {relation['confidence']}, 原因: {', '.join(relation.get('reasons', []))}")
            ddl_lines.append(f"ALTER TABLE {simplified_foreign} ADD CONSTRAINT {constraint_name}")
            ddl_lines.append(f"    FOREIGN KEY ({foreign_key}) REFERENCES {simplified_primary}({primary_key});")
            ddl_lines.append("")
        
        ddl_lines.append("-- 中等可信度跨类型关系 (0.6-0.8)")
        mid_conf_relations = [r for r in self.cross_type_relations if 0.6 <= r['confidence'] < 0.8]
        
        for relation in mid_conf_relations:
            primary_table = relation['primary_table']
            primary_key = relation['primary_key']
            foreign_table = relation['foreign_table']
            foreign_key = relation['foreign_key']
            
            simplified_primary = self._simplify_table_name(primary_table)
            simplified_foreign = self._simplify_table_name(foreign_table)
            
            if primary_key[0].isdigit() or primary_key.startswith('_'):
                primary_key = f"fld_{primary_key}"
            if foreign_key[0].isdigit() or foreign_key.startswith('_'):
                foreign_key = f"fld_{foreign_key}"
                
            constraint_name = f"fk_{simplified_foreign}_{foreign_key[:6]}"
            
            ddl_lines.append(f"-- 可信度: {relation['confidence']}, 原因: {', '.join(relation.get('reasons', []))}")
            ddl_lines.append(f"-- ALTER TABLE {simplified_foreign} ADD CONSTRAINT {constraint_name}")
            ddl_lines.append(f"--     FOREIGN KEY ({foreign_key}) REFERENCES {simplified_primary}({primary_key});")
            ddl_lines.append("")
        
        ddl_lines.append("-- 创建索引")
        
        processed_indexes = set()
        
        ddl_lines.append("-- 主键和外键索引")
        for relation in high_conf_relations:
            foreign_table = self._simplify_table_name(relation['foreign_table'])
            foreign_key = relation['foreign_key']
            
            if foreign_key[0].isdigit() or foreign_key.startswith('_'):
                foreign_key = f"fld_{foreign_key}"
                
            index_key = f"{foreign_table}.{foreign_key}"
            if index_key not in processed_indexes:
                processed_indexes.add(index_key)
                ddl_lines.append(f"CREATE INDEX idx_{foreign_table}_{foreign_key} ON {foreign_table}({foreign_key});")
        
        for data_type, analyzer in self.analyzers.items():
            for table_name, table_info in analyzer.tables.items():
                if table_info.get('parent'):
                    simplified_table = self._simplify_table_name(table_name)
                    simplified_parent = self._simplify_table_name(table_info['parent'])
                    parent_id = f"{simplified_parent}_id"
                    
                    index_key = f"{simplified_table}.{parent_id}"
                    if index_key not in processed_indexes:
                        processed_indexes.add(index_key)
                        ddl_lines.append(f"CREATE INDEX idx_{simplified_table}_{parent_id} ON {simplified_table}({parent_id});")
        
        ddl_lines.append("")
        
        ddl_lines.append("-- 全文搜索索引")
        text_search_fields = [
            ('recall', 'reason_for_recall'),
            ('recall', 'product_description'),
            ('event', 'device_name'),
            ('event_mdr_text', 'text')
        ]
        
        for table, field in text_search_fields:
            simplified_table = self._simplify_table_name(table)
            
            if field[0].isdigit() or field.startswith('_'):
                field = f"fld_{field}"
                
            index_key = f"{simplified_table}.{field}.fts"
            if index_key not in processed_indexes:
                processed_indexes.add(index_key)
                ddl_lines.append(f"CREATE INDEX idx_{simplified_table}_{field}_tsvector ON {simplified_table} USING GIN (to_tsvector('english', {field}));")
        
        ddl_lines.append("")
        
        ddl_lines.append("-- JSONB索引")
        jsonb_fields = [
            ('event', 'device'),
            ('event', 'patient'),
            ('udi', 'identifiers'),
            ('recall', 'openfda')
        ]
        
        for table, field in jsonb_fields:
            simplified_table = self._simplify_table_name(table)
            
            if field[0].isdigit() or field.startswith('_'):
                field = f"fld_{field}"
                
            index_key = f"{simplified_table}.{field}.jsonb"
            if index_key not in processed_indexes:
                processed_indexes.add(index_key)
                ddl_lines.append(f"CREATE INDEX idx_{simplified_table}_{field}_gin ON {simplified_table} USING GIN ({field});")
        
        ddl_lines.append("")
        
        ddl_lines.append("-- 常用查询字段索引")
        common_fields = [
            ('recall', 'recall_status'),
            ('recall', 'event_date_initiated'),
            ('event', 'date_received'),
            ('pma', 'decision_date'),
            ('k510', 'decision_date'),
            ('udi', 'publish_date')
        ]
        
        for table, field in common_fields:
            simplified_table = self._simplify_table_name(table)
            
            if field[0].isdigit() or field.startswith('_'):
                field = f"fld_{field}"
                
            index_key = f"{simplified_table}.{field}"
            if index_key not in processed_indexes:
                processed_indexes.add(index_key)
                
                if 'date' in field.lower():
                    ddl_lines.append(f"CREATE INDEX idx_{simplified_table}_{field}_brin ON {simplified_table} USING BRIN ({field});")
                else:
                    ddl_lines.append(f"CREATE INDEX idx_{simplified_table}_{field} ON {simplified_table}({field});")
        
        ddl_lines.append("")
        
        ddl_lines.append("-- 自动更新时间戳触发器")
        ddl_lines.append("""
CREATE OR REPLACE FUNCTION update_timestamp_column()
RETURNS TRIGGER AS $
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$ LANGUAGE plpgsql;
        """)
        
        for table_name in sorted_tables:
            if table_name in self.all_tables:
                simplified_name = self._simplify_table_name(table_name)
                ddl_lines.append(f"""
CREATE TRIGGER update_timestamp
BEFORE UPDATE ON {simplified_name}
FOR EACH ROW EXECUTE FUNCTION update_timestamp_column();
                """)
        
        combined_ddl = os.path.join(self.output_dir, "fda_combined_schema_optimized.sql")
        with open(combined_ddl, 'w', encoding='utf-8') as f:
            f.write("\n".join(ddl_lines))
        logger.info(f"已生成优化版综合数据库模式: {combined_ddl}")
        
        return combined_ddl
    
    def _infer_optimized_data_type(self, field, table_name):
        if field.get('use_jsonb', False):
            return "JSONB"
        elif field.get('is_array_type', False):
            element_type = "VARCHAR"
            return f"{element_type}[]"
        
        field_name = field['name']
        if field_name[0].isdigit() or field_name.startswith('_'):
            field_name = f"fld_{field_name}"
            
        lower_name = field_name.lower()
        sample_value = field.get('sample', '')
        
        if any(x in lower_name for x in ['postal_code', 'zip_code', 'zip']):
            return "postal_code_type"
        elif any(x in lower_name for x in ['phone_number', 'phone_num', 'phone']):
            return "phone_number_type"
        elif lower_name in ['product_code', 'device_code']:
            return "product_code_type"
        elif lower_name == 'regulation_number':
            return "VARCHAR(10)"
        
        if lower_name == 'device_class':
            return "device_class_enum"
        elif lower_name == 'recall_status' and table_name.startswith('recall'):
            return "recall_status_enum"
        
        if any(x in lower_name for x in ['date', 'created', 'updated']):
            if 'time' in lower_name:
                return "TIMESTAMP"
            else:
                return "DATE"
        elif any(x in lower_name for x in ['is_', 'has_', '_flag']):
            return "BOOLEAN"
        
        max_length = field.get('max_length', 0)
        if max_length > 0:
            if max_length > 1000:
                return "TEXT"
            elif max_length > 255:
                return f"VARCHAR(1000)"
            elif max_length > 100:
                return f"VARCHAR(255)"
            elif max_length > 50:
                return f"VARCHAR(100)"
            else:
                return f"VARCHAR(50)"
        
        return "VARCHAR(255)"
    
    def _build_dependency_graph(self):
        graph = defaultdict(list)
        
        for data_type, analyzer in self.analyzers.items():
            for table_name, table_info in analyzer.tables.items():
                if table_info.get('parent'):
                    parent_table = table_info['parent']
                    graph[table_name].append(parent_table)
        
        for relation in self.cross_type_relations:
            if relation['confidence'] >= 0.8:
                graph[relation['foreign_table']].append(relation['primary_table'])
        
        return graph
    
    def _topological_sort(self, graph):
        in_degree = {node: 0 for node in graph}
        for node in graph:
            for neighbor in graph[node]:
                if neighbor in in_degree:
                    in_degree[neighbor] += 1
                else:
                    in_degree[neighbor] = 1
        
        queue = [node for node in in_degree if in_degree[node] == 0]
        
        result = []
        
        while queue:
            node = queue.pop(0)
            result.append(node)
            
            for neighbor in graph[node]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)
        
        if len(result) != len(in_degree):
            logger.warning("表依赖关系中存在循环，无法完全排序")
            for node in in_degree:
                if node not in result:
                    result.append(node)
        
        for table_name in self.all_tables:
            if table_name not in result:
                result.append(table_name)
        
        return result
    
    def _simplify_table_name(self, table_name):
        if table_name == "510k":
            return "fda_k510"
        
        if table_name[0].isdigit():
            table_name = f"fda_{table_name}"
        
        if len(table_name) > 60:
            parts = table_name.split('_')
            if len(parts) > 3:
                return f"{parts[0]}_{parts[-2]}_{parts[-1]}"
        
        return table_name
    
    def _get_optimized_table_description(self, table_name, table_info):
        if table_info['type'] == 'main':
            desc = f"FDA {table_name} 主数据表"
        elif table_info['type'] == 'array':
            desc = f"{table_info['parent']} 的数组子表，存储 {table_name.split('_')[-1]} 数据"
        else:
            desc = f"{table_info['parent']} 的嵌套对象，存储 {table_name.split('_')[-1]} 数据"
        
        field_count = len(table_info.get('fields', []))
        
        path = table_info.get('array_path', table_info.get('object_path', ''))
        
        return f"{desc} ({field_count}个字段){', ' + path if path else ''}"
    
    def _apply_business_rules(self):
        business_relations = []
        
        if 'device' in self.analyzers and 'classification' in self.analyzers:
            business_relations.append({
                'primary_table': 'classification',
                'primary_key': 'device_class',
                'foreign_table': 'device', 
                'foreign_key': 'device_class',
                'confidence': 0.9,
                'reasons': ['FDA业务规则: 设备分类关系']
            })
        
        if 'registrationlisting' in self.analyzers:
            for data_type in self.analyzers:
                if data_type != 'registrationlisting' and data_type in ['device', 'drug', 'food']:
                    business_relations.append({
                        'primary_table': 'registrationlisting',
                        'primary_key': 'registration_number',
                        'foreign_table': data_type,
                        'foreign_key': 'registration_number',
                        'confidence': 0.85,
                        'reasons': ['FDA业务规则: 注册与产品关系']
                    })
        
        return business_relations


def main():
    parser = argparse.ArgumentParser(description='FDA跨类型JSON分析工具 (优化版)')
    parser.add_argument('--input_dir', default='.', help='输入目录路径')
    parser.add_argument('--data_type', help='数据类型标识(不指定则自动发现所有类型)')
    parser.add_argument('--pattern', help='文件名匹配模式')
    parser.add_argument('--output_dir', default='./output', help='输出目录')
    parser.add_argument('--max_files', type=int, default=10, help='每种类型最大处理文件数')
    parser.add_argument('--max_records', type=int, default=100000, help='每个文件最大处理记录数')
    parser.add_argument('--recursive', action='store_true', help='是否递归搜索子目录')
    
    args = parser.parse_args()
    
    start_time = datetime.datetime.now()
    logger.info(f"开始分析: {start_time}")
    
    if args.data_type:
        if args.data_type == "510k":
            data_type = "k510"
        else:
            data_type = args.data_type
            
        logger.info(f"使用指定数据类型: {data_type}")
        analyzer = BatchJsonAnalyzer(args.output_dir, data_type)
        
        pattern = args.pattern
        if not pattern:
            if data_type == "k510":
                pattern = f"*510k*.json"
            else:
                pattern = f"*{data_type}*.json"
        
        result = analyzer.process_directory(
            args.input_dir, pattern, 
            args.max_files, args.max_records, args.recursive
        )
        
        if result:
            logger.info("\n批量分析结果:")
            logger.info(f"  数据类型: {result['data_type']}")
            logger.info(f"  处理文件数: {result['processed_files']}")
            logger.info(f"  处理记录数: {result['total_records']}")
            logger.info(f"  唯一路径数: {result['unique_paths']}")
            logger.info(f"  嵌套路径数: {result['nested_paths']}")
            logger.info(f"  识别表数: {result['tables']}")
            
            output_files = analyzer.export_table_structure(args.data_type)
            
            logger.info("\n输出文件:")
            for file_type, file_path in output_files.items():
                logger.info(f"  {file_type}: {file_path}")
        else:
            logger.error("分析失败!")
            logger.error("提示: 尝试添加 --recursive 参数以搜索子目录")
    else:
        logger.info("启动跨类型分析模式，将自动发现所有数据类型")
        cross_analyzer = CrossTypeAnalyzer(args.output_dir)
        
        result = cross_analyzer.analyze_all_types(
            args.input_dir, args.recursive, 
            args.max_files, args.max_records
        )
        
        if result:
            logger.info("\n跨类型分析结果:")
            logger.info(f"  发现数据类型: {', '.join(result['data_types'])}")
            logger.info(f"  总表数: {result['total_tables']}")
            logger.info(f"  跨类型关系: {result['cross_type_relations']}")
            
            if os.path.exists(os.path.join(args.output_dir, "relation_graph.dot")):
                logger.info("\n要生成关系图，可使用以下命令:")
                logger.info("  dot -Tpng output/relation_graph.dot -o output/relation_graph.png")
                logger.info("  或访问 https://dreampuf.github.io/GraphvizOnline/ 上传dot文件可视化")
        else:
            logger.error("分析失败!")
            logger.error("提示: 尝试添加 --recursive 参数以搜索子目录")
    
    end_time = datetime.datetime.now()
    duration = end_time - start_time
    logger.info(f"分析完成: {end_time}")
    logger.info(f"总耗时: {duration}")


if __name__ == "__main__":
    main()