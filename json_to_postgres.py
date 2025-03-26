#!/usr/bin/env python3
"""
FDA JSON到SQL数据库导入工具

该程序执行SQL模式并将FDA JSON数据导入SQL数据库，
使用CSV文件中的映射信息正确转换和插入数据。
"""

import os
import sys
import json
import pandas as pd
import psycopg2
import argparse
from collections import defaultdict
import glob
import datetime

class JsonToSqlImporter:
    """基于映射信息将FDA JSON数据导入SQL数据库的类"""
    
    def __init__(self, db_config, input_dir, schema_file, mapping_dir, data_type="event"):
        """使用配置参数初始化导入器
        
        Args:
            db_config (dict): 数据库连接参数
            input_dir (str): 包含JSON文件的目录
            schema_file (str): SQL模式文件路径
            mapping_dir (str): 包含映射CSV文件的目录
            data_type (str): 数据类型标识符（event, recall等）
        """
        self.db_config = db_config
        self.input_dir = input_dir
        self.schema_file = schema_file
        self.mapping_dir = mapping_dir
        self.data_type = data_type
        
        # 数据库连接
        self.conn = None
        self.cursor = None
        
        # 映射信息
        self.tables = {}         # 表信息
        self.fields = {}         # 按表分组的字段信息
        self.relationships = {}  # 表关系
        self.path_to_field = {}  # JSON路径到数据库字段的映射
        
        # 统计信息
        self.files_processed = 0
        self.records_processed = 0
        self.records_failed = 0
    
    def connect_to_database(self):
        """连接到PostgreSQL数据库
        
        Returns:
            bool: 连接成功返回True，否则返回False
        """
        try:
            self.conn = psycopg2.connect(**self.db_config)
            self.cursor = self.conn.cursor()
            
            # 启用详细错误报告
            self.cursor.execute("SET client_min_messages TO NOTICE;")
            
            print(f"已连接到数据库: {self.db_config['database']}")
            return True
        except Exception as e:
            print(f"连接数据库时出错: {str(e)}")
            return False
    
    def execute_schema(self):
        """执行SQL模式文件以创建数据库表，如果表已存在则先删除
        
        Returns:
            bool: 模式执行成功返回True，否则返回False
        """
        try:
            # 先检查表是否存在并删除
            print("检查并删除已存在的表...")
            
            # 查询当前数据库中的所有表
            self.cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
                AND table_type = 'BASE TABLE'
            """)
            
            existing_tables = [row[0] for row in self.cursor.fetchall()]
            
            # 根据关系先删除子表，再删除主表
            if self.data_type in existing_tables:
                # 查找所有引用主表的外键
                self.cursor.execute("""
                    SELECT tc.table_name, tc.constraint_name
                    FROM information_schema.table_constraints AS tc
                    JOIN information_schema.constraint_column_usage AS ccu
                    ON tc.constraint_name = ccu.constraint_name
                    WHERE tc.constraint_type = 'FOREIGN KEY'
                    AND ccu.table_name = %s
                """, (self.data_type,))
                
                # 删除外键约束
                for row in self.cursor.fetchall():
                    table_name = row[0]
                    constraint_name = row[1]
                    print(f"删除表 {table_name} 上的外键约束 {constraint_name}")
                    self.cursor.execute(f"ALTER TABLE {table_name} DROP CONSTRAINT {constraint_name}")
                
                # 删除表名以data_type开头的所有表
                tables_to_drop = [t for t in existing_tables if t.startswith(self.data_type)]
                if tables_to_drop:
                    # 按名称逆序排序，通常子表有更长的名称
                    tables_to_drop.sort(reverse=True)
                    
                    # 删除每个表
                    for table in tables_to_drop:
                        print(f"删除表: {table}")
                        self.cursor.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
            
            # 现在执行模式文件
            with open(self.schema_file, 'r', encoding='utf-8') as f:
                sql_script = f.read()
                
            print(f"执行来自{self.schema_file}的模式")
            self.cursor.execute(sql_script)
            self.conn.commit()
            print("模式执行成功")
            return True
            
        except Exception as e:
            print(f"执行模式时出错: {str(e)}")
            if self.conn:
                self.conn.rollback()
            return False
    
    def load_mappings(self):
        """从CSV文件加载映射信息
        
        Returns:
            bool: 映射加载成功返回True，否则返回False
        """
        try:
            # 加载表信息
            tables_file = os.path.join(self.mapping_dir, f"{self.data_type}_tables.csv")
            tables_df = pd.read_csv(tables_file)
            
            # 创建表信息字典
            for _, row in tables_df.iterrows():
                table_name = row['table_name']
                self.tables[table_name] = {
                    'type': row['table_type'],
                    'parent': row['parent_table'] if pd.notna(row['parent_table']) else None,
                    'has_arrays': row['has_arrays'] == 'Yes',
                    'path': row['path'] if pd.notna(row['path']) else None
                }
            
            # 加载字段映射
            fields_file = os.path.join(self.mapping_dir, f"{self.data_type}_fields.csv")
            fields_df = pd.read_csv(fields_file)
            
            # 创建字段信息字典
            for _, row in fields_df.iterrows():
                table_name = row['table_name']
                field_name = row['field_name']
                path = row['original_path']
                is_array = row['is_array'] == 'Yes'
                
                # 如果表字段不存在则初始化
                if table_name not in self.fields:
                    self.fields[table_name] = {}
                
                # 添加字段信息
                self.fields[table_name][field_name] = {
                    'path': path,
                    'is_array': is_array
                }
                
                # 添加路径到字段的映射
                if not is_array:  # 跳过数组字段的直接映射
                    if path not in self.path_to_field:
                        self.path_to_field[path] = []
                    self.path_to_field[path].append((table_name, field_name))
            
            # 加载表关系
            relationships_file = os.path.join(self.mapping_dir, f"{self.data_type}_relationships.csv")
            relationships_df = pd.read_csv(relationships_file)
            
            # 创建关系字典
            for _, row in relationships_df.iterrows():
                child_table = row['child_table']
                parent_table = row['parent_table']
                foreign_key = row['foreign_key_field']
                path = row['path'] if pd.notna(row['path']) else None
                
                self.relationships[child_table] = {
                    'parent': parent_table,
                    'foreign_key': foreign_key,
                    'path': path
                }
            
            print(f"已从{self.mapping_dir}加载映射信息")
            print(f"表: {len(self.tables)}, 字段: {sum(len(fields) for fields in self.fields.values())}")
            
            return True
        except Exception as e:
            print(f"加载映射时出错: {str(e)}")
            return False
    
    def _process_value(self, value, data_type, max_length, field_name):
        """根据数据类型处理值
        
        Args:
            value: 原始值
            data_type: 数据库字段类型
            max_length: 最大长度（如果适用）
            field_name: 字段名称
            
        Returns:
            处理后的值，或None如果值不兼容
        """
        try:
            # 处理空值
            if value is None or value == '':
                return None
                
            # 根据数据类型处理值
            if 'char' in data_type or 'text' in data_type:
                # 字符串类型
                if isinstance(value, (dict, list)):
                    # 将复杂类型转换为JSON字符串
                    value = json.dumps(value)
                elif not isinstance(value, str):
                    # 将其他类型转换为字符串
                    value = str(value)
                
                # 检查长度是否超过限制
                if max_length and len(value) > max_length:
                    value = value[:max_length]  # 截断
                    
                return value
                
            elif 'int' in data_type or data_type == 'integer':
                # 整数类型
                if isinstance(value, str) and not value.isdigit():
                    return None  # 非数字字符串不能转换为整数
                try:
                    return int(value)
                except (ValueError, TypeError):
                    return None
                    
            elif 'bool' in data_type:
                # 布尔类型
                if isinstance(value, bool):
                    return value
                elif isinstance(value, str):
                    return value.lower() in ('true', 'yes', 'y', '1', 't')
                elif isinstance(value, (int, float)):
                    return bool(value)
                else:
                    return None
                    
            elif 'date' in data_type:
                # 日期类型
                if not isinstance(value, str):
                    return None
                    
                # 尝试处理常见的日期格式
                date_formats = [
                    '%Y%m%d',        # 20210315
                    '%Y-%m-%d',      # 2021-03-15
                    '%Y/%m/%d',      # 2021/03/15
                    '%d/%m/%Y',      # 15/03/2021
                    '%m/%d/%Y',      # 03/15/2021
                    '%Y-%m-%dT%H:%M:%S'  # ISO格式
                ]
                
                # 对于关键日期字段的特殊处理
                if field_name.startswith('date_'):
                    # 保留符合PostgreSQL日期格式的部分
                    if 'T' in value:
                        value = value.split('T')[0]  # 仅保留日期部分
                        
                    # FDA特有的日期格式 - YYYYMMDD
                    if len(value) == 8 and value.isdigit():
                        try:
                            year = int(value[:4])
                            month = int(value[4:6])
                            day = int(value[6:8])
                            if 1900 <= year <= 2100 and 1 <= month <= 12 and 1 <= day <= 31:
                                return f"{year}-{month:02d}-{day:02d}"  # 转换为ISO格式
                        except ValueError:
                            pass
                
                # 尝试各种日期格式
                for fmt in date_formats:
                    try:
                        date_obj = datetime.datetime.strptime(value, fmt)
                        return date_obj.strftime('%Y-%m-%d')  # 标准化为ISO格式
                    except ValueError:
                        continue
                        
                # 无法解析日期，返回原始字符串（让数据库处理转换）
                return value
                
            elif 'json' in data_type:
                # JSON类型
                if isinstance(value, (dict, list)):
                    return json.dumps(value)
                elif isinstance(value, str):
                    try:
                        # 确保是有效的JSON
                        json.loads(value)
                        return value
                    except json.JSONDecodeError:
                        return json.dumps(value)  # 作为字符串处理
                else:
                    return json.dumps(value)
                    
            else:
                # 对于其他类型，返回原始值
                return value
                
        except Exception as e:
            print(f"处理字段{field_name}的值时出错: {str(e)}")
            return None
    
    def find_json_files(self, input_dir=None, pattern=None, recursive=False):
        """在输入目录中查找JSON文件
        
        Args:
            input_dir (str, optional): 输入目录路径（默认使用self.input_dir）
            pattern (str, optional): 要匹配的文件名模式
            recursive (bool, optional): 是否搜索子目录
            
        Returns:
            list: 符合条件的文件路径列表
        """
        if input_dir is None:
            input_dir = self.input_dir
            
        if pattern is None:
            pattern = "*.json"
            
        files = []
        if recursive:
            # 递归搜索
            for root, _, filenames in os.walk(input_dir):
                for filename in filenames:
                    if filename.endswith('.json'):
                        files.append(os.path.join(root, filename))
        else:
            # 非递归搜索
            search_pattern = os.path.join(input_dir, pattern)
            files = glob.glob(search_pattern)
            
        return files
    
    def process_json_files(self, max_files=10, max_records_per_file=100, recursive=True):
        """处理JSON文件并将数据导入数据库
        
        Args:
            max_files (int, optional): 要处理的最大文件数
            max_records_per_file (int, optional): 每个文件要处理的最大记录数
            recursive (bool, optional): 是否搜索子目录
            
        Returns:
            bool: 处理成功返回True，否则返回False
        """
        # 查找JSON文件
        json_files = self.find_json_files(recursive=recursive)
        if not json_files:
            print(f"在{self.input_dir}中未找到JSON文件")
            return False
            
        # 限制文件数量
        json_files = json_files[:min(len(json_files), max_files)]
        print(f"找到{len(json_files)}个JSON文件，将处理{len(json_files)}个")
        
        # 处理每个文件
        for i, file_path in enumerate(json_files):
            print(f"处理文件{i+1}/{len(json_files)}: {os.path.basename(file_path)}")
            try:
                success = self.process_file(file_path, max_records_per_file)
                
                if success > 0:
                    self.files_processed += 1
                    # 每个文件后提交
                    self.conn.commit()
                else:
                    # 如果文件处理失败，回滚以便处理下一个文件
                    if self.conn:
                        self.conn.rollback()
            except Exception as e:
                print(f"处理文件时发生错误: {str(e)}")
                if self.conn:
                    self.conn.rollback()
        
        # 打印摘要
        print("\n导入摘要:")
        print(f"已处理文件数: {self.files_processed}")
        print(f"已处理记录数: {self.records_processed}")
        print(f"失败记录数: {self.records_failed}")
        
        return self.files_processed > 0
    
    def process_file(self, file_path, max_records=100):
        """处理单个JSON文件并导入其数据
        
        Args:
            file_path (str): JSON文件路径
            max_records (int, optional): 要处理的最大记录数
            
        Returns:
            int: 处理的记录数
        """
        try:
            # 加载JSON文件
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # 识别记录数组
            records = []
            if isinstance(data, list):
                # 文件包含记录数组
                records = data[:max_records]
            elif 'results' in data and isinstance(data['results'], list):
                # 标准FDA格式，带有'results'数组
                records = data['results'][:max_records]
            else:
                # 文件中的单个记录
                records = [data]
            
            # 处理每个记录
            processed_count = 0
            for record in records:
                success = self.import_record(record)
                if success:
                    processed_count += 1
                    self.records_processed += 1
                else:
                    self.records_failed += 1
            
            print(f"从文件中处理了{processed_count}/{len(records)}条记录")
            return processed_count
        
        except Exception as e:
            print(f"处理文件{file_path}时出错: {str(e)}")
            return 0
    
    def extract_all_paths(self, obj, path="", base_path=None):
        """从JSON对象中提取所有路径和值
        
        Args:
            obj (dict/list): JSON对象或数组
            path (str, optional): 当前路径前缀
            base_path (str, optional): 要添加到所有路径前面的基础路径
            
        Returns:
            dict: 路径-值对的字典
        """
        paths = {}
        
        # 如果提供了基础路径，则应用它
        if base_path and not path:
            path = base_path
        elif base_path:
            path = f"{base_path}.{path}"
        
        if isinstance(obj, dict):
            # 特殊处理FDA结构
            # 如果这是根对象，并且我们处理的是'event'类型，
            # 并且路径为空，则添加'event'前缀
            if not path and self.data_type == 'event' and 'results' not in obj:
                path = 'event'
            
            # 特殊处理FDA API结构：如果存在results数组
            if 'results' in obj and isinstance(obj['results'], list) and not path:
                # 这是一个FDA API响应，我们应该直接处理results数组的第一项
                if obj['results']:
                    # 设置路径为data_type（例如'event'）并处理第一个结果
                    return self.extract_all_paths(obj['results'][0], self.data_type)
                return paths
                
            for key, value in obj.items():
                new_path = f"{path}.{key}" if path else key
                
                # 存储此路径的值
                paths[new_path] = value
                
                # 处理嵌套结构
                if isinstance(value, (dict, list)):
                    nested_paths = self.extract_all_paths(value, new_path)
                    paths.update(nested_paths)
        
        elif isinstance(obj, list):
            # 对于列表，在当前路径存储整个列表
            if path:
                paths[path] = obj
                array_path = f"{path}[]"
                paths[array_path] = obj
                
            # 处理每个列表项
            for i, item in enumerate(obj):
                if isinstance(item, (dict, list)):
                    item_path = f"{path}[{i}]"
                    nested_paths = self.extract_all_paths(item, item_path)
                    paths.update(nested_paths)
        
        return paths
    
    def extract_nested_data(self, record, path):
        """根据路径从记录中提取嵌套数据
        
        Args:
            record (dict): JSON记录
            path (str): 要提取的路径
            
        Returns:
            object: 在指定路径提取的数据
        """
        if not path:
            return None
            
        # 将路径分割成组件
        parts = path.split('.')
        
        # 导航通过嵌套结构
        current = record
        for part in parts:
            if part == self.data_type:
                # 跳过数据类型前缀
                continue
                
            # 处理数组索引（如果存在）
            if '[' in part:
                base_part = part.split('[')[0]
                if base_part in current:
                    current = current[base_part]
                else:
                    return None
            elif part in current:
                current = current[part]
            else:
                return None
                
        return current
    
    def import_record(self, record):
        """将单个JSON记录导入数据库
        
        Args:
            record (dict): 要导入的JSON记录
            
        Returns:
            bool: 导入成功返回True，否则返回False
        """
        try:
            # 从主表开始（data_type）
            main_table = self.data_type
            
            # 从记录中提取所有路径和值
            all_paths = self.extract_all_paths(record)
            
            # 获取表的列信息
            self.cursor.execute(f"""
                SELECT column_name, data_type, character_maximum_length 
                FROM information_schema.columns 
                WHERE table_name = '{main_table}'
            """)
            
            columns_info = {}
            for row in self.cursor.fetchall():
                column_name = row[0]
                data_type = row[1]
                max_length = row[2]
                columns_info[column_name] = {'type': data_type, 'max_length': max_length}
            
            # 将路径映射到主表的数据库字段
            main_table_data = {}
            for path, value in all_paths.items():
                if path in self.path_to_field:
                    for table, field in self.path_to_field[path]:
                        if table == main_table:
                            # 检查字段是否在表中存在
                            if field not in columns_info:
                                continue
                                
                            # 获取字段类型信息
                            field_type = columns_info[field]['type']
                            max_length = columns_info[field]['max_length']
                            
                            # 根据字段类型处理值
                            processed_value = self._process_value(value, field_type, max_length, field)
                            
                            # 只添加非None值
                            if processed_value is not None:
                                main_table_data[field] = processed_value
            
            # 打印第一条记录的详细信息进行调试
            if self.records_processed < 1:
                print(f"\n调试 - 主表字段映射:")
                for field, value in main_table_data.items():
                    print(f"  {field}: {type(value).__name__} = {value}")
            
            # 插入主记录
            main_id = self.insert_into_table(main_table, main_table_data)
            if main_id is None:
                print(f"未能将主记录插入{main_table}")
                return False
            
            # 处理子表
            for child_table, child_info in self.tables.items():
                if child_info['parent'] == main_table:
                    # 这是主表的直接子表
                    self.process_child_table(child_table, main_id, record, all_paths)
            
            return True
            
        except Exception as e:
            print(f"导入记录时出错: {str(e)}")
            return False
    
    def process_child_table(self, table_name, parent_id, record, all_paths):
        """处理子表并插入其数据
        
        Args:
            table_name (str): 子表名称
            parent_id (int): 父记录ID
            record (dict): JSON记录
            all_paths (dict): 所有提取的路径和值
            
        Returns:
            bool: 处理成功返回True，否则返回False
        """
        try:
            # 获取表信息
            table_info = self.tables[table_name]
            table_type = table_info['type']
            path = table_info['path']
            
            # 获取关系信息
            relationship = self.relationships.get(table_name)
            if not relationship:
                print(f"未找到表{table_name}的关系信息")
                return False
            
            foreign_key = relationship['foreign_key']
            
            # 获取列信息
            self.cursor.execute(f"""
                SELECT column_name, data_type, character_maximum_length 
                FROM information_schema.columns 
                WHERE table_name = '{table_name}'
            """)
            
            columns_info = {}
            for row in self.cursor.fetchall():
                column_name = row[0]
                data_type = row[1]
                max_length = row[2]
                columns_info[column_name] = {'type': data_type, 'max_length': max_length}
            
            # 处理不同的表类型
            if table_type == 'array':
                # 数组表 - 需要处理每个数组项
                array_data = self.extract_nested_data(record, path)
                
                if isinstance(array_data, list):
                    for item in array_data:
                        # 处理数组中的每个项目
                        if isinstance(item, dict):
                            # 提取项目路径
                            item_paths = self.extract_all_paths(item, base_path=path)
                            
                            # 将路径映射到字段
                            item_data = {foreign_key: parent_id}  # 添加外键
                            
                            for item_path, value in item_paths.items():
                                if item_path in self.path_to_field:
                                    for field_table, field_name in self.path_to_field[item_path]:
                                        if field_table == table_name and field_name in columns_info:
                                            # 获取字段类型
                                            field_type = columns_info[field_name]['type']
                                            max_length = columns_info[field_name]['max_length']
                                            
                                            # 处理值
                                            processed_value = self._process_value(value, field_type, max_length, field_name)
                                            if processed_value is not None:
                                                item_data[field_name] = processed_value
                            
                            # 插入项目记录
                            item_id = self.insert_into_table(table_name, item_data)
                            
                            # 处理孙表
                            if item_id is not None:
                                for grandchild, grandchild_info in self.tables.items():
                                    if grandchild_info['parent'] == table_name:
                                        self.process_child_table(grandchild, item_id, item, item_paths)
            
            elif table_type == 'object':
                # 对象表 - 单个记录
                object_data = self.extract_nested_data(record, path)
                
                if isinstance(object_data, dict):
                    # 提取对象路径
                    object_paths = self.extract_all_paths(object_data, base_path=path)
                    
                    # 将路径映射到字段
                    object_record = {foreign_key: parent_id}  # 添加外键
                    
                    for object_path, value in object_paths.items():
                        if object_path in self.path_to_field:
                            for field_table, field_name in self.path_to_field[object_path]:
                                if field_table == table_name and field_name in columns_info:
                                    # 获取字段类型
                                    field_type = columns_info[field_name]['type']
                                    max_length = columns_info[field_name]['max_length']
                                    
                                    # 处理值
                                    processed_value = self._process_value(value, field_type, max_length, field_name)
                                    if processed_value is not None:
                                        object_record[field_name] = processed_value
                    
                    # 插入对象记录
                    object_id = self.insert_into_table(table_name, object_record)
                    
                    # 处理孙表
                    if object_id is not None:
                        for grandchild, grandchild_info in self.tables.items():
                            if grandchild_info['parent'] == table_name:
                                self.process_child_table(grandchild, object_id, object_data, object_paths)
            
            return True
            
        except Exception as e:
            print(f"处理子表{table_name}时出错: {str(e)}")
            return False
    
    def insert_into_table(self, table_name, data):
        """将数据插入数据库表
        
        Args:
            table_name (str): 表名
            data (dict): 要插入的字段-值对
            
        Returns:
            int: 插入的记录ID或者插入失败时返回None
        """
        try:
            # 如果没有数据要插入，则跳过
            if not data:
                print(f"没有数据要插入到{table_name}")
                return None
            
            # 添加默认id字段（仅当不存在时）
            if 'id' not in data:
                data['id'] = 'DEFAULT'
                
            # 进行新事务以隔离错误
            self.conn.rollback()  # 回滚任何先前失败的事务
            
            # 获取表的列信息
            self.cursor.execute(f"""
                SELECT column_name, data_type, character_maximum_length 
                FROM information_schema.columns 
                WHERE table_name = '{table_name}'
            """)
            
            valid_columns = {}
            for row in self.cursor.fetchall():
                column_name = row[0]
                data_type = row[1]
                max_length = row[2]
                valid_columns[column_name] = {'type': data_type, 'max_length': max_length}
            
            # 过滤并处理数据
            filtered_data = {}
            for col, val in data.items():
                if col in valid_columns:
                    # 获取字段类型信息
                    col_type = valid_columns[col]['type']
                    max_length = valid_columns[col]['max_length']
                    
                    # 特殊处理
                    if val == 'DEFAULT':
                        filtered_data[col] = val  # 保持DEFAULT关键字
                    else:
                        # 转换值以匹配数据库类型
                        processed_val = self._process_value(val, col_type, max_length, col)
                        if processed_val is not None:
                            filtered_data[col] = processed_val
            
            if not filtered_data:
                print(f"过滤后没有有效数据插入到{table_name}")
                return None
                
            # 构建SQL查询
            columns = list(filtered_data.keys())
            
            # 特殊处理DEFAULT值
            values = []
            placeholders = []
            for col in columns:
                if filtered_data[col] == 'DEFAULT':
                    placeholders.append('DEFAULT')
                else:
                    placeholders.append('%s')
                    values.append(filtered_data[col])
            
            query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(placeholders)}) RETURNING id"
            
            # 输出调试信息 - 查看将要执行的SQL
            if table_name == 'event' and self.records_processed < 2:  # 仅对前两条记录显示详细信息
                print(f"\n调试 - 执行SQL: {query}")
                print(f"调试 - 参数值: {values}")
            
            # 执行查询
            self.cursor.execute(query, values)
            inserted_id = self.cursor.fetchone()[0]
            self.conn.commit()  # 立即提交此记录
            
            return inserted_id
        
        except Exception as e:
            print(f"插入到{table_name}时出错: {str(e)}")
            if table_name == 'event':
                print(f"调试 - 尝试插入的数据: {data}")
            self.conn.rollback()  # 回滚以便下一次尝试
            return None
    
    def close(self):
        """关闭数据库连接"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
            print("数据库连接已关闭")

def main():
    """解析参数并运行导入器的主函数"""
    parser = argparse.ArgumentParser(description='将FDA JSON数据导入SQL数据库')
    parser.add_argument('--input_dir', required=True, help='包含JSON文件的目录')
    parser.add_argument('--schema_file', default='public', help='SQL模式文件路径')
    parser.add_argument('--mapping_dir', required=True, help='包含映射CSV文件的目录')
    parser.add_argument('--data_type', default='event', help='数据类型标识符（event, recall等）')
    parser.add_argument('--db_host', default='localhost', help='数据库主机')
    parser.add_argument('--db_port', type=int, default=5432, help='数据库端口')
    parser.add_argument('--db_name', default='fda_device', help='数据库名称')
    parser.add_argument('--db_user', default='postgres', help='数据库用户名')
    parser.add_argument('--db_password', default=12345687, help='数据库密码')
    parser.add_argument('--max_files', type=int, default=10, help='要处理的最大文件数')
    parser.add_argument('--max_records', type=int, default=100, help='每个文件的最大记录数')
    parser.add_argument('--recursive', action='store_true', help='搜索子目录中的JSON文件')
    parser.add_argument('--skip_schema', action='store_true', help='跳过执行模式文件，直接导入数据')
    parser.add_argument('--debug', action='store_true', help='启用详细的调试输出')
    
    args = parser.parse_args()
    
    # 数据库配置
    db_config = {
        'host': args.db_host,
        'port': args.db_port,
        'database': args.db_name,
        'user': args.db_user,
        'password': args.db_password
    }
    
    # 创建导入器
    importer = JsonToSqlImporter(
        db_config, 
        args.input_dir,
        args.schema_file,
        args.mapping_dir,
        args.data_type
    )
    
    try:
        # 连接到数据库
        if not importer.connect_to_database():
            return 1
            
        # 执行模式（除非设置了skip_schema标志）
        if not args.skip_schema:
            if not importer.execute_schema():
                return 1
            
        # 加载映射
        if not importer.load_mappings():
            return 1
            
        # 处理JSON文件
        if not importer.process_json_files(args.max_files, args.max_records, args.recursive):
            return 1
            
        print("导入成功完成")
        return 0
    
    finally:
        # 始终关闭连接
        importer.close()

if __name__ == "__main__":
    sys.exit(main())