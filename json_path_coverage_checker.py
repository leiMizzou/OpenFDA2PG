#!/usr/bin/env python3
"""
JSON-CSV 路径覆盖检查工具 (增强版)

检查JSON文件中的所有路径是否已被paths CSV文件完整覆盖。
如果覆盖率为100%，根据fields CSV文件中的映射关系进行数据导入。
当处理目录中的多个JSON文件时，会为每个文件单独生成一个SQL文件。
增加了字段长度验证功能，确保数据不会超过数据库表字段规定的长度范围。

用法:
    # 处理单个文件
    python json_path_coverage_checker.py --json_file data.json --csv_file paths.csv --fields_csv fields.csv [--prefix event] [--output_sql output.sql] [--truncate_policy warn]
    
    # 处理目录下所有JSON文件，为每个文件生成独立SQL
    python json_path_coverage_checker.py --json_file ./data_dir --csv_file paths.csv --fields_csv fields.csv [--prefix event] [--output_sql output_base.sql] [--truncate_policy truncate]
    
    # 如果指定了--output_sql参数，将使用其作为基础名生成[output_base]_[json文件名].sql格式的文件
    # 如果未指定--output_sql参数，将使用[json文件名]_output.sql作为输出文件名
    
    # 可选的截断策略:
    # - warn: 仅警告但不截断 (默认)
    # - truncate: 自动截断超出长度的值
    # - both: 截断并警告
    # - error: 遇到超长值时报错并停止处理
"""

import json
import csv
import argparse
import os
import re
import uuid
import sys
from collections import defaultdict, Counter
from datetime import datetime

def extract_json_paths(json_obj, prefix="", path="", collected_paths=None):
    """递归提取JSON中所有可能路径
    
    Args:
        json_obj: JSON对象
        prefix: 字段前缀
        path: 当前路径
        collected_paths: 已收集路径集合
        
    Returns:
        set: 所有路径集合
    """
    if collected_paths is None:
        collected_paths = set()
        
    current_path = f"{path}.{prefix}" if path else prefix
    
    if isinstance(json_obj, dict):
        for key, value in json_obj.items():
            new_path = f"{current_path}.{key}" if current_path else key
            collected_paths.add(new_path)
            
            # 递归处理嵌套结构
            if isinstance(value, (dict, list)):
                extract_json_paths(value, key, current_path, collected_paths)
    
    elif isinstance(json_obj, list):
        # 记录数组路径
        array_path = f"{current_path}[]"
        collected_paths.add(array_path)
        
        # 处理数组元素
        for i, item in enumerate(json_obj[:3]):  # 只处理前3个元素，避免过大数组
            if isinstance(item, (dict, list)):
                item_path = f"{current_path}[{i}]"
                # 递归处理每个数组元素
                extract_json_paths(item, f"{prefix}[{i}]", path, collected_paths)
    
    return collected_paths

def load_csv_paths(csv_file):
    """从CSV文件加载所有路径
    
    Args:
        csv_file: CSV文件路径
        
    Returns:
        set: CSV中的路径集合
    """
    csv_paths = set()
    
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        # 确保CSV有path列
        if 'path' not in reader.fieldnames:
            raise ValueError("CSV文件缺少'path'列")
            
        for row in reader:
            path = row['path']
            if path:
                csv_paths.add(path)
    
    return csv_paths

def load_field_mappings(fields_csv):
    """从字段CSV文件加载映射关系
    
    Args:
        fields_csv: 字段CSV文件路径
        
    Returns:
        dict: 映射关系字典，按表名分组
    """
    mappings = defaultdict(list)
    
    with open(fields_csv, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        required_columns = ['table_name', 'field_name', 'original_path', 'data_type']
        for col in required_columns:
            if col not in reader.fieldnames:
                raise ValueError(f"CSV文件缺少'{col}'列")
        
        for row in reader:
            table_name = row['table_name']
            field_name = row['field_name']
            original_path = row['original_path']
            data_type = row['data_type']
            
            # 解析max_length (如果存在)
            max_length = None
            if 'max_length' in row and row['max_length']:
                try:
                    max_length = int(row['max_length'])
                except ValueError:
                    print(f"警告: 字段 {field_name} 的max_length值 '{row['max_length']}' 无效，应为整数")
            
            # 从数据类型中提取长度限制 (如 VARCHAR(255))
            if not max_length and data_type.startswith(('VARCHAR', 'CHAR')):
                length_match = re.search(r'\((\d+)\)', data_type)
                if length_match:
                    max_length = int(length_match.group(1))
            
            mappings[table_name].append({
                'field_name': field_name,
                'original_path': original_path,
                'data_type': data_type,
                'is_array': row.get('is_array', 'false').lower() == 'true',
                'max_length': max_length  # 添加最大长度字段
            })
    
    return mappings

def process_json_file(file_path, prefix="event"):
    """处理单个JSON文件
    
    Args:
        file_path: JSON文件路径
        prefix: 路径前缀
        
    Returns:
        tuple: (路径集合, JSON数据)
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # 获取记录列表
        records = []
        if 'results' in data and isinstance(data['results'], list):
            # FDA标准格式
            records = data['results']
        elif isinstance(data, list):
            # 纯数组格式
            records = data
        else:
            # 单个对象
            records = [data]
        
        # 处理所有记录，收集所有路径
        all_paths = set()
        for record in records:
            paths = extract_json_paths(record, prefix)
            all_paths.update(paths)
            
        return all_paths, records
            
    except Exception as e:
        print(f"处理文件 {file_path} 时出错: {str(e)}")
        return set(), []

def check_path_coverage(json_paths, csv_paths):
    """检查路径覆盖情况
    
    Args:
        json_paths: JSON中的路径集合
        csv_paths: CSV中的路径集合
        
    Returns:
        tuple: (是否完全覆盖, 缺失路径列表, 覆盖百分比)
    """
    # 找出JSON中存在但CSV中缺失的路径
    missing_paths = json_paths - csv_paths
    
    # 计算覆盖百分比
    coverage_percentage = 0
    if json_paths:
        coverage_percentage = (len(json_paths) - len(missing_paths)) / len(json_paths) * 100
    
    return len(missing_paths) == 0, missing_paths, coverage_percentage

def extract_value_by_path(record, path, prefix="event"):
    """根据路径从JSON记录中提取值
    
    Args:
        record: JSON记录
        path: 路径字符串
        prefix: 路径前缀
        
    Returns:
        任意值: 路径对应的值，如果路径不存在则返回None
    """
    # 处理前缀
    if path.startswith(prefix + '.'):
        path = path[len(prefix) + 1:]
    elif path == prefix:
        return record
    
    # 分解路径
    parts = path.split('.')
    current = record
    
    try:
        for part in parts:
            # 处理数组索引
            array_match = re.match(r'([^\[]+)\[(\d+)\]', part)
            if array_match:
                name, index = array_match.groups()
                index = int(index)
                current = current.get(name, [])
                if len(current) > index:
                    current = current[index]
                else:
                    return None
            else:
                current = current.get(part)
                
            if current is None:
                return None
    except (TypeError, AttributeError):
        return None
        
    return current

def format_value_for_sql(value, data_type, max_length=None, truncate_policy='warn', truncated_fields=None, field_info=None):
    """格式化值用于SQL语句，增强对特殊字符的处理，并处理长度限制
    
    Args:
        value: 原始值
        data_type: 数据类型
        max_length: 最大长度限制
        truncate_policy: 截断策略 ('warn', 'truncate', 'both', 'error')
        truncated_fields: 记录被截断的字段 (dict)
        field_info: 字段信息，用于报告
        
    Returns:
        str: 格式化后的SQL值
    """
    if value is None:
        return "NULL"
    
    if truncated_fields is None:
        truncated_fields = {}
    
    # 获取字段名和表名，用于错误报告
    field_name = field_info.get('field_name', 'unknown') if field_info else 'unknown'
    table_name = field_info.get('table_name', 'unknown') if field_info else 'unknown'
    
    if data_type.startswith('VARCHAR') or data_type.startswith('TEXT') or data_type.startswith('CHAR'):
        if isinstance(value, (dict, list)):
            # 将对象转换为JSON字符串
            try:
                import json
                value = json.dumps(value, ensure_ascii=False)
            except Exception:
                value = str(value)
        
        # 转换为字符串
        value_str = str(value)
        
        # 处理长度限制
        if max_length and len(value_str) > max_length:
            original_length = len(value_str)
            table_field = f"{table_name}.{field_name}"
            
            # 根据策略处理
            if truncate_policy in ('truncate', 'both'):
                # 截断值
                value_str = value_str[:max_length]
                
                # 记录被截断的字段
                if table_field not in truncated_fields:
                    truncated_fields[table_field] = {
                        'count': 0,
                        'max_original_length': 0,
                        'examples': []
                    }
                
                truncated_fields[table_field]['count'] += 1
                truncated_fields[table_field]['max_original_length'] = max(
                    truncated_fields[table_field]['max_original_length'], 
                    original_length
                )
                
                # 记录一些示例（限制数量以避免内存问题）
                if len(truncated_fields[table_field]['examples']) < 3:
                    preview = value_str[:30] + "..." if len(value_str) > 30 else value_str
                    truncated_fields[table_field]['examples'].append({
                        'original_length': original_length,
                        'preview': preview
                    })
            
            if truncate_policy in ('warn', 'both'):
                # 显示警告
                print(f"警告: {table_name}.{field_name} 的值已超过最大长度 {max_length}，原始长度为 {original_length}")
            
            if truncate_policy == 'error':
                # 停止处理并报错
                raise ValueError(f"错误: {table_name}.{field_name} 的值超出最大长度 {max_length}，原始长度为 {original_length}")
        
        # 转义单引号 (SQL标准转义是双写单引号)
        escaped_value = value_str.replace("'", "''")
        
        # 检查并处理不可见字符
        escaped_value = ''.join(char for char in escaped_value if ord(char) >= 32 or char in ('\n', '\r', '\t'))
        
        # 处理字符串中的反斜杠
        escaped_value = escaped_value.replace("\\", "\\\\")
        
        # 将换行符转换为其文本表示形式，防止SQL语法错误
        escaped_value = escaped_value.replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t")
        
        return f"'{escaped_value}'"
    
    elif data_type.startswith('DATE') or data_type.startswith('TIMESTAMP'):
        # 检查是否已经是日期格式
        if isinstance(value, (str)) and re.match(r'\d{4}-\d{2}-\d{2}', value):
            return f"'{value}'"
        else:
            try:
                # 尝试转换为日期
                datetime_obj = datetime.strptime(str(value), '%Y%m%d')
                formatted_date = datetime_obj.strftime('%Y-%m-%d')
                return f"'{formatted_date}'"
            except ValueError:
                # 如果无法转换，返回NULL
                return "NULL"
    
    elif data_type.startswith('BOOLEAN'):
        return "TRUE" if value else "FALSE"
    
    elif data_type.startswith('INTEGER') or data_type.startswith('SERIAL'):
        try:
            return str(int(value))
        except (ValueError, TypeError):
            return "NULL"
    
    elif data_type.startswith('FLOAT') or data_type.startswith('DOUBLE') or data_type.startswith('DECIMAL'):
        try:
            return str(float(value))
        except (ValueError, TypeError):
            return "NULL"
    
    elif data_type.startswith('JSON') or data_type.startswith('JSONB'):
        # 将对象转换为JSON字符串
        try:
            import json
            json_str = json.dumps(value, ensure_ascii=False)
            # 转义单引号
            escaped_json = json_str.replace("'", "''")
            return f"'{escaped_json}'"
        except Exception:
            # 如果失败，返回空JSON对象
            return "'{}'::jsonb"
    
    else:
        # 对于其他类型，直接返回字符串形式，并增加单引号
        try:
            value_str = str(value)
            escaped_value = value_str.replace("'", "''")
            return f"'{escaped_value}'"
        except Exception:
            return "NULL"

def generate_insert_statements(records, field_mappings, prefix="event", truncate_policy='warn'):
    """生成SQL插入语句
    
    Args:
        records: JSON记录列表
        field_mappings: 字段映射关系
        prefix: 路径前缀
        truncate_policy: 截断策略
        
    Returns:
        tuple: (按表分组的SQL插入语句, 被截断的字段信息)
    """
    inserts = defaultdict(list)
    record_count = len(records)
    
    # 跟踪被截断的字段
    truncated_fields = {}
    
    # 追踪每条记录的主表ID，用于外键关联
    record_ids = {}  # 记录ID映射
    parent_variables = {}  # 映射每条记录到SQL变量
    
    for i, record in enumerate(records):
        # 显示处理进度
        if i % 100 == 0 or i == record_count - 1:
            print(f"正在处理记录 {i+1}/{record_count}...")
        
        # 为每条记录生成唯一的UUID
        record_id = str(uuid.uuid4())
        record_ids[i] = record_id
        
        # 创建一个SQL变量名以存储生成的ID
        var_name = f"record_id_{i}"
        parent_variables[i] = var_name
        
        # 按表处理映射
        for table_name, fields in field_mappings.items():
            # 如果是主表以外的表，且没有父记录，则跳过
            if table_name != prefix and i not in record_ids:
                continue
                
            table_values = {}
            has_values = False
            
            # 收集该表的所有字段值
            for field in fields:
                field_name = field['field_name']
                path = field['original_path']
                data_type = field['data_type']
                max_length = field['max_length']
                
                value = extract_value_by_path(record, path, prefix)
                if value is not None:
                    # 添加表名到字段信息，用于错误报告
                    field_info = {**field, 'table_name': table_name}
                    
                    formatted_value = format_value_for_sql(
                        value, 
                        data_type, 
                        max_length, 
                        truncate_policy, 
                        truncated_fields,
                        field_info
                    )
                    table_values[field_name] = formatted_value
                    has_values = True
            
            # 如果有值，生成INSERT语句
            if has_values:
                field_names = []
                field_values = []
                
                # 如果是主表，添加预定义的ID
                if table_name == prefix:
                    field_names.append('id')  # ID字段
                    field_values.append(f"'{record_id}'")  # 使用预生成的UUID
                
                # 添加其他字段
                for name, value in table_values.items():
                    field_names.append(name)
                    field_values.append(value)
                
                # 如果不是主表，添加外键关联
                if table_name != prefix and i in record_ids:
                    # 获取主表记录ID
                    parent_id = record_ids[i]
                    parent_field = f"{prefix}_id"
                    
                    # 只有当字段不存在时才添加
                    if parent_field not in field_names:
                        field_names.append(parent_field)
                        field_values.append(f"'{parent_id}'")
                
                # 生成SQL
                field_names_str = ', '.join(field_names)
                field_values_str = ', '.join(field_values)
                
                if table_name == prefix:
                    # 添加注释和事件标识
                    if 'event_key' in table_values:
                        event_key = table_values['event_key'].strip("'")
                        insert_sql = f"-- Event Key: {event_key}\n"
                    else:
                        insert_sql = f"-- Record {i+1}\n"
                    
                    insert_sql += f"INSERT INTO {table_name} ({field_names_str}) VALUES ({field_values_str});"
                else:
                    insert_sql = f"INSERT INTO {table_name} ({field_names_str}) VALUES ({field_values_str});"
                
                inserts[table_name].append(insert_sql)
                
    return inserts, truncated_fields

def write_sql_file(inserts, output_file, truncated_fields=None):
    """将SQL语句写入文件
    
    Args:
        inserts: 按表分组的SQL插入语句
        output_file: 输出文件路径
        truncated_fields: 被截断的字段信息
    """
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write("-- 自动生成的数据导入SQL\n")
            f.write(f"-- 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            # 如果有被截断的字段，添加警告信息
            if truncated_fields:
                f.write("-- ⚠️ 警告: 以下字段值因超出长度限制而被截断:\n")
                for table_field, info in truncated_fields.items():
                    f.write(f"-- • {table_field}: {info['count']} 个值被截断, 最大原始长度: {info['max_original_length']}\n")
                f.write("\n")
            
            f.write("BEGIN;\n\n")
            
            # 按表顺序写入SQL语句
            tables_order = [
                'event',
                'event_device',
                'event_device_openfda',
                'event_patient',
                'event_mdr_text'
            ]
            
            # 先写入主表和已知序的表
            for table in tables_order:
                if table in inserts:
                    f.write(f"-- {table} 表数据\n")
                    for insert in inserts[table]:
                        f.write(insert + "\n")
                    f.write("\n")
            
            # 写入其他表
            for table, statements in inserts.items():
                if table not in tables_order:
                    f.write(f"-- {table} 表数据\n")
                    for insert in statements:
                        f.write(insert + "\n")
                    f.write("\n")
            
            f.write("COMMIT;\n")
            
        print(f"SQL语句已保存到: {output_file}")
    except Exception as e:
        print(f"写入SQL文件时出错: {str(e)}")

def write_truncation_report(truncated_fields, output_file):
    """生成字段截断报告
    
    Args:
        truncated_fields: 被截断的字段信息
        output_file: 输出报告文件路径
    """
    if not truncated_fields:
        return
        
    report_file = output_file.replace('.sql', '_truncation_report.txt')
    try:
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write("字段截断报告\n")
            f.write("=" * 80 + "\n\n")
            f.write(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            f.write(f"共有 {len(truncated_fields)} 个字段的值被截断\n\n")
            
            for table_field, info in sorted(truncated_fields.items()):
                f.write(f"字段: {table_field}\n")
                f.write(f"被截断值数量: {info['count']}\n")
                f.write(f"最大原始长度: {info['max_original_length']}\n")
                
                if info['examples']:
                    f.write("示例:\n")
                    for i, example in enumerate(info['examples']):
                        f.write(f"  {i+1}. 原始长度: {example['original_length']}, 预览: {example['preview']}\n")
                
                f.write("\n" + "-" * 40 + "\n\n")
        
        print(f"截断报告已保存到: {report_file}")
    except Exception as e:
        print(f"写入截断报告时出错: {str(e)}")

def main():
    parser = argparse.ArgumentParser(description='检查JSON文件中的路径是否被CSV完整覆盖，并生成数据导入SQL')
    parser.add_argument('--json_file', required=True, help='JSON文件路径或包含JSON文件的目录路径')
    parser.add_argument('--csv_file', required=True, help='路径CSV文件路径')
    parser.add_argument('--fields_csv', help='字段映射CSV文件路径')
    parser.add_argument('--prefix', default='event', help='路径前缀(如"event")')
    parser.add_argument('--output_sql', help='输出SQL文件路径')
    parser.add_argument('--truncate_policy', default='warn', choices=['warn', 'truncate', 'both', 'error'],
                        help='字段值超出长度限制时的处理策略: warn(仅警告), truncate(自动截断), both(截断并警告), error(报错)')
    
    args = parser.parse_args()
    
    # 检查CSV文件是否存在
    if not os.path.exists(args.csv_file):
        print(f"错误: CSV文件不存在: {args.csv_file}")
        return
    
    # 确定要处理的JSON文件
    json_files = []
    
    if os.path.isdir(args.json_file):
        # 是目录，处理目录下所有JSON文件
        for file in os.listdir(args.json_file):
            if file.lower().endswith('.json'):
                json_files.append(os.path.join(args.json_file, file))
        json_files.sort()  # 按文件名排序
        if not json_files:
            print(f"错误: 目录中没有找到JSON文件: {args.json_file}")
            return
        print(f"在目录 {args.json_file} 中找到 {len(json_files)} 个JSON文件")
    else:
        # 是单个文件
        if not os.path.exists(args.json_file):
            print(f"错误: JSON文件不存在: {args.json_file}")
            return
        json_files = [args.json_file]
    
    # 收集所有JSON文件中的路径用于覆盖检查
    all_json_paths = set()
    
    for i, json_file in enumerate(json_files):
        print(f"\n[{i+1}/{len(json_files)}] 正在分析JSON文件路径: {json_file}")
        json_paths, _ = process_json_file(json_file, args.prefix)
        print(f"  - 发现 {len(json_paths)} 个路径")
        all_json_paths.update(json_paths)
    
    print(f"\n所有JSON文件中共发现 {len(all_json_paths)} 个不同路径")
    
    print(f"\n正在加载CSV文件: {args.csv_file}")
    csv_paths = load_csv_paths(args.csv_file)
    print(f"CSV中共有 {len(csv_paths)} 个路径")
    
    # 检查覆盖情况
    is_fully_covered, missing_paths, coverage_percentage = check_path_coverage(all_json_paths, csv_paths)
    
    # 显示结果
    print("\n覆盖检查结果:")
    print(f"覆盖率: {coverage_percentage:.2f}%")
    
    if is_fully_covered:
        print("✅ 完全覆盖 - CSV文件包含所有JSON文件中的所有路径")
        
        # 如果覆盖率为100%且提供了字段映射文件，则为每个JSON文件生成SQL
        if args.fields_csv and os.path.exists(args.fields_csv):
            print("\n开始准备数据导入...")
            
            # 加载映射关系
            print(f"正在加载字段映射文件: {args.fields_csv}")
            field_mappings = load_field_mappings(args.fields_csv)
            print(f"加载了 {sum(len(fields) for fields in field_mappings.values())} 个字段映射")
            
            # 检查字段长度限制信息
            fields_with_length = 0
            for table, fields in field_mappings.items():
                for field in fields:
                    if field['max_length'] is not None:
                        fields_with_length += 1
            
            if fields_with_length > 0:
                print(f"发现 {fields_with_length} 个字段设置了长度限制")
                print(f"字段值超长处理策略: {args.truncate_policy}")
            else:
                print("警告: 没有字段设置长度限制，将无法进行长度验证")
                if args.truncate_policy != 'warn':
                    print("提示: 您指定了截断策略，但由于没有字段设置长度限制，该策略不会生效")
            
            # 为每个JSON文件单独生成SQL
            total_insert_count = 0
            all_truncated_fields = {}
            
            for i, json_file in enumerate(json_files):
                json_filename = os.path.basename(json_file)
                json_basename = os.path.splitext(json_filename)[0]
                
                print(f"\n[{i+1}/{len(json_files)}] 正在处理JSON文件: {json_file}")
                
                # 重新读取JSON文件以获取记录
                _, records = process_json_file(json_file, args.prefix)
                
                if not records:
                    print(f"  - 文件中没有有效记录，跳过SQL生成")
                    continue
                
                # 生成SQL语句
                print(f"  - 正在为 {len(records)} 条记录生成SQL插入语句...")
                inserts, truncated_fields = generate_insert_statements(
                    records, 
                    field_mappings, 
                    args.prefix, 
                    args.truncate_policy
                )
                
                # 合并截断字段信息
                for table_field, info in truncated_fields.items():
                    if table_field not in all_truncated_fields:
                        all_truncated_fields[table_field] = info
                    else:
                        all_truncated_fields[table_field]['count'] += info['count']
                        all_truncated_fields[table_field]['max_original_length'] = max(
                            all_truncated_fields[table_field]['max_original_length'],
                            info['max_original_length']
                        )
                        # 合并示例，但限制数量
                        remaining = 3 - len(all_truncated_fields[table_field]['examples'])
                        if remaining > 0:
                            all_truncated_fields[table_field]['examples'].extend(
                                info['examples'][:remaining]
                            )
                
                # 统计每个表的记录数
                file_records = sum(len(statements) for statements in inserts.values())
                total_insert_count += file_records
                print(f"  - 共生成 {file_records} 条INSERT语句")
                
                # 显示截断字段统计
                truncated_count = sum(info['count'] for info in truncated_fields.values())
                if truncated_count > 0:
                    print(f"  - ⚠️ 有 {truncated_count} 个值被截断（超出字段长度限制）")
                
                # 确定输出文件名
                if args.output_sql:
                    # 基于提供的输出路径创建针对当前文件的输出路径
                    output_dir = os.path.dirname(args.output_sql) or '.'
                    output_basename = os.path.basename(args.output_sql)
                    output_name, output_ext = os.path.splitext(output_basename)
                    output_file = os.path.join(output_dir, f"{output_name}_{json_basename}{output_ext}")
                else:
                    # 使用默认名称，基于JSON文件名
                    output_file = f"{json_basename}_output.sql"
                
                # 写入SQL文件
                print(f"  - 正在写入SQL文件: {output_file}")
                write_sql_file(inserts, output_file, truncated_fields)
                
                # 如果有截断字段，生成截断报告
                if truncated_fields:
                    write_truncation_report(truncated_fields, output_file)
            
            print(f"\n总共为所有文件生成了 {total_insert_count} 条INSERT语句")
            
            # 显示所有文件的截断字段统计
            total_truncated_count = sum(info['count'] for info in all_truncated_fields.values())
            if total_truncated_count > 0:
                print(f"\n⚠️ 所有文件中共有 {total_truncated_count} 个值被截断（超出字段长度限制）")
                print(f"受影响的字段数: {len(all_truncated_fields)}")
                print("\n字段截断摘要:")
                for table_field, info in sorted(all_truncated_fields.items()):
                    print(f"  • {table_field}: {info['count']} 个值被截断, 最大原始长度: {info['max_original_length']}")
    else:
        print(f"❌ 不完全覆盖 - 有 {len(missing_paths)} 个路径在CSV中缺失")
        print("\n缺失路径:")
        for path in sorted(missing_paths):
            print(f"  - {path}")
    
    # 如果还想检查CSV中有但JSON中没有的路径（冗余路径）
    redundant_paths = csv_paths - all_json_paths
    if redundant_paths:
        print(f"\n注意: CSV中有 {len(redundant_paths)} 个冗余路径（JSON中不存在）")
        print("这可能是正常的，因为CSV可能包含来自其他JSON文件的路径")

if __name__ == "__main__":
    main()