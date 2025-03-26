#!/usr/bin/env python3
"""
JSON节点计数工具

统计JSON文件中各节点的数量，便于数据验证。

用法:
    python json_node_counter.py --json_file data.json [--prefix event] [--output_csv counts.csv]
"""

import json
import csv
import argparse
import os
import re
from collections import Counter, defaultdict
import sys

def extract_json_paths(json_obj, prefix="", path="", collected_paths=None):
    """递归提取JSON中所有可能路径"""
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

def extract_value_by_path(record, path, prefix="event"):
    """根据路径从JSON记录中提取值"""
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

def process_json_file(file_path, prefix="event"):
    """处理单个JSON文件"""
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

def count_json_nodes(records, prefix="event"):
    """统计JSON记录中各节点的数量和类型，排除空内容的节点"""
    path_counts = Counter()
    node_types = defaultdict(Counter)
    empty_counts = Counter()  # 记录空节点
    record_count = len(records)
    
    print(f"正在分析 {record_count} 条记录的节点数量...")
    
    for i, record in enumerate(records):
        # 显示进度
        if record_count > 100 and (i % 100 == 0 or i == record_count - 1):
            print(f"  处理进度: {i+1}/{record_count}")
            
        # 提取此记录的路径并计数
        record_paths = extract_json_paths(record, prefix)
        
        # 统计每个路径的值类型，并排除空值
        for path in record_paths:
            value = extract_value_by_path(record, path, prefix)
            
            # 检查值是否为空
            is_empty = (value is None or 
                       (isinstance(value, (str, list, dict)) and len(value) == 0) or
                       (isinstance(value, (int, float)) and value == 0))
            
            if is_empty:
                empty_counts[path] += 1
            else:
                path_counts[path] += 1
                value_type = type(value).__name__
                node_types[path][value_type] += 1
    
    # 筛选主节点（前缀后的第一级）
    main_nodes = {}
    main_node_types = {}
    prefix_pattern = f"^{prefix}\\."
    
    for path, count in path_counts.items():
        # 获取主节点（前缀后一级）
        if re.match(prefix_pattern, path):
            parts = path.split('.')
            if len(parts) >= 2:
                main_node = parts[1]
                if '.' not in main_node and '[' not in main_node:
                    main_nodes[main_node] = count
                    main_node_types[main_node] = dict(node_types[path])
    
    # 整体数据集统计
    print(f"\n数据集统计:")
    print(f"  记录总数: {record_count}")
    print(f"  不同路径数: {len(path_counts)}")
    print(f"  一级节点数: {len(main_nodes)}")
    print(f"  空节点总数: {sum(empty_counts.values())}")
    
    print("\n主要节点统计 (排除空值):")
    for node, count in sorted(main_nodes.items(), key=lambda x: x[1], reverse=True):
        node_path = f"{prefix}.{node}"
        types = main_node_types.get(node, {})
        types_str = ", ".join([f"{t}: {c}" for t, c in types.items()])
        empty_count = empty_counts.get(node_path, 0)
        if empty_count > 0:
            print(f"  {node}: {count} (有效), {empty_count} (空值) ({types_str})")
        else:
            print(f"  {node}: {count} ({types_str})")
    
    return path_counts, main_nodes, node_types, empty_counts

def main():
    parser = argparse.ArgumentParser(description='统计JSON文件中各节点的数量，便于数据验证')
    parser.add_argument('--json_file', required=True, help='JSON文件路径')
    parser.add_argument('--prefix', default='event', help='路径前缀(如"event")')
    parser.add_argument('--output_csv', help='输出CSV文件路径')
    parser.add_argument('--detailed', action='store_true', help='输出详细路径计数')
    
    args = parser.parse_args()
    
    # 检查文件是否存在
    if not os.path.exists(args.json_file):
        print(f"错误: JSON文件不存在: {args.json_file}")
        return 1
    
    # 处理文件
    print(f"正在分析JSON文件: {args.json_file}")
    json_paths, records = process_json_file(args.json_file, args.prefix)
    
    if not records:
        print("错误: 无法从JSON文件中提取有效记录")
        return 1
        
    print(f"JSON中共发现 {len(json_paths)} 个路径，{len(records)} 条记录")
    
    # 计算各节点数量
    path_counts, main_nodes, node_types, empty_counts = count_json_nodes(records, args.prefix)
    
    # 如果指定了输出文件，保存结果
    if args.output_csv:
        output_file = args.output_csv
    else:
        output_file = os.path.splitext(args.json_file)[0] + "_node_counts.csv"
    
    with open(output_file, 'w', encoding='utf-8', newline='') as f:
        if args.detailed:
            # 输出所有路径的详细计数
            writer = csv.writer(f)
            writer.writerow(["path", "有效数量", "空值数量", "类型"])
            for path, count in sorted(path_counts.items()):
                types_str = ";".join([f"{t}:{c}" for t, c in node_types[path].items()])
                empty_count = empty_counts.get(path, 0)
                writer.writerow([path, count, empty_count, types_str])
        else:
            # 只输出主节点计数
            writer = csv.writer(f)
            writer.writerow(["节点", "有效数量", "空值数量", "类型"])
            for node, count in sorted(main_nodes.items(), key=lambda x: x[1], reverse=True):
                node_path = f"{args.prefix}.{node}"
                types_str = ";".join([f"{t}:{c}" for t, c in node_types[node_path].items()])
                empty_count = empty_counts.get(node_path, 0)
                writer.writerow([node, count, empty_count, types_str])
    
    print(f"节点计数已保存到: {output_file}")
    return 0

if __name__ == "__main__":
    sys.exit(main())