#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import json
import argparse
import logging
import psycopg2
import psycopg2.extras
import pandas as pd
from collections import defaultdict

logging.basicConfig(
    level=logging.DEBUG,  # 调试级别
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger("json_to_db_importer")


def read_csv_definitions(tables_csv, fields_csv, relationships_csv):
    """
    读取由 universal_json_analyzer.py 生成的 CSV 文件:
      - tables_csv: event_tables.csv
      - fields_csv: event_fields.csv
      - relationships_csv: event_relationships.csv
    
    返回三个 DataFrame (df_tables, df_fields, df_relationships)。
    """
    df_tables = pd.read_csv(tables_csv)
    df_fields = pd.read_csv(fields_csv)
    df_relationships = pd.read_csv(relationships_csv)
    logger.debug(f"读到 {len(df_tables)} 条表定义, "
                 f"{len(df_fields)} 条字段定义, "
                 f"{len(df_relationships)} 条关系定义.")
    return df_tables, df_fields, df_relationships


def build_table_structures(df_tables, df_fields, df_relationships):
    """
    将表、字段、关系信息构建成一个易于查找的数据结构。
    
    返回:
        tables_dict: {
            table_name: {
                'type': ...,
                'parent_table': ...,
                'fields': [
                    {
                        'field_name': ...,
                        'original_path': ...,
                        'is_array': ... (True/False),
                        'data_type': ...,
                    },
                    ...
                ],
                'has_arrays': ...,
                'relationship_type': '一对多' / '多对多' / None,
                'fk_field': 'xxx_id' 或 None,
                'path':  # 在 analyzer 输出中, 常常表示该表对应的 array_path/object_path
            },
            ...
        }
    """
    tables_dict = {}

    # 先按表名聚合表信息
    for _, row in df_tables.iterrows():
        table_name = row['table_name']
        tables_dict[table_name] = {
            'type': row['table_type'],
            'parent_table': row['parent_table'] if isinstance(row['parent_table'], str) and row['parent_table'] else None,
            'fields': [],
            'has_arrays': (row['has_arrays'] == 'Yes'),
            'relationship_type': None,
            'fk_field': None,
            # 如果 analyzer 的 CSV 里带有 path 字段，存一下
            'path': row.get('path', '')
        }

    # 填充字段信息
    for _, row in df_fields.iterrows():
        table_name = row['table_name']
        if table_name not in tables_dict:
            continue
        tables_dict[table_name]['fields'].append({
            'field_name': row['field_name'],
            'original_path': row['original_path'],
            'is_array': (row['is_array'] == 'Yes'),
            'data_type': row['data_type'],
        })

    # 填充关系信息
    for _, row in df_relationships.iterrows():
        child_table = row['child_table']
        if child_table in tables_dict:
            tables_dict[child_table]['relationship_type'] = row['relationship_type']
            tables_dict[child_table]['fk_field'] = row['foreign_key_field']

    # 调试输出
    for tname, info in tables_dict.items():
        logger.debug(f"表: {tname}, 类型: {info['type']}, 父表: {info['parent_table']}, "
                     f"字段数: {len(info['fields'])}, fk_field: {info['fk_field']}")
    return tables_dict


def connect_database(host, port, dbname, user, password):
    """
    用 psycopg2 连接 PostgreSQL 并返回 conn。
    """
    logger.debug(f"连接数据库 -> host={host}, port={port}, dbname={dbname}, user={user}")
    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password
    )
    return conn


def insert_main_table_record(conn, table_name, tables_dict, record_data):
    """
    插入记录到某个表的主记录（如主表或对象表）。
    record_data 是从 JSON 中根据 original_path 提取出来的键值对。

    注意新增处理:
      - 若字段值是 dict / list, 用 json.dumps 序列化为字符串, 避免 can't adapt type 'dict' 错误。

    返回该插入记录的 id (serial 主键)，如果跳过插入则返回 None。
    """
    table_info = tables_dict[table_name]
    field_list = table_info['fields']

    row_to_insert = {}
    for f in field_list:
        fname = f['field_name']
        if f['is_array']:
            # array 字段不在此处插
            continue

        val = record_data.get(fname, None)
        # 如果是 dict 或 list, 序列化为字符串
        if isinstance(val, (dict, list)):
            val = json.dumps(val, ensure_ascii=False)

        row_to_insert[fname] = val

    col_values = list(row_to_insert.values())
    # 如果所有值都是 None，就不插入
    if all(v is None for v in col_values):
        logger.debug(f"[{table_name}] 全字段均为空，跳过插入: {row_to_insert}")
        return None

    col_names = list(row_to_insert.keys())
    placeholders = [f"%s" for _ in col_names]

    insert_sql = f"""
        INSERT INTO {table_name} ({','.join(col_names)})
        VALUES ({','.join(placeholders)})
        RETURNING id;
    """
    logger.debug(f"[{table_name}] INSERT -> {insert_sql}, values={col_values}")

    with conn.cursor() as cur:
        cur.execute(insert_sql, col_values)
        new_id = cur.fetchone()[0]
        logger.debug(f"[{table_name}] 插入成功，new_id={new_id}")

    return new_id


def insert_enum_values(conn, enum_table, tables_dict, parent_id, parent_fk_field, values):
    """
    往枚举表插入多个枚举值 (如简单数组内容)，并设置外键。
    如果需要去重或增加计数，可在此处做 upsert 逻辑。
    """
    if not values:
        return
    if parent_id is None:
        # 没有父ID，无法插入
        logger.debug(f"插入枚举表 {enum_table} 时父ID为空，跳过。")
        return

    table_info = tables_dict[enum_table]
    if not table_info['fields']:
        return

    # 枚举表通常只有一个真正的值字段
    value_field_name = table_info['fields'][0]['field_name']  
    insert_sql = f"""
        INSERT INTO {enum_table} ({parent_fk_field}, {value_field_name})
        VALUES (%s, %s)
    """
    records_to_insert = []
    for val in values:
        if val is None:
            continue
        # 如果 val 是 dict/list, 也要 json.dumps
        if isinstance(val, (dict, list)):
            val = json.dumps(val, ensure_ascii=False)

        records_to_insert.append((parent_id, val))

    if not records_to_insert:
        logger.debug(f"[{enum_table}] 无有效枚举值可插入.")
        return

    logger.debug(f"[{enum_table}] BULK INSERT 枚举 {len(records_to_insert)} 条, SQL={insert_sql}")
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, insert_sql, records_to_insert, page_size=1000)


def extract_json_value_by_path(json_obj, path):
    """
    根据 analyzer 输出的 original_path 来提取值。
    若 path 包含 '.' / '[0]' 等，则逐层解析。若取不到则返回 None。
    
    (可选) 如果想跳过第一层 'event'/'recall'，可自行在此加一行:
      parts = path.split('.')
      if len(parts) > 1:
          parts = parts[1:]
      ...
    """
    logger.debug(f"extract_json_value_by_path: path={path}")
    if not path:
        return None

    parts = path.split('.')
    # 这里示例保留 analyzer 的完整路径, 不跳过第一个节点
    # 如果您想跳过, 可执行:
    # if len(parts) > 1:
    #     parts = parts[1:]

    cur_val = json_obj
    for p in parts:
        if not p:
            continue

        # 如果包含 [，说明有数组下标
        if '[' in p and ']' in p:
            field_name = p.split('[')[0]
            idx_str = p.split('[')[1].replace(']', '')
            if not isinstance(cur_val, dict):
                logger.debug(f"当前值不是 dict，无法取字段 {field_name}")
                return None
            if field_name not in cur_val:
                logger.debug(f"dict 中无字段 {field_name}")
                return None
            arr_val = cur_val[field_name]
            if not isinstance(arr_val, list):
                logger.debug(f"{field_name} 对应的值不是 list: {arr_val}")
                return None
            try:
                idx = int(idx_str)
                if idx < len(arr_val):
                    cur_val = arr_val[idx]
                else:
                    logger.debug(f"数组索引 {idx} 超出长度 {len(arr_val)}")
                    return None
            except ValueError:
                logger.debug(f"下标 {idx_str} 转整数失败")
                return None
        else:
            # 普通字段
            if not isinstance(cur_val, dict):
                logger.debug(f"当前值不是 dict, 无法取字段 {p}")
                return None
            if p not in cur_val:
                logger.debug(f"dict 中无字段 {p}")
                return None
            cur_val = cur_val[p]

    logger.debug(f"extract_json_value_by_path 结果={cur_val}")
    return cur_val


def flatten_json_record(json_obj, tables_dict, table_name, parent_id=None, conn=None):
    """
    递归处理当前表的字段 & 子表/子数组/枚举表。
    - json_obj: 当前 JSON 对象
    - table_name: 当前要处理的表
    - parent_id: 父表ID（如果有）
    - conn: 数据库连接

    返回当前表插入生成的 id（如果有）。
    """
    table_info = tables_dict[table_name]
    logger.debug(f"开始处理表 {table_name}, parent_id={parent_id}")
    field_list = table_info['fields']

    # 先构建 record_data，用于插入 main 记录（排除 array 字段）
    record_data = {}
    for f in field_list:
        if f['is_array']:
            continue
        val = extract_json_value_by_path(json_obj, f['original_path'])
        record_data[f['field_name']] = val

    # 如果有父外键，需要补上
    fk_field = table_info['fk_field']
    if fk_field and parent_id is not None:
        record_data[fk_field] = parent_id

    # 执行插入
    new_id = insert_main_table_record(conn, table_name, tables_dict, record_data)
    logger.debug(f"{table_name} -> new_id={new_id}")

    # 如果没插上(可能全是 None)，则不继续
    if not new_id:
        return None

    # 处理数组字段（枚举表 或 子表）
    for f in field_list:
        if not f['is_array']:
            continue

        # 原始 array 路径
        array_core_path = f['original_path']
        # 有时 analyzer 可能输出 "event.device[].brand_name" 之类
        # 您可根据场景只保留 "event.device[]" 做解析
        # 这里示例直接 extract 整个路径, 也能拿到 list, 
        # 但若拿不到可自行截断 ".brand_name"

        arr_val = extract_json_value_by_path(json_obj, array_core_path)
        if not isinstance(arr_val, list):
            logger.debug(f"未取到数组或不是 list: path={array_core_path}, value={arr_val}")
            continue
        logger.debug(f"取到数组 {array_core_path} 长度={len(arr_val)}")

        # 找下级表名
        child_table_name = None
        for tname, tinfo in tables_dict.items():
            # 若 tinfo['path'] 与 array_core_path 对应，则认为是其子表
            if tinfo.get('path') == array_core_path:
                child_table_name = tname
                break

        if not child_table_name:
            # 找不到就直接看 relationships, 或硬编码, 具体实现看需要
            logger.warning(f"无法为数组 {array_core_path} 找到子表，跳过处理。")
            continue

        # 判断枚举表 / 对象数组表
        if tables_dict[child_table_name]['type'] == 'enum':
            # 简单数组 -> 枚举表
            insert_enum_values(
                conn=conn,
                enum_table=child_table_name,
                tables_dict=tables_dict,
                parent_id=new_id,
                parent_fk_field=tables_dict[child_table_name]['fk_field'],
                values=arr_val
            )
        else:
            # 对象数组 -> 子表
            for item in arr_val:
                if not isinstance(item, dict):
                    # 如果里面还有字典以外的东西，可选 json.dumps 等
                    logger.debug(f"数组项不是 dict：{item}")
                    continue
                flatten_json_record(
                    item,
                    tables_dict,
                    child_table_name,
                    parent_id=new_id,
                    conn=conn
                )

    return new_id


def main():
    parser = argparse.ArgumentParser(description="基于 universal_json_analyzer 输出的结构，将 JSON 数据导入 PostgreSQL")
    parser.add_argument("--json_file", default='/Volumes/Lexar SSD 4TB - RAID0/GitHub/FAERS/datafiles/unzip/device/event/2023q4/device-event-0001-of-0006.json', help="待导入的单个 JSON 文件路径(或包含 results 数组的 JSON)")
    parser.add_argument("--tables_csv", default="./fda_analysis/event_tables.csv")
    parser.add_argument("--fields_csv", default="./fda_analysis/event_fields.csv")
    parser.add_argument("--relationships_csv", default="./fda_analysis/event_relationships.csv")
    parser.add_argument("--host", default="localhost", help="PostgreSQL 主机")
    parser.add_argument("--port", default=5432, type=int, help="PostgreSQL 端口")
    parser.add_argument("--dbname", default="fda_database", help="PostgreSQL 数据库名")
    parser.add_argument("--user", default="postgres", help="PostgreSQL 用户名")
    parser.add_argument("--password", default="12345687", help="PostgreSQL 密码")
    args = parser.parse_args()

    # 1. 读取 analyzer 生成的 CSV 定义
    if not (os.path.exists(args.tables_csv) and os.path.exists(args.fields_csv) and os.path.exists(args.relationships_csv)):
        logger.error("必须保证 event_tables.csv / event_fields.csv / event_relationships.csv 存在")
        sys.exit(1)
    df_tables, df_fields, df_relationships = read_csv_definitions(
        args.tables_csv, args.fields_csv, args.relationships_csv
    )

    # 2. 构建表结构字典
    tables_dict = build_table_structures(df_tables, df_fields, df_relationships)

    # 3. 连接数据库
    conn = connect_database(
        host=args.host,
        port=args.port,
        dbname=args.dbname,
        user=args.user,
        password=args.password
    )

    # 4. 读取 JSON 数据
    if not os.path.exists(args.json_file):
        logger.error(f"JSON 文件不存在: {args.json_file}")
        sys.exit(1)
    with open(args.json_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    # 判断数据结构(是否包含 'results' 或是数组)
    records = []
    if isinstance(data, dict) and "results" in data and isinstance(data["results"], list):
        records = data["results"]
        logger.debug(f"JSON 是标准FDA格式, results大小={len(records)}")
    elif isinstance(data, list):
        records = data
        logger.debug(f"JSON 是数组格式, 长度={len(records)}")
    else:
        # 单个对象
        records = [data]
        logger.debug("JSON 是单个对象格式。")

    # 找到主表( type='main' )
    main_table_name = None
    for tname, tinfo in tables_dict.items():
        if tinfo['type'] == 'main':
            main_table_name = tname
            break
    if not main_table_name:
        logger.error("在表定义中未找到 type=main 的主表！请检查 CSV。")
        sys.exit(1)

    # 5. 逐条插入
    inserted_count = 0
    for i, record in enumerate(records):
        logger.debug(f"\n======= 准备插入第 {i+1} 条记录 =======")
        new_id = flatten_json_record(
            record,
            tables_dict,
            main_table_name,
            parent_id=None,
            conn=conn
        )
        if new_id:
            inserted_count += 1

        # 打印进度
        if (i + 1) % 100 == 0:
            logger.info(f"已处理 {i+1} 条 JSON 记录 (实际插入 {inserted_count} 行主表记录)")

    logger.info(f"处理完毕, 共处理 {len(records)} 条 JSON 记录, 成功插入主表行数={inserted_count}")
    conn.commit()
    conn.close()
    logger.info("数据库已提交并关闭连接.")


if __name__ == "__main__":
    main()
