#!/usr/bin/env python3
"""
增强版SQL文件执行工具
执行指定目录下的所有SQL文件，按文件名顺序导入数据到PostgreSQL数据库，能够处理主键冲突。
改进版显示具体导致失败的字段名称。

用法:
    python execute_sql_files.py --sql_dir /path/to/sql/files [--host localhost] [--port 5432] 
    [--dbname postgres] [--user postgres] [--password yourpassword] [--skip_duplicates]
    
    或者执行单个文件:
    
    python execute_sql_files.py --sql_file output.sql [--host localhost] [--port 5432] 
    [--dbname postgres] [--user postgres] [--password yourpassword] [--skip_duplicates]
"""
import psycopg2
import argparse
import sys
import os
import time
import re


def handle_duplicate_keys(sql_content, skip_duplicates=True):
    """
    处理SQL中可能的重复键问题
    
    Args:
        sql_content: 原始SQL内容
        skip_duplicates: 是否跳过重复键（True）或更新重复键（False）
    
    Returns:
        str: 修改后的SQL内容
    """
    # 查找所有INSERT语句
    insert_pattern = re.compile(r'(INSERT INTO\s+\w+\s*\([^)]+\)\s*VALUES\s*\([^)]+\))', re.IGNORECASE)
    
    if skip_duplicates:
        # 添加 ON CONFLICT DO NOTHING
        modified_sql = insert_pattern.sub(r'\1 ON CONFLICT DO NOTHING', sql_content)
    else:
        # 这里需要针对特定表结构定制更新逻辑
        # 这是一个简化示例，实际使用时需要根据表结构调整
        modified_sql = sql_content
        
        # 找出所有表名
        tables = set(re.findall(r'INSERT INTO\s+(\w+)', sql_content, re.IGNORECASE))
        
        for table in tables:
            # 对每个表生成适当的ON CONFLICT子句
            # 这里假设每个表都有id作为主键
            # 在实际应用中，你需要为每个表定制此逻辑
            table_pattern = re.compile(f'(INSERT INTO\\s+{table}\\s*\\(([^)]+)\\)\\s*VALUES\\s*\\([^)]+\\))', re.IGNORECASE)
            
            matches = table_pattern.findall(modified_sql)
            for match in matches:
                full_insert, columns = match
                
                # 解析列名
                column_list = [c.strip() for c in columns.split(',')]
                
                # 假设第一列是主键
                primary_key = column_list[0]
                
                # 生成更新部分
                update_parts = []
                for col in column_list[1:]:
                    update_parts.append(f"{col} = EXCLUDED.{col}")
                
                update_clause = ", ".join(update_parts)
                
                # 替换INSERT语句
                replacement = f"{full_insert} ON CONFLICT ({primary_key}) DO UPDATE SET {update_clause}"
                modified_sql = modified_sql.replace(full_insert, replacement)
    
    return modified_sql


def extract_error_details(error_msg, cursor):
    """
    从错误消息中提取详细信息，包括表名和列名
    
    Args:
        error_msg: PostgreSQL错误消息
        cursor: 数据库游标
    
    Returns:
        dict: 包含错误详情的字典
    """
    error_details = {
        'table_name': None,
        'column_name': None,
        'value': None,
        'error_type': None
    }
    
    # 常见错误模式匹配
    # 1. 值太长错误
    value_too_long = re.search(r'value too long for type ([a-zA-Z\s()0-9]+)', error_msg)
    if value_too_long:
        error_details['error_type'] = 'value_too_long'
        error_details['data_type'] = value_too_long.group(1)
        
        # 尝试获取列名 (PostgreSQL 10+特有的详细错误格式)
        column_match = re.search(r'column "([^"]+)"', error_msg)
        if column_match:
            error_details['column_name'] = column_match.group(1)
            
            # 尝试获取表名
            try:
                cursor.execute("""
                    SELECT table_name 
                    FROM information_schema.columns 
                    WHERE column_name = %s 
                    LIMIT 1
                """, (error_details['column_name'],))
                result = cursor.fetchone()
                if result:
                    error_details['table_name'] = result[0]
            except:
                pass
    
    # 2. 违反唯一约束
    unique_violation = re.search(r'duplicate key value violates unique constraint "([^"]+)"', error_msg)
    if unique_violation:
        error_details['error_type'] = 'unique_violation'
        constraint_name = unique_violation.group(1)
        error_details['constraint_name'] = constraint_name
        
        # 获取违反约束的值
        key_match = re.search(r'Key \(([^)]+)\)=\(([^)]+)\)', error_msg)
        if key_match:
            error_details['column_name'] = key_match.group(1)
            error_details['value'] = key_match.group(2)
            
            # 尝试获取表名
            try:
                cursor.execute("""
                    SELECT table_name 
                    FROM information_schema.table_constraints 
                    WHERE constraint_name = %s 
                    LIMIT 1
                """, (constraint_name,))
                result = cursor.fetchone()
                if result:
                    error_details['table_name'] = result[0]
            except:
                pass
    
    # 3. 外键约束违反
    foreign_key_violation = re.search(r'violates foreign key constraint "([^"]+)"', error_msg)
    if foreign_key_violation:
        error_details['error_type'] = 'foreign_key_violation'
        constraint_name = foreign_key_violation.group(1)
        error_details['constraint_name'] = constraint_name
        
        # 尝试获取详细信息
        fk_details = re.search(r'Key \(([^)]+)\)=\(([^)]+)\)', error_msg)
        if fk_details:
            error_details['column_name'] = fk_details.group(1)
            error_details['value'] = fk_details.group(2)
    
    # 4. 数据类型不匹配
    invalid_input = re.search(r'invalid input syntax for type ([a-z0-9]+): "([^"]+)"', error_msg)
    if invalid_input:
        error_details['error_type'] = 'invalid_input'
        error_details['data_type'] = invalid_input.group(1)
        error_details['value'] = invalid_input.group(2)
    
    return error_details


def find_problematic_insert(sql_content, error_details):
    """
    尝试在SQL内容中查找可能导致错误的INSERT语句
    
    Args:
        sql_content: SQL文件内容
        error_details: 错误详情字典
    
    Returns:
        str: 可能导致错误的INSERT语句，如果找不到则返回None
    """
    if not error_details.get('column_name') or not error_details.get('table_name'):
        return None
    
    # 查找匹配表名的INSERT语句
    table_name = error_details['table_name']
    column_name = error_details['column_name']
    
    # 构建正则表达式来查找包含特定表和列的INSERT语句
    pattern = re.compile(
        r'INSERT INTO\s+' + re.escape(table_name) + 
        r'\s*\(([^)]*' + re.escape(column_name) + r'[^)]*)\)\s*VALUES\s*\(([^)]+)\)',
        re.IGNORECASE
    )
    
    matches = pattern.findall(sql_content)
    if not matches:
        return None
    
    # 对于每个匹配，找出列的位置以及对应的值
    for columns_str, values_str in matches:
        columns = [c.strip() for c in columns_str.split(',')]
        
        # 找出特定列的索引
        try:
            col_index = columns.index(column_name)
        except ValueError:
            continue
        
        # 解析VALUES部分 (这是一个简化的实现，不处理复杂的SQL值表达式)
        values = []
        current_value = ""
        in_quotes = False
        escape_next = False
        
        for char in values_str:
            if escape_next:
                current_value += char
                escape_next = False
            elif char == '\\':
                current_value += char
                escape_next = True
            elif char == "'" and not in_quotes:
                in_quotes = True
                current_value += char
            elif char == "'" and in_quotes:
                in_quotes = False
                current_value += char
            elif char == ',' and not in_quotes:
                values.append(current_value.strip())
                current_value = ""
            else:
                current_value += char
        
        if current_value:  # 添加最后一个值
            values.append(current_value.strip())
        
        # 检查是否有足够的值
        if col_index < len(values):
            problematic_value = values[col_index]
            
            # 构建问题INSERT语句展示
            insert_stmt = f"INSERT INTO {table_name} ({columns_str}) VALUES ({values_str})"
            return {
                'statement': insert_stmt,
                'column_index': col_index,
                'problematic_value': problematic_value
            }
    
    return None


def split_sql_statements(sql_content):
    """
    将SQL内容分割成单独的语句
    
    Args:
        sql_content: 完整的SQL内容
        
    Returns:
        list: SQL语句列表
    """
    # 简单的SQL语句分割，查找分号，但忽略引号内的分号
    statements = []
    current_statement = []
    in_string = False
    escaped = False
    
    for char in sql_content:
        current_statement.append(char)
        
        if escaped:
            escaped = False
        elif char == '\\':
            escaped = True
        elif char == "'":
            in_string = not in_string
        elif char == ';' and not in_string:
            statements.append(''.join(current_statement))
            current_statement = []
    
    # 添加最后一个语句（如果没有以分号结尾）
    if current_statement:
        statements.append(''.join(current_statement))
        
    # 过滤空语句
    return [stmt.strip() for stmt in statements if stmt.strip()]


def get_table_schema(cursor, table_name):
    """
    获取表结构信息
    
    Args:
        cursor: 数据库游标
        table_name: 表名
        
    Returns:
        dict: 表结构信息
    """
    try:
        # 查询表的列信息
        cursor.execute("""
            SELECT column_name, data_type, character_maximum_length
            FROM information_schema.columns
            WHERE table_name = %s
        """, (table_name,))
        
        columns = {}
        for row in cursor.fetchall():
            column_name, data_type, max_length = row
            columns[column_name] = {
                'data_type': data_type,
                'max_length': max_length
            }
        
        return {
            'exists': len(columns) > 0,
            'columns': columns
        }
    except:
        return {'exists': False, 'columns': {}}


def diagnose_value_too_long_error(cursor, error_message, stmt):
    """
    诊断值超长错误，尝试确定具体是哪个列引起的问题
    
    Args:
        cursor: 数据库游标
        error_message: 错误消息
        stmt: 引起错误的SQL语句
        
    Returns:
        dict: 包含诊断结果
    """
    result = {
        'column_name': None,
        'table_name': None,
        'max_length': None,
        'data_type': None
    }
    
    # 1. 从错误消息中提取数据类型和长度限制
    type_match = re.search(r'value too long for type ([a-zA-Z\s()0-9]+)', error_message)
    if type_match:
        data_type = type_match.group(1)
        result['data_type'] = data_type
        
        # 从数据类型中提取长度限制
        type_length_match = re.search(r'varying\((\d+)\)', data_type)
        if type_length_match:
            max_length = int(type_length_match.group(1))
            result['max_length'] = max_length
    
    # 2. 尝试从SQL语句中提取表名
    if stmt.upper().startswith('INSERT'):
        # 对于INSERT语句，提取表名
        match = re.match(r'INSERT\s+INTO\s+(?:(?:"([^"]+)")|([^\s(]+))', stmt, re.IGNORECASE)
        if match:
            table_name = match.group(1) if match.group(1) else match.group(2)
            result['table_name'] = table_name
    
    # 如果找到了表名，获取其结构
    if result['table_name']:
        schema = get_table_schema(cursor, result['table_name'])
        
        # 3. 如果能找到表结构，看看哪些列是character varying(128)
        if schema['exists']:
            varchar128_columns = []
            for col_name, col_info in schema['columns'].items():
                if (col_info['data_type'] == 'character varying' and 
                    col_info['max_length'] == result['max_length']):
                    varchar128_columns.append(col_name)
            
            if len(varchar128_columns) == 1:
                # 如果只有一个这样的列，很可能就是它
                result['column_name'] = varchar128_columns[0]
            elif len(varchar128_columns) > 1:
                # 有多个这样的列，尝试通过解析INSERT语句确定
                if stmt.upper().startswith('INSERT'):
                    # 提取列名列表
                    cols_match = re.search(r'INSERT\s+INTO\s+(?:(?:"[^"]+")|[^\s(]+)\s*\(([^)]+)\)', stmt, re.IGNORECASE)
                    if cols_match:
                        columns_str = cols_match.group(1)
                        columns = [c.strip().strip('"') for c in columns_str.split(',')]
                        
                        # 提取VALUES部分
                        values_match = re.search(r'VALUES\s*\(([^)]+)\)', stmt, re.IGNORECASE)
                        if values_match:
                            values_str = values_match.group(1)
                            
                            # 使用更复杂的解析来处理嵌套括号和引号
                            values = []
                            current_value = ""
                            in_quotes = False
                            bracket_level = 0
                            
                            for char in values_str:
                                if char == "'" and (len(current_value) == 0 or current_value[-1] != '\\'):
                                    in_quotes = not in_quotes
                                    current_value += char
                                elif char == '(' and not in_quotes:
                                    bracket_level += 1
                                    current_value += char
                                elif char == ')' and not in_quotes:
                                    bracket_level -= 1
                                    current_value += char
                                elif char == ',' and not in_quotes and bracket_level == 0:
                                    values.append(current_value.strip())
                                    current_value = ""
                                else:
                                    current_value += char
                            
                            if current_value:
                                values.append(current_value.strip())
                            
                            # 检查每个character varying(128)列的值长度
                            for col in varchar128_columns:
                                if col in columns:
                                    idx = columns.index(col)
                                    if idx < len(values):
                                        val = values[idx]
                                        # 如果是字符串值
                                        if val.startswith("'") and val.endswith("'"):
                                            # 去掉引号，计算实际长度
                                            actual_val = val[1:-1].replace("''", "'")
                                            if len(actual_val) > result['max_length']:
                                                result['column_name'] = col
                                                result['actual_length'] = len(actual_val)
                                                result['value'] = (actual_val[:50] + '...' + actual_val[-50:]) if len(actual_val) > 100 else actual_val
                                                break
    
    # 4. 尝试通过直接执行查询确定有问题的列
    if result['table_name'] and not result['column_name']:
        try:
            # 创建一个临时表来测试每个列
            cursor.execute(f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = %s 
                  AND data_type = 'character varying' 
                  AND character_maximum_length = %s
            """, (result['table_name'], result['max_length']))
            
            potential_columns = [row[0] for row in cursor.fetchall()]
            
            if potential_columns:
                result['potential_columns'] = potential_columns
                # 当有多个可能的列时，列出所有可能性
                if len(potential_columns) > 1:
                    result['column_name'] = f"可能是以下列之一: {', '.join(potential_columns)}"
                else:
                    result['column_name'] = potential_columns[0]
        except:
            pass
    
    # 5. 如果上述方法都失败，尝试简单地解析错误消息
    if not result['column_name']:
        # PostgreSQL 9.6+有时会在错误消息中包含列名
        column_match = re.search(r'column "([^"]+)"', error_message)
        if column_match:
            result['column_name'] = column_match.group(1)
    
    return result


def execute_sql_file(sql_file, conn, cursor, skip_duplicates=True, batch_size=5000):
    """
    执行SQL文件
    
    Args:
        sql_file: SQL文件路径
        conn: 数据库连接
        cursor: 数据库游标
        skip_duplicates: 是否跳过重复键（True）或更新重复键（False）
        batch_size: 批处理大小，用于大型文件
    
    Returns:
        bool: 是否执行成功
    """
    try:
        # 检查文件是否存在
        if not os.path.exists(sql_file):
            print(f"错误: SQL文件不存在: {sql_file}")
            return False
            
        # 读取SQL文件内容
        with open(sql_file, 'r', encoding='utf-8') as f:
            sql_content = f.read()
            
        file_size_kb = os.path.getsize(sql_file) / 1024
        print(f"成功加载SQL文件: {sql_file} ({file_size_kb:.2f} KB)")
        
        # 处理可能的重复键问题
        sql_content = handle_duplicate_keys(sql_content, skip_duplicates)
        
        # 开始计时
        start_time = time.time()
        
        print("开始执行SQL语句...")
        print(f"重复键处理策略: {'跳过重复记录' if skip_duplicates else '更新重复记录'}")
        
        # 分析文件来获取表名和模式信息
        table_pattern = re.compile(r'INSERT INTO\s+(\w+)', re.IGNORECASE)
        table_matches = table_pattern.findall(sql_content)
        
        tables = {}
        if table_matches:
            for table_name in set(table_matches):
                # 获取表结构信息
                tables[table_name] = get_table_schema(cursor, table_name)
                if tables[table_name]['exists']:
                    print(f"发现表 '{table_name}' 及其结构")
        
        # 对于大型文件，将SQL拆分为语句执行
        if file_size_kb > 1000:  # 大于1MB的文件
            print(f"检测到大型SQL文件，将按语句分批执行...")
            statements = split_sql_statements(sql_content)
            total_statements = len(statements)
            print(f"总共有 {total_statements} 条SQL语句需要执行")
            
            # 提取所有INSERT语句，用于详细分析
            insert_statements = [stmt for stmt in statements if stmt.upper().startswith('INSERT')]
            print(f"其中包含 {len(insert_statements)} 条INSERT语句")
            
            success = True
            for i, stmt in enumerate(statements, 1):
                if i % 100 == 0 or i == 1 or i == total_statements:
                    print(f"正在执行第 {i}/{total_statements} 条语句...")
                
                try:
                    cursor.execute(stmt)
                    if i % batch_size == 0:
                        conn.commit()
                        print(f"已提交 {i} 条语句的事务")
                
                except psycopg2.Error as e:
                    # 发生错误，详细分析
                    print(f"\n在执行第 {i} 条语句时发生错误:")
                    
                    # 获取基本错误信息
                    print(f"数据库错误: {e}")
                    print(f"SQL状态: {e.pgcode}")
                    
                    error_message = str(e)
                    if hasattr(e, 'pgerror') and e.pgerror:
                        error_message = e.pgerror
                        print(f"详细信息: {error_message}")
                    
                    # 显示部分导致错误的SQL语句(截断显示)
                    if len(stmt) > 200:
                        print(f"问题SQL语句: {stmt[:100]}...{stmt[-100:]}")
                    else:
                        print(f"问题SQL语句: {stmt}")
                    
                    # 对于值太长错误，尝试找出具体是哪个列
                    if "value too long for type" in error_message:
                        print("\n详细错误分析:")
                        print(f"错误类型: 值超过了字段长度限制")
                        
                        # 使用增强的诊断函数
                        diagnosis = diagnose_value_too_long_error(cursor, error_message, stmt)
                        
                        if diagnosis['table_name']:
                            print(f"表名: {diagnosis['table_name']}")
                        
                        if diagnosis['column_name']:
                            print(f"列名: {diagnosis['column_name']}")
                        
                        if diagnosis.get('potential_columns'):
                            print(f"可能的问题列: {', '.join(diagnosis['potential_columns'])}")
                        
                        if diagnosis['data_type']:
                            print(f"字段类型: {diagnosis['data_type']}")
                        
                        if diagnosis['max_length']:
                            print(f"字段最大长度: {diagnosis['max_length']} 字符")
                        
                        if diagnosis.get('actual_length'):
                            print(f"实际值长度: {diagnosis['actual_length']} 字符")
                            print(f"超出长度: {diagnosis['actual_length'] - diagnosis['max_length']} 字符")
                        
                        if diagnosis.get('value'):
                            print(f"问题值: {diagnosis['value']}")
                            
                        # 进一步尝试通过ALTER TABLE测试
                        if diagnosis['table_name'] and not diagnosis['column_name'] and diagnosis['max_length']:
                            print("\n正在进行进一步诊断...")
                            try:
                                # 查询所有character varying(128)列
                                cursor.execute("""
                                    SELECT column_name
                                    FROM information_schema.columns
                                    WHERE table_name = %s
                                    AND data_type = 'character varying'
                                    AND character_maximum_length = %s
                                """, (diagnosis['table_name'], diagnosis['max_length']))
                                
                                varchar_columns = [row[0] for row in cursor.fetchall()]
                                
                                if varchar_columns:
                                    print(f"可能的问题列: {', '.join(varchar_columns)}")
                                
                                # 尝试解析INSERT语句的值部分
                                if stmt.upper().startswith('INSERT'):
                                    values_match = re.search(r'VALUES\s*\(([^)]+(?:\([^)]*\)[^)]*)*)\)', stmt, re.IGNORECASE)
                                    if values_match:
                                        values_part = values_match.group(1)
                                        # 显示VALUES部分以便手动分析
                                        if len(values_part) > 200:
                                            print(f"INSERT值部分: {values_part[:100]}...{values_part[-100:]}")
                                        else:
                                            print(f"INSERT值部分: {values_part}")
                                
                            except Exception as diag_error:
                                print(f"诊断过程出错: {diag_error}")
                            
                    elif "violates unique constraint" in error_message:
                        print("\n详细错误分析:")
                        print(f"错误类型: 违反唯一约束")
                        
                        constraint_match = re.search(r'constraint "([^"]+)"', error_message)
                        if constraint_match:
                            constraint_name = constraint_match.group(1)
                            print(f"约束名: {constraint_name}")
                        
                        key_match = re.search(r'Key \(([^)]+)\)=\(([^)]+)\)', error_message)
                        if key_match:
                            key_columns = key_match.group(1)
                            key_values = key_match.group(2)
                            print(f"冲突的列: {key_columns}")
                            print(f"冲突的值: {key_values}")
                    
                    # 回滚当前语句
                    conn.rollback()
                    success = False
                    break  # 遇到错误终止处理
            
            # 提交最后的事务（如果没有错误）
            if success:
                conn.commit()
        else:
            # 对于小型文件，直接执行整个内容
            try:
                cursor.execute(sql_content)
                conn.commit()
                print("SQL执行完成")
                success = True
            except psycopg2.Error as e:
                print(f"数据库错误: {e}")
                print(f"SQL状态: {e.pgcode}")
                
                if hasattr(e, 'pgerror') and e.pgerror:
                    print(f"详细信息: {e.pgerror}")
                
                conn.rollback()
                success = False
        
        # 计算执行时间
        elapsed_time = time.time() - start_time
        
        if success:
            print(f"\nSQL执行成功! 耗时: {elapsed_time:.2f} 秒")
        else:
            print(f"\nSQL执行失败! 耗时: {elapsed_time:.2f} 秒")
        
        return success
        
    except Exception as e:
        print(f"执行SQL文件时出错: {str(e)}")
        
        # 回滚事务
        conn.rollback()
        return False


def get_sql_files_from_directory(directory):
    """
    获取指定目录下的所有SQL文件并按文件名排序
    
    Args:
        directory: 目录路径
    
    Returns:
        list: 排序后的SQL文件路径列表
    """
    if not os.path.exists(directory):
        print(f"错误: 目录不存在: {directory}")
        return []
        
    if not os.path.isdir(directory):
        print(f"错误: 指定路径不是目录: {directory}")
        return []
    
    # 获取所有.sql文件
    sql_files = [
        os.path.join(directory, file)
        for file in os.listdir(directory)
        if file.lower().endswith('.sql')
    ]
    
    # 按文件名排序
    sql_files.sort()
    
    print(f"找到 {len(sql_files)} 个SQL文件")
    for i, file in enumerate(sql_files, 1):
        print(f"  {i}. {os.path.basename(file)}")
    
    return sql_files


def main():
    parser = argparse.ArgumentParser(description='执行SQL文件或指定目录下的所有SQL文件并导入数据到PostgreSQL数据库')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--sql_file', help='单个SQL文件路径')
    group.add_argument('--sql_dir', help='包含SQL文件的目录路径')
    
    parser.add_argument('--host', default='localhost', help='数据库主机名')
    parser.add_argument('--port', default='5432', help='数据库端口')
    parser.add_argument('--dbname', required=True, help='数据库名称')
    parser.add_argument('--user', required=True, help='数据库用户名')
    parser.add_argument('--password', help='数据库密码')
    parser.add_argument('--skip_duplicates', action='store_true', 
                        help='遇到重复主键时跳过记录 (默认行为)')
    parser.add_argument('--update_duplicates', action='store_true',
                        help='遇到重复主键时更新记录')
    parser.add_argument('--batch_size', type=int, default=5000,
                        help='批处理大小，用于大型SQL文件 (默认: 5000)')
    parser.add_argument('--continue_on_error', action='store_true',
                        help='遇到错误时继续执行（默认在遇到错误时停止）')
    
    args = parser.parse_args()
    
    # 构建连接参数
    conn_params = {
        'host': args.host,
        'port': args.port,
        'dbname': args.dbname,
        'user': args.user
    }
    
    # 如果提供了密码，添加到连接参数
    if args.password:
        conn_params['password'] = args.password
    
    # 确定重复键处理策略
    skip_duplicates = not args.update_duplicates
    
    try:
        # 连接数据库
        print(f"正在连接到数据库: {conn_params['dbname']}@{conn_params['host']}:{conn_params['port']}...")
        conn = psycopg2.connect(**conn_params)
        print("数据库连接成功")
        
        # 创建游标
        cursor = conn.cursor()
        
        # 为大型SQL文件设置较长的超时时间
        cursor.execute("SET statement_timeout = '3600000';")  # 1小时
        
        # 执行单个SQL文件或目录中的所有SQL文件
        success = True
        
        if args.sql_file:
            # 执行单个SQL文件
            print(f"执行单个SQL文件: {args.sql_file}")
            success = execute_sql_file(args.sql_file, conn, cursor, skip_duplicates, args.batch_size)
        else:
            # 获取目录中的所有SQL文件
            print(f"获取目录中的所有SQL文件: {args.sql_dir}")
            sql_files = get_sql_files_from_directory(args.sql_dir)
            
            if not sql_files:
                print(f"目录中没有找到SQL文件: {args.sql_dir}")
                sys.exit(1)
            
            # 按顺序执行所有SQL文件
            print(f"按文件名排序执行所有SQL文件...")
            
            for i, sql_file in enumerate(sql_files, 1):
                print(f"\n[{i}/{len(sql_files)}] 执行SQL文件: {os.path.basename(sql_file)}")
                file_success = execute_sql_file(sql_file, conn, cursor, skip_duplicates, args.batch_size)
                if not file_success:
                    print(f"SQL文件执行失败: {sql_file}")
                    success = False
                    # 是否继续执行
                    if not args.continue_on_error:
                        print("遇到错误停止执行。使用 --continue_on_error 参数可以在出错时继续执行其他文件。")
                        break
                    else:
                        print("继续执行下一个文件...")
        
        # 关闭数据库连接
        cursor.close()
        conn.close()
        print("数据库连接已关闭")
        
        if success:
            print("\n所有SQL文件执行成功!")
            sys.exit(0)
        else:
            print("\n一个或多个SQL文件执行失败!")
            sys.exit(1)
    
    except psycopg2.Error as e:
        print(f"数据库连接错误: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"执行过程中出现错误: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()