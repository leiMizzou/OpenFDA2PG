"""
FDA Medical Device Database Quality Analyzer

This tool performs comprehensive analysis of the FDA medical device database to:
1. Identify database structure optimization opportunities
2. Detect data quality issues
3. Analyze data distributions and patterns
4. Provide recommendations for data preprocessing and analysis
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats
import psycopg2
from psycopg2.extras import RealDictCursor
import time
from tqdm.notebook import tqdm
from IPython.display import display, HTML, Markdown
import warnings
warnings.filterwarnings('ignore')

class DataQualityAnalyzer:
    """分析FDA医疗设备数据库的数据质量和优化机会"""
    
    def __init__(self, db_config):
        """初始化数据质量分析器"""
        self.db_config = db_config
        self.conn = None
        self.cur = None
        self.schema = "device"  # 默认模式
        
        # 存储分析结果
        self.table_stats = {}
        self.index_analysis = {}
        self.null_analysis = {}
        self.duplicate_analysis = {}
        self.column_stats = {}
        self.relationship_analysis = {}
        self.query_recommendations = {}
        self.preprocessing_recommendations = {}
        
    def connect(self):
        """连接到PostgreSQL数据库"""
        dbname = self.db_config['dbname']
        
        try:
            self.conn = psycopg2.connect(**self.db_config)
            self.conn.autocommit = True  # 自动提交
            self.cur = self.conn.cursor(cursor_factory=RealDictCursor)  # 返回字典结果
            self.cur.execute(f"SET search_path TO {self.schema};")
            print(f"✅ 成功连接到PostgreSQL数据库 {dbname}")
            return True
        except Exception as e:
            print(f"❌ 数据库连接失败: {str(e)}")
            return False
    
    def close(self):
        """关闭数据库连接"""
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()
        print("📌 数据库连接已关闭")
    
    def run_full_analysis(self):
        """运行完整的数据质量和优化分析"""
        display(HTML("<h1>FDA医疗设备数据库质量分析报告</h1>"))
        display(HTML(f"<p>生成时间: {time.strftime('%Y-%m-%d %H:%M:%S')}</p>"))
        
        # 1. 数据库结构分析
        display(HTML("<h2>1. 数据库结构分析</h2>"))
        self.analyze_table_statistics()
        self.analyze_indexes()
        self.analyze_foreign_keys()
        
        # 2. 数据质量分析
        display(HTML("<h2>2. 数据质量分析</h2>"))
        self.analyze_null_values()
        self.analyze_duplicates()
        self.analyze_data_consistency()
        
        # 3. 数据值分析
        display(HTML("<h2>3. 数据值分析</h2>"))
        self.analyze_column_statistics()
        self.analyze_categorical_distributions()
        self.analyze_time_series_patterns()
        
        # 4. 数据关系分析
        display(HTML("<h2>4. 数据关系分析</h2>"))
        self.analyze_table_relationships()
        self.analyze_entity_connections()
        
        # 5. 优化和建议
        display(HTML("<h2>5. 优化和建议</h2>"))
        self.generate_optimization_recommendations()
        self.generate_query_recommendations()
        self.generate_preprocessing_recommendations()
        self.generate_analysis_recommendations()
        
        print("✅ 数据库质量分析完成")
    
    def analyze_table_statistics(self):
        """分析表统计信息"""
        display(HTML("<h3>表统计信息</h3>"))
        
        try:
            # 获取所有表名
            self.cur.execute(f"""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = '{self.schema}'
                AND table_type = 'BASE TABLE'
                ORDER BY table_name
            """)
            
            tables = [row['table_name'] for row in self.cur.fetchall()]
            
            # 初始化结果
            results = []
            
            for table in tqdm(tables, desc="分析表统计信息"):
                # 表行数
                self.cur.execute(f"SELECT COUNT(*) as row_count FROM {self.schema}.{table}")
                row_count = self.cur.fetchone()['row_count']
                
                # 表大小
                self.cur.execute(f"""
                    SELECT 
                        pg_size_pretty(pg_total_relation_size('{self.schema}.{table}')) as total_size,
                        pg_size_pretty(pg_relation_size('{self.schema}.{table}')) as table_size,
                        pg_size_pretty(pg_total_relation_size('{self.schema}.{table}') - 
                                        pg_relation_size('{self.schema}.{table}')) as index_size
                """)
                size_info = self.cur.fetchone()
                
                # 列数
                self.cur.execute(f"""
                    SELECT COUNT(*) as column_count
                    FROM information_schema.columns
                    WHERE table_schema = '{self.schema}'
                    AND table_name = '{table}'
                """)
                column_count = self.cur.fetchone()['column_count']
                
                # 主键
                self.cur.execute(f"""
                    SELECT 
                        c.column_name
                    FROM 
                        information_schema.table_constraints tc
                    JOIN 
                        information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name)
                    JOIN 
                        information_schema.columns AS c 
                        ON c.table_schema = tc.constraint_schema
                        AND c.table_name = tc.table_name
                        AND c.column_name = ccu.column_name
                    WHERE 
                        constraint_type = 'PRIMARY KEY' 
                        AND tc.table_schema = '{self.schema}'
                        AND tc.table_name = '{table}'
                """)
                pk_columns = [row['column_name'] for row in self.cur.fetchall()]
                pk_info = ", ".join(pk_columns) if pk_columns else "无主键"
                
                # 存储结果
                results.append({
                    '表名': table,
                    '行数': row_count,
                    '总大小': size_info['total_size'],
                    '表大小': size_info['table_size'],
                    '索引大小': size_info['index_size'],
                    '列数': column_count,
                    '主键': pk_info
                })
                
                # 存储详细信息供后续分析使用
                self.table_stats[table] = {
                    'row_count': row_count,
                    'total_size': size_info['total_size'],
                    'table_size': size_info['table_size'],
                    'index_size': size_info['index_size'],
                    'column_count': column_count,
                    'primary_key': pk_columns
                }
            
            # 显示结果
            result_df = pd.DataFrame(results)
            display(result_df.sort_values(by='行数', ascending=False))
            
            # 提供主要表的摘要
            main_tables = result_df[result_df['行数'] > 1000].sort_values(by='行数', ascending=False)
            
            display(HTML("<h4>主要表摘要</h4>"))
            display(Markdown(f"""
            数据库中共有 **{len(tables)}** 个表，总行数超过 **{result_df['行数'].sum():,}**。

            最大的表是:
            - **{main_tables.iloc[0]['表名']}**: {main_tables.iloc[0]['行数']:,} 行 ({main_tables.iloc[0]['总大小']})
            - **{main_tables.iloc[1]['表名']}**: {main_tables.iloc[1]['行数']:,} 行 ({main_tables.iloc[1]['总大小']})
            - **{main_tables.iloc[2]['表名']}**: {main_tables.iloc[2]['行数']:,} 行 ({main_tables.iloc[2]['总大小']})

            * 主键缺失的表: **{sum(1 for row in results if row['主键'] == '无主键')}** 个
            * 最大表索引占比: **{main_tables.iloc[0]['索引大小']}** / **{main_tables.iloc[0]['总大小']}**
            """))
            
            # 可视化前10大表
            top10_tables = result_df.nlargest(10, '行数')
            plt.figure(figsize=(12, 6))
            sns.barplot(x='表名', y='行数', data=top10_tables)
            plt.xticks(rotation=45, ha='right')
            plt.title('数据库中前10大表（按行数）')
            plt.tight_layout()
            plt.show()
            
            # 存储异常发现
            findings = []
            
            # 检查无主键的表
            no_pk_tables = [row['表名'] for row in results if row['主键'] == '无主键']
            if no_pk_tables:
                findings.append(f"发现 {len(no_pk_tables)} 个表没有主键: {', '.join(no_pk_tables)}")
            
            # 检查超大表
            very_large_tables = [row['表名'] for row in results if row['行数'] > 10000000]
            if very_large_tables:
                findings.append(f"发现 {len(very_large_tables)} 个非常大的表 (>1000万行): {', '.join(very_large_tables)}")
            
            # 检查极小表（可能是没用的表）
            very_small_tables = [row['表名'] for row in results if row['行数'] < 10 and row['表名'] not in ['dataset_metadata']]
            if very_small_tables:
                findings.append(f"发现 {len(very_small_tables)} 个极小的表 (<10行): {', '.join(very_small_tables)}")
            
            if findings:
                display(HTML("<h4>结构问题发现</h4>"))
                for finding in findings:
                    display(Markdown(f"- {finding}"))
            
        except Exception as e:
            print(f"❌ 分析表统计信息时出错: {str(e)}")
    
    def analyze_indexes(self):
        """分析索引情况"""
        display(HTML("<h3>索引分析</h3>"))
        
        try:
            # 获取所有索引信息
            self.cur.execute(f"""
                SELECT
                    t.relname AS table_name,
                    i.relname AS index_name,
                    a.attname AS column_name,
                    ix.indisunique AS is_unique,
                    ix.indisprimary AS is_primary,
                    pg_size_pretty(pg_relation_size(i.oid)) AS index_size
                FROM
                    pg_class t,
                    pg_class i,
                    pg_index ix,
                    pg_attribute a
                WHERE
                    t.oid = ix.indrelid
                    AND i.oid = ix.indexrelid
                    AND a.attrelid = t.oid
                    AND a.attnum = ANY(ix.indkey)
                    AND t.relkind = 'r'
                    AND t.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = '{self.schema}')
                ORDER BY
                    t.relname,
                    i.relname
            """)
            
            index_data = self.cur.fetchall()
            
            # 将索引数据转换为每个表及其索引的结构
            table_indexes = {}
            for row in index_data:
                table_name = row['table_name']
                index_name = row['index_name']
                
                if table_name not in table_indexes:
                    table_indexes[table_name] = {}
                
                if index_name not in table_indexes[table_name]:
                    table_indexes[table_name][index_name] = {
                        'columns': [],
                        'is_unique': row['is_unique'],
                        'is_primary': row['is_primary'],
                        'size': row['index_size']
                    }
                
                table_indexes[table_name][index_name]['columns'].append(row['column_name'])
            
            # 分析每个表的索引情况
            index_analysis = []
            
            for table_name, indexes in table_indexes.items():
                # 跳过小表
                if table_name in self.table_stats and self.table_stats[table_name]['row_count'] < 100:
                    continue
                
                # 合并列名
                for index_name, index_info in indexes.items():
                    index_info['columns'] = ", ".join(index_info['columns'])
                
                # 计算索引和表的比率
                row_count = self.table_stats.get(table_name, {}).get('row_count', 0)
                total_indexes = len(indexes)
                
                has_primary = any(index_info['is_primary'] for index_info in indexes.values())
                unique_indexes = sum(1 for index_info in indexes.values() if index_info['is_unique'])
                
                # 添加到分析结果
                index_analysis.append({
                    '表名': table_name,
                    '行数': row_count,
                    '索引数': total_indexes,
                    '有主键索引': '是' if has_primary else '否',
                    '唯一索引数': unique_indexes,
                    '索引列表': ", ".join(indexes.keys())
                })
                
                # 存储索引分析结果
                self.index_analysis[table_name] = {
                    'indexes': list(indexes.keys()),
                    'total_indexes': total_indexes,
                    'has_primary': has_primary,
                    'unique_indexes': unique_indexes,
                    'index_details': indexes
                }
            
            # 显示结果
            index_df = pd.DataFrame(index_analysis)
            display(index_df.sort_values(by=['行数', '索引数'], ascending=[False, False]))
            
            # 寻找索引优化机会
            display(HTML("<h4>索引优化机会</h4>"))
            
            # 大表但索引少
            large_tables_few_indexes = index_df[(index_df['行数'] > 100000) & (index_df['索引数'] < 3)]
            if not large_tables_few_indexes.empty:
                display(Markdown(f"**大表索引不足**: 以下表包含超过10万行数据但索引少于3个:"))
                display(large_tables_few_indexes[['表名', '行数', '索引数', '有主键索引']])
            
            # 没有主键的表
            tables_without_pk = index_df[index_df['有主键索引'] == '否']
            if not tables_without_pk.empty:
                display(Markdown(f"**缺少主键索引**: 以下表没有主键索引:"))
                display(tables_without_pk[['表名', '行数']])
            
            # 可能存在索引冗余的表
            tables_many_indexes = index_df[(index_df['索引数'] > 5)]
            if not tables_many_indexes.empty:
                display(Markdown(f"**可能存在索引冗余**: 以下表索引数量较多，可能存在冗余:"))
                display(tables_many_indexes[['表名', '行数', '索引数', '索引列表']])
        
        except Exception as e:
            print(f"❌ 分析索引时出错: {str(e)}")
    
    def analyze_foreign_keys(self):
        """分析外键关系"""
        display(HTML("<h3>外键关系分析</h3>"))
        
        try:
            # 获取所有外键关系
            self.cur.execute(f"""
                SELECT
                    tc.table_name AS table_name,
                    kcu.column_name AS column_name,
                    ccu.table_name AS foreign_table_name,
                    ccu.column_name AS foreign_column_name
                FROM
                    information_schema.table_constraints AS tc
                JOIN
                    information_schema.key_column_usage AS kcu
                    ON tc.constraint_name = kcu.constraint_name
                JOIN
                    information_schema.constraint_column_usage AS ccu
                    ON ccu.constraint_name = tc.constraint_name
                WHERE
                    tc.constraint_type = 'FOREIGN KEY'
                    AND tc.table_schema = '{self.schema}'
                ORDER BY
                    tc.table_name,
                    kcu.column_name
            """)
            
            fk_data = self.cur.fetchall()
            
            if not fk_data:
                display(Markdown("数据库中没有定义外键关系。这可能导致数据完整性问题，建议考虑添加适当的外键约束。"))
                return
            
            # 构建外键关系列表
            fk_relations = []
            for row in fk_data:
                fk_relations.append({
                    '表名': row['table_name'],
                    '列名': row['column_name'],
                    '引用表': row['foreign_table_name'],
                    '引用列': row['foreign_column_name']
                })
            
            fk_df = pd.DataFrame(fk_relations)
            display(fk_df)
            
            # 计算每个表的外键数
            table_fk_counts = fk_df['表名'].value_counts().to_dict()
            
            # 检查外键索引情况
            fk_index_status = []
            
            for row in fk_relations:
                table_name = row['表名']
                column_name = row['列名']
                
                # 检查外键列是否有索引
                self.cur.execute(f"""
                    SELECT
                        i.relname AS index_name
                    FROM
                        pg_class t,
                        pg_class i,
                        pg_index ix,
                        pg_attribute a
                    WHERE
                        t.oid = ix.indrelid
                        AND i.oid = ix.indexrelid
                        AND a.attrelid = t.oid
                        AND a.attnum = ANY(ix.indkey)
                        AND t.relkind = 'r'
                        AND t.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = '{self.schema}')
                        AND t.relname = '{table_name}'
                        AND a.attname = '{column_name}'
                """)
                
                has_index = len(self.cur.fetchall()) > 0
                
                fk_index_status.append({
                    '表名': table_name,
                    '外键列': column_name,
                    '引用表': row['引用表'],
                    '引用列': row['引用列'],
                    '有索引': '是' if has_index else '否'
                })
            
            # 显示外键索引状态
            display(HTML("<h4>外键索引状态</h4>"))
            fk_index_df = pd.DataFrame(fk_index_status)
            display(fk_index_df)
            
            # 找出没有索引的外键
            fk_without_index = fk_index_df[fk_index_df['有索引'] == '否']
            
            if not fk_without_index.empty:
                display(HTML("<h4>优化建议：外键索引</h4>"))
                display(Markdown("以下外键没有索引，为它们添加索引可以改善连接查询性能:"))
                display(fk_without_index)
                
                # 生成创建索引的SQL
                display(Markdown("**建议的索引创建语句:**"))
                
                for _, row in fk_without_index.iterrows():
                    table = row['表名']
                    column = row['外键列']
                    index_name = f"idx_{table}_{column}"
                    
                    sql = f"CREATE INDEX {index_name} ON {self.schema}.{table} ({column});"
                    display(Markdown(f"```sql\n{sql}\n```"))
            
            # 检查外键数据完整性
            display(HTML("<h4>外键数据完整性检查</h4>"))
            
            integrity_issues = []
            for _, row in fk_df.iterrows():
                table = row['表名']
                column = row['列名']
                ref_table = row['引用表']
                ref_column = row['引用列']
                
                # 检查是否有外键值在引用表中不存在
                self.cur.execute(f"""
                    SELECT COUNT(*) as invalid_count
                    FROM {self.schema}.{table} t
                    LEFT JOIN {self.schema}.{ref_table} r ON t.{column} = r.{ref_column}
                    WHERE t.{column} IS NOT NULL AND r.{ref_column} IS NULL
                """)
                
                result = self.cur.fetchone()
                invalid_count = result['invalid_count'] if result else 0
                
                if invalid_count > 0:
                    integrity_issues.append({
                        '表名': table,
                        '外键列': column,
                        '引用表': ref_table,
                        '引用列': ref_column,
                        '无效引用数': invalid_count
                    })
            
            if integrity_issues:
                display(Markdown("**检测到外键数据完整性问题:**"))
                display(pd.DataFrame(integrity_issues))
                
                # 提供修复建议
                display(Markdown("**建议:**"))
                display(Markdown("- 检查数据导入过程，确保外键完整性\n- 考虑添加外键约束以防止将来出现无效引用\n- 可以使用以下查询识别具体的无效引用记录:"))
                
                for issue in integrity_issues:
                    table = issue['表名']
                    column = issue['外键列']
                    ref_table = issue['引用表']
                    ref_column = issue['引用列']
                    
                    sql = f"""
                    SELECT t.*
                    FROM {self.schema}.{table} t
                    LEFT JOIN {self.schema}.{ref_table} r ON t.{column} = r.{ref_column}
                    WHERE t.{column} IS NOT NULL AND r.{ref_column} IS NULL
                    LIMIT 10;
                    """
                    
                    display(Markdown(f"```sql\n{sql}\n```"))
            else:
                display(Markdown("✅ 未发现外键数据完整性问题。"))
        
        except Exception as e:
            print(f"❌ 分析外键关系时出错: {str(e)}")
    
    def analyze_null_values(self):
        """分析空值情况"""
        display(HTML("<h3>空值分析</h3>"))
        
        try:
            # 获取主要表（跳过空表和非常小的表）
            main_tables = [table for table, stats in self.table_stats.items() 
                          if stats['row_count'] > 100]
            
            null_analysis_results = []
            
            for table in tqdm(main_tables, desc="分析空值"):
                # 获取表的列
                self.cur.execute(f"""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema = '{self.schema}'
                    AND table_name = '{table}'
                    ORDER BY ordinal_position
                """)
                
                columns = self.cur.fetchall()
                
                for column in columns:
                    column_name = column['column_name']
                    data_type = column['data_type']
                    
                    # 计算空值百分比
                    self.cur.execute(f"""
                        SELECT 
                            COUNT(*) as total_count,
                            SUM(CASE WHEN {column_name} IS NULL THEN 1 ELSE 0 END) as null_count
                        FROM {self.schema}.{table}
                    """)
                    
                    result = self.cur.fetchone()
                    total_count = result['total_count']
                    null_count = result['null_count']
                    
                    if total_count > 0:
                        null_percentage = (null_count / total_count) * 100
                    else:
                        null_percentage = 0
                    
                    null_analysis_results.append({
                        '表名': table,
                        '列名': column_name,
                        '数据类型': data_type,
                        '总记录数': total_count,
                        '空值数': null_count,
                        '空值百分比': round(null_percentage, 2)
                    })
                    
                    # 存储空值分析结果
                    if table not in self.null_analysis:
                        self.null_analysis[table] = {}
                    
                    self.null_analysis[table][column_name] = {
                        'data_type': data_type,
                        'null_count': null_count,
                        'null_percentage': null_percentage
                    }
            
            null_df = pd.DataFrame(null_analysis_results)
            
            # 空值百分比较高的列
            high_null_df = null_df[null_df['空值百分比'] > 50].sort_values(by='空值百分比', ascending=False)
            
            if not high_null_df.empty:
                display(HTML("<h4>空值比例高的列</h4>"))
                display(high_null_df.head(20))
                
                # 按表分组显示空值比例高的列
                display(HTML("<h4>每个表空值比例高的列</h4>"))
                
                for table in main_tables:
                    table_high_nulls = null_df[(null_df['表名'] == table) & (null_df['空值百分比'] > 30)]
                    
                    if not table_high_nulls.empty:
                        display(Markdown(f"**表 {table}:**"))
                        display(table_high_nulls[['列名', '数据类型', '空值百分比']].sort_values(by='空值百分比', ascending=False))
                
                # 可视化排名前10的高空值列
                plt.figure(figsize=(12, 6))
                top_nulls = high_null_df.head(15)
                sns.barplot(x='空值百分比', y='列名', hue='表名', data=top_nulls)
                plt.title('空值比例最高的15个列')
                plt.tight_layout()
                plt.show()
            
            # 提供分析和建议
            display(HTML("<h4>空值分析与建议</h4>"))
            
            # 找出具有较多空值的主要表
            high_null_tables = {}
            for table in main_tables:
                table_nulls = null_df[null_df['表名'] == table]
                avg_null_pct = table_nulls['空值百分比'].mean()
                high_null_tables[table] = avg_null_pct
            
            # 按空值百分比排序
            high_null_tables = {k: v for k, v in sorted(high_null_tables.items(), key=lambda item: item[1], reverse=True)}
            
            top_high_null_tables = list(high_null_tables.items())[:5]
            
            if top_high_null_tables:
                display(Markdown("**空值比例较高的表:**"))
                for table, null_pct in top_high_null_tables:
                    display(Markdown(f"- **{table}**: 平均空值比例 {null_pct:.2f}%"))
            
            # 提供处理空值的建议
            display(Markdown("""
            **处理空值的建议:**
            
            1. **对于空值比例极高的列 (>90%):**
               - 考虑是否真正需要这些字段
               - 检查数据收集流程，确认为什么这些字段大多为空
               
            2. **对于空值比例中等的列 (30-90%):**
               - 为分析目的填充空值 (均值、中位数、众数等)
               - 考虑添加"缺失"标志列以保留空值信息
               - 分析空值的分布模式，确定是否与特定条件相关
               
            3. **对于关键字段中的空值:**
               - 检查数据完整性和质量控制流程
               - 考虑使用业务规则或外部数据源填充
               - 确定这些空值是否表示真实的"未知"或是数据问题
            """))
        
        except Exception as e:
            print(f"❌ 分析空值时出错: {str(e)}")
    
    def analyze_duplicates(self):
        """分析重复记录"""
        display(HTML("<h3>重复记录分析</h3>"))
        
        try:
            # 选择主要表进行分析
            main_tables = {
                'adverse_events': 'report_number',
                'device_recalls': 'recall_number',
                'enforcement_actions': 'recall_number',
                'udi_records': 'public_device_record_key',
                'event_devices': ['event_id', 'device_sequence_number'],
                'product_codes': 'product_code'
            }
            
            duplicate_results = []
            
            for table, key_columns in main_tables.items():
                # 确保表存在
                if table not in self.table_stats:
                    continue
                
                # 如果只有一个键列
                if isinstance(key_columns, str):
                    self.cur.execute(f"""
                        SELECT 
                            '{table}' as table_name, 
                            '{key_columns}' as key_column,
                            COUNT(*) as total_records,
                            COUNT(DISTINCT {key_columns}) as unique_keys,
                            COUNT(*) - COUNT(DISTINCT {key_columns}) as duplicate_count,
                            CASE 
                                WHEN COUNT(*) > 0 THEN 
                                    ROUND(((COUNT(*) - COUNT(DISTINCT {key_columns}))::float / COUNT(*) * 100), 2)
                                ELSE 0 
                            END as duplicate_percentage
                        FROM {self.schema}.{table}
                        WHERE {key_columns} IS NOT NULL
                    """)
                else:
                    # 多个键列的情况
                    key_columns_str = ', '.join(key_columns)
                    self.cur.execute(f"""
                        WITH KeyCounts AS (
                            SELECT 
                                {key_columns_str},
                                COUNT(*) as key_count
                            FROM {self.schema}.{table}
                            GROUP BY {key_columns_str}
                            HAVING COUNT(*) > 1
                        )
                        SELECT 
                            '{table}' as table_name, 
                            '{key_columns_str}' as key_column,
                            (SELECT COUNT(*) FROM {self.schema}.{table}) as total_records,
                            (SELECT COUNT(*) FROM (SELECT DISTINCT {key_columns_str} FROM {self.schema}.{table}) t) as unique_keys,
                            (SELECT SUM(key_count) - COUNT(*) FROM KeyCounts) as duplicate_count,
                            CASE 
                                WHEN (SELECT COUNT(*) FROM {self.schema}.{table}) > 0 THEN 
                                    ROUND(((SELECT COALESCE(SUM(key_count) - COUNT(*), 0) FROM KeyCounts)::float / 
                                           (SELECT COUNT(*) FROM {self.schema}.{table}) * 100), 2)
                                ELSE 0 
                            END as duplicate_percentage
                    """)
                
                result = self.cur.fetchone()
                
                if result and result['total_records'] > 0:
                    duplicate_results.append({
                        '表名': result['table_name'],
                        '键列': result['key_column'],
                        '总记录数': result['total_records'],
                        '唯一键数': result['unique_keys'],
                        '重复记录数': result['duplicate_count'] or 0,
                        '重复百分比': result['duplicate_percentage'] or 0
                    })
                    
                    # 如果存在重复，获取一些示例
                    if result['duplicate_count'] and result['duplicate_count'] > 0:
                        if isinstance(key_columns, str):
                            self.cur.execute(f"""
                                WITH DuplicateKeys AS (
                                    SELECT {key_columns}
                                    FROM {self.schema}.{table}
                                    GROUP BY {key_columns}
                                    HAVING COUNT(*) > 1
                                    LIMIT 5
                                )
                                SELECT {key_columns}, COUNT(*) as count
                                FROM {self.schema}.{table}
                                WHERE {key_columns} IN (SELECT {key_columns} FROM DuplicateKeys)
                                GROUP BY {key_columns}
                                ORDER BY count DESC
                            """)
                        else:
                            key_columns_str = ', '.join(key_columns)
                            self.cur.execute(f"""
                                WITH DuplicateKeys AS (
                                    SELECT {key_columns_str}
                                    FROM {self.schema}.{table}
                                    GROUP BY {key_columns_str}
                                    HAVING COUNT(*) > 1
                                    LIMIT 5
                                )
                                SELECT {key_columns_str}, COUNT(*) as count
                                FROM {self.schema}.{table}
                                WHERE ({key_columns_str}) IN (SELECT {key_columns_str} FROM DuplicateKeys)
                                GROUP BY {key_columns_str}
                                ORDER BY count DESC
                            """)
                        
                        duplicate_examples = self.cur.fetchall()
                        
                        # 存储重复示例
                        if table not in self.duplicate_analysis:
                            self.duplicate_analysis[table] = {
                                'key_columns': key_columns,
                                'duplicate_count': result['duplicate_count'],
                                'duplicate_percentage': result['duplicate_percentage'],
                                'examples': duplicate_examples
                            }
            
            # 显示结果
            duplicate_df = pd.DataFrame(duplicate_results)
            display(duplicate_df.sort_values(by='重复百分比', ascending=False))
            
            # 如果存在重复记录，显示示例
            tables_with_duplicates = [row['表名'] for _, row in duplicate_df.iterrows() 
                                     if row['重复记录数'] > 0]
            
            if tables_with_duplicates:
                display(HTML("<h4>重复记录示例</h4>"))
                
                for table in tables_with_duplicates:
                    if table in self.duplicate_analysis:
                        examples = self.duplicate_analysis[table]['examples']
                        duplicate_count = self.duplicate_analysis[table]['duplicate_count']
                        
                        display(Markdown(f"**表 {table} (共 {duplicate_count} 条重复记录):**"))
                        display(pd.DataFrame(examples))
                
                # 提供处理重复记录的建议
                display(HTML("<h4>处理重复记录的建议</h4>"))
                display(Markdown("""
                **重复记录处理建议:**
                
                1. **基于主键/唯一键的去重:**
                   ```sql
                   -- 示例：使用ROW_NUMBER()保留最新记录
                   WITH RankedRows AS (
                       SELECT *, 
                           ROW_NUMBER() OVER(PARTITION BY key_column ORDER BY updated_at DESC) as rn
                       FROM schema.table
                   )
                   DELETE FROM schema.table
                   WHERE id IN (
                       SELECT id FROM RankedRows WHERE rn > 1
                   );
                   ```
                
                2. **创建带唯一约束的临时表:**
                   ```sql
                   -- 步骤1：创建临时表并带有唯一约束
                   CREATE TABLE temp_table (LIKE original_table);
                   ALTER TABLE temp_table ADD CONSTRAINT unique_key UNIQUE (key_column);
                   
                   -- 步骤2：插入不重复的数据
                   INSERT INTO temp_table
                   SELECT DISTINCT ON (key_column) *
                   FROM original_table
                   ORDER BY key_column, updated_at DESC;
                   
                   -- 步骤3：替换原表
                   DROP TABLE original_table;
                   ALTER TABLE temp_table RENAME TO original_table;
                   ```
                
                3. **添加或强化唯一约束:**
                   在处理完当前重复后，考虑添加唯一约束以防止将来出现重复。
                """))
            else:
                display(Markdown("✅ 未在主要表中发现重复记录。"))
        
        except Exception as e:
            print(f"❌ 分析重复记录时出错: {str(e)}")
    
    def analyze_data_consistency(self):
        """分析数据一致性"""
        display(HTML("<h3>数据一致性分析</h3>"))
        
        try:
            # 1. 检查日期字段的有效性
            display(HTML("<h4>日期字段有效性</h4>"))
            
            date_fields = {}
            
            # 获取所有日期类型的字段
            self.cur.execute(f"""
                SELECT 
                    table_name, 
                    column_name
                FROM 
                    information_schema.columns
                WHERE 
                    table_schema = '{self.schema}'
                    AND (data_type LIKE '%date%' OR data_type LIKE '%time%')
                ORDER BY
                    table_name, 
                    column_name
            """)
            
            date_columns = self.cur.fetchall()
            
            date_validation_results = []
            
            for col in date_columns:
                table = col['table_name']
                column = col['column_name']
                
                # 跳过很小的表
                if table in self.table_stats and self.table_stats[table]['row_count'] < 100:
                    continue
                
                # 检查日期的范围
                self.cur.execute(f"""
                    SELECT 
                        MIN({column}) as min_date,
                        MAX({column}) as max_date,
                        COUNT(*) as total_count,
                        SUM(CASE WHEN {column} IS NULL THEN 1 ELSE 0 END) as null_count,
                        SUM(CASE WHEN {column} > CURRENT_DATE THEN 1 ELSE 0 END) as future_date_count
                    FROM {self.schema}.{table}
                """)
                
                result = self.cur.fetchone()
                
                if result and result['total_count'] > 0:
                    # 计算百分比
                    null_percentage = (result['null_count'] / result['total_count']) * 100 if result['total_count'] > 0 else 0
                    future_percentage = (result['future_date_count'] / result['total_count']) * 100 if result['total_count'] > 0 else 0
                    
                    date_validation_results.append({
                        '表名': table,
                        '列名': column,
                        '最小日期': result['min_date'],
                        '最大日期': result['max_date'],
                        '空值百分比': round(null_percentage, 2),
                        '未来日期数': result['future_date_count'],
                        '未来日期百分比': round(future_percentage, 2)
                    })
                    
                    # 存储日期字段信息
                    if table not in date_fields:
                        date_fields[table] = []
                    
                    date_fields[table].append({
                        'column': column,
                        'min_date': result['min_date'],
                        'max_date': result['max_date'],
                        'null_percentage': null_percentage,
                        'future_date_count': result['future_date_count']
                    })
            
            # 显示日期验证结果
            if date_validation_results:
                date_df = pd.DataFrame(date_validation_results)
                
                # 查找问题日期字段
                problem_dates_df = date_df[(date_df['未来日期数'] > 0) | 
                                           (date_df['最小日期'] < '1900-01-01') |
                                           (date_df['空值百分比'] > 80)]
                
                if not problem_dates_df.empty:
                    display(Markdown("**检测到以下日期字段可能存在问题:**"))
                    display(problem_dates_df.sort_values(by=['未来日期数', '空值百分比'], ascending=False))
                    
                    # 提供建议
                    display(Markdown("""
                    **日期字段修复建议:**
                    
                    1. **未来日期处理:**
                       - 验证是否为真实的未来日期（预期日期）或是数据错误
                       - 考虑将明显错误的未来日期设置为NULL或合理的默认值
                       
                    2. **异常早期日期:**
                       - 检查1900年之前的日期是否合理
                       - 可能需要设置为NULL或更合理的日期
                       
                    3. **日期验证SQL示例:**
                       ```sql
                       -- 查找未来日期
                       SELECT * FROM schema.table WHERE date_column > CURRENT_DATE;
                       
                       -- 修复明显错误的日期
                       UPDATE schema.table 
                       SET date_column = NULL
                       WHERE date_column > CURRENT_DATE + INTERVAL '1 year';
                       ```
                    """))
                else:
                    display(Markdown("✅ 日期字段验证通过，未发现明显异常。"))
            
            # 2. 检查数值字段的有效性
            display(HTML("<h4>数值字段有效性</h4>"))
            
            # 获取数值类型字段
            self.cur.execute(f"""
                SELECT 
                    table_name, 
                    column_name,
                    data_type
                FROM 
                    information_schema.columns
                WHERE 
                    table_schema = '{self.schema}'
                    AND (
                        data_type LIKE '%int%' OR 
                        data_type LIKE '%float%' OR
                        data_type LIKE '%double%' OR
                        data_type LIKE '%numeric%' OR
                        data_type LIKE '%decimal%'
                    )
                ORDER BY
                    table_name, 
                    column_name
            """)
            
            numeric_columns = self.cur.fetchall()
            
            numeric_validation_results = []
            
            for col in numeric_columns:
                table = col['table_name']
                column = col['column_name']
                data_type = col['data_type']
                
                # 跳过ID列和很小的表
                if column in ('id', 'event_id', 'report_id', 'device_id') or \
                   (table in self.table_stats and self.table_stats[table]['row_count'] < 100):
                    continue
                
                # 检查数值范围和分布
                self.cur.execute(f"""
                    SELECT 
                        MIN({column}) as min_value,
                        MAX({column}) as max_value,
                        AVG({column}) as avg_value,
                        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {column}) as median_value,
                        COUNT(*) as total_count,
                        SUM(CASE WHEN {column} IS NULL THEN 1 ELSE 0 END) as null_count,
                        SUM(CASE WHEN {column} < 0 THEN 1 ELSE 0 END) as negative_count,
                        SUM(CASE WHEN {column} = 0 THEN 1 ELSE 0 END) as zero_count
                    FROM {self.schema}.{table}
                """)
                
                result = self.cur.fetchone()
                
                if result and result['total_count'] > 0 and result['total_count'] - result['null_count'] > 0:
                    # 计算统计值
                    null_percentage = (result['null_count'] / result['total_count']) * 100 if result['total_count'] > 0 else 0
                    negative_percentage = (result['negative_count'] / (result['total_count'] - result['null_count'])) * 100 \
                                          if (result['total_count'] - result['null_count']) > 0 else 0
                    zero_percentage = (result['zero_count'] / (result['total_count'] - result['null_count'])) * 100 \
                                      if (result['total_count'] - result['null_count']) > 0 else 0
                    
                    # 计算最大值与平均值的比率，检测异常值
                    if result['avg_value'] and result['avg_value'] != 0:
                        max_avg_ratio = result['max_value'] / result['avg_value'] if result['max_value'] else 0
                    else:
                        max_avg_ratio = 0
                    
                    numeric_validation_results.append({
                        '表名': table,
                        '列名': column,
                        '数据类型': data_type,
                        '最小值': result['min_value'],
                        '最大值': result['max_value'],
                        '平均值': round(result['avg_value'], 2) if result['avg_value'] else None,
                        '中位数': round(result['median_value'], 2) if result['median_value'] else None,
                        '空值百分比': round(null_percentage, 2),
                        '负值百分比': round(negative_percentage, 2),
                        '零值百分比': round(zero_percentage, 2),
                        '最大/平均比': round(max_avg_ratio, 2) if max_avg_ratio else None
                    })
            
            # 显示数值验证结果
            if numeric_validation_results:
                numeric_df = pd.DataFrame(numeric_validation_results)
                
                # 查找可能有问题的数值字段
                problem_numeric_df = numeric_df[
                    (numeric_df['最大/平均比'] > 100) |  # 可能存在异常值
                    ((numeric_df['负值百分比'] > 0) & (numeric_df['列名'].str.contains('count|amount|quantity|number', case=False))) |  # 不应该为负的字段有负值
                    (numeric_df['空值百分比'] > 80)  # 大多数为空
                ]
                
                if not problem_numeric_df.empty:
                    display(Markdown("**检测到以下数值字段可能存在问题:**"))
                    display(problem_numeric_df.sort_values(by=['最大/平均比', '负值百分比'], ascending=False))
                    
                    # 提供建议
                    display(Markdown("""
                    **数值字段修复建议:**
                    
                    1. **异常值处理:**
                       - 对于最大值远大于平均值的字段，应检查是否存在极端值
                       - 考虑使用统计方法（如Z-score或IQR方法）识别和处理异常值
                       
                    2. **负值处理:**
                       - 对于名称暗示应为正值的字段（如计数、数量、金额），检查负值
                       - 根据业务规则确定是否需要将负值转换为0或NULL
                       
                    3. **数值检验SQL示例:**
                       ```sql
                       -- 查找异常值（使用IQR方法）
                       WITH Stats AS (
                           SELECT 
                               percentile_cont(0.25) WITHIN GROUP (ORDER BY numeric_column) AS q1,
                               percentile_cont(0.75) WITHIN GROUP (ORDER BY numeric_column) AS q3
                           FROM schema.table
                       )
                       SELECT *
                       FROM schema.table, Stats
                       WHERE numeric_column > (q3 + (q3 - q1) * 1.5)
                          OR numeric_column < (q1 - (q3 - q1) * 1.5);
                       ```
                    """))
                else:
                    display(Markdown("✅ 数值字段验证通过，未发现明显异常。"))
            
            # 3. 检查枚举/代码值的一致性
            display(HTML("<h4>枚举/代码值一致性</h4>"))
            
            # 可能的枚举字段列表（基于列名模式）
            enum_patterns = [
                '%_type', '%_code', '%_status', '%_flag', 'classification', 
                'device_class', 'category', '%_level'
            ]
            
            potential_enum_fields = []
            
            for pattern in enum_patterns:
                self.cur.execute(f"""
                    SELECT 
                        table_name, 
                        column_name,
                        data_type
                    FROM 
                        information_schema.columns
                    WHERE 
                        table_schema = '{self.schema}'
                        AND column_name LIKE '{pattern}'
                        AND data_type NOT IN ('boolean', 'uuid', 'bytea')
                    ORDER BY
                        table_name, 
                        column_name
                """)
                
                potential_enum_fields.extend(self.cur.fetchall())
            
            enum_validation_results = []
            
            for col in potential_enum_fields:
                table = col['table_name']
                column = col['column_name']
                data_type = col['data_type']
                
                # 跳过小表
                if table in self.table_stats and self.table_stats[table]['row_count'] < 100:
                    continue
                
                # 获取不同值及其计数
                self.cur.execute(f"""
                    SELECT 
                        {column} as value,
                        COUNT(*) as count
                    FROM 
                        {self.schema}.{table}
                    WHERE 
                        {column} IS NOT NULL
                    GROUP BY 
                        {column}
                    ORDER BY 
                        COUNT(*) DESC
                """)
                
                value_counts = self.cur.fetchall()
                
                if value_counts:
                    # 计算不同值的数量
                    distinct_values = len(value_counts)
                    
                    # 计算最常见值占比
                    top_value = value_counts[0]['value'] if value_counts else None
                    top_count = value_counts[0]['count'] if value_counts else 0
                    
                    self.cur.execute(f"""
                        SELECT COUNT(*) as total
                        FROM {self.schema}.{table}
                        WHERE {column} IS NOT NULL
                    """)
                    
                    total_result = self.cur.fetchone()
                    total_count = total_result['total'] if total_result else 0
                    
                    top_percentage = (top_count / total_count * 100) if total_count > 0 else 0
                    
                    enum_validation_results.append({
                        '表名': table,
                        '列名': column,
                        '数据类型': data_type,
                        '不同值数量': distinct_values,
                        '最常见值': str(top_value)[:50] if top_value is not None else None,  # 截断长值
                        '最常见值占比': round(top_percentage, 2),
                        '值分布': [
                            f"{row['value']}: {row['count']}" 
                            for row in value_counts[:5]  # 只显示前5个最常见的值
                        ]
                    })
            
            # 显示枚举验证结果
            if enum_validation_results:
                enum_df = pd.DataFrame(enum_validation_results)
                
                # 识别可能的问题字段
                nonstandard_enums = enum_df[enum_df['不同值数量'] > 20].sort_values(by='不同值数量', ascending=False)
                
                if not nonstandard_enums.empty:
                    display(Markdown("**以下代码/枚举字段可能存在标准化问题:**"))
                    display(nonstandard_enums[['表名', '列名', '不同值数量', '最常见值', '最常见值占比']])
                    
                    # 显示一些特定字段的详细值分布
                    if len(nonstandard_enums) > 0:
                        for i, row in nonstandard_enums.head(3).iterrows():
                            table = row['表名']
                            column = row['列名']
                            
                            display(Markdown(f"**{table}.{column} 值分布:**"))
                            
                            self.cur.execute(f"""
                                SELECT 
                                    {column} as value,
                                    COUNT(*) as count,
                                    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM {self.schema}.{table} WHERE {column} IS NOT NULL), 2) as percentage
                                FROM 
                                    {self.schema}.{table}
                                WHERE 
                                    {column} IS NOT NULL
                                GROUP BY 
                                    {column}
                                ORDER BY 
                                    COUNT(*) DESC
                                LIMIT 15
                            """)
                            
                            detailed_counts = self.cur.fetchall()
                            display(pd.DataFrame(detailed_counts))
                    
                    # 提供标准化建议
                    display(Markdown("""
                    **代码/枚举字段标准化建议:**
                    
                    1. **数据清洗和标准化:**
                       - 处理大小写不一致（可以全部转为大写或小写）
                       - 删除多余的空格、标点符号等
                       - 合并相似或等价的值（例如"Y"/"YES"/"1"统一为"Y"）
                       
                    2. **创建标准代码表:**
                       - 为重要的枚举字段创建查找表
                       - 使用外键强制引用有效值
                       
                    3. **标准化SQL示例:**
                       ```sql
                       -- 大小写标准化示例
                       UPDATE schema.table
                       SET code_column = UPPER(TRIM(code_column))
                       WHERE code_column IS NOT NULL;
                       
                       -- 创建标准代码表示例
                       CREATE TABLE schema.standard_codes (
                           code VARCHAR(50) PRIMARY KEY,
                           description TEXT,
                           is_active BOOLEAN DEFAULT TRUE
                       );
                       ```
                    """))
                else:
                    display(Markdown("✅ 代码/枚举字段验证通过，未发现明显标准化问题。"))
        
        except Exception as e:
            print(f"❌ 分析数据一致性时出错: {str(e)}")
    
    def analyze_column_statistics(self):
        """分析关键列的统计信息"""
        display(HTML("<h3>关键列统计分析</h3>"))
        
        try:
            # 选择要分析的关键表和列
            key_columns = {
                'adverse_events': ['date_received', 'date_of_event', 'event_type', 'health_professional'],
                'device_recalls': ['event_date_initiated', 'classification', 'product_code'],
                'product_codes': ['device_class', 'implant_flag', 'life_sustain_support_flag'],
                'udi_records': ['publish_date', 'is_single_use', 'is_rx', 'is_kit']
            }
            
            all_stats = []
            
            for table, columns in key_columns.items():
                # 检查表是否存在
                if table not in self.table_stats:
                    continue
                
                for column in columns:
                    # 查找列的数据类型
                    self.cur.execute(f"""
                        SELECT data_type
                        FROM information_schema.columns
                        WHERE table_schema = '{self.schema}'
                        AND table_name = '{table}'
                        AND column_name = '{column}'
                    """)
                    
                    column_type_result = self.cur.fetchone()
                    
                    if not column_type_result:
                        continue
                        
                    data_type = column_type_result['data_type']
                    
                    # 根据数据类型选择合适的统计
                    if data_type in ('integer', 'numeric', 'decimal', 'double precision', 'real'):
                        # 数值型统计
                        self.cur.execute(f"""
                            SELECT 
                                MIN({column}) as min_value,
                                MAX({column}) as max_value,
                                AVG({column}) as avg_value,
                                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {column}) as median,
                                STDDEV({column}) as std_dev,
                                COUNT(*) as total_count,
                                COUNT({column}) as non_null_count
                            FROM {self.schema}.{table}
                        """)
                        
                        result = self.cur.fetchone()
                        
                        if result and result['total_count'] > 0:
                            stats = {
                                '表名': table,
                                '列名': column,
                                '数据类型': '数值型',
                                '总记录数': result['total_count'],
                                '非空记录数': result['non_null_count'],
                                '最小值': result['min_value'],
                                '最大值': result['max_value'],
                                '平均值': round(result['avg_value'], 2) if result['avg_value'] else None,
                                '中位数': round(result['median'], 2) if result['median'] else None,
                                '标准差': round(result['std_dev'], 2) if result['std_dev'] else None
                            }
                            all_stats.append(stats)
                            
                    elif data_type in ('date', 'timestamp', 'timestamp without time zone', 'timestamp with time zone'):
                        # 日期型统计
                        self.cur.execute(f"""
                            SELECT 
                                MIN({column}) as min_date,
                                MAX({column}) as max_date,
                                COUNT(*) as total_count,
                                COUNT({column}) as non_null_count
                            FROM {self.schema}.{table}
                        """)
                        
                        result = self.cur.fetchone()
                        
                        if result and result['total_count'] > 0:
                            # 计算年度分布
                            self.cur.execute(f"""
                                SELECT 
                                    EXTRACT(YEAR FROM {column}) as year,
                                    COUNT(*) as count
                                FROM {self.schema}.{table}
                                WHERE {column} IS NOT NULL
                                GROUP BY EXTRACT(YEAR FROM {column})
                                ORDER BY year DESC
                                LIMIT 5
                            """)
                            
                            year_distribution = self.cur.fetchall()
                            
                            stats = {
                                '表名': table,
                                '列名': column,
                                '数据类型': '日期型',
                                '总记录数': result['total_count'],
                                '非空记录数': result['non_null_count'],
                                '最早日期': result['min_date'],
                                '最晚日期': result['max_date'],
                                '年度分布': [f"{row['year']}: {row['count']}" for row in year_distribution]
                            }
                            all_stats.append(stats)
                            
                    elif data_type in ('boolean'):
                        # 布尔型统计
                        self.cur.execute(f"""
                            SELECT 
                                COUNT(*) as total_count,
                                COUNT({column}) as non_null_count,
                                SUM(CASE WHEN {column} = TRUE THEN 1 ELSE 0 END) as true_count,
                                SUM(CASE WHEN {column} = FALSE THEN 1 ELSE 0 END) as false_count
                            FROM {self.schema}.{table}
                        """)
                        
                        result = self.cur.fetchone()
                        
                        if result and result['total_count'] > 0:
                            true_percentage = (result['true_count'] / result['non_null_count'] * 100) if result['non_null_count'] > 0 else 0
                            stats = {
                                '表名': table,
                                '列名': column,
                                '数据类型': '布尔型',
                                '总记录数': result['total_count'],
                                '非空记录数': result['non_null_count'],
                                'TRUE值数': result['true_count'],
                                'FALSE值数': result['false_count'],
                                'TRUE占比': f"{round(true_percentage, 2)}%"
                            }
                            all_stats.append(stats)
                            
                    else:
                        # 字符串/分类型统计
                        self.cur.execute(f"""
                            SELECT 
                                COUNT(*) as total_count,
                                COUNT({column}) as non_null_count,
                                COUNT(DISTINCT {column}) as distinct_count
                            FROM {self.schema}.{table}
                        """)
                        
                        result = self.cur.fetchone()
                        
                        if result and result['total_count'] > 0:
                            # 获取前5个最常见值
                            self.cur.execute(f"""
                                SELECT 
                                    {column} as value,
                                    COUNT(*) as count,
                                    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM {self.schema}.{table} WHERE {column} IS NOT NULL), 2) as percentage
                                FROM {self.schema}.{table}
                                WHERE {column} IS NOT NULL
                                GROUP BY {column}
                                ORDER BY COUNT(*) DESC
                                LIMIT 5
                            """)
                            
                            top_values = self.cur.fetchall()
                            
                            stats = {
                                '表名': table,
                                '列名': column,
                                '数据类型': '字符串/分类型',
                                '总记录数': result['total_count'],
                                '非空记录数': result['non_null_count'],
                                '不同值数量': result['distinct_count'],
                                '最常见值': [f"{row['value']}: {row['count']} ({row['percentage']}%)" for row in top_values]
                            }
                            all_stats.append(stats)
            
            # 显示统计结果
            if all_stats:
                display(Markdown("**关键列统计分析:**"))
                
                # 分组显示不同类型的统计
                data_types = set(stat['数据类型'] for stat in all_stats)
                
                for data_type in data_types:
                    display(Markdown(f"**{data_type}字段:**"))
                    
                    type_stats = [stat for stat in all_stats if stat['数据类型'] == data_type]
                    if data_type == '数值型':
                        df = pd.DataFrame(type_stats)[['表名', '列名', '非空记录数', '最小值', '最大值', '平均值', '中位数', '标准差']]
                    elif data_type == '日期型':
                        df = pd.DataFrame(type_stats)[['表名', '列名', '非空记录数', '最早日期', '最晚日期']]
                        # 显示年度分布
                        for i, row in enumerate(type_stats):
                            if '年度分布' in row and row['年度分布']:
                                display(Markdown(f"_{row['表名']}.{row['列名']} 年度分布:_ {', '.join(row['年度分布'])}"))
                    elif data_type == '布尔型':
                        df = pd.DataFrame(type_stats)[['表名', '列名', '非空记录数', 'TRUE值数', 'FALSE值数', 'TRUE占比']]
                    else:  # 字符串/分类型
                        df = pd.DataFrame(type_stats)[['表名', '列名', '非空记录数', '不同值数量']]
                        # 显示最常见值
                        for i, row in enumerate(type_stats):
                            if '最常见值' in row and row['最常见值']:
                                display(Markdown(f"_{row['表名']}.{row['列名']} 最常见值:_ {'; '.join(row['最常见值'])}"))
                    
                    display(df)
                
                # 为一些有趣的统计制作可视化
                boolean_stats = [stat for stat in all_stats if stat['数据类型'] == '布尔型' and stat['非空记录数'] > 100]
                
                if boolean_stats:
                    plt.figure(figsize=(12, 6))
                    
                    # 创建布尔字段分布条形图
                    labels = [f"{stat['表名']}.{stat['列名']}" for stat in boolean_stats]
                    true_vals = [stat['TRUE值数'] for stat in boolean_stats]
                    false_vals = [stat['FALSE值数'] for stat in boolean_stats]
                    
                    x = range(len(labels))
                    width = 0.35
                    
                    fig, ax = plt.subplots(figsize=(14, 7))
                    rects1 = ax.bar([i - width/2 for i in x], true_vals, width, label='True')
                    rects2 = ax.bar([i + width/2 for i in x], false_vals, width, label='False')
                    
                    ax.set_title('布尔字段值分布')
                    ax.set_xticks(x)
                    ax.set_xticklabels(labels, rotation=45, ha='right')
                    ax.legend()
                    
                    plt.tight_layout()
                    plt.show()
            
            # 存储这些统计用于后续分析
            self.column_stats = {f"{stat['表名']}.{stat['列名']}": stat for stat in all_stats}
        
        except Exception as e:
            print(f"❌ 分析列统计信息时出错: {str(e)}")
    
    def analyze_categorical_distributions(self):
        """分析分类值的分布"""
        display(HTML("<h3>分类值分布分析</h3>"))
        
        try:
            # 选择要分析的分类列
            categorical_columns = {
                'device_recalls': 'classification',
                'adverse_events': 'event_type',
                'product_codes': 'device_class',
                'udi_records': 'mri_safety'
            }
            
            for table, column in categorical_columns.items():
                # 检查表和列是否存在
                self.cur.execute(f"""
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_schema = '{self.schema}'
                    AND table_name = '{table}'
                    AND column_name = '{column}'
                """)
                
                if not self.cur.fetchone():
                    continue
                
                display(Markdown(f"**{table}.{column} 值分布:**"))
                
                # 获取值分布
                self.cur.execute(f"""
                    SELECT 
                        {column} as value,
                        COUNT(*) as count,
                        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
                    FROM {self.schema}.{table}
                    WHERE {column} IS NOT NULL
                    GROUP BY {column}
                    ORDER BY COUNT(*) DESC
                """)
                
                distribution = self.cur.fetchall()
                
                if distribution:
                    df = pd.DataFrame(distribution)
                    display(df)
                    
                    # 创建饼图或条形图
                    plt.figure(figsize=(10, 6))
                    
                    # 如果值太多，只显示前10个
                    if len(distribution) > 10:
                        top_values = df.head(10)
                        others_sum = df.iloc[10:]['count'].sum()
                        others_pct = df.iloc[10:]['percentage'].sum()
                        
                        # 添加"其他"类别
                        if others_sum > 0:
                            top_values = pd.concat([
                                top_values, 
                                pd.DataFrame([{'value': '其他', 'count': others_sum, 'percentage': others_pct}])
                            ])
                        
                        plt.bar(top_values['value'].astype(str), top_values['count'])
                        plt.title(f'{table}.{column} 值分布 (前10名)')
                        plt.xticks(rotation=45, ha='right')
                    else:
                        # 如果不同值少于5个，使用饼图
                        if len(distribution) <= 5:
                            plt.pie(df['count'], labels=df['value'].astype(str), autopct='%1.1f%%')
                            plt.title(f'{table}.{column} 值分布')
                        else:
                            plt.bar(df['value'].astype(str), df['count'])
                            plt.title(f'{table}.{column} 值分布')
                            plt.xticks(rotation=45, ha='right')
                    
                    plt.tight_layout()
                    plt.show()
        
        except Exception as e:
            print(f"❌ 分析分类值分布时出错: {str(e)}")
    
    def analyze_time_series_patterns(self):
        """分析时间序列模式"""
        display(HTML("<h3>时间序列模式分析</h3>"))
        
        try:
            # 定义要分析的时间序列
            time_series = {
                'adverse_events': {'date_column': 'date_received', 'count_column': 'report_number', 'group_by': 'event_type'},
                'device_recalls': {'date_column': 'event_date_initiated', 'count_column': 'recall_number', 'group_by': 'classification'}
            }
            
            for table, config in time_series.items():
                date_column = config['date_column']
                count_column = config['count_column']
                group_by = config.get('group_by')
                
                # 检查表和列是否存在
                self.cur.execute(f"""
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_schema = '{self.schema}'
                    AND table_name = '{table}'
                    AND column_name = '{date_column}'
                """)
                
                if not self.cur.fetchone():
                    continue
                
                # 按月汇总数据
                if group_by:
                    # 分组时间序列
                    self.cur.execute(f"""
                        SELECT 
                            TO_CHAR(DATE_TRUNC('month', {date_column}), 'YYYY-MM') as month,
                            {group_by} as category,
                            COUNT({count_column}) as count
                        FROM {self.schema}.{table}
                        WHERE {date_column} IS NOT NULL
                        AND {date_column} >= '2000-01-01'
                        AND {date_column} <= CURRENT_DATE
                        AND {group_by} IS NOT NULL
                        GROUP BY month, {group_by}
                        ORDER BY month, {group_by}
                    """)
                    
                    results = self.cur.fetchall()
                    
                    if results:
                        df = pd.DataFrame(results)
                        
                        # 找出主要类别（排除罕见类别以避免图表过于复杂）
                        main_categories = df['category'].value_counts().head(5).index.tolist()
                        df_filtered = df[df['category'].isin(main_categories)]
                        
                        # 透视表以便绘图
                        pivot_df = df_filtered.pivot(index='month', columns='category', values='count')
                        
                        # 绘制随时间变化的类别趋势
                        plt.figure(figsize=(14, 7))
                        pivot_df.plot(figsize=(14, 7), title=f'{table} 按 {group_by} 分类的月度趋势')
                        plt.xticks(rotation=45)
                        plt.grid(True, alpha=0.3)
                        plt.tight_layout()
                        plt.show()
                        
                        # 计算同比增长率
                        display(Markdown(f"**{table} 按 {group_by} 分类的年度总数:**"))
                        
                        self.cur.execute(f"""
                            SELECT 
                                EXTRACT(YEAR FROM {date_column}) as year,
                                {group_by} as category,
                                COUNT({count_column}) as count
                            FROM {self.schema}.{table}
                            WHERE {date_column} IS NOT NULL
                            AND {date_column} >= '2000-01-01'
                            AND {date_column} <= CURRENT_DATE
                            AND {group_by} IS NOT NULL
                            GROUP BY year, {group_by}
                            ORDER BY year, {group_by}
                        """)
                        
                        yearly_results = self.cur.fetchall()
                        
                        if yearly_results:
                            yearly_df = pd.DataFrame(yearly_results)
                            
                            # 只保留主要类别
                            yearly_df = yearly_df[yearly_df['category'].isin(main_categories)]
                            
                            yearly_pivot = yearly_df.pivot(index='year', columns='category', values='count')
                            display(yearly_pivot)
                            
                            # 计算同比变化
                            display(Markdown(f"**{table} 按 {group_by} 分类的同比增长率 (%):**"))
                            display(yearly_pivot.pct_change() * 100)
                else:
                    # 简单时间序列
                    self.cur.execute(f"""
                        SELECT 
                            TO_CHAR(DATE_TRUNC('month', {date_column}), 'YYYY-MM') as month,
                            COUNT({count_column}) as count
                        FROM {self.schema}.{table}
                        WHERE {date_column} IS NOT NULL
                        AND {date_column} >= '2000-01-01'
                        AND {date_column} <= CURRENT_DATE
                        GROUP BY month
                        ORDER BY month
                    """)
                    
                    results = self.cur.fetchall()
                    
                    if results:
                        df = pd.DataFrame(results)
                        
                        # 绘制简单时间序列
                        plt.figure(figsize=(14, 7))
                        plt.plot(df['month'], df['count'])
                        plt.title(f'{table} 按月记录数')
                        plt.xticks(rotation=45)
                        plt.grid(True, alpha=0.3)
                        plt.tight_layout()
                        plt.show()
                        
                        # 计算年度汇总
                        display(Markdown(f"**{table} 年度记录数:**"))
                        
                        self.cur.execute(f"""
                            SELECT 
                                EXTRACT(YEAR FROM {date_column}) as year,
                                COUNT({count_column}) as count
                            FROM {self.schema}.{table}
                            WHERE {date_column} IS NOT NULL
                            AND {date_column} >= '2000-01-01'
                            AND {date_column} <= CURRENT_DATE
                            GROUP BY year
                            ORDER BY year
                        """)
                        
                        yearly_results = self.cur.fetchall()
                        
                        if yearly_results:
                            yearly_df = pd.DataFrame(yearly_results)
                            display(yearly_df)
                            
                            # 计算同比增长率
                            yearly_df['增长率%'] = yearly_df['count'].pct_change() * 100
                            display(yearly_df)
        
        except Exception as e:
            print(f"❌ 分析时间序列模式时出错: {str(e)}")
    
    def analyze_table_relationships(self):
        """分析表间关系"""
        display(HTML("<h3>表间关系分析</h3>"))
        
        try:
            # 查找外键关系
            self.cur.execute(f"""
                SELECT
                    tc.table_name AS table_name,
                    kcu.column_name AS column_name,
                    ccu.table_name AS foreign_table_name,
                    ccu.column_name AS foreign_column_name
                FROM
                    information_schema.table_constraints AS tc
                JOIN
                    information_schema.key_column_usage AS kcu
                    ON tc.constraint_name = kcu.constraint_name
                JOIN
                    information_schema.constraint_column_usage AS ccu
                    ON ccu.constraint_name = tc.constraint_name
                WHERE
                    tc.constraint_type = 'FOREIGN KEY'
                    AND tc.table_schema = '{self.schema}'
                ORDER BY
                    tc.table_name,
                    kcu.column_name
            """)
            
            fk_relations = self.cur.fetchall()
            
            table_relations = {}
            foreign_key_links = []
            
            if fk_relations:
                for relation in fk_relations:
                    source_table = relation['table_name']
                    source_column = relation['column_name']
                    target_table = relation['foreign_table_name']
                    target_column = relation['foreign_column_name']
                    
                    if source_table not in table_relations:
                        table_relations[source_table] = {'references': [], 'referenced_by': []}
                    
                    if target_table not in table_relations:
                        table_relations[target_table] = {'references': [], 'referenced_by': []}
                    
                    # 添加引用关系
                    table_relations[source_table]['references'].append({
                        'table': target_table,
                        'source_column': source_column,
                        'target_column': target_column
                    })
                    
                    # 添加被引用关系
                    table_relations[target_table]['referenced_by'].append({
                        'table': source_table,
                        'source_column': source_column,
                        'target_column': target_column
                    })
                    
                    # 存储链接
                    foreign_key_links.append({
                        'source_table': source_table,
                        'source_column': source_column,
                        'target_table': target_table,
                        'target_column': target_column
                    })
            
            # 查找潜在的隐式关系（相同列名但没有外键约束）
            implicit_relationships = []
            
            # 获取所有表
            self.cur.execute(f"""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = '{self.schema}'
                AND table_type = 'BASE TABLE'
            """)
            
            all_tables = [row['table_name'] for row in self.cur.fetchall()]
            
            # 对于每个表，查找具有相同名称的列
            for i, table1 in enumerate(all_tables):
                for table2 in all_tables[i+1:]:
                    # 查找两个表中具有相同名称的列
                    self.cur.execute(f"""
                        SELECT 
                            t1.column_name
                        FROM 
                            information_schema.columns t1
                        JOIN 
                            information_schema.columns t2
                            ON t1.column_name = t2.column_name
                        WHERE 
                            t1.table_schema = '{self.schema}'
                            AND t2.table_schema = '{self.schema}'
                            AND t1.table_name = '{table1}'
                            AND t2.table_name = '{table2}'
                            AND t1.column_name != 'id'  -- 排除常见的标识符列
                            AND t1.column_name NOT LIKE '%_id'  -- 排除已经在外键关系中的列
                            AND t1.column_name NOT LIKE 'created_%'  -- 排除常见的元数据列
                            AND t1.column_name NOT LIKE 'updated_%'
                    """)
                    
                    common_columns = [row['column_name'] for row in self.cur.fetchall()]
                    
                    # 检查是否已经存在外键关系
                    existing_fk = next((link for link in foreign_key_links if 
                                     (link['source_table'] == table1 and link['target_table'] == table2) or
                                     (link['source_table'] == table2 and link['target_table'] == table1)), None)
                    
                    # 如果没有外键关系但有共同列，检查值是否匹配
                    if not existing_fk and common_columns:
                        for column in common_columns:
                            # 检查两个表中列是否包含匹配值
                            self.cur.execute(f"""
                                SELECT COUNT(*) as match_count
                                FROM (
                                    SELECT DISTINCT {column} as val
                                    FROM {self.schema}.{table1}
                                    WHERE {column} IS NOT NULL
                                    INTERSECT
                                    SELECT DISTINCT {column} as val
                                    FROM {self.schema}.{table2}
                                    WHERE {column} IS NOT NULL
                                ) t
                            """)
                            
                            match_result = self.cur.fetchone()
                            match_count = match_result['match_count'] if match_result else 0
                            
                            if match_count > 0:
                                # 查看两个表中列的重叠百分比
                                self.cur.execute(f"""
                                    WITH table1_values AS (
                                        SELECT DISTINCT {column} as val
                                        FROM {self.schema}.{table1}
                                        WHERE {column} IS NOT NULL
                                    ),
                                    table2_values AS (
                                        SELECT DISTINCT {column} as val
                                        FROM {self.schema}.{table2}
                                        WHERE {column} IS NOT NULL
                                    ),
                                    intersection AS (
                                        SELECT val FROM table1_values
                                        INTERSECT
                                        SELECT val FROM table2_values
                                    )
                                    SELECT
                                        (SELECT COUNT(*) FROM table1_values) as table1_distinct,
                                        (SELECT COUNT(*) FROM table2_values) as table2_distinct,
                                        (SELECT COUNT(*) FROM intersection) as intersection_count
                                """)
                                
                                overlap_result = self.cur.fetchone()
                                
                                if overlap_result:
                                    table1_distinct = overlap_result['table1_distinct']
                                    table2_distinct = overlap_result['table2_distinct']
                                    intersection_count = overlap_result['intersection_count']
                                    
                                    # 计算重叠百分比
                                    if table1_distinct > 0 and table2_distinct > 0:
                                        overlap_pct1 = (intersection_count / table1_distinct) * 100
                                        overlap_pct2 = (intersection_count / table2_distinct) * 100
                                        
                                        # 如果重叠足够大，认为存在隐式关系
                                        if overlap_pct1 > 5 or overlap_pct2 > 5:
                                            implicit_relationships.append({
                                                'table1': table1,
                                                'table2': table2,
                                                'column': column,
                                                'table1_distinct_values': table1_distinct,
                                                'table2_distinct_values': table2_distinct,
                                                'matching_values': intersection_count,
                                                'table1_overlap_pct': round(overlap_pct1, 2),
                                                'table2_overlap_pct': round(overlap_pct2, 2)
                                            })
            
            # 显示显式关系
            if foreign_key_links:
                display(Markdown("**显式外键关系:**"))
                fk_df = pd.DataFrame(foreign_key_links)
                display(fk_df)
                
                # 构建指向或引用最多的表排名
                references_count = {}
                referenced_by_count = {}
                
                for table, relations in table_relations.items():
                    references_count[table] = len(relations['references'])
                    referenced_by_count[table] = len(relations['referenced_by'])
                
                # 最常被引用的表
                top_referenced = sorted(referenced_by_count.items(), key=lambda x: x[1], reverse=True)[:5]
                # 引用最多其他表的表
                top_referencing = sorted(references_count.items(), key=lambda x: x[1], reverse=True)[:5]
                
                display(Markdown("**最常被引用的表:**"))
                display(pd.DataFrame(top_referenced, columns=['表名', '被引用次数']))
                
                display(Markdown("**引用最多其他表的表:**"))
                display(pd.DataFrame(top_referencing, columns=['表名', '引用其他表次数']))
            
            # 显示隐式关系
            if implicit_relationships:
                display(Markdown("**潜在的隐式关系:**"))
                implicit_df = pd.DataFrame(implicit_relationships)
                display(implicit_df)
                
                # 提供创建外键的建议
                display(Markdown("**潜在外键关系建议:**"))
                
                for relation in implicit_relationships:
                    table1 = relation['table1']
                    table2 = relation['table2']
                    column = relation['column']
                    pct1 = relation['table1_overlap_pct']
                    pct2 = relation['table2_overlap_pct']
                    
                    if pct1 > 50 and pct2 > 50:
                        display(Markdown(f"- **强关系**: `{table1}.{column}` 和 `{table2}.{column}` 有显著重叠 ({pct1}% / {pct2}%)，应考虑建立外键约束"))
                    else:
                        display(Markdown(f"- **弱关系**: `{table1}.{column}` 和 `{table2}.{column}` 有部分重叠 ({pct1}% / {pct2}%)，可能存在关系"))
            
            # 存储关系分析结果
            self.relationship_analysis = {
                'table_relations': table_relations,
                'foreign_key_links': foreign_key_links,
                'implicit_relationships': implicit_relationships
            }
        
        except Exception as e:
            print(f"❌ 分析表间关系时出错: {str(e)}")
    
    def analyze_entity_connections(self):
        """分析实体间的关联"""
        display(HTML("<h3>实体关联分析</h3>"))
        
        try:
            # 定义FDA医疗设备数据库中的主要实体及其关键表
            entities = {
                '设备': ['product_codes', 'device_classifications', 'udi_records'],
                '召回': ['device_recalls'],
                '不良事件': ['adverse_events', 'event_devices', 'event_patients'],
                '执法行动': ['enforcement_actions'],
                '公司': ['companies']
            }
            
            # 探索关键表之间的直接关联
            display(Markdown("**主要实体关联:**"))
            
            # 产品代码与不良事件的关联
            self.cur.execute("""
                SELECT 
                    pc.product_code,
                    pc.device_name,
                    COUNT(DISTINCT ed.event_id) as adverse_event_count
                FROM device.product_codes pc
                JOIN device.event_devices ed ON pc.id = ed.product_code_id
                GROUP BY pc.product_code, pc.device_name
                ORDER BY adverse_event_count DESC
                LIMIT 10
            """)
            
            product_events = self.cur.fetchall()
            
            if product_events:
                display(Markdown("**产品与不良事件的主要关联 (前10名):**"))
                display(pd.DataFrame(product_events))
            
            # 产品代码与召回的关联
            self.cur.execute("""
                SELECT 
                    pc.product_code,
                    pc.device_name,
                    COUNT(DISTINCT dr.id) as recall_count
                FROM device.product_codes pc
                JOIN device.device_recalls dr ON pc.id = dr.product_code_id
                GROUP BY pc.product_code, pc.device_name
                ORDER BY recall_count DESC
                LIMIT 10
            """)
            
            product_recalls = self.cur.fetchall()
            
            if product_recalls:
                display(Markdown("**产品与召回的主要关联 (前10名):**"))
                display(pd.DataFrame(product_recalls))
            
            # 公司与产品的关联
            self.cur.execute("""
                SELECT 
                    c.name as company_name,
                    COUNT(DISTINCT pc.id) as product_count
                FROM device.companies c
                JOIN device.product_codes pc ON pc.id IN (
                    SELECT product_code_id FROM device.udi_records WHERE company_id = c.id
                )
                GROUP BY c.name
                ORDER BY product_count DESC
                LIMIT 10
            """)
            
            company_products = self.cur.fetchall()
            
            if company_products:
                display(Markdown("**公司与产品的主要关联 (前10名):**"))
                display(pd.DataFrame(company_products))
            
            # 分析关键实体的关联强度
            display(HTML("<h4>实体关联强度分析</h4>"))
            
            # 企业与不良事件关联
            self.cur.execute("""
                SELECT 
                    c.name as company_name,
                    COUNT(DISTINCT ae.id) as adverse_event_count
                FROM device.companies c
                JOIN device.adverse_events ae ON c.id = ae.company_id
                GROUP BY c.name
                ORDER BY adverse_event_count DESC
                LIMIT 10
            """)
            
            company_events = self.cur.fetchall()
            
            if company_events:
                display(Markdown("**企业与不良事件的主要关联 (前10名):**"))
                display(pd.DataFrame(company_events))
                
                # 可视化前10家公司的不良事件数量
                plt.figure(figsize=(12, 6))
                plt.bar([row['company_name'] for row in company_events], 
                        [row['adverse_event_count'] for row in company_events])
                plt.title('前10家公司的不良事件数量')
                plt.xticks(rotation=45, ha='right')
                plt.tight_layout()
                plt.show()
            
            # 生成实体关系概述
            display(HTML("<h4>实体关系概述</h4>"))
            
            # 计算各实体表的记录数
            entity_counts = {}
            for entity, tables in entities.items():
                entity_counts[entity] = 0
                for table in tables:
                    if table in self.table_stats:
                        entity_counts[entity] += self.table_stats[table]['row_count']
            
            # 计算实体间的关联数
            entity_connections = []
            
            # 产品与不良事件
            self.cur.execute("""
                SELECT COUNT(DISTINCT ed.event_id) as connection_count
                FROM device.product_codes pc
                JOIN device.event_devices ed ON pc.id = ed.product_code_id
            """)
            
            result = self.cur.fetchone()
            if result and result['connection_count']:
                entity_connections.append({
                    '源实体': '设备',
                    '目标实体': '不良事件',
                    '关联数量': result['connection_count'],
                    '关联类型': '产品代码关联'
                })
            
            # 产品与召回
            self.cur.execute("""
                SELECT COUNT(DISTINCT dr.id) as connection_count
                FROM device.product_codes pc
                JOIN device.device_recalls dr ON pc.id = dr.product_code_id
            """)
            
            result = self.cur.fetchone()
            if result and result['connection_count']:
                entity_connections.append({
                    '源实体': '设备',
                    '目标实体': '召回',
                    '关联数量': result['connection_count'],
                    '关联类型': '产品代码关联'
                })
            
            # 公司与产品
            self.cur.execute("""
                SELECT COUNT(DISTINCT ur.id) as connection_count
                FROM device.companies c
                JOIN device.udi_records ur ON c.id = ur.company_id
            """)
            
            result = self.cur.fetchone()
            if result and result['connection_count']:
                entity_connections.append({
                    '源实体': '公司',
                    '目标实体': '设备',
                    '关联数量': result['connection_count'],
                    '关联类型': 'UDI记录关联'
                })
            
            # 公司与不良事件
            self.cur.execute("""
                SELECT COUNT(DISTINCT ae.id) as connection_count
                FROM device.companies c
                JOIN device.adverse_events ae ON c.id = ae.company_id
            """)
            
            result = self.cur.fetchone()
            if result and result['connection_count']:
                entity_connections.append({
                    '源实体': '公司',
                    '目标实体': '不良事件',
                    '关联数量': result['connection_count'],
                    '关联类型': '公司ID关联'
                })
            
            # 公司与召回
            self.cur.execute("""
                SELECT COUNT(DISTINCT dr.id) as connection_count
                FROM device.companies c
                JOIN device.device_recalls dr ON c.id = dr.company_id
            """)
            
            result = self.cur.fetchone()
            if result and result['connection_count']:
                entity_connections.append({
                    '源实体': '公司',
                    '目标实体': '召回',
                    '关联数量': result['connection_count'],
                    '关联类型': '公司ID关联'
                })
            
            # 显示实体关系概述
            display(Markdown("**实体数量:**"))
            display(pd.DataFrame(list(entity_counts.items()), columns=['实体', '记录数']))
            
            display(Markdown("**实体间关联:**"))
            display(pd.DataFrame(entity_connections))
            
            # 提供实体连接质量分析
            display(HTML("<h4>实体连接质量分析</h4>"))
            
            # 分析连接完整性
            connection_quality = []
            
            # 产品代码连接完整性
            self.cur.execute("""
                SELECT 
                    'event_devices' as table_name,
                    COUNT(*) as total_records,
                    SUM(CASE WHEN product_code_id IS NULL THEN 1 ELSE 0 END) as missing_connections,
                    ROUND((SUM(CASE WHEN product_code_id IS NULL THEN 1 ELSE 0 END)::float / COUNT(*)) * 100, 2) as missing_percentage
                FROM device.event_devices
            """)
            
            result = self.cur.fetchone()
            if result:
                connection_quality.append({
                    '表名': result['table_name'],
                    '总记录数': result['total_records'],
                    '缺失连接数': result['missing_connections'],
                    '缺失百分比': result['missing_percentage']
                })
            
            # 公司连接完整性（不良事件）
            self.cur.execute("""
                SELECT 
                    'adverse_events' as table_name,
                    COUNT(*) as total_records,
                    SUM(CASE WHEN company_id IS NULL THEN 1 ELSE 0 END) as missing_connections,
                    ROUND((SUM(CASE WHEN company_id IS NULL THEN 1 ELSE 0 END)::float / COUNT(*)) * 100, 2) as missing_percentage
                FROM device.adverse_events
            """)
            
            result = self.cur.fetchone()
            if result:
                connection_quality.append({
                    '表名': result['table_name'],
                    '总记录数': result['total_records'],
                    '缺失连接数': result['missing_connections'],
                    '缺失百分比': result['missing_percentage']
                })
            
            # 公司连接完整性（召回）
            self.cur.execute("""
                SELECT 
                    'device_recalls' as table_name,
                    COUNT(*) as total_records,
                    SUM(CASE WHEN company_id IS NULL THEN 1 ELSE 0 END) as missing_connections,
                    ROUND((SUM(CASE WHEN company_id IS NULL THEN 1 ELSE 0 END)::float / COUNT(*)) * 100, 2) as missing_percentage
                FROM device.device_recalls
            """)
            
            result = self.cur.fetchone()
            if result:
                connection_quality.append({
                    '表名': result['table_name'],
                    '总记录数': result['total_records'],
                    '缺失连接数': result['missing_connections'],
                    '缺失百分比': result['missing_percentage']
                })
            
            # 显示连接质量分析
            if connection_quality:
                display(Markdown("**实体连接质量:**"))
                display(pd.DataFrame(connection_quality))
                
                # 提供建议
                poor_connections = [row for row in connection_quality if row['缺失百分比'] > 10]
                if poor_connections:
                    display(Markdown("**连接质量问题:**"))
                    for conn in poor_connections:
                        display(Markdown(f"- {conn['表名']} 中有 {conn['缺失百分比']}% 的记录缺少关联，这可能影响跨实体分析的准确性"))
                    
                    display(Markdown("""
                    **提高实体连接质量的建议:**
                    
                    1. **完善产品代码映射:**
                       - 使用字符串匹配或模糊匹配技术映射缺失的产品代码
                       - 考虑使用辅助信息（如产品名称）辅助匹配
                       
                    2. **完善公司映射:**
                       - 使用公司名称标准化和匹配技术识别相同公司的不同表述
                       - 考虑使用外部公司数据源（如DUNS等）来完善映射
                       
                    3. **改进数据采集流程:**
                       - 如果您负责数据采集，确保收集关键的连接字段
                       - 考虑使用跨引用验证确保关联完整性
                    """))
        
        except Exception as e:
            print(f"❌ 分析实体关联时出错: {str(e)}")
    
    def generate_optimization_recommendations(self):
        """生成数据库优化建议"""
        display(HTML("<h3>数据库优化建议</h3>"))
        
        try:
            recommendations = []
            
            # 1. 索引优化建议
            display(HTML("<h4>索引优化建议</h4>"))
            
            # 1.1 缺少索引的大表
            large_tables_without_indexes = []
            
            for table_name, stats in self.table_stats.items():
                if stats['row_count'] > 100000:  # 大于10万行的表
                    if table_name not in self.index_analysis or self.index_analysis[table_name]['total_indexes'] < 3:
                        large_tables_without_indexes.append({
                            '表名': table_name,
                            '行数': stats['row_count'],
                            '索引数': self.index_analysis.get(table_name, {}).get('total_indexes', 0)
                        })
            
            if large_tables_without_indexes:
                display(Markdown("**大表缺少足够索引:**"))
                display(pd.DataFrame(large_tables_without_indexes))
                
                # 为这些表生成建议的索引
                display(Markdown("**建议添加以下索引:**"))
                
                for table_info in large_tables_without_indexes:
                    table_name = table_info['表名']
                    
                    # 查找该表的常用查询列
                    self.cur.execute(f"""
                        SELECT column_name, data_type
                        FROM information_schema.columns
                        WHERE table_schema = '{self.schema}'
                        AND table_name = '{table_name}'
                        AND (
                            column_name LIKE '%\\_id' OR
                            column_name LIKE '%date%' OR
                            column_name LIKE '%code%' OR
                            column_name LIKE '%number%' OR
                            column_name LIKE '%key%' OR
                            column_name LIKE '%status%'
                        )
                        AND column_name NOT IN (
                            SELECT a.attname
                            FROM pg_class t
                            JOIN pg_index ix ON t.oid = ix.indrelid
                            JOIN pg_class i ON i.oid = ix.indexrelid
                            JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
                            WHERE t.relname = '{table_name}'
                            AND t.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = '{self.schema}')
                        )
                        LIMIT 5
                    """)
                    
                    potential_index_columns = self.cur.fetchall()
                    
                    for i, col in enumerate(potential_index_columns):
                        column_name = col['column_name']
                        data_type = col['data_type']
                        
                        # 为这些列生成CREATE INDEX语句
                        index_name = f"idx_{table_name}_{column_name}"
                        
                        sql = f"CREATE INDEX {index_name} ON {self.schema}.{table_name} ({column_name});"
                        display(Markdown(f"```sql\n{sql}\n```"))
                        
                        # 提供解释
                        if "date" in column_name:
                            display(Markdown(f"- 为 `{table_name}.{column_name}` 添加索引将加速按日期范围的查询"))
                        elif "id" in column_name:
                            display(Markdown(f"- 为 `{table_name}.{column_name}` 添加索引将加速与其他表的连接查询"))
                        elif "code" in column_name or "number" in column_name:
                            display(Markdown(f"- 为 `{table_name}.{column_name}` 添加索引将加速按代码或编号的过滤查询"))
                        elif "status" in column_name:
                            display(Markdown(f"- 为 `{table_name}.{column_name}` 添加索引将加速按状态过滤的查询"))
            
            # 1.2 检查索引冗余
            redundant_indexes = []
            
            for table_name, index_info in self.index_analysis.items():
                if index_info['total_indexes'] > 5:  # 索引数量较多的表
                    # 检查是否有相似的索引列
                    similar_indexes = []
                    
                    for i, index1 in enumerate(index_info.get('index_details', {}).items()):
                        index1_name, index1_data = index1
                        index1_columns = index1_data['columns'].split(', ')
                        
                        for j, index2 in enumerate(list(index_info.get('index_details', {}).items())[i+1:]):
                            index2_name, index2_data = index2
                            index2_columns = index2_data['columns'].split(', ')
                            
                            # 检查两个索引是否覆盖相似的列
                            if len(index1_columns) > 0 and len(index2_columns) > 0:
                                if index1_columns[0] in index2_columns:
                                    similar_indexes.append({
                                        '表名': table_name,
                                        '索引1': index1_name,
                                        '列1': index1_data['columns'],
                                        '索引2': index2_name,
                                        '列2': index2_data['columns']
                                    })
                    
                    if similar_indexes:
                        redundant_indexes.extend(similar_indexes)
            
            if redundant_indexes:
                display(Markdown("**潜在冗余索引:**"))
                display(pd.DataFrame(redundant_indexes))
                
                display(Markdown("""
                **处理冗余索引的建议:**
                
                1. 审查这些相似索引，确定哪些可以合并或删除
                2. 优先保留多列复合索引，它们通常可以替代单列索引
                3. 使用数据库工具(如pg_stat_statements)监控索引使用情况，删除未使用的索引
                
                例如，如果有以下两个索引：
                - idx_table_col1
                - idx_table_col1_col2
                
                通常可以只保留第二个复合索引(idx_table_col1_col2)，它同时支持对col1和(col1,col2)的查询。
                """))
            
            # 2. 表分区建议
            display(HTML("<h4>表分区建议</h4>"))
            
            # 确定可能适合分区的大表
            partition_candidates = []
            
            for table_name, stats in self.table_stats.items():
                if stats['row_count'] > 10000000:  # 超过1000万行的表
                    # 检查是否有适合分区的日期列
                    self.cur.execute(f"""
                        SELECT column_name, data_type
                        FROM information_schema.columns
                        WHERE table_schema = '{self.schema}'
                        AND table_name = '{table_name}'
                        AND (data_type LIKE '%date%' OR data_type LIKE '%time%')
                    """)
                    
                    date_columns = self.cur.fetchall()
                    
                    # 查找第一个适合分区的日期列
                    partition_column = None
                    date_range = None
                    
                    for col in date_columns:
                        column_name = col['column_name']
                        
                        self.cur.execute(f"""
                            SELECT 
                                MIN({column_name}) as min_date,
                                MAX({column_name}) as max_date,
                                COUNT(DISTINCT EXTRACT(YEAR FROM {column_name})) as num_years
                            FROM {self.schema}.{table_name}
                            WHERE {column_name} IS NOT NULL
                        """)
                        
                        result = self.cur.fetchone()
                        
                        if result and result['min_date'] and result['max_date'] and result['num_years'] > 1:
                            partition_column = column_name
                            date_range = {
                                'min_date': result['min_date'],
                                'max_date': result['max_date'],
                                'num_years': result['num_years']
                            }
                            break
                    
                    if partition_column:
                        partition_candidates.append({
                            '表名': table_name,
                            '行数': stats['row_count'],
                            '分区列': partition_column,
                            '日期范围': f"{date_range['min_date']} 至 {date_range['max_date']}",
                            '年数': date_range['num_years']
                        })
            
            if partition_candidates:
                display(Markdown("**适合分区的大表:**"))
                display(pd.DataFrame(partition_candidates))
                
                display(Markdown("""
                **表分区建议:**
                
                大型表的分区可以显著提高查询性能和管理效率，尤其是时间序列数据。PostgreSQL支持声明式分区，推荐按年或按月分区：
                
                ```sql
                -- 示例：按月分区表
                CREATE TABLE partitioned_table (
                    id SERIAL,
                    date_column DATE,
                    -- 其他列
                ) PARTITION BY RANGE (date_column);
                
                -- 创建月度分区
                CREATE TABLE partitioned_table_y2022m01 PARTITION OF partitioned_table
                    FOR VALUES FROM ('2022-01-01') TO ('2022-02-01');
                CREATE TABLE partitioned_table_y2022m02 PARTITION OF partitioned_table
                    FOR VALUES FROM ('2022-02-01') TO ('2022-03-01');
                -- 更多分区...
                ```
                
                **分区实施建议:**
                
                1. 对于历史数据，在维护窗口期间执行分区迁移
                2. 使用临时表+重命名策略最小化停机时间
                3. 为新的分区创建相同的索引结构
                4. 考虑自动化分区创建以处理新数据
                """))
            
            # 3. 数据归档建议
            display(HTML("<h4>数据归档建议</h4>"))
            
            # 检查包含旧数据的表
            archive_candidates = []
            
            for table_name, stats in self.table_stats.items():
                if stats['row_count'] > 1000000:  # 超过100万行的表
                    # 检查是否有日期列
                    self.cur.execute(f"""
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_schema = '{self.schema}'
                        AND table_name = '{table_name}'
                        AND (data_type LIKE '%date%' OR data_type LIKE '%time%')
                    """)
                    
                    date_columns = [row['column_name'] for row in self.cur.fetchall()]
                    
                    # 如果有日期列，检查有多少旧数据
                    if date_columns:
                        for date_column in date_columns[:1]:  # 只检查第一个日期列
                            self.cur.execute(f"""
                                SELECT
                                    COUNT(*) as total_count,
                                    SUM(CASE WHEN {date_column} < CURRENT_DATE - INTERVAL '5 years' THEN 1 ELSE 0 END) as old_data_count
                                FROM {self.schema}.{table_name}
                                WHERE {date_column} IS NOT NULL
                            """)
                            
                            result = self.cur.fetchone()
                            
                            if result and result['old_data_count'] > 100000:  # 超过10万行旧数据
                                old_percentage = (result['old_data_count'] / result['total_count']) * 100 if result['total_count'] > 0 else 0
                                
                                if old_percentage > 20:  # 超过20%是旧数据
                                    archive_candidates.append({
                                        '表名': table_name,
                                        '总行数': stats['row_count'],
                                        '旧数据行数': result['old_data_count'],
                                        '旧数据百分比': f"{round(old_percentage, 2)}%",
                                        '日期列': date_column
                                    })
            
            if archive_candidates:
                display(Markdown("**适合数据归档的表:**"))
                display(pd.DataFrame(archive_candidates))
                
                display(Markdown("""
                **数据归档建议:**
                
                对于包含大量历史数据的表，建议实施数据归档策略以提高性能并优化存储：
                
                1. **归档策略：**
                   - 创建与源表结构相同的归档表
                   - 将旧数据移动到归档表
                   - 建立可访问归档数据的视图
                
                2. **归档方案示例：**
                   ```sql
                   -- 创建归档表
                   CREATE TABLE archive_table (LIKE source_table INCLUDING ALL);
                   
                   -- 迁移旧数据
                   BEGIN;
                   INSERT INTO archive_table SELECT * FROM source_table WHERE date_column < CURRENT_DATE - INTERVAL '5 years';
                   DELETE FROM source_table WHERE date_column < CURRENT_DATE - INTERVAL '5 years';
                   COMMIT;
                   
                   -- 创建统一访问视图
                   CREATE VIEW full_data_view AS
                       SELECT * FROM source_table
                       UNION ALL
                       SELECT * FROM archive_table;
                   ```
                
                3. **增量归档：**
                   - 设置定期归档作业，例如每月或每季度运行一次
                   - 使用批处理方法避免长时间锁定
                   - 考虑在低峰时段执行归档
                
                4. **归档后操作：**
                   - 对归档表执行VACUUM FULL释放空间
                   - 重建源表索引以优化性能
                   - 更新表统计信息以确保查询优化器使用正确的计划
                """))
            
        except Exception as e:
            print(f"❌ 生成数据库优化建议时出错: {str(e)}")
    
    def generate_query_recommendations(self):
        """生成查询优化建议"""
        display(HTML("<h3>查询优化建议</h3>"))
        
        try:
            # 生成常见查询模式及其优化建议
            display(Markdown("""
            基于对FDA医疗设备数据库结构和数据特点的分析，以下是常见查询场景的优化建议：
            
            ### 1. 查询不良事件数据
            
            **常见模式:** 按日期范围和事件类型查询不良事件
            
            ```sql
            -- 未优化的查询
            SELECT *
            FROM device.adverse_events
            WHERE date_received BETWEEN '2020-01-01' AND '2020-12-31'
            AND event_type = 'Malfunction';
            ```
            
            **优化建议:**
            ```sql
            -- 优化的查询
            SELECT ae.id, ae.report_number, ae.date_received, ae.event_type
            FROM device.adverse_events ae
            WHERE ae.date_received BETWEEN '2020-01-01' AND '2020-12-31'
            AND ae.event_type = 'Malfunction'
            LIMIT 1000;  -- 限制结果集大小
            ```
            
            **性能提升点:**
            - 选择必要的列而非使用 * 
            - 对大结果集使用LIMIT
            - 确保date_received和event_type列有索引
            
            ### 2. 跨表连接查询
            
            **常见模式:** 关联设备、公司和不良事件数据
            
            ```sql
            -- 未优化的查询
            SELECT *
            FROM device.adverse_events ae
            JOIN device.event_devices ed ON ae.id = ed.event_id
            JOIN device.product_codes pc ON ed.product_code_id = pc.id
            JOIN device.companies c ON ae.company_id = c.id
            WHERE ae.date_received > '2019-01-01';
            ```
            
            **优化建议:**
            ```sql
            -- 优化的查询
            SELECT 
                ae.report_number,
                ae.date_received,
                ae.event_type,
                pc.product_code,
                pc.device_name,
                c.name as company_name
            FROM device.adverse_events ae
            JOIN device.event_devices ed ON ae.id = ed.event_id
            JOIN device.product_codes pc ON ed.product_code_id = pc.id
            JOIN device.companies c ON ae.company_id = c.id
            WHERE ae.date_received > '2019-01-01'
            ORDER BY ae.date_received DESC
            LIMIT 500;
            ```
            
            **性能提升点:**
            - 选择必要的列而非使用 *
            - 确保所有连接列都有索引
            - 优化WHERE子句中的筛选条件
            - 对结果进行排序和分页
            
            ### 3. 聚合查询
            
            **常见模式:** 按产品代码和设备类型统计不良事件
            
            ```sql
            -- 未优化的查询
            SELECT 
                pc.product_code,
                pc.device_name,
                COUNT(*) as event_count
            FROM device.adverse_events ae
            JOIN device.event_devices ed ON ae.id = ed.event_id
            JOIN device.product_codes pc ON ed.product_code_id = pc.id
            GROUP BY pc.product_code, pc.device_name;
            ```
            
            **优化建议:**
            ```sql
            -- 优化的查询
            SELECT 
                pc.product_code,
                pc.device_name,
                COUNT(*) as event_count
            FROM device.adverse_events ae
            JOIN device.event_devices ed ON ae.id = ed.event_id
            JOIN device.product_codes pc ON ed.product_code_id = pc.id
            WHERE ae.date_received > CURRENT_DATE - INTERVAL '5 years'
            GROUP BY pc.product_code, pc.device_name
            HAVING COUNT(*) > 10
            ORDER BY event_count DESC
            LIMIT 100;
            ```
            
            **性能提升点:**
            - 添加时间范围过滤器减少处理记录数
            - 使用HAVING子句过滤低价值结果
            - 对结果排序和分页
            - 考虑创建物化视图以加速常用的聚合查询
            
            ### 4. 时间序列分析查询
            
            **常见模式:** 按月分析不良事件趋势
            
            ```sql
            -- 未优化的查询
            SELECT 
                DATE_TRUNC('month', date_received) as month,
                COUNT(*) as event_count
            FROM device.adverse_events
            GROUP BY month
            ORDER BY month;
            ```
            
            **优化建议:**
            ```sql
            -- 优化的查询
            SELECT 
                DATE_TRUNC('month', date_received) as month,
                event_type,
                COUNT(*) as event_count
            FROM device.adverse_events
            WHERE date_received >= CURRENT_DATE - INTERVAL '3 years'
            GROUP BY month, event_type
            ORDER BY month DESC, event_count DESC;
            ```
            
            **性能提升点:**
            - 限制时间范围
            - 按更多维度聚合以获得更详细的洞察
            - 考虑预计算常用时间段的聚合结果
            
            ### 5. 优化数据导出查询
            
            **常见模式:** 导出大量数据用于外部分析
            
            ```sql
            -- 未优化的查询
            COPY (SELECT * FROM device.adverse_events) TO '/tmp/export.csv' WITH CSV HEADER;
            ```
            
            **优化建议:**
            ```sql
            -- 优化的数据导出
            COPY (
                SELECT 
                    ae.report_number,
                    ae.date_received,
                    ae.event_type,
                    pc.product_code,
                    pc.device_name,
                    c.name as company_name
                FROM device.adverse_events ae
                LEFT JOIN device.event_devices ed ON ae.id = ed.event_id
                LEFT JOIN device.product_codes pc ON ed.product_code_id = pc.id
                LEFT JOIN device.companies c ON ae.company_id = c.id
                WHERE ae.date_received > '2020-01-01'
                ORDER BY ae.date_received DESC
                LIMIT 1000000
            ) TO '/tmp/adverse_events_export.csv' WITH (FORMAT CSV, HEADER);
            ```
            
            **性能提升点:**
            - 只导出必要的列和记录
            - 使用LEFT JOIN避免排除没有关联记录的行
            - 考虑分批导出大数据集
            - 优先使用WHERE过滤而非客户端过滤
            """))
            
            # 保存查询建议以供后续分析
            self.query_recommendations = {
                'adverse_events': [
                    "确保date_received, event_type, report_number列有索引",
                    "使用LIMIT和分页处理大结果集",
                    "建议对date_received列创建BRIN索引以支持高效的日期范围扫描"
                ],
                'event_devices': [
                    "确保event_id和product_code_id列都有索引",
                    "考虑创建(event_id, product_code_id)复合索引以优化连接查询"
                ],
                'product_codes': [
                    "对product_code列创建唯一索引",
                    "确保device_class列有索引以支持过滤查询"
                ],
                'companies': [
                    "为name列创建索引以支持公司名称查询",
                    "考虑为name列添加trigram索引以支持模糊匹配"
                ]
            }
        
        except Exception as e:
            print(f"❌ 生成查询优化建议时出错: {str(e)}")
    
    def generate_preprocessing_recommendations(self):
        """生成数据预处理建议"""
        display(HTML("<h3>数据预处理建议</h3>"))
        
        try:
            display(Markdown("""
            基于对FDA医疗设备数据的分析，以下是针对不同分析场景的数据预处理建议：
            
            ### 1. 日期和时间预处理
            
            FDA医疗设备数据库中的日期字段可能存在多种问题，包括缺失值、未来日期和异常早期日期。建议的预处理步骤：
            
            ```python
            import pandas as pd
            import numpy as np
            from datetime import datetime, timedelta
            
            # 加载数据
            df = pd.read_sql("SELECT * FROM device.adverse_events LIMIT 1000000", conn)
            
            # 1. 转换日期列
            date_columns = ['date_received', 'date_of_event', 'date_report']
            for col in date_columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
            
            # 2. 处理未来日期
            current_date = datetime.now()
            future_threshold = current_date + timedelta(days=30)  # 允许小范围的未来日期（例如30天）
            
            for col in date_columns:
                # 将过远的未来日期设置为NaT
                mask_far_future = df[col] > future_threshold
                df.loc[mask_far_future, col] = pd.NaT
                print(f"将{mask_far_future.sum()}条{col}列的远期未来日期设置为NaT")
            
            # 3. 处理异常早期日期
            min_valid_date = pd.Timestamp('1980-01-01')  # 设置合理的最早日期
            
            for col in date_columns:
                # 将过早的日期设置为NaT
                mask_too_early = df[col] < min_valid_date
                df.loc[mask_too_early, col] = pd.NaT
                print(f"将{mask_too_early.sum()}条{col}列的异常早期日期设置为NaT")
            
            # 4. 创建日期层次字段
            for col in date_columns:
                if df[col].notna().any():
                    df[f'{col}_year'] = df[col].dt.year
                    df[f'{col}_month'] = df[col].dt.month
                    df[f'{col}_quarter'] = df[col].dt.quarter
            ```
            
            ### 2. 分类变量预处理
            
            FDA数据中的代码、类型和状态等分类字段通常存在标准化和一致性问题：
            
            ```python
            # 1. 字符串标准化
            categorical_columns = ['event_type', 'report_source_code', 'manufacturer_name']
            
            for col in categorical_columns:
                # 转换为字符串类型
                df[col] = df[col].astype(str)
                
                # 去除空格和标准化大小写
                df[col] = df[col].str.strip().str.upper()
                
                # 替换特殊空值表示
                df[col] = df[col].replace(['NONE', 'NULL', 'NA', 'NAN', ''], np.nan)
            
            # 2. 标准化特定字段的值
            # 示例：标准化不良事件类型
            event_type_mapping = {
                'INJURY': 'INJURY',
                'INJURIES': 'INJURY',
                'PATIENT INJURY': 'INJURY',
                'MALFUNCTION': 'MALFUNCTION',
                'DEVICE MALFUNCTION': 'MALFUNCTION',
                'DEATH': 'DEATH',
                'PATIENT DEATH': 'DEATH',
                'OTHER': 'OTHER'
            }
            
            df['event_type_std'] = df['event_type'].map(
                lambda x: next((v for k, v in event_type_mapping.items() if k in str(x).upper()), 'OTHER')
            )
            
            # 3. 处理高基数分类变量
            high_cardinality_columns = ['manufacturer_name']
            for col in high_cardinality_columns:
                # 找出最常见的值
                top_values = df[col].value_counts().head(20).index.tolist()
                
                # 创建一个简化版本的列
                df[f'{col}_grouped'] = df[col].apply(lambda x: x if x in top_values else 'Other')
            ```
            
            ### 3. 数值变量预处理
            
            处理FDA数据中的数值型字段，包括异常值检测和处理：
            
            ```python
            import scipy.stats as stats
            
            # 1. 识别数值列
            numeric_columns = df.select_dtypes(include=['int64', 'float64']).columns.tolist()
            
            # 2. 异常值处理
            for col in numeric_columns:
                if df[col].notna().sum() > 0:
                    # 使用IQR方法检测异常值
                    Q1 = df[col].quantile(0.25)
                    Q3 = df[col].quantile(0.75)
                    IQR = Q3 - Q1
                    lower_bound = Q1 - 1.5 * IQR
                    upper_bound = Q3 + 1.5 * IQR
                    
                    # 检测异常值
                    outliers = ((df[col] < lower_bound) | (df[col] > upper_bound))
                    print(f"列 {col} 中检测到 {outliers.sum()} 个异常值")
                    
                    # 根据分析需求选择适当的异常值处理方法
                    # 选项1：替换为边界值（如果需要保留数据点）
                    # df.loc[df[col] < lower_bound, col] = lower_bound
                    # df.loc[df[col] > upper_bound, col] = upper_bound
                    
                    # 选项2：将异常值替换为NaN（如果分析允许缺失值）
                    # df.loc[outliers, col] = np.nan
                    
                    # 选项3：为异常值创建标志列（保留原始数据但标记异常值）
                    df[f'{col}_is_outlier'] = outliers
            
            # 3. 标准化/归一化数值特征
            from sklearn.preprocessing import StandardScaler, MinMaxScaler
            
            # 选择要标准化的列
            scale_columns = [col for col in numeric_columns if df[col].notna().sum() > 0]
            
            # 标准化（均值为0，标准差为1）
            scaler = StandardScaler()
            df[f"{scale_columns}_scaled"] = scaler.fit_transform(df[scale_columns].fillna(0))
            
            # 或者归一化（范围从0到1）
            normalizer = MinMaxScaler()
            df[f"{scale_columns}_normalized"] = normalizer.fit_transform(df[scale_columns].fillna(0))
            ```
            
            ### 4. 处理缺失值
            
            FDA数据中存在大量缺失值，需要根据分析需求采用适当的处理策略：
            
            ```python
            # 1. 计算缺失值比例
            missing_percentages = df.isnull().mean().sort_values(ascending=False) * 100
            print("各列缺失值比例:")
            print(missing_percentages[missing_percentages > 0])
            
            # 2. 删除缺失值比例极高的列（可选）
            high_missing_cols = missing_percentages[missing_percentages > 90].index.tolist()
            df_reduced = df.drop(columns=high_missing_cols)
            print(f"删除了 {len(high_missing_cols)} 列，缺失值比例超过90%")
            
            # 3. 针对不同类型的字段使用不同的填充策略
            
            # 分类字段填充为"Unknown"
            for col in categorical_columns:
                df[col] = df[col].fillna('Unknown')
            
            # 数值字段可以填充为中位数
            for col in numeric_columns:
                df[col] = df[col].fillna(df[col].median())
            
            # 日期字段可以保留为NaT或填充为特定值
            # 例如：用最早的有效日期填充缺失的事件日期
            for col in date_columns:
                df[col + '_missing'] = df[col].isna()  # 创建缺失指示器
            
            # 4. 高级填充方法（可选）- 使用相关列预测缺失值
            from sklearn.impute import KNNImputer
            
            # 示例：使用KNN估算缺失值
            # 选择要进行KNN插补的列
            knn_cols = ['col1', 'col2', 'col3']
            
            # 创建KNN估算器
            imputer = KNNImputer(n_neighbors=5)
            df[knn_cols] = imputer.fit_transform(df[knn_cols])
            ```
            
            ### 5. 特征工程
            
            为FDA数据创建有助于分析的新特征：
            
            ```python
            # 1. 时间特征
            # 计算事件报告延迟
            if 'date_of_event' in df.columns and 'date_received' in df.columns:
                mask = (df['date_of_event'].notna() & df['date_received'].notna())
                df.loc[mask, 'report_delay_days'] = (df.loc[mask, 'date_received'] - df.loc[mask, 'date_of_event']).dt.days
            
            # 2. 计算设备年龄（如果相关字段可用）
            if 'device_manufacture_date' in df.columns and 'date_of_event' in df.columns:
                mask = (df['device_manufacture_date'].notna() & df['date_of_event'].notna())
                df.loc[mask, 'device_age_days'] = (df.loc[mask, 'date_of_event'] - df.loc[mask, 'device_manufacture_date']).dt.days
            
            # 3. 创建组合特征
            # 例如：合并设备类型和问题类型
            if 'device_class' in df.columns and 'event_type' in df.columns:
                df['device_class_event'] = df['device_class'].astype(str) + '_' + df['event_type'].astype(str)
            
            # 4. 创建基于文本的特征
            if 'reason_for_recall' in df.columns:
                # 计算描述长度
                df['reason_length'] = df['reason_for_recall'].str.len()
                
                # 创建关键词指示器
                keywords = ['battery', 'software', 'contamination', 'sterile', 'label']
                for keyword in keywords:
                    df[f'has_{keyword}'] = df['reason_for_recall'].str.contains(keyword, case=False).astype(int)
            
            # 5. 聚合特征
            # 如果数据集有多个表，可以创建聚合特征
            # 例如：计算每个设备代码相关的不良事件数量
            device_event_counts = df.groupby('product_code')['report_number'].count().reset_index()
            device_event_counts.columns = ['product_code', 'event_count']
            
            # 将聚合特征合并回主数据框
            df = df.merge(device_event_counts, on='product_code', how='left')
            ```
            
            ### 6. 数据集划分策略
            
            对于机器学习分析，需要适当划分训练、验证和测试集：
            
            ```python
            from sklearn.model_selection import train_test_split
            
            # 1. 基于时间的划分（推荐用于时间序列数据）
            if 'date_received' in df.columns:
                # 根据日期排序
                df = df.sort_values('date_received')
                
                # 确定训练集和测试集的分割点（例如使用最后1年的数据作为测试集）
                split_date = df['date_received'].max() - pd.Timedelta(days=365)
                
                # 划分数据集
                train_df = df[df['date_received'] <= split_date]
                test_df = df[df['date_received'] > split_date]
                
                print(f"训练集: {len(train_df)} 行 ({len(train_df)/len(df):.1%})")
                print(f"测试集: {len(test_df)} 行 ({len(test_df)/len(df):.1%})")
            
            # 2. 随机划分（适用于非时间序列数据）
            else:
                # 划分特征和目标变量
                X = df.drop(columns=['target_column'])
                y = df['target_column']
                
                # 创建训练、验证和测试集
                X_train, X_temp, y_train, y_temp = train_test_split(X, y, test_size=0.3, random_state=42)
                X_val, X_test, y_val, y_test = train_test_split(X_temp, y_temp, test_size=0.5, random_state=42)
                
                print(f"训练集: {len(X_train)} 行 ({len(X_train)/len(X):.1%})")
                print(f"验证集: {len(X_val)} 行 ({len(X_val)/len(X):.1%})")
                print(f"测试集: {len(X_test)} 行 ({len(X_test)/len(X):.1%})")
            ```
            
            ### 7. 数据合规性预处理
            
            由于FDA数据可能包含敏感信息，在某些情况下需要进行数据脱敏：
            
            ```python
            # 1. 删除敏感列
            sensitive_columns = ['patient_id', 'specific_patient_details']
            df_safe = df.drop(columns=sensitive_columns)
            
            # 2. 脱敏处理
            import hashlib
            
            # 对特定列进行哈希处理
            if 'unique_device_identifier' in df.columns:
                df['hashed_udi'] = df['unique_device_identifier'].apply(
                    lambda x: hashlib.md5(str(x).encode()).hexdigest() if pd.notna(x) else None
                )
                df = df.drop(columns=['unique_device_identifier'])
            
            # 3. 分组/聚合敏感数据
            # 例如：将精确位置替换为更广泛的地理区域
            if 'facility_zip' in df.columns:
                df['facility_region'] = df['facility_zip'].str[:3]  # 使用邮编前3位表示大致区域
                df = df.drop(columns=['facility_zip'])
            ```
            
            ### 8. 数据质量检查
            
            在预处理完成后，应该进行一系列质量检查，确保数据可用于分析：
            
            ```python
            # 1. 最终缺失值检查
            final_missing = df.isnull().sum()
            if final_missing.sum() > 0:
                print("警告：预处理后数据仍有缺失值:")
                print(final_missing[final_missing > 0])
            
            # 2. 数据类型检查
            print("数据类型概览:")
            print(df.dtypes)
            
            # 3. 数值范围检查
            for col in df.select_dtypes(include=['int64', 'float64']).columns:
                print(f"{col}: 范围 [{df[col].min()} - {df[col].max()}], 均值: {df[col].mean():.2f}")
            
            # 4. 重复值检查
            duplicates = df.duplicated().sum()
            if duplicates > 0:
                print(f"警告：检测到 {duplicates} 条重复记录")
            
            # 5. 保存预处理后的数据
            df.to_csv('preprocessed_fda_data.csv', index=False)
            print(f"预处理完成，保存了 {len(df)} 行数据")
            ```
            
            遵循这些预处理步骤将帮助您准备FDA医疗设备数据以进行各种分析任务，包括描述性统计、趋势分析、预测模型等。根据您的具体分析目标，可能需要调整这些步骤的优先级或添加特定处理。
            """))
            
            # 保存预处理建议
            self.preprocessing_recommendations = {
                'date_handling': [
                    "处理未来日期和异常早期日期",
                    "创建日期层次字段（年、月、季度）",
                    "计算日期差值（如报告延迟）"
                ],
                'categorical_handling': [
                    "标准化分类字段（大小写、空格、特殊值）",
                    "创建标准化映射以处理不一致的值",
                    "对高基数分类变量进行分组"
                ],
                'numeric_handling': [
                    "使用IQR或Z分数检测并处理异常值",
                    "针对特定分析进行标准化或归一化",
                    "为异常值创建标志列"
                ],
                'missing_values': [
                    "根据分析需求采用不同的缺失值策略",
                    "为缺失值创建指示器列",
                    "考虑使用KNN或其他高级方法填充重要变量"
                ],
                'feature_engineering': [
                    "创建时间特征（如延迟、设备年龄）",
                    "创建组合特征",
                    "从文本字段中提取关键词特征",
                    "计算聚合特征"
                ]
            }
        
        except Exception as e:
            print(f"❌ 生成数据预处理建议时出错: {str(e)}")
    
def generate_analysis_recommendations(self):
    """生成分析建议"""
    display(HTML("<h3>分析建议</h3>"))
    
    try:
        display(Markdown("""
        基于对FDA医疗设备数据库的全面分析，以下是针对不同分析目标的建议：
        
        ### 1. 描述性统计分析
        
        **目标：** 了解医疗设备不良事件、召回和产品分布的基本情况
        
        **建议分析：**
        
        - **设备类型分布分析**
          - 按设备类别(Class I/II/III)统计产品数量和不良事件数量
          - 分析设备类型与风险级别的关系
        
        - **不良事件趋势分析**
          - 按年/月/季度统计不良事件数量
          - 分析不同事件类型(死亡/伤害/故障)的比例变化
          - 识别高峰期和可能的季节性模式
        
        - **召回情况分析**
          - 按召回原因和分类统计召回数量
          - 分析召回与设备类型的关系
          - 评估召回响应时间(从发现问题到召回行动)
        
        - **地理分布分析**
          - 按国家/地区分析不良事件和召回分布
          - 识别高风险地区或报告率异常的地区
        
        **示例SQL查询：**
        ```sql
        -- 按设备类别统计不良事件
        SELECT 
            pc.device_class,
            COUNT(DISTINCT ed.event_id) as event_count,
            COUNT(DISTINCT CASE WHEN ae.event_type = 'Death' THEN ed.event_id END) as death_count,
            COUNT(DISTINCT CASE WHEN ae.event_type = 'Injury' THEN ed.event_id END) as injury_count,
            COUNT(DISTINCT CASE WHEN ae.event_type = 'Malfunction' THEN ed.event_id END) as malfunction_count
        FROM 
            device.product_codes pc
        JOIN 
            device.event_devices ed ON pc.id = ed.product_code_id
        JOIN 
            device.adverse_events ae ON ed.event_id = ae.id
        WHERE 
            pc.device_class IS NOT NULL
        GROUP BY 
            pc.device_class
        ORDER BY 
            event_count DESC;
        ```
        
        ### 2. 趋势分析
        
        **目标：** 识别设备安全性和质量的长期趋势和模式
        
        **建议分析：**
        
        - **时间序列分析**
          - 使用移动平均和趋势分解技术分析长期趋势
          - 识别不良事件报告中的季节性模式
          - 评估政策变化对报告率的影响
        
        - **设备生命周期分析**
          - 分析设备从上市到出现首次不良事件的时间
          - 评估设备类型与问题出现时间的关系
          - 识别产品生命周期中的高风险期
        
        - **产品代码趋势比较**
          - 比较不同产品代码的不良事件趋势
          - 识别问题率上升或下降的产品类别
          - 评估特定产品代码的风险变化
        
        **示例Python分析：**
        ```python
        import pandas as pd
        import matplotlib.pyplot as plt
        from statsmodels.tsa.seasonal import seasonal_decompose
        
        # 加载数据
        query = '''
            SELECT 
                DATE_TRUNC('month', date_received) as month,
                COUNT(*) as event_count
            FROM 
                device.adverse_events
            WHERE 
                date_received BETWEEN '2010-01-01' AND CURRENT_DATE
            GROUP BY 
                month
            ORDER BY 
                month
        '''
        df = pd.read_sql(query, conn)
        df.set_index('month', inplace=True)
        
        # 执行时间序列分解
        result = seasonal_decompose(df['event_count'], model='multiplicative', period=12)
        
        # 绘制结果
        fig, (ax1, ax2, ax3, ax4) = plt.subplots(4, 1, figsize=(12, 10))
        result.observed.plot(ax=ax1, title='原始不良事件时间序列')
        result.trend.plot(ax=ax2, title='趋势成分')
        result.seasonal.plot(ax=ax3, title='季节性成分')
        result.resid.plot(ax=ax4, title='残差成分')
        plt.tight_layout()
        plt.show()
        ```
        
        ### 3. 关联分析
        
        **目标：** 识别设备特性、问题类型和结果之间的关系
        
        **建议分析：**
        
        - **设备特性与不良事件关联**
          - 分析设备类别、材料、使用环境与不良事件类型的关系
          - 识别与特定问题高度相关的设备特性
          - 评估设备复杂性与故障率的关系
        
        - **公司表现分析**
          - 比较不同公司的不良事件率和召回频率
          - 分析公司规模与产品质量的关系
          - 识别质量表现异常的制造商
        
        - **问题类型网络分析**
          - 构建问题代码的共现网络
          - 识别经常一起出现的问题类型
          - 发现潜在的因果链和风险模式
        
        **示例分析代码：**
        ```python
        import pandas as pd
        import numpy as np
        import matplotlib.pyplot as plt
        import seaborn as sns
        
        # 加载数据
        query = '''
            SELECT 
                c.name as company_name,
                COUNT(DISTINCT ae.id) as adverse_event_count,
                COUNT(DISTINCT CASE WHEN ae.event_type = 'Death' THEN ae.id END) as death_count,
                COUNT(DISTINCT dr.id) as recall_count
            FROM 
                device.companies c
            LEFT JOIN 
                device.adverse_events ae ON c.id = ae.company_id
            LEFT JOIN 
                device.device_recalls dr ON c.id = dr.company_id
            GROUP BY 
                c.name
            HAVING 
                COUNT(DISTINCT ae.id) > 10
            ORDER BY 
                adverse_event_count DESC
            LIMIT 50
        '''
        df = pd.read_sql(query, conn)
        
        # 计算比率
        df['death_ratio'] = df['death_count'] / df['adverse_event_count']
        df['recall_ratio'] = df['recall_count'] / df['adverse_event_count']
        
        # 绘制散点图
        plt.figure(figsize=(10, 8))
        sns.scatterplot(data=df, x='adverse_event_count', y='recall_ratio', size='death_ratio', 
                       sizes=(20, 200), alpha=0.7, hue='death_ratio')
        
        plt.title('公司不良事件、召回率和死亡率关系图')
        plt.xlabel('不良事件数量')
        plt.ylabel('召回率(召回数/不良事件数)')
        plt.xscale('log')
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.show()
        ```
        
        ### 4. 风险预测模型
        
        **目标：** 开发模型预测设备风险和问题概率
        
        **建议分析：**
        
        - **不良事件风险评估模型**
          - 使用设备特性和历史数据预测不良事件风险
          - 开发风险评分系统以识别高风险设备
          - 构建设备类别的风险比较框架
        
        - **召回预测模型**
          - 基于早期不良事件模式预测未来召回可能性
          - 识别与高召回率相关的设备特性和公司特征
          - 开发召回风险预警系统
        
        - **文本分析模型**
          - 从不良事件描述和召回原因中提取关键信息
          - 使用NLP技术对风险描述进行分类
          - 开发事件严重性自动评估系统
        
        **示例模型开发代码：**
        ```python
        import pandas as pd
        import numpy as np
        from sklearn.model_selection import train_test_split
        from sklearn.ensemble import RandomForestClassifier
        from sklearn.metrics import classification_report, roc_auc_score
        from sklearn.preprocessing import StandardScaler
        
        # 假设我们已经有了预处理好的数据集
        # 特征包括设备类型、公司历史、设备材料等
        # 目标变量是设备是否发生严重不良事件
        
        # 准备数据
        X = df[['device_class', 'company_event_history', 'is_implant', 'is_reusable', 
               'company_age', 'previous_recalls', 'device_complexity']]
        y = df['had_serious_event']
        
        # 划分训练集和测试集
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.25, random_state=42)
        
        # 标准化特征
        scaler = StandardScaler()
        X_train_scaled = scaler.fit_transform(X_train)
        X_test_scaled = scaler.transform(X_test)
        
        # 训练随机森林模型
        model = RandomForestClassifier(n_estimators=100, random_state=42)
        model.fit(X_train_scaled, y_train)
        
        # 评估模型
        y_pred = model.predict(X_test_scaled)
        y_prob = model.predict_proba(X_test_scaled)[:, 1]
        
        print(classification_report(y_test, y_pred))
        print(f"ROC AUC: {roc_auc_score(y_test, y_prob):.4f}")
        
        # 分析特征重要性
        feature_importance = pd.DataFrame({
            'feature': X.columns,
            'importance': model.feature_importances_
        }).sort_values('importance', ascending=False)
        
        print("特征重要性:")
        print(feature_importance)
        ```
        
        ### 5. 网络和图分析
        
        **目标：** 探索设备、公司和问题之间的关系网络
        
        **建议分析：**
        
        - **公司-产品网络**
          - 构建公司与产品之间的二部图
          - 识别共享技术或问题的公司集群
          - 分析问题传播模式
        
        - **产品问题关联网络**
          - 创建基于共同问题的产品相似性网络
          - 识别产品类别中的风险簇
          - 评估问题的传递性和相关性
        
        - **监管行动影响图**
          - 分析监管行动对不良事件报告的影响
          - 评估监管网络的覆盖范围和效果
          - 识别监管盲点或重点关注区域
        
        **示例网络分析代码：**
        ```python
        import pandas as pd
        import numpy as np
        import networkx as nx
        import matplotlib.pyplot as plt
        
        # 构建公司-产品网络
        query = '''
            SELECT 
                c.name as company_name,
                pc.product_code,
                COUNT(DISTINCT ae.id) as connection_strength
            FROM 
                device.companies c
            JOIN 
                device.adverse_events ae ON c.id = ae.company_id
            JOIN 
                device.event_devices ed ON ae.id = ed.event_id
            JOIN 
                device.product_codes pc ON ed.product_code_id = pc.id
            GROUP BY 
                c.name, pc.product_code
            HAVING 
                COUNT(DISTINCT ae.id) > 5
        '''
        edges_df = pd.read_sql(query, conn)
        
        # 创建二部图
        G = nx.Graph()
        
        # 添加节点
        for company in edges_df['company_name'].unique():
            G.add_node(company, node_type='company')
            
        for product in edges_df['product_code'].unique():
            G.add_node(product, node_type='product')
        
        # 添加边
        for _, row in edges_df.iterrows():
            G.add_edge(row['company_name'], row['product_code'], weight=row['connection_strength'])
        
        # 计算网络指标
        company_centrality = nx.degree_centrality(G)
        top_companies = sorted([(node, cent) for node, cent in company_centrality.items() 
                               if G.nodes[node].get('node_type') == 'company'],
                              key=lambda x: x[1], reverse=True)[:10]
        
        print("网络中最中心的公司:")
        for company, cent in top_companies:
            print(f"{company}: {cent:.4f}")
            
        # 可视化网络
        plt.figure(figsize=(12, 12))
        pos = nx.spring_layout(G, k=0.3)
        
        # 绘制节点
        company_nodes = [node for node, attr in G.nodes(data=True) if attr.get('node_type') == 'company']
        product_nodes = [node for node, attr in G.nodes(data=True) if attr.get('node_type') == 'product']
        
        nx.draw_networkx_nodes(G, pos, nodelist=company_nodes, node_color='red', node_size=100, alpha=0.8)
        nx.draw_networkx_nodes(G, pos, nodelist=product_nodes, node_color='blue', node_size=50, alpha=0.8)
        
        # 绘制边
        edges = G.edges(data=True)
        weights = [data['weight'] for _, _, data in edges]
        nx.draw_networkx_edges(G, pos, width=[w/10 for w in weights], alpha=0.3)
        
        plt.title('公司-产品关联网络')
        plt.axis('off')
        plt.tight_layout()
        plt.show()
        ```
        
        ### 6. 文本挖掘分析
        
        **目标：** 从不良事件描述和召回文本中提取见解
        
        **建议分析：**
        
        - **关键词提取和趋势**
          - 从不良事件报告中提取常见问题描述
          - 跟踪关键术语使用频率的变化
          - 识别新出现的风险术语
        
        - **主题建模**
          - 使用LDA或其他主题模型分析事件描述
          - 识别常见问题类型和主题
          - 探索主题随时间的演变
        
        - **情感分析**
          - 分析不良事件报告中的术语严重性
          - 评估报告语言与事件严重程度的关系
          - 发现报告偏差和模式
        
        **示例文本分析代码：**
        ```python
        import pandas as pd
        import numpy as np
        import matplotlib.pyplot as plt
        import re
        from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer
        from sklearn.decomposition import LatentDirichletAllocation
        from wordcloud import WordCloud
        
        # 加载文本数据
        query = '''
            SELECT 
                et.text,
                ae.event_type,
                ae.date_received
            FROM 
                device.event_texts et
            JOIN 
                device.adverse_events ae ON et.event_id = ae.id
            WHERE 
                et.text_type_code = 'Description'
                AND ae.date_received > '2015-01-01'
            LIMIT 10000
        '''
        df = pd.read_sql(query, conn)
        
        # 文本预处理
        def preprocess_text(text):
            if pd.isna(text):
                return ""
            # 转换为小写
            text = text.lower()
            # 移除特殊字符
            text = re.sub(r'[^\\w\\s]', '', text)
            # 移除数字
            text = re.sub(r'\\d+', '', text)
            # 移除多余空格
            text = re.sub(r'\\s+', ' ', text).strip()
            return text
        
        df['processed_text'] = df['text'].apply(preprocess_text)
        
        # 创建TF-IDF向量
        tfidf_vectorizer = TfidfVectorizer(max_features=1000, stop_words='english', min_df=5)
        tfidf_matrix = tfidf_vectorizer.fit_transform(df['processed_text'])
        
        # 主题建模
        lda = LatentDirichletAllocation(n_components=5, random_state=42)
        lda.fit(tfidf_matrix)
        
        # 显示主题
        feature_names = tfidf_vectorizer.get_feature_names_out()
        
        for topic_idx, topic in enumerate(lda.components_):
            top_words_idx = topic.argsort()[:-11:-1]
            top_words = [feature_names[i] for i in top_words_idx]
            print(f"主题 #{topic_idx+1}:")
            print(", ".join(top_words))
            print()
        
        # 根据事件类型生成词云
        event_types = df['event_type'].unique()
        
        fig, axes = plt.subplots(1, len(event_types), figsize=(15, 5))
        
        for i, event_type in enumerate(event_types):
            text = " ".join(df[df['event_type'] == event_type]['processed_text'])
            
            wordcloud = WordCloud(width=800, height=400, background_color='white', max_words=100).generate(text)
            
            axes[i].imshow(wordcloud, interpolation='bilinear')
            axes[i].set_title(f'事件类型: {event_type}')
            axes[i].axis('off')
        
        plt.tight_layout()
        plt.show()
        ```
        
        ### 7. 地理空间分析
        
        **目标：** 探索设备问题的地理分布和模式
        
        **建议分析：**
        
        - **地理热点分析**
          - 绘制不良事件和召回的地理分布
          - 识别问题高发地区
          - 分析区域报告率差异
        
        - **空间聚类分析**
          - 使用空间聚类算法识别相似问题区域
          - 分析区域与设备类型的关系
          - 评估地理位置与结果严重性的关系
        
        - **区域比较分析**
          - 比较不同区域的设备问题模式
          - 分析区域监管差异的影响
          - 识别跨区域风险传播模式
        
        **示例地理分析：**
        ```python
        import pandas as pd
        import numpy as np
        import matplotlib.pyplot as plt
        import geopandas as gpd
        
        # 加载地理数据
        usa = gpd.read_file('usa_shapefile.shp')
        
        # 加载带地理信息的数据
        query = '''
            SELECT 
                reporter_state as state,
                COUNT(*) as event_count
            FROM 
                device.adverse_events
            WHERE 
                reporter_state IS NOT NULL
                AND reporter_state != ''
            GROUP BY 
                reporter_state
        '''
        state_counts = pd.read_sql(query, conn)
        
        # 合并地理数据和事件数据
        merged = usa.merge(state_counts, left_on='STATE_ABBR', right_on='state', how='left')
        merged['event_count'] = merged['event_count'].fillna(0)
        
        # 绘制地图
        fig, ax = plt.subplots(1, 1, figsize=(15, 10))
        merged.plot(column='event_count', ax=ax, legend=True, cmap='OrRd', 
                   legend_kwds={'label': "不良事件数量", 'orientation': "horizontal"})
        
        plt.title('美国各州医疗设备不良事件分布')
        plt.axis('off')
        plt.tight_layout()
        plt.show()
        
        # 计算人口调整后的率
        population_data = pd.read_csv('state_population.csv')
        merged = merged.merge(population_data, left_on='STATE_ABBR', right_on='state_abbr', how='left')
        
        # 计算每十万人口的事件率
        merged['event_rate'] = (merged['event_count'] / merged['population']) * 100000
        
        # 绘制人口调整后的地图
        fig, ax = plt.subplots(1, 1, figsize=(15, 10))
        merged.plot(column='event_rate', ax=ax, legend=True, cmap='OrRd', 
                   legend_kwds={'label': "每十万人口不良事件数", 'orientation': "horizontal"})
        
        plt.title('美国各州医疗设备不良事件率(人口调整后)')
        plt.axis('off')
        plt.tight_layout()
        plt.show()
        ```
        
        ### 8. 监管有效性分析
        
        **目标：** 评估监管行动对设备安全性的影响
        
        **建议分析：**
        
        - **政策影响评估**
          - 分析监管变化前后的不良事件和召回模式
          - 使用中断时间序列方法评估政策效果
          - 识别有效和低效的监管干预
        
        - **风险响应分析**
          - 分析从问题识别到监管响应的时间
          - 评估监管措施与减少事件的关系
          - 比较不同类型监管行动的效果
        
        - **合规表现分析**
          - 分析公司过去合规历史与不良事件的关系
          - 评估监管警告后的公司表现变化
          - 识别需要加强监管的领域
        
        **示例分析代码：**
        ```python
        import pandas as pd
        import numpy as np
        import matplotlib.pyplot as plt
        import statsmodels.api as sm
        from statsmodels.tsa.statespace.sarimax import SARIMAX
        
        # 加载监管变化前后的事件数据
        # 假设我们在2018年1月有一项重要的监管变化
        intervention_date = '2018-01-01'
        
        query = '''
            SELECT 
                DATE_TRUNC('month', date_received) as month,
                COUNT(*) as event_count
            FROM 
                device.adverse_events
            WHERE 
                date_received BETWEEN '2015-01-01' AND '2021-12-31'
            GROUP BY 
                month
            ORDER BY 
                month
        '''
        df = pd.read_sql(query, conn)
        df['month'] = pd.to_datetime(df['month'])
        df.set_index('month', inplace=True)
        
        # 创建干预标志
        df['intervention'] = df.index >= intervention_date
        
        # 中断时间序列分析
        model = SARIMAX(df['event_count'], 
                       exog=df['intervention'],
                       order=(1, 1, 1),
                       seasonal_order=(1, 1, 1, 12))
        
        results = model.fit()
        print(results.summary())
        
        # 绘制结果
        plt.figure(figsize=(12, 6))
        plt.plot(df.index, df['event_count'], label='实际事件数')
        
        # 预测结果
        pred = results.get_prediction()
        plt.plot(df.index, pred.predicted_mean, label='模型预测', color='red')
        
        # 绘制干预线
        intervention_idx = df.index.get_loc(pd.to_datetime(intervention_date))
        plt.axvline(x=pd.to_datetime(intervention_date), color='green', linestyle='--')
        plt.text(pd.to_datetime(intervention_date), df['event_count'].max() * 0.9, 
                '监管变化', rotation=90)
        
        plt.title('监管变化对不良事件报告的影响')
        plt.xlabel('日期')
        plt.ylabel('月度不良事件数')
        plt.legend()
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.show()
        ```
        """))
        
    except Exception as e:
        print(f"❌ 生成分析建议时出错: {str(e)}")

# 示例使用方法
if __name__ == "__main__":
    # 使用配置文件中的数据库连接信息
    from config import DB_CONFIG
    
    # 创建分析器实例
    analyzer = DataQualityAnalyzer(DB_CONFIG)
    
    # 连接数据库
    if analyzer.connect():
        # 运行完整分析
        analyzer.run_full_analysis()
        
        # 关闭连接
        analyzer.close()
    else:
        print("无法连接到数据库，请检查配置信息。")