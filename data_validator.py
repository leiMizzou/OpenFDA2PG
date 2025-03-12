"""
Enhanced data validator for FDA device data with duplicate record validation
"""
import os
import pandas as pd
import json
from IPython.display import display, HTML
from tqdm.notebook import tqdm
from logger import log_info, log_error, log_success, log_warning
from file_handler import FileHandler

class DataValidator:
    """验证导入的数据"""
    
    def __init__(self, conn, cur):
        """初始化数据验证器"""
        self.conn = conn
        self.cur = cur
    
    def validate(self):
        """验证导入的数据"""
        log_info("开始验证导入的数据...")
        
        # 检查主表记录数
        main_tables = [
            'medical_specialties', 'regulatory_panels', 'regulations', 'submission_types',
            'companies', 'company_contacts', 'product_codes', 'device_classifications', 
            'premarket_submissions', 'device_premarket_submissions', 'device_recalls',
            'recall_code_info', 'recall_pma_numbers', 'adverse_events', 'event_devices',
            'event_patients', 'patient_problems', 'product_problems', 'event_texts',
            'udi_records', 'udi_product_codes', 'udi_identifiers', 'udi_sterilization',
            'udi_device_sizes', 'udi_gmdn_terms', 'udi_customer_contacts', 'udi_premarket_submissions',
            'enforcement_actions', 'openfda_data', 'openfda_identifiers'
        ]
        
        try:
            # 创建一个数据框来显示表记录数
            table_counts = []
            for table in main_tables:
                self.cur.execute(f"SELECT COUNT(*) FROM device.{table}")
                count = self.cur.fetchone()[0]
                table_counts.append({"表名": table, "记录数": count})
            
            display(HTML("<h3>表记录数统计</h3>"))
            display(pd.DataFrame(table_counts).sort_values(by='记录数', ascending=False))
            
            # 检查设备类别统计
            self.cur.execute("""
                SELECT device_class, COUNT(*) as count 
                FROM device.product_codes 
                WHERE device_class IS NOT NULL
                GROUP BY device_class 
                ORDER BY count DESC
            """)
            device_class_stats = self.cur.fetchall()
            display(HTML("<h3>设备类别统计</h3>"))
            display(pd.DataFrame(device_class_stats, columns=['设备类别', '数量']))
            
            # 检查医疗专业统计
            self.cur.execute("""
                SELECT ms.code, ms.description, COUNT(pc.id) as count
                FROM device.medical_specialties ms
                LEFT JOIN device.product_codes pc ON ms.id = pc.medical_specialty_id
                GROUP BY ms.code, ms.description
                ORDER BY count DESC
            """)
            specialty_stats = self.cur.fetchall()
            display(HTML("<h3>医疗专业统计</h3>"))
            display(pd.DataFrame(specialty_stats, columns=['专业代码', '专业描述', '产品数量']))
            
            # 检查不良事件类型统计
            self.cur.execute("""
                SELECT event_type, COUNT(*) as count 
                FROM device.adverse_events 
                GROUP BY event_type 
                ORDER BY count DESC
            """)
            event_type_stats = self.cur.fetchall()
            display(HTML("<h3>不良事件类型统计</h3>"))
            display(pd.DataFrame(event_type_stats, columns=['事件类型', '数量']))
            
            # 检查召回分类统计
            self.cur.execute("""
                SELECT classification, COUNT(*) as count 
                FROM device.device_recalls 
                GROUP BY classification 
                ORDER BY count DESC
            """)
            recall_class_stats = self.cur.fetchall()
            display(HTML("<h3>召回分类统计</h3>"))
            display(pd.DataFrame(recall_class_stats, columns=['分类', '数量']))
            
            # 检查UDI类型分布
            self.cur.execute("""
                SELECT 
                    SUM(CASE WHEN is_single_use THEN 1 ELSE 0 END) as single_use_count,
                    SUM(CASE WHEN is_rx THEN 1 ELSE 0 END) as rx_count,
                    SUM(CASE WHEN is_otc THEN 1 ELSE 0 END) as otc_count,
                    SUM(CASE WHEN is_kit THEN 1 ELSE 0 END) as kit_count,
                    SUM(CASE WHEN is_combination_product THEN 1 ELSE 0 END) as combination_product_count,
                    SUM(CASE WHEN is_pm_exempt THEN 1 ELSE 0 END) as pm_exempt_count
                FROM 
                    device.udi_records
            """)
            udi_type_stats = self.cur.fetchone()
            if udi_type_stats:
                display(HTML("<h3>UDI设备类型分布</h3>"))
                display(pd.DataFrame([{
                    '一次性使用': udi_type_stats[0] or 0,
                    '处方药': udi_type_stats[1] or 0,
                    '非处方药': udi_type_stats[2] or 0,
                    '套件': udi_type_stats[3] or 0,
                    '组合产品': udi_type_stats[4] or 0,
                    'PM豁免': udi_type_stats[5] or 0
                }]))
            
            # 检查上市前提交类型分布
            self.cur.execute("""
                SELECT submission_type, COUNT(*) as count 
                FROM device.premarket_submissions 
                WHERE submission_type IS NOT NULL
                GROUP BY submission_type 
                ORDER BY count DESC
            """)
            submission_type_stats = self.cur.fetchall()
            display(HTML("<h3>上市前提交类型分布</h3>"))
            display(pd.DataFrame(submission_type_stats, columns=['提交类型', '数量']))
            
            # 连接分析 - 产品代码在不同表中的出现情况
            self.cur.execute("""
                SELECT 
                    pc.product_code,
                    pc.device_name,
                    pc.device_class,
                    ms.description as medical_specialty,
                    COUNT(DISTINCT dc.id) AS classification_count,
                    COUNT(DISTINCT dr.id) AS recall_count,
                    COUNT(DISTINCT ed.id) AS event_device_count,
                    COUNT(DISTINCT upc.id) AS udi_count
                FROM 
                    device.product_codes pc
                LEFT JOIN
                    device.medical_specialties ms ON pc.medical_specialty_id = ms.id
                LEFT JOIN
                    device.device_classifications dc ON pc.id = dc.product_code_id
                LEFT JOIN
                    device.device_recalls dr ON pc.id = dr.product_code_id
                LEFT JOIN
                    device.event_devices ed ON pc.id = ed.product_code_id
                LEFT JOIN
                    device.udi_product_codes upc ON pc.id = upc.product_code_id
                GROUP BY
                    pc.product_code, pc.device_name, pc.device_class, ms.description
                HAVING
                    COUNT(DISTINCT dc.id) > 0 
                    OR COUNT(DISTINCT dr.id) > 0 
                    OR COUNT(DISTINCT ed.id) > 0
                    OR COUNT(DISTINCT upc.id) > 0
                ORDER BY 
                    (COUNT(DISTINCT dc.id) + COUNT(DISTINCT dr.id) + 
                     COUNT(DISTINCT ed.id) + COUNT(DISTINCT upc.id)) DESC
                LIMIT 10
            """)
            
            cross_reference_stats = self.cur.fetchall()
            display(HTML("<h3>产品代码出现在多个数据源中的情况 (前10名)</h3>"))
            display(pd.DataFrame(cross_reference_stats, 
                                columns=['产品代码', '设备名称', '设备类别', '医疗专业',
                                         '分类数量', '召回数量', '不良事件数量', 'UDI记录数量']))
            
            # 检查患者问题统计
            self.cur.execute("""
                SELECT problem, COUNT(*) as count 
                FROM device.patient_problems 
                GROUP BY problem 
                ORDER BY count DESC
                LIMIT 10
            """)
            patient_problem_stats = self.cur.fetchall()
            display(HTML("<h3>常见患者问题 (前10名)</h3>"))
            display(pd.DataFrame(patient_problem_stats, columns=['患者问题', '数量']))
            
            # 检查产品问题统计
            self.cur.execute("""
                SELECT problem, COUNT(*) as count 
                FROM device.product_problems 
                GROUP BY problem 
                ORDER BY count DESC
                LIMIT 10
            """)
            product_problem_stats = self.cur.fetchall()
            display(HTML("<h3>常见产品问题 (前10名)</h3>"))
            display(pd.DataFrame(product_problem_stats, columns=['产品问题', '数量']))
            
            # 检查执法行动与召回对比
            self.cur.execute("""
                SELECT 'device_recalls' as data_source, classification, COUNT(*) as count 
                FROM device.device_recalls 
                GROUP BY classification 
                UNION ALL
                SELECT 'enforcement_actions' as data_source, classification, COUNT(*) as count 
                FROM device.enforcement_actions 
                GROUP BY classification 
                ORDER BY data_source, classification
            """)
            enforcement_recall_stats = self.cur.fetchall()
            display(HTML("<h3>执法行动与召回分类对比</h3>"))
            display(pd.DataFrame(enforcement_recall_stats, columns=['数据源', '分类', '数量']))
            
            # 检查GMDN术语分布
            self.cur.execute("""
                SELECT code, name, COUNT(*) as count 
                FROM device.udi_gmdn_terms 
                GROUP BY code, name
                ORDER BY count DESC
                LIMIT 10
            """)
            gmdn_stats = self.cur.fetchall()
            display(HTML("<h3>常见GMDN术语 (前10名)</h3>"))
            display(pd.DataFrame(gmdn_stats, columns=['GMDN代码', 'GMDN名称', '数量']))
            
            # 检查各文本类型分布
            self.cur.execute("""
                SELECT text_type_code, COUNT(*) as count 
                FROM device.event_texts 
                GROUP BY text_type_code 
                ORDER BY count DESC
            """)
            text_type_stats = self.cur.fetchall()
            display(HTML("<h3>事件文本类型分布</h3>"))
            display(pd.DataFrame(text_type_stats, columns=['文本类型', '数量']))
            
            # 时间序列分析 - 不良事件报告
            self.cur.execute("""
                SELECT 
                    date_trunc('month', date_received) as month,
                    COUNT(*) as count
                FROM 
                    device.adverse_events
                WHERE 
                    date_received IS NOT NULL
                GROUP BY 
                    month
                ORDER BY 
                    month DESC
                LIMIT 12
            """)
            event_time_stats = self.cur.fetchall()
            if event_time_stats:
                display(HTML("<h3>不良事件报告月度趋势 (近12个月)</h3>"))
                display(pd.DataFrame(event_time_stats, columns=['月份', '报告数量']))
            
            # 检查不同类型的设备事件文本
            self.cur.execute("""
                SELECT 
                    et.text_type_code,
                    LEFT(et.text, 100) as text_sample
                FROM 
                    device.event_texts et
                JOIN
                    device.adverse_events ae ON et.event_id = ae.id
                WHERE 
                    et.text IS NOT NULL
                GROUP BY 
                    et.text_type_code, LEFT(et.text, 100)
                LIMIT 5
            """)
            text_samples = self.cur.fetchall()
            display(HTML("<h3>事件文本样例</h3>"))
            display(pd.DataFrame(text_samples, columns=['文本类型', '文本样例 (前100字符)']))
            
            # NEW: 专门验证重复记录处理
            self.validate_duplicate_records()
            
            log_success("数据验证完成")
            return True
            
        except Exception as e:
            log_error(f"数据验证失败: {str(e)}")
            return False
    
    def validate_duplicate_records(self):
        """验证重复记录的处理情况"""
        display(HTML("<h3>重复记录处理验证</h3>"))
        
        # 1. 验证不良事件重复记录
        self.validate_adverse_event_duplicates()
        
        # 2. 验证执法行动重复记录
        self.validate_enforcement_duplicates()
        
        # 3. 验证召回重复记录
        self.validate_recall_duplicates()
        
        # 4. 验证UDI记录重复
        self.validate_udi_duplicates()
    
    def validate_adverse_event_duplicates(self):
        """验证不良事件重复记录的处理"""
        log_info("验证不良事件重复记录处理...")
        
        try:
            # 1. 检查report_number重复情况
            self.cur.execute("""
                WITH report_counts AS (
                    SELECT
                        report_number,
                        COUNT(*) OVER (PARTITION BY report_number) as occurrence_count
                    FROM
                        (SELECT DISTINCT report_number FROM device.adverse_events) t
                )
                SELECT
                    occurrence_count,
                    COUNT(*) as count
                FROM
                    report_counts
                GROUP BY
                    occurrence_count
                ORDER BY
                    occurrence_count
            """)
            
            results = self.cur.fetchall()
            
            if results:
                df = pd.DataFrame(results, columns=['报告出现次数', '记录数'])
                display(HTML("<h4>不良事件报告重复统计</h4>"))
                display(df)
            
            # 2. 找出有冲突值的重复记录
            self.cur.execute("""
                SELECT
                    ae1.report_number,
                    ae1.event_type,
                    ae1.report_source_code,
                    ae1.date_changed
                FROM
                    device.adverse_events ae1
                JOIN (
                    -- 找出有冲突字段的report_number
                    SELECT
                        report_number
                    FROM
                        device.adverse_events
                    GROUP BY
                        report_number
                    HAVING
                        COUNT(DISTINCT event_type) > 1
                        OR COUNT(DISTINCT report_source_code) > 1
                ) conflicts ON ae1.report_number = conflicts.report_number
                ORDER BY
                    ae1.report_number,
                    ae1.date_changed DESC NULLS LAST
                LIMIT 20
            """)
            
            conflicts = self.cur.fetchall()
            
            if conflicts:
                # 记录冲突在验证过程中是否被解决
                last_report = None
                conflict_records = []
                
                for report_number, event_type, report_source_code, date_changed in conflicts:
                    if last_report != report_number:
                        last_report = report_number
                        conflict_records.append({
                            'report_number': report_number,
                            'event_type': event_type,
                            'report_source_code': report_source_code,
                            'date_changed': date_changed
                        })
                
                if conflict_records:
                    display(HTML("<h4>不良事件冲突记录采样</h4>"))
                    display(pd.DataFrame(conflict_records))
                    log_warning(f"发现{len(conflict_records)}个不良事件冲突记录示例，新的导入处理应解决这些冲突")
            else:
                log_success("未发现不良事件重复记录冲突！")
            
        except Exception as e:
            log_error(f"验证不良事件重复记录处理失败: {str(e)}")
    
    def validate_enforcement_duplicates(self):
        """验证执法行动重复记录的处理"""
        log_info("验证执法行动重复记录处理...")
        
        try:
            # 1. 检查recall_number重复情况
            self.cur.execute("""
                WITH recall_counts AS (
                    SELECT
                        recall_number,
                        COUNT(*) OVER (PARTITION BY recall_number) as occurrence_count
                    FROM
                        (SELECT DISTINCT recall_number FROM device.enforcement_actions) t
                )
                SELECT
                    occurrence_count,
                    COUNT(*) as count
                FROM
                    recall_counts
                GROUP BY
                    occurrence_count
                ORDER BY
                    occurrence_count
            """)
            
            results = self.cur.fetchall()
            
            if results:
                df = pd.DataFrame(results, columns=['召回出现次数', '记录数'])
                display(HTML("<h4>执法行动重复统计</h4>"))
                display(df)
            
            # 2. 找出有冲突值的重复记录
            self.cur.execute("""
                SELECT
                    ea1.recall_number,
                    ea1.status,
                    ea1.classification
                FROM
                    device.enforcement_actions ea1
                JOIN (
                    -- 找出有冲突字段的recall_number
                    SELECT
                        recall_number
                    FROM
                        device.enforcement_actions
                    GROUP BY
                        recall_number
                    HAVING
                        COUNT(DISTINCT status) > 1
                        OR COUNT(DISTINCT classification) > 1
                ) conflicts ON ea1.recall_number = conflicts.recall_number
                ORDER BY
                    ea1.recall_number
                LIMIT 20
            """)
            
            conflicts = self.cur.fetchall()
            
            if conflicts:
                # 构建冲突记录信息
                conflict_records = []
                
                for recall_number, status, classification in conflicts:
                    conflict_records.append({
                        'recall_number': recall_number,
                        'status': status,
                        'classification': classification
                    })
                
                if conflict_records:
                    display(HTML("<h4>执法行动冲突记录采样</h4>"))
                    display(pd.DataFrame(conflict_records))
                    log_warning(f"发现{len(conflict_records)}个执法行动冲突记录示例，新的导入处理应解决这些冲突")
            else:
                log_success("未发现执法行动重复记录冲突！")
            
        except Exception as e:
            log_error(f"验证执法行动重复记录处理失败: {str(e)}")
    
    def validate_recall_duplicates(self):
        """验证召回记录重复处理"""
        log_info("验证召回记录重复处理...")
        
        try:
            # 与上面类似的实现，针对device_recalls表
            # 1. 检查recall_number重复情况
            self.cur.execute("""
                WITH recall_counts AS (
                    SELECT
                        recall_number,
                        COUNT(*) OVER (PARTITION BY recall_number) as occurrence_count
                    FROM
                        (SELECT DISTINCT recall_number FROM device.device_recalls) t
                )
                SELECT
                    occurrence_count,
                    COUNT(*) as count
                FROM
                    recall_counts
                GROUP BY
                    occurrence_count
                ORDER BY
                    occurrence_count
            """)
            
            results = self.cur.fetchall()
            
            if results:
                df = pd.DataFrame(results, columns=['召回出现次数', '记录数'])
                display(HTML("<h4>设备召回重复统计</h4>"))
                display(df)
            
            # 2. 找出有冲突值的重复记录
            self.cur.execute("""
                SELECT
                    dr1.recall_number,
                    dr1.status,
                    dr1.classification
                FROM
                    device.device_recalls dr1
                JOIN (
                    -- 找出有冲突字段的recall_number
                    SELECT
                        recall_number
                    FROM
                        device.device_recalls
                    GROUP BY
                        recall_number
                    HAVING
                        COUNT(DISTINCT status) > 1
                        OR COUNT(DISTINCT classification) > 1
                ) conflicts ON dr1.recall_number = conflicts.recall_number
                ORDER BY
                    dr1.recall_number
                LIMIT 20
            """)
            
            conflicts = self.cur.fetchall()
            
            if conflicts:
                # 构建冲突记录信息
                conflict_records = []
                
                for recall_number, status, classification in conflicts:
                    conflict_records.append({
                        'recall_number': recall_number,
                        'status': status,
                        'classification': classification
                    })
                
                if conflict_records:
                    display(HTML("<h4>设备召回冲突记录采样</h4>"))
                    display(pd.DataFrame(conflict_records))
                    log_warning(f"发现{len(conflict_records)}个设备召回冲突记录示例，新的导入处理应解决这些冲突")
            else:
                log_success("未发现设备召回重复记录冲突！")
                
        except Exception as e:
            log_error(f"验证召回记录重复处理失败: {str(e)}")
    
    def validate_udi_duplicates(self):
        """验证UDI记录重复处理"""
        log_info("验证UDI记录重复处理...")
        
        try:
            # 1. 检查public_device_record_key重复情况
            self.cur.execute("""
                WITH udi_counts AS (
                    SELECT
                        public_device_record_key,
                        COUNT(*) OVER (PARTITION BY public_device_record_key) as occurrence_count
                    FROM
                        (SELECT DISTINCT public_device_record_key FROM device.udi_records) t
                )
                SELECT
                    occurrence_count,
                    COUNT(*) as count
                FROM
                    udi_counts
                GROUP BY
                    occurrence_count
                ORDER BY
                    occurrence_count
            """)
            
            results = self.cur.fetchall()
            
            if results:
                df = pd.DataFrame(results, columns=['UDI出现次数', '记录数'])
                display(HTML("<h4>UDI记录重复统计</h4>"))
                display(df)
            
            # 2. 找出有冲突值的重复记录
            self.cur.execute("""
                SELECT
                    ur1.public_device_record_key,
                    ur1.record_status,
                    ur1.public_version_number
                FROM
                    device.udi_records ur1
                JOIN (
                    -- 找出有冲突字段的public_device_record_key
                    SELECT
                        public_device_record_key
                    FROM
                        device.udi_records
                    GROUP BY
                        public_device_record_key
                    HAVING
                        COUNT(DISTINCT record_status) > 1
                        OR COUNT(DISTINCT public_version_number) > 1
                ) conflicts ON ur1.public_device_record_key = conflicts.public_device_record_key
                ORDER BY
                    ur1.public_device_record_key
                LIMIT 20
            """)
            
            conflicts = self.cur.fetchall()
            
            if conflicts:
                # 构建冲突记录信息
                conflict_records = []
                
                for record_key, record_status, version_number in conflicts:
                    conflict_records.append({
                        'public_device_record_key': record_key,
                        'record_status': record_status,
                        'public_version_number': version_number
                    })
                
                if conflict_records:
                    display(HTML("<h4>UDI记录冲突采样</h4>"))
                    display(pd.DataFrame(conflict_records))
                    log_warning(f"发现{len(conflict_records)}个UDI记录冲突示例，新的导入处理应解决这些冲突")
            else:
                log_success("未发现UDI记录重复冲突！")
                
        except Exception as e:
            log_error(f"验证UDI记录重复处理失败: {str(e)}")
    
    def compare_source_vs_db_counts(self, source_data_counts):
        """比较源数据与数据库记录数"""
        display(HTML("<h3>源数据与数据库记录数对比</h3>"))
        
        try:
            # 获取数据库中各表的记录数
            self.cur.execute("""
                SELECT 'device_classifications' as table_name, COUNT(*) as db_count FROM device.device_classifications
                UNION ALL
                SELECT 'enforcement_actions' as table_name, COUNT(*) as db_count FROM device.enforcement_actions
                UNION ALL
                SELECT 'device_recalls' as table_name, COUNT(*) as db_count FROM device.device_recalls
                UNION ALL
                SELECT 'adverse_events' as table_name, COUNT(*) as db_count FROM device.adverse_events
                UNION ALL
                SELECT 'udi_records' as table_name, COUNT(*) as db_count FROM device.udi_records
            """)
            
            db_counts = {row[0]: row[1] for row in self.cur.fetchall()}
            
            # 构建比较数据
            comparison_data = []
            total_source = 0
            total_db = 0
            total_diff = 0
            
            for table_name, source_count in source_data_counts.items():
                db_count = db_counts.get(table_name, 0)
                diff = source_count - db_count
                diff_percent = (diff / source_count * 100) if source_count > 0 else 0
                
                comparison_data.append({
                    '数据表': table_name,
                    '源数据记录数': source_count,
                    '数据库记录数': db_count,
                    '差异数': diff,
                    '差异百分比': f"{diff_percent:.2f}%"
                })
                
                total_source += source_count
                total_db += db_count
                total_diff += diff
            
            # 添加总计行
            total_diff_percent = (total_diff / total_source * 100) if total_source > 0 else 0
            comparison_data.append({
                '数据表': '总计',
                '源数据记录数': total_source,
                '数据库记录数': total_db,
                '差异数': total_diff,
                '差异百分比': f"{total_diff_percent:.2f}%"
            })
            
            display(pd.DataFrame(comparison_data))
            
            # 输出差异分析结果
            if total_diff > 0:
                log_info(f"源数据与数据库记录差异为 {total_diff} 条记录 ({total_diff_percent:.2f}%)，" + 
                         "这可能是由于源数据中存在重复记录，而数据库通过唯一键约束进行了去重。")
                log_info("新的导入处理逻辑应确保在处理重复记录时正确地更新所有字段，以保持数据一致性。")
            else:
                log_success("源数据与数据库记录数匹配！")
            
        except Exception as e:
            log_error(f"比较源数据与数据库记录数失败: {str(e)}")