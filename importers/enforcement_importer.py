"""
Enforcement action data importer
"""
import os
from tqdm.notebook import tqdm
from file_handler import FileHandler
from logger import log_info, log_error, log_success, log_warning
from importers.base_importer import BaseImporter
from utils import parse_date

class EnforcementImporter(BaseImporter):
    """处理执法行动数据导入"""
    
    def import_data(self, files, batch_size=100):
        """导入执法行动数据"""
        total_processed = 0
        conflict_count = 0  # Track conflicts for monitoring
        
        try:
            # 遍历每个执法行动文件
            for file_path in tqdm(files, desc="处理执法行动文件"):
                data = FileHandler.load_json(file_path)
                
                if not data or 'results' not in data:
                    log_warning(f"文件格式无效: {file_path}")
                    continue
                
                results = data['results']
                total_results = len(results)
                batches = [results[i:i + batch_size] for i in range(0, total_results, batch_size)]
                
                log_info(f"开始处理文件 {os.path.basename(file_path)}, 共 {total_results} 条记录, 分 {len(batches)} 批")
                
                # 按批次处理数据
                for batch_idx, batch in enumerate(batches):
                    batch_processed = 0
                    batch_conflicts = 0  # Count conflicts per batch
                    
                    # 使用事务处理导入
                    try:
                        with self.conn:
                            for enforcement in batch:
                                # 修改这里: 直接使用recall_number
                                recall_number = enforcement.get('recall_number')
                                if not recall_number:
                                    continue
                                
                                # 提取基本识别信息 - 注意执法行动可能有不同的字段名
                                event_id = enforcement.get('event_id')
                                status = enforcement.get('status', enforcement.get('recall_status'))
                                classification = enforcement.get('classification')
                                product_code = enforcement.get('product_code')
                                product_type = enforcement.get('product_type')
                                
                                # 处理日期
                                event_date_initiated = parse_date(enforcement.get('event_date_initiated'))
                                event_date_posted = parse_date(enforcement.get('event_date_posted'))
                                enforcement_initiation_date = parse_date(enforcement.get('recall_initiation_date', 
                                                                                      enforcement.get('enforcement_initiation_date',
                                                                                                  enforcement.get('event_date_initiated'))))
                                center_classification_date = parse_date(enforcement.get('center_classification_date'))
                                report_date = parse_date(enforcement.get('report_date'))
                                
                                # 提取公司信息
                                firm_name = enforcement.get('recalling_firm', enforcement.get('firm_name', ''))
                                address_1 = enforcement.get('address_1')
                                address_2 = enforcement.get('address_2')
                                city = enforcement.get('city')
                                state = enforcement.get('state')
                                postal_code = enforcement.get('postal_code')
                                country = enforcement.get('country')
                                
                                # 提取执法行动信息
                                voluntary_mandated = enforcement.get('voluntary_mandated')
                                initial_firm_notification = enforcement.get('initial_firm_notification')
                                product_description = enforcement.get('product_description')
                                action = enforcement.get('action')
                                distribution_pattern = enforcement.get('distribution_pattern')
                                code_info = enforcement.get('code_info')
                                reason_for_recall = enforcement.get('reason_for_recall')
                                
                                # 提取OpenFDA数据
                                openfda = enforcement.get('openfda', {})
                                
                                # 处理公司
                                company_details = {
                                    'address_line_1': address_1,
                                    'address_line_2': address_2,
                                    'city': city,
                                    'state': state,
                                    'postal_code': postal_code,
                                    'country': country
                                }
                                company_id = self.get_or_create_company(firm_name, company_details)
                                
                                # 处理产品代码
                                product_code_id = None
                                if product_code:
                                    # 从openfda中提取适合product_codes表的数据，排除k_number和pma_number等不适合的字段
                                    product_code_data = {}
                                    if openfda:
                                        # 只复制适合product_codes表的字段
                                        if 'device_name' in openfda:
                                            product_code_data['device_name'] = openfda['device_name']
                                        if 'device_class' in openfda:
                                            product_code_data['device_class'] = openfda['device_class']
                                        if 'regulation_number' in openfda:
                                            product_code_data['regulation_number'] = openfda['regulation_number']
                                        if 'medical_specialty_description' in openfda:
                                            product_code_data['medical_specialty_description'] = openfda['medical_specialty_description']
                                    
                                    product_code_id = self.get_or_create_product_code(
                                        product_code, product_description, product_code_data
                                    )
                                
                                # SOLUTION: Check for existing record before insert
                                self.cur.execute(
                                    """
                                    SELECT id, status, classification, 
                                           event_date_posted, center_classification_date, report_date
                                    FROM device.enforcement_actions 
                                    WHERE recall_number = %s
                                    """, 
                                    (recall_number,)
                                )
                                existing_record = self.cur.fetchone()
                                
                                if existing_record:
                                    # Found existing record - check if it has different data
                                    (existing_id, existing_status, existing_classification, 
                                     existing_date_posted, existing_center_date, existing_report_date) = existing_record
                                    
                                    # SOLUTION: Log conflicts and update with all fields
                                    has_conflict = False
                                    if (existing_status != status or 
                                        existing_classification != classification):
                                        has_conflict = True
                                        batch_conflicts += 1
                                        log_warning(f"Data conflict for enforcement {recall_number}: " +
                                            f"status: {existing_status}->{status}, " +
                                            f"classification: {existing_classification}->{classification}")
                                    
                                    # Update the record with ALL fields regardless of conflicts
                                    # This ensures all data is consistent
                                    self.cur.execute(
                                        """
                                        UPDATE device.enforcement_actions SET
                                            status = %s,
                                            classification = %s,
                                            product_code = %s,
                                            product_code_id = %s,
                                            product_type = %s,
                                            event_id = %s,
                                            event_date_initiated = %s,
                                            event_date_posted = %s,
                                            enforcement_initiation_date = %s,
                                            center_classification_date = %s,
                                            report_date = %s,
                                            firm_name = %s,
                                            company_id = %s,
                                            address_1 = %s,
                                            address_2 = %s,
                                            city = %s,
                                            state = %s,
                                            postal_code = %s,
                                            country = %s,
                                            voluntary_mandated = %s,
                                            initial_firm_notification = %s,
                                            product_description = %s,
                                            action = %s,
                                            distribution_pattern = %s,
                                            code_info = %s,
                                            reason_for_recall = %s
                                        WHERE id = %s
                                        RETURNING id
                                        """,
                                        (
                                            status, classification, product_code, product_code_id,
                                            product_type, event_id, event_date_initiated, event_date_posted,
                                            enforcement_initiation_date, center_classification_date, report_date,
                                            firm_name, company_id, address_1, address_2, city, state, postal_code,
                                            country, voluntary_mandated, initial_firm_notification, product_description,
                                            action, distribution_pattern, code_info, reason_for_recall, existing_id
                                        )
                                    )
                                    enforcement_id = self.cur.fetchone()[0]
                                else:
                                    # 插入执法行动 - 修改字段名以匹配数据库结构
                                    self.cur.execute(
                                        """
                                        INSERT INTO device.enforcement_actions (
                                            recall_number, status, classification, product_code, product_code_id,
                                            product_type, event_id, event_date_initiated, event_date_posted,
                                            enforcement_initiation_date, center_classification_date, report_date,
                                            firm_name, company_id, address_1, address_2, city, state, postal_code,
                                            country, voluntary_mandated, initial_firm_notification, product_description,
                                            action, distribution_pattern, code_info, reason_for_recall
                                        ) VALUES (
                                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                            %s, %s, %s, %s, %s, %s, %s, %s
                                        )
                                        RETURNING id
                                        """,
                                        (
                                            recall_number, status, classification, product_code, product_code_id,
                                            product_type, event_id, event_date_initiated, event_date_posted,
                                            enforcement_initiation_date, center_classification_date, report_date,
                                            firm_name, company_id, address_1, address_2, city, state, postal_code,
                                            country, voluntary_mandated, initial_firm_notification, product_description,
                                            action, distribution_pattern, code_info, reason_for_recall
                                        )
                                    )
                                    
                                    enforcement_id = self.cur.fetchone()[0]
                                
                                # 存储OpenFDA数据
                                self.store_openfda_data(enforcement_id, 'enforcement_actions', openfda)
                                
                                batch_processed += 1
                            
                        conflict_count += batch_conflicts
                        log_info(f"已处理文件 {os.path.basename(file_path)} 的第 {batch_idx+1}/{len(batches)} 批, "
                                f"{batch_processed} 条记录, {batch_conflicts} 条冲突")
                        total_processed += batch_processed
                    
                    except Exception as e:
                        self.conn.rollback()
                        log_error(f"处理执法行动数据批次 {batch_idx+1} 失败: {str(e)}")
                        
                        # 尝试重置连接状态
                        try:
                            # 检查连接状态并尝试重置
                            if self.conn.closed == 0:  # 连接未关闭
                                self.conn.rollback()
                                log_info("已重置数据库连接状态")
                        except Exception as reset_error:
                            log_error(f"重置连接状态失败，尝试重新连接: {str(reset_error)}")
                            self.close()
                            if not self.connect():
                                log_error("无法重新连接数据库，中止导入")
                                return total_processed
            
            # 更新元数据
            self.update_metadata('enforcement_actions', total_processed)
            log_success(f"执法行动数据导入完成，共处理 {total_processed} 条记录，解决 {conflict_count} 条冲突")
            return total_processed
            
        except Exception as e:
            self.conn.rollback()
            log_error(f"导入执法行动数据失败: {str(e)}")
            raise