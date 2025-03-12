"""
Recall data importer
"""
import os
from tqdm.notebook import tqdm
from file_handler import FileHandler
from logger import log_info, log_error, log_success, log_warning
from importers.base_importer import BaseImporter
from utils import parse_date, parse_code_info, convert_to_array

class RecallImporter(BaseImporter):
    """处理设备召回数据导入"""
    
    def import_data(self, files, batch_size=100):
        """导入设备召回数据"""
        total_processed = 0
        
        try:
            # 遍历每个召回文件
            for file_path in tqdm(files, desc="处理设备召回文件"):
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
                    
                    # 使用事务处理导入
                    try:
                        with self.conn:
                            for recall in batch:
                                # 修改这里: 使用product_res_number作为主要标识符，回退到recall_number
                                recall_number = recall.get('product_res_number') or recall.get('recall_number')
                                if not recall_number:
                                    continue
                                
                                # 提取基本识别信息
                                cfres_id = recall.get('cfres_id')
                                res_event_number = recall.get('res_event_number')
                                status = recall.get('status') or recall.get('recall_status')
                                classification = recall.get('classification')
                                product_code = recall.get('product_code')
                                product_type = recall.get('product_type')
                                event_id = recall.get('event_id')
                                
                                # 处理日期
                                event_date_initiated = parse_date(recall.get('event_date_initiated'))
                                event_date_posted = parse_date(recall.get('event_date_posted'))
                                recall_initiation_date = parse_date(recall.get('recall_initiation_date'))
                                center_classification_date = parse_date(recall.get('center_classification_date'))
                                report_date = parse_date(recall.get('report_date'))
                                
                                # 提取公司信息
                                recalling_firm = recall.get('recalling_firm')
                                address_1 = recall.get('address_1')
                                address_2 = recall.get('address_2')
                                city = recall.get('city')
                                state = recall.get('state')
                                postal_code = recall.get('postal_code')
                                country = recall.get('country')
                                
                                # 提取召回信息
                                voluntary_mandated = recall.get('voluntary_mandated')
                                initial_firm_notification = recall.get('initial_firm_notification')
                                product_description = recall.get('product_description')
                                product_quantity = recall.get('product_quantity')
                                code_info = recall.get('code_info')
                                reason_for_recall = recall.get('reason_for_recall')
                                root_cause_description = recall.get('root_cause_description')
                                action = recall.get('action')
                                distribution_pattern = recall.get('distribution_pattern')
                                additional_info_contact = recall.get('additional_info_contact')
                                
                                # 提取PMA号码数组
                                pma_numbers = []
                                if 'pma_numbers' in recall and recall['pma_numbers']:
                                    pma_numbers = convert_to_array(recall['pma_numbers'])
                                
                                # 提取OpenFDA数据
                                openfda = recall.get('openfda', {})
                                
                                # 处理公司
                                company_details = {
                                    'address_line_1': address_1,
                                    'address_line_2': address_2,
                                    'city': city,
                                    'state': state,
                                    'postal_code': postal_code,
                                    'country': country
                                }
                                company_id = self.get_or_create_company(recalling_firm, company_details)
                                
                                # 处理联系信息
                                if additional_info_contact:
                                    contact_lines = additional_info_contact.split('\n')
                                    for line in contact_lines:
                                        line = line.strip()
                                        if not line:
                                            continue
                                            
                                        # 尝试识别联系信息类型
                                        if any(phone_char in line for phone_char in ['(', ')', '-']):  # 可能是电话号码
                                            self.add_company_contact(company_id, 'phone', line)
                                        elif '@' in line:  # 电子邮件
                                            self.add_company_contact(company_id, 'email', line)
                                        else:
                                            self.add_company_contact(company_id, 'other', line)
                                
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
                                
                                # 插入召回
                                self.cur.execute(
                                    """
                                    INSERT INTO device.device_recalls (
                                        recall_number, cfres_id, res_event_number, status, classification,
                                        product_code, product_code_id, product_type, event_id, event_date_initiated,
                                        event_date_posted, recall_initiation_date, center_classification_date,
                                        report_date, recalling_firm, company_id, address_1, address_2, city,
                                        state, postal_code, country, voluntary_mandated, initial_firm_notification,
                                        product_description, product_quantity, code_info, reason_for_recall,
                                        root_cause_description, action, distribution_pattern, additional_info_contact
                                    ) VALUES (
                                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                                    )
                                    ON CONFLICT (recall_number) DO UPDATE SET 
                                        status = EXCLUDED.status,
                                        classification = EXCLUDED.classification,
                                        event_date_posted = EXCLUDED.event_date_posted,
                                        center_classification_date = EXCLUDED.center_classification_date,
                                        report_date = EXCLUDED.report_date
                                    RETURNING id
                                    """,
                                    (
                                        recall_number, cfres_id, res_event_number, status, classification,
                                        product_code, product_code_id, product_type, event_id, event_date_initiated,
                                        event_date_posted, recall_initiation_date, center_classification_date,
                                        report_date, recalling_firm, company_id, address_1, address_2, city,
                                        state, postal_code, country, voluntary_mandated, initial_firm_notification,
                                        product_description, product_quantity, code_info, reason_for_recall,
                                        root_cause_description, action, distribution_pattern, additional_info_contact
                                    )
                                )
                                
                                recall_id = self.cur.fetchone()[0]
                                
                                # 存储OpenFDA数据
                                self.store_openfda_data(recall_id, 'device_recalls', openfda)
                                
                                # 处理结构化code_info
                                if code_info:
                                    parsed_codes = parse_code_info(code_info)
                                    for code_item in parsed_codes:
                                        self.cur.execute(
                                            """
                                            INSERT INTO device.recall_code_info (
                                                recall_id, item_type, item_value
                                            ) VALUES (%s, %s, %s)
                                            ON CONFLICT DO NOTHING
                                            """,
                                            (recall_id, code_item['item_type'], code_item['item_value'])
                                        )
                                
                                # 处理PMA号码
                                if pma_numbers:
                                    for pma_number in pma_numbers:
                                        if pma_number:
                                            # 创建premarket submission
                                            submission_id = self.get_or_create_premarket_submission(pma_number, 'PMA')
                                            
                                            # 关联到召回
                                            self.cur.execute(
                                                """
                                                INSERT INTO device.recall_pma_numbers (
                                                    recall_id, pma_number, submission_id
                                                ) VALUES (%s, %s, %s)
                                                ON CONFLICT (recall_id, pma_number) DO NOTHING
                                                """,
                                                (recall_id, pma_number, submission_id)
                                            )
                                            
                                            # 关联设备和提交
                                            self.link_device_to_submission(recall_id, 'device_recalls', submission_id)
                                
                                batch_processed += 1
                            
                        log_info(f"已处理文件 {os.path.basename(file_path)} 的第 {batch_idx+1}/{len(batches)} 批, {batch_processed} 条记录")
                        total_processed += batch_processed
                    
                    except Exception as e:
                        self.conn.rollback()
                        log_error(f"处理召回数据批次 {batch_idx+1} 失败: {str(e)}")
                        
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
            self.update_metadata('device_recalls', total_processed)
            log_success(f"设备召回数据导入完成，共处理 {total_processed} 条记录")
            return total_processed
            
        except Exception as e:
            self.conn.rollback()
            log_error(f"导入设备召回数据失败: {str(e)}")
            raise