"""
Adverse event data importer
"""
import os
from tqdm.notebook import tqdm
from file_handler import FileHandler
from logger import log_info, log_error, log_success, log_warning
from importers.base_importer import BaseImporter
from utils import parse_date, parse_boolean, convert_to_array

class AdverseEventImporter(BaseImporter):
    """处理不良事件数据导入"""
    
    def import_data(self, files, batch_size=100):
        """导入不良事件数据"""
        total_processed = 0
        conflict_count = 0  # Track conflicts for monitoring
        
        try:
            # 遍历每个事件文件
            for file_path in tqdm(files, desc="处理不良事件文件"):
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
                        with self.conn:  # 每批使用单独的事务
                            for event in batch:
                                report_number = event.get('report_number')
                                if not report_number:
                                    continue
                                
                                # 提取基本事件数据
                                mdr_report_key = event.get('mdr_report_key')
                                event_type = event.get('event_type')
                                event_key = event.get('event_key', '')
                                date_received = parse_date(event.get('date_received'))
                                date_of_event = parse_date(event.get('date_of_event'))
                                date_report = parse_date(event.get('date_report'))
                                date_manufacturer_received = parse_date(event.get('date_manufacturer_received'))
                                date_added = parse_date(event.get('date_added'))
                                date_changed = parse_date(event.get('date_changed'))
                                
                                # 提取报告者信息
                                reporter_occupation_code = event.get('reporter_occupation_code')
                                reporter_country_code = event.get('reporter_country_code')
                                health_professional = parse_boolean(event.get('health_professional'))
                                report_source_code = event.get('report_source_code')
                                
                                # 提取制造商信息
                                manufacturer_name = event.get('manufacturer_name')
                                manufacturer_link_flag = parse_boolean(event.get('manufacturer_link_flag'))
                                
                                # 提取其他元数据
                                summary_report_flag = parse_boolean(event.get('summary_report_flag'))
                                pma_pmn_number = event.get('pma_pmn_number')
                                previous_use_code = event.get('previous_use_code')
                                removal_correction_number = event.get('removal_correction_number')
                                single_use_flag = parse_boolean(event.get('single_use_flag'))
                                reprocessed_and_reused_flag = parse_boolean(event.get('reprocessed_and_reused_flag'))
                                type_of_report = convert_to_array(event.get('type_of_report'))
                                adverse_event_flag = parse_boolean(event.get('adverse_event_flag'))
                                product_problem_flag = parse_boolean(event.get('product_problem_flag'))
                                
                                # 处理公司
                                # 收集可用的制造商地址信息
                                manufacturer_details = {}
                                for field in ['manufacturer_address_1', 'manufacturer_address_2', 'manufacturer_city', 
                                            'manufacturer_state', 'manufacturer_postal_code', 'manufacturer_country']:
                                    if field in event and event[field]:
                                        key = field.replace('manufacturer_', '')
                                        manufacturer_details[key] = event[field]
                                
                                company_id = self.get_or_create_company(manufacturer_name, manufacturer_details)
                                
                                # 处理上市前提交号
                                submission_id = None
                                if pma_pmn_number:
                                    # 尝试确定提交类型
                                    if pma_pmn_number.startswith('P'):
                                        submission_type = 'PMA'
                                    elif pma_pmn_number.startswith('K'):
                                        submission_type = '510(k)'
                                    else:
                                        submission_type = None
                                        
                                    submission_id = self.get_or_create_premarket_submission(
                                        pma_pmn_number, submission_type
                                    )
                                
                                # SOLUTION 1: Check for existing record before insert
                                self.cur.execute(
                                    """
                                    SELECT id, event_type, report_source_code, date_changed 
                                    FROM device.adverse_events 
                                    WHERE report_number = %s
                                    """, 
                                    (report_number,)
                                )
                                existing_record = self.cur.fetchone()
                                
                                if existing_record:
                                    # Found existing record - check if it has newer data
                                    existing_id, existing_event_type, existing_source_code, existing_date_changed = existing_record
                                    
                                    # SOLUTION 2: Use date_changed to determine which record is newer
                                    update_record = True
                                    if existing_date_changed and date_changed:
                                        if existing_date_changed > date_changed:
                                            # Skip update if existing record is newer
                                            update_record = False
                                    
                                    if update_record:
                                        # Log conflicts for monitoring
                                        if (existing_event_type != event_type or 
                                            existing_source_code != report_source_code):
                                            batch_conflicts += 1
                                            log_warning(f"Data conflict for report {report_number}: " +
                                                f"event_type: {existing_event_type}->{event_type}, " +
                                                f"report_source_code: {existing_source_code}->{report_source_code}")
                                            
                                        # Update existing record with current data
                                        self.cur.execute(
                                            """
                                            UPDATE device.adverse_events SET
                                                event_type = %s,
                                                date_changed = %s,
                                                manufacturer_link_flag = %s,
                                                type_of_report = %s,
                                                report_source_code = %s
                                            WHERE id = %s
                                            RETURNING id
                                            """,
                                            (
                                                event_type, date_changed, manufacturer_link_flag,
                                                type_of_report, report_source_code, existing_id
                                            )
                                        )
                                        event_id = self.cur.fetchone()[0]
                                    else:
                                        # Skip update but use existing ID
                                        event_id = existing_id
                                else:
                                    # Insert new record
                                    self.cur.execute(
                                        """
                                        INSERT INTO device.adverse_events (
                                            report_number, mdr_report_key, event_type, event_key,
                                            date_received, date_of_event, date_report, date_manufacturer_received,
                                            date_added, date_changed, reporter_occupation_code, reporter_country_code,
                                            health_professional, report_source_code, manufacturer_name, company_id,
                                            manufacturer_link_flag, summary_report_flag, pma_pmn_number, submission_id,
                                            previous_use_code, removal_correction_number, single_use_flag,
                                            reprocessed_and_reused_flag, type_of_report, adverse_event_flag,
                                            product_problem_flag
                                        ) VALUES (
                                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                            %s, %s, %s, %s, %s, %s, %s, %s, %s
                                        )
                                        RETURNING id
                                        """,
                                        (
                                            report_number, mdr_report_key, event_type, event_key,
                                            date_received, date_of_event, date_report, date_manufacturer_received,
                                            date_added, date_changed, reporter_occupation_code, reporter_country_code,
                                            health_professional, report_source_code, manufacturer_name, company_id,
                                            manufacturer_link_flag, summary_report_flag, pma_pmn_number, submission_id,
                                            previous_use_code, removal_correction_number, single_use_flag,
                                            reprocessed_and_reused_flag, type_of_report, adverse_event_flag,
                                            product_problem_flag
                                        )
                                    )
                                    event_id = self.cur.fetchone()[0]
                                
                                # 关联提交
                                if submission_id:
                                    self.link_device_to_submission(event_id, 'adverse_events', submission_id)
                                
                                # 处理设备数据 - 可能有多个设备
                                if 'device' in event and event['device']:
                                    devices = event['device']
                                    if not isinstance(devices, list):
                                        devices = [devices]
                                        
                                    for device in devices:
                                        # [Device processing code remains unchanged]
                                        # 提取设备信息
                                        device_sequence_number = device.get('device_sequence_number')
                                        brand_name = device.get('brand_name')
                                        generic_name = device.get('generic_name')
                                        manufacturer_name = device.get('manufacturer_d_name')
                                        manufacturer_address_1 = device.get('manufacturer_d_address_1')
                                        manufacturer_address_2 = device.get('manufacturer_d_address_2')
                                        manufacturer_city = device.get('manufacturer_d_city')
                                        manufacturer_state = device.get('manufacturer_d_state')
                                        manufacturer_postal_code = device.get('manufacturer_d_postal_code')
                                        manufacturer_country = device.get('manufacturer_d_country')
                                        manufacturer_zip_code = device.get('manufacturer_d_zip_code')
                                        manufacturer_zip_code_ext = device.get('manufacturer_d_zip_code_ext')
                                        model_number = device.get('model_number')
                                        catalog_number = device.get('catalog_number')
                                        lot_number = device.get('lot_number')
                                        expiration_date = parse_date(device.get('expiration_date_of_device'))
                                        device_availability = device.get('device_availability')
                                        device_operator = device.get('device_operator')
                                        device_age_text = device.get('device_age_text')
                                        device_evaluated = device.get('device_evaluated_by_manufacturer')
                                        implant_flag = parse_boolean(device.get('implant_flag'))
                                        product_code = device.get('device_report_product_code')
                                        udi_di = device.get('udi_di')
                                        udi_public = device.get('udi_public')
                                        openfda = device.get('openfda', {})
                                        
                                        # 处理产品代码
                                        product_code_id = None
                                        if product_code:
                                            device_name = brand_name or generic_name or ''
                                            product_code_id = self.get_or_create_product_code(
                                                product_code, device_name, openfda
                                            )
                                        
                                        # 将设备插入到事件设备表
                                        self.cur.execute(
                                            """
                                            INSERT INTO device.event_devices (
                                                event_id, device_sequence_number, brand_name, generic_name,
                                                manufacturer_name, manufacturer_address_1, manufacturer_address_2,
                                                manufacturer_city, manufacturer_state, manufacturer_postal_code,
                                                manufacturer_country, manufacturer_zip_code, manufacturer_zip_code_ext,
                                                model_number, catalog_number, lot_number, expiration_date,
                                                device_availability, device_operator, device_age_text,
                                                device_evaluated_by_manufacturer, implant_flag, product_code,
                                                product_code_id, udi_di, udi_public
                                            ) VALUES (
                                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                                            )
                                            RETURNING id
                                            """,
                                            (
                                                event_id, device_sequence_number, brand_name, generic_name,
                                                manufacturer_name, manufacturer_address_1, manufacturer_address_2,
                                                manufacturer_city, manufacturer_state, manufacturer_postal_code,
                                                manufacturer_country, manufacturer_zip_code, manufacturer_zip_code_ext,
                                                model_number, catalog_number, lot_number, expiration_date,
                                                device_availability, device_operator, device_age_text,
                                                device_evaluated, implant_flag, product_code,
                                                product_code_id, udi_di, udi_public
                                            )
                                        )
                                        
                                        device_id = self.cur.fetchone()[0]
                                        
                                        # 存储OpenFDA数据
                                        if openfda:
                                            self.store_openfda_data(device_id, 'event_devices', openfda)
                                
                                # 处理患者数据 - 可能有多个患者
                                if 'patient' in event and event['patient']:
                                    patients = event['patient']
                                    if not isinstance(patients, list):
                                        patients = [patients]
                                        
                                    for patient in patients:
                                        patient_sequence_number = patient.get('patient_sequence_number')
                                        
                                        # 插入患者记录
                                        self.cur.execute(
                                            """
                                            INSERT INTO device.event_patients (
                                                event_id, patient_sequence_number
                                            ) VALUES (%s, %s)
                                            RETURNING id
                                            """,
                                            (event_id, patient_sequence_number)
                                        )
                                        
                                        patient_id = self.cur.fetchone()[0]
                                        
                                        # 处理患者问题
                                        if 'patient_problems' in patient and patient['patient_problems']:
                                            problems = patient['patient_problems']
                                            if not isinstance(problems, list):
                                                problems = [problems]
                                                
                                            for problem in problems:
                                                if problem:
                                                    self.cur.execute(
                                                        """
                                                        INSERT INTO device.patient_problems (
                                                            patient_id, problem
                                                        ) VALUES (%s, %s)
                                                        """,
                                                        (patient_id, problem)
                                                    )
                                
                                # 处理产品问题
                                if 'product_problems' in event and event['product_problems']:
                                    problems = event['product_problems']
                                    if not isinstance(problems, list):
                                        problems = [problems]
                                        
                                    for problem in problems:
                                        if problem:
                                            self.cur.execute(
                                                """
                                                INSERT INTO device.product_problems (
                                                    event_id, problem
                                                ) VALUES (%s, %s)
                                                """,
                                                (event_id, problem)
                                            )
                                
                                # 处理事件文本
                                if 'mdr_text' in event and event['mdr_text']:
                                    texts = event['mdr_text']
                                    if not isinstance(texts, list):
                                        texts = [texts]
                                        
                                    for text_item in texts:
                                        text_type_code = text_item.get('text_type_code')
                                        text = text_item.get('text')
                                        patient_sequence_number = text_item.get('patient_sequence_number')
                                        
                                        if text and text_type_code:
                                            self.cur.execute(
                                                """
                                                INSERT INTO device.event_texts (
                                                    event_id, text_type_code, text, patient_sequence_number
                                                ) VALUES (%s, %s, %s, %s)
                                                """,
                                                (event_id, text_type_code, text, patient_sequence_number)
                                            )
                                
                                batch_processed += 1
                            
                        conflict_count += batch_conflicts
                        log_info(f"已处理文件 {os.path.basename(file_path)} 的第 {batch_idx+1}/{len(batches)} 批, "
                                f"{batch_processed} 条记录, {batch_conflicts} 条冲突")
                        total_processed += batch_processed
                    
                    except Exception as e:
                        # 错误已经被回滚，确保连接状态良好
                        self.conn.rollback()
                        log_error(f"处理不良事件数据批次 {batch_idx+1} 失败: {str(e)}")
                        
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
            self.update_metadata('adverse_events', total_processed)
            log_success(f"不良事件数据导入完成，共处理 {total_processed} 条记录，解决 {conflict_count} 条冲突")
            return total_processed
            
        except Exception as e:
            self.conn.rollback()
            log_error(f"导入不良事件数据失败: {str(e)}")
            raise