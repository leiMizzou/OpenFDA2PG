"""
UDI data importer
"""
import os
from tqdm.notebook import tqdm
from file_handler import FileHandler
from logger import log_info, log_error, log_success, log_warning
from importers.base_importer import BaseImporter
from utils import parse_date, parse_boolean, convert_to_array

class UDIImporter(BaseImporter):
    """处理UDI数据导入"""
    
    def import_data(self, files, batch_size=100):
        """导入UDI数据"""
        total_processed = 0
        
        try:
            # 遍历每个UDI文件
            for file_path in tqdm(files, desc="处理UDI文件"):
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
                            for udi in batch:
                                public_device_record_key = udi.get('public_device_record_key')
                                if not public_device_record_key:
                                    continue
                                
                                # 提取基本数据
                                device_description = udi.get('device_description')
                                brand_name = udi.get('brand_name')
                                version_or_model_number = udi.get('version_or_model_number')
                                company_name = udi.get('company_name')
                                labeler_duns_number = udi.get('labeler_duns_number')
                                record_status = udi.get('record_status')
                                public_version_number = udi.get('public_version_number')
                                public_version_date = parse_date(udi.get('public_version_date'))
                                public_version_status = udi.get('public_version_status')
                                publish_date = parse_date(udi.get('publish_date'))
                                
                                # 提取布尔标志
                                is_single_use = parse_boolean(udi.get('is_single_use'))
                                is_rx = parse_boolean(udi.get('is_rx'))
                                is_otc = parse_boolean(udi.get('is_otc'))
                                is_kit = parse_boolean(udi.get('is_kit'))
                                is_combination_product = parse_boolean(udi.get('is_combination_product'))
                                is_hct_p = parse_boolean(udi.get('is_hct_p'))
                                is_pm_exempt = parse_boolean(udi.get('is_pm_exempt'))
                                is_direct_marking_exempt = parse_boolean(udi.get('is_direct_marking_exempt'))
                                has_lot_or_batch_number = parse_boolean(udi.get('has_lot_or_batch_number'))
                                has_serial_number = parse_boolean(udi.get('has_serial_number'))
                                has_manufacturing_date = parse_boolean(udi.get('has_manufacturing_date'))
                                has_expiration_date = parse_boolean(udi.get('has_expiration_date'))
                                has_donation_id_number = parse_boolean(udi.get('has_donation_id_number'))
                                is_labeled_as_nrl = parse_boolean(udi.get('is_labeled_as_nrl'))
                                is_labeled_as_no_nrl = parse_boolean(udi.get('is_labeled_as_no_nrl'))
                                
                                # 提取其他字段
                                mri_safety = udi.get('mri_safety')
                                commercial_distribution_status = udi.get('commercial_distribution_status')
                                device_count_in_base_package = udi.get('device_count_in_base_package')
                                
                                # 处理公司
                                company_details = {
                                    'duns_number': labeler_duns_number
                                }
                                company_id = self.get_or_create_company(company_name, company_details)
                                
                                # 插入UDI记录
                                self.cur.execute(
                                    """
                                    INSERT INTO device.udi_records (
                                        public_device_record_key, device_description, brand_name,
                                        version_or_model_number, company_name, company_id, labeler_duns_number,
                                        record_status, public_version_number, public_version_date,
                                        public_version_status, publish_date, is_single_use,
                                        is_rx, is_otc, is_kit, is_combination_product, is_hct_p,
                                        is_pm_exempt, is_direct_marking_exempt, has_lot_or_batch_number,
                                        has_serial_number, has_manufacturing_date, has_expiration_date,
                                        has_donation_id_number, is_labeled_as_nrl, is_labeled_as_no_nrl,
                                        mri_safety, commercial_distribution_status, device_count_in_base_package
                                    ) VALUES (
                                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                                    )
                                    ON CONFLICT (public_device_record_key) DO UPDATE SET 
                                        record_status = EXCLUDED.record_status,
                                        public_version_number = EXCLUDED.public_version_number,
                                        public_version_date = EXCLUDED.public_version_date,
                                        public_version_status = EXCLUDED.public_version_status
                                    RETURNING id
                                    """,
                                    (
                                        public_device_record_key, device_description, brand_name,
                                        version_or_model_number, company_name, company_id, labeler_duns_number,
                                        record_status, public_version_number, public_version_date,
                                        public_version_status, publish_date, is_single_use,
                                        is_rx, is_otc, is_kit, is_combination_product, is_hct_p,
                                        is_pm_exempt, is_direct_marking_exempt, has_lot_or_batch_number,
                                        has_serial_number, has_manufacturing_date, has_expiration_date,
                                        has_donation_id_number, is_labeled_as_nrl, is_labeled_as_no_nrl,
                                        mri_safety, commercial_distribution_status, device_count_in_base_package
                                    )
                                )
                                
                                udi_id = self.cur.fetchone()[0]
                                
                                # 处理标识符
                                if 'identifiers' in udi and udi['identifiers']:
                                    identifiers = udi['identifiers']
                                    if not isinstance(identifiers, list):
                                        identifiers = [identifiers]
                                        
                                    for identifier in identifiers:
                                        identifier_type = identifier.get('type')
                                        issuing_agency = identifier.get('issuing_agency')
                                        identifier_value = identifier.get('id')
                                        
                                        if identifier_type and identifier_value:
                                            self.cur.execute(
                                                """
                                                INSERT INTO device.udi_identifiers (
                                                    udi_record_id, identifier_type, issuing_agency, identifier_value
                                                ) VALUES (%s, %s, %s, %s)
                                                ON CONFLICT DO NOTHING
                                                """,
                                                (udi_id, identifier_type, issuing_agency, identifier_value)
                                            )
                                
                                # 处理产品代码 - 可能有多个产品代码
                                if 'product_codes' in udi and udi['product_codes']:
                                    product_codes = udi['product_codes']
                                    if not isinstance(product_codes, list):
                                        product_codes = [product_codes]
                                        
                                    for pc in product_codes:
                                        product_code = pc.get('code')
                                        device_name = pc.get('name')
                                        openfda = pc.get('openfda', {})
                                        
                                        if product_code:
                                            # 创建或获取产品代码
                                            product_code_id = self.get_or_create_product_code(
                                                product_code, device_name, openfda
                                            )
                                            
                                            # 关联UDI和产品代码
                                            self.cur.execute(
                                                """
                                                INSERT INTO device.udi_product_codes (
                                                    udi_record_id, product_code, product_code_id, device_name
                                                ) VALUES (%s, %s, %s, %s)
                                                ON CONFLICT (udi_record_id, product_code) DO NOTHING
                                                """,
                                                (udi_id, product_code, product_code_id, device_name)
                                            )
                                            
                                            # 存储OpenFDA数据
                                            if openfda:
                                                self.store_openfda_data(udi_id, 'udi_records', openfda)
                                
                                # 处理灭菌信息
                                if 'sterilization' in udi and udi['sterilization']:
                                    sterilization = udi['sterilization']
                                    is_sterile = parse_boolean(sterilization.get('is_sterile'))
                                    is_sterilization_prior_use = parse_boolean(sterilization.get('is_sterilization_prior_use'))
                                    sterilization_methods = None
                                    
                                    # 处理灭菌方法，可能是字符串或数组
                                    if 'sterilization_methods' in sterilization and sterilization['sterilization_methods']:
                                        sterilization_methods = convert_to_array(sterilization['sterilization_methods'])
                                    
                                    self.cur.execute(
                                        """
                                        INSERT INTO device.udi_sterilization (
                                            udi_record_id, is_sterile, is_sterilization_prior_use,
                                            sterilization_methods
                                        ) VALUES (%s, %s, %s, %s)
                                        ON CONFLICT (udi_record_id) DO UPDATE SET 
                                            is_sterile = EXCLUDED.is_sterile,
                                            is_sterilization_prior_use = EXCLUDED.is_sterilization_prior_use,
                                            sterilization_methods = EXCLUDED.sterilization_methods
                                        """,
                                        (udi_id, is_sterile, is_sterilization_prior_use, sterilization_methods)
                                    )
                                
                                # 处理设备尺寸
                                if 'device_sizes' in udi and udi['device_sizes']:
                                    sizes = udi['device_sizes']
                                    if not isinstance(sizes, list):
                                        sizes = [sizes]
                                        
                                    for size in sizes:
                                        size_type = size.get('type')
                                        size_value = size.get('value')
                                        size_unit = size.get('unit')
                                        
                                        if size_type and size_value:
                                            self.cur.execute(
                                                """
                                                INSERT INTO device.udi_device_sizes (
                                                    udi_record_id, size_type, size_value, size_unit
                                                ) VALUES (%s, %s, %s, %s)
                                                ON CONFLICT DO NOTHING
                                                """,
                                                (udi_id, size_type, size_value, size_unit)
                                            )
                                
                                # 处理GMDN术语
                                if 'gmdn_terms' in udi and udi['gmdn_terms']:
                                    terms = udi['gmdn_terms']
                                    if not isinstance(terms, list):
                                        terms = [terms]
                                        
                                    for term in terms:
                                        code = term.get('code')
                                        name = term.get('name')
                                        definition = term.get('definition')
                                        implantable = parse_boolean(term.get('implantable'))
                                        code_status = term.get('code_status')
                                        
                                        if code:
                                            self.cur.execute(
                                                """
                                                INSERT INTO device.udi_gmdn_terms (
                                                    udi_record_id, code, name, definition,
                                                    implantable, code_status
                                                ) VALUES (%s, %s, %s, %s, %s, %s)
                                                ON CONFLICT DO NOTHING
                                                """,
                                                (udi_id, code, name, definition, implantable, code_status)
                                            )
                                
                                # 处理客户联系信息
                                if 'customer_contacts' in udi and udi['customer_contacts']:
                                    contacts = udi['customer_contacts']
                                    if not isinstance(contacts, list):
                                        contacts = [contacts]
                                        
                                    for contact in contacts:
                                        phone = contact.get('phone')
                                        email = contact.get('email')
                                        
                                        if phone:
                                            self.cur.execute(
                                                """
                                                INSERT INTO device.udi_customer_contacts (
                                                    udi_record_id, contact_type, contact_value
                                                ) VALUES (%s, %s, %s)
                                                ON CONFLICT DO NOTHING
                                                """,
                                                (udi_id, 'phone', phone)
                                            )
                                            
                                            # 同时更新公司联系信息
                                            if company_id:
                                                self.add_company_contact(company_id, 'phone', phone)
                                        
                                        if email:
                                            self.cur.execute(
                                                """
                                                INSERT INTO device.udi_customer_contacts (
                                                    udi_record_id, contact_type, contact_value
                                                ) VALUES (%s, %s, %s)
                                                ON CONFLICT DO NOTHING
                                                """,
                                                (udi_id, 'email', email)
                                            )
                                            
                                            # 同时更新公司联系信息
                                            if company_id:
                                                self.add_company_contact(company_id, 'email', email)
                                
                                # 处理上市前提交信息
                                if 'premarket_submissions' in udi and udi['premarket_submissions']:
                                    submissions = udi['premarket_submissions']
                                    if not isinstance(submissions, list):
                                        submissions = [submissions]
                                        
                                    for submission in submissions:
                                        submission_number = submission.get('submission_number')
                                        supplement_number = submission.get('supplement_number')
                                        
                                        if submission_number:
                                            # 确定提交类型
                                            submission_type = None
                                            if submission_number.startswith('K'):
                                                submission_type = '510(k)'
                                            elif submission_number.startswith('P'):
                                                submission_type = 'PMA'
                                            elif submission_number.startswith('D'):
                                                submission_type = 'De Novo'
                                            elif submission_number.startswith('H'):
                                                submission_type = 'HDE'
                                            
                                            # 创建或获取上市前提交
                                            submission_id = self.get_or_create_premarket_submission(
                                                submission_number, submission_type, supplement_number
                                            )
                                            
                                            # 关联UDI和提交
                                            self.cur.execute(
                                                """
                                                INSERT INTO device.udi_premarket_submissions (
                                                    udi_record_id, submission_number, submission_id, supplement_number
                                                ) VALUES (%s, %s, %s, %s)
                                                ON CONFLICT DO NOTHING
                                                """,
                                                (udi_id, submission_number, submission_id, supplement_number)
                                            )
                                            
                                            # 关联设备和提交
                                            self.link_device_to_submission(udi_id, 'udi_records', submission_id)
                                
                                batch_processed += 1
                            
                        log_info(f"已处理文件 {os.path.basename(file_path)} 的第 {batch_idx+1}/{len(batches)} 批, {batch_processed} 条记录")
                        total_processed += batch_processed
                    
                    except Exception as e:
                        self.conn.rollback()
                        log_error(f"处理UDI数据批次 {batch_idx+1} 失败: {str(e)}")
            
            # 更新元数据
            self.update_metadata('udi_records', total_processed)
            log_success(f"UDI数据导入完成，共处理 {total_processed} 条记录")
            return total_processed
            
        except Exception as e:
            self.conn.rollback()
            log_error(f"导入UDI数据失败: {str(e)}")
            raise
