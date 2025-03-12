"""
Classification data importer
"""
import os
from tqdm.notebook import tqdm
from file_handler import FileHandler
from logger import log_info, log_error, log_success, log_warning
from importers.base_importer import BaseImporter
from utils import parse_boolean

class ClassificationImporter(BaseImporter):
    """处理设备分类数据导入"""
    
    def import_data(self, files, batch_size=100):
        """导入设备分类数据"""
        total_processed = 0
        
        try:
            # 遍历每个分类文件
            for file_path in tqdm(files, desc="处理设备分类文件"):
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
                            for classification in batch:
                                product_code = classification.get('product_code')
                                if not product_code:
                                    continue
                                
                                # 提取基本数据
                                device_name = classification.get('device_name', '')
                                device_class = classification.get('device_class')
                                review_panel = classification.get('review_panel')
                                review_panel_id = self.get_or_create_review_panel(review_panel)
                                medical_specialty = classification.get('medical_specialty')
                                medical_specialty_id = self.get_or_create_medical_specialty(
                                    medical_specialty, 
                                    classification.get('medical_specialty_description')
                                )
                                definition = classification.get('definition')
                                regulation_number = classification.get('regulation_number')
                                regulation_id = self.get_or_create_regulation(regulation_number)
                                implant_flag = parse_boolean(classification.get('implant_flag'))
                                third_party_flag = parse_boolean(classification.get('third_party_flag'))
                                life_sustain_support_flag = parse_boolean(classification.get('life_sustain_support_flag'))
                                gmp_exempt_flag = parse_boolean(classification.get('gmp_exempt_flag'))
                                summary_malfunction_reporting = classification.get('summary_malfunction_reporting')
                                unclassified_reason = classification.get('unclassified_reason')
                                review_code = classification.get('review_code')
                                submission_type_id = classification.get('submission_type_id')
                                submission_type_ref = self.get_or_create_submission_type(submission_type_id)
                                medical_specialty_description = classification.get('medical_specialty_description')
                                openfda = classification.get('openfda', {})
                                
                                # 创建或获取产品代码
                                additional_data = {
                                    'device_class': device_class,
                                    'regulation_number': regulation_number,
                                    'medical_specialty': medical_specialty,
                                    'medical_specialty_description': medical_specialty_description,
                                    'review_panel': review_panel,
                                    'definition': definition,
                                    'implant_flag': implant_flag,
                                    'life_sustain_support_flag': life_sustain_support_flag,
                                    'gmp_exempt_flag': gmp_exempt_flag,
                                    'summary_malfunction_reporting': summary_malfunction_reporting,
                                    'submission_type_id': submission_type_id
                                }
                                product_code_id = self.get_or_create_product_code(
                                    product_code, device_name, additional_data
                                )
                                
                                # 插入分类
                                self.cur.execute(
                                    """
                                    INSERT INTO device.device_classifications (
                                        product_code, product_code_id, review_panel, review_panel_id,
                                        device_class, device_name, definition, regulation_number, regulation_id,
                                        medical_specialty, medical_specialty_id, medical_specialty_description,
                                        implant_flag, third_party_flag, life_sustain_support_flag,
                                        gmp_exempt_flag, unclassified_reason, review_code, 
                                        summary_malfunction_reporting, submission_type_id, submission_type_ref
                                    ) VALUES (
                                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                                    )
                                    ON CONFLICT (product_code) DO UPDATE SET 
                                        product_code_id = EXCLUDED.product_code_id,
                                        review_panel = EXCLUDED.review_panel,
                                        review_panel_id = EXCLUDED.review_panel_id,
                                        device_class = EXCLUDED.device_class,
                                        device_name = EXCLUDED.device_name,
                                        definition = EXCLUDED.definition,
                                        regulation_number = EXCLUDED.regulation_number,
                                        regulation_id = EXCLUDED.regulation_id,
                                        medical_specialty = EXCLUDED.medical_specialty,
                                        medical_specialty_id = EXCLUDED.medical_specialty_id,
                                        medical_specialty_description = EXCLUDED.medical_specialty_description,
                                        implant_flag = EXCLUDED.implant_flag,
                                        third_party_flag = EXCLUDED.third_party_flag,
                                        life_sustain_support_flag = EXCLUDED.life_sustain_support_flag,
                                        gmp_exempt_flag = EXCLUDED.gmp_exempt_flag,
                                        unclassified_reason = EXCLUDED.unclassified_reason,
                                        review_code = EXCLUDED.review_code,
                                        summary_malfunction_reporting = EXCLUDED.summary_malfunction_reporting,
                                        submission_type_id = EXCLUDED.submission_type_id,
                                        submission_type_ref = EXCLUDED.submission_type_ref
                                    RETURNING id
                                    """,
                                    (
                                        product_code, product_code_id, review_panel, review_panel_id,
                                        device_class, device_name, definition, regulation_number, regulation_id,
                                        medical_specialty, medical_specialty_id, medical_specialty_description,
                                        implant_flag, third_party_flag, life_sustain_support_flag,
                                        gmp_exempt_flag, unclassified_reason, review_code, 
                                        summary_malfunction_reporting, submission_type_id, submission_type_ref
                                    )
                                )
                                
                                classification_id = self.cur.fetchone()[0]
                                
                                # 存储OpenFDA数据
                                self.store_openfda_data(classification_id, 'device_classifications', openfda)
                                
                                # 处理premarket submission数据
                                if 'openfda' in classification and classification['openfda']:
                                    for submission_type in ['k_number', 'pma_number']:
                                        if submission_type in classification['openfda']:
                                            submission_numbers = classification['openfda'][submission_type]
                                            if not isinstance(submission_numbers, list):
                                                submission_numbers = [submission_numbers]
                                            
                                            for submission_number in submission_numbers:
                                                if submission_number:
                                                    sub_type = 'PMA' if submission_type == 'pma_number' else '510(k)'
                                                    submission_id = self.get_or_create_premarket_submission(
                                                        submission_number, sub_type
                                                    )
                                                    if submission_id:
                                                        self.link_device_to_submission(
                                                            classification_id, 'device_classifications', submission_id
                                                        )
                                
                                batch_processed += 1
                            
                        log_info(f"已处理文件 {os.path.basename(file_path)} 的第 {batch_idx+1}/{len(batches)} 批, {batch_processed} 条记录")
                        total_processed += batch_processed
                    
                    except Exception as e:
                        self.conn.rollback()
                        log_error(f"处理分类数据批次 {batch_idx+1} 失败: {str(e)}")
            
            # 更新元数据
            self.update_metadata('device_classifications', total_processed)
            log_success(f"设备分类数据导入完成，共处理 {total_processed} 条记录")
            return total_processed
            
        except Exception as e:
            self.conn.rollback()
            log_error(f"导入设备分类数据失败: {str(e)}")
            raise
