"""
Base importer class with common functionality
"""
import re
import json
import psycopg2
import datetime
from logger import log_info, log_error, log_warning, log_success
from utils import parse_date, parse_boolean, convert_to_array

class BaseImporter:
    """处理FDA医疗设备数据导入PostgreSQL的基类"""
    
    def __init__(self, db_config):
        """初始化数据库连接"""
        self.db_config = db_config
        self.conn = None
        self.cur = None
        self.company_cache = {}  # 缓存公司ID，避免重复查询
        self.product_code_cache = {}  # 缓存产品代码ID，避免重复查询
        self.regulation_cache = {}  # 缓存法规ID，避免重复查询
        self.medical_specialty_cache = {}  # 缓存医疗专业ID，避免重复查询
        self.review_panel_cache = {}  # 缓存监管部门ID，避免重复查询
        self.submission_type_cache = {}  # 缓存提交类型ID，避免重复查询
        self.premarket_submission_cache = {}  # 缓存上市前提交ID，避免重复查询
        
    def connect(self):
        """连接到PostgreSQL数据库"""
        dbname = self.db_config['dbname']
        
        try:
            self.conn = psycopg2.connect(**self.db_config)
            self.conn.autocommit = False
            self.cur = self.conn.cursor()
            self.cur.execute("SET search_path TO device;")
            log_info(f"成功连接到PostgreSQL数据库 {dbname}")
            
            # 加载缓存
            self._load_caches()
            
            return True
        except Exception as e:
            log_error(f"数据库连接失败: {str(e)}")
            return False
    
    def _load_caches(self):
        """加载常用表的缓存以提高性能"""
        try:
            # 加载医疗专业缓存
            self.cur.execute("SELECT code, id FROM device.medical_specialties")
            self.medical_specialty_cache = {code: id for code, id in self.cur.fetchall()}
            
            # 加载监管部门缓存
            self.cur.execute("SELECT code, id FROM device.regulatory_panels")
            self.review_panel_cache = {code: id for code, id in self.cur.fetchall()}
            
            # 加载法规缓存
            self.cur.execute("SELECT regulation_number, id FROM device.regulations")
            self.regulation_cache = {reg: id for reg, id in self.cur.fetchall()}
            
            # 加载提交类型缓存
            self.cur.execute("SELECT submission_type_id, id FROM device.submission_types")
            self.submission_type_cache = {type_id: id for type_id, id in self.cur.fetchall()}
            
            log_info("已加载基础数据缓存")
        except Exception as e:
            log_warning(f"加载缓存失败: {str(e)}")
    
    def close(self):
        """关闭数据库连接"""
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()
        log_info("数据库连接已关闭")
    
    def get_or_create_medical_specialty(self, code, description=None):
        """获取或创建医疗专业"""
        if not code:
            return None
            
        # 检查缓存
        if code in self.medical_specialty_cache:
            return self.medical_specialty_cache[code]
            
        # 尝试查找现有医疗专业
        self.cur.execute("SELECT id FROM device.medical_specialties WHERE code = %s", (code,))
        result = self.cur.fetchone()
        
        if result:
            specialty_id = result[0]
        else:
            # 创建新医疗专业
            self.cur.execute(
                "INSERT INTO device.medical_specialties (code, description) VALUES (%s, %s) RETURNING id",
                (code, description)
            )
            specialty_id = self.cur.fetchone()[0]
        
        # 更新缓存
        self.medical_specialty_cache[code] = specialty_id
        return specialty_id
    
    def get_or_create_review_panel(self, code, description=None):
        """获取或创建监管部门"""
        if not code:
            return None
            
        # 检查缓存
        if code in self.review_panel_cache:
            return self.review_panel_cache[code]
            
        # 尝试查找现有监管部门
        self.cur.execute("SELECT id FROM device.regulatory_panels WHERE code = %s", (code,))
        result = self.cur.fetchone()
        
        if result:
            panel_id = result[0]
        else:
            # 创建新监管部门
            self.cur.execute(
                "INSERT INTO device.regulatory_panels (code, description) VALUES (%s, %s) RETURNING id",
                (code, description)
            )
            panel_id = self.cur.fetchone()[0]
        
        # 更新缓存
        self.review_panel_cache[code] = panel_id
        return panel_id
    
    def get_or_create_regulation(self, regulation_number):
        """获取或创建法规"""
        if not regulation_number:
            return None
            
        # 检查缓存
        if regulation_number in self.regulation_cache:
            return self.regulation_cache[regulation_number]
            
        # 尝试解析法规号
        title = None
        part = None
        section = None
        
        # 尝试匹配格式如 "21 CFR 888.3080"
        match = re.match(r'(\d+)\s+CFR\s+(\d+)\.(\d+)', regulation_number)
        if match:
            title = match.group(1)
            part = match.group(2)
            section = match.group(3)
        else:
            # 尝试匹配格式如 "888.3080"
            match = re.match(r'(\d+)\.(\d+)', regulation_number)
            if match:
                title = "21"  # 假设为21 CFR
                part = match.group(1)
                section = match.group(2)
            
        # 尝试查找现有法规
        self.cur.execute("SELECT id FROM device.regulations WHERE regulation_number = %s", (regulation_number,))
        result = self.cur.fetchone()
        
        if result:
            regulation_id = result[0]
        else:
            # 创建新法规
            self.cur.execute(
                """
                INSERT INTO device.regulations (regulation_number, title, part, section)
                VALUES (%s, %s, %s, %s) RETURNING id
                """,
                (regulation_number, title, part, section)
            )
            regulation_id = self.cur.fetchone()[0]
        
        # 更新缓存
        self.regulation_cache[regulation_number] = regulation_id
        return regulation_id
    
    def get_or_create_submission_type(self, submission_type_id):
        """获取或创建提交类型"""
        if not submission_type_id:
            return None
            
        # 检查缓存
        if submission_type_id in self.submission_type_cache:
            return self.submission_type_cache[submission_type_id]
            
        # 尝试查找现有提交类型
        self.cur.execute("SELECT id FROM device.submission_types WHERE submission_type_id = %s", (submission_type_id,))
        result = self.cur.fetchone()
        
        if result:
            type_id = result[0]
            # 更新缓存
            self.submission_type_cache[submission_type_id] = type_id
            return type_id
        else:
            return None  # 如果不存在，应该已经在预填充步骤中添加了所有类型，所以返回None
    
    def get_or_create_company(self, name, details=None):
        """获取或创建公司记录，缓存以提高性能"""
        if not name:
            return None
        
        # 标准化公司名称，去除额外空格
        name = name.strip()
        
        # 检查缓存
        if name in self.company_cache:
            return self.company_cache[name]
        
        # 尝试查找现有公司
        self.cur.execute("SELECT id FROM device.companies WHERE name = %s", (name,))
        result = self.cur.fetchone()
        
        if result:
            company_id = result[0]
            
            # 如果提供了更多详细信息，更新公司记录
            if details:
                update_fields = []
                update_values = []
                
                for field, value in details.items():
                    if value:
                        update_fields.append(f"{field} = %s")
                        update_values.append(value)
                
                if update_fields:
                    update_values.append(company_id)
                    self.cur.execute(
                        f"UPDATE device.companies SET {', '.join(update_fields)} WHERE id = %s",
                        update_values
                    )
        else:
            # 创建新公司
            if details:
                fields = ['name'] + list(details.keys())
                placeholders = ['%s'] * len(fields)
                values = [name] + list(details.values())
                
                self.cur.execute(
                    f"INSERT INTO device.companies ({', '.join(fields)}) VALUES ({', '.join(placeholders)}) RETURNING id",
                    values
                )
            else:
                self.cur.execute(
                    "INSERT INTO device.companies (name) VALUES (%s) RETURNING id",
                    (name,)
                )
                
            company_id = self.cur.fetchone()[0]
        
        # 更新缓存
        self.company_cache[name] = company_id
        return company_id
    
    def add_company_contact(self, company_id, contact_type, contact_value):
        """添加公司联系信息"""
        if not company_id or not contact_type or not contact_value:
            return None
        
        try:
            self.cur.execute(
                """
                INSERT INTO device.company_contacts (company_id, contact_type, contact_value)
                VALUES (%s, %s, %s)
                ON CONFLICT DO NOTHING
                RETURNING id
                """,
                (company_id, contact_type, contact_value)
            )
            result = self.cur.fetchone()
            if result:
                return result[0]
            return None
        except Exception as e:
            log_warning(f"添加公司联系信息失败: {str(e)}")
            return None
    
    def get_or_create_premarket_submission(self, submission_number, submission_type=None, supplement_number=None):
        """获取或创建上市前提交记录"""
        if not submission_number:
            return None
            
        # 检查缓存
        if submission_number in self.premarket_submission_cache:
            return self.premarket_submission_cache[submission_number]
            
        try:
            # 尝试查找现有提交
            self.cur.execute("SELECT id FROM device.premarket_submissions WHERE submission_number = %s", (submission_number,))
            result = self.cur.fetchone()
            
            if result:
                submission_id = result[0]
                
                # 如果提供了更多详细信息，更新记录
                if submission_type or supplement_number:
                    update_fields = []
                    update_values = []
                    
                    if submission_type:
                        update_fields.append("submission_type = %s")
                        update_values.append(submission_type)
                    
                    if supplement_number:
                        update_fields.append("supplement_number = %s")
                        update_values.append(supplement_number)
                    
                    if update_fields:
                        update_values.append(submission_id)
                        self.cur.execute(
                            f"UPDATE device.premarket_submissions SET {', '.join(update_fields)} WHERE id = %s",
                            update_values
                        )
            else:
                # 创建新提交记录
                self.cur.execute(
                    """
                    INSERT INTO device.premarket_submissions (submission_number, submission_type, supplement_number)
                    VALUES (%s, %s, %s) RETURNING id
                    """,
                    (submission_number, submission_type, supplement_number)
                )
                submission_id = self.cur.fetchone()[0]
                # 确保立即提交此记录以防止外键约束错误
                self.conn.commit()
            
            # 更新缓存
            self.premarket_submission_cache[submission_number] = submission_id
            return submission_id
        except Exception as e:
            self.conn.rollback()
            log_warning(f"创建上市前提交记录失败: {str(e)}")
            return None
    
    def link_device_to_submission(self, device_id, device_type, submission_id):
        """关联设备和上市前提交"""
        if not device_id or not device_type or not submission_id:
            return None
            
        try:
            self.cur.execute(
                """
                INSERT INTO device.device_premarket_submissions (device_id, device_type, submission_id)
                VALUES (%s, %s, %s)
                ON CONFLICT (device_id, device_type, submission_id) DO NOTHING
                RETURNING id
                """,
                (device_id, device_type, submission_id)
            )
            result = self.cur.fetchone()
            if result:
                return result[0]
            return None
        except Exception as e:
            log_warning(f"关联设备和上市前提交失败: {str(e)}")
            return None
    
    def get_or_create_product_code(self, product_code, device_name=None, additional_data=None):
        """获取或创建产品代码记录"""
        if not product_code:
            return None
            
        # 检查缓存
        if product_code in self.product_code_cache:
            return self.product_code_cache[product_code]
            
        # 尝试查找现有产品代码
        self.cur.execute("SELECT id FROM device.product_codes WHERE product_code = %s", (product_code,))
        result = self.cur.fetchone()
        
        if result:
            product_code_id = result[0]
            
            # 如果提供了更多详细信息，更新记录
            if additional_data:
                update_fields = []
                update_values = []
                
                if device_name and not device_name.isspace():
                    update_fields.append("device_name = %s")
                    update_values.append(device_name)
                
                # 只更新product_codes表中存在的列
                valid_fields = [
                    'device_class', 'regulation_number', 'medical_specialty', 
                    'review_panel', 'definition', 'implant_flag', 
                    'life_sustain_support_flag', 'gmp_exempt_flag', 
                    'summary_malfunction_reporting', 'submission_type_id',
                    'medical_specialty_description'
                ]
                
                for field, value in additional_data.items():
                    # 只更新有效字段，排除OpenFDA特有的字段
                    if field in valid_fields and value is not None:
                        update_fields.append(f"{field} = %s")
                        update_values.append(value)
                
                if update_fields:
                    update_values.append(product_code_id)
                    try:
                        self.cur.execute(
                            f"UPDATE device.product_codes SET {', '.join(update_fields)} WHERE id = %s",
                            update_values
                        )
                    except Exception as e:
                        log_warning(f"更新产品代码信息失败: {str(e)}")
        else:
            # 准备产品代码的基本信息
            if not device_name:
                device_name = product_code
            
            device_class = None
            regulation_number = None
            regulation_id = None
            medical_specialty_code = None
            medical_specialty_id = None
            review_panel_code = None
            review_panel_id = None
            definition = None
            implant_flag = None
            life_sustain_support_flag = None
            gmp_exempt_flag = None
            summary_malfunction_reporting = None
            submission_type_id = None
            submission_type_ref = None
            
            # 从additional_data获取信息
            if additional_data:
                device_class = additional_data.get('device_class')
                regulation_number = additional_data.get('regulation_number')
                if regulation_number:
                    regulation_id = self.get_or_create_regulation(regulation_number)
                
                medical_specialty_code = additional_data.get('medical_specialty')
                if medical_specialty_code:
                    medical_specialty_id = self.get_or_create_medical_specialty(
                        medical_specialty_code, 
                        additional_data.get('medical_specialty_description')
                    )
                
                review_panel_code = additional_data.get('review_panel')
                if review_panel_code:
                    review_panel_id = self.get_or_create_review_panel(review_panel_code)
                
                definition = additional_data.get('definition')
                implant_flag = parse_boolean(additional_data.get('implant_flag'))
                life_sustain_support_flag = parse_boolean(additional_data.get('life_sustain_support_flag'))
                gmp_exempt_flag = parse_boolean(additional_data.get('gmp_exempt_flag'))
                summary_malfunction_reporting = additional_data.get('summary_malfunction_reporting')
                submission_type_id = additional_data.get('submission_type_id')
                if submission_type_id:
                    submission_type_ref = self.get_or_create_submission_type(submission_type_id)
            
            # 创建新产品代码
            try:
                self.cur.execute(
                    """
                    INSERT INTO device.product_codes (
                        product_code, device_name, device_class, regulation_number, regulation_id,
                        medical_specialty_code, medical_specialty_id, review_panel_code, review_panel_id,
                        definition, implant_flag, life_sustain_support_flag, gmp_exempt_flag,
                        summary_malfunction_reporting, submission_type_id, submission_type_ref
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (
                        product_code, device_name, device_class, regulation_number, regulation_id,
                        medical_specialty_code, medical_specialty_id, review_panel_code, review_panel_id,
                        definition, implant_flag, life_sustain_support_flag, gmp_exempt_flag,
                        summary_malfunction_reporting, submission_type_id, submission_type_ref
                    )
                )
                product_code_id = self.cur.fetchone()[0]
            except Exception as e:
                log_warning(f"创建产品代码失败: {str(e)}")
                return None
        
        # 更新缓存
        self.product_code_cache[product_code] = product_code_id
        return product_code_id
    
    def store_openfda_data(self, entity_id, entity_type, openfda):
        """存储OpenFDA数据到关系表中"""
        if not openfda:
            return None
            
        try:
            # 插入基本OpenFDA数据
            self.cur.execute(
                """
                INSERT INTO device.openfda_data (
                    entity_id, entity_type, device_name, device_class,
                    regulation_number, medical_specialty_description
                ) VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (entity_id, entity_type) DO UPDATE SET
                    device_name = EXCLUDED.device_name,
                    device_class = EXCLUDED.device_class,
                    regulation_number = EXCLUDED.regulation_number,
                    medical_specialty_description = EXCLUDED.medical_specialty_description
                RETURNING id
                """,
                (
                    entity_id, entity_type,
                    openfda.get('device_name'),
                    openfda.get('device_class'),
                    openfda.get('regulation_number'),
                    openfda.get('medical_specialty_description')
                )
            )
            
            openfda_id = self.cur.fetchone()[0]
            
            # 处理各种标识符
            identifier_types = {
                'k_number': openfda.get('k_number', []),
                'registration_number': openfda.get('registration_number', []),
                'fei_number': openfda.get('fei_number', []),
                'pma_number': openfda.get('pma_number', [])
            }
            
            # 插入所有标识符
            for identifier_type, values in identifier_types.items():
                if not values:
                    continue
                    
                # 确保values是列表
                if not isinstance(values, list):
                    values = [values]
                    
                for value in values:
                    if value:  # 确保值不为空
                        try:
                            self.cur.execute(
                                """
                                INSERT INTO device.openfda_identifiers (
                                    openfda_id, identifier_type, identifier_value
                                ) VALUES (%s, %s, %s)
                                ON CONFLICT (openfda_id, identifier_type, identifier_value) DO NOTHING
                                """,
                                (openfda_id, identifier_type, value)
                            )
                            
                            # 如果是k号或pma号，同时创建上市前提交记录
                            if identifier_type in ['k_number', 'pma_number']:
                                submission_type = 'PMA' if identifier_type == 'pma_number' else '510(k)'
                                self.get_or_create_premarket_submission(value, submission_type)
                                
                        except Exception as e:
                            log_warning(f"插入OpenFDA标识符失败: {str(e)}, 标识符: {identifier_type}={value}")
            
            return openfda_id
            
        except Exception as e:
            log_warning(f"存储OpenFDA数据失败: {str(e)}")
            return None
    
    def update_metadata(self, dataset_name, record_count):
        """更新数据集元数据"""
        try:
            self.cur.execute(
                """
                INSERT INTO device.dataset_metadata (
                    dataset_name, last_updated, record_count, source
                ) VALUES (%s, %s, %s, %s)
                ON CONFLICT (dataset_name) DO UPDATE SET 
                    last_updated = EXCLUDED.last_updated,
                    record_count = EXCLUDED.record_count,
                    source = EXCLUDED.source
                """,
                (
                    dataset_name, datetime.datetime.now(),
                    record_count, 'FDA OpenFDA API'
                )
            )
            self.conn.commit()
        except Exception as e:
            log_error(f"更新元数据失败: {str(e)}")