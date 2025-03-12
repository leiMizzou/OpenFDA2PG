"""
Database schema creation for FDA device data import
"""
import psycopg2
from logger import log_info, log_error, log_warning, log_success

class SchemaCreator:
    """用于创建数据库模式的类"""
    
    def __init__(self, db_config):
        """初始化数据库连接"""
        self.db_config = db_config
        self.conn = None
        self.cur = None
    
    def database_exists(self, dbname):
        """检查数据库是否存在"""
        # 连接到默认的postgres数据库
        postgres_config = self.db_config.copy()
        postgres_config['dbname'] = 'postgres'
        
        try:
            conn = psycopg2.connect(**postgres_config)
            conn.autocommit = True
            cur = conn.cursor()
            
            # 查询数据库是否存在
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (dbname,))
            exists = cur.fetchone() is not None
            
            cur.close()
            conn.close()
            
            return exists
        except Exception as e:
            log_error(f"检查数据库是否存在失败: {str(e)}")
            return False
    
    def create_database(self, dbname):
        """创建数据库"""
        # 连接到默认的postgres数据库
        postgres_config = self.db_config.copy()
        postgres_config['dbname'] = 'postgres'
        
        try:
            conn = psycopg2.connect(**postgres_config)
            conn.autocommit = True
            cur = conn.cursor()
            
            # 创建数据库
            cur.execute(f"CREATE DATABASE {dbname}")
            
            cur.close()
            conn.close()
            
            log_success(f"成功创建数据库 {dbname}")
            return True
        except Exception as e:
            log_error(f"创建数据库失败: {str(e)}")
            return False
    
    def connect(self):
        """连接到PostgreSQL数据库，如果数据库不存在则创建"""
        dbname = self.db_config['dbname']
        
        # 检查数据库是否存在，不存在则创建
        if not self.database_exists(dbname):
            log_warning(f"数据库 {dbname} 不存在，尝试创建...")
            if not self.create_database(dbname):
                return False
        
        try:
            self.conn = psycopg2.connect(**self.db_config)
            self.conn.autocommit = False
            self.cur = self.conn.cursor()
            log_info(f"成功连接到PostgreSQL数据库 {dbname}")
            return True
        except Exception as e:
            log_error(f"数据库连接失败: {str(e)}")
            return False
    
    def close(self):
        """关闭数据库连接"""
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()
        log_info("数据库连接已关闭")
    
    def create_schema(self):
        """创建改进的数据库模式"""
        log_info("开始创建改进数据库模式...")
        
        try:
            # 删除已有模式并创建新模式
            self.cur.execute("DROP SCHEMA IF EXISTS device CASCADE;")
            self.cur.execute("CREATE SCHEMA device;")
            self.cur.execute("SET search_path TO device;")
            log_info("已创建device模式")
            
            # 创建基础表
            
            # 创建医疗专业表 - 存储专业信息
            self.cur.execute("""
                CREATE TABLE medical_specialties (
                    id SERIAL PRIMARY KEY,
                    code TEXT UNIQUE,
                    description TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE INDEX idx_medical_specialties_code ON medical_specialties(code);
            """)
            log_info("已创建medical_specialties表")
            
            # 创建监管部门表 - 存储监管部门信息
            self.cur.execute("""
                CREATE TABLE regulatory_panels (
                    id SERIAL PRIMARY KEY,
                    code TEXT UNIQUE,
                    description TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE INDEX idx_regulatory_panels_code ON regulatory_panels(code);
            """)
            log_info("已创建regulatory_panels表")
            
            # 创建法规表 - 存储法规信息
            self.cur.execute("""
                CREATE TABLE regulations (
                    id SERIAL PRIMARY KEY,
                    regulation_number TEXT UNIQUE,
                    title TEXT,
                    part TEXT,
                    section TEXT,
                    description TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE INDEX idx_regulations_number ON regulations(regulation_number);
            """)
            log_info("已创建regulations表")
            
            # 创建提交类型表 - 存储提交类型信息
            self.cur.execute("""
                CREATE TABLE submission_types (
                    id SERIAL PRIMARY KEY,
                    submission_type_id TEXT UNIQUE,
                    description TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE INDEX idx_submission_types_id ON submission_types(submission_type_id);
            """)
            log_info("已创建submission_types表")
            
            # 创建companies表 - 存储公司/制造商信息
            self.cur.execute("""
                CREATE TABLE companies (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    duns_number TEXT,
                    address_line_1 TEXT,
                    address_line_2 TEXT,
                    city TEXT,
                    state TEXT,
                    postal_code TEXT,
                    country TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE INDEX idx_companies_name ON companies(name);
                CREATE INDEX idx_companies_duns ON companies(duns_number);
            """)
            log_info("已创建companies表")
            
            # 创建company_contacts表 - 存储公司联系信息
            self.cur.execute("""
                CREATE TABLE company_contacts (
                    id SERIAL PRIMARY KEY,
                    company_id INTEGER REFERENCES companies(id),
                    contact_type TEXT, -- phone, email, website, etc.
                    contact_value TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE INDEX idx_company_contacts_company_id ON company_contacts(company_id);
            """)
            log_info("已创建company_contacts表")
            
            # 创建product_codes表 - 存储产品代码信息
            self.cur.execute("""
                CREATE TABLE product_codes (
                    id SERIAL PRIMARY KEY,
                    product_code TEXT UNIQUE NOT NULL,
                    device_name TEXT NOT NULL,
                    device_class TEXT,
                    regulation_number TEXT,
                    regulation_id INTEGER REFERENCES regulations(id),
                    medical_specialty_code TEXT,
                    medical_specialty_id INTEGER REFERENCES medical_specialties(id),
                    medical_specialty_description TEXT,
                    review_panel_code TEXT,
                    review_panel_id INTEGER REFERENCES regulatory_panels(id),
                    definition TEXT,
                    implant_flag BOOLEAN,
                    life_sustain_support_flag BOOLEAN,
                    gmp_exempt_flag BOOLEAN,
                    summary_malfunction_reporting TEXT,
                    submission_type_id TEXT,
                    submission_type_ref INTEGER REFERENCES submission_types(id),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE INDEX idx_product_codes_code ON product_codes(product_code);
                CREATE INDEX idx_product_codes_device_class ON product_codes(device_class);
                CREATE INDEX idx_product_codes_regulation ON product_codes(regulation_id);
                CREATE INDEX idx_product_codes_specialty ON product_codes(medical_specialty_id);
            """)
            log_info("已创建product_codes表")
            
            # 创建device_classifications表 - 设备分类信息
            self.cur.execute("""
                CREATE TABLE device_classifications (
                    id SERIAL PRIMARY KEY,
                    product_code TEXT NOT NULL,
                    product_code_id INTEGER REFERENCES product_codes(id),
                    review_panel TEXT,
                    review_panel_id INTEGER REFERENCES regulatory_panels(id),
                    device_class TEXT,
                    device_name TEXT,
                    definition TEXT,
                    regulation_number TEXT,
                    regulation_id INTEGER REFERENCES regulations(id),
                    medical_specialty TEXT,
                    medical_specialty_id INTEGER REFERENCES medical_specialties(id),
                    medical_specialty_description TEXT,
                    implant_flag BOOLEAN,
                    third_party_flag BOOLEAN,
                    life_sustain_support_flag BOOLEAN,
                    gmp_exempt_flag BOOLEAN,
                    unclassified_reason TEXT,
                    review_code TEXT,
                    summary_malfunction_reporting TEXT,
                    submission_type_id TEXT,
                    submission_type_ref INTEGER REFERENCES submission_types(id),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(product_code)
                );
                
                CREATE INDEX idx_device_classifications_product_code ON device_classifications(product_code);
                CREATE INDEX idx_device_classifications_product_code_id ON device_classifications(product_code_id);
                CREATE INDEX idx_device_classifications_device_class ON device_classifications(device_class);
            """)
            log_info("已创建device_classifications表")
            
            # 创建premarket_submissions表 - 上市前提交信息
            self.cur.execute("""
                CREATE TABLE premarket_submissions (
                    id SERIAL PRIMARY KEY,
                    submission_number TEXT UNIQUE NOT NULL,
                    submission_type TEXT,
                    supplement_number TEXT,
                    decision_date DATE,
                    decision TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE INDEX idx_premarket_submissions_number ON premarket_submissions(submission_number);
            """)
            log_info("已创建premarket_submissions表")
            
            # 创建device_premarket_submissions表 - 设备与上市前提交的多对多关系
            self.cur.execute("""
                CREATE TABLE device_premarket_submissions (
                    id SERIAL PRIMARY KEY,
                    device_id INTEGER NOT NULL,
                    device_type TEXT NOT NULL, -- 表示设备来自哪个表：'classification', 'recall', 'adverse_event', 'udi'
                    submission_id INTEGER REFERENCES premarket_submissions(id) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(device_id, device_type, submission_id)
                );
                
                CREATE INDEX idx_device_premarket_submissions_device ON device_premarket_submissions(device_id, device_type);
                CREATE INDEX idx_device_premarket_submissions_submission ON device_premarket_submissions(submission_id);
            """)
            log_info("已创建device_premarket_submissions表")
            
            # 创建device_recalls表 - 设备召回信息
            self.cur.execute("""
                CREATE TABLE device_recalls (
                    id SERIAL PRIMARY KEY,
                    recall_number TEXT NOT NULL,
                    cfres_id TEXT,
                    res_event_number TEXT,
                    status TEXT,
                    classification TEXT,
                    product_code TEXT,
                    product_code_id INTEGER REFERENCES product_codes(id),
                    product_type TEXT,
                    event_id TEXT,
                    event_date_initiated DATE,
                    event_date_posted DATE,
                    recall_initiation_date DATE,
                    center_classification_date DATE,
                    report_date DATE,
                    recalling_firm TEXT,
                    company_id INTEGER REFERENCES companies(id),
                    address_1 TEXT,
                    address_2 TEXT,
                    city TEXT,
                    state TEXT,
                    postal_code TEXT,
                    country TEXT,
                    voluntary_mandated TEXT,
                    initial_firm_notification TEXT,
                    product_description TEXT,
                    product_quantity TEXT,
                    code_info TEXT,
                    reason_for_recall TEXT,
                    root_cause_description TEXT,
                    action TEXT,
                    distribution_pattern TEXT,
                    additional_info_contact TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(recall_number)
                );
                
                CREATE INDEX idx_device_recalls_product_code ON device_recalls(product_code);
                CREATE INDEX idx_device_recalls_product_code_id ON device_recalls(product_code_id);
                CREATE INDEX idx_device_recalls_status ON device_recalls(status);
                CREATE INDEX idx_device_recalls_classification ON device_recalls(classification);
            """)
            log_info("已创建device_recalls表")
            
            # 创建recall_code_info表 - 结构化召回代码信息
            self.cur.execute("""
                CREATE TABLE recall_code_info (
                    id SERIAL PRIMARY KEY,
                    recall_id INTEGER REFERENCES device_recalls(id) NOT NULL,
                    item_type TEXT, -- 'lot', 'serial', 'udi', 'expiration', etc.
                    item_value TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE INDEX idx_recall_code_info_recall_id ON recall_code_info(recall_id);
            """)
            log_info("已创建recall_code_info表")
            
            # 创建recall_pma_numbers表 - 召回PMA号码的关联表
            self.cur.execute("""
                CREATE TABLE recall_pma_numbers (
                    id SERIAL PRIMARY KEY,
                    recall_id INTEGER REFERENCES device_recalls(id) NOT NULL,
                    pma_number TEXT NOT NULL,
                    submission_id INTEGER REFERENCES premarket_submissions(id),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(recall_id, pma_number)
                );
                
                CREATE INDEX idx_recall_pma_numbers_recall_id ON recall_pma_numbers(recall_id);
                CREATE INDEX idx_recall_pma_numbers_pma_number ON recall_pma_numbers(pma_number);
            """)
            log_info("已创建recall_pma_numbers表")
            
            # 创建adverse_events表 - 不良事件基本信息
            self.cur.execute("""
                CREATE TABLE adverse_events (
                    id SERIAL PRIMARY KEY,
                    report_number TEXT NOT NULL,
                    mdr_report_key TEXT,
                    event_type TEXT,
                    event_key TEXT,
                    date_received DATE,
                    date_of_event DATE,
                    date_report DATE,
                    date_manufacturer_received DATE,
                    date_added DATE,
                    date_changed DATE,
                    reporter_occupation_code TEXT,
                    reporter_country_code TEXT,
                    health_professional BOOLEAN,
                    report_source_code TEXT,
                    manufacturer_name TEXT,
                    company_id INTEGER REFERENCES companies(id),
                    manufacturer_link_flag BOOLEAN,
                    summary_report_flag BOOLEAN,
                    pma_pmn_number TEXT,
                    submission_id INTEGER REFERENCES premarket_submissions(id),
                    previous_use_code TEXT,
                    removal_correction_number TEXT,
                    single_use_flag BOOLEAN,
                    reprocessed_and_reused_flag BOOLEAN,
                    type_of_report TEXT[],
                    adverse_event_flag BOOLEAN,
                    product_problem_flag BOOLEAN,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(report_number)
                );
                
                CREATE INDEX idx_adverse_events_report_number ON adverse_events(report_number);
                CREATE INDEX idx_adverse_events_date_received ON adverse_events(date_received);
                CREATE INDEX idx_adverse_events_event_type ON adverse_events(event_type);
            """)
            log_info("已创建adverse_events表")
            
            # 创建event_devices表 - 不良事件中涉及的设备
            self.cur.execute("""
                CREATE TABLE event_devices (
                    id SERIAL PRIMARY KEY,
                    event_id INTEGER REFERENCES adverse_events(id) NOT NULL,
                    device_sequence_number TEXT,
                    brand_name TEXT,
                    generic_name TEXT,
                    manufacturer_name TEXT,
                    manufacturer_address_1 TEXT,
                    manufacturer_address_2 TEXT,
                    manufacturer_city TEXT,
                    manufacturer_state TEXT,
                    manufacturer_postal_code TEXT,
                    manufacturer_country TEXT,
                    manufacturer_zip_code TEXT,
                    manufacturer_zip_code_ext TEXT,
                    model_number TEXT,
                    catalog_number TEXT,
                    lot_number TEXT,
                    expiration_date DATE,
                    device_availability TEXT,
                    device_operator TEXT,
                    device_age_text TEXT,
                    device_evaluated_by_manufacturer TEXT,
                    implant_flag BOOLEAN,
                    product_code TEXT,
                    product_code_id INTEGER REFERENCES product_codes(id),
                    udi_di TEXT,
                    udi_public TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE INDEX idx_event_devices_event_id ON event_devices(event_id);
                CREATE INDEX idx_event_devices_product_code ON event_devices(product_code);
                CREATE INDEX idx_event_devices_brand_name ON event_devices(brand_name);
            """)
            log_info("已创建event_devices表")
            
            # 创建event_patients表 - 不良事件中涉及的患者
            self.cur.execute("""
                CREATE TABLE event_patients (
                    id SERIAL PRIMARY KEY,
                    event_id INTEGER REFERENCES adverse_events(id) NOT NULL,
                    patient_sequence_number TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE INDEX idx_event_patients_event_id ON event_patients(event_id);
            """)
            log_info("已创建event_patients表")
            
            # 创建patient_problems表 - 患者问题信息
            self.cur.execute("""
                CREATE TABLE patient_problems (
                    id SERIAL PRIMARY KEY,
                    patient_id INTEGER REFERENCES event_patients(id) NOT NULL,
                    problem TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE INDEX idx_patient_problems_patient_id ON patient_problems(patient_id);
            """)
            log_info("已创建patient_problems表")
            
            # 创建product_problems表 - 产品问题信息
            self.cur.execute("""
                CREATE TABLE product_problems (
                    id SERIAL PRIMARY KEY,
                    event_id INTEGER REFERENCES adverse_events(id) NOT NULL,
                    problem TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE INDEX idx_product_problems_event_id ON product_problems(event_id);
            """)
            log_info("已创建product_problems表")
            
            # 创建event_texts表 - 事件文本信息
            self.cur.execute("""
                CREATE TABLE event_texts (
                    id SERIAL PRIMARY KEY,
                    event_id INTEGER REFERENCES adverse_events(id) NOT NULL,
                    text_type_code TEXT,
                    text TEXT,
                    patient_sequence_number TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE INDEX idx_event_texts_event_id ON event_texts(event_id);
                CREATE INDEX idx_event_texts_text_type ON event_texts(text_type_code);
            """)
            log_info("已创建event_texts表")
            
            # 创建udi_records表 - UDI记录基本信息
            self.cur.execute("""
                CREATE TABLE udi_records (
                    id SERIAL PRIMARY KEY,
                    public_device_record_key TEXT NOT NULL,
                    device_description TEXT,
                    brand_name TEXT,
                    version_or_model_number TEXT,
                    company_name TEXT,
                    company_id INTEGER REFERENCES companies(id),
                    labeler_duns_number TEXT,
                    record_status TEXT,
                    public_version_number TEXT,
                    public_version_date DATE,
                    public_version_status TEXT,
                    publish_date DATE,
                    is_single_use BOOLEAN,
                    is_kit BOOLEAN,
                    is_combination_product BOOLEAN,
                    is_hct_p BOOLEAN,
                    is_pm_exempt BOOLEAN,
                    is_direct_marking_exempt BOOLEAN,
                    has_lot_or_batch_number BOOLEAN,
                    has_serial_number BOOLEAN,
                    has_manufacturing_date BOOLEAN,
                    has_expiration_date BOOLEAN,
                    has_donation_id_number BOOLEAN,
                    is_labeled_as_nrl BOOLEAN,
                    is_labeled_as_no_nrl BOOLEAN,
                    is_rx BOOLEAN,
                    is_otc BOOLEAN,
                    mri_safety TEXT,
                    commercial_distribution_status TEXT,
                    device_count_in_base_package TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(public_device_record_key)
                );
                
                CREATE INDEX idx_udi_records_brand_name ON udi_records(brand_name);
                CREATE INDEX idx_udi_records_labeler_duns ON udi_records(labeler_duns_number);
            """)
            log_info("已创建udi_records表")
            
            # 创建udi_product_codes表 - UDI产品代码多对多关系
            self.cur.execute("""
                CREATE TABLE udi_product_codes (
                    id SERIAL PRIMARY KEY,
                    udi_record_id INTEGER REFERENCES udi_records(id) NOT NULL,
                    product_code TEXT NOT NULL,
                    product_code_id INTEGER REFERENCES product_codes(id),
                    device_name TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(udi_record_id, product_code)
                );
                
                CREATE INDEX idx_udi_product_codes_udi_id ON udi_product_codes(udi_record_id);
                CREATE INDEX idx_udi_product_codes_product_code ON udi_product_codes(product_code);
            """)
            log_info("已创建udi_product_codes表")
            
            # 创建udi_identifiers表 - UDI标识符信息
            self.cur.execute("""
                CREATE TABLE udi_identifiers (
                    id SERIAL PRIMARY KEY,
                    udi_record_id INTEGER REFERENCES udi_records(id) NOT NULL,
                    identifier_type TEXT NOT NULL,
                    issuing_agency TEXT,
                    identifier_value TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE INDEX idx_udi_identifiers_udi_id ON udi_identifiers(udi_record_id);
                CREATE INDEX idx_udi_identifiers_type_value ON udi_identifiers(identifier_type, identifier_value);
            """)
            log_info("已创建udi_identifiers表")
            
            # 创建udi_sterilization表 - UDI灭菌信息
            self.cur.execute("""
                CREATE TABLE udi_sterilization (
                    id SERIAL PRIMARY KEY,
                    udi_record_id INTEGER REFERENCES udi_records(id) NOT NULL,
                    is_sterile BOOLEAN,
                    is_sterilization_prior_use BOOLEAN,
                    sterilization_methods TEXT[],
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(udi_record_id)
                );
                
                CREATE INDEX idx_udi_sterilization_udi_id ON udi_sterilization(udi_record_id);
            """)
            log_info("已创建udi_sterilization表")
            
            # 创建udi_device_sizes表 - UDI设备尺寸信息
            self.cur.execute("""
                CREATE TABLE udi_device_sizes (
                    id SERIAL PRIMARY KEY,
                    udi_record_id INTEGER REFERENCES udi_records(id) NOT NULL,
                    size_type TEXT,
                    size_value TEXT,
                    size_unit TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE INDEX idx_udi_device_sizes_udi_id ON udi_device_sizes(udi_record_id);
            """)
            log_info("已创建udi_device_sizes表")
            
            # 创建udi_gmdn_terms表 - UDI GMDN术语信息
            self.cur.execute("""
                CREATE TABLE udi_gmdn_terms (
                    id SERIAL PRIMARY KEY,
                    udi_record_id INTEGER REFERENCES udi_records(id) NOT NULL,
                    code TEXT,
                    name TEXT,
                    definition TEXT,
                    implantable BOOLEAN,
                    code_status TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE INDEX idx_udi_gmdn_terms_udi_id ON udi_gmdn_terms(udi_record_id);
                CREATE INDEX idx_udi_gmdn_terms_code ON udi_gmdn_terms(code);
            """)
            log_info("已创建udi_gmdn_terms表")
            
            # 创建udi_customer_contacts表 - UDI客户联系信息
            self.cur.execute("""
                CREATE TABLE udi_customer_contacts (
                    id SERIAL PRIMARY KEY,
                    udi_record_id INTEGER REFERENCES udi_records(id) NOT NULL,
                    contact_type TEXT,
                    contact_value TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE INDEX idx_udi_customer_contacts_udi_id ON udi_customer_contacts(udi_record_id);
            """)
            log_info("已创建udi_customer_contacts表")
            
            # 创建udi_premarket_submissions表 - UDI上市前提交信息
            self.cur.execute("""
                CREATE TABLE udi_premarket_submissions (
                    id SERIAL PRIMARY KEY,
                    udi_record_id INTEGER REFERENCES udi_records(id) NOT NULL,
                    submission_number TEXT NOT NULL,
                    submission_id INTEGER REFERENCES premarket_submissions(id),
                    supplement_number TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE INDEX idx_udi_premarket_submissions_udi_id ON udi_premarket_submissions(udi_record_id);
                CREATE INDEX idx_udi_premarket_submissions_number ON udi_premarket_submissions(submission_number);
            """)
            log_info("已创建udi_premarket_submissions表")
            
            # 创建enforcement_actions表 - 执法行动表（与device_recalls区分）
            # 修改这里：改用recall_number作为主键字段
            self.cur.execute("""
                CREATE TABLE enforcement_actions (
                    id SERIAL PRIMARY KEY,
                    recall_number TEXT NOT NULL,
                    status TEXT,
                    classification TEXT,
                    product_code TEXT,
                    product_code_id INTEGER REFERENCES product_codes(id),
                    product_type TEXT,
                    event_id TEXT,
                    event_date_initiated DATE,
                    event_date_posted DATE,
                    enforcement_initiation_date DATE,
                    center_classification_date DATE,
                    report_date DATE,
                    firm_name TEXT,
                    company_id INTEGER REFERENCES companies(id),
                    address_1 TEXT,
                    address_2 TEXT,
                    city TEXT,
                    state TEXT,
                    postal_code TEXT,
                    country TEXT,
                    voluntary_mandated TEXT,
                    initial_firm_notification TEXT,
                    product_description TEXT,
                    action TEXT,
                    distribution_pattern TEXT,
                    code_info TEXT,
                    reason_for_recall TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(recall_number)
                );
                
                CREATE INDEX idx_enforcement_actions_product_code ON enforcement_actions(product_code);
                CREATE INDEX idx_enforcement_actions_status ON enforcement_actions(status);
                CREATE INDEX idx_enforcement_actions_event_id ON enforcement_actions(event_id);
            """)
            log_info("已创建enforcement_actions表")
            
            # 创建OpenFDA数据表 - 存储OpenFDA基本信息
            self.cur.execute("""
                CREATE TABLE openfda_data (
                    id SERIAL PRIMARY KEY,
                    entity_id INTEGER NOT NULL,
                    entity_type TEXT NOT NULL,
                    device_name TEXT,
                    device_class TEXT,
                    regulation_number TEXT,
                    medical_specialty_description TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(entity_id, entity_type)
                );
                
                CREATE INDEX idx_openfda_data_entity ON openfda_data(entity_id, entity_type);
                CREATE INDEX idx_openfda_data_device_class ON openfda_data(device_class);
            """)
            log_info("已创建openfda_data表")
            
            # 创建OpenFDA标识符表 - 存储各种标识符
            self.cur.execute("""
                CREATE TABLE openfda_identifiers (
                    id SERIAL PRIMARY KEY,
                    openfda_id INTEGER REFERENCES openfda_data(id) NOT NULL,
                    identifier_type TEXT NOT NULL,
                    identifier_value TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(openfda_id, identifier_type, identifier_value)
                );
                
                CREATE INDEX idx_openfda_identifiers_type_value ON openfda_identifiers(identifier_type, identifier_value);
            """)
            log_info("已创建openfda_identifiers表")
            
            # 创建dataset_metadata表 - 元数据表
            self.cur.execute("""
                -- 元数据表记录最后更新时间
                CREATE TABLE dataset_metadata (
                    id SERIAL PRIMARY KEY,
                    dataset_name TEXT UNIQUE NOT NULL,
                    last_updated TIMESTAMP,
                    source TEXT,
                    record_count INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            log_info("已创建dataset_metadata表")
            
            # 预填充一些基础数据
            
            # 预填充医疗专业
            self.cur.execute("""
                INSERT INTO medical_specialties (code, description) VALUES
                ('AN', 'Anesthesiology'),
                ('CV', 'Cardiovascular'),
                ('DE', 'Dental'),
                ('GU', 'Gastroenterology, Urology'),
                ('HO', 'Hematology'),
                ('MI', 'Microbiology'),
                ('NE', 'Neurology'),
                ('OB', 'Obstetrics/Gynecology'),
                ('OP', 'Ophthalmic'),
                ('OR', 'Orthopedic'),
                ('PM', 'Physical Medicine'),
                ('RA', 'Radiology'),
                ('SU', 'General, Plastic Surgery'),
                ('TX', 'Clinical Toxicology'),
                ('', 'Unknown')
                ON CONFLICT (code) DO NOTHING;
            """)
            log_info("已预填充medical_specialties表")
            
            # 预填充提交类型
            self.cur.execute("""
                INSERT INTO submission_types (submission_type_id, description) VALUES
                ('1', 'Premarket Notification - 510(k)'),
                ('2', 'Premarket Approval - PMA'),
                ('3', 'Product Development Protocol - PDP'),
                ('4', 'Exempt'),
                ('5', 'Pre-AMEND (Pre-amendment)'),
                ('6', 'Transitional'),
                ('9', 'Emergency Use Authorization - EUA'),
                ('C', 'HDE - Humanitarian Device Exemption'),
                ('D', 'De Novo')
                ON CONFLICT (submission_type_id) DO NOTHING;
            """)
            log_info("已预填充submission_types表")
            
            # 提交事务
            self.conn.commit()
            log_success("数据库模式创建成功并已提交")
            
            return True
            
        except Exception as e:
            self.conn.rollback()
            log_error(f"创建数据库模式失败: {str(e)}")
            return False