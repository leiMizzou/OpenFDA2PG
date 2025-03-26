-- 自动生成的udi数据表结构
-- 生成日期: 2025-03-26 01:11:42
-- 分析文件数: 44
-- 分析记录数: 436508
-- 唯一字段路径: 160
-- 数组路径: 8
-- 简单数组: 1

-- 添加UUID扩展 (如果尚未添加)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE udi (
    id VARCHAR(128) PRIMARY KEY DEFAULT uuid_generate_v4(),
    has_donation_id_number BOOLEAN,
    mri_safety VARCHAR(128),
    record_status VARCHAR(128),
    is_labeled_as_nrl BOOLEAN,
    commercial_distribution_status TEXT,
    public_device_record_key VARCHAR(128),
    has_serial_number BOOLEAN,
    public_version_date DATE,
    public_version_number VARCHAR(128),
    labeler_duns_number VARCHAR(128),
    identifiers TEXT,
    is_single_use BOOLEAN,
    has_manufacturing_date DATE,
    brand_name TEXT,
    product_codes TEXT,
    device_count_in_base_package VARCHAR(128),
    is_pm_exempt BOOLEAN,
    has_lot_or_batch_number BOOLEAN,
    public_version_status VARCHAR(128),
    has_expiration_date DATE,
    gmdn_terms TEXT,
    publish_date DATE,
    company_name TEXT,
    sterilization TEXT,
    version_or_model_number VARCHAR(128),
    is_rx BOOLEAN,
    is_otc BOOLEAN,
    is_combination_product BOOLEAN,
    is_kit BOOLEAN,
    is_hct_p BOOLEAN,
    is_labeled_as_no_nrl BOOLEAN,
    is_direct_marking_exempt BOOLEAN,
    device_description TEXT,
    customer_contacts TEXT,
    catalog_number DATE,
    premarket_submissions TEXT,
    device_sizes TEXT,
    storage TEXT,
    commercial_distribution_end_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 添加UUID扩展 (如果尚未添加)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE udi_customer_contacts (
    id VARCHAR(128) PRIMARY KEY DEFAULT uuid_generate_v4(),
    udi_id VARCHAR(128) REFERENCES udi(id),
    phone VARCHAR(128),
    email VARCHAR(128),
    ext VARCHAR(128),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_udi_customer_contacts_udi_id ON udi_customer_contacts(udi_id);

-- 添加UUID扩展 (如果尚未添加)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE udi_device_sizes (
    id VARCHAR(128) PRIMARY KEY DEFAULT uuid_generate_v4(),
    udi_id VARCHAR(128) REFERENCES udi(id),
    type VARCHAR(128),
    unit VARCHAR(128),
    value DECIMAL(12,2),
    text TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_udi_device_sizes_udi_id ON udi_device_sizes(udi_id);

-- 添加UUID扩展 (如果尚未添加)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE udi_gmdn_terms (
    id VARCHAR(128) PRIMARY KEY DEFAULT uuid_generate_v4(),
    udi_id VARCHAR(128) REFERENCES udi(id),
    code TEXT,
    name TEXT,
    definition TEXT,
    implantable BOOLEAN,
    code_status TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_udi_gmdn_terms_udi_id ON udi_gmdn_terms(udi_id);

-- 添加UUID扩展 (如果尚未添加)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE udi_identifiers (
    id VARCHAR(128) PRIMARY KEY DEFAULT uuid_generate_v4(),
    udi_id VARCHAR(128) REFERENCES udi(id),
    id VARCHAR(128),
    type VARCHAR(128),
    issuing_agency VARCHAR(128),
    quantity_per_package VARCHAR(128),
    unit_of_use_id VARCHAR(128),
    package_status VARCHAR(128),
    package_type VARCHAR(128),
    package_discontinue_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_udi_identifiers_udi_id ON udi_identifiers(udi_id);

-- 添加UUID扩展 (如果尚未添加)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE udi_premarket_submissions (
    id VARCHAR(128) PRIMARY KEY DEFAULT uuid_generate_v4(),
    udi_id VARCHAR(128) REFERENCES udi(id),
    submission_number VARCHAR(128),
    supplement_number VARCHAR(128),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_udi_premarket_submissions_udi_id ON udi_premarket_submissions(udi_id);

-- 添加UUID扩展 (如果尚未添加)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE udi_product_codes (
    id VARCHAR(128) PRIMARY KEY DEFAULT uuid_generate_v4(),
    udi_id VARCHAR(128) REFERENCES udi(id),
    code TEXT,
    name TEXT,
    openfda TEXT,
    device_name TEXT,
    medical_specialty_description TEXT,
    regulation_number VARCHAR(128),
    device_class VARCHAR(128),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_udi_product_codes_udi_id ON udi_product_codes(udi_id);

-- 添加UUID扩展 (如果尚未添加)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE udi_sterilization (
    id VARCHAR(128) PRIMARY KEY DEFAULT uuid_generate_v4(),
    udi_id VARCHAR(128) REFERENCES udi(id),
    is_sterile BOOLEAN,
    is_sterilization_prior_use BOOLEAN,
    sterilization_methods VARCHAR(128),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_udi_sterilization_udi_id ON udi_sterilization(udi_id);

-- 添加UUID扩展 (如果尚未添加)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE udi_storage (
    id VARCHAR(128) PRIMARY KEY DEFAULT uuid_generate_v4(),
    udi_id VARCHAR(128) REFERENCES udi(id),
    type VARCHAR(128),
    high TEXT,
    low TEXT,
    special_conditions TEXT,
    unit VARCHAR(128),
    value VARCHAR(128),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_udi_storage_udi_id ON udi_storage(udi_id);

-- 添加UUID扩展 (如果尚未添加)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE udi_sterilization_sterilization_methods (
    id VARCHAR(128) PRIMARY KEY DEFAULT uuid_generate_v4(),
    sterilization_id VARCHAR(128) REFERENCES udi_sterilization(id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_udi_sterilization_sterilization_methods_sterilization_id ON udi_sterilization_sterilization_methods(sterilization_id);
