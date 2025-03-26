-- 自动生成的recall数据表结构
-- 生成日期: 2025-03-26 00:40:18
-- 分析文件数: 1
-- 分析记录数: 54687
-- 唯一字段路径: 42
-- 数组路径: 6
-- 简单数组: 6

-- 添加UUID扩展 (如果尚未添加)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE recall (
    id VARCHAR(128) PRIMARY KEY DEFAULT uuid_generate_v4(),
    product_res_number VARCHAR(128),
    event_date_initiated DATE,
    recall_status VARCHAR(128),
    res_event_number VARCHAR(128),
    product_description TEXT,
    code_info TEXT,
    openfda TEXT,
    cfres_id DATE,
    root_cause_description TEXT,
    distribution_pattern TEXT,
    reason_for_recall TEXT,
    action TEXT,
    product_code TEXT,
    recalling_firm TEXT,
    city VARCHAR(255),
    state VARCHAR(255),
    postal_code VARCHAR(128),
    product_quantity TEXT,
    address_1 TEXT,
    additional_info_contact TEXT,
    event_date_terminated DATE,
    event_date_posted DATE,
    k_numbers TEXT,
    firm_fei_number DATE,
    event_date_created DATE,
    address_2 TEXT,
    other_submission_description TEXT,
    pma_numbers TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 添加UUID扩展 (如果尚未添加)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE recall_openfda (
    id VARCHAR(128) PRIMARY KEY DEFAULT uuid_generate_v4(),
    recall_id VARCHAR(128) REFERENCES recall(id),
    device_name TEXT,
    medical_specialty_description TEXT,
    regulation_number VARCHAR(128),
    device_class VARCHAR(128),
    registration_number TEXT,
    fei_number TEXT,
    k_number TEXT,
    pma_number TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_recall_openfda_recall_id ON recall_openfda(recall_id);

-- 添加UUID扩展 (如果尚未添加)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE recall_openfda_fei_number (
    id VARCHAR(128) PRIMARY KEY DEFAULT uuid_generate_v4(),
    openfda_id VARCHAR(128) REFERENCES recall_openfda(id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_recall_openfda_fei_number_openfda_id ON recall_openfda_fei_number(openfda_id);

-- 添加UUID扩展 (如果尚未添加)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE recall_openfda_k_number (
    id VARCHAR(128) PRIMARY KEY DEFAULT uuid_generate_v4(),
    openfda_id VARCHAR(128) REFERENCES recall_openfda(id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_recall_openfda_k_number_openfda_id ON recall_openfda_k_number(openfda_id);

-- 添加UUID扩展 (如果尚未添加)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE recall_openfda_pma_number (
    id VARCHAR(128) PRIMARY KEY DEFAULT uuid_generate_v4(),
    openfda_id VARCHAR(128) REFERENCES recall_openfda(id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_recall_openfda_pma_number_openfda_id ON recall_openfda_pma_number(openfda_id);

-- 添加UUID扩展 (如果尚未添加)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE recall_openfda_registration_number (
    id VARCHAR(128) PRIMARY KEY DEFAULT uuid_generate_v4(),
    openfda_id VARCHAR(128) REFERENCES recall_openfda(id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_recall_openfda_registration_number_openfda_id ON recall_openfda_registration_number(openfda_id);
