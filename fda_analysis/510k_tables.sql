-- 自动生成的510k数据表结构
-- 生成日期: 2025-03-26 00:43:52
-- 分析文件数: 1
-- 分析记录数: 171029
-- 唯一字段路径: 32
-- 数组路径: 2
-- 简单数组: 2

-- 添加UUID扩展 (如果尚未添加)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE 510k (
    id VARCHAR(128) PRIMARY KEY DEFAULT uuid_generate_v4(),
    third_party_flag BOOLEAN,
    city VARCHAR(255),
    advisory_committee_description TEXT,
    address_1 TEXT,
    address_2 TEXT,
    statement_or_summary TEXT,
    product_code TEXT,
    openfda TEXT,
    zip_code VARCHAR(128),
    applicant VARCHAR(128),
    decision_date DATE,
    decision_code TEXT,
    country_code TEXT,
    device_name TEXT,
    advisory_committee VARCHAR(128),
    contact VARCHAR(128),
    expedited_review_flag BOOLEAN,
    k_number TEXT,
    state VARCHAR(255),
    date_received DATE,
    review_advisory_committee VARCHAR(128),
    postal_code VARCHAR(128),
    decision_description TEXT,
    clearance_type VARCHAR(128),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 添加UUID扩展 (如果尚未添加)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE 510k_openfda (
    id VARCHAR(128) PRIMARY KEY DEFAULT uuid_generate_v4(),
    510k_id VARCHAR(128) REFERENCES 510k(id),
    device_name TEXT,
    medical_specialty_description TEXT,
    regulation_number VARCHAR(128),
    device_class VARCHAR(128),
    registration_number TEXT,
    fei_number TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_510k_openfda_510k_id ON 510k_openfda(510k_id);

-- 添加UUID扩展 (如果尚未添加)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE 510k_openfda_fei_number (
    id VARCHAR(128) PRIMARY KEY DEFAULT uuid_generate_v4(),
    openfda_id VARCHAR(128) REFERENCES 510k_openfda(id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_510k_openfda_fei_number_openfda_id ON 510k_openfda_fei_number(openfda_id);

-- 添加UUID扩展 (如果尚未添加)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE 510k_openfda_registration_number (
    id VARCHAR(128) PRIMARY KEY DEFAULT uuid_generate_v4(),
    openfda_id VARCHAR(128) REFERENCES 510k_openfda(id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_510k_openfda_registration_number_openfda_id ON 510k_openfda_registration_number(openfda_id);
