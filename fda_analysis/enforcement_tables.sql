-- 自动生成的enforcement数据表结构
-- 生成日期: 2025-03-25 23:14:24
-- 分析文件数: 1
-- 分析记录数: 35451
-- 唯一字段路径: 25
-- 数组路径: 0
-- 简单数组: 0

-- 添加UUID扩展 (如果尚未添加)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE enforcement (
    id VARCHAR(128) PRIMARY KEY DEFAULT uuid_generate_v4(),
    status VARCHAR(128),
    city VARCHAR(255),
    state VARCHAR(255),
    country VARCHAR(255),
    classification VARCHAR(128),
    openfda TEXT,
    product_type VARCHAR(128),
    event_id VARCHAR(128),
    recalling_firm VARCHAR(128),
    address_1 TEXT,
    address_2 TEXT,
    postal_code VARCHAR(128),
    voluntary_mandated DATE,
    initial_firm_notification TEXT,
    distribution_pattern TEXT,
    recall_number VARCHAR(128),
    product_description TEXT,
    product_quantity VARCHAR(128),
    reason_for_recall TEXT,
    recall_initiation_date DATE,
    report_date DATE,
    code_info TEXT,
    center_classification_date DATE,
    termination_date DATE,
    more_code_info TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
