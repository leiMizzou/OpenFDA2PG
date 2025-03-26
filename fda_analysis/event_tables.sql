-- 自动生成的event数据表结构
-- 生成日期: 2025-03-26 01:27:42
-- 分析文件数: 8
-- 分析记录数: 709497
-- 唯一字段路径: 205
-- 数组路径: 15
-- 简单数组: 12

-- 添加UUID扩展 (如果尚未添加)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE event (
    id VARCHAR(128) PRIMARY KEY DEFAULT uuid_generate_v4(),
    manufacturer_contact_zip_ext VARCHAR(128),
    manufacturer_g1_address_2 TEXT,
    event_location VARCHAR(255),
    report_to_fda TEXT,
    manufacturer_contact_t_name TEXT,
    manufacturer_contact_state VARCHAR(255),
    manufacturer_link_flag BOOLEAN,
    manufacturer_contact_address_2 TEXT,
    manufacturer_g1_city VARCHAR(255),
    manufacturer_contact_address_1 TEXT,
    manufacturer_contact_pcity DATE,
    event_type VARCHAR(128),
    report_number VARCHAR(128),
    type_of_report TEXT,
    product_problem_flag BOOLEAN,
    date_received DATE,
    manufacturer_address_2 TEXT,
    pma_pmn_number VARCHAR(128),
    reprocessed_and_reused_flag BOOLEAN,
    manufacturer_address_1 TEXT,
    exemption_number VARCHAR(128),
    manufacturer_contact_zip_code VARCHAR(128),
    reporter_occupation_code TEXT,
    manufacturer_contact_plocal VARCHAR(128),
    noe_summarized VARCHAR(128),
    manufacturer_contact_l_name TEXT,
    source_type VARCHAR(128),
    distributor_zip_code_ext VARCHAR(128),
    manufacturer_g1_postal_code VARCHAR(128),
    manufacturer_g1_state VARCHAR(255),
    reporter_country_code TEXT,
    manufacturer_contact_area_code VARCHAR(128),
    date_added DATE,
    manufacturer_contact_f_name TEXT,
    previous_use_code TEXT,
    device TEXT,
    manufacturer_zip_code VARCHAR(128),
    suppl_dates_mfr_received DATE,
    manufacturer_contact_country VARCHAR(255),
    date_changed DATE,
    health_professional BOOLEAN,
    summary_report_flag BOOLEAN,
    manufacturer_g1_zip_code_ext VARCHAR(128),
    manufacturer_contact_extension VARCHAR(128),
    manufacturer_city VARCHAR(255),
    manufacturer_contact_phone_number VARCHAR(128),
    patient TEXT,
    distributor_city VARCHAR(255),
    initial_report_to_fda TEXT,
    distributor_state VARCHAR(255),
    event_key VARCHAR(128),
    manufacturer_g1_country VARCHAR(255),
    manufacturer_contact_city VARCHAR(255),
    mdr_report_key DATE,
    removal_correction_number VARCHAR(128),
    number_devices_in_event VARCHAR(128),
    manufacturer_name TEXT,
    report_source_code TEXT,
    remedial_action TEXT,
    manufacturer_g1_zip_code VARCHAR(128),
    report_to_manufacturer TEXT,
    manufacturer_zip_code_ext VARCHAR(128),
    manufacturer_g1_name TEXT,
    adverse_event_flag BOOLEAN,
    distributor_address_1 TEXT,
    manufacturer_state VARCHAR(255),
    distributor_address_2 TEXT,
    manufacturer_postal_code VARCHAR(128),
    single_use_flag BOOLEAN,
    manufacturer_country VARCHAR(255),
    mdr_text TEXT,
    number_patients_in_event VARCHAR(128),
    distributor_name TEXT,
    manufacturer_g1_address_1 TEXT,
    distributor_zip_code VARCHAR(128),
    manufacturer_contact_postal_code VARCHAR(128),
    manufacturer_contact_exchange VARCHAR(128),
    manufacturer_contact_pcountry VARCHAR(255),
    suppl_dates_fda_received DATE,
    product_problems TEXT,
    date_of_event DATE,
    date_report DATE,
    date_manufacturer_received DATE,
    device_date_of_manufacturer DATE,
    date_facility_aware DATE,
    report_date DATE,
    date_report_to_fda DATE,
    date_report_to_manufacturer DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 添加UUID扩展 (如果尚未添加)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE event_device (
    id VARCHAR(128) PRIMARY KEY DEFAULT uuid_generate_v4(),
    event_id VARCHAR(128) REFERENCES event(id),
    device_event_key VARCHAR(128),
    implant_flag BOOLEAN,
    date_removed_flag DATE,
    device_sequence_number VARCHAR(128),
    date_received DATE,
    brand_name TEXT,
    generic_name TEXT,
    manufacturer_d_name TEXT,
    manufacturer_d_address_1 TEXT,
    manufacturer_d_address_2 TEXT,
    manufacturer_d_city VARCHAR(255),
    manufacturer_d_state VARCHAR(255),
    manufacturer_d_zip_code VARCHAR(128),
    manufacturer_d_zip_code_ext VARCHAR(128),
    manufacturer_d_country VARCHAR(255),
    manufacturer_d_postal_code VARCHAR(128),
    device_operator VARCHAR(128),
    model_number VARCHAR(128),
    catalog_number VARCHAR(128),
    lot_number DATE,
    other_id_number VARCHAR(128),
    device_availability VARCHAR(128),
    device_report_product_code TEXT,
    device_age_text TEXT,
    device_evaluated_by_manufacturer BOOLEAN,
    combination_product_flag BOOLEAN,
    udi_di VARCHAR(128),
    udi_public VARCHAR(128),
    openfda TEXT,
    date_returned_to_manufacturer DATE,
    expiration_date_of_device DATE,
    device_name TEXT,
    medical_specialty_description TEXT,
    regulation_number VARCHAR(128),
    device_class VARCHAR(128),
    registration_number TEXT,
    fei_number TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_event_device_event_id ON event_device(event_id);

-- 添加UUID扩展 (如果尚未添加)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE event_mdr_text (
    id VARCHAR(128) PRIMARY KEY DEFAULT uuid_generate_v4(),
    event_id VARCHAR(128) REFERENCES event(id),
    mdr_text_key VARCHAR(128),
    text_type_code TEXT,
    patient_sequence_number VARCHAR(128),
    text TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_event_mdr_text_event_id ON event_mdr_text(event_id);

-- 添加UUID扩展 (如果尚未添加)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE event_patient (
    id VARCHAR(128) PRIMARY KEY DEFAULT uuid_generate_v4(),
    event_id VARCHAR(128) REFERENCES event(id),
    patient_sequence_number VARCHAR(128),
    patient_problems TEXT,
    date_received DATE,
    sequence_number_treatment TEXT,
    sequence_number_outcome TEXT,
    patient_age VARCHAR(128),
    patient_sex VARCHAR(128),
    patient_weight VARCHAR(128),
    patient_ethnicity VARCHAR(255),
    patient_race VARCHAR(128),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_event_patient_event_id ON event_patient(event_id);
