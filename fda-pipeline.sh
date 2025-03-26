#!/bin/bash
# FDA JSON数据分析导入Pipeline
# 这个脚本自动执行FDA JSON数据分析、验证、导入和验证的完整流程

# 设置环境变量
DATA_DIR="/path/to/fda/data" # FDA数据目录
QUARTER="2022q4"             # 数据季度
DATA_TYPE="event"            # 数据类型
OUTPUT_DIR="./fda_analysis"  # 分析输出目录
MAX_FILES=120                # 最大处理文件数
MAX_RECORDS=1000000          # 最大处理记录数
DB_NAME="fda_database"       # 数据库名称
DB_USER="postgres"           # 数据库用户名
DB_PASSWORD="your_password"  # 数据库密码
TARGET_FILE="${DATA_DIR}/${DATA_TYPE}/${QUARTER}/${DATA_TYPE}-${QUARTER}-0001-of-0006.json" # 目标导入文件

echo "====== FDA JSON数据分析导入Pipeline开始执行 ======"
echo "数据目录: $DATA_DIR"
echo "数据季度: $QUARTER"
echo "数据类型: $DATA_TYPE"
echo "分析输出目录: $OUTPUT_DIR"

# 步骤1: 分析JSON结构
echo ""
echo "===== 步骤1: 分析JSON结构 ====="
echo "执行命令: python fda_json_flattener.py --input_dir ${DATA_DIR}/${DATA_TYPE}/${QUARTER}/ --data_type ${DATA_TYPE} --max_files ${MAX_FILES} --max_records ${MAX_RECORDS} --output_dir ${OUTPUT_DIR} --recursive"

python fda_json_flattener.py --input_dir "${DATA_DIR}/${DATA_TYPE}/${QUARTER}/" \
    --data_type "${DATA_TYPE}" \
    --max_files "${MAX_FILES}" \
    --max_records "${MAX_RECORDS}" \
    --output_dir "${OUTPUT_DIR}" \
    --recursive

if [ $? -ne 0 ]; then
    echo "步骤1执行失败，请检查错误信息"
    exit 1
fi

# 步骤2: 检查路径覆盖并生成SQL
echo ""
echo "===== 步骤2: 检查路径覆盖并生成SQL ====="
echo "处理文件: $TARGET_FILE"
echo "执行命令: python json_path_coverage_checker.py --json_file ${TARGET_FILE} --csv_file ${OUTPUT_DIR}/${DATA_TYPE}_paths.csv --prefix ${DATA_TYPE} --fields_csv ${OUTPUT_DIR}/${DATA_TYPE}_fields.csv --output_sql import_data.sql"

python json_path_coverage_checker.py --json_file "${TARGET_FILE}" \
    --csv_file "${OUTPUT_DIR}/${DATA_TYPE}_paths.csv" \
    --prefix "${DATA_TYPE}" \
    --fields_csv "${OUTPUT_DIR}/${DATA_TYPE}_fields.csv" \
    --output_sql "import_data.sql"

if [ $? -ne 0 ]; then
    echo "步骤2执行失败，请检查错误信息"
    exit 1
fi

# 步骤3: 执行SQL导入数据
echo ""
echo "===== 步骤3: 执行SQL导入数据 ====="
echo "导入SQL: import_data.sql"
echo "执行命令: python execute_sql_file_enhanced.py --sql_file import_data.sql --dbname ${DB_NAME} --user ${DB_USER} --password ${DB_PASSWORD} --skip_duplicates"

python execute_sql_file_enhanced.py --sql_file "import_data.sql" \
    --dbname "${DB_NAME}" \
    --user "${DB_USER}" \
    --password "${DB_PASSWORD}" \
    --skip_duplicates

if [ $? -ne 0 ]; then
    echo "步骤3执行失败，请检查错误信息"
    exit 1
fi

# 步骤4: 验证导入的数据
echo ""
echo "===== 步骤4: 验证导入的数据 ====="
echo "执行命令: python json_node_counter.py --json_file ${TARGET_FILE} --prefix ${DATA_TYPE} --output_csv ${TARGET_FILE}_node_counts.csv"

python json_node_counter.py --json_file "${TARGET_FILE}" \
    --prefix "${DATA_TYPE}" \
    --output_csv "${TARGET_FILE}_node_counts.csv"

if [ $? -ne 0 ]; then
    echo "步骤4执行失败，请检查错误信息"
    exit 1
fi

echo ""
echo "====== FDA JSON数据分析导入Pipeline执行完成 ======"
echo "分析结果保存在: ${OUTPUT_DIR}"
echo "节点计数报告: ${TARGET_FILE}_node_counts.csv"
echo "数据已导入到数据库: ${DB_NAME}"
