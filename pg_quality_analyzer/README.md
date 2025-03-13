# PostgreSQL 数据质量检查与分析工具

一个通用的面向PostgreSQL的数据质量检查和分析工具，能够自动检查数据库质量问题、提供优化建议，并支持通过Gemini API进行AI增强分析。

## 功能特点

- **自动数据库分析**：连接PostgreSQL数据库，自动遍历库表结构并采样数据
- **全面质量检查**：检测空值、异常值、分布异常、数据一致性等问题
- **结构化/非结构化数据识别**：自动区分结构化和非结构化字段
- **非结构化数据处理**：分析JSON、文本等非结构化内容并提供建议
- **数据预处理建议**：针对不同类型的数据提供合适的预处理策略
- **优化建议生成**：提供存储优化、查询优化、架构改进等建议
- **AI增强分析**：集成Gemini API实现智能分析和建议生成
- **可视化报告**：生成HTML、Markdown或JSON格式的分析报告

## 安装

### 要求

- Python 3.8+
- PostgreSQL 数据库
- Google Gemini API密钥(可选，用于AI增强功能)

### 步骤

1. 克隆本仓库：
   ```bash
   git clone https://github.com/yourusername/pgsql-quality-analyzer.git
   cd pgsql-quality-analyzer
   ```

2. 安装依赖：
   ```bash
   pip install -r requirements.txt
   ```

3. 创建配置文件：
   ```bash
   cp config.yaml.template config.yaml
   ```

4. 编辑配置文件，设置数据库连接参数和其他选项：
   ```bash
   # 使用任意文本编辑器编辑配置文件
   nano config.yaml  # 或者 vim, vscode 等
   ```

5. 根据需要配置Gemini API密钥（可选）：
   - 在`config.yaml`中设置`gemini.api_key`
   - 或者设置环境变量`GEMINI_API_KEY`

## 使用方法

### 基本用法

```bash
python main.py
```

这将使用`config.yaml`中的配置连接到数据库并执行分析。

### 使用指定配置文件

```bash
python main.py --config my_custom_config.yaml
```

### 通过命令行参数指定数据库连接

```bash
python main.py --host localhost --port 5432 --user postgres --password your_password --dbname your_database --schema public
```

### 调整分析参数

```bash
python main.py --sample-size 2000 --max-tables 50
```

### 禁用AI增强功能

```bash
python main.py --no-ai
```

### 完整的命令行参数列表

```
--config, -c        配置文件路径（默认：config.yaml）
--host              数据库主机地址
--port              数据库端口
--user, -u          数据库用户名
--password, -p      数据库密码
--dbname, -d        数据库名称
--schema, -s        Schema名称（默认：public）
--output-format     报告输出格式(html, markdown, json)
--output-path       报告输出路径
--max-tables        最大分析表数量
--sample-size       每表采样大小
--no-ai             禁用AI增强功能
```

## 配置文件详解

`config.yaml` 文件包含以下主要部分：

### 数据库连接配置

```yaml
database:
  host: localhost     # 数据库服务器地址
  port: 5432          # 数据库端口
  user: postgres      # 数据库用户名
  password: password  # 数据库密码
  dbname: mydb        # 数据库名称
  schema: public      # 要分析的schema
```

### 分析配置

```yaml
analysis:
  sample_size: 1000    # 每表采样数据条数
  sample_method: random  # 采样方法：random 或 sequential
  max_tables: 100        # 最大分析表数量
  exclude_tables: []     # 排除的表名列表
  include_tables: []     # 仅包含的表名列表 (如果为空，则分析所有未排除的表)
```

### 质量检查配置

```yaml
quality_checks:
  null_threshold: 0.3       # 空值率阈值
  cardinality_threshold: 0.9  # 基数比率阈值
  outlier_threshold: 3.0    # 异常值标准差倍数
  correlation_threshold: 0.8  # 相关性阈值
  enable_all: true          # 启用所有检查
  checks:
    null_analysis: true       # 空值分析
    distribution_analysis: true  # 分布分析
    consistency_analysis: true   # 一致性分析
    unstructured_analysis: true  # 非结构化数据分析
```

### Gemini API配置

```yaml
gemini:
  api_key: your_api_key     # Gemini API密钥
  model: gemini-pro         # 模型名称
  temperature: 0.2          # 温度参数（创造性）
  max_tokens: 1024          # 最大返回tokens
  enable: true              # 是否启用Gemini API
  custom_check_generation: true  # 是否启用自定义检查生成
  unstructured_analysis: true    # 是否启用非结构化数据分析
```

### 输出配置

```yaml
output:
  format: html            # 输出格式：html, markdown, json
  path: ./reports         # 输出目录
  filename: pgsql_quality_report  # 输出文件名（不含扩展名）
  show_plots: true        # 是否在报告中展示图表
```

### 日志配置

```yaml
logging:
  level: INFO             # 日志级别：DEBUG, INFO, WARNING, ERROR, CRITICAL
  file: pgsql_analyzer.log  # 日志文件路径
```

## 分析内容

该工具执行的分析内容包括：

### Schema分析
- **表结构和统计**：行数、列数、大小、索引等
- **外键关系**：表之间的关系和依赖
- **数据类型分布**：各类型列的数量和分布
- **约束分析**：主键、唯一键、外键等约束分析

### 数据质量检查
- **空值分析**
  - 高空值率列检测
  - 空值分布模式识别
  - 关联空值分析
  
- **分布分析**
  - 异常值检测
  - 分布形状分析（偏斜、峰度等）
  - 数值范围一致性检查
  - 基数分析（唯一值比例）
  
- **一致性分析**
  - 数据格式一致性检查
  - 参照完整性验证
  - 跨列关系检查
  - 类型和值域一致性
  
- **非结构化数据分析**
  - JSON结构识别和验证
  - 文本内容分析
  - 文档结构一致性检查
  - 非结构化数据质量评估

### 优化建议
- **存储优化**：表分区、压缩策略、空间回收策略
- **查询优化**：索引建议、物化视图、统计信息管理
- **维护建议**：VACUUM和ANALYZE策略、数据归档
- **架构改进**：表设计优化、字段规范化、关系改进

## AI增强功能

启用Gemini API集成后，程序可以提供更智能的分析：

### 自定义检查规则生成
- 基于数据内容自动生成检查规则
- 识别领域特定的数据质量标准
- 创建针对特定数据模式的验证规则

### 非结构化数据深度分析
- 对文本内容进行语义分析
- JSON结构智能识别和推断
- 文档内容分类和关键信息提取

### 个性化预处理建议
- 针对特定数据内容提供预处理策略
- 自适应清洗和转换建议
- 数据规范化和结构化建议

### 智能优化建议
- 基于数据访问模式的优化策略
- 数据生命周期管理建议
- 高级索引和分区策略

### 报告见解和总结
- 质量问题优先级排序
- 业务影响分析
- 高级改进建议和最佳实践

## 项目结构

```
pgsql-quality-analyzer/
├── config.py                    # 配置处理模块
├── db_connector.py              # 数据库连接
├── schema_analyzer.py           # Schema分析
├── data_sampler.py              # 数据采样
├── data_type_detector.py        # 数据类型检测
├── quality_checker/             # 质量检查引擎目录
│   ├── __init__.py
│   ├── base_checker.py
│   ├── null_checker.py
│   ├── distribution_checker.py
│   ├── consistency_checker.py
│   ├── unstructured_checker.py
│   └── custom_checker.py
├── unstructured_analyzer.py     # 非结构化数据分析
├── preprocessing_advisor.py     # 预处理策略推荐
├── optimization_analyzer.py     # 优化分析器
├── gemini_integrator.py         # Gemini API集成
├── report_generator.py          # 报告生成
├── main.py                      # 主入口
├── templates/                   # 报告模板目录
│   ├── report_template.md       # Markdown报告模板
│   └── report_template.html     # HTML报告模板(会自动生成)
├── reports/                     # 输出报告目录(程序会自动创建)
├── config.yaml.template         # 配置文件模板
├── config.yaml                  # 用户创建的实际配置文件(基于模板)
├── requirements.txt             # 依赖项
├── pgsql_analyzer.log           # 程序日志文件(运行时生成)
└── README.md                    # 项目文档
```

## 示例报告内容

生成的质量分析报告包含以下主要部分：

1. **概述部分**：数据库基本信息、表数量、总行数、问题摘要等
2. **质量评分**：基于发现问题的严重性和数量计算的总体质量评分
3. **Schema分析**：表大小分布、关系分析、列类型分布等
4. **数据质量问题**：按表和类型组织的发现问题列表
5. **优化建议**：按类型（存储、查询、维护、架构）分类的建议
6. **表详情**：各表的详细信息、列清单、发现的问题等
7. **AI见解**（如启用）：AI生成的质量评估、关键问题、改进步骤等

## 故障排除

### 常见问题

1. **连接数据库失败**
   - 检查数据库连接配置是否正确
   - 确保数据库服务器可以从您的位置访问
   - 检查用户名和密码是否正确
   - 确认用户有权限访问指定的schema

2. **Gemini API相关错误**
   - 检查API密钥是否正确
   - 确认网络连接可以访问Google API
   - 尝试使用`--no-ai`选项禁用AI功能

3. **报告生成失败**
   - 检查`reports`目录是否可写
   - 查看日志文件中的详细错误
   - 尝试更改输出格式（如使用`--output-format markdown`）

4. **分析过程非常慢**
   - 减少`sample_size`采样大小
   - 使用`max_tables`限制分析的表数量
   - 使用`include_tables`或`exclude_tables`选择性分析表

### 日志文件

程序会在指定的日志文件（默认为`pgsql_analyzer.log`）中记录详细信息。如果遇到问题，请先查看日志文件。

## 许可证

MIT

## 贡献

欢迎提交问题报告和拉取请求！如有任何问题或建议，请在GitHub上开issue或提交PR。

---

*此项目旨在帮助数据库管理员和开发人员提高PostgreSQL数据库的质量。通过自动化的检查和分析，发现潜在问题，并提供专业的优化建议。*
