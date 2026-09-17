-- FR 2052a 数据湖建表：REF 层（引用数据）
-- 命名空间：ref
-- 运行：python python/lakehouse/run_sql_file.py sql/iceberg/01_create_ref_tables.sql
-- 幂等：全部 IF NOT EXISTS，可重复执行
--
-- REF 是小表，不做分区；Iceberg format-version 2 支持行级删除与 Time Travel，
-- 后续重述（restatement）与审计回溯都依赖它。

-- 法人实体层级：报送主体与子公司，合并口径的基础
CREATE TABLE IF NOT EXISTS ref.ref_entity_hierarchy (
    entity_code STRING COMMENT '法人实体编码',
    entity_name STRING COMMENT '法人实体名称',
    lei_code STRING COMMENT '法人实体 LEI 代码，20 位字母数字',
    parent_entity_code STRING COMMENT '上级法人实体编码，用于合并层级',
    entity_level INT COMMENT '实体层级，1 为最终母公司',
    jurisdiction STRING COMMENT '注册地国家/地区 ISO 3166-1 alpha-2',
    entity_type STRING COMMENT '实体类型：BANK/BROKER/HOLDING/SPV',
    is_material_entity BOOLEAN COMMENT '是否为 FR 2052a 重要实体',
    consolidation_method STRING COMMENT '合并方法：FULL/PROPORTIONAL/EQUITY',
    is_active BOOLEAN COMMENT '是否有效',
    effective_date DATE COMMENT '生效日期',
    expiry_date DATE COMMENT '失效日期'
)
USING iceberg
TBLPROPERTIES ('format-version' = '2');

-- 交易对手：ODS 各表的 counterparty_id / borrower_id / issuer_id 都引用它
CREATE TABLE IF NOT EXISTS ref.ref_counterparty (
    counterparty_id STRING COMMENT '交易对手编号',
    counterparty_name STRING COMMENT '交易对手名称',
    lei_code STRING COMMENT 'LEI 代码',
    counterparty_type STRING COMMENT '交易对手类型：BANK/BROKER/CORPORATE/SOVEREIGN/CENTRAL_BANK',
    country_code STRING COMMENT '国家/地区代码',
    credit_rating STRING COMMENT '信用评级',
    industry_code STRING COMMENT '行业代码'
)
USING iceberg
TBLPROPERTIES ('format-version' = '2');

-- 到期分桶：现金流按剩余期限归类
CREATE TABLE IF NOT EXISTS ref.ref_maturity_bucket (
    bucket_code STRING COMMENT '分桶编码：O/N、1-7D、8-30D 等',
    bucket_description STRING COMMENT '分桶描述',
    min_days INT COMMENT '最小剩余天数（含）',
    max_days INT COMMENT '最大剩余天数（含）',
    sort_order INT COMMENT '排序序号',
    fr2052a_display_order INT COMMENT 'FR 2052a 展示顺序'
)
USING iceberg
TBLPROPERTIES ('format-version' = '2');

-- FR 2052a 行项目字典：Section A–K 的行项目定义
CREATE TABLE IF NOT EXISTS ref.ref_fr2052a_line_items (
    line_item_code STRING COMMENT 'FR 2052a 行项目编码',
    section_code STRING COMMENT '所属 Section：A/B/C/D/E/F/G/H/I/J/K',
    line_description STRING COMMENT '行项目描述',
    parent_line_item STRING COMMENT '父级行项目编码',
    is_calculated BOOLEAN COMMENT '是否为计算项',
    calculation_formula STRING COMMENT '计算公式',
    data_type STRING COMMENT '数据类型：AMOUNT/COUNT/RATIO',
    sign_convention STRING COMMENT '符号约定：POSITIVE/NEGATIVE/EITHER',
    mandatory_flag BOOLEAN COMMENT '是否必填',
    sort_order INT COMMENT '排序序号'
)
USING iceberg
TBLPROPERTIES ('format-version' = '2');

-- 汇率：外币金额折算 USD 的唯一依据
CREATE TABLE IF NOT EXISTS ref.ref_exchange_rates (
    rate_date DATE COMMENT '汇率日期',
    from_currency STRING COMMENT '源币种 ISO 4217',
    to_currency STRING COMMENT '目标币种 ISO 4217',
    spot_rate DECIMAL(18,8) COMMENT '即期汇率',
    rate_type STRING COMMENT '汇率类型：MID/BID/ASK',
    rate_source STRING COMMENT '汇率来源：BLOOMBERG/REUTERS/CENTRAL_BANK'
)
USING iceberg
TBLPROPERTIES ('format-version' = '2');

-- 监管映射：字段到 FR 2052a 行项目与规则出处的对应，血缘展示用
CREATE TABLE IF NOT EXISTS ref.ref_regulatory_mapping (
    section_code STRING COMMENT 'FR 2052a Section',
    line_item_code STRING COMMENT 'FR 2052a 行项目编码',
    field_name STRING COMMENT '字段名',
    regulatory_reference STRING COMMENT '监管规则出处',
    rule_id STRING COMMENT '规则编号',
    haircut_rate DECIMAL(8,4) COMMENT '折扣率',
    owner STRING COMMENT '负责人或团队',
    effective_date DATE COMMENT '生效日期',
    expiry_date DATE COMMENT '失效日期'
)
USING iceberg
TBLPROPERTIES ('format-version' = '2');

-- 行为假设：存款流失率与资金流入率，现金流预测用
CREATE TABLE IF NOT EXISTS ref.ref_behavior_assumptions (
    product_category STRING COMMENT '产品类别',
    customer_segment STRING COMMENT '客户细分',
    maturity_bucket STRING COMMENT '到期分桶',
    runoff_rate DECIMAL(8,4) COMMENT '流失率',
    inflow_rate DECIMAL(8,4) COMMENT '流入率',
    effective_date DATE COMMENT '生效日期',
    expiry_date DATE COMMENT '失效日期'
)
USING iceberg
TBLPROPERTIES ('format-version' = '2');

-- 交易日历：判断工作日与节假日
CREATE TABLE IF NOT EXISTS ref.ref_calendar (
    calendar_date DATE COMMENT '日历日期',
    is_business_day BOOLEAN COMMENT '是否工作日',
    holiday_name STRING COMMENT '节假日名称',
    jurisdiction STRING COMMENT '适用国家/地区'
)
USING iceberg
TBLPROPERTIES ('format-version' = '2');

-- 校验规则：与 [02] §2.7 的 20 条 VDQ 一一对应，供数据质量引擎读取执行
CREATE TABLE IF NOT EXISTS ref.ref_validation_rules (
    rule_id STRING COMMENT '校验规则编号',
    rule_name STRING COMMENT '规则名称',
    rule_category STRING COMMENT '规则类别：COMPLETENESS/ACCURACY/CONSISTENCY/TIMELINESS/BUSINESS',
    apply_layer STRING COMMENT '适用层级：ODS/OWD/OWS/ADS',
    severity STRING COMMENT '严重度：ERROR/WARNING/INFO',
    sql_expression STRING COMMENT '校验 SQL 表达式',
    description STRING COMMENT '规则描述',
    is_active BOOLEAN COMMENT '是否启用'
)
USING iceberg
TBLPROPERTIES ('format-version' = '2');
