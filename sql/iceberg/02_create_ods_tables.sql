-- FR 2052a 数据湖建表：ODS 层（原始业务明细）
-- 命名空间：bronze
-- 运行：python python/lakehouse/run_sql_file.py sql/iceberg/02_create_ods_tables.sql
-- 幂等：全部 IF NOT EXISTS，可重复执行
--
-- 列序与 python/generators 产出的 CSV 表头逐列一致，多出的 etl_load_timestamp
-- 由入湖作业按「报告日 + 1 天的 02:00」派生（数据时间线，不用真实时钟，见 python/consumers/kafka_to_iceberg.py）。
--
-- 相对 [99] §3.3 的三处扩展（日报批次必需）：
--   report_date  业务日期，同时作为分区键，保证按日重跑幂等
--   entity_code  记账法人实体，支撑合并口径与子公司口径
--   event_time   源系统事件时间，Kafka 重放与乱序判断用

-- 存款头寸：零售、对公、同业存款，含活期与定期
CREATE TABLE IF NOT EXISTS bronze.ods_deposits (
    source_system STRING COMMENT '来源系统编码',
    source_record_id STRING COMMENT '源系统记录主键',
    report_date DATE COMMENT '报告日',
    entity_code STRING COMMENT '记账法人实体编码',
    account_number STRING COMMENT '账户号',
    customer_id STRING COMMENT '客户编号',
    product_code STRING COMMENT '产品代码',
    deposit_type STRING COMMENT '存款类型原始编码：SAV/CHK/CD/MMDA/TIME',
    currency STRING COMMENT '币种 ISO 4217',
    principal_amount DECIMAL(18,4) COMMENT '本金金额',
    accrued_interest DECIMAL(18,4) COMMENT '应计利息',
    interest_rate DECIMAL(10,6) COMMENT '利率',
    open_date DATE COMMENT '开户日期',
    maturity_date DATE COMMENT '到期日期，活期为空',
    branch_code STRING COMMENT '机构代码',
    customer_type_raw STRING COMMENT '客户类型原始值：IND/CORP/GOV/FI；集团内配对腿为 AFFIL',
    insured_flag STRING COMMENT '是否受保存款 Y/N',
    event_time TIMESTAMP COMMENT '源系统事件时间',
    etl_batch_id STRING COMMENT 'ETL 批次号',
    etl_source_file STRING COMMENT 'ETL 来源文件',
    etl_load_timestamp TIMESTAMP COMMENT '入湖时间'
)
USING iceberg
PARTITIONED BY (days(report_date))
TBLPROPERTIES ('format-version' = '2');

-- 回购与逆回购交易
CREATE TABLE IF NOT EXISTS bronze.ods_repo_transactions (
    source_system STRING COMMENT '来源系统编码',
    source_record_id STRING COMMENT '源系统记录主键',
    report_date DATE COMMENT '报告日',
    entity_code STRING COMMENT '记账法人实体编码',
    deal_id STRING COMMENT '交易编号',
    counterparty_id STRING COMMENT '交易对手编号',
    repo_type STRING COMMENT '回购类型：REPO/REVERSE_REPO',
    currency STRING COMMENT '币种 ISO 4217',
    cash_amount DECIMAL(18,4) COMMENT '现金金额',
    collateral_market_value DECIMAL(18,4) COMMENT '抵押品市值',
    haircut_pct DECIMAL(8,4) COMMENT '折扣率',
    interest_rate DECIMAL(10,6) COMMENT '利率',
    start_date DATE COMMENT '开始日期',
    end_date DATE COMMENT '到期日期',
    collateral_isin STRING COMMENT '抵押品 ISIN',
    collateral_type_raw STRING COMMENT '抵押品类型原始值：UST/AGENCY/MBS/CORP',
    netting_agreement_id STRING COMMENT '净额结算协议编号',
    event_time TIMESTAMP COMMENT '源系统事件时间',
    etl_batch_id STRING COMMENT 'ETL 批次号',
    etl_source_file STRING COMMENT 'ETL 来源文件',
    etl_load_timestamp TIMESTAMP COMMENT '入湖时间'
)
USING iceberg
PARTITIONED BY (days(report_date))
TBLPROPERTIES ('format-version' = '2');

-- 贷款台账：已用额度与未提取额度，支撑表内外融资口径
CREATE TABLE IF NOT EXISTS bronze.ods_loans (
    source_system STRING COMMENT '来源系统编码',
    source_record_id STRING COMMENT '源系统记录主键',
    report_date DATE COMMENT '报告日',
    entity_code STRING COMMENT '记账法人实体编码',
    loan_id STRING COMMENT '贷款编号',
    borrower_id STRING COMMENT '借款人编号',
    loan_type STRING COMMENT '贷款类型原始编码',
    facility_amount DECIMAL(18,4) COMMENT '授信额度',
    outstanding_amount DECIMAL(18,4) COMMENT '未偿还余额',
    undrawn_amount DECIMAL(18,4) COMMENT '未提取额度',
    currency STRING COMMENT '币种 ISO 4217',
    interest_rate DECIMAL(10,6) COMMENT '利率',
    rate_type STRING COMMENT '利率类型：FIXED/FLOAT',
    origination_date DATE COMMENT '放款日期',
    maturity_date DATE COMMENT '到期日期',
    next_payment_date DATE COMMENT '下次还款日期',
    collateral_flag STRING COMMENT '是否有抵押 Y/N',
    borrower_type_raw STRING COMMENT '借款人类型原始值',
    credit_grade_raw STRING COMMENT '信用评级原始值',
    event_time TIMESTAMP COMMENT '源系统事件时间',
    etl_batch_id STRING COMMENT 'ETL 批次号',
    etl_source_file STRING COMMENT 'ETL 来源文件',
    etl_load_timestamp TIMESTAMP COMMENT '入湖时间'
)
USING iceberg
PARTITIONED BY (days(report_date))
TBLPROPERTIES ('format-version' = '2');

-- 证券持仓：HQLA 分级的原始依据，含质押标记
CREATE TABLE IF NOT EXISTS bronze.ods_securities (
    source_system STRING COMMENT '来源系统编码',
    source_record_id STRING COMMENT '源系统记录主键',
    report_date DATE COMMENT '报告日',
    entity_code STRING COMMENT '记账法人实体编码',
    security_id STRING COMMENT '证券编号',
    isin STRING COMMENT 'ISIN 代码',
    cusip STRING COMMENT 'CUSIP 代码',
    security_type STRING COMMENT '证券类型：TREASURY/AGENCY_DEBT/MBS/ABS/CORP_BOND/EQUITY',
    portfolio_code STRING COMMENT '组合代码：HTP/AFS/HFT',
    issuer_id STRING COMMENT '发行人编号',
    currency STRING COMMENT '币种 ISO 4217',
    face_amount DECIMAL(18,4) COMMENT '面值金额',
    market_value DECIMAL(18,4) COMMENT '市场价值',
    book_value DECIMAL(18,4) COMMENT '账面价值',
    coupon_rate DECIMAL(10,6) COMMENT '票息率',
    purchase_date DATE COMMENT '购买日期',
    maturity_date DATE COMMENT '到期日期',
    credit_rating_raw STRING COMMENT '信用评级原始值',
    pledged_flag STRING COMMENT '是否已质押 Y/N',
    event_time TIMESTAMP COMMENT '源系统事件时间',
    etl_batch_id STRING COMMENT 'ETL 批次号',
    etl_source_file STRING COMMENT 'ETL 来源文件',
    etl_load_timestamp TIMESTAMP COMMENT '入湖时间'
)
USING iceberg
PARTITIONED BY (days(report_date))
TBLPROPERTIES ('format-version' = '2');

-- 衍生品交易：盯市价值与双边抵押品
CREATE TABLE IF NOT EXISTS bronze.ods_derivatives (
    source_system STRING COMMENT '来源系统编码',
    source_record_id STRING COMMENT '源系统记录主键',
    report_date DATE COMMENT '报告日',
    entity_code STRING COMMENT '记账法人实体编码',
    trade_id STRING COMMENT '交易编号',
    counterparty_id STRING COMMENT '交易对手编号',
    instrument_type STRING COMMENT '工具类型：IRS/CDS/FX_FWD/FX_SWAP/OPTION/FUTURES',
    notional_amount DECIMAL(18,4) COMMENT '名义本金',
    currency STRING COMMENT '币种 ISO 4217',
    currency_pair STRING COMMENT '货币对，如 USD/EUR',
    trade_date DATE COMMENT '交易日期',
    maturity_date DATE COMMENT '到期日期',
    mark_to_market DECIMAL(18,4) COMMENT '盯市价值，可正可负',
    mtm_currency STRING COMMENT '盯市价值币种',
    is_central_cleared STRING COMMENT '是否中央清算 Y/N',
    csa_agreement_id STRING COMMENT 'CSA 协议编号',
    collateral_posted DECIMAL(18,4) COMMENT '已提交抵押品',
    collateral_received DECIMAL(18,4) COMMENT '已收到抵押品',
    event_time TIMESTAMP COMMENT '源系统事件时间',
    etl_batch_id STRING COMMENT 'ETL 批次号',
    etl_source_file STRING COMMENT 'ETL 来源文件',
    etl_load_timestamp TIMESTAMP COMMENT '入湖时间'
)
USING iceberg
PARTITIONED BY (days(report_date))
TBLPROPERTIES ('format-version' = '2');

-- 集团总账余额：GL 对账的依据
CREATE TABLE IF NOT EXISTS bronze.ods_gl_balances (
    source_system STRING COMMENT '来源系统编码',
    source_record_id STRING COMMENT '源系统记录主键',
    report_date DATE COMMENT '报告日',
    entity_code STRING COMMENT '记账法人实体编码',
    gl_account_id STRING COMMENT '总账科目号',
    account_name STRING COMMENT '科目名称',
    debit_balance DECIMAL(20,2) COMMENT '借方余额',
    credit_balance DECIMAL(20,2) COMMENT '贷方余额',
    currency STRING COMMENT '币种 ISO 4217',
    event_time TIMESTAMP COMMENT '源系统事件时间',
    etl_batch_id STRING COMMENT 'ETL 批次号',
    etl_source_file STRING COMMENT 'ETL 来源文件',
    etl_load_timestamp TIMESTAMP COMMENT '入湖时间'
)
USING iceberg
PARTITIONED BY (days(report_date))
TBLPROPERTIES ('format-version' = '2');

-- 司库现金头寸：库存现金盘点数 + 各代理行对账单余额，附未达账项明细。
-- 为什么单独一张表：FR 2052a 的 Section E（现金）在 GL 对账里原本两侧都读总账 1001/1100，
-- 等于自己跟自己比，差异恒为零、什么错误都查不出来。司库系统给的是「对账单 + 盘点」口径，
-- 与账面口径天然是两个来源，差额由在途存款与未兑现支票逐项解释。
CREATE TABLE IF NOT EXISTS bronze.ods_treasury_cash_position (
    source_system STRING COMMENT '来源系统编码',
    source_record_id STRING COMMENT '源系统记录主键',
    report_date DATE COMMENT '报告日',
    entity_code STRING COMMENT '记账法人实体编码',
    position_type STRING COMMENT '头寸类型：VAULT_CASH 库存现金 / DUE_FROM_BANKS 存放同业',
    custodian_id STRING COMMENT '保管机构：OWN-VAULT 本行库房、CB-XXX 代理行',
    account_ref STRING COMMENT '账户或库房标识',
    currency STRING COMMENT '币种 ISO 4217',
    balance_amount DECIMAL(20,2) COMMENT '对账单余额或盘点金额（原币）',
    in_transit_deposits_amount DECIMAL(20,2) COMMENT '在途存款（账面已记、对账单未到）',
    outstanding_checks_amount DECIMAL(20,2) COMMENT '未兑现支票（账面已扣、对账单未扣）',
    event_time TIMESTAMP COMMENT '源系统事件时间',
    etl_batch_id STRING COMMENT 'ETL 批次号',
    etl_source_file STRING COMMENT 'ETL 来源文件',
    etl_load_timestamp TIMESTAMP COMMENT '入湖时间'
)
USING iceberg
PARTITIONED BY (days(report_date))
TBLPROPERTIES ('format-version' = '2');

-- 表外承诺：授信承诺、信用证、担保，进 Section J
CREATE TABLE IF NOT EXISTS bronze.ods_off_bs_commitments (
    source_system STRING COMMENT '来源系统编码',
    source_record_id STRING COMMENT '源系统记录主键',
    report_date DATE COMMENT '报告日',
    entity_code STRING COMMENT '记账法人实体编码',
    commitment_id STRING COMMENT '承诺编号',
    counterparty_id STRING COMMENT '交易对手编号',
    commitment_type STRING COMMENT '承诺类型：CREDIT_COMMITMENT/LETTER_OF_CREDIT/GUARANTEE',
    facility_amount DECIMAL(18,4) COMMENT '授信额度',
    undrawn_amount DECIMAL(18,4) COMMENT '未提取金额',
    currency STRING COMMENT '币种 ISO 4217',
    maturity_date DATE COMMENT '到期日期',
    event_time TIMESTAMP COMMENT '源系统事件时间',
    etl_batch_id STRING COMMENT 'ETL 批次号',
    etl_source_file STRING COMMENT 'ETL 来源文件',
    etl_load_timestamp TIMESTAMP COMMENT '入湖时间'
)
USING iceberg
PARTITIONED BY (days(report_date))
TBLPROPERTIES ('format-version' = '2');
