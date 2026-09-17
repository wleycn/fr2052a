# 数据设计

> 真源：`sql/iceberg/`（DDL）+ `docs/business/DATA-DESIGN.md`（本文档）。两者必须同步更新。

## §1 数据流图

### 端到端数据流

```text
源系统
  核心银行 / 资金交易 / 衍生品 / 托管 / 贷款 / 财务总账
        │ 文件 / CDC / API / Kafka
        ▼
接入层
  Kafka Topics + 批文件网关
        │
        ▼
数据湖 Bronze / ODS
  Iceberg on MinIO：原始镜像、追加、Schema 演进、Time Travel
        │
        ▼
数据湖 Silver / OWD + OWS
  清洗、标准化、汇率转换、引用数据关联、到期分桶、HQLA 分类、现金流预测
        │
        ▼
Gold / ADS
  PostgreSQL：FR 2052a 报表、明细、校验日志、GL 对账、报送状态
        │
        ├── 报送文件生成：XBRL / XML / CSV
        ├── DataHub：技术血缘 + 业务血缘 + 监管映射
        └── 监控告警：Airflow SLA + Grafana + 邮件/企业微信
```

### 真源划分

| 数据 | 真源 | 说明 |
|------|------|------|
| 表结构 DDL | `sql/iceberg/*.sql` | 唯一真源，改代码必同步本节 |
| 接口契约 | `docs/business/INTERFACE-DESIGN.md` | dbt macro / Python API / Kafka Topic |
| 业务规则 | `dbt/macros/fr2052a_rules.sql` | HQLA、到期分桶、现金流 Cap |
| 元数据 | `config/pipeline_topics.json` | Topic 配置 |
| 派生视图 | 无真源 | Iceberg snapshot / dbt 物化视图可随时重建 |

## §2 数据结构设计

### 2.1 分层表清单

#### 接入层（Kafka Topics）

| Topic | 内容 | 生产者 | 消费者 |
|-------|------|--------|--------|
| `core_banking_txns` | 存款、取款、转账 | Core Banking System | Spark Streaming → Bronze |
| `treasury_deals` | 回购、逆回购、融资 | Treasury System | Spark Streaming → Bronze |
| `derivatives_trades` | 衍生品交易 | Derivatives System | Spark Streaming → Bronze |
| `market_data_prices` | 汇率、证券价格 | Market Data Feed | Spark Streaming → 维表 |
| `gl_entries` | 总账分录 | GL System | 批/流 → GL 对账 |
| `reference_data_updates` | 主数据变更 | MDM System | 维表更新 |
| `fr2052a_alerts` | 实时预警 | 校验引擎 | 告警服务 |

#### Bronze 层（Iceberg）

| 表 | 说明 | 加载方式 |
|----|------|----------|
| `lakehouse.bronze.ods_deposits_stream` | 存款原始流数据 | Kafka → Iceberg |
| `lakehouse.bronze.ods_treasury_deals` | 资金交易原始 | Kafka → Iceberg |
| `lakehouse.bronze.ods_derivatives` | 衍生品原始 | Kafka → Iceberg |
| `lakehouse.bronze.ref_entity_hierarchy` | 法人实体层级（批） | CSV → Iceberg |
| `lakehouse.bronze.ref_exchange_rates` | 汇率（批） | CSV → Iceberg |

#### Silver 层（OWD + OWS）

| 表 | 说明 | 来源 |
|----|------|------|
| `owd_deposits` | 标准化存款 | `ods_deposits` 清洗 + 汇率转换 |
| `owd_secured_financing` | 标准化有担保融资 | `ods_repo_transactions` |
| `owd_loans` | 标准化贷款 | `ods_loans` |
| `owd_securities` | 标准化证券 | `ods_securities` |
| `owd_derivatives` | 标准化衍生品 | `ods_derivatives` |
| `ows_funding_summary` | 融资汇总 | `owd_*` 聚合 + 到期分桶 |
| `ows_hqla_summary` | HQLA 汇总 | `owd_securities` HQLA 分类 |
| `ows_cashflow_projection` | 现金流预测 | `owd_*` 现金流计算 |

#### Gold 层（ADS in PG）

| 表 | 说明 | 来源 |
|----|------|------|
| `ads_fr2052a_report` | FR 2052a 报表主表 | `ows_*` 汇总 |
| `ads_fr2052a_detail` | 报表明细行 | `owd_*` 明细展开 |
| `ads_gl_reconciliation` | GL 对账结果 | `ows_funding_summary` vs GL |
| `ads_restatement_log` | 重述日志 | SCD2 版本变更 |
| `ads_fr2052a_validation_log` | 校验日志 | GE 校验结果 |
| `ads_fr2052a_submission` | 报送状态 | 报送回执 |
| `fr2052a_alerts` | 合规告警 | 校验失败 → 熔断 |

#### 引用数据（Ref）

| 表 | 说明 |
|----|------|
| `ref_entity_hierarchy` | 法人实体层级（合并口径 + 重要子公司） |
| `ref_maturity_bucket` | 到期分桶定义 |
| `ref_fr2052a_line_items` | FR 2052a 行项目映射 |
| `ref_exchange_rates` | 汇率（报告日即期） |
| `ref_regulatory_mapping` | 监管映射（字段 → Section/Line Item） |
| `ref_behavior_assumptions` | 行为假设（流出率、HQLA 分类） |
| `ref_calendar` | 银行营业日 |
| `ref_validation_rules` | 数据质量规则定义 |
| `ref_counterparty` | 交易对手主数据 |

#### 审计与治理

| 表 | 说明 |
|----|------|
| `audit_change_log` | 变更审计日志 |
| `audit_access_log` | 访问审计日志 |
| `audit_data_lineage` | 数据血缘 |

### 2.2 关键表结构

#### `fr2052a_db.ads.ads_fr2052a_report`

```sql
CREATE TABLE ads.ads_fr2052a_report (
    report_id BIGSERIAL PRIMARY KEY,
    report_date DATE NOT NULL,
    entity_code VARCHAR(20) NOT NULL,
    is_consolidated BOOLEAN NOT NULL,
    -- Section A: Wholesale Unsecured Financing
    sec_a_cp_outstanding NUMERIC(20,2),
    sec_a_cd_outstanding NUMERIC(20,2),
    sec_a_unsecured_borrow NUMERIC(20,2),
    sec_a_total NUMERIC(20,2),
    -- Section B: Wholesale Secured Financing
    sec_b_repo_outstanding NUMERIC(20,2),
    sec_b_total NUMERIC(20,2),
    -- Section C: Deposits
    sec_c_retail_demand NUMERIC(20,2),
    sec_c_retail_savings NUMERIC(20,2),
    sec_c_total NUMERIC(20,2),
    -- Section F: Loan Portfolio (Cash Inflows)
    sec_f_total_inflow NUMERIC(20,2),
    -- Section G: Securities Portfolio
    sec_g_hqla_l1_mv NUMERIC(20,2),
    sec_g_hqla_l2a_mv NUMERIC(20,2),
    sec_g_hqla_l2b_mv NUMERIC(20,2),
    sec_g_total_mv NUMERIC(20,2),
    -- 汇总指标
    total_funding NUMERIC(20,2),
    total_hqla NUMERIC(20,2),
    net_funding_outflow NUMERIC(20,2),
    created_at TIMESTAMPTZ DEFAULT NOW()
);
```

#### `fr2052a_db.ads.fr2052a_alerts`

```sql
CREATE TABLE ads.fr2052a_alerts (
    alert_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    report_date DATE NOT NULL,
    source_model VARCHAR(255) NOT NULL,
    rule_id VARCHAR(50),
    severity VARCHAR(20) NOT NULL CHECK (severity IN ('CRITICAL', 'WARNING', 'INFO')),
    message TEXT NOT NULL,
    variance_amount NUMERIC(38,10),
    context_json JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    resolved_at TIMESTAMPTZ,
    is_resolved BOOLEAN DEFAULT FALSE,
    resolution_note TEXT,
    airflow_run_id VARCHAR(100),
    CONSTRAINT chk_resolved_consistency CHECK (NOT is_resolved OR resolved_at IS NOT NULL)
);

CREATE INDEX idx_alerts_unresolved_critical ON fr2052a_alerts (report_date, severity, is_resolved)
WHERE severity = 'CRITICAL' AND is_resolved = FALSE;
```

### 2.3 落库契约

| 层 | 写入方 | 读取方 | 幂等策略 |
|----|--------|--------|----------|
| Bronze | Spark Streaming / 批加载 | OWD 模型 | 追加（append）|
| Silver | dbt model | OWS 模型 | MERGE（upsert）|
| Gold (PG) | export_gold_to_pg.py | ADS 报表、DataHub | 先清后写（truncate + insert）|
| Ref | 批加载 | 所有层 | MERGE（按主键）|

## §3 业务规则

### 3.1 到期分桶

| Bucket | 最小天数 | 最大天数 |
|--------|----------|----------|
| O/N | 0 | 0 |
| 1-7D | 1 | 7 |
| 8-30D | 8 | 30 |
| 31-90D | 31 | 90 |
| 91-180D | 91 | 180 |
| 181D-1Y | 181 | 365 |
| >1Y | 366 | 99999 |
| OPEN | NULL | NULL |

### 3.2 HQLA 分类

| 等级 | 示例 | Haircut |
|------|------|---------|
| Level 1 | 现金、央行准备金、国债 | 0% |
| Level 2A | 机构债、GSE | 15% |
| Level 2B | 投资级公司债、部分股票 | 50% |
| Non-HQLA | 其他 | 不纳入 |

### 3.3 关键约束

| 规则 | 约束 |
|------|------|
| 现金流 Cap | 预期流入 ≤ 总流出 × 75%，超出按 75% 截断并记录 WARNING |
| HQLA 二级资产上限 | Level 2A + Level 2B ≤ 总 HQLA × 40% |
| Operational 存款 | 流出率低于 Non-Operational |
| 币种转换 | 报告日即期汇率转 USD |
| 净额结算 | 仅有有效净额协议时允许 |

### 3.4 数据质量规则（VDQ）

| 编号 | 层级 | 规则 | 严重度 |
|------|------|------|--------|
| VDQ-001 | ODS | 文件完整到达 | ERROR |
| VDQ-002 | ODS | 关键字段非空 | ERROR |
| VDQ-003 | OWD | 金额 ≥ 0 | ERROR |
| VDQ-006 | OWD | outstanding ≤ facility | ERROR |
| VDQ-010 | OWS | 汇总 = SUM 明细 | ERROR |
| VDQ-013 | ADS | Section 合计 = 行项目合计 | ERROR |
| VDQ-016 | ODS | T+1 08:00 ET 前加载 | ERROR |
| VDQ-017 | ADS | L2A + L2B ≤ 总 HQLA 40% | WARNING |
| VDQ-018 | ADS | 现金流入 cap = 总流出 75% | ERROR |

## §4 数据血缘

```text
Core Banking Deposit Module
  → ods_deposits (Bronze)
  → owd_deposits (Silver)
  → ows_funding_summary (Silver)
  → ads_fr2052a_report.sec_c_retail_demand (Gold)
  → FR 2052a Section C / Line Item
```

完整血缘由 DataHub 摄取 dbt 元数据后自动生成，见 [INTERFACE-DESIGN.md](INTERFACE-DESIGN.md#datahub)。
