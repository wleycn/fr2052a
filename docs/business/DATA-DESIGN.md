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

#### `ads.ads_fr2052a_report`

列由 dbt 模型产出，完整列清单见 `dbt/models/marts/ads_fr2052a_report.sql`。本文档不复制一份：复制出来的列清单必然在报表加列时漏改一处。

| 项 | 内容 |
|---|---|
| 主键 | `report_id`，四段区位码「机构-报表-报告期-口径」，例 `ENT001-FR2052A-20260916-01` |
| 末段口径码 | `01` = 集团并表，`02` = 法人单体 |
| 粒度 | 一个报送主体一行，即（报告日 + 实体 + 口径） |
| 金额列 | Section A–K 的金额与监管上限后的认列额，另有三项汇总指标 |

为什么不用自增序列做主键：本表每轮导出是全量覆盖，序列值属于数据库状态而不是数据，同一个业务报表在不同批次会拿到不同的号。而重述登记要跨批次引用「原报表 / 新报表」，键一旦会变，这层对应关系就不成立。

#### `ads.ads_fr2052a_alerts`（流动性预警，一行一个规则状态）

```sql
CREATE TABLE ads.ads_fr2052a_alerts (
    alert_id BIGSERIAL PRIMARY KEY,                   -- 预警主键，交给序列生成
    report_date DATE NOT NULL,                        -- 报告日
    entity_code TEXT NOT NULL,                        -- 法人实体编码
    alert_code TEXT NOT NULL,                         -- 规则编码，如 CB-LCR-001
    severity TEXT NOT NULL,                           -- CRITICAL / WARNING / INFO
    metric_name TEXT,                                 -- 触发指标名
    metric_value NUMERIC(20, 4),                      -- 触发时指标实际值
    threshold_value NUMERIC(20, 4),                   -- 判定阈值
    message TEXT NOT NULL,                            -- 人读说明
    blocks_submission BOOLEAN NOT NULL DEFAULT FALSE, -- 是否阻断报送
    occurrence_count INTEGER NOT NULL DEFAULT 1,      -- 同一规则重复触发的累计次数
    status TEXT NOT NULL DEFAULT 'OPEN',              -- OPEN / CLOSED
    first_detected_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_detected_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT ads_fr2052a_alerts_uk UNIQUE (report_date, entity_code, alert_code),
    CONSTRAINT ads_fr2052a_alerts_severity_ck CHECK (severity IN ('CRITICAL', 'WARNING', 'INFO')),
    CONSTRAINT ads_fr2052a_alerts_status_ck CHECK (status IN ('OPEN', 'CLOSED'))
);
```

规则状态与事件流水分两张表。本表是「规则当前状态」，同一规则重复命中只累加 `occurrence_count`；「一笔事件一行」的流水另住 `ads.ads_fr2052a_realtime_alerts`。混在一张表里会让「一条记录代表什么」说不清。

### 2.3 落库契约

| 层 | 写入方 | 读取方 | 幂等策略 |
|----|--------|--------|----------|
| Ref | `load_ref_tables.py` 批加载 | 所有层 | 按主键 MERGE |
| Bronze | `kafka_to_iceberg.py` 消费 Kafka | OWD 模型 | 按主键 MERGE 去重，同一条消息重放不产生第二行 |
| Silver | dbt 模型（table 物化） | OWS / ADS 模型 | 整表重建，先建后换，不留半成品 |
| Silver 版本历史 | `owd_scd2.py` | 重述登记与审计 | 全表重算，同一主键的版本号连续 |
| Gold（Iceberg） | dbt marts 模型 | 导出作业 | 整表重建 |
| ADS（PG） | `export_gold_to_pg.py` | 报送、预警、对账 | 先清后写，写时开 `truncate=true` 以保留表上的授权与触发器 |
| 控制与审计（PG） | 各环节脚本 | 放行闸、巡检、审计 | 按业务键 upsert；按批次累积的表先清本批次再追加 |

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
