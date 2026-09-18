# 表契约：bronze.ods_treasury_cash_position

## 层级

Bronze（源系统接入，Iceberg `bronze` 命名空间）。没有 PostgreSQL 侧副本。

## 主题

司库系统的现金头寸快照：库存现金盘点数 + 各代理行对账单余额 + 未达账项明细。它是 GL 对账 Section E（现金）的独立基准：报送侧的现金出自总账 1001/1100，两侧若同源，对账差异恒为零、什么错都查不出来。恒等关系（逐实体、逐账户精确到分）：总账 1001 + 1100 = 对账单余额 + 在途存款 − 未兑现支票。演示假设：库存现金盘点数与账面一致（实地盘点无差额）；未达账项只出现在代理行账户上；所有账户为美元账户。

## 粒度

一行 = 源系统的一条业务记录。

## 业务主键

`source_system` + `source_record_id` + `report_date`（源系统记录主键 + 报告日）。

## 去重方式

入湖按主键 MERGE 去重（`python/consumers/kafka_to_iceberg.py`）：主键含 report_date，同一源记录在不同报告日是两条独立记录；同一批次内按 Kafka offset 取最新一条，重放同一条消息不产生第二行。

## 分区

`days(report_date)`；`format-version = 2`。

- **理由**：按日重跑要能整分区替换，`report_date` 是唯一的重跑边界。
- **单分区数据量**：一个分区 = 一个报告日，每期 20 行 = 5 个法人实体 × (1 行库存现金 + 3 个代理行账户 CITI / JPM / HSBC)。

## 字段清单

| 字段 | 类型 | 说明 |
|---|---|---|
| `source_system` | STRING | 来源系统编码 |
| `source_record_id` | STRING | 源系统记录主键 |
| `report_date` | DATE | 报告日 |
| `entity_code` | STRING | 记账法人实体编码 |
| `position_type` | STRING | 头寸类型：VAULT_CASH 库存现金 / DUE_FROM_BANKS 存放同业 |
| `custodian_id` | STRING | 保管机构：OWN-VAULT 本行库房、CB-XXX 代理行 |
| `account_ref` | STRING | 账户或库房标识 |
| `currency` | STRING | 币种 ISO 4217（本演示恒为 USD） |
| `balance_amount` | DECIMAL(20,2) | 对账单余额或盘点金额（原币） |
| `in_transit_deposits_amount` | DECIMAL(20,2) | 在途存款（账面已记、对账单未到） |
| `outstanding_checks_amount` | DECIMAL(20,2) | 未兑现支票（账面已扣、对账单未扣） |
| `event_time` | TIMESTAMP | 源系统事件时间 |
| `etl_batch_id` | STRING | ETL 批次号 |
| `etl_source_file` | STRING | ETL 来源文件 |
| `etl_load_timestamp` | TIMESTAMP | 入湖时间，入湖作业按「报告日 +1 天 02:00」补齐 |

## 金额单位约定

原币种，`DECIMAL(20,2)`。本层不做任何换算，折算在 OWD 层发生。

## PII 字段与脱敏方式

本层不含个人数据：`custodian_id` 是保管机构、`account_ref` 是本行自有账户标识，都不是个人数据。

## 生命周期

Iceberg v2 表，快照保留 7 天或至少 10 个（`python/lakehouse/maintain_tables.py`）。小文件合并走 `rewrite_data_files`。不设额外数据保留期：整表重建即最新状态，历史由版本历史表或审计表承担。

## 新鲜度 SLA 与 owner

每日 06:00 由 `fr2052a_daily_batch` DAG 触发，须在 T+1 08:00 报送截止前完成。owner：仓库维护者。

## 上下游依赖

- **上游**：`TREASURY_SYS` 的 `treasury_cash_position_{ymd}.csv`，经 Kafka 主题 `treasury_cash_position` 流入。生成器 `python/generators/ods_data.py` 的 `generate_treasury_cash_position`，在 `generate_gl_balances` 之后运行（对账单余额以该期账面余额为锚）。建表 DDL：`sql/iceberg/02_create_ods_tables.sql`。
- **下游**：`silver.owd_treasury_cash_position`（dbt staging 模型），再进 `gold.ads_gl_reconciliation` 的基准侧。

## 质量规则清单

本层规则共 2 条：`VDQ-001` 表非空、`VDQ-002` `source_record_id` 与 `currency` 非空（见 `python/validators/run_dq_rules.py` 的 `ALL_BRONZE`）。表结构漂移闸：`python/lakehouse/verify_ods_schema.py`（CSV 表头与表列逐列比对）。
