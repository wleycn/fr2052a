# 表契约：bronze.ods_deposits

## 层级

Bronze（源系统接入，Iceberg `bronze` 命名空间）

## 主题

存款头寸：零售、对公与同业存款，含活期与定期。

## 粒度

一行 = 源系统的一条业务记录。

## 业务主键

`source_system` + `source_record_id` + `report_date`（源系统记录主键 + 报告日）。

## 去重方式

入湖按主键 MERGE 去重（`python/consumers/kafka_to_iceberg.py`）：主键含 report_date，同一源记录在不同报告日是两条独立记录；同一批次内按 Kafka offset 取最新一条，重放同一条消息不产生第二行。

## 分区

`days(report_date)`。

- **理由**：按日重跑要能整分区替换，`report_date` 是唯一的重跑边界。
- **单分区数据量**：一个分区 = 一个报告日。演示数据 8 张 ODS 合计 1868 行，单分区约百行量级；生产环境按日切分后单分区仍是「一天的源系统明细」这一量级。

## 字段清单

| 字段 | 类型 | 说明 |
|---|---|---|
| `source_system` | STRING | 来源系统编码 |
| `source_record_id` | STRING | 源系统记录主键 |
| `report_date` | DATE | 报告日 |
| `entity_code` | STRING | 记账法人实体编码 |
| `account_number` | STRING | 账户号 |
| `customer_id` | STRING | 客户编号 |
| `product_code` | STRING | 产品代码 |
| `deposit_type` | STRING | 存款类型原始编码：SAV/CHK/CD/MMDA/TIME |
| `currency` | STRING | 币种 ISO 4217 |
| `principal_amount` | DECIMAL(18,4) | 本金金额 |
| `accrued_interest` | DECIMAL(18,4) | 应计利息 |
| `interest_rate` | DECIMAL(10,6) | 利率 |
| `open_date` | DATE | 开户日期 |
| `maturity_date` | DATE | 到期日期，活期为空 |
| `branch_code` | STRING | 机构代码 |
| `customer_type_raw` | STRING | 客户类型原始值：IND/CORP/GOV/FI；集团内配对腿为 AFFIL |
| `insured_flag` | STRING | 是否受保存款 Y/N |
| `event_time` | TIMESTAMP | 源系统事件时间 |
| `etl_batch_id` | STRING | ETL 批次号 |
| `etl_source_file` | STRING | ETL 来源文件 |
| `etl_load_timestamp` | TIMESTAMP | 入湖时间 |

## 金额单位约定

原币种，`DECIMAL(18,4)`。本层不做任何换算，折算在 OWD 层发生。

## PII 字段与脱敏方式

本层保留原始标识（如 `account_number`、`customer_id`），**脱敏在 OWD 层发生**。明文对照只住 `secure.fr2052a_pii_map`，由合规员与管理员两个角色可见。

## 生命周期

Iceberg v2 表，快照保留 7 天或至少 10 个（`python/lakehouse/maintain_tables.py`）。小文件合并走 `rewrite_data_files`。不设额外数据保留期：整表重建即最新状态，历史由版本历史表或审计表承担。

## 新鲜度 SLA 与 owner

每日 06:00 由 `fr2052a_daily_batch` DAG 触发，须在 T+1 08:00 报送截止前完成（`VDQ-016` 的判据）。owner：仓库维护者。

## 上下游依赖

- **上游**：Kafka 主题 `core_banking_txns`，由 `python/producers/replay_ods_to_kafka.py` 重放样本数据。
- **下游**：`silver.owd_deposits`。

## 质量规则清单

本层规则共 3 条：`VDQ-001` 源文件整批到达、`VDQ-002` 主键与币种非空、`VDQ-016` 加载早于 T+1 08:00。规则定义住 `ref.ref_validation_rules`（`apply_layer = 'ODS'`），由 `python/validators/run_dq_rules.py` 执行，结论落 `ads.ads_fr2052a_validation_log`。
