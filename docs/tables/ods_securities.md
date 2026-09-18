# 表契约：bronze.ods_securities

## 层级

Bronze（源系统接入，Iceberg `bronze` 命名空间）

## 主题

证券持仓：面值、市值、账面价值与质押标记。

## 粒度

一行 = 源系统的一条业务记录。

## 业务主键

`source_system` + `source_record_id` + `report_date`（源系统记录主键 + 报告日）。

## 去重方式

入湖按主键 MERGE 去重（`python/consumers/kafka_to_iceberg.py`）：主键含 report_date，同一源记录在不同报告日是两条独立记录；同一批次内按 Kafka offset 取最新一条，重放同一条消息不产生第二行。

## 分区

`days(report_date)`。

- **理由**：按日重跑要能整分区替换，`report_date` 是唯一的重跑边界。
- **单分区数据量**：一个分区 = 一个报告日。演示数据 7 张 ODS 合计 1505 行，单分区约百行量级；生产环境按日切分后单分区仍是「一天的源系统明细」这一量级。

## 字段清单

| 字段 | 类型 | 说明 |
|---|---|---|
| `source_system` | STRING | 来源系统编码 |
| `source_record_id` | STRING | 源系统记录主键 |
| `report_date` | DATE | 报告日 |
| `entity_code` | STRING | 记账法人实体编码 |
| `security_id` | STRING | 证券编号 |
| `isin` | STRING | ISIN 代码 |
| `cusip` | STRING | CUSIP 代码 |
| `security_type` | STRING | 证券类型：TREASURY/AGENCY_DEBT/MBS/ABS/CORP_BOND/EQUITY |
| `portfolio_code` | STRING | 组合代码：HTP/AFS/HFT |
| `issuer_id` | STRING | 发行人编号 |
| `currency` | STRING | 币种 ISO 4217 |
| `face_amount` | DECIMAL(18,4) | 面值金额 |
| `market_value` | DECIMAL(18,4) | 市场价值 |
| `book_value` | DECIMAL(18,4) | 账面价值 |
| `coupon_rate` | DECIMAL(10,6) | 票息率 |
| `purchase_date` | DATE | 购买日期 |
| `maturity_date` | DATE | 到期日期 |
| `credit_rating_raw` | STRING | 信用评级原始值 |
| `pledged_flag` | STRING | 是否已质押 Y/N |
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

- **上游**：Kafka 主题 `custody_positions`。
- **下游**：`silver.owd_securities`。

## 质量规则清单

本层规则共 3 条：`VDQ-001` 源文件整批到达、`VDQ-002` 主键与币种非空、`VDQ-016` 加载早于 T+1 08:00。规则定义住 `ref.ref_validation_rules`（`apply_layer = 'ODS'`），由 `python/validators/run_dq_rules.py` 执行，结论落 `ads.ads_fr2052a_validation_log`。
