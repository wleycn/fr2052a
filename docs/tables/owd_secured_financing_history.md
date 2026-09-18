# 表契约：silver.owd_secured_financing_history

> 本表是 `silver.owd_secured_financing` 的版本历史，结构 = 基表列 + 6 个版本列（`begin_date`、`end_date`、`is_active`、`last_modified_reason`、`record_version`、`row_hash`）。

## 层级

Silver（版本历史，Iceberg `silver` 命名空间）

## 主题

`silver.owd_secured_financing` 的版本历史。

## 粒度

一行 = 一个业务键的一个版本。

## 业务主键

`source_system` + `source_record_id` + `record_version`。

## 去重方式

由 `python/lakehouse/owd_scd2.py` 全表重算，同一业务键的版本号连续；重跑同一处理日结果一致。

## 分区

无。整表重建，读的时候是整表替换，分区不会缩小任何一次读。

## 字段清单

| 字段 | 类型 | 说明 |
|---|---|---|
| `source_system` |  |  |
| `source_record_id` |  |  |
| `report_date` |  |  |
| `entity_code` |  |  |
| `financing_id` |  |  |
| `counterparty_id` |  |  |
| `counterparty_name` |  |  |
| `counterparty_type` |  |  |
| `counterparty_country` |  |  |
| `transaction_type` |  |  |
| `currency_code` |  |  |
| `cash_amount_lc` |  |  |
| `cash_amount_usd` |  |  |
| `collateral_mv_lc` |  |  |
| `collateral_mv_usd` |  |  |
| `haircut_pct` |  |  |
| `net_exposure_usd` |  |  |
| `interest_rate` |  |  |
| `collateral_isin` |  |  |
| `collateral_type` |  |  |
| `collateral_hqla_level` |  |  |
| `deal_start_date` |  |  |
| `deal_end_date` |  |  |
| `days_to_maturity` |  |  |
| `maturity_bucket` |  |  |
| `is_open_ended` |  |  |
| `master_agreement_type` |  |  |
| `netting_agreement_id` |  |  |
| `is_nettable` |  |  |
| `event_time` |  |  |
| `etl_batch_id` |  |  |
| `begin_date` |  | 版本列 |
| `end_date` |  | 版本列 |
| `is_active` |  | 版本列 |
| `last_modified_reason` |  | 版本列 |
| `record_version` |  | 版本列 |
| `row_hash` |  | 版本列 |

## 金额单位约定

USD，`DECIMAL(18,2)`。保留 `*_lc` 原币列与 `exchange_rate` 用于核对折算。

## PII 字段与脱敏方式

随基表：本表不含个人标识。

## 生命周期

Iceberg v2 表，快照保留 7 天或至少 10 个（`python/lakehouse/maintain_tables.py`）。小文件合并走 `rewrite_data_files`。不设额外数据保留期：整表重建即最新状态，历史由版本历史表或审计表承担。 版本区间用 `begin_date` / `end_date` 表示，**当前有效版本的 `end_date` 为空**；`is_active` 是 `end_date` 的冗余列，两者必须一致。

## 新鲜度 SLA 与 owner

每日 06:00 由 `fr2052a_daily_batch` DAG 触发，须在 T+1 08:00 报送截止前完成（`VDQ-016` 的判据）。owner：仓库维护者。

## 上下游依赖

- **上游**：见上游模型与环节脚本。
- **下游**：重述登记与版本核对。

## 质量规则清单

版本区间不得反向：`end_date` 不早于本版本的 `begin_date`；`end_date IS NULL` 与 `is_active` 必须同真同假；版本号连续无重复。由 `python/lakehouse/verify_scd2.py` 在日批的 `verify-scd2` 环节核对。
