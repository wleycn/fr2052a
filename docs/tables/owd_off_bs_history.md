# 表契约：silver.owd_off_bs_history

> 本表是 `silver.owd_off_bs` 的版本历史，结构 = 基表列 + 6 个版本列（`begin_date`、`end_date`、`is_active`、`last_modified_reason`、`record_version`、`row_hash`）。

## 层级

Silver（版本历史，Iceberg `silver` 命名空间）

## 主题

`silver.owd_off_bs` 的版本历史。

## 粒度

一行 = 一个业务键的一个版本。

## 业务主键

`source_system` + `source_record_id` + `report_date` + `record_version`。自然键为 `source_system` + `source_record_id` + `report_date`（同 `python/lakehouse/owd_scd2.py` 的 `KEY_COLUMNS`）：同一源记录号在不同报告日是两条独立记录，各起一条版本链；`record_version` 只区分同一自然键内部的版本序号。

## 去重方式

由 `python/lakehouse/owd_scd2.py` 全表重算，同一业务键的版本号连续；重跑同一处理日结果一致。

## 分区

无。整表重建，读的时候是整表替换，分区不会缩小任何一次读。

## 字段清单

| 字段 | 说明 |
| --- | --- |
| `source_system` |  |
| `source_record_id` |  |
| `report_date` |  |
| `entity_code` |  |
| `commitment_id` |  |
| `counterparty_id` |  |
| `counterparty_name` |  |
| `counterparty_type` |  |
| `commitment_type` |  |
| `facility_amount_usd` |  |
| `undrawn_amount_usd` |  |
| `currency_code` |  |
| `maturity_date` |  |
| `days_to_maturity` |  |
| `maturity_bucket` |  |
| `is_intracompany` | 集团内往来标记：交易对手类型为 `AFFILIATE` 时 true，其余（含 NULL）为 false，由 `is_affiliate_counterparty` 宏判定。 |
| `event_time` |  |
| `etl_batch_id` |  |
| `begin_date` | 版本列 |
| `end_date` | 版本列 |
| `is_active` | 版本列 |
| `last_modified_reason` | 版本列 |
| `record_version` | 版本列 |
| `row_hash` | 版本列 |

## 金额单位约定

USD。金额列一律为 `decimal` 类型，小数位固定 2 位；整数位精度由 Spark 按源类型推断。本表没有 `*_lc` 原币列，也没有汇率列：`facility_amount_usd`、`undrawn_amount_usd` 由 `currency_code` 对应汇率折成 USD，汇率只在折算时使用。

## PII 字段与脱敏方式

随基表：本表不含个人标识。

## 生命周期

Iceberg v2 表，`format-version = 2`，快照保留 7 天且至少保留 10 个，元数据文件随提交清理（建表属性见 `python/lakehouse/owd_scd2.py` 的 `TABLE_PROPERTIES`，`python/lakehouse/maintain_tables.py` 每轮重申）。小文件合并走 `rewrite_data_files`。不设额外数据保留期：每轮整表重算版本行，历史都留在本表里。版本区间按处理时间推进（`--effective-date`，日批取报告日次日），不按报告日；失效日 = 生效日 − 1，且不早于本版本生效日；`end_date` 为空 ⇔ 当前有效版本；`is_active` 是 `end_date` 的冗余列，两者必须一致。 版本区间用 `begin_date` / `end_date` 表示，**当前有效版本的 `end_date` 为空**；`is_active` 是 `end_date` 的冗余列，两者必须一致。

## 新鲜度 SLA 与 owner

每日 06:00 由 `fr2052a_daily_batch` DAG 触发，须在 T+1 08:00 报送截止前完成（`VDQ-016` 的判据）。owner：仓库维护者。

## 上下游依赖

- **上游**：见上游模型与环节脚本。
- **下游**：重述登记与版本核对。

## 质量规则清单

版本区间不得反向：`end_date` 不早于本版本的 `begin_date`；`end_date IS NULL` 与 `is_active` 必须同真同假；版本号连续无重复。由 `python/lakehouse/verify_scd2.py` 在日批的 `verify-scd2` 环节核对。
