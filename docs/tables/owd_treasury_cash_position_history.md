# 表契约：silver.owd_treasury_cash_position_history

> 本表是 `silver.owd_treasury_cash_position` 的版本历史，结构 = 基表列 + 6 个版本列（`begin_date`、`end_date`、`is_active`、`last_modified_reason`、`record_version`、`row_hash`）。

## 层级

Silver（版本历史，Iceberg `silver` 命名空间）

## 主题

`silver.owd_treasury_cash_position` 的版本历史。

## 粒度

一行 = 一个业务键的一个版本。

## 业务主键

`source_system` + `source_record_id` + `report_date` + `record_version`。自然键为 `source_system` + `source_record_id` + `report_date`：同一源记录在不同报告日是两条独立记录，因为 ODS 是按报告日的头寸快照，不是事件流。

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
| `position_type` |  |  |
| `custodian_id` |  |  |
| `account_ref` |  |  |
| `currency_code` |  |  |
| `balance_usd` |  |  |
| `in_transit_deposits_usd` |  |  |
| `outstanding_checks_usd` |  |  |
| `event_time` |  |  |
| `etl_batch_id` |  |  |
| `begin_date` |  | 版本列 |
| `end_date` |  | 版本列 |
| `is_active` |  | 版本列 |
| `last_modified_reason` |  | 版本列 |
| `record_version` |  | 版本列 |
| `row_hash` |  | 版本列 |

## 金额单位约定

USD，`DECIMAL(18,2)`。金额列继承自基表，折算在 OWD 层已完成。

## PII 字段与脱敏方式

随基表：`custodian_id` 是保管机构、`account_ref` 是本行自有账户标识，都不是个人数据。

## 生命周期

Iceberg v2 表，`format-version = 2`，快照保留 7 天且至少保留 10 个，元数据文件随提交清理（`python/lakehouse/maintain_tables.py`）。小文件合并走 `rewrite_data_files`。不设额外数据保留期：整表重建即最新状态，历史由版本历史表或审计表承担。版本区间按处理时间推进（`--effective-date`，日批取报告日次日），不按报告日；`end_date` 为空 ⇔ 当前有效版本；`is_active` 是 `end_date` 的冗余列，两者必须一致。`row_hash` 参与变更比较的列 = 除 `etl_load_timestamp`、`etl_batch_id` 之外的全部列（按当前实现）。一次性重建脚本：`sql/iceberg/oneoff/06_rebuild_owd_history.sql`（内含本表的 DROP 语句；一次性脚本，常规跑批路径不得引用）。

## 新鲜度 SLA 与 owner

每日 06:00 由 `fr2052a_daily_batch` DAG 触发，须在 T+1 08:00 报送截止前完成。owner：仓库维护者。

## 上下游依赖

- **上游**：`silver.owd_treasury_cash_position`，由 `python/lakehouse/owd_scd2.py` 建表与归并。
- **下游**：版本核对。

## 质量规则清单

版本区间核对：`end_date` 为空 ⇔ 当前有效版本、版本号连续、无重复键。由 `python/lakehouse/verify_scd2.py` 核对。
