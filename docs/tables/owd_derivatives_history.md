# 表契约：silver.owd_derivatives_history

> 本表是 `silver.owd_derivatives` 的版本历史，结构 = 基表列 + 6 个版本列（`begin_date`、`end_date`、`is_active`、`last_modified_reason`、`record_version`、`row_hash`）。

## 层级

Silver（版本历史，Iceberg `silver` 命名空间）

## 主题

`silver.owd_derivatives` 的版本历史。

## 粒度

一行 = 一个业务键的一个版本。

## 业务主键

`source_system` + `source_record_id` + `report_date` + `record_version`。自然键为 `source_system` + `source_record_id` + `report_date`（同 `python/lakehouse/owd_scd2.py` 的 `KEY_COLUMNS`）：同一源记录号在不同报告日是两条独立记录，各起一条版本链；`record_version` 只区分同一自然键内部的版本序号。

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
| `derivative_id` |  |  |
| `counterparty_id` |  |  |
| `counterparty_type` |  |  |
| `counterparty_country` |  |  |
| `instrument_type` |  |  |
| `product_class` |  |  |
| `notional_usd` |  |  |
| `currency_code` |  |  |
| `currency_pair` |  |  |
| `trade_date` |  |  |
| `maturity_date` |  |  |
| `days_to_maturity` |  |  |
| `maturity_bucket` |  |  |
| `is_intracompany` | BOOLEAN | 集团内往来标记：交易对手类型为 `AFFILIATE` 时 true，其余（含 NULL）为 false，由 `is_affiliate_counterparty` 宏判定。 |
| `mtm_value_usd` |  |  |
| `mtm_currency` | STRING | 盯市价值币种（ODS 声明）；盯市折算所用汇率按此币种取。 |
| `mtm_exchange_rate` | DECIMAL(18,8) | 折算 `mtm_value_usd` 所用的 `mtm_currency` 对 USD 汇率（`stg_fx_rates.spot_rate`）。 |
| `is_central_cleared` |  |  |
| `csa_agreement_id` |  |  |
| `is_bilateral_netted` |  |  |
| `collateral_posted_usd` |  |  |
| `collateral_received_usd` |  |  |
| `net_exposure_usd` |  |  |
| `event_time` |  |  |
| `etl_batch_id` |  |  |
| `begin_date` |  | 版本列 |
| `end_date` |  | 版本列 |
| `is_active` |  | 版本列 |
| `last_modified_reason` |  | 版本列 |
| `record_version` |  | 版本列 |
| `row_hash` |  | 版本列 |

## 金额单位约定

USD，`DECIMAL(18,2)`。本表没有 `*_lc` 原币列：名义本金与抵押品按 `currency_code` 的汇率折 USD，盯市价值按 `mtm_currency` 折算，所用币种与汇率已落成 `mtm_currency`、`mtm_exchange_rate` 两列，折算过程可逐行核对。

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
