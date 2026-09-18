# 表契约：silver.owd_loans

## 层级

Silver（标准化明细，Iceberg `silver` 命名空间）

## 主题

标准化贷款：额度与未提取额折算 USD，借款人属性补齐，区分表内贷款与表外承诺。

## 粒度

一行 = 一笔贷款或一项承诺。

## 业务主键

`source_system` + `source_record_id`。

## 去重方式

dbt `table` 物化，每次运行整表重建：先建后换，不留半成品，失败时保留上一版。

## 分区

无。整表重建，读的时候是整表替换，分区不会缩小任何一次读。

## 字段清单

| 字段 |
|---|
| `source_system` |
| `source_record_id` |
| `report_date` |
| `entity_code` |
| `loan_id` |
| `borrower_id` |
| `borrower_name` |
| `borrower_type` |
| `borrower_country` |
| `borrower_industry` |
| `currency_code` |
| `facility_amount_usd` |
| `outstanding_usd` |
| `undrawn_usd` |
| `loan_type` |
| `is_commitment` |
| `commitment_type` |
| `interest_rate` |
| `rate_type` |
| `internal_rating` |
| `is_collateralized` |
| `origination_date` |
| `maturity_date` |
| `next_payment_date` |
| `days_to_maturity` |
| `maturity_bucket` |
| `event_time` |
| `etl_batch_id` |

列清单与顺序以 `dbt/models/staging/owd_loans.sql` 的最终投影为准；类型由 Spark 在写入 Iceberg 时推断，因此这里只列字段名，不复制一份会过期的类型表。

## 金额单位约定

USD，`DECIMAL(18,2)`。保留 `*_lc` 原币列与 `exchange_rate` 用于核对折算。

## PII 字段与脱敏方式

**直接标识**：`loan_id` 与 `borrower_id`，由 `mask_pii` 宏脱敏成 `h_` 前缀 token；`borrower_name` 是名称而非标识，本层保留原值用于人工核对。**脱敏位置**：本层是脱敏发生地。**对照**：`secure.fr2052a_pii_map`。

## 生命周期

Iceberg v2 表，快照保留 7 天或至少 10 个（`python/lakehouse/maintain_tables.py`）。小文件合并走 `rewrite_data_files`。不设额外数据保留期：整表重建即最新状态，历史由版本历史表或审计表承担。

## 新鲜度 SLA 与 owner

每日 06:00 由 `fr2052a_daily_batch` DAG 触发，须在 T+1 08:00 报送截止前完成（`VDQ-016` 的判据）。owner：仓库维护者。

## 上下游依赖

- **上游**：`bronze.ods_loans` 与 `silver.stg_fx_rates`。
- **下游**：`silver.owd_loans_history`、`ows_cashflow_projection`、报表 Section F 与 `ads_fr2052a_detail`。

## 质量规则清单

本层规则共 8 条：`VDQ-003` 金额非负、`VDQ-004` 利率区间、`VDQ-005` 币种三位 ISO、`VDQ-006` 已用额度不超授信、`VDQ-007` 回购抵押品市值合理、`VDQ-008` 到期日不早于报告日、`VDQ-009` 折算误差小于 1%、`VDQ-020` LEI 格式。
