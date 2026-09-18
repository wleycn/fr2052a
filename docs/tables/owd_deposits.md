# 表契约：silver.owd_deposits

## 层级

Silver（标准化明细，Iceberg `silver` 命名空间）

## 主题

标准化存款：外币折算 USD、客群与产品口径归一、到期分桶、受保金额按存款保险上限截断。

## 粒度

一行 = 一条存款明细。

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
| `deposit_id` |
| `customer_id` |
| `customer_segment` |
| `customer_type` |
| `product_category` |
| `deposit_type` |
| `currency_code` |
| `principal_amount_lc` |
| `principal_amount_usd` |
| `accrued_interest_lc` |
| `accrued_interest_usd` |
| `exchange_rate` |
| `interest_rate` |
| `open_date` |
| `maturity_date` |
| `days_to_maturity` |
| `maturity_bucket` |
| `is_insured` |
| `insured_amount_usd` |
| `branch_code` |
| `event_time` |
| `etl_batch_id` |

列清单与顺序以 `dbt/models/staging/owd_deposits.sql` 的最终投影为准；类型由 Spark 在写入 Iceberg 时推断，因此这里只列字段名，不复制一份会过期的类型表。

## 金额单位约定

USD，`DECIMAL(18,2)`。保留 `*_lc` 原币列与 `exchange_rate` 用于核对折算。

## PII 字段与脱敏方式

**直接标识**：`deposit_id`（原 `account_number`）与 `customer_id`，均以 `mask_pii` 宏脱敏成 `h_` 前缀的确定性 token。**脱敏位置**：本层是脱敏发生地，ODS 层仍是明文。**保持单射**：token 由带盐 SHA-256 截取，同一客户在不同表里仍是同一 token，聚合与关联不受影响。**对照**：明文与原值的对应关系只住 `secure.fr2052a_pii_map`。

## 生命周期

Iceberg v2 表，快照保留 7 天或至少 10 个（`python/lakehouse/maintain_tables.py`）。小文件合并走 `rewrite_data_files`。不设额外数据保留期：整表重建即最新状态，历史由版本历史表或审计表承担。

## 新鲜度 SLA 与 owner

每日 06:00 由 `fr2052a_daily_batch` DAG 触发，须在 T+1 08:00 报送截止前完成（`VDQ-016` 的判据）。owner：仓库维护者。

## 上下游依赖

- **上游**：`bronze.ods_deposits`（清洗与折算）与 `silver.stg_fx_rates`。
- **下游**：`silver.owd_deposits_history`、`ows_cash_position`、`ows_cashflow_projection`、`ads_fr2052a_report` 的 Section C、`ads_fr2052a_detail`。

## 质量规则清单

本层规则共 8 条：`VDQ-003` 金额非负、`VDQ-004` 利率区间、`VDQ-005` 币种三位 ISO、`VDQ-006` 已用额度不超授信、`VDQ-007` 回购抵押品市值合理、`VDQ-008` 到期日不早于报告日、`VDQ-009` 折算误差小于 1%、`VDQ-020` LEI 格式。
