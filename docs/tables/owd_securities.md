# 表契约：silver.owd_securities

## 层级

Silver（标准化明细，Iceberg `silver` 命名空间）

## 主题

标准化证券：折算 USD、HQLA 分类与折扣率、质押与受限标记。

## 粒度

一行 = 一笔证券持仓。

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
| `security_id` |
| `isin` |
| `cusip` |
| `security_type` |
| `portfolio_category` |
| `issuer_id` |
| `issuer_name` |
| `issuer_type` |
| `issuer_country` |
| `currency_code` |
| `face_amount_usd` |
| `market_value_usd` |
| `book_value_usd` |
| `unrealized_gl_usd` |
| `coupon_rate` |
| `credit_rating` |
| `hqla_classification` |
| `hqla_haircut_rate` |
| `is_pledged` |
| `is_encumbered` |
| `purchase_date` |
| `maturity_date` |
| `days_to_maturity` |
| `maturity_bucket` |
| `event_time` |
| `etl_batch_id` |

列清单与顺序以 `dbt/models/staging/owd_securities.sql` 的最终投影为准；类型由 Spark 在写入 Iceberg 时推断，因此这里只列字段名，不复制一份会过期的类型表。

## 金额单位约定

USD，`DECIMAL(18,2)`。保留 `*_lc` 原币列与 `exchange_rate` 用于核对折算。

## PII 字段与脱敏方式

无直接标识。`issuer_id` 是发行机构编码。

## 生命周期

Iceberg v2 表，快照保留 7 天或至少 10 个（`python/lakehouse/maintain_tables.py`）。小文件合并走 `rewrite_data_files`。不设额外数据保留期：整表重建即最新状态，历史由版本历史表或审计表承担。

## 新鲜度 SLA 与 owner

每日 06:00 由 `fr2052a_daily_batch` DAG 触发，须在 T+1 08:00 报送截止前完成（`VDQ-016` 的判据）。owner：仓库维护者。

## 上下游依赖

- **上游**：`bronze.ods_securities` 与 `silver.stg_fx_rates`。
- **下游**：`silver.owd_securities_history`、`ows_hqla_summary`、报表 Section G 与 Section I。

## 质量规则清单

本层规则共 8 条：`VDQ-003` 金额非负、`VDQ-004` 利率区间、`VDQ-005` 币种三位 ISO、`VDQ-006` 已用额度不超授信、`VDQ-007` 回购抵押品市值合理、`VDQ-008` 到期日不早于报告日、`VDQ-009` 折算误差小于 1%、`VDQ-020` LEI 格式。
