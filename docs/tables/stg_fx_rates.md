# 表契约：silver.stg_fx_rates

## 层级

Silver（标准化明细，Iceberg `silver` 命名空间）

## 主题

汇率取数：把 ref 层汇率整理成折算可直接用的形状。

## 粒度

一行 = 一个币种在一个报告日的即期价。

## 业务主键

`currency_code` + `rate_date`。

## 去重方式

dbt `table` 物化，每次运行整表重建：先建后换，不留半成品，失败时保留上一版。

## 分区

无。整表重建，读的时候是整表替换，分区不会缩小任何一次读。

## 字段清单

| 字段 |
|---|
| `currency_code` |
| `report_currency` |
| `spot_rate` |
| `rate_date` |

列清单与顺序以 `dbt/models/staging/stg_fx_rates.sql` 的最终投影为准；类型由 Spark 在写入 Iceberg 时推断，因此这里只列字段名，不复制一份会过期的类型表。

## 金额单位约定

不适用：本表不含金额列。

## PII 字段与脱敏方式

无。本表不含个人标识或直接标识。

## 生命周期

Iceberg v2 表，快照保留 7 天或至少 10 个（`python/lakehouse/maintain_tables.py`）。小文件合并走 `rewrite_data_files`。不设额外数据保留期：整表重建即最新状态，历史由版本历史表或审计表承担。

## 新鲜度 SLA 与 owner

每日 06:00 由 `fr2052a_daily_batch` DAG 触发，须在 T+1 08:00 报送截止前完成（`VDQ-016` 的判据）。owner：仓库维护者。

## 上下游依赖

- **上游**：`ref.ref_exchange_rates`。
- **下游**：各 `owd_*` 模型的折算（`owd_deposits`、`owd_secured_financing`、`owd_loans`、`owd_securities`、`owd_derivatives`）。

## 质量规则清单

本层规则共 8 条：`VDQ-003` 金额非负、`VDQ-004` 利率区间、`VDQ-005` 币种三位 ISO、`VDQ-006` 已用额度不超授信、`VDQ-007` 回购抵押品市值合理、`VDQ-008` 到期日不早于报告日、`VDQ-009` 折算误差小于 1%、`VDQ-020` LEI 格式。
