# 表契约：silver.ows_cashflow_projection

## 层级

Silver（业务汇总，Iceberg `silver` 命名空间）

## 主题

现金流预测：按 Section、行项目与到期桶给出 30 天内的预期流入、流出与净额。

## 粒度

一行 = 一个实体在一个报告日的一个汇总维度组合。

## 业务主键

`report_date` + `entity_code` + `section_code` + `line_item` + `maturity_bucket`。

## 去重方式

dbt `table` 物化，每次运行整表重建：先建后换，不留半成品，失败时保留上一版。

## 分区

无。整表重建，读的时候是整表替换，分区不会缩小任何一次读。

## 字段清单

| 字段 |
|---|
| `report_date` |
| `entity_code` |
| `section_code` |
| `line_item` |
| `maturity_bucket` |
| `expected_inflow_usd` |
| `expected_outflow_usd` |
| `net_cash_flow_usd` |

列清单与顺序以 `dbt/models/intermediate/ows_cashflow_projection.sql` 的最终投影为准；类型由 Spark 在写入 Iceberg 时推断，因此这里只列字段名，不复制一份会过期的类型表。

## 金额单位约定

USD，`DECIMAL(18,2)`。保留 `*_lc` 原币列与 `exchange_rate` 用于核对折算。

## PII 字段与脱敏方式

无。汇总层不含标识列。

## 生命周期

Iceberg v2 表，快照保留 7 天或至少 10 个（`python/lakehouse/maintain_tables.py`）。小文件合并走 `rewrite_data_files`。不设额外数据保留期：整表重建即最新状态，历史由版本历史表或审计表承担。

## 新鲜度 SLA 与 owner

每日 06:00 由 `fr2052a_daily_batch` DAG 触发，须在 T+1 08:00 报送截止前完成（`VDQ-016` 的判据）。owner：仓库维护者。

## 上下游依赖

- **上游**：`silver.owd_loans`、`silver.owd_deposits`、`silver.owd_off_bs`，流出率取自行为假设。
- **下游**：报表 Section F 与 Section K、`ads.ads_liquidity_metrics` 的流入流出两侧。

## 质量规则清单

本层规则共 3 条：`VDQ-010` 汇总等于明细求和、`VDQ-011` 非受限不超总量、`VDQ-012` 质押不超市值。
