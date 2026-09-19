# 表契约：silver.ows_hqla_summary

## 层级

Silver（业务汇总，Iceberg `silver` 命名空间）

## 主题

HQLA 汇总：按实体与 HQLA 分类汇总市值、折扣后价值、受限与可用额。

## 粒度

一行 = 一个实体在一个报告日的一个汇总维度组合。

## 业务主键

`report_date` + `entity_code` + `hqla_classification`。

## 去重方式

dbt `table` 物化，每次运行整表重建：先建后换，不留半成品，失败时保留上一版。

## 分区

无。整表重建，读的时候是整表替换，分区不会缩小任何一次读。

## 字段清单

| 字段 |
|---|
| `report_date` |
| `entity_code` |
| `hqla_classification` |
| `hqla_haircut_rate` |
| `record_count` |
| `market_value_usd` |
| `haircuted_value_usd` |
| `encumbered_value_usd` |
| `unencumbered_value_usd` |
| `available_liquidity_usd` |

列清单与顺序以 `dbt/models/intermediate/ows_hqla_summary.sql` 的最终投影为准；类型由 Spark 在写入 Iceberg 时推断，因此这里只列字段名，不复制一份会过期的类型表。

## 金额单位约定

USD，`DECIMAL(18,2)`。本层不保留原币列，也没有汇率列：五个 OWS 模型的最终投影里 `*_lc` 与 `exchange_rate` 均为 0 列 —— 折算在 staging 层完成（`dbt/models/staging/owd_deposits.sql` 保留 `principal_amount_lc`、`accrued_interest_lc` 与 `exchange_rate` 供核对），本层所有金额已是 USD。

## PII 字段与脱敏方式

无。汇总层不含标识列。

## 生命周期

Iceberg v2 表，快照保留 7 天或至少 10 个（`python/lakehouse/maintain_tables.py`）。小文件合并走 `rewrite_data_files`。不设额外数据保留期：整表重建即最新状态，历史由版本历史表或审计表承担。

## 新鲜度 SLA 与 owner

每日 06:00 由 `fr2052a_daily_batch` DAG 触发，须在 T+1 08:00 报送截止前完成（`VDQ-016` 的判据）。owner：仓库维护者。

## 上下游依赖

- **上游**：`silver.owd_securities`。
- **下游**：当前无消费者 —— 报表直接读 `owd_securities` 明细，`ads.ads_liquidity_metrics` 读的是报表列。本表已登记为待下线（见 KNOWN-ISSUE）。

## 质量规则清单

本层**没有独立的校验环节**：全仓 `python/`、`dbt/` 中 0 处引用 `silver.ows*`，`python/validators/run_dq_rules.py` 的 `RULE_TARGETS` 也不含任何 OWS 表。`ref.ref_validation_rules` 里标 `apply_layer = OWS` 的 3 条（`VDQ-010` 汇总等于明细求和、`VDQ-011` 非受限不超总量、`VDQ-012` 质押不超市值）在 `run_dq_rules.py` 归入 `CROSS_TABLE_RULES`，只标注「由核对脚本 `python/lakehouse/verify_gold.py` 覆盖」，而该复算是报表级的（输入取自 `silver.owd_*` 明细与 `gold` 报表）。本表当前无消费者（见「上下游依赖」），也不在任何复算的输入里 —— 它自身的正确性没有任何校验覆盖。
