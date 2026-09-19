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

本表不是 `run_dq_rules.py` 的规则目标：`RULE_TARGETS` 里那 8 条规则没有一条落到 `silver.stg_fx_rates` —— `VDQ-003` 落 `silver.owd_deposits`，`VDQ-004` 落 `owd_deposits`/`owd_secured_financing`/`owd_loans`，`VDQ-005` 落 `ALL_OWD`，`VDQ-006` 落 `owd_loans`，`VDQ-007` 落 `owd_secured_financing`，`VDQ-008` 落 5 张 `owd_*`，`VDQ-009` 是跨表规则（由 `python/lakehouse/verify_silver.py` 重算覆盖），`VDQ-020` 落 `ref.ref_counterparty`。

本表自身的正确性由 dbt 单数测试 `dbt/tests/assert_fx_covered.sql` 守：ODS 六张明细表出现的 `(report_date, currency)` 必须在 `stg_fx_rates` 里有 MID 汇率，缺一条即测试失败。判据的用意是「折算失败必须出声」—— 缺汇率时 OWD 的 join 落空、金额列变 NULL、报表出来是 0 而不报错。

折算链路上游（各 ODS 表）由 `ALL_BRONZE` 上的 `VDQ-002`（关键字段非空）与 `VDQ-016`（T+1 加载时效）把关；`ref.ref_exchange_rates` 不设 VDQ 校验。
