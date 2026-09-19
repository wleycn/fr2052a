# 表契约：silver.owd_treasury_cash_position

## 层级

Silver（标准化明细，Iceberg `silver` 命名空间）

## 主题

标准化现金头寸：外币折算为 USD，作为 GL 对账 Section E 的基准侧。

## 粒度

一行 = 一个实体在一个报告日的一个头寸记录。

## 业务主键

`source_system` + `source_record_id` + `report_date`。

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
| `position_type` |
| `custodian_id` |
| `account_ref` |
| `currency_code` |
| `balance_usd` |
| `in_transit_deposits_usd` |
| `outstanding_checks_usd` |
| `event_time` |
| `etl_batch_id` |

列清单与顺序以 `dbt/models/staging/owd_treasury_cash_position.sql` 的最终投影为准；类型由 Spark 在写入 Iceberg 时推断，因此这里只列字段名，不复制一份会过期的类型表。`balance_usd` 为对账单余额或盘点金额折美元；`in_transit_deposits_usd` 为在途存款折美元（库存现金恒为 0）；`outstanding_checks_usd` 为未兑现支票折美元（库存现金恒为 0）。本表没有 `maturity_bucket`：余额快照没有到期日概念，已在 `python/lakehouse/verify_silver.py` 的 `TABLES_WITHOUT_MATURITY` 登记。

## 金额单位约定

USD。金额列一律为 `decimal` 类型，小数位固定 2 位；整数位精度由 Spark 按源类型推断。本表不保留原币列与汇率列，只存折美元后的金额。

折算通过 join `stg_fx_rates` 完成，连接键是 `currency_code` 与 `rate_date = report_date`。参与折算的是上游 `bronze.ods_treasury_cash_position` 的三个原币金额列：`balance_amount`、`in_transit_deposits_amount`、`outstanding_checks_amount`。

三列各自的折算结果落在本表的 `balance_usd`、`in_transit_deposits_usd`、`outstanding_checks_usd`，算法是乘 `spot_rate` 后四舍五入到 2 位小数。核对折算须回到上游读原币金额。

## PII 字段与脱敏方式

无。`custodian_id` 是保管机构、`account_ref` 是本行自有账户标识，都不是个人数据。

## 生命周期

Iceberg v2 表，快照保留 7 天或至少 10 个（`python/lakehouse/maintain_tables.py`）。小文件合并走 `rewrite_data_files`。不设额外数据保留期：整表重建即最新状态，历史由版本历史表或审计表承担。

## 新鲜度 SLA 与 owner

每日 06:00 由 `fr2052a_daily_batch` DAG 触发，须在 T+1 08:00 报送截止前完成。owner：仓库维护者。

## 上下游依赖

- **上游**：`bronze.ods_treasury_cash_position`。
- **下游**：`silver.owd_treasury_cash_position_history`、`gold.ads_gl_reconciliation`（Section E 的基准金额与调节项）、`python/lakehouse/verify_silver.py`（行数与折算重算）、`python/lakehouse/verify_gold.py`（对账基准独立性核对）、`python/lakehouse/verify_scd2.py`、`python/validators/run_dq_rules.py`（VDQ-005 币种长度）。

## 质量规则清单

本表适用的单表规则共 1 条：`VDQ-005` 币种是三位 ISO 4217 代码。

跨表规则 `VDQ-009` 至 `VDQ-015` 由核对脚本覆盖（`verify_silver.py`、`verify_gold.py` 与 GL 对账模型），不在跑批的单表断言里。`VDQ-017` 至 `VDQ-019` 针对 `gold.ads_fr2052a_report`，`VDQ-020` 针对 `ref.ref_counterparty`，`VDQ-021` 针对 `bronze.ods_securities`，都不落本表。
