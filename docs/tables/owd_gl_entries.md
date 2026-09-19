# 表契约：silver.owd_gl_entries

## 层级

Silver（标准化明细，Iceberg `silver` 命名空间）

## 主题

标准化总账余额：借贷方轧成净额，标出余额方向。

## 粒度

一行 = 一个实体在一个报告日的一个科目。

## 业务主键

`source_system` + `source_record_id` + `report_date`。与 `python/lakehouse/owd_scd2.py` 的 `KEY_COLUMNS` 一致；必须带 `report_date`：同一源记录号在不同报告日是两条独立记录，去掉它会把不同报告日的两天并成同一条记录、抹掉其中一天。

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
| `gl_account_id` |
| `account_name` |
| `debit_balance` |
| `credit_balance` |
| `currency_code` |
| `net_balance_usd` |
| `balance_side` |
| `etl_batch_id` |

列清单与顺序以 `dbt/models/staging/owd_gl_entries.sql` 的最终投影为准；类型由 Spark 在写入 Iceberg 时推断，因此这里只列字段名，不复制一份会过期的类型表。

## 金额单位约定

USD，`DECIMAL(18,2)`。总账本身以集团本位币 USD 记账，不做折算，因此本表既没有原币列也没有汇率列。

## PII 字段与脱敏方式

无。总账科目不含客户标识。

## 生命周期

Iceberg v2 表，快照保留 7 天或至少 10 个（`python/lakehouse/maintain_tables.py`）。小文件合并走 `rewrite_data_files`。不设额外数据保留期：整表重建即最新状态，历史由版本历史表或审计表承担。

## 新鲜度 SLA 与 owner

每日 06:00 由 `fr2052a_daily_batch` DAG 触发，须在 T+1 08:00 报送截止前完成（`VDQ-016` 的判据）。owner：仓库维护者。

## 上下游依赖

- **上游**：`bronze.ods_gl_balances`。
- **下游**：`silver.owd_gl_entries_history`、`ads_gl_reconciliation` 的对账。

## 质量规则清单

本层规则共 8 条：`VDQ-003` 金额非负、`VDQ-004` 利率区间、`VDQ-005` 币种三位 ISO、`VDQ-006` 已用额度不超授信、`VDQ-007` 回购抵押品市值合理、`VDQ-008` 到期日不早于报告日、`VDQ-009` 折算误差小于 1%、`VDQ-020` LEI 格式。
