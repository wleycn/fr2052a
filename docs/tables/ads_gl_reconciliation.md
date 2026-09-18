# 表契约：gold.ads_gl_reconciliation（Iceberg）→ `ads.ads_gl_reconciliation`（PostgreSQL）

## 层级

Gold（Iceberg）与 ADS（PostgreSQL）同一份数据的两个落点

## 主题

总账对账结果：报表口径与总账口径按 Section 逐项比对。

## 粒度

一行 = 一个 Section 在一个报告日的一个总账科目。

## 业务主键

`report_date` + `section_code` + `gl_account_id`。

## 去重方式

dbt `table` 物化整表重建；导出 PostgreSQL 时先清后写。

## 分区

无。dbt `table` 物化整表重建，每次读都是整表替换，分区不会缩小任何一次读；需要按日回溯时走版本历史表。

## 字段清单

| 字段 |
|---|
| `report_date` |
| `section_code` |
| `gl_account_id` |
| `account_name` |
| `gl_amount` |
| `fr2052a_amount` |
| `variance` |
| `status` |

列清单与顺序以 `dbt/models/marts/ads_gl_reconciliation.sql` 的最终投影为准；类型由 Spark 在写入 Iceberg 时推断，因此这里只列字段名，不复制一份会过期的类型表。

## 金额单位约定

USD，`DECIMAL(20,2)`。总账侧按 Section 汇总后与报表比，不按单个科目比。

## PII 字段与脱敏方式

无。本表不含个人标识或直接标识。

## 生命周期

随跑批整表重建，PostgreSQL 侧随导出覆盖写。

## 新鲜度 SLA 与 owner

随日批产出，须在 T+1 08:00 报送截止前可用。owner：Finance Control（声明于 `dbt/models/marts/schema.yml` 的 meta）。

## 上下游依赖

- **上游**：`silver.owd_gl_entries` 与报表 `gold.ads_fr2052a_report`。
- **下游**：GL 对账 DAG `fr2052a_gl_reconciliation`、放行闸与熔断判定。

## 质量规则清单

`VDQ-013` 与 `VDQ-014` 覆盖 Section 合计与融资总量；差额超过阈值即 `status = FAIL`，说明报表与总账两套口径已经分叉。
