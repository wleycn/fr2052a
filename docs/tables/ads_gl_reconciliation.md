# 表契约：gold.ads_gl_reconciliation（Iceberg）→ `ads.ads_gl_reconciliation`（PostgreSQL）

## 层级

Gold（Iceberg）与 ADS（PostgreSQL）同一份数据的两个落点

## 主题

对账结果：报送口径与**独立基准**按 Section 逐项比对。

基准侧分两类（`benchmark_source` 列标明）：非现金 Section 取总账科目余额；Section E（现金）取司库现金头寸，即银行对账单余额与库存现金盘点数。

Section E 为什么不沿用总账：报送侧的现金本身就出自总账 `1001/1100`，两侧同源等于自己跟自己比，差异恒为零，改坏任何一侧都查不出来。换成司库口径后两侧是两个来源，差额由在途存款与未兑现支票逐项解释，记在 `reconciling_item_usd` 列。

## 粒度

一行 = 报告期 × 视角 × Section。视角由 `entity_code` 区分：`GRP001` 为全球合并，其余为法人实体单体。

## 业务主键

`report_date` + `entity_code` + `section_code`（库侧有同名唯一约束 `ads_gl_reconciliation_uk`）。

`gl_account_id` 不是键的一部分：一个 Section 可能对应多个总账科目（`C` = `2001+2002`、`E` = `1001+1100`），该列存的是科目组合的展示串，不拆分行。

## 去重方式

dbt `table` 物化整表重建；导出 PostgreSQL 时先清后写。

## 分区

无。dbt `table` 物化整表重建，每次读都是整表替换，分区不会缩小任何一次读；需要按日回溯时走版本历史表。

## 字段清单

| 字段 |
|---|
| `report_date` |
| `entity_code` |
| `section_code` |
| `gl_account_id` |
| `account_name` |
| `benchmark_amount` |
| `benchmark_source` |
| `reconciling_item_usd` |
| `fr2052a_amount` |
| `variance` |
| `status` |

列清单与顺序以 `dbt/models/marts/ads_gl_reconciliation.sql` 的最终投影为准；类型由 Spark 在写入 Iceberg 时推断，因此这里只列字段名，不复制一份会过期的类型表。

## 金额单位约定

USD，`DECIMAL(20,2)`。基准侧按 Section 汇总后与报送金额比，不按单个科目比。

判定公式：`variance = benchmark_amount + reconciling_item_usd − fr2052a_amount`，落在容差内判 `PASS`，否则 `FAIL`（`FAIL` 阻断报送）。

容差分两档：总账口径基准为报送金额的 1% 且不少于 1 分钱；对账单口径基准（Section E）只留 1 分钱 —— 差额已被调节项逐项解释，再留 1% 等于放行几十万的错误。

## PII 字段与脱敏方式

无。本表不含个人标识或直接标识。

## 生命周期

随跑批整表重建，PostgreSQL 侧随导出覆盖写。

## 新鲜度 SLA 与 owner

随日批产出，须在 T+1 08:00 报送截止前可用。owner：Finance Control（声明于 `dbt/models/marts/schema.yml` 的 meta）。

## 上下游依赖

- **上游**：`silver.owd_gl_entries`（非现金 Section 的基准）、`silver.owd_treasury_cash_position`（Section E 的基准与调节项）。Section G / I 的非现金基准由本模型自算，不读报表。
- **下游**：GL 对账 DAG `fr2052a_gl_reconciliation`、放行闸与熔断判定。

## 质量规则清单

`VDQ-013` 与 `VDQ-014` 覆盖 Section 合计与融资总量；差额超过阈值即 `status = FAIL`，说明报表与基准两套口径已经分叉。

列级声明见 `dbt/models/marts/schema.yml`（`FR2052A-REC-01` 至 `FR2052A-REC-05`）。

另有两条 dbt 断言守着「对账有意义」这条前提，任一失败即视为对账结构被破坏：
- `assert_recon_benchmark_independent`：Section E 的基准来源必须是司库现金头寸、基准与报送金额必须不相等、调节项必须非零 —— 抓「退回自比对」；
- `assert_fx_covered`：对账单金额折 USD 的汇率必须齐备。
