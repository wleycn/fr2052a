# 表契约：gold.ads_fr2052a_detail（Iceberg）→ `ads.ads_fr2052a_detail`（PostgreSQL）

## 层级

Gold（Iceberg）与 ADS（PostgreSQL）同一份数据的两个落点

## 主题

报表明细行：报表上每个 Section 金额背后按产品、对手方类型、币种与到期桶展开的行。

## 粒度

一行 = 报告日 + 实体 + Section + 行项目 + 产品 + 对手方类型 + 币种 + 到期桶 + 集团内标记。

## 业务主键

无单独主键列，业务键为上述维度组合。

## 去重方式

dbt `table` 物化整表重建；导出 PostgreSQL 时先清后写。

## 分区

无。dbt `table` 物化整表重建，每次读都是整表替换，分区不会缩小任何一次读；需要按日回溯时走版本历史表。

## 字段清单

| 字段 |
|---|
| `report_date` |
| `entity_code` |
| `is_intracompany` |
| `section_code` |
| `line_item` |
| `line_description` |
| `product_category` |
| `counterparty_type` |
| `currency_code` |
| `maturity_bucket` |
| `outstanding_amount` |
| `inflow_amount` |
| `outflow_amount` |
| `net_amount` |
| `market_value` |

列清单与顺序以 `dbt/models/marts/ads_fr2052a_detail.sql` 的最终投影为准；类型由 Spark 在写入 Iceberg 时推断，因此这里只列字段名，不复制一份会过期的类型表。

## 金额单位约定

USD，`DECIMAL(20,2)`。

## PII 字段与脱敏方式

无。明细已聚合到维度组合，不含客户标识。

## 生命周期

随跑批整表重建，PostgreSQL 侧随导出覆盖写。

## 新鲜度 SLA 与 owner

随日批产出，须在 T+1 08:00 报送截止前可用。owner：Treasury Liquidity Team（声明于 `dbt/models/marts/schema.yml` 的 meta）。

## 上下游依赖

- **上游**：`silver.owd_deposits`、`owd_secured_financing`、`owd_loans`、`owd_securities`、`owd_off_bs`。
- **下游**：报表数字的人工回溯与 Section 级核对。

## 质量规则清单

明细是回溯报表数字的底稿，核对点写在 `verify_gold.py` 的明细回溯检查里；本层不单列 VDQ。
