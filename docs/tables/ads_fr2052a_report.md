# 表契约：gold.ads_fr2052a_report（Iceberg）→ `ads.ads_fr2052a_report`（PostgreSQL）

## 层级

Gold（Iceberg）与 ADS（PostgreSQL）同一份数据的两个落点

## 主题

FR 2052a 报送报表：按 Section A–K 装配行项目金额，每个报送主体一行。

## 粒度

一行 = 一个报送主体（报告日 + 实体 + 口径）。三档视角：全球合并（`GRP001`）、法人实体单体、母公司单体。合并行只汇总 `is_intracompany = false` 的明细行，集团内往来的存款腿与贷款腿两侧同时排除。

## 业务主键

`report_id`，四段区位码「机构-报表-报告期-口径」，例 `GRP001-FR2052A-20260916-01`。

口径码三档（末段）：
- `01` = 全球合并（`is_consolidated` 为真，`entity_code = 'GRP001'`）。`GRP001` 是保留码，不代表任何法人实体，只用于合并行。
- `02` = 法人实体单体（`is_consolidated` 为假且 `entity_code <> 'ENT001'`）。
- `03` = 母公司单体（`is_consolidated` 为假且 `entity_code = 'ENT001'`）。

口径码由 `is_consolidated` + `entity_code` 推出，不另加常量列。

## 去重方式

dbt `table` 物化整表重建；导出 PostgreSQL 时先清后写并开 `truncate=true`。

## 分区

无。dbt `table` 物化整表重建，每次读都是整表替换，分区不会缩小任何一次读；需要按日回溯时走版本历史表。

## 字段清单

| 字段 |
|---|
| `entity_code` |
| `sec_a_cp_outstanding` |
| `sec_a_cd_outstanding` |
| `sec_a_unsecured_borrow` |
| `sec_a_fed_funds` |
| `sec_a_total` |
| `sec_b_repo_outstanding` |
| `sec_b_reverse_repo` |
| `sec_b_sec_lending` |
| `sec_b_fhlb_advances` |
| `sec_b_total` |
| `sec_c_retail_demand` |
| `sec_c_retail_savings` |
| `sec_c_retail_time` |
| `sec_c_wholesale_demand` |
| `sec_c_wholesale_time` |
| `sec_c_brokered` |
| `sec_c_total` |
| `sec_c_insured_total` |
| `sec_d_total` |
| `sec_e_cash_total` |
| `sec_e_cash_on_hand` |
| `sec_e_due_from_banks` |
| `sec_e_central_bank_dep` |
| `sec_f_commercial_inflow` |
| `sec_f_retail_inflow` |
| `sec_f_mortgage_inflow` |
| `sec_f_total_inflow` |
| `sec_g_hqla_l1_mv` |
| `sec_g_hqla_l2a_mv` |
| `sec_g_hqla_l2b_mv` |
| `sec_g_non_hqla_mv` |
| `sec_g_total_mv` |
| `sec_g_hqla_capped_total_usd` |
| `sec_h_net_mtm_asset` |
| `sec_h_net_mtm_liability` |
| `sec_h_collateral_posted` |
| `sec_h_collateral_received` |
| `sec_h_expected_inflow_30d` |
| `sec_h_expected_outflow_30d` |
| `sec_i_unencumbered_hqla_l1` |
| `sec_i_unencumbered_hqla_l2a` |
| `sec_i_unencumbered_hqla_l2b` |
| `sec_i_unencumbered_non_hqla` |
| `sec_i_encumbered_total` |
| `sec_j_credit_commitments` |
| `sec_j_letters_of_credit` |
| `sec_j_guarantees` |
| `sec_j_total_contingent` |
| `sec_k_total_funding` |
| `sec_k_total_inflows` |
| `sec_k_total_outflows` |
| `sec_k_net_funding_gap` |
| `sec_k_cumulative_30d_gap` |
| `report_date` |
| `is_consolidated` |
| `report_id` |

列清单与顺序以 `dbt/models/marts/ads_fr2052a_report.sql` 的最终投影为准；类型由 Spark 在写入 Iceberg 时推断，因此这里只列字段名，不复制一份会过期的类型表。

## 金额单位约定

USD，`DECIMAL(20,2)`（导出时由 Spark 推断）。二级资产已按 40% 上限截断，流入已按流出的 75% 上限认列。

## PII 字段与脱敏方式

无。报表层只有聚合金额。

## 生命周期

Iceberg 侧随跑批整表重建；PostgreSQL 侧随导出覆盖写，保留授权与触发器。

## 新鲜度 SLA 与 owner

随日批产出，须在 T+1 08:00 报送截止前可用。owner：Treasury Liquidity Team（声明于 `dbt/models/marts/schema.yml` 的 meta）。

## 上下游依赖

- **上游**：`silver.ows_cash_position`、`ows_cashflow_projection` 与各 `owd_*` 明细（存款、有担保融资、贷款、证券、衍生品、表外、总账）。`ows_hqla_summary`、`ows_collateral_summary`、`ows_funding_summary` 三张 OWS 表无消费者，已登记待下线。
- **下游**：报送文件生成 `python/exporters/generate_submission.py`、放行闸 `python/validators/check_submission_gate.py`、报表版本历史 `ads.ads_fr2052a_report_history`。

## 质量规则清单

本层规则共 6 条：`VDQ-013` Section 合计等于行项目、`VDQ-014` 融资总量对资产负债表、`VDQ-015` 期比异动、`VDQ-017` 二级资产不超 HQLA 的 40%、`VDQ-018` 流入认列不超流出的 75%、`VDQ-019` 必备行项目齐备。 另由 `python/lakehouse/verify_gold.py` 在 `verify-ads` 环节核对合并口径、明细回溯与监管上限。
