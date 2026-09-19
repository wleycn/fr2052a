# 表契约：ads.ads_liquidity_metrics

## 层级

控制与审计（PostgreSQL）

## 主题

流动性指标：熔断判定的输入事实，一次跑批每个报告日一张快照。

## 粒度

一行 = 一个实体在一个报告日的一张指标快照。

## 业务主键

`report_date` + `entity_code`（由 `python/alerts/liquidity_monitor.py` 按此 upsert）。

## 去重方式

按业务键 upsert；按批次累积的表先清本批次再追加，重跑不翻倍。

## 分区

无。PostgreSQL 表，按环节写入或覆盖；不涉及分区裁剪。

## 字段清单

| 字段 | 类型 | 说明 |
|---|---|---|
| `report_date` | DATE NOT NULL | 报告日 |
| `entity_code` | TEXT NOT NULL | 法人实体编码：法人实体单体为 `ENT001`–`ENT005`，集团合并行用保留码 `GRP001`（不代表任何法人实体）。指标由报表行逐行换算（`python/alerts/liquidity_monitor.py`），合并行取到的即 `GRP001` |
| `is_consolidated` | BOOLEAN NOT NULL DEFAULT FALSE | 是否合并口径 |
| `hqla_l1_unencumbered_usd` | NUMERIC(20, 2) | 未质押一级资产市值 + 现金与同业存放（后者按 LCR 口径全额计入，无质押概念） |
| `hqla_l2a_unencumbered_usd` | NUMERIC(20, 2) | 未质押二级 A 类资产市值。三个等级都只计剩余期限 30 天以上的部分：窗口内到期的走 30 天流入，两边都算就是双向计量 |
| `hqla_l2b_unencumbered_usd` | NUMERIC(20, 2) | 未质押二级 B 类资产市值（同前，限 30 天以上到期） |
| `hqla_unencumbered_capped_usd` | NUMERIC(20, 2) | 未质押 HQLA 认列额，二级资产认列额按一级资产的 2/3 截断后计入 |
| `hqla_encumbered_usd` | NUMERIC(20, 2) | 已质押资产市值，不计入 LCR 分子 |
| `expected_inflow_30d_usd` | NUMERIC(20, 2) | 30 天预期流入（未加限制） |
| `expected_inflow_capped_usd` | NUMERIC(20, 2) | 30 天认列流入，上限为流出的 75% |
| `expected_outflow_30d_usd` | NUMERIC(20, 2) | 30 天预期流出 |
| `net_cash_outflow_30d_usd` | NUMERIC(20, 2) | 净现金流出 = 流出 - 认列流入 |
| `lcr_ratio` | NUMERIC(12, 4) | 流动性覆盖率 = 认列 HQLA / 净现金流出 |
| `l2_cap_ratio` | NUMERIC(12, 4) | 二级资产占 HQLA 比例，超过 0.4（等价于二级认列额超过一级的 2/3）即说明认列被上限截断 |
| `inflow_cap_ratio` | NUMERIC(12, 4) | 流入占流出比例，超过 75% 说明认列被上限截断 |
| `regulatory_min_ratio` | NUMERIC(12, 4) | 判定时采用的监管下限，留痕以便回溯口径 |
| `headroom_usd` | NUMERIC(20, 2) | 距红线余量 = 认列 HQLA - 下限 × 净现金流出 |
| `computed_at` | TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP |  |

## 金额单位约定

USD，`NUMERIC(20,2)`；比率列 `NUMERIC(12,4)`。

## PII 字段与脱敏方式

无。本表不含个人标识或直接标识。

## 生命周期

PostgreSQL 常驻表，按环节写入或覆盖；无快照与压缩策略。

## 新鲜度 SLA 与 owner

日批 `liquidity-monitor` 环节写入，判定所需的报送口径数据此前已就绪。owner：仓库维护者。

## 上下游依赖

- **上游**：报表 Section G / I / F 与 `silver.ows_*` 汇总。
- **下游**：熔断表 `ads.ads_circuit_breaker`、预警表 `ads.ads_fr2052a_alerts`、巡检 `python/governance/pipeline_health.py`。

## 质量规则清单

`VDQ-017`（二级资产认列额等于 min(原始二级市值, 一级市值 × 2/3)）与 `VDQ-018`（流入认列不超流出的 75%）在此判定；判据同时落 `ads.ads_fr2052a_validation_log`。
