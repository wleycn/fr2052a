# 表契约：ads.ads_circuit_breaker

## 层级

控制与审计（PostgreSQL）

## 主题

熔断闸：全局一行的闸门状态，`OPEN` 放行、`HALTED` 拦停报送。

## 粒度

一行 = 一个 scope（当前只有 `GLOBAL`）。

## 业务主键

`scope`。

## 去重方式

按业务键 upsert；按批次累积的表先清本批次再追加，重跑不翻倍。

## 分区

无。PostgreSQL 表，按环节写入或覆盖；不涉及分区裁剪。

## 字段清单

| 字段 | 类型 | 说明 |
|---|---|---|
| `scope` | TEXT PRIMARY KEY | 报送范围，本演示只有 GLOBAL |
| `state` | TEXT NOT NULL | OPEN = 放行 / HALTED = 熔断 |
| `reason` | TEXT | 当前状态的原因 |
| `triggered_by_alert_code` | TEXT | 触发熔断的规则编码 |
| `triggered_at` | TIMESTAMP | 最近一次进入熔断的时刻 |
| `cleared_at` | TIMESTAMP | 最近一次解除熔断的时刻 |
| `trip_count` | INTEGER NOT NULL DEFAULT 0 | 累计熔断次数，只在状态翻转时加一 |
| `updated_at` | TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP |  |

## 金额单位约定

不适用。本表不含金额列。

## PII 字段与脱敏方式

无。本表不含个人标识或直接标识。

## 生命周期

PostgreSQL 常驻表，按环节写入或覆盖；无快照与压缩策略。

## 新鲜度 SLA 与 owner

日批 `liquidity-monitor` 环节按阈值判定后更新；状态变化是运维信号。owner：仓库维护者。

## 上下游依赖

- **上游**：`ads.ads_liquidity_metrics` 与 `ads.ads_fr2052a_alerts`。
- **下游**：放行闸 `check_submission_gate.py` 与巡检 `pipeline_health.py`。

## 质量规则清单

`state` 只允许 `OPEN` / `HALTED`；`triggered_by_alert_code` 指向触发它的预警规则，留痕以便回溯判定依据。
