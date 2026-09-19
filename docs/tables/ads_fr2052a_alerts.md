# 表契约：ads.ads_fr2052a_alerts

## 层级

控制与审计（PostgreSQL）

## 主题

流动性预警：一行 = 一条规则的当前状态，同一规则重复命中只累加次数。

## 粒度

一行 = 一个实体 + 报告日 + 规则。

## 业务主键

`alert_id` 主键，业务唯一键 `report_date` + `entity_code` + `alert_code`。

## 去重方式

按业务键 upsert；按批次累积的表先清本批次再追加，重跑不翻倍。

## 分区

无。PostgreSQL 表，按环节写入或覆盖；不涉及分区裁剪。

## 字段清单

| 字段 | 类型 | 说明 |
|---|---|---|
| `alert_id` | BIGSERIAL PRIMARY KEY | 预警主键，交给序列生成，写入方不列此列 |
| `report_date` | DATE NOT NULL | 报告日 |
| `entity_code` | TEXT NOT NULL | 法人实体编码 |
| `alert_code` | TEXT NOT NULL | 规则编码，如 CB-LCR-001 |
| `severity` | TEXT NOT NULL | CRITICAL / WARNING / INFO |
| `metric_name` | TEXT | 触发指标名 |
| `metric_value` | NUMERIC(20, 4) | 触发时指标实际值 |
| `threshold_value` | NUMERIC(20, 4) | 判定阈值 |
| `message` | TEXT NOT NULL | 人读说明 |
| `blocks_submission` | BOOLEAN NOT NULL DEFAULT FALSE | 是否阻断报送 |
| `occurrence_count` | INTEGER NOT NULL DEFAULT 1 | 同一规则重复触发的累计次数 |
| `status` | TEXT NOT NULL DEFAULT 'OPEN' | OPEN / CLOSED，规则不再命中时置 CLOSED |
| `first_detected_at` | TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP |  |
| `last_detected_at` | TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP |  |

## 金额单位约定

`metric_value` 与 `threshold_value` 为 `NUMERIC(20,4)`，单位随指标名而定。

## PII 字段与脱敏方式

无。本表不含个人标识或直接标识。

## 生命周期

PostgreSQL 常驻表，按环节写入或覆盖；无快照与压缩策略。

## 新鲜度 SLA 与 owner

日批 `liquidity-monitor` 环节写入。owner：仓库维护者。

## 上下游依赖

- **上游**：`ads.ads_liquidity_metrics`。判定阈值来自 `config/liquidity_thresholds.json`（`python/alerts/liquidity_monitor.py` 的 `--config` 传入）。
- **下游**：放行闸 `python/validators/check_submission_gate.py` 与熔断表。

## 质量规则清单

`severity` 限定 `CRITICAL` / `WARNING` / `INFO`，`status` 限定 `OPEN` / `CLOSED`，由 CHECK 约束兜底；`blocks_submission` 为真时放行闸拦下报送。
