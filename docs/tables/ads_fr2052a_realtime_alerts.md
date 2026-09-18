# 表契约：ads.ads_fr2052a_realtime_alerts

## 层级

控制与审计（PostgreSQL）

## 主题

实时敞口事件流水：一行 = 一笔被识别出来的大额未保险存款事件。

## 粒度

一行 = 一笔事件（`source_topic` + `source_record_id` + `report_date`）。

## 业务主键

`event_id` 主键；业务键为 `source_topic` + `source_record_id` + `report_date`。

## 去重方式

按业务键 upsert；按批次累积的表先清本批次再追加，重跑不翻倍。

## 分区

无。PostgreSQL 表，按环节写入或覆盖；不涉及分区裁剪。

## 字段清单

| 字段 | 类型 | 说明 |
|---|---|---|
| `event_id` | TEXT PRIMARY KEY | 事件唯一键：源记录 + 报告日 + 规则 的哈希 |
| `report_date` | DATE NOT NULL | 取自消息里的报告日 |
| `entity_code` | TEXT NOT NULL | 记账法人实体 |
| `alert_code` | TEXT NOT NULL | 规则编码 |
| `severity` | TEXT NOT NULL | CRITICAL / WARNING / INFO |
| `source_topic` | TEXT NOT NULL | 来源主题 |
| `source_record_id` | TEXT NOT NULL | 来源记录主键 |
| `amount_usd` | NUMERIC(20, 2) | 敞口金额（USD） |
| `segment` | TEXT | 客户类别，如 CORP / GOV / FI |
| `message` | TEXT NOT NULL | 人读说明 |
| `detected_at` | TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP |  |

## 金额单位约定

`amount_usd` 为 USD。原币在 ODS，折算发生在 OWD，本表只收 USD 口径。

## PII 字段与脱敏方式

无。事件只带实体、金额与客群，不带账户号。

## 生命周期

PostgreSQL 常驻表，按环节写入或覆盖；无快照与压缩策略。

## 新鲜度 SLA 与 owner

日批 `realtime-scan` 环节消费核心存款主题后写入，非实时流。owner：仓库维护者。

## 上下游依赖

- **上游**：Kafka 主题 `core_banking_txns`，经 `python/alerts/realtime_scanner.py` 识别。
- **下游**：汇总脚本 `python/alerts/summarize_realtime_alerts.py`。

## 质量规则清单

与 `ads_fr2052a_alerts` 分表：本表是事件流水，那张是规则状态；混表会让「一行代表什么」说不清。
