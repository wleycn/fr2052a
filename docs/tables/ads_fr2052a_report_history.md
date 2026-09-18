# 表契约：ads.ads_fr2052a_report_history

## 层级

控制与审计（PostgreSQL）

## 主题

报表版本历史：每份报表的每个版本一行，`snapshot` 存整行快照。

## 粒度

一行 = 一份报表（`report_id`）的一个版本（`record_version`）。

## 业务主键

`report_version_id` 主键（`report_id` + `-v` + 版本号）；唯一键 `report_id` + `record_version`。

## 去重方式

按业务键 upsert；按批次累积的表先清本批次再追加，重跑不翻倍。

## 分区

无。PostgreSQL 表，按环节写入或覆盖；不涉及分区裁剪。

## 字段清单

| 字段 | 类型 | 说明 |
|---|---|---|
| `report_version_id` | TEXT PRIMARY KEY | 版本主键：report_id || '-v' || record_version |
| `report_id` | TEXT NOT NULL | 报表身份，同一份报表的各版本共用 |
| `record_version` | INTEGER NOT NULL | 版本号，从 1 起 |
| `report_date` | DATE NOT NULL | 报告日 |
| `entity_code` | TEXT NOT NULL | 法人实体编码 |
| `begin_date` | DATE NOT NULL | 本版本生效日（处理日） |
| `end_date` | DATE | 本版本失效日；**为空表示当前有效** |
| `is_active` | BOOLEAN NOT NULL DEFAULT TRUE | end_date 的冗余列，便于建索引 |
| `last_modified_reason` | TEXT NOT NULL DEFAULT 'ORIGINAL' | ORIGINAL / CORRECTION / RESTATEMENT |
| `snapshot` | JSONB NOT NULL | 该版本报表的整行快照 |
| `snapshot_hash` | TEXT | 快照整行哈希，用于比对「这一版到底变没变」 |
| `captured_at` | TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP |  |

## 金额单位约定

随报表：快照里的金额为 USD。

## PII 字段与脱敏方式

无。本表不含个人标识或直接标识。

## 生命周期

PostgreSQL 常驻表，按环节写入或覆盖；无快照与压缩策略。

## 新鲜度 SLA 与 owner

日批 `restate-capture` 环节写入。owner：仓库维护者。

## 上下游依赖

- **上游**：`ads.ads_fr2052a_report`。
- **下游**：`ads.ads_restatement_log` 与审计追溯。

## 质量规则清单

`CHECK ((end_date IS NULL) = is_active)`：`is_active` 是冗余列，允许冗余但不允许与 `end_date` 分叉。`snapshot_hash` 用来判断「这一版到底变没变」。
