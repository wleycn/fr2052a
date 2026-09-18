# 表契约：ads.ads_fr2052a_submission

## 层级

控制与审计（PostgreSQL）

## 主题

报送台账：一行 = 一个报送文件当前状态，含路径、哈希、字节数与回执。

## 粒度

一行 = 一个报送主体的一个文件（当前版本）。

## 业务主键

`submission_id` 主键；唯一键 `report_id` + `file_format`。

## 去重方式

按业务键（report_id + file_format）upsert。重生成时更新哈希、大小、时间与回执，不堆新行。

## 分区

无。PostgreSQL 表，按环节写入或覆盖；不涉及分区裁剪。

## 字段清单

| 字段 | 类型 | 说明 |
|---|---|---|
| `submission_id` | BIGSERIAL PRIMARY KEY | 报送主键 |
| `report_id` | TEXT NOT NULL | 报表主键（report_date-entity_code） |
| `report_date` | DATE NOT NULL | 报告日 |
| `entity_code` | TEXT NOT NULL | 法人实体编码 |
| `file_format` | TEXT NOT NULL | 文件格式：XBRL / XML / CSV |
| `file_path` | TEXT NOT NULL | 文件在报送服务端落盘路径 |
| `file_hash` | TEXT NOT NULL | 当前版本的文件 SHA-256（重生成时被更新） |
| `file_size_bytes` | BIGINT | 文件字节数 |
| `submitted_at` | TIMESTAMP | 提交时刻 |
| `submission_status` | TEXT NOT NULL | GENERATED / SUBMITTED / ACCEPTED / REJECTED |
| `receipt_id` | TEXT | 回执编号 |
| `receipt_message` | TEXT | 回执消息 |
| `created_at` | TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP |  |

## 金额单位约定

不适用。本表不含金额列。

## PII 字段与脱敏方式

无。本表不含个人标识或直接标识。

## 生命周期

PostgreSQL 常驻表，按业务键 upsert 覆盖当前状态；无快照与压缩策略。重生成历史住 `ads_fr2052a_submission_audit`。

## 新鲜度 SLA 与 owner

日批 `submission` 环节产出文件后写台账；报送截止为 T+1 08:00。owner：仓库维护者。

## 上下游依赖

- **上游**：`python/exporters/generate_submission.py` 产出的 XBRL / XML / CSV 文件。
- **下游**：报送核对与审计留痕。

## 质量规则清单

回执与文件哈希由 `python/validators/verify_submission.py` 在 `verify-submission` 环节核对；同一文件重复报送由唯一键挡下。重生成历史由 `ads.ads_fr2052a_submission_audit` 留痕。
