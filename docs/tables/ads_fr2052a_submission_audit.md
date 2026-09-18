# 表契约：ads.ads_fr2052a_submission_audit

## 层级

控制与审计（PostgreSQL）

## 主题

报送重生成审计流水：一行 = 一次文件生成，记录哈希是否变化。

## 粒度

一行 = 一个报送主体的一种文件格式的一次生成。

## 业务主键

`audit_id` 主键；业务键 `report_id` + `file_format` + `generated_at`。

## 去重方式

只追加，不 upsert。每次生成都写一行，不论内容是否变化。

## 分区

无。PostgreSQL 表，按业务键索引查询；不涉及分区裁剪。

## 字段清单

| 字段 | 类型 | 说明 |
|---|---|---|
| `audit_id` | BIGSERIAL PRIMARY KEY | 审计主键 |
| `report_id` | TEXT NOT NULL | 报表主键，与台账同源 |
| `file_format` | TEXT NOT NULL | 文件格式：XBRL / XML / CSV |
| `file_hash` | TEXT NOT NULL | 本次生成的文件 SHA-256 |
| `previous_hash` | TEXT | 被替换的上一次哈希；首次生成为 NULL |
| `file_size_bytes` | BIGINT | 文件字节数 |
| `is_content_change` | BOOLEAN NOT NULL | 与上一条台账状态相比内容是否变化，首次生成为 true |
| `entry_source` | TEXT NOT NULL DEFAULT 'GENERATE' | GENERATE = 报送脚本写的，MIGRATION = 迁移回填的 |
| `generated_at` | TIMESTAMP NOT NULL | 本次生成的时刻 |

## 金额单位约定

不适用。本表不含金额列。

## PII 字段与脱敏方式

无。本表不含个人标识或直接标识。

## 生命周期

PostgreSQL 常驻表，只追加不覆盖。复位时由 `sql/admin/reset_demo.sql` 一并清空。

## 新鲜度 SLA 与 owner

日批 `submission` 环节每次生成报送文件时写一行。owner：仓库维护者。

## 上下游依赖

- **上游**：`python/exporters/generate_submission.py` 在写台账前写审计行。
- **下游**：审计查询，回答「这份文件生成过几版、哈希怎么变的」。

## 质量规则清单

`is_content_change` 与 `previous_hash` 由 CHECK 约束 `ads_fr2052a_submission_audit_change_ck` 保证一致：`is_content_change = (previous_hash IS NULL OR previous_hash <> file_hash)`。
