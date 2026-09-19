# 表契约：audit.audit_change_log

## 层级

控制与审计（PostgreSQL）

## 主题

变更审计：谁在什么时候改了哪条记录，改前改后各是什么。

## 粒度

一行 = 一次记录级变更。

## 业务主键

`change_id`。

## 去重方式

只追加，不做 upsert，重跑不去重。唯一写入方是 `secure.fr2052a_pii_map` 的变更触发器（`sql/postgres/20_security.sql` 的 `secure.log_pii_map_change()`），每次变更追加一行。

## 分区

无。PostgreSQL 表，按环节写入或覆盖；不涉及分区裁剪。

## 字段清单

| 字段 | 类型 | 说明 |
|---|---|---|
| `change_id` | BIGSERIAL PRIMARY KEY | 变更日志主键 |
| `table_name` | TEXT NOT NULL | 被变更的表 |
| `record_key` | TEXT | 被变更记录的键 |
| `change_type` | TEXT NOT NULL | INSERT / UPDATE / DELETE |
| `old_value` | TEXT | 变更前整行 |
| `new_value` | TEXT | 变更后整行 |
| `changed_by` | TEXT | 修改人 |
| `change_time` | TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP |  |
| `reason` | TEXT | 修改原因 |

## 金额单位约定

不适用。本表不含金额列。

## PII 字段与脱敏方式

`old_value` / `new_value` 按设计只记非标识字段；若某次变更涉及标识，明文仍只住 `secure.fr2052a_pii_map`。

## 生命周期

PostgreSQL 常驻表，按环节写入或覆盖；无快照与压缩策略。

## 新鲜度 SLA 与 owner

由触发器与各环节脚本写入，随时间累积。owner：仓库维护者。

## 上下游依赖

- **上游**：`secure.fr2052a_pii_map` 的变更触发器。当前只有这一路写入，尚无环节脚本写入。
- **下游**：审计与合规复核。

## 质量规则清单

只追加不原地覆盖：审计表的语义是历史，覆盖写等于销毁证据。
