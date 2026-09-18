# 表契约：audit.audit_access_log

## 层级

控制与审计（PostgreSQL）

## 主题

访问审计：谁以哪个角色读了哪个对象。

## 粒度

一行 = 一次访问。

## 业务主键

`access_id`。

## 去重方式

按业务键 upsert；按批次累积的表先清本批次再追加，重跑不翻倍。

## 分区

无。PostgreSQL 表，按环节写入或覆盖；不涉及分区裁剪。

## 字段清单

| 字段 | 类型 | 说明 |
|---|---|---|
| `access_id` | BIGSERIAL PRIMARY KEY | 访问日志主键 |
| `user_name` | TEXT | 数据库角色名 |
| `role_name` | TEXT | 角色 |
| `object_name` | TEXT | 被访问对象 |
| `action` | TEXT | 操作 |
| `access_time` | TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP |  |
| `client_ip` | TEXT | 客户端 IP |

## 金额单位约定

不适用。本表不含金额列。

## PII 字段与脱敏方式

无。记录的是访问者与对象名，不含被访问数据本身。

## 生命周期

PostgreSQL 常驻表，按环节写入或覆盖；无快照与压缩策略。

## 新鲜度 SLA 与 owner

随访问累积。owner：仓库维护者。

## 上下游依赖

- **上游**：各角色的查询与脚本调用。
- **下游**：合规复核与权限评审。

## 质量规则清单

与 `python/governance/verify_rbac.py` 的权限矩阵核对：4 个角色 × 3 类对象的授权结果应当与台账一致。
