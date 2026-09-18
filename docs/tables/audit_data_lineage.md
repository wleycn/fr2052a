# 表契约：audit.audit_data_lineage

## 层级

控制与审计（PostgreSQL）

## 主题

血缘：表级与列级的上下游边，含转换类型与所属作业。

## 粒度

一行 = 一条血缘边。

## 业务主键

`lineage_id` 主键；唯一键 `source_object` + `target_object` + `transform_type` + `job_name`。

## 去重方式

按业务键 upsert；按批次累积的表先清本批次再追加，重跑不翻倍。

## 分区

无。PostgreSQL 表，按环节写入或覆盖；不涉及分区裁剪。

## 字段清单

| 字段 | 类型 | 说明 |
|---|---|---|
| `lineage_id` | BIGSERIAL PRIMARY KEY | 血缘主键 |
| `source_object` | TEXT NOT NULL | 源对象 |
| `target_object` | TEXT NOT NULL | 目标对象 |
| `transform_type` | TEXT | 转换类型 |
| `job_name` | TEXT | 作业名称 |
| `run_id` | TEXT | 运行编号 |
| `created_at` | TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP |  |

## 金额单位约定

不适用。本表不含金额列。

## PII 字段与脱敏方式

无。记录的是对象名与列名。

## 生命周期

PostgreSQL 常驻表，按环节写入或覆盖；无快照与压缩策略。

## 新鲜度 SLA 与 owner

日批 `lineage` 环节由 `python/governance/render_lineage.py` 写入。owner：仓库维护者。

## 上下游依赖

- **上游**：dbt 模型 meta、`ref.ref_regulatory_mapping`。
- **下游**：业务文档 §4 血缘图与监管问答。

## 质量规则清单

列级映射来自 `ref.ref_regulatory_mapping`，表级边来自 dbt 模型依赖；当前落 48 条表级边与 20 条列级监管映射。
