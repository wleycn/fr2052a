# 表契约：ref.ref_validation_rules

## 层级

Ref（引用数据，Iceberg `ref` 命名空间）

## 主题

数据质量规则定义：20 条 VDQ 的表达式、层级、严重度。

## 粒度

一行 = 一个业务主键，或一个主键在有效期内的一个版本。

## 业务主键

`rule_id`。

## 去重方式

由 `python/lakehouse/load_ref_tables.py` 整表覆盖写（`INSERT OVERWRITE`）。字典表量小，重跑任意次结果一致。

## 分区

无。整表重建，读的时候是整表替换，分区不会缩小任何一次读。

## 字段清单

| 字段 | 类型 | 说明 |
|---|---|---|
| `rule_id` | STRING | 校验规则编号 |
| `rule_name` | STRING | 规则名称 |
| `rule_category` | STRING | 规则类别：COMPLETENESS/ACCURACY/CONSISTENCY/TIMELINESS/BUSINESS |
| `apply_layer` | STRING | 适用层级：ODS/OWD/OWS/ADS |
| `severity` | STRING | 严重度：ERROR/WARNING/INFO |
| `sql_expression` | STRING | 校验 SQL 表达式 |
| `description` | STRING | 规则描述 |
| `is_active` | BOOLEAN | 是否启用 |

## 金额单位约定

不适用：本表不含金额列。

## PII 字段与脱敏方式

无。字典表只放编码与名称。

## 生命周期

Iceberg v2 表，快照保留 7 天或至少 10 个（`python/lakehouse/maintain_tables.py`）。小文件合并走 `rewrite_data_files`。不设额外数据保留期：整表重建即最新状态，历史由版本历史表或审计表承担。

## 新鲜度 SLA 与 owner

日批第一步 `ref-load` 由 `python/lakehouse/load_ref_tables.py` 加载，其后整批跑批依赖它。owner：仓库维护者。

## 上下游依赖

- **上游**：无上游表，种子来自 `python/generators/ref_data.py`（与 `[02]模块设计.md` §2.7 的 20 条逐条对应）。
- **下游**：`python/validators/run_dq_rules.py` 读它执行校验，结论落 `ads.ads_fr2052a_validation_log`。

## 质量规则清单

本层不设 VDQ 校验（规则自身的定义就住 `ref.ref_validation_rules`，由 `run_dq_rules.py` 读取后执行）。
