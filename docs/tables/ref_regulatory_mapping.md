# 表契约：ref.ref_regulatory_mapping

## 层级

Ref（引用数据，Iceberg `ref` 命名空间）

## 主题

监管映射：字段 → Section / 行项目，含监管条文出处与折扣率。

## 粒度

一行 = 一个业务主键，或一个主键在有效期内的一个版本。

## 业务主键

`section_code` + `line_item_code` + `field_name`。

## 去重方式

由 `python/lakehouse/load_ref_tables.py` 整表覆盖写（`INSERT OVERWRITE`）。字典表量小，重跑任意次结果一致。

## 分区

无。整表重建，读的时候是整表替换，分区不会缩小任何一次读。

## 字段清单

| 字段 | 类型 | 说明 |
|---|---|---|
| `section_code` | STRING | FR 2052a Section |
| `line_item_code` | STRING | FR 2052a 行项目编码 |
| `field_name` | STRING | 字段名 |
| `regulatory_reference` | STRING | 监管规则出处 |
| `rule_id` | STRING | 规则编号 |
| `haircut_rate` | DECIMAL(8,4) | 折扣率 |
| `owner` | STRING | 负责人或团队 |
| `effective_date` | DATE | 生效日期 |
| `expiry_date` | DATE | 失效日期 |

## 金额单位约定

不适用：本表不含金额列。

## PII 字段与脱敏方式

无。字典表只放编码与名称。

## 生命周期

Iceberg v2 表，快照保留 7 天或至少 10 个（`python/lakehouse/maintain_tables.py`）。小文件合并走 `rewrite_data_files`。不设额外数据保留期：整表重建即最新状态，历史由版本历史表或审计表承担。

## 新鲜度 SLA 与 owner

日批第一步 `ref-load` 由 `python/lakehouse/load_ref_tables.py` 加载，其后整批跑批依赖它。owner：仓库维护者。

## 上下游依赖

- **上游**：无上游表，种子来自 `python/generators/ref_data.py`。
- **下游**：`python/governance/render_lineage.py` 的列级监管映射输出，落 `audit.audit_data_lineage`。

## 质量规则清单

本层不设 VDQ 校验（规则自身的定义就住 `ref.ref_validation_rules`，由 `run_dq_rules.py` 读取后执行）。
