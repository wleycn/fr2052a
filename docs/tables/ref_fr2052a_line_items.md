# 表契约：ref.ref_fr2052a_line_items

## 层级

Ref（引用数据，Iceberg `ref` 命名空间）

## 主题

FR 2052a 行项目字典：Section 与行项目的层级、符号约定、是否必备。

## 粒度

一行 = 一个业务主键（本表无有效期列，不做版本化）。

## 业务主键

`line_item_code`。

## 去重方式

由 `python/lakehouse/load_ref_tables.py` 整表覆盖写（`INSERT OVERWRITE`）。字典表量小，重跑任意次结果一致。

## 分区

无。整表重建，读的时候是整表替换，分区不会缩小任何一次读。

## 字段清单

| 字段 | 类型 | 说明 |
|---|---|---|
| `line_item_code` | STRING | FR 2052a 行项目编码 |
| `section_code` | STRING | 所属 Section：A/B/C/D/E/F/G/H/I/J/K |
| `line_description` | STRING | 行项目描述 |
| `parent_line_item` | STRING | 父级行项目编码 |
| `is_calculated` | BOOLEAN | 是否为计算项 |
| `calculation_formula` | STRING | 计算公式 |
| `data_type` | STRING | 数据类型：AMOUNT/COUNT/RATIO |
| `sign_convention` | STRING | 符号约定：POSITIVE/NEGATIVE/EITHER |
| `mandatory_flag` | BOOLEAN | 是否必填 |
| `sort_order` | INT | 排序序号 |

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
- **下游**：报表与明细的 Section 归集；`VDQ-019` 必备行项目判据。

## 质量规则清单

本层不设 VDQ 校验（规则自身的定义就住 `ref.ref_validation_rules`，由 `run_dq_rules.py` 读取后执行）。
