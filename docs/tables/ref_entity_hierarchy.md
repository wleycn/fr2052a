# 表契约：ref.ref_entity_hierarchy

## 层级

Ref（引用数据，Iceberg `ref` 命名空间）

## 主题

法人实体层级：并表口径与重要子公司。

## 粒度

一行 = 一个实体在一个生效区间内的定义。

## 业务主键

`entity_code`（`effective_date` 起生效，`expiry_date` 为空表示当前有效）。

## 去重方式

由 `python/lakehouse/load_ref_tables.py` 整表覆盖写（`INSERT OVERWRITE`）。字典表量小，重跑任意次结果一致。

## 分区

无。整表重建，读的时候是整表替换，分区不会缩小任何一次读。

## 字段清单

| 字段 | 类型 | 说明 |
|---|---|---|
| `entity_code` | STRING | 法人实体编码 |
| `entity_name` | STRING | 法人实体名称 |
| `lei_code` | STRING | 法人实体 LEI 代码，20 位字母数字 |
| `parent_entity_code` | STRING | 上级法人实体编码，用于合并层级 |
| `entity_level` | INT | 实体层级，1 为最终母公司 |
| `jurisdiction` | STRING | 注册地国家/地区 ISO 3166-1 alpha-2 |
| `entity_type` | STRING | 实体类型：BANK/BROKER/HOLDING/SPV |
| `is_material_entity` | BOOLEAN | 是否为 FR 2052a 重要实体 |
| `consolidation_method` | STRING | 合并方法：FULL/PROPORTIONAL/EQUITY |
| `is_active` | BOOLEAN | 是否有效 |
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

- **上游**：无上游表。种子数据由 `python/generators/ref_data.py` 生成 CSV 后批加载。
- **下游**：报表模型的实体清单与合并口径（`dbt/models/marts/ads_fr2052a_report.sql` 的 `entities` CTE）。

## 质量规则清单

本层不设 VDQ 校验（规则自身的定义就住 `ref.ref_validation_rules`，由 `run_dq_rules.py` 读取后执行）。
