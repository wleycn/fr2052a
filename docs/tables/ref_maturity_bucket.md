# 表契约：ref.ref_maturity_bucket

## 层级

Ref（引用数据，Iceberg `ref` 命名空间）

## 主题

到期分桶定义：把剩余期限映射成监管口径的桶。

## 粒度

一行 = 一个业务主键（本表无有效期列，不做版本化）。

## 业务主键

`bucket_code`。

## 去重方式

由 `python/lakehouse/load_ref_tables.py` 整表覆盖写（`INSERT OVERWRITE`）。字典表量小，重跑任意次结果一致。

## 分区

无。整表重建，读的时候是整表替换，分区不会缩小任何一次读。

## 字段清单

| 字段 | 类型 | 说明 |
|---|---|---|
| `bucket_code` | STRING | 分桶编码：O/N、1-7D、8-30D 等 |
| `bucket_description` | STRING | 分桶描述 |
| `min_days` | INT | 最小剩余天数（含） |
| `max_days` | INT | 最大剩余天数（含） |
| `sort_order` | INT | 排序序号 |
| `fr2052a_display_order` | INT | FR 2052a 展示顺序 |

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
- **下游**：`maturity_bucket` 宏，被各 `owd_*` 模型调用。

## 质量规则清单

本层不设 VDQ 校验（规则自身的定义就住 `ref.ref_validation_rules`，由 `run_dq_rules.py` 读取后执行）。
