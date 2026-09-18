# 表契约：ref.ref_behavior_assumptions

## 层级

Ref（引用数据，Iceberg `ref` 命名空间）

## 主题

行为假设：按产品、客群与期限桶给出流出率与流入率。

## 粒度

一行 = 一个业务主键，或一个主键在有效期内的一个版本。

## 业务主键

`product_category` + `customer_segment` + `maturity_bucket`。

## 去重方式

由 `python/lakehouse/load_ref_tables.py` 整表覆盖写（`INSERT OVERWRITE`）。字典表量小，重跑任意次结果一致。

## 分区

无。整表重建，读的时候是整表替换，分区不会缩小任何一次读。

## 字段清单

| 字段 | 类型 | 说明 |
|---|---|---|
| `product_category` | STRING | 产品类别 |
| `customer_segment` | STRING | 客户细分 |
| `maturity_bucket` | STRING | 到期分桶 |
| `runoff_rate` | DECIMAL(8,4) | 流失率 |
| `inflow_rate` | DECIMAL(8,4) | 流入率 |
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
- **下游**：现金流量预测 `ows_cashflow_projection` 与 LCR 计算的流出侧。

## 质量规则清单

本层不设 VDQ 校验（规则自身的定义就住 `ref.ref_validation_rules`，由 `run_dq_rules.py` 读取后执行）。
