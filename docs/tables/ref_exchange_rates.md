# 表契约：ref.ref_exchange_rates

## 层级

Ref（引用数据，Iceberg `ref` 命名空间）

## 主题

汇率：报告日即期价，用于把所有币种折算成 USD。

## 粒度

一行 = 一个业务主键，或一个主键在有效期内的一个版本。

## 业务主键

`rate_date` + `from_currency` + `to_currency` + `rate_type`。

## 去重方式

由 `python/lakehouse/load_ref_tables.py` 整表覆盖写（`INSERT OVERWRITE`）。字典表量小，重跑任意次结果一致。

## 分区

无。整表重建，读的时候是整表替换，分区不会缩小任何一次读。

## 字段清单

| 字段 | 类型 | 说明 |
|---|---|---|
| `rate_date` | DATE | 汇率日期 |
| `from_currency` | STRING | 源币种 ISO 4217 |
| `to_currency` | STRING | 目标币种 ISO 4217 |
| `spot_rate` | DECIMAL(18,8) | 即期汇率 |
| `rate_type` | STRING | 汇率类型：MID/BID/ASK |
| `rate_source` | STRING | 汇率来源：BLOOMBERG/REUTERS/CENTRAL_BANK |

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
- **下游**：`silver.stg_fx_rates`，再经它进入各 `owd_*` 模型的折算。

## 质量规则清单

本层不设 VDQ 校验（规则自身的定义就住 `ref.ref_validation_rules`，由 `run_dq_rules.py` 读取后执行）。
