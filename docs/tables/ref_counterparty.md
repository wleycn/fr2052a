# 表契约：ref.ref_counterparty

## 层级

Ref（引用数据，Iceberg `ref` 命名空间）

## 主题

交易对手主数据：类型、国别、评级、行业。

## 粒度

一行 = 一个业务主键（本表无有效期列，不做版本化）。

## 业务主键

`counterparty_id`。

## 去重方式

由 `python/lakehouse/load_ref_tables.py` 整表覆盖写（`INSERT OVERWRITE`）。字典表量小，重跑任意次结果一致。

## 分区

无。整表重建，读的时候是整表替换，分区不会缩小任何一次读。

## 字段清单

| 字段 | 类型 | 说明 |
|---|---|---|
| `counterparty_id` | STRING | 交易对手编号 |
| `counterparty_name` | STRING | 交易对手名称 |
| `lei_code` | STRING | LEI 代码 |
| `counterparty_type` | STRING | 交易对手类型：BANK/BROKER/CORPORATE/SOVEREIGN/CENTRAL_BANK/AFFILIATE |
| `country_code` | STRING | 国家/地区代码 |
| `credit_rating` | STRING | 信用评级 |
| `industry_code` | STRING | 行业代码 |

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
- **下游**：`owd_derivatives`、`owd_secured_financing`、`owd_off_bs` 的对手方属性补齐。

## 质量规则清单

本层不设 VDQ 校验（规则自身的定义就住 `ref.ref_validation_rules`，由 `run_dq_rules.py` 读取后执行）。
