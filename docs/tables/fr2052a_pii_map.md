# 表契约：secure.fr2052a_pii_map

## 层级

控制与审计（PostgreSQL）

## 主题

脱敏对照表：token 与明文的对应关系，全项目明文的唯一落点。

## 粒度

一行 = 一个 token 在一张源表的一个列上的对应关系。

## 业务主键

`pii_token` + `source_object` + `pii_column`。

## 去重方式

按业务键 upsert；按批次累积的表先清本批次再追加，重跑不翻倍。

## 分区

无。PostgreSQL 表，按环节写入或覆盖；不涉及分区裁剪。

## 字段清单

| 字段 | 类型 | 说明 |
|---|---|---|
| `pii_token` | TEXT NOT NULL | 脱敏后的确定性 token |
| `pii_plaintext` | TEXT NOT NULL | 明文原值 |
| `source_object` | TEXT NOT NULL | 明文来源，如 bronze.ods_deposits |
| `pii_column` | TEXT NOT NULL | 明文列名，如 customer_id |
| `entity_code` | TEXT | 该记录所属法人实体，便于按实体授权 |
| `loaded_at` | TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP |  |

## 金额单位约定

不适用。本表不含金额列。

## PII 字段与脱敏方式

**本表就是敏感数据本体**：`pii_plaintext` 是明文列。**控制手段**：表建在 `secure` 命名空间，只授给合规员与管理员两个角色，其余角色连接口都看不到；脱敏 token 由 `python/governance/build_pii_vault.py` 用同一份带盐模板生成，保证与 dbt 侧口径一致。

## 生命周期

随跑批按 token 覆盖写；不设自动清理，明文对照需要按保留期人工裁决。

## 新鲜度 SLA 与 owner

日批 `pii-vault` 环节写入。owner：合规员（访问权与保留期由其裁定）。

## 上下游依赖

- **上游**：`bronze.ods_deposits`、`ods_loans` 的明文标识列。
- **下游**：合规调查与监管问答，不参与日常分析链路。

## 质量规则清单

单射校验：同一明文只能映射一个 token，否则按客户维度的聚合会把多个客户塌成一桶（`verify_rbac.py` 与 `build_pii_vault.py` 自检）。
