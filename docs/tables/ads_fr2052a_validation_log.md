# 表契约：ads.ads_fr2052a_validation_log

## 层级

控制与审计（PostgreSQL）

## 主题

数据质量结论：一行 = 一个批次里的一条规则检查结果。

## 粒度

一行 = 批次 + 规则。

## 业务主键

`batch_id` + `validation_rule_id`（先清本批次再追加，重跑不翻倍，见 `python/validators/clear_dq_batch.py`）。

## 去重方式

按业务键 upsert；按批次累积的表先清本批次再追加，重跑不翻倍。

## 分区

无。PostgreSQL 表，按环节写入或覆盖；不涉及分区裁剪。

## 字段清单

| 字段 | 类型 | 说明 |
|---|---|---|
| `batch_id` | TEXT NOT NULL | 批次号 |
| `validation_rule_id` | TEXT NOT NULL | 规则编码 |
| `rule_description` | TEXT | 规则说明 |
| `rule_category` | TEXT | 规则类别 |
| `severity` | TEXT | ERROR / WARNING / INFO |
| `check_result` | TEXT | PASS / FAIL / SKIPPED |
| `actual_value` | TEXT | 实际值 |
| `expected_value` | TEXT | 期望值 |
| `detail` | TEXT | 明细说明 |
| `apply_layer` | TEXT | 规则适用层 |
| `affected_line_item` | TEXT | 受影响的报送行项目 |
| `created_at` | TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP |  |

## 金额单位约定

`actual_value` / `expected_value` 为文本，内容随规则而定。

## PII 字段与脱敏方式

无。本表不含个人标识或直接标识。

## 生命周期

PostgreSQL 常驻表，按环节写入或覆盖；无快照与压缩策略。

## 新鲜度 SLA 与 owner

日批 `dq-rules` 环节写入。owner：仓库维护者。

## 上下游依赖

- **上游**：`ref.ref_validation_rules` 与 `python/validators/run_dq_rules.py`。
- **下游**：放行闸、巡检与验收证据。

## 质量规则清单

`check_result` 三态（PASS / WARNING / FAIL）；跨批次保留历史，因此「本批次几条 ERROR」必须按 `batch_id` 过滤后再数。
