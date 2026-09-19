# 表契约：ads.ads_restatement_log

## 层级

控制与审计（PostgreSQL）

## 主题

重述登记：原报表与新报表的对应关系，说明为什么重报。

## 粒度

一行 = 一次重述。

## 业务主键

`restatement_id` 主键；`original_report_id` 与 `new_report_id` 互指。

## 去重方式

只追加，不 upsert。每次重述登记一行，重跑会再落一行，不做去重。

## 分区

无。PostgreSQL 表，按环节写入或覆盖；不涉及分区裁剪。

## 字段清单

| 字段 | 类型 | 说明 |
|---|---|---|
| `restatement_id` | BIGSERIAL PRIMARY KEY | 重述主键 |
| `original_report_id` | TEXT | 原报表主键 |
| `new_report_id` | TEXT | 新报表主键 |
| `report_date` | DATE NOT NULL | 报告日 |
| `entity_code` | TEXT NOT NULL | 法人实体编码 |
| `reason` | TEXT | 重述原因 |
| `requested_by` | TEXT | 申请人 |
| `approved_by` | TEXT | 审批人 |
| `status` | TEXT NOT NULL DEFAULT 'PENDING' | PENDING / APPROVED / APPLIED / REJECTED |
| `created_at` | TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP |  |

## 金额单位约定

不适用。本表不含金额列。

## PII 字段与脱敏方式

无。本表不含个人标识或直接标识。

## 生命周期

PostgreSQL 常驻表，按环节写入或覆盖；无快照与压缩策略。

## 新鲜度 SLA 与 owner

由 `python/lakehouse/restate.py` 在 `restate-capture` / `restate-register` 环节写入。owner：仓库维护者。

## 上下游依赖

- **上游**：报表版本历史 `ads.ads_fr2052a_report_history`。
- **下游**：审计与对外解释。

## 质量规则清单

`reason` 与 `requested_by` / `approved_by` 必填：重述是合规事件，无审批人的重述视同未登记。
