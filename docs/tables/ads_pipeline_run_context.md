# 表契约：ads.ads_pipeline_run_context

## 层级

控制与审计（PostgreSQL）

## 主题

运行上下文：一次跑批的日期与状态，所有环节与放行闸的日期单源。

## 粒度

一行 = 一次跑批（一个 batch_id）。

## 业务主键

`batch_id`。

## 去重方式

按 `batch_id` upsert。开口写一行 `status=RUNNING`，收口更新 `status` 与 `finished_at`。同一批次重跑会覆盖上下文行。

## 分区

无。PostgreSQL 表，按环节写入或覆盖；不涉及分区裁剪。

## 字段清单

| 字段 | 类型 | 说明 |
|---|---|---|
| `batch_id` | TEXT PRIMARY KEY | 批次号，一次跑批的唯一标识 |
| `report_date` | DATE NOT NULL | 报告日（= 业务日期），本次跑批处理哪一天的数据 |
| `processing_date` | DATE NOT NULL | 处理日，跑批实际执行的日期 |
| `effective_date` | DATE NOT NULL | 生效日，数据版本从哪天开始生效 |
| `run_type` | TEXT NOT NULL | DAILY = 日批 DAG 拉起；MANUAL = 人工整链跑 |
| `status` | TEXT NOT NULL | RUNNING 未收口 / SUCCEEDED / FAILED |
| `started_at` | TIMESTAMP NOT NULL | 开口时刻 |
| `finished_at` | TIMESTAMP | 收口时刻，未收口为 NULL |

## 金额单位约定

不适用。本表不含金额列。

## PII 字段与脱敏方式

无。本表不含个人标识或直接标识。

## 生命周期

PostgreSQL 常驻表，按批次追加，跨批次保留历史。复位脚本清空。无快照与压缩策略。

## 新鲜度 SLA 与 owner

日批 `run-context-open` 环节在链首写入，`run-context-close` 在链尾更新。owner：仓库维护者。

## 上下游依赖

- **上游**：`python/governance/run_context.py`（开口与收口）。
- **下游**：放行闸 `check_submission_gate.py` 读最近一行的 `status` 判断本报告日是否跑成功。

## 质量规则清单

日期链 CHECK 把当前口径编码进 schema：`processing_date = report_date + 1` 且 `effective_date = processing_date`。口径变了就必须改约束，是一次看得见的动作而不是口头约定。`run_type` 只允许 `DAILY` / `MANUAL`；`status` 只允许 `RUNNING` / `SUCCEEDED` / `FAILED`。
