# DEVELOP-FLOW.md — 开发流程

> 本文件定义项目的开发流程。所有开发者必须遵守。

## 阶段定义

| 阶段 | 名称 | 输入 | 输出 | 验收标准 |
|------|------|------|------|----------|
| 1 | 需求分析 | 业务诉求 | 需求文档 | 可验收的规格说明 |
| 2 | 方案设计 | 需求文档 | 设计文档 | 架构决策记录（ADR）|
| 3 | 任务拆分 | 设计文档 | 任务列表 | 可独立验证的子任务 |
| 4 | 编码实现 | 任务列表 | 代码 | `make lint` 无错误、数据层核对脚本通过 |
| 5 | 单元测试 | 代码 | 测试报告 | 覆盖率 ≥ 80% |
| 6 | 代码评审 | 代码 + 测试 | 评审意见 | 至少 1 人 approve |
| 7 | 集成测试 | 合并代码 | 测试报告 | 端到端测试通过 |
| 8 | 预发验证 | 集成测试通过 | 验收报告 | UAT 签字确认 |
| 9 | 上线部署 | 验收报告 | 生产环境 | 监控正常、无告警 |
| 10 | 线上观测 | 生产环境 | 观测报告 | SLO 达标 |

本项目的取舍，其余阶段照常：

| 阶段 | 本项目做法 |
|------|-----------|
| 5 单元测试 | 未建单元测试套件。判据改为数据层核对脚本全绿：bronze、silver、scd2、ads、rbac 五个核对环节都在跑批序列里，见 ACCEPTANCE-CHECKLIST |
| 6 代码评审 | 无第二人时由 `make lint` 与核对脚本兜底；交付前的第三方判定仍走 `independent-review` |
| 8 预发验证 | 无预发环境，以 `bash deploy/reset-demo.sh --apply` 重建后全链路重跑作为验收证据 |

## 阶段 4 子步骤

| 子步骤 | 名称 | 内容 |
|--------|------|------|
| 4a | 接口实现 | 先写接口契约，再写实现 |
| 4b | 数据模型 | 先写 DDL，再写迁移脚本 |
| 4c | 业务逻辑 | 先写测试，再写实现 |
| 4d | 接线自测 | 单元测试 + 集成测试 |

## 质量检查点

### §5.3 基线检查

| 检查项 | 命令 | 频率 |
|--------|------|------|
| Lint + 格式 + 类型 | `make lint` | 每次提交前，由 `.githooks/pre-commit` 自动跑 |
| 数据层核对 | `bash deploy/server2/run-daily-pipeline.sh verify-bronze verify-silver verify-scd2 verify-ads verify-rbac` | 每次跑批后 |
| dbt 模型 | `bash deploy/server2/run-daily-pipeline.sh dbt-run` | 每次模型变更后 |
| 端到端重跑 | `bash deploy/reset-demo.sh --apply` | 交付前 |
| 部署无漂移 | `bash deploy/sync-deploy.sh --check` | 交付前 |

以上命令都在 Server 2 的 `~/fr2052a-infra` 下执行，除 `make lint` 在 dev 机执行。

### §5.4 阶段落地

| 阶段 | 落地命令或产物 |
|------|----------------|
| 1 | `requirements/` 下的需求文档，汇总进 `docs/business/PROJECT.md` |
| 2 | `docs/business/DATA-DESIGN.md`、`MODULE-DESIGN.md`、`INTERFACE-DESIGN.md` |
| 4 | 改完先 `make lint`；提交经 `.githooks/pre-commit` |
| 5 | `bash deploy/server2/run-daily-pipeline.sh verify-bronze verify-silver verify-scd2 verify-ads verify-rbac` |
| 7 | `bash deploy/reset-demo.sh --apply` 整条链路重跑 |
| 9 | `bash deploy/sync-deploy.sh` 把脚本同步到两台服务器 |

## 变更流程

1. 创建 feature 分支
2. 实现功能 + 写测试
3. 本地 lint + test 通过
4. 提交并推送
5. 创建 MR
6. 等待 CI 通过
7. 人工评审
8. 合并到 main

## 回滚策略

- Git  revert：最近 3 次提交可 revert
- 数据库：保留 schema 迁移历史，支持反向迁移
- 配置：版本化管理，支持回退到历史版本
