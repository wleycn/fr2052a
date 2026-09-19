# AGENTS.md — AI 编码约束（数据处理类项目模板）

> 规则分两级。🔴 红线：AI 不得生成违反它的代码，评审直接打回。🟡 建议：偏离时要在回报或 `CHANGELOG` 里说明理由。
> 在本仓库工作的 AI 编码工具和 agent 运行时，都受本文件约束。
> 这包括 Hermes Agent 自己，以及它派出的 coder profile、委托子代理和 cron 任务。
> Claude Code、Codex、Cursor 及其它 CLI / IDE 助手同样适用。
> 判断标准只有一条：**以本仓库为 cwd 即受约束**。没被上面点名的工具，不因此不受约束。Hermes 侧的生效路径见 §8。

## 0. 规范优先级

本文件效力最高。往下依次是 `docs/rules/` 四件套、`docs/business/` 业务文档、既有代码风格、通用业界实践。几层冲突时取效力高的那层。

发现既有代码与高层规则冲突时，**不得默然跟随既有代码**。要在变更记录里指出冲突。

## 1. 技术栈

- 语言与运行环境：Python 3.11（uv venv）、SQL
- 存储引擎：PostgreSQL 18.6（ADS/审计）、MinIO（Iceberg 对象存储）
- 计算引擎：Apache Spark 3.5.9（Standalone，容器内 JDK 17）
- 消息队列：Apache Kafka 4.3.1（KRaft 单节点）
- 调度编排：Apache Airflow 2.10.5（LocalExecutor）
- 转换：dbt-postgres + dbt-spark[PySpark]（dbt 1.12.5）
- 数据质量：自研规则引擎，规则落 `ref.ref_validation_rules`，由 `run_dq_rules.py` 执行
- 元数据治理：dbt `schema.yml` 的 meta 声明 + `render_lineage.py` 渲染血缘
- 开放表格式：Apache Iceberg 1.11（JDBC catalog：元数据在 PG，数据在 MinIO）
- 巡检：`pipeline_health.py` 检查熔断、校验、预警、报送、重述、实时事件、PG 连接、Kafka 滞后与磁盘（8 项）

## 2. 上下文加载（动手前必做）

按顺序读完再动手：

1. 读本文 §3 红线清单
2. 读 `docs/rules/PROJECT-STRUCTURE.md` 的「目录职责」与「收口点」
3. 读 `docs/rules/CODING-STANDARD.md` 的本类型红线
4. 涉及数据变更 → 读 `docs/business/DATA-DESIGN.md` 的表契约（分层表清单、字段语义、口径）
5. **合计不超过 3 个规则文件**（防上下文过载）

## 3. 红线清单

> AI 不得输出违反以下内容的代码；违反即阻断，不接受「先合入后续再改」。

1. 🔴 **写入必须幂等**（MERGE 或分区级 overwrite），**禁止裸 append**
2. 🔴 **查询必须带分区裁剪**。禁止全表扫描，禁止把全量数据拉到驱动端
3. 🔴 **金额禁止用 `float`**。单位换算只在接入边界做一次
4. 🔴 **PII 在明细层必须脱敏**。禁止明文外泄到日志或对外表
5. 🔴 **凭证、endpoint、路径不得硬编码**，统一走配置收口
6. 🔴 **计算与 catalog 走收口文件**。禁止散建会话实例
7. 🔴 **破坏性操作必须有人类显式授权**，包括 DROP、DELETE、snapshots expire
8. 🔴 **schema evolution 只走迁移**。禁止隐式加列
9. 🔴 **每张表必须有快照保留与压缩策略**。禁止无限保留
10. 🔴 **分区参数由调度注入**。禁止代码读系统当前时间

## 4. Agent 行为准则

- **不做「顺手改进」**：只改授权范围内的文件。发现相邻问题另行提出，不代改。
- **失败必留痕**：异常要记日志。禁止捕获后空处理。
- **不确定就停**：想不清接口形状、目录结构、改动该落在哪个文件时，先提问。
- **不擅自执行破坏性操作**：DROP、DELETE、批量覆盖、快照过期，一律先问。
- 🔴 单人加 agent 的项目**直接提交到 `main`**，不开 feature 分支、不提 MR：没有第三方评审人时，分支与 MR 只增加动作。出问题靠 `git revert` 回退，不改写已推送的历史。此条是已登记的规范偏离，见 `docs/business/KNOWN-ISSUE.md` 的「直接在 main 提交」行。

## 5. 输出要求

- 只给代码或明确的 diff，不夹带无关说明
- **新建或重写**的代码文件，头部加 `[AI-GENERATED] model=<m> date=<d> reviewed_by=<human>` 注释。两类文件豁免：装配工具生成的产物、以及文档 —— 它们的来源由下面的变更留痕承担。存量未标记的文件按一次性回填批补齐（字段怎么填见技能 `rule-mechanization` 的案例文件）。
- `reviewed_by=pending` 表示还没有人 review 过；review 完成后由 review 者替换成自己的名字。
- commit message 写清「改了什么 + 为什么」，**不区分提交者**：agent 是代用户执行，它提交的即用户提交，不加 `[AI]` 一类标记。头注缺失与项目地图里的悬空引用由 `.githooks/pre-commit` 调用的共享门禁检查（仅 agent 会话生效；头注一项是告警级，存量已补齐）。
- 单次变更不超过文件总量的 **40%**。阈值**只对改动前达到 200 行的文件**适用，不足 200 行不受限。超出就拆成多次，逐步验证。
- 注释与 docstring 要和代码在同一个提交里改。不许留下与实现不符的注释。注释解释「**为什么**」，不复述「是什么」。细则见 `docs/rules/CODING-STANDARD.md` 的注释一节。
- 写文档、写回报、写交付说明之前，先载入技能 `docs-writing-discipline`，按其检查表通读一遍再交
- 迁移脚本必须由人类逐行 review 并在 PR 中 comment 确认

## 6. 变更留痕

功能或契约变更，在 `docs/changes/{module}.md` **追加**一条目。条目格式照 `docs/changes/` 下已有条目，四段为范围 / 变更 / 验证 / 回滚；首个条目见 `docs/changes/engineering.md`。`{module}` 取 `python/` 顶层模块目录名；非功能变更落 `engineering.md`。该目录**只放条目文件**，不放 README、说明或附件。模块清单见 §9 项目地图。

- 上线后追加部署记录。

## 7. 不确定行为

- 规范没覆盖但改动可以安全回退时，照本仓库最相近的类比规则执行，并在变更记录里说明类比的是哪一条。
- 下面四类必须向人类确认，不得自决：金额怎么算、去重怎么做、回刷范围、破坏性操作。
- 不许自行引入新的第三方依赖。要引入就写进依赖清单，并走评审。
- 规范没覆盖或互相冲突时：**停止 → 提问 → 等确认**。不许自行放宽红线，也不许「先合入，之后再改」。

## 8. 本文件如何被读取（生效路径）

- 生效方式：工具按 **cwd → git 根** 的目录链在**会话启动**时注入本文件。它不是「放进项目就自动生效」。
- 会话、委托、测试 harness 都必须**以项目根为 cwd 启动**。从项目外启动只会晚一步懒加载，在那之前动作等于没有约束。
- **三种情况本文件不生效**：非 git 项目从子目录启动、`delegate_task` 子代理、未设 `workdir` 的 cron 任务。
- **Hermes 侧保证**：Hermes 及其委托链路一律**以项目根为 cwd 启动**。委托链路包括 coder profile、`delegate_task` 子代理和 cron 任务。委托任务书必须写明 `cwd=/home/hermes/workspace/demo-fr2052a`。**不得假设执行者已读过本文件**，拿不准时把 §3 红线**内联**进任务书。
- 红线要**可执行化**：能写成 lint、检查脚本或 CI check，就不要指望「模型会读到」。

## 9. 项目地图（文件索引）

> 回答「东西在哪」。
> 生成规则：把**项目里真实存在**的文件填进下表，删掉不适用的行。表内路径必须真实可访问。被引用文件的存在性检查尚未实现，已登记在 `KNOWN-ISSUE.md`；当前靠评审核对。目录路径与 `config/` 下的条目要由审查者核实。

| 类别 | 位置 | 用途 |
|---|---|---|
| 人类入口 | `README.md` | 这是什么 / 怎么上手（5 秒测试） |
| AI 约束 | `AGENTS.md`（本文） | 红线与行为准则，优先级最高 |
| 规范 | `docs/rules/` | 四件套：结构 / 编码 / 流程 / 验收 |
| 表契约 | `docs/tables/` | 一表一份：层级 / 主题 / 粒度 / 主键 / 去重 / 分区 / 金额口径 / PII 与脱敏 / 生命周期 / 新鲜度与 owner / 依赖 / 质量规则 |
| 业务文档 | `docs/business/` | 项目说明 / 模块 / 数据 / 接口 / 术语 / 变更 / 已知问题 |
| 偏离登记处 | `docs/business/KNOWN-ISSUE.md` | 已知坑 / 设计决策 / 与上游规范不一致处的逐条登记（禁止无登记降标准） |
| 变更留痕 | `docs/changes/{module}.md` | 功能与契约变更的按模块条目（范围 / 变更 / 验证 / 回滚）；E0–E7 的构建过程另见 `docs/build-log.md` |
| 交接单 | `todo/` | 会话接力：一份一次交接，`<YYYYMMDD>-<主题>.<状态>`，状态迁移用 `git mv` 改名，`.done` 留档不删 |
| 部署清单 | `deploy/` | Server 1/2 部署脚本与配置 |
| SQL DDL | `sql/iceberg/` | Iceberg 表定义（真源） |
| dbt 项目 | `dbt/` | 模型 / 宏 / 配置 |
| Python 脚本 | `python/` | 生成器 / Lakehouse / Producer / Consumer / Exporter / Validator |
| 配置契约 | `config/pipeline_topics.json` | Kafka Topic 配置 |
| 样本数据 | `sample_data/` | 可重建测试数据（不落 git） |
| 原始需求归档 | `references/` | 项目启动时的需求原件（内容已拆解进 `docs/business/`） |

## 10. 技能地图（阶段 → 技能）

> 回答「这个阶段该用哪个技能」。
> 阶段定义见 `docs/rules/DEVELOP-FLOW.md` §1（十阶段）与 §1.1（阶段 4 子步骤）。下表只接**技能库里真实存在**的技能，名字必须可查；当前由 `make lint`（本地钩子与 CI 都跑它）覆盖代码侧检查。
> 用法：进入某阶段前先载入该阶段技能（`skill_view`），按它的 Phase 或 Step 执行。禁用「通用做法」替代技能流程。

| 阶段 | 技能 | 什么时候用 |
|---|---|---|
| 1 需求分析 | `requirement-analysis`；存量改造先 `legacy-recon`；可行性未知 → `feasibility-probe` | 诉求模糊 / 接手陌生仓库 / 方案 A·B 选型 |
| 2 方案设计 | `system-architecture`（架构与 ADR）、`project-doc-system`（文档体系）、`api-contract-design`、`data-layer-design`、`identity-access-design`、`secrets-management`、`threat-modeling`、`ui-foundation-design` | 立架构 / 定文档 / 改契约 |
| 3 任务拆分 | 无专用技能：按 `coding-flow` 七步执行序拆；委托子代理用 `coder-profile-delegation` | 拆到「能独立验证」的粒度 |
| 4 编码实现 | `coding-flow`（流程）、`minimal-diff`（改哪几行）、`engineering-naming-discipline`（命名）；MCP / 工具服务 → `mcp-server-development` | 每次动代码前必载 |
| 4a–4d 编码子步骤 | 同上；4b 接口实现另载 `api-contract-design`；4d 接线自测另载 `http-e2e-testing` | 4a→4b→4c→4d 逐步推进，**不得并行大改** |
| 5 单元测试 | `tdd-discipline`（RED-GREEN-REFACTOR）；卡住 → `python-debugging` / `node-debugging` | 写生产代码前先有失败测试 |
| 6 代码评审 | `pre-commit-gate`（提交闸）、`independent-review`（第三方判定）、`ai-code-audit`（AI 生成代码安全） | 每次提交 / 交付前 |
| 7 集成测试 | `http-e2e-testing`（HTTP 端到端）；接口级 `api-testing`；数据层交接 `verify-data-layer` | 有接口或数据链路时 |
| 8 预发验证 | `exploratory-qa`（像真实用户挑刺）、`persona-walkthrough`（界面走查）、`ui-finish-gate`（UI 收口）、`accessibility-audit` | 面向用户的功能 / 界面 |
| 9 上线部署 | `ci-cd-delivery`；提交纪律 `git-submit` | 有流水线 / 多环境时 |
| 10 线上观测 | `observability-sre`（SLO / 告警）、`incident-postmortem`（故障复盘）、`performance-benchmarking` | 有生产环境时 |
| 横切（任意阶段） | `doc-code-drift`（契约与代码漂移）、`post-change-cleanup`（改后清理）、`module-retirement`（下线旧模块）、`docs-writing-discipline`（写文档与回报前）；日常纪律 `ops-basics-discipline` / `path-ssot-governance` / `secret-sprawl-audit` | 阶段完成 / 交付前 / 发现漂移时 |
| 任意阶段（本类型专属） | `data-layer-design`（schema 与查询计划）；`verify-data-layer`（数据层交接独立验证）；`pg-query`；`performance-benchmarking` | 涉及表 / 分区 / 数据链路时 |
