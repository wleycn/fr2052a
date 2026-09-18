# 已知坑与决策

> **纪律**：
> 1. 锚点名用 `#kebab-case`，**一经写入不改拼写**（PROJECT 索引行逐字引用）
> 2. 每个锚点必须在 PROJECT.md 已知坑区有索引行（防孤儿锚点）
> 3. 决策型条目（废弃方案/改设计）也记录，标 ✅ 决策，避免重复讨论
> 4. 修复后回填"修复"列；待定标 ⏳ 待决策

| 锚点 | 日期 | 现象 | 根因 | 修复 | 影响范围 | 关联文档 |
|------|------|------|------|------|----------|----------|
| #pg18-data-dir-change | 09-16 | PG 18 启动失败，日志报 `there appears to be PostgreSQL data in /var/lib/postgresql/data (unused mount/volume)` | `postgres:18-alpine` 的 `PGDATA` 变为 `/var/lib/postgresql/18/docker`，镜像的 `VOLUME` 声明在 `/var/lib/postgresql`。按 17 及以前习惯把卷挂在 `/var/lib/postgresql/data` 会被判定为废弃挂载并拒绝启动 | ✅ 修法：卷改挂 `/var/lib/postgresql`。重建数据卷时必须沿用此路径 | Server 1 部署 | BUILD-LOG E1 |
| #spark-minio-endpoint | 09-16 | Spark 连接 MinIO 失败，报 connection refused | Spark 中必须用 `192.168.17.22:9000`，不能用 `localhost`（跨容器网络）| ✅ 修正 Spark catalog config 中的 endpoint | Server 2 Spark 配置 | BUILD-LOG E3 |
| #gl-reconciliation-mismatch | 09-16 | GL 对账 8 项全 FAIL | ① 按单科目比 Section 合计（应汇总后再比）② 按实体匹配集团口径（应去掉实体匹配）③ 总账原本独立随机数，与业务明细无关 | ✅ 修法：按 Section 汇总总账后再比；总账由业务明细倒推（资产科目取自各业务表，权益作轧差项） | GL 对账模块 | BUILD-LOG E4.1 |
| #hqla-cap-not-applied | 09-16 | 报表未应用 HQLA 二级资产 40% 上限，二级资产占比 83.66% | 原样报出，未做监管计算截断 | ✅ 补 `sec_g_hqla_capped_total_usd` = 一级全额 + 二级按 40% 截断 | ADS 报表计算 | BUILD-LOG E4.1 |
| #python314-incompatible | 09-16 | Python 3.14.4 装不上 Great Expectations 与 pyspark | GE 要求 `>=3.10,<3.14`；pyspark 3.5.0 不支持 3.14 | ✅ 在服务器上用 uv 装独立 Python 3.12 venv | Server 2 环境 | BUILD-LOG E0 |
| #dockerhub-image-removed | 09-16 | `minio/minio` 与 `bitnami/spark` 从 Docker Hub 下架 | 镜像源失效 | ✅ MinIO 改走 `quay.io/minio/minio`；Spark 改用官方 `apache/spark` | Server 1/2 部署 | BUILD-LOG E0 |
| #detail-report-mismatch | 09-16 | Section B 明细与报表口径不一致，差 25 亿；Section F 明细 13 亿 vs 报表 0 | 明细把正回购与逆回购混在一起，报表只算正回购；明细没按 30 天过滤 | ✅ 明细按 `line_item` 拆开，30 天过滤下沉到明细 | OWD/OWS 模型 | BUILD-LOG E4.1 |
| #delegate-audit-20260917 | 09-17 | 三路独立审查（代码 / dbt 与 SQL / 文档）共提出 80 条发现，其中一条是三路都报同一条（DAG 授权晚于导出） | 长期单方视角审查，缺口集中在「规则声称的机制没落地」「文档抄自设计稿而非实现」两类 | ✅ 已修 29 条（含 DAG 顺序、恒成功的源文件检查、宏表与 CLI 契约表、README 跑不通的命令）；剩 45 条涉及口径与设计取舍，未动 | 全项目 | `docs/AUDIT-2026-09-17-delegate-review.md` |
| #scd2-reversed-interval | 09-17 | 版本历史表出现「失效日早于生效日」的反向区间（owd_deposits 1 行、owd_gl_entries 20 行） | 写失效日时直接取「本次生效日 - 1」，未与该版本自己的生效日比较。两个调用方的生效日约定一旦不一致（重述用处理日 2026-09-17、日批用报告日 2026-09-16），后跑的那次必然算出反向区间 | ✅ 修法：写入侧用 `greatest(生效日 - 1, 该版本生效日)` 兜底并写完自检不变式；日批生效日改为报告日次日；`verify-scd2` 纳入日批环节；存量脏行由 `sql/iceberg/oneoff/07_fix_reversed_intervals.sql` 修 | SCD2 版本历史 | BUILD-LOG E6.2 |
| #dead-ows-tables | 09-18 | 三张 OWS 汇总表（`ows_hqla_summary`、`ows_collateral_summary`、`ows_funding_summary`）在全仓无任何下游消费者，却随每次 dbt 全量物化 | 血缘图与表契约原先声称它们喂报表，实际报表直接读 OWD 明细 —— 文档抄的是设计稿，不是实现 | ✅ 已把文档与血缘改成与实现一致并登记待下线。保留原因：`migration-notes` 记的汇总层设计占位，dbt 按工程整体物化，删掉要同时改模型与建表脚本；下线条件 = 确认无人接入后删模型、建表脚本与表契约三处 | OWS 汇总层 | `docs/AUDIT-2026-09-17-delegate-review.md` 第 17 条 |
| #report-history-reset-exception | 09-18 | `reset_demo.sql` 原先会 `TRUNCATE TABLE ads.ads_fr2052a_report_history`，与该表「历史不可变」的硬性质冲突 | 复位脚本把版本历史表与可变派生表混在同一个 TRUNCATE 清单里 | ✅ 修法：从复位清单移除该表的 TRUNCATE；如需清空走单独人工步骤并在此登记 | 复位脚本 `sql/admin/reset_demo.sql` | — |

<!-- PROJECT.md 索引行（复制区）：
- `#pg18-data-dir-change` — PG 18 改了数据目录约定
- `#spark-minio-endpoint` — Spark 连接 MinIO 必须用 IP，不能用 localhost
- `#gl-reconciliation-mismatch` — GL 对账需按 Section 汇总后比对
- `#hqla-cap-not-applied` — HQLA 二级资产 40% 上限需显式截断
- `#python314-incompatible` — Python 3.14 不兼容 GE 与 pyspark
- `#dockerhub-image-removed` — minio/spark 官方镜像已从 Docker Hub 下架
- `#detail-report-mismatch` — 明细与报表口径不一致（正回购/30天过滤）
- `#scd2-reversed-interval` — 版本区间不得反向（失效日早于生效日）
- `#dead-ows-tables` — 三张 OWS 汇总表无消费者（登记待下线）
- `#report-history-reset-exception` — 版本历史表不在复位清单（历史不可变是硬性质）
- `#delegate-audit-20260917` — 三路独立审查的 80 条发现与处置
-->

## 规范偏离（本项目 vs 上游）

> 本节是偏离记录的**唯一落点**：纪律、表、适用边界、代价与回退都住这里。

标准做法是**默认值**，偏离是例外。允许偏离的前提是四条**同时**成立：

1. **逐条登记**：一行一条，四列 = 问题 / 上游要求 / 本项目做法 / 处置。
2. **处置列指向真实锚点**：写成本文件里已有的 `### #slug` 定义；只写「已说明」不合格。
3. **说明为什么不采用标准做法**：「本项目特殊」这类空理由不合格。
4. **记代价与回退成本**：在本节末尾的汇总里各写一行。

**禁止无登记地默默降标准**；「这只是个例」不是免于登记的理由。

| 问题 | 上游要求 | 本项目做法 | 处置 |
|---|---|---|---|
| 双轨制（Core/Advanced） | 单一技术栈 | Core（PG+dbt+Airflow）+ Advanced（Kafka+Iceberg+Spark）并行 | ✅ 决策：演示需要展示两种架构，见 `[01]架构设计.md` §1.5 |
| 存储分工（ODS 进 Iceberg 不进 PG） | ODS 通常进 PG | ODS/OWD/OWS 进 Iceberg，仅 ADS 进 PG | ✅ 决策：湖仓一体架构，见 `[04]环境设计.md` §4.3 存储分工 |
| requirements/ 不落九文档 | 原始需求应归档 | 保留 requirements/ 参考，待收尾阶段处理 | ⏳ 待决策：收尾时移入 references/ 或 archive/ |
| 报表主键用区位码而非自增序列 | `[99]详细材料.md` 要求 `report_id BIGSERIAL PRIMARY KEY`，子表 `BIGINT` 外键 | 「机构-报表-报告期-口径」四段文本码，由 dbt 模型产出，例 `ENT001-FR2052A-20260916-01` | ✅ 决策：自增号随每轮全量覆盖重编号，撑不起跨批次的重述引用；见 `dbt/models/marts/ads_fr2052a_report.sql` 列注释 |
| 明细表不带报表身份外键 | `[99]详细材料.md` 要求 `ads_fr2052a_detail.report_id` 引用报表表 | 明细用「报告日 + 实体」定位，不设 report_id 列 | ✅ 决策：明细是一个主体的多行下钻，报表身份由主体唯一确定 |
| 版本历史放独立表 | 上游未定义版本历史 | `silver.owd_*_history` 与 `ads.ads_fr2052a_report_history`，OWD 物化方式不动 | ✅ 决策：改 OWD 为增量物化要重写 7 个已验证模型；见 `[04]环境设计.md` §4.3 |
| 报送文件按报送主体出 | 上游只要求生成 XBRL / XML / CSV | 一个报送主体三份文件，文件名即 report_id | ✅ 决策：FR 2052a 按法人实体分别报送，集团口径由母公司另报 |
| 覆盖写显式开 `truncate=true` | Spark JDBC 写库默认 `truncate=false`，即 DROP + CREATE | 导出作业显式 `truncate=true`，写前比对模型列与目标表列 | ✅ 决策：默认行为会静默清掉授权、触发器与库侧列 |
| 数据质量用自研规则引擎 | `[99]详细材料.md` 指定 Great Expectations | 规则定义已在 `ref.ref_validation_rules`，由 `run_dq_rules.py` 执行 | ✅ 决策：转成 GX suite 等于规则定义存两份，必然漂移 |
| 血缘用 dbt meta 自渲染 | `[99]详细材料.md` 指定 DataHub | `dbt/models/**/schema.yml` 的 meta 声明 + `render_lineage.py` 渲染 | ✅ 决策：DataHub 部署成本高，演示价值等价 |
| 机器门禁只落地一半 | 红线要求「能写成 lint、检查脚本或 CI check，就不指望模型读到」 | 代码侧闸已齐（`make lint` + 本地钩子 + CI）；**头注缺失告警**与**被引用文件存在性检查**尚未实现。曾是机器闸的 `[AI]` 提交标记已按用户决定撤销 | ⏳ 待决策：补两个检查脚本，或在 AGENTS 里保持「未实现」的明说（当前已改为明说）|
| 监控与 BI 栈未落地 | 需求文档提到 Grafana / Prometheus / Superset | 三者都没有部署，巡检由 `pipeline_health.py`、血缘由 `render_lineage.py` 直接输出 | ⏳ 待决策：接监控栈，或明确「演示项目不做面板」并保留现状 |
| 变更留痕目录为空 | AGENTS 要求功能变更在 `docs/changes/{module}.md` 追加条目 | 已按格式补齐：`docs/changes/engineering.md` 收录 E6 质量闸、E7 审查修复与本次工程变更 | ✅ 已补：条目格式为四段（范围 / 变更 / 验证 / 回滚），见 `docs/changes/engineering.md` 的 `changes-trail-bootstrap` |
| 直接在 main 提交 | `AGENTS.md` §4 要求走 feature 分支再提交 MR | 全部历史都在 main 上直接提交，仓库只有 main 一个分支 | ⏳ 待决策：单人加 agent 的项目没有第三方评审人，分支与 MR 只增加动作；若保留该红线，须同步改 §4 并写明谁 review |
| DQ 结果日志按批次先清后写 | 上游未定义日志粒度 | 一行 = 一个批次的一条规则，重跑前由 `clear_dq_batch.py` 清该批次 | ✅ 决策：不清则重跑静默翻倍，「本批次几条 ERROR」随之翻倍 |
| lint 口径排除 4 类规则 | 上游要求 `ruff check .`、`ruff format .`、`mypy .` 全过 | 配置收在项目根 `pyproject.toml`，排除 `D415`、`N812`、`RUF001`、`RUF002`、`RUF003` | ✅ 决策：前两类与中文写作冲突（`D415` 只认 ASCII 句末标点、`RUF001-003` 把全角标点当歧义字符），`N812` 与 PySpark 的 `functions as F` 写法冲突。逐条理由与命中数写在 `pyproject.toml` 注释里；无命中的 `D401`/`D202` 不排除，继续管事 |
| 提交闸两道，本地在前 | 上游要求「等待 CI 通过」后合并 | 本地 `.githooks/pre-commit` 跑 `make lint`；推送后 GitHub Actions 跑同一条命令 | ✅ 决策：本地那道先拦住，省一次往返；CI 兜住没配钩子的克隆。gitee 只作镜像，没有 runner |
| 不建单元测试套件 | 上游要求覆盖率 ≥ 80% | 未建 pytest 套件，判据改为数据层核对脚本全绿加端到端重跑 | ✅ 决策：本项目的风险在数据与编排，不在函数分支；核对脚本要能区分「零命中」与「读不到」 |

**适用边界**（条件条目为什么不在表里、本项目实际取了哪条路）：

- 两套栈的设想：需求里写了 Core 版与 Advanced 版两套部署形态。实现上只有一套链路（存储与调度在 Server 1、计算与消息在 Server 2）。分两台是因为两台机器角色不同，不是两个可切换的版本。
- 存储分工：湖仓一体是现代数据栈标准做法，PG 只做报表输出层
- 报表主键：区位码要求「每一段都有业务含义」，因此末段只放口径码；把版本号编进主键会与 `record_version` 形成两份定义

**代价与回退汇总**（每行偏离一条）：

- **双轨制**（✅ 决策）——代价：维护两套部署清单；回退：保留 Core 版即可，Advanced 版可下线
- **存储分工**（✅ 决策）——代价：ODS 不可直接用 PG 查询，需走 Spark/Iceberg CLI；回退：把 ODS 建到 PG 需改 deploy 脚本
- **报表主键区位码**（✅ 决策）——代价：键比整数长，人工手写易错；回退：业务码加唯一约束，另立整数代理键
- **明细表不带报表身份外键**（✅ 决策）——代价：查明细要带两个条件；回退：加回 report_id 列并按主体回填
- **版本历史放独立表**（✅ 决策）——代价：当前快照与历史两条路径各自维护；回退：OWD 改增量物化并合并历史
- **报送文件按主体出**（✅ 决策）——代价：文件数为主体数乘三；回退：合并回整批一份，台账按整批一行
- **覆盖写开 truncate**（✅ 决策）——代价：模型结构变更时导出直接失败，须先补迁移；回退：回到默认删除重建并每次重放授权
- **自研 DQ 引擎**（✅ 决策）——代价：GX 现成的算子与报告不可用；回退：把 ref 表规则翻译成 GX suite
- **血缘自渲染**（✅ 决策）——代价：没有 DataHub 的搜索与影响面分析界面；回退：接入 DataHub 并导入 dbt manifest
- **DQ 日志先清后写**（✅ 决策）——代价：日志环节多一步前置脚本；回退：改日志表为只留最近一次
- **lint 排除 4 类规则**（✅ 决策）——代价：这几类问题不再有机器兜底，只能靠评审看；回退：删掉 `pyproject.toml` 里对应的 `ignore` 项并批量整改
- **提交闸本地优先**（✅ 决策）——代价：本地与 CI 都要维护可用环境，版本口径靠 `Makefile` 单源约束；回退：删掉 `.githooks/`，只留 CI
- **不建单元测试套件**（✅ 决策）——代价：函数级回归只能靠核对脚本与端到端重跑，粒度偏粗；回退：补 pytest 套件并接进 `make lint`
- **机器门禁减半**（⏳ 待决策）——代价：头注与文件引用仍靠人工核对；回退：把 AGENTS 里的两处声明删掉即可自洽。`[AI]` 提交标记闸已按用户决定撤销，不在本项范围内
- **监控栈缺席**（⏳ 待决策）——代价：没有历史趋势与告警推送，只有一次性的巡检输出；回退：接入 Prometheus + Grafana（约一张 compose 文件）
- **变更留痕启用**（✅ 已补）——代价：每次功能与契约变更多写一条四段条目；回退：删除 `docs/changes/` 下的条目文件并恢复 AGENTS §6 的原始措辞
- **直接提交主分支**（⏳ 待决策）——代价：没有分支隔离，出问题只能靠 revert 回退；回退：恢复 feature 分支加 MR 流程，并指定评审人
