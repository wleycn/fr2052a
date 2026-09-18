# engineering.md — 工程级变更留痕

> 记录 `python/` 模块之外的工程变更：质量闸、CI、编排脚本、文档体系、协作纪律。
> 追加式，新的在下。每条四段：范围 / 变更 / 验证 / 回滚。样例代码与配置不进本文件。
> 目录纪律见 `AGENTS.md` §6：`docs/changes/` 只放 `{module}.md` 条目文件。

---

## lint-type-gate

- 范围：`pyproject.toml`、`Makefile`、`.githooks/pre-commit`、`.github/workflows/lint.yml`、`python/**` 与 `deploy/server1/airflow/dags/**` 的注解补齐、`docs/rules/` 三份文档的检查点
- 变更：`docs/rules/` 早就写明了要跑 ruff 与 mypy，实现侧却一直没配。本次把这条静态检查补成机器闸。配置唯一源是 `pyproject.toml`，`Makefile` 包一层命令并提供 `uvx` 兜底，提交前钩子与 CI 跑同一条 `make lint`。ruff 选 14 个规则类，行宽 120，docstring 用 Google 风格；mypy 打开 `disallow_untyped_defs` 与 `disallow_any_generics`。四类与中文写作冲突的规则（`D415`、`RUF001`、`RUF002`、`RUF003`）和与 PySpark 写法冲突的 `N812` 逐条排除，理由与命中数写在配置注释里。验收清单与开发流程文档改成引用 `make lint`，不再单列步骤。
- 验证：`ruff check` 从 166 个问题清零，`mypy` 从 86 个问题清零，其中补了 113 条 docstring 与 18 处类型注解。等价性三层证据：格式化前后按字节码指纹比对 202 个代码对象逐项一致；16 份导出 CSV 重生成的 md5 与基线 16/16 相同；端到端重置重跑 16 个环节全绿。CI 双向验证：正常提交通过，运行号 35237994950；故意留下未用导入的临时分支被拦下，运行号 35238108100，临时分支已删除。
- 回滚：删除 `pyproject.toml`、`Makefile`、`.githooks/`、`.github/workflows/lint.yml`，并把 `docs/rules/` 三份文档的检查点改回人工核对。代码侧的 docstring 与类型注解可保留，不影响运行。

## delegate-review-fixes

- 范围：`deploy/server1/airflow/dags/fr2052a_daily_batch.py`、`deploy/server2/run-daily-pipeline.sh`、`docs/business/` 五份业务文档、`docs/rules/PROJECT-STRUCTURE.md`、`AGENTS.md`、`README.md`
- 变更：三个只读子代理分别审应用代码、dbt 与 SQL、文档，共提出 80 条发现。已修 29 条，其中两处会真实误导运行：日批 DAG 把「施加授权与迁移」排在「导出报表」之后，与跑批脚本和接口文档都相反；源文件到位检查查的是容器内路径，而任务实际在宿主执行，加上错误被吞、`wc -l` 恒返回 0，导致这道检查永远通过。修复后授权环节前移，源文件检查挪进跑批脚本新增的 `check-source` 环节，判据由调用方改为看退出码。另补上缺失的 `verify_scd2` 任务（日批任务数 17 到 18），并按代码实际内容重写文档里的宏表、CLI 契约表、Topic 数与端口。剩 45 条涉及口径与设计取舍，未改，逐条登记在 `docs/AUDIT-2026-09-17-delegate-review.md`。审查期间一度新增 `.githooks/commit-msg` 强制提交标记，该钩子已按 `commit-message-policy` 撤销。
- 验证：`airflow dags list-import-errors` 无错误，日批 DAG 任务链与脚本步骤名逐一对照。`check-source` 正负两向实跑：源文件齐全时退出码 0，缺文件时退出码 1。文档侧按代码逐项复核命令签名与表名，README 里跑不通的参数已换成实际存在的选项。
- 回滚：`git revert 74fdcc7`。该提交只动编排、文档与钩子，不涉及数据表结构，回退后须重跑一次日批确认任务链顺序。

## commit-message-policy

- 范围：`AGENTS.md` §5、`docs/rules/CODING-STANDARD.md` Git 规范、`.githooks/commit-msg`、`docs/business/KNOWN-ISSUE.md`
- 变更：撤销「agent 提交必须带 `[AI]` 前缀」这一条，`.githooks/commit-msg` 删除。理由由用户给出：agent 是代用户执行，它提交的即用户提交，提交信息不该体现执行者是谁。提交信息按 `type: subject` 写，说明改了什么、为什么。代码文件头的 `[AI-GENERATED]` 与 `reviewed_by` 保留，那部分记录的是代码来源与评审状态，与提交归属无关。
- 验证：撤销后本地提交不再被拦截，提交信息形如 `fix: ...`。钩子目录只剩 `pre-commit`，它仍跑 `make lint`。`grep -rn "\[AI\]"` 在全仓只命中本文件与历史审计报告、构建日志。
- 回滚：恢复 `.githooks/commit-msg` 与两处文档措辞即可。回滚成本低，无数据影响。

## changes-trail-bootstrap

- 范围：`docs/changes/engineering.md`、`AGENTS.md` §6、`docs/rules/PROJECT-STRUCTURE.md` 目录职责表、`docs/business/KNOWN-ISSUE.md` 偏离表
- 变更：`AGENTS.md` 要求功能与契约变更在 `docs/changes/{module}.md` 追加条目，该目录却一直是空的，E6 与 E7 的变更散落在 `docs/build-log.md`。本次按格式补上条目，本文件即首个条目文件，同时成为后续条目的格式样例。`AGENTS.md`、`PROJECT-STRUCTURE.md`、`KNOWN-ISSUE.md` 里「目录为空」「模板待补」的口径同步更正。条目 slug 不再要求与分支同名，本项目直接在主分支推进，没有 feature 分支。
- 验证：目录内只有 `engineering.md` 一个条目文件，无 README 与附件。四段结构逐条自检：范围写清文件、变更写清动机与做法、验证给可复现的证据、回滚给具体动作。
- 回滚：删除本文件并恢复三处文档措辞。回滚后变更史重新退回只存在于 `docs/build-log.md` 的状态。

## table-contracts

- 范围：新增 `docs/tables/`（51 份契约）、`docs/rules/PROJECT-STRUCTURE.md`、`AGENTS.md` §9、`README.md`、`docs/business/PROJECT.md`、`docs/business/DATA-DESIGN.md` §2.1
- 变更：数据类项目的上游规范要求「每表一份 `docs/tables/{table}.md`」，本项目此前没有这个目录。本次补齐 51 份：ref 9 张、bronze 7 张、silver 明细 8 张、silver 汇总 5 张、版本历史 7 张、gold 与 ADS 报表 3 张、PostgreSQL 控制与审计 12 张。每份 13 节：层级 / 主题 / 粒度 / 业务主键 / 去重方式 / 分区 / 字段清单 / 金额单位约定 / PII 与脱敏 / 生命周期 / 新鲜度 SLA 与 owner / 上下游依赖 / 质量规则清单。字段清单不是手抄的：ref 与 bronze 从 `sql/iceberg/*.sql` 的 DDL 抽取，PostgreSQL 表从 `sql/postgres/*.sql` 抽取，silver 与 gold 从 dbt 模型 SQL 的最终投影抽取，版本历史表从 `owd_scd2.py` 的结构加 6 个版本列。文档地图、结构规范与 README 的目录清单同步登记该目录。
- 验证：51 份文件与 51 张表一一对应，逐份核对三件事：13 节齐备且每节非空、字段名与抽取结果逐项一致、无占位词。DDL 侧另做了抽取数与 DDL 声明数的逐表比对，28 张全部一致（例：`ads_fr2052a_report` 的 55 列覆盖 Section A–K 全部行项目）。不入契约的两类在 `DATA-DESIGN.md` §2.1 写明：连通性自检的探针表、只有主题声明的三个 Kafka 主题。
- 回滚：删除 `docs/tables/` 目录，恢复四处文档的目录清单。契约是纯文档，回滚不影响任何运行链路。

## bronze-dedup-key-fix

- 范围：`python/consumers/kafka_to_iceberg.py`、`python/alerts/realtime_scanner.py`、`docs/tables/ods_*.md`（7 份）、`docs/tables/ads_fr2052a_realtime_alerts.md`
- 变更：bronze 层 MERGE 的匹配条件和批内去重窗口都缺 report_date，同一源记录在不同报告日会被当成同一条，后一个报告日覆盖前一个。改为三列 source_system + source_record_id + report_date。实时扫描器的事件 ID 和落库 report_date 列原先取命令行常量，改为取消息体里的 report_date，使同一笔敞口跨报告日算出不同 ID。命令行 --report-date 保留，用途收窄为日志与不一致计数提示。
- 验证：自验四条断言全 PASS（MERGE 含 report_date、去重窗口含三列、落库 report_date 不再取命令行常量、事件 ID 的哈希输入含消息里的报告日）；`make lint` 三子命令全绿。上线实测三项：① 把 500 行源数据改标为 2026-09-17 重放后，`bronze.ods_deposits` 两期各 500 行并存（旧实现会把 2026-09-16 的 500 行改写成 2026-09-17，表内仍只 500 行，且行数核对照样通过）② 以 `REPORT_DATE=2026-09-17` 刻意传错运行参数执行实时扫描，落库 47 条事件的 `report_date` 全为消息里的 2026-09-16，并打印 500 条消息与运行参数不一致的提示 ③ 只清消费位点、保留事件表重放时，作业以主键冲突报错退出，确认「重复出现」仍是显式失败而不是静默重复。
- 回滚：`git checkout -- python/consumers/kafka_to_iceberg.py python/alerts/realtime_scanner.py docs/tables/`。回退后 bronze 层恢复两列匹配，实时事件 ID 恢复取命令行日期。
