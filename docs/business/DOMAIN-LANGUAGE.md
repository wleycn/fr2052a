# DOMAIN-LANGUAGE.md — 项目术语表

> **纪律**：新增概念先在此登记再落代码；发现术语漂移（代码与本文不一致）→ 改代码或改本文，不允许两套并存。

## 1. 术语表

### 1.1 核心业务术语

| 术语（中文） | 英文 / 代码标识 | 定义（一句话，可判定） | 禁止说法 | 所属模块 | 备注 |
|---|---|---|---|---|---|
| FR 2052a | FR 2052a Report | 美联储复杂机构流动性监控报告，Section A-K | "流动性报表"、"LCR 报表" | ads | 全称 Federal Reserve 2052a |
| 批发无担保融资 | Wholesale Unsecured Financing | 商业票据、大额存单、无担保借款等 | "无担保融资"（缺少批发限定）| sec_a | Section A |
| 批发有担保融资 | Wholesale Secured Financing | 回购协议、证券借贷、FHLB 借款等 | "有担保融资"（缺少批发限定）| sec_b | Section B |
| 存款 | Deposits | 零售/批发活期、储蓄、定期存款 | "存款余额"（缺少分类）| sec_c | 含 Retail 与 Wholesale |
| 其他资金来源 | Other Funding / Liabilities | 批发存款以外的负债与融资：其他有担保与无担保债务 | "其他负债"（缺少融资含义）| sec_d | Section D |
| 现金及等价物 | Cash & Cash Equivalents | 央行存款、短期高流动性资产 | "现金"（缺少等价物）| sec_e | 不含受限现金 |
| 贷款组合 | Loan Portfolio | 商业贷款、零售贷款、按揭贷款 | "贷款"（缺少组合限定）| sec_f | 现金流入侧 |
| 证券投资组合 | Securities Portfolio | 国债、机构债、MBS/ABS、公司债 | "证券"（缺少投资组合限定）| sec_g | 含 HQLA 与非 HQLA |
| 衍生品 | Derivatives | IRS、FX、期权、期货、CDS | "衍生工具" | sec_h | 按 Net MTM 计 |
| 抵押品 | Collateral | HQLA、非 HQLA、已质押资产 | "质押品" | sec_i | 区分已受限/非受限 |
| 或有负债 | Contingent Liabilities | 信用证、担保、贷款承诺 | "表外项目"（过于宽泛）| sec_j | Off-Balance Sheet |
| 汇总指标 | Summary Metrics | LCR 补充、净资金流出、缺口 | "汇总数据" | sec_k | Calculated Metrics |

### 1.2 数据层术语

| 术语（中文） | 英文 / 代码标识 | 定义 | 禁止说法 | 所属模块 |
|---|---|---|---|---|
| ODS | Operational Data Store | 原始数据镜像层，1:1 映射源系统 | "原始表"（模糊）| ods |
| OWD | Operational Warehouse Detail | 标准化明细层，清洗 + 编码统一 | "明细表"（缺少标准化含义）| owd |
| OWS | Operational Warehouse Summary | 汇总计算层，分桶 + 聚合 | "汇总表"（缺少计算含义）| ows |
| ADS | Application Data Store | 报表输出层，FR 2052a 对齐 | "报表表"（循环定义）| ads |
| Bronze | Iceberg Bronze Layer | 原始镜像，追加写入，支持 Time Travel | "原始层"（缺少 Iceberg 语境）| lakehouse.bronze |
| Silver | Iceberg Silver Layer | 标准化层，MERGE upsert | "清洗层"（不够准确）| lakehouse.silver |
| Gold | Iceberg Gold Layer | 报表层，导出到 PG | "最终层"（模糊）| lakehouse.gold |
| LZ | Landing Zone | 落地库：源系统原样落盘，未做任何加工，对应 ODS 与 Iceberg bronze | "原始数据表" | ods / bronze |
| DZ | Data Zone | 加工库：事实与维度成形，对应 OWD 与 OWS | "处理后的表" | owd / ows |
| HZ | History Zone | 历史库：保留同一主键的历次版本，对应 SCD2 的 `*_history` 表 | "历史数据表"（未说明版本语义）| silver.owd_*_history |
| RPT | Report Zone | 报表库：面向报送的输出，对应 ADS 的报送服务层 | "报表落地区" | ads |

### 1.3 技术术语

| 术语（中文） | 英文 / 代码标识 | 定义 | 禁止说法 |
|---|---|---|---|
| SCD2 | Slowly Changing Dimension Type 2 | 类型 2 缓慢变化维，保留历史版本 | "历史表" |
| Time Travel | Iceberg Time Travel | 按 snapshot 查询历史数据 | "历史查询" |
| Kafka KRaft | Kafka Raft Consensus | Kafka 无 ZooKeeper 模式 | "Kafka 集群"（模糊）|
| dbt model | Data Build Tool Transformation Model | dbt 转换模型，SQL 定义数据转换 | "SQL 文件" |
| 规则引擎 | Rule Engine（`run_dq_rules.py`） | 执行 `ref.ref_validation_rules` 里声明的校验规则，结论落校验日志 | "校验脚本" |
| Airflow DAG | Directed Acyclic Graph | 调度依赖图 | "任务"（缺少依赖语义）|
| 血缘渲染 | `render_lineage.py` | 用 dbt 元数据与 SQL 解析出表级血缘、列级监管映射 | "血缘工具" |
| MinIO | 对象存储 | S3 兼容对象存储 | "文件存储" |
| Iceberg | Apache Iceberg | 开放表格式 | "表格式" |

### 1.4 工程与协作术语

本项目文档与日常沟通里高频出现，含义固定，不要另造说法：

| 术语 | 英文 / 代码 | 定义 | 说明 |
|------|-------------|------|------|
| 闸 / 机器闸 | gate | 由程序判定并通过退出码拦停的检查，不依赖人自觉 | 本项目三道：放行闸、权限自测、数据层核对 |
| 放行闸 | submission gate | 决定「这次报送能不能生成文件」的那道闸 | `check_submission_gate.py`，退出码 0 放行 / 2 阻断 / 3 无法判定（同样不放行）|
| 判据 | criterion | 验收清单里那条**可执行**的检查动作 | 写形容词（如「性能良好」）不算判据 |
| 证据 | evidence | 判据实核时看到的原始输出：命令、退出码、行数、文件名 | 没有证据的勾选一律不算通过 |
| 单源 | SSOT（Single Source of Truth） | 同一事实只在一处声明，其他地方引用它 | 阈值在 JSON、工具版本在 Makefile、主题映射在一个 JSON |
| 偏离登记 | deviation log | 有意不按上游标准做法时，必须留下的一条记录 | 唯一落点 `KNOWN-ISSUE.md`，含代价与回退方式 |
| 锚点 | anchor | 已知坑的稳定标识（`#kebab-case`），一经写入不改拼写 | 由 `PROJECT.md` 索引行引用 |
| 演练 | dry-run | 只列清单、不动数据的预演 | 默认模式；真做要显式加 `--apply` |
| 幂等 | idempotent | 同一输入重复执行，结果与执行一次相同 | 重放、跑批、导出的硬要求 |
| 静默失败 | silent failure | 失败与「没命中」在输出上分不出来，都表现为「跑完了、结果为空」 | 本项目历史上最高发的缺陷类型 |
| 脱敏单射 | injective masking | 脱敏后的值能一对一映射回原文，不会两个客户塌成同一个值 | 加盐 SHA-256；非单射会让汇总口径塌成一桶 |
| PII 对照表 | PII vault | 存 token 与明文对应关系的表，明文唯一落点 | `secure.fr2052a_pii_map`，仅合规与管理员可读 |
| 台账 | ledger | 记录「发生过什么」的表，可追溯、可核对 | 报送台账、重述登记、审计日志 |
| 检查点 | checkpoint | 流式作业记录「读到哪了」的状态目录 | 与消费位点一起清，才能干净重放 |
| 重放 | replay | 把样本数据重新打进 Kafka，模拟源系统重新上报 | 只清主题不清位点，重放会一条都读不到 |
| 快照 | snapshot | Iceberg 表在某一刻的完整状态，时间旅行按它查 | 保留策略 7 天且至少留 10 个 |
| 血缘 | lineage | 数据从源头表到报表表的依赖关系，含列级监管映射 | `audit.audit_data_lineage` + `LINEAGE.md` |
| 环节 | step | 跑批流水线里可单独调用的一个步骤，有名字与说明 | 共 26 个，见 `run-daily-pipeline.sh --list` |
| 批号 | batch_id | 一次跑批的标识，质量结果与审计日志按它归档 | 形如 `BATCH-20260916-001` |

## 2. 口径类术语（数值/时间/状态）

| 术语 | 口径 | 单位 | 精度/舍入 | 示例 |
|---|---|---|---|---|
| 金额 | 以**美元**为单位 | USD | 2 位小数，half-up | `1234567.89` |
| 比率 | 小数表示 | 无 | 6 位小数 | `0.123456` = 12.3456% |
| 时间戳 | **UTC** | — | 毫秒 | `2026-09-17T08:00:00Z` |
| 报告日 | 报表基准日（= 业务日期），与 `sql/iceberg/02_create_ods_tables.sql` 注释里的「业务日期」是同一个值的两种叫法 | — | DATE | `2026-09-16` |
| 日分区 | `ds` 字符串 | — | `YYYY-MM-DD` | `2026-09-16` |
| 到期日 | 合同到期日 | — | DATE | `2027-09-16` |
| 剩余天数 | 报告日到到期日的天数 | 天 | 整数 | `365` |
| 处理日 | 跑批实际执行的日期，本项目取报告日次日 | DATE | — | `2026-09-17` |
| 生效日 | 数据版本从哪天开始生效，取处理日 | DATE | — | `2026-09-17` |
| 报送时刻 | 报送动作实际发生的时刻，住台账 `submitted_at`；报告日 ≠ 报送时刻（报告日是报表基准日） | TIMESTAMP | — | `2026-09-17T08:00:00Z` |
| 版本区间 | 一条记录的生效到失效区间；右端为空表示当前有效 | DATE | 闭区间 | `2026-09-16 起，失效日为空` |
| 30 天窗口 | Section F 只统计该窗口内到期的头寸 | 天 | 整数 | `30` |
| LCR 监管下限 | 流动性覆盖率不得低于此值 | 倍数 | 2 位小数 | `1.00` |
| LCR 内部预警线 | 低于此值即发预警，比监管线更早提醒 | 倍数 | 2 位小数 | `1.10` |
| 二级资产上限 | HQLA 二级资产（2A+2B）不得超过 HQLA 总额的比例 | 比率 | 6 位小数 | `0.400000` |
| 现金流入上限 | 30 天现金流入最多可抵减流出的比例 | 比率 | 6 位小数 | `0.750000` |
| 大额未保险存款门槛 | 触发实时预警的单笔未保险存款金额 | USD | 整数 | `3000000` |
| 未提取承诺门槛 | 触发实时预警的单笔未提取承诺金额 | USD | 整数 | `2000000` |
| 快照保留 | Iceberg 快照至少保留的时长与个数 | 天 / 个 | 整数 | `7 天，且不少于 10 个` |

> 上表的数值口径只在一处声明：`config/liquidity_thresholds.json`。改口径改那一份，判定与放行两侧同时生效。

> 🔴 **口径混用**（美元/本币、UTC/本地、小数/百分数）是本类**最高频事故来源**，必须逐项写明。

## 3. 状态与枚举

### 3.1 报表状态

| 枚举 | 取值 | 含义 | 状态流转 | 代码位置 |
|---|---|---|---|---|
| `submission_status` | `GENERATED` / `SUBMITTED` / `ACCEPTED` / `REJECTED` | 文件已生成 / 已提交 / 监管已接收 / 监管退回 | 生成脚本写 `ACCEPTED`/`REJECTED`（按模拟回执）；`GENERATED`/`SUBMITTED` 是列注释里的预留值，当前代码未写 | `ads.ads_fr2052a_submission` |
| `file_format` | `XBRL` / `XML` / `CSV` | 报送文件格式，每个实体各出一套 | — | `ads.ads_fr2052a_submission` |
| `severity` | `CRITICAL` / `WARNING` / `INFO` | 预警严重度 | — | `ads.ads_fr2052a_alerts` |
| `status`（预警） | `OPEN` / `CLOSED` | 规则是否仍在命中 | 命中即 `OPEN`，不再命中自动转 `CLOSED` | `ads.ads_fr2052a_alerts` |
| `state`（熔断闸） | `OPEN` / `HALTED` | 报送是否放行 | 出现阻断级预警即 `HALTED`，清零后回 `OPEN` | `ads.ads_circuit_breaker` |
| `status`（对账） | `PASS` / `FAIL` | 单个 Section 是否对平 | 每轮重算 | `ads.ads_gl_reconciliation` |
| `check_result` | `PASS` / `FAIL` / `SKIPPED` | 单条规则的结论 | — | `ads.ads_fr2052a_validation_log` |

### 3.2 质量与预警字段

| 枚举 | 取值 | 含义 |
|---|---|---|
| `severity`（规则声明） | `ERROR` / `WARNING` | 规则违反时的严重度；`ERROR` 计入熔断判定 |
| `blocks_submission`（预警） | `TRUE` / `FALSE` | 该条预警是否阻断报送——放行闸读的就是这个字段 |
| `occurrence_count` | 整数 | 同一规则在同一主体上累计命中的次数；重跑不清零即为异常 |
| `scope`（熔断范围） | `GLOBAL` | 熔断作用范围，本演示只有全局一个 |

### 3.3 退出码

程序用退出码表达结论，不看输出文字。调用方（Airflow、流水线）据此阻断：

| 退出码 | 含义 | 谁在用 |
|---|---|---|
| `0` | 通过 / 放行 | 全部核对脚本、放行闸 |
| `1` | 失败（核对不通过、自检不过） | `verify_*.py`、生成器自检 |
| `2` | 阻断：熔断中，禁止报送 | 放行闸 |
| `3` | 无法判定：状态缺失或依赖表读不到，按不放行处理 | 放行闸、血缘渲染缺依赖时 |

> 🔴 **枚举以代码定义为准**，本文与之逐字一致；新增取值必须同时更新两处。

## 4. 模块/服务与边界

| 名称 | 职责（一句话） | 拥有数据 | 不负责 |
|---|---|---|---|
| 数据接入 | 从源系统采集数据到 ODS | `ods_*` 表 | 数据清洗、业务校验 |
| ODS 层 | 1:1 保留源数据，加 ETL 元数据 | `ods.*` schema | 标准化、转换 |
| OWD 层 | 清洗、编码统一、汇率转换 | `owd.*` schema | 聚合、报表对齐 |
| OWS 层 | 到期分桶、现金流计算、HQLA 分类 | `ows.*` schema | 明细展开、GL 对账 |
| ADS 层 | FR 2052a 报表输出 | `ads.*` schema | 数据采集、实时预警 |
| 调度编排 | Airflow DAG 管理 | DAG 定义 | 业务逻辑、数据访问 |
| 数据质量 | 规则引擎执行 `ref.ref_validation_rules` | 校验日志 | 数据纠错、阻断报送 |
| 合规熔断 | `ads.ads_fr2052a_alerts` 检查 | 告警表 | 告警发送、修复 |
| 元数据/血缘 | dbt meta 声明 + `render_lineage.py` | 血缘表与 Markdown 报告 | 数据质量、业务映射 |
| PII 脱敏 | 动态脱敏、权限控制 | 脱敏视图 | 数据加密、审计日志 |

## 5. 易混术语对照

| 容易混淆 | 区别 | 判定示例 |
|---|---|---|
| ODS vs Bronze | ODS 是 PG schema 概念，Bronze 是 Iceberg 层概念；两者可以重叠（Bronze 层即 ODS） | `ods_deposits` 可存在于 PG.ods 或 Iceberg.bronze |
| OWD vs Silver | 同 ODS vs Bronze | `owd_deposits` 可存在于 PG.owd 或 Iceberg.silver |
| 批发融资 vs 零售融资 | 批发面向机构，零售面向个人 | 商业票据（批发）vs 零售存款（零售）|
| HQLA Level 1 vs Level 2A | Level 1 零 haircut，Level 2A 15% haircut | 现金（L1）vs 机构债（L2A）|
| GL 对账 vs 数据校验 | GL 对账是财务总账与报表比对，数据校验是规则引擎检查 | 对账差异 > 阈值 → 阻断；校验失败 → 记录 |
| 重述 vs 回刷 | 重述是修正历史数据并生成新版本，回刷是重新运行整个管道 | 重述触发 SCD2 版本；回刷覆盖快照 |
| 报告日 vs 处理日 vs 生效日 | 报告日=报表基准日；处理日=实际跑批日；生效日=版本从哪天开始生效 | 09-16 的报告在 09-17 跑批，版本生效日写 09-17 |
| 报告日 vs 报送时刻 | 报告日是报表基准日（哪天的数据）；报送时刻是报送动作发生的时刻（住台账 `submitted_at`） | 09-16 的报告在 09-17 08:00 报送 |
| 熔断闸 vs 放行闸 | 熔断闸只记录状态（`OPEN`/`HALTED`）；放行闸读状态做拦停 | 状态 `OPEN` 但存在阻断级预警时，放行闸仍拒绝报送 |
| 覆盖写 vs 追加写 | 覆盖写整表替换（导出用 `TRUNCATE` 保住表结构与授权）；追加写只加行 | 导出到 PG 是覆盖写；bronze 入湖按主键 MERGE |
| 演练 vs 执行 | 演练只列清单不动数据；执行必须显式给 `--apply` | `reset-demo.sh` 与 `maintain_tables.py` 都按这个约定 |
| 规则预警 vs 实时预警 | 规则预警按跑批重算，一条规则加一个主体聚合成一行；实时预警是事件流，一行一个事件 | `ads_fr2052a_alerts` 与 `ads_fr2052a_realtime_alerts` 是两张表 |
| 集团合并口径 vs 单实体口径 | 合并口径只有集团一行（`is_consolidated=TRUE`）；单实体口径每个法人各一行 | 基线报表 5 行 = 集团 1 行 + 单个法人 4 行 |

## 6. 监管术语（中英对照）

| 中文 | 英文 | 缩写 | 说明 |
|---|---|---|---|
| 流动性覆盖率 | Liquidity Coverage Ratio | LCR | 高压stress下30天流动性充裕度 |
| 净稳定资金比率 | Net Stable Funding Ratio | NSFR | 长期稳定资金充足度 |
| 高质量流动性资产 | High-Quality Liquid Assets | HQLA | Level 1/2A/2B 分类 |
| 批发无担保融资 | Wholesale Unsecured Financing | — | Section A |
| 批发有担保融资 | Wholesale Secured Financing | — | Section B |
| 存款 | Deposits | — | Section C |
| 其他资金来源 | Other Funding / Liabilities | — | Section D |
| 现金及等价物 | Cash & Cash Equivalents | — | Section E |
| 贷款组合 | Loan Portfolio | — | Section F |
| 证券投资组合 | Securities Portfolio | — | Section G |
| 衍生品 | Derivatives | — | Section H |
| 抵押品 | Collateral | — | Section I |
| 或有负债 | Contingent Liabilities | — | Section J |
| 汇总指标 | Summary / Calculated Metrics | — | Section K |
| 法人实体 | Legal Entity | LE | 合并口径/重要子公司 |
| 交易对手 | Counterparty | CP | 央行、银行、券商、企业 |
| 到期分桶 | Maturity Bucket | — | O/N, 1-7D, ..., >1Y |
|  haircut | Haircut | — | HQLA 折扣率 |
| 受限资产 | Encumbered Assets | — | 已质押/已担保资产 |
| 非受限资产 | Unencumbered Assets | — | 可用于抵押的资产 |
| 总账 | General Ledger | GL | 财务总账系统 |
| 对账 | Reconciliation | GL Recon | GL 与报表比对 |
| 重述 | Restatement | — | 迟到数据修正 |
| 缓慢变化维 | Slowly Changing Dimension | SCD | 类型 2 保留历史版本 |
| 数据血缘 | Data Lineage | — | 数据从源头到报表的追踪 |
| 合规熔断 | Compliance Circuit Breaker | — | 告警阻断报送 |
| 未保险存款 | Uninsured Deposits | — | 超出存款保险限额的存款，实时预警的对象 |
| 大额敞口 | Large Exposure | — | 单笔金额超过门槛的敞口，实时扫描的对象 |
| 净现金流出 | Net Cash Outflow | — | 30 天流出，减去按上限抵减的流入，LCR 的分母 |
| 二级资产 | Level 2 Assets | L2A / L2B | 需打折的优质流动性资产，合计不超过 HQLA 的 40% |
| 集团合并 | Consolidated | — | 整个集团一个口径，由母公司另行报送 |
| 单实体报送 | Entity-level Filing | — | 每个法人实体各出一套文件，文件名即报表主键 |
| 报送回执 | Acknowledgement | — | 监管方返回的接收凭据，含回执编号与消息 |

## 7. 变更记录

| 日期 | 术语 | 变更 | 影响面 |
|---|---|---|---|
| 2026-09-17 | 新增 | 完整术语表 | 本文档 |
| 2026-09-17 | 新增 | 补「工程与协作术语」一节（闸、判据、证据、单源、偏离、锚点等）与退出码一节 | 本文档、协作口径 |
| 2026-09-17 | 修正 | LZ / DZ / HZ / RPT 四行释义错乱（DZ 与 HZ 描述重复、RPT 写成落地库），按实际分层重写 | 本文档、日常沟通 |
| 2026-09-17 | 修正 | 报送枚举与实现不一致：`submission_status` 缺 `GENERATED`/`SUBMITTED`；`DQResult`、`ValidationError`、`SubmissionStatus`、`FilingMethod` 四个名字在代码里不存在 | 本文档、验收 |
| 2026-09-18 | 新增 | 「报送时刻」术语条目与易混对照行；报告日口径补注「= 业务日期」 | 本文档 |
