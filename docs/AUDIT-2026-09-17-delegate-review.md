# 审计报告：三路独立审查（2026-09-17）

> 一次外部审查的原始记录与处置结果。**不是规范文档**，不进九文档体系；它是本轮审查的证据留档。
> 审查方式：三个子代理并行只读审查（应用代码 / dbt 与 SQL 口径 / 文档体系），禁止修改文件；
> 结论由主执行方**逐条复核后**才动手修 —— 子代理自报不算事实。

## 审查范围与产出

| 子代理 | 范围 | 发现数 | 交付状态 |
|---|---|---|---|
| 1 | `python/` 全部脚本 + 5 个 DAG | 22 | 结构合规，一次通过 |
| 2 | `dbt/models/**`、`sql/**` 口径 | 26 | 回答未通过输出结构校验（重试一次仍失败），内容完整可用 |
| 3 | README / AGENTS / `docs/**` / 配置 | 32 | 同上 |

> 给子代理的输出结构定得太严（`findings` 每项都要求 `title` 与 `evidence`，实际有项缺字段），
> 两个子代理连试两次都被判失败——但内容拿到了。教训：结构要宽，只锁真正要读的字段。

## 本轮处置汇总

| 类别 | 已修 | 要点 |
|---|---|---|
| 代码 | 3 | DAG 授权晚于导出（三路都报了同一条）、`check_source_arrival` 恒成功、DAG 链条漏项 |
| dbt / 调度 | 1 | DAG 缺 `verify_scd2` 环节 |
| 文档 | 25 | 宏表与 CLI 契约表（**原文四个宏名全错**）、README 跑不通的命令、主题数 7→10、端口 9092→9094、巡检 6→8 项、验收清单证据数字与自循环判据、AGENTS 三处死链与两句不成立的"机器门禁"声明 |
| 新机制 | 1 | `.githooks/commit-msg`：agent 会话提交必须带 `[AI]`，人工不受影响（三种情形已单测） |

## 验证期新发现的缺陷（批次 C3-2a 上线核对时暴露）

这三条都不在原审查清单里，是上线核对过程中因为「环节说 OK 但结果不对」才被揪出来的。

| 序 | 级别 | 缺陷 | 处置 |
|---|---|---|---|
| 1 | 高 | `run-daily-pipeline.sh` 的 `execute_step` 被 `run_one` 当 `if` 条件调用，**条件上下文里 `set -e` 对函数体不生效**，于是一个环节里的多条命令只有最后一条的状态算数。`dbt-run` 环节是两条命令（`dbt run` + `dbt test`），实测 `dbt run` 报 `AMBIGUOUS_REFERENCE`、gold 表没建出来，环节却报 `[OK]`，直到导出环节才炸 | ✅ 已修：`dbt run ... && dbt test ...` 显式串联，并在 `execute_step` 上方写明这条约束（多条命令必须串联） |
| 2 | 中 | `verify_scd2.py` 用**有序列表**比较「历史表业务列」与「OWD 当前业务列」。历史表新增列是 `ALTER TABLE ADD COLUMN` 追在末尾，与模型里的位置不同 → 判据失败，而打印出来的「缺 []，多 []」是空的，看的人无从下手 | ✅ 已修：改成按集合比较（列顺序对 SCD2 语义没有影响），并保留差异清单用于报错 |
| 3 | 中 | `verify_gold.py` 的抵销核对把**全部**集团内放款都从 Section F 里减掉，而报表的 Section F 只含 30 天内到期的贷款本金 —— 减了一个报表里本来就没有的金额，于是「模型是对的、核对是错的」 | ✅ 已修：抵销项按报表列归档（`sec_c_total` / `k` 融资 ← 存款腿；`sec_f_total_inflow` ← 只有 30 天内到期的贷款腿），并新增一条「抵销前提」核对守住「数据里确实存在两条腿」，避免判据在而场景不在 |
| 4 | 中 | 同一个抵销核对只抵销**余额列**，没抵销**现金流列**：活期/储蓄按行为口径归 `O/N` 之后，集团内存款腿（子公司存于母公司）也开始产生 30 天流失额 —— 实体行含它、合并行（只汇总非集团内行）不含它，于是 `sec_k_total_outflows` 逐期差 135,000（= 7,500,000 × 1.8% 流失率）。属口径修正的连带效应：修好一个判据的漏算，暴露出另一个判据的漏算 | ✅ 已修（批次 C4a）：抵销项补 `sec_k_total_outflows ← 存款腿 30 天流失额`，金额按 `ref.ref_behavior_assumptions` 的流失率从 silver 明细独立算出（不从报表读回，避免自证循环）；仍是逐报告期验算。实测：修前 verify-ads 报 `sec_k_total_outflows: Σ实体 … − 抵销 0 = …` 不一致；修后 PASS 且把抵销项写进结论行「存款腿 30 天流失额 135,000.00」，整条日批 18/18 全绿 |

| 5 | 中 | 批次 C5 新加的 `assert_one_consolidated_row_per_period` 写成 `where is_consolidated group by report_date having count(*) <> 1` —— 只能抓「多于一行」。**合并行整个缺失**（最严重的形态）时过滤后一行不剩、`group by` 连分组都不产生、`having` 从不求值，测试照样绿 | ✅ 已修（在负向探针里发现）：基准改成「报表里出现过的全部报告期」，左连合并行计数、缺的补 0，再筛 `<> 1`。A/B：故意把某期合并行的 `is_consolidated` 改成 false → 旧写法 PASS、新写法 FAIL（返回 1 行）；改回后 `dbt test` 23/23 全绿 |

## 修的时候自己踩的坑（值得记住）

**容器路径 ≠ 宿主路径。** 修第 1 条时，我先把 `check_source_arrival` 的判据改成
`test "$(ls -1 /opt/fr2052a-app/sample_data/ods/*.csv | wc -l)" -eq 7`，在真机上一跑**恒失败**：
`/opt/fr2052a-app` 是 Spark **容器内**的挂载点，而 DAG 的 SSH 任务是在**宿主**上执行，
宿主上的真实路径是 `~/fr2052a-infra/app/sample_data/ods`（那里确实有 7 个文件）。

原命令之所以「恒成功」，正是因为它查的是一个不存在的路径（`ls` 报错被 2>/dev/null 吞掉，`wc -l` 输出 0 且退出 0）。
所以这条缺陷有两层：判据不成立（`wc -l` 恒 0），路径也不对。

最终修法拉齐了单一来源：判断挪进跑批脚本新增的 `check-source` 环节（用脚本里已有的 `HOST_APP_DIR`），
DAG 只写 `pipeline_command("check-source")`。正负两向都实跑过：正常目录 rc=0，指向空目录 rc=1。

## 未改的部分（45 条）

代码与 dbt 两侧的深层缺陷本轮**一律没有改**：其中多数涉及口径与设计取舍
（LCR 分子是否含现金、30 天流入是否设下界、报表表要不要加主键、汇率缺失是否该拦），
改动会动到已经验证过的基线（样本 md5、16 环节核对、验收清单证据），应当由项目所有者决定取舍后再动。
这些条目的证据与建议都在下表里，可直接作为下一轮的任务清单。

## 一、应用代码审查（22 条）

| # | 严重度 | 发现 | 本轮处置 | Rocky 备注 |
|---|---|---|---|
| 1 | 高 | bronze 入湖 MERGE 主键缺 report_date：跨报告日重放会改写上一日快照，且行数核对抓不到 | ✅ 已修（批次 A1）：匹配条件与批内去重窗口改为三列；上线实测两报告日各 500 行并存 | 系统原始输入数据缺少一个buz_date的日期, 它代表当前business_date数据的值, 可以理解为业务数据的产生日期, 这样就可以区分 开 report_date |
| 2 | 高 | restate.py 的快照哈希两侧序列化不同，「内容未变」短路判据永不成立——每次都登记新版本 | ✅ 已修（批次 B1）：两侧统一走 `canonical_hash`（规范化 JSON 取 md5），不再在 SQL 侧算；存量哈希不再参与比较，无需迁移。实测：改动前用 SQL 复现两侧 md5 不等（`old_equal = f`）；改动后同一内容连跑两次 capture，第二次短路「无需重复登记」；改一行数据再 capture 则按 CORRECTION 登记 v2 | 需要解释业务逻辑, 没看懂 |
| 3 | 高 | 报送台账唯一键含 file_hash：数据一变就多一行旧台账，verify_submission 之后永久失败 | ✅ 已修（批次 A2）：键改为 report_id + 文件格式，哈希降为被更新的属性；新增重生成审计表；上线实测「人为造重复 → 核对红 → 迁移 → 台账回 15 行 → 核对绿」，迁移后同键再插第二行被数据库直接拒绝 | 这个是不是应该 使用 report_id 做 跨业务的主键, 而不是 file_hash ?|
| 4 | 高 | 日批 DAG 把 export_pg 排到 publish_access 之前，与跑批脚本和接口文档规定的顺序相反 | ✅ 已修：DAG 链改为 publish_access → export_pg | |
| 5 | 高 | 「确认 7 张 ODS 源文件全部到位」的任务恒成功，判据从未生效 | ✅ 已修：改用 `test "$(ls -1 …/*.csv|wc -l)" -eq 7`，数量不对即失败 | |
| 6 | 高 | 报送放行闸只校验熔断闸行存在，不校验本报告日是否跑过预警判定——新报告日可静默放行 | ✅ 已修（批次 A3）：新增运行上下文表（日期口径编码进 CHECK），闸先判「本报告日**最近一次**日批是否 SUCCEEDED」、再判「有无判定痕迹」，两条不满足均退 3；上线实测四种场景（成功放行 0 / 更晚的未收口批次 3 / 从未跑过 3 / 无判定痕迹 3） | 这个看起来还是日期处理逻辑的问题, 我建议增加项目级日期控制, 例如: 今天处理的是 哪个 buz_date 的 数据, 这个日期要全系统统一, 可以是T, 也可以是T-1|
| 7 | 中 | 实时扫描丢弃消息自带的报告日，改用命令行常量；与日批每日重放叠加会撞事件表主键 | ✅ 已修（批次 A1）：事件 ID 与落库报告日都改取消息里的值；刻意传错运行参数实测落库报告日仍正确；只清位点重放仍以主键冲突报错退出（重复写入保持显式失败） | ??? |
| 8 | 中 | 三处核对脚本在「一个对象都没核对到」时报告通过（零命中与读不到不可分辨） | ✅ 已修（批次 B1）：三处加前置断言，空集即退 1 并打印分侧计数。实测：空 data-dir、无生产者配置、空 ref 目录三种情形都退 1（旧实现三种都退 0）；正常目录仍退 0 并打印「ref 9 张 + bronze 7 张」 | |
| 9 | 中 | 数据质量引擎按异常文本逐表跳过，跳过面不出现在结果里 | ✅ 已修（批次 B2）：被跳过的表收进清单，detail 里写出「已评估 N/M 张，跳过 [...]」，ERROR 级规则只要少查了表即判 FAIL。实测 A/B：给 VDQ-002 的目标表混入一张没有该规则列的 ref 表后，旧判据仍 PASS（整体退 0），新判据 FAIL「已评估 7/8 张，跳过 ['ref.ref_entity_hierarchy']」并退 1；正常跑 20 条规则全部覆盖 7/7、5/5、1/1，无一条缩水 |
| 10 | 中 | 7 条 Kafka 流共用同名临时视图，存在跨表串写风险 | ✅ 已修（批次 B1）：视图名按表派生（`staging_raw_<表>` / `staging_<表>`），MERGE 也用它。实测：7 条流整批跑完（重放 1505 行），逐表行数与 CSV 完全一致、无跨表行；并发重叠本身未在单机复现，属结构级修复 | |
| 11 | 中 | 回执被拒（REJECTED）时生成脚本仍返回 0 | ✅ 已修（批次 B2）：任一回执不为 `accepted` 即打印「报送未通过：N 个报送主体回执被拒」并退 1；台账仍登记 REJECTED（退回也是事实）。实测：用外部回执文件造 5 个主体全部被拒 → 退 1、台账 15 行全为 REJECTED；换回模拟回执再跑 → 退 0、台账回到 15 行 ACCEPTED |
| 12 | 中 | 导出作业的「表结构比对」被宽 except 吞掉，退化成「当作首次导出」 | ✅ 已修（批次 B2），并查出比原判更重的根因：`target_columns` 把 `properties` **按位置参数**传给了 `spark.read.jdbc`（第三位是 `column`），Spark 断言 `lowerBound can not be None when column is specified` 抛 AssertionError，正好被那个宽 `except` 吞成 `None` —— 也就是说结构比对**从来没有跑过**，不是「偶尔跳过」。修法：删掉宽 `except`，改先查存在性再读列，并把两处调用改成 `properties=properties` 关键字传参。判存在用 `pg_catalog` 而非 `information_schema`：实测无权限角色看 `information_schema.tables` 是 0 行、看 `pg_class` 是 1 行，用前者会把「表在但读不到」藏成「不存在」。实测：旧实现对**已存在**的表返回 None（复现「比对永远跳过」）；新实现 表存在可读 → 55 列、表不存在 → None、表存在但无权限读 → 抛异常（不再静默跳过）；导出环节整跑退 0 且三张表回读一致 |
| 13 | 中 | ADS 层「报表行数 = 交易实体数 + 1」的判据实际只要求 ≥ 2 行 | ✅ 已修（批次 B1）：改成三集合比对（报表实体 vs bronze 存款实体 vs ref 层级表实体）。实测：正常跑 PASS（4/4 实体 + 1 合并行）；突变探针让报表少一个实体 → FAIL「报表缺 ['ENT005']」退 1（旧的 ≥ 2 判据在 3 个实体时会放过） | |
| 14 | 中 | OWD 折算核对是 inner join，缺汇率的行不进校验却仍报通过 | ✅ 已修（批次 B1）：改以 ODS 上游为基准的 left join，统计 unjoined 并断言 `checked == upstream_rows`（join 同时补报告日，防多期行数相乘）。实测 A/B：同一份「JPY 汇率挂空」数据下，旧判据 82 行缺汇率仍 PASS，新判据 FAIL 退 1；正常跑缺汇率 0 条 | |
| 15 | 中 | 重述登记不校验版本区间方向，PG 侧也没有对应约束（与 owd_scd2 的处理不对称） | ✅ 已修（批次 B1）：关旧版本用 `greatest(date, begin_date)`、新版本生效日取旧版本生效日与传入值的较大者，被夹住时打印 `[WARN]`；库侧补幂等 `CHECK (end_date IS NULL OR end_date >= begin_date)`。实测：传入早于旧版本生效日的日期 → 告警并收口到 2026-09-17，版本历史无反向区间；直接插入反向区间行被数据库拒绝 | |
| 16 | 中 | time_travel 用 f-string 拼表名、快照号与键值，构成注入面 | ✅ 已修（批次 B2）：四个参数进 Spark 前白名单校验（表名「库.表」格式、列名标识符格式、快照号纯数字转 int），键值走 `sql_literal` 单引号翻倍转义，非法即退 2。实测：`--table "silver.owd_deposits; DROP TABLE silver.owd_deposits; --"`、`--key-column "id; --"`、`--diff 1 "2; DROP TABLE x"` 三种都退 2 并打印非法值原文；键值 `DEP-000001'; DROP TABLE silver.owd_deposits; --` 被当成普通字面量（无命中、退 0），冰表快照数不变 |
| 17 | 低 | CSV 导出把布尔列当金额格式化，与 XML 导出同一字段取值不一致 | ✅ 已修（批次 B1）：新增 `to_cell`（布尔先于数值判断）。实测：重新生成报送文件后 CSV 的 `is_consolidated` 为 `true`，XML 同字段 `consolidated="true"`，两格式一致 | |
| 18 | 低 | 明细回溯核对把 NULL 与 0 合并，两侧都为 0 时空集也算通过 | ✅ 已修（批次 B1）：删掉 `or 0` 兜底，命中零行、明细合计 NULL、报表侧 NULL 各自显式 FAIL。实测：正常跑 5 个 Section 全部给出真实数字；突变探针把 Section B 的行项目过滤条件写成不存在的值 → FAIL「过滤条件未命中明细表」退 1（旧实现会因 `or 0` 报 PASS） | |
| 19 | 低 | 巡检用 min(submission_status) 代表整批报送状态，REJECTED 会被 ACCEPTED 掩盖 | ✅ 已修（批次 B1）：改按状态分别计数 + `string_agg`，被拒文件写进巡检 findings。实测 A/B：台账里造 1 行 REJECTED 后，旧口径 `min()` 仍给 ACCEPTED，新口径给 `ACCEPTED/REJECTED` 并报「有 1 个文件回执被拒」；还原后台账回到 15 行 ACCEPTED | |
| 20 | 低 | bronze 层 DDL 注释与入湖实现不符（etl_load_timestamp 的语义已改成数据时间线） | ✅ 已修（批次 B1）：DDL 注释改为「按报告日 +1 天的 02:00 派生」，DAG 里 publish_access 的说明从 `DROP + CREATE` 改为 `truncate=true` 覆盖写（两处过时口径全仓清零） | |
| 21 | 低 | 衍生品盯市按交易币种折算，mtm_currency 字段被忽略（两条链路同源同错，无判据可发现） | ⬜ 待你定（证据已核，未动） | |
| 22 | 低 | DAG 与文档的链条描述漂移：漏项、缺项、各说一套 | ✅ 已修：docstring 链条补 publish_access / owd_scd2 / verify_rbac / verify_scd2 | |

<details><summary>展开：一、应用代码审查 的逐条证据与建议</summary>

**1. bronze 入湖 MERGE 主键缺 report_date：跨报告日重放会改写上一日快照，且行数核对抓不到**（高）
- 证据：python/consumers/kafka_to_iceberg.py:34-41 MERGE_TEMPLATE 的匹配条件只有两列：`ON target.source_system = source.source_system\n AND target.source_record_id = source.source_record_id`；而入湖表的自然键在三处被显式定义为三列：python/lakehouse/owd_scd2.py:64-66 `# 自然键：源系统 + 源记录号 + 报告日。同一源记录在不同报告日是两条独立的记录` / `KEY_COLUMNS = ("source_system", "source_record_id", "report_date")`；sql/iceberg/02_create_ods_tables.sql:10 `-- report_date 业务日期，同时作为分区键，保证按日重跑幂等`。bronze 表按天分区：sql/iceberg/02_create_ods_tables.sql `PARTITIONED BY (days(report_date))`。producer 每次重放整表：python/producers/replay_ods_to_kafka.py:41-47 `F.col("source_record_id").alias("key")`、python/alerts/realtime_scanner.py:34-37 亦记录 `core_banking_txns` 是每日重放。
- 为什么算问题：同一 source_record_id 在第二个报告日到达时，`WHEN MATCHED THEN UPDATE SET *` 会把既有行整体改写（含作为分区键的 report_date），旧的日报快照被原地吞掉，bronze 永远只有一份「最新报告日的当前值」。这不是假想：日批每个报告日都会重放同一批 ODS（deploy/server2/run-daily-pipeline.sh:47 ods-replay）。而唯一的行数闸 python/lakehouse/verify_bronze.py:56-64 只比「CSV 行数 == 表行数」，覆盖后行数依旧相等，因此静默通过。
- 建议：MERGE 的 ON 补上 `AND target.report_date = source.report_date`（与 owd_scd2.KEY_COLUMNS 对齐）；并在 verify_bronze 增加「bronze 的 report_date 去重集合 == {本批报告日}」的断言，否则覆盖写仍然无声。

**2. restate.py 的快照哈希两侧序列化不同，「内容未变」短路判据永不成立——每次都登记新版本**（高）
- 证据：写入侧取 PG 的 json 文本哈希：python/lakehouse/restate.py:83 `SELECT *, md5(row_to_json(t)::text) AS snapshot_hash`；比较侧取 jsonb 文本哈希：:109 `"SELECT record_version, begin_date, md5(snapshot::text) AS snapshot_hash "`、:116 `if existing["snapshot_hash"] == snapshot_hash:`（register 同形：:184、:195）。快照列类型为 jsonb：sql/postgres/10_control_tables.sql:175 `snapshot JSONB NOT NULL`，写入值是 Python json.dumps 文本：restate.py:158 `json.dumps(report, ensure_ascii=False, default=json_default)`。
- 为什么算问题：jsonb 落库时被归一化（键重排、`": "` 带空格），`snapshot::text` 与 `row_to_json(t)::text`（紧凑、列序）对任意多列行都不同串，因此两个 md5 永不相等。后果：capture 重复跑必然走 :121-126 的 `[WARN] ... 按 CORRECTION 登记新版本` 分支（本意是给「有人绕过重述流程改数」留痕，现在每次运维重跑都误报一次），register 也会把「数字没变、不构成重述」的正常重跑登记成 RESTATEMENT 并写 ads_restatement_log；版本号与重述次数随重跑次数单调增长。
- 建议：两侧用同一个表达式：比较侧也写 `md5(row_to_json(t)::text)`（或把写入值改存 `md5(snapshot::text)` 的派生值并在两处统一），并加一条单测：同一行两次计算必须相等。

**3. 报送台账唯一键含 file_hash：数据一变就多一行旧台账，verify_submission 之后永久失败**（高）
- 证据：python/exporters/generate_submission.py:246 `ON CONFLICT (report_date, entity_code, file_format, file_hash) DO UPDATE SET`（唯一约束同列：sql/postgres/10_control_tables.sql:136-137 `CONSTRAINT ads_fr2052a_submission_uk UNIQUE (report_date, entity_code, file_format, file_hash)`）；文件名固定不含哈希：:299-301 `("XBRL", output_dir / f"{report_id}.xbrl")` 等；逐行重算校验：python/validators/verify_submission.py:114-125 `actual_hash = sha256_of(path)` … `if actual_hash != row["file_hash"] or actual_size != row["file_size_bytes"]:` → `failures.append(...)`。
- 为什么算问题：重述/重跑后同一路径的文件被覆盖，内容哈希变了 ⇒ 插入新台账行，旧行仍在表里且 file_path 指向同一路径。verify_submission 会遍历该报告日的全部台账行并逐条重算哈希，旧行必然不匹配 → 退出 1 → fr2052a_submission DAG 的 verify_submission 任务永久红。脚本 docstring 自称「重复跑同一批不会堆重复行 —— 内容没变就只是刷新时间戳」，只在内容不变时成立。
- 建议：台账按 (report_date, entity_code, file_format) 唯一，ON CONFLICT 更新 file_hash/file_size；历史版本另存一张审计表。或至少让 verify_submission 只核对每 (report_id, file_format) 的最新一条。

**4. 日批 DAG 把 export_pg 排到 publish_access 之前，与跑批脚本和接口文档规定的顺序相反**（高）
- 证据：deploy/server1/airflow/dags/fr2052a_daily_batch.py:166-170 `>> owd_scd2 >> dq_validate >> export_pg >> publish_access >> liquidity_monitor`；而同一环节的脚本 STEPS 顺序是 publish-access 在 export-pg 之前：deploy/server2/run-daily-pipeline.sh:53-55 `dq-rules` / `publish-access` / `export-pg`，且 :74 `[publish-access]="发布库对象：先于导出施加迁移与授权，并核对无零授权对象"`、:122 `# 必须在 export-pg 之前：先施加结构迁移与授权，导出用 truncate=true 保住它们。`；接口契约同样如此：docs/business/INTERFACE-DESIGN.md:219-220 `→ publish_access 施加库侧迁移与授权` / `→ export_pg 导出到报送服务层`。
- 为什么算问题：DAG 的每个 task 只跑单个环节，所以 DAG 路径上顺序由 DAG 自己决定：它在灌数之后才施加结构迁移与授权。按 export_gold_to_pg 自己的设计，模型多一列时导出会以「需要先写迁移」失败并中止（python/exporters/export_gold_to_pg.py:114-124），此时 publish_access 还没跑，唯一能补迁移的环节被卡在失败任务之后 —— 契约变更后 DAG 路径会死锁在人工介入。这条也违反该 DAG docstring 自称的「编排逻辑只有那一份脚本，DAG 不复制命令」。
- 建议：把 publish_access 提到 export_pg 之前（交换两个 task 的依赖），或让 DAG 直接调 `run-daily-pipeline.sh publish-access export-pg` 一条命令，把顺序交回脚本单源。

**5. 「确认 7 张 ODS 源文件全部到位」的任务恒成功，判据从未生效**（高）
- 证据：deploy/server1/airflow/dags/fr2052a_daily_batch.py:72-76 `check_source_arrival = ssh_task("check_source_arrival", f"ls -1 {REMOTE_APP_DIR}/sample_data/ods/*.csv | wc -l", "确认 7 张 ODS 源文件全部到位，避免空跑一整轮")`。本机实测（read-only）：`ls -1 /nonexistent-dir/*.csv | wc -l` 输出 `0` 且退出码为 0。
- 为什么算问题：退出码取的是管道末端 `wc -l` 的状态，glob 无匹配时 `ls` 报错也不影响；而且即使成功，产出的数字也没有与 7 比较。源文件缺失时该任务会打印 0 并成功，后面的环节照常起跑 —— 正是它声称要避免的「空跑一整轮」。
- 建议：改成可失败的判据，例如 `n=$(ls -1 ... | wc -l); echo $n; [ "$n" -eq 7 ]`，或对 7 个文件名逐个 `test -f`。

**6. 报送放行闸只校验熔断闸行存在，不校验本报告日是否跑过预警判定——新报告日可静默放行**（高）
- 证据：python/validators/check_submission_gate.py:57-64 `SELECT state, reason, trip_count, updated_at FROM ads.ads_circuit_breaker WHERE scope = %s` + `if breaker is None: ... return EXIT_UNKNOWN`；阻断预警的查询带报告日：:68-74 `FROM ads.ads_fr2052a_alerts WHERE report_date = %s AND status = 'OPEN' AND blocks_submission`；放行条件：:83-85 `if state == "OPEN" and not blocking: ... return EXIT_PASS`。熔断表按 scope 唯一、与报告日无关：sql/postgres/10_control_tables.sql:88 `scope TEXT PRIMARY KEY`；脚本 docstring 自称「monitor 挂掉或漏判时不会静默放行（闸读不到状态就报错退出，而不是当成通过）」。
- 为什么算问题：breaker 行由任意历史报告日的 liquidity_monitor 写入（python/alerts/liquidity_monitor.py:399-429 只按 scope upsert，state 由本轮 alerts 决定）。因此只要曾经跑过任何一天并留下 OPEN 行，对某个从未判定过的报告日执行闸，会得到「0 条阻断预警」→ 退出 0 放行。「本报告日没有预警」与「本报告日的判定根本没跑」在闸眼里等价，正好是闸的 docstring 声称要防的那种静默。
- 建议：闸除读 breaker 外，再断言本报告日至少存在一行判定痕迹（例如 ads.ads_liquidity_metrics 有该 report_date 的行，或 ads_fr2052a_validation_log 有该批次的记录），缺失即退 3。

**7. 实时扫描丢弃消息自带的 report_date，改用命令行常量；与日批每日重放叠加会撞事件表主键**（中）
- 证据：python/alerts/realtime_scanner.py:63-66 解析 schema 含 `report_date STRING`；:132 又把它覆盖掉 `.withColumn("report_date", F.to_date(F.lit(report_date)))`；event_id 由此拼出：:121-131 `F.md5(F.concat_ws("|", F.lit(ALERT_CODE), F.col("source_record_id"), F.lit(report_date)))`；表主键：sql/postgres/10_control_tables.sql:69 `event_id TEXT PRIMARY KEY`；写库方式为 append：realtime_scanner.py:216 `candidates.write.jdbc(..., mode="append", ...)`；日批每个报告日重放同一批 ODS：deploy/server2/run-daily-pipeline.sh:47 `ods-replay`。
- 为什么算问题：消息里那个权威的 report_date 被常量取代，于是同一笔源记录在不同报告日算出同一个 event_id：第二次消费到同一 source_record_id 时（日批重放是设计内的常规动作）append 直接撞主键，实时扫描从此每个批次失败；反过来若把 CLI 报告日改一下，又会把同一笔敞口重复记一条。docstring 只把「删 checkpoint 重放」列为有意的失败，没覆盖「日批每日重放」这条常规路径。
- 建议：event_id 用 payload 里的 report_date（`F.col("report_date")`）而不是 CLI 常量，或在 event_id 里加入 Kafka 摄入日/批次标识；同时把 CLI 参数降级为记录范围过滤。

**8. 三处核对脚本在「一个对象都没核对到」时报告通过（零命中与读不到不可分辨）**（中）
- 证据：python/lakehouse/verify_ods_schema.py:71-95 `checked = 0` 起始、循环体只遍历 `sorted((data_root / subdir).glob("*.csv"))`，尾部 `if failed: return 1` / `print("表结构与 CSV 表头全部一致")` 后 `return 0`，全程没有 `checked == 0` 的断言；python/lakehouse/verify_bronze.py:45 `targets = [topic for topic in config["topics"] if topic.get("has_producer")]`、:53-72 循环为空时直接 `print("bronze 层与样本数据行数一致")`、`return 0`；python/lakehouse/load_ref_tables.py:64-81 `for csv_path in sorted(ref_dir.glob("*.csv"))`，无文件时打印 `加载完成：成功 0 张，失败 0 张` 并 `return 0`。项目自身把这条列为验收标准：docs/business/KNOWN-ISSUE.md:59 `核对脚本要能区分「零命中」与「读不到」`。
- 为什么算问题：配置键改名、data-dir 传错、CSV 未同步都会让核对对象集合变成空集，闸以 0 项失败收场并打出「全部一致」，与「真的都一致」在输出与退出码上完全一样。
- 建议：三处都加前置断言：核对对象数为 0（或与期望集合不等）即退出 1，并把「期望 N 个/实际核对 M 个」打进输出。

**9. 数据质量引擎按异常文本逐表跳过，跳过面不出现在结果里**（中）
- 证据：python/validators/run_dq_rules.py:117-125 `for table in targets: try: violations = spark.sql(...)` / `except Exception as error:` / `if "UNRESOLVED_COLUMN" in message or "cannot be resolved" in message: continue` / `evaluated += 1`；只有全部表都跳过才记 SKIPPED：:162-176 `if evaluated == 0: ... check_result="SKIPPED"`；其余情况记 PASS：:201 `check_result="PASS" if passed else "FAIL"`。
- 为什么算问题：「部分表被跳过」这一事实既不进 detail（:202 只列违规表名），也不影响 PASS/FAIL。列名漂移（例如某层把 currency 改成 currency_code）会让该规则悄悄从 5 张表缩到 3 张表，输出仍是 PASS；这与 ref_data.py:224-225 注释申明的「规则与实现两张皮的话……等于没校验」正相反，恰恰是这一类失效。
- 建议：把跳过的表收集起来（skipped_tables），在 detail 里显式写出「已评估 x/N 张，跳过 [..]」，并对 ERROR 级规则要求覆盖数不低于声明目标数。

**10. 7 条 Kafka 流共用同名临时视图，存在跨表串写风险**（中）
- 证据：python/consumers/kafka_to_iceberg.py:77 `valid.createOrReplaceTempView("staging_raw")`、:78-89 `spark.sql("CREATE OR REPLACE TEMPORARY VIEW staging AS ...")`（视图名与表无关，是全局常量）；启动方式为 7 个 query 先全部 start()、再统一等待：:141-152 `query = (...).start()` / `queries.append((topic, query))`、:154-155 `for topic, query in queries: query.awaitTermination()`。
- 为什么算问题：同一 SparkSession 里 7 条流各自在自己的驱动线程上调用 write_batch，而 staging_raw/staging 是会话级共享名字：A 表的分批视图可能被 B 表的批覆盖，A 的 MERGE 于是读到 B 的数据（或反之）。这是「顺序与并发假设」类缺陷，只在并发批重叠时出现，报错形态是数据串表而非报错。
- 建议：视图名带上表名（f"staging_raw_{table}" / f"staging_{table}"），或把整批用一个 DataFrame 直接做去重+dropDuplicates 后写 MERGE，不走临时视图。

**11. 回执被拒（REJECTED）时生成脚本仍返回 0**（中）
- 证据：python/exporters/generate_submission.py:322 `"submission_status": "ACCEPTED" if receipt.get("accepted") else "REJECTED"`；函数收尾 :345 `return 0`（无任何按状态的分支）；调用链只认退出码：deploy/server2/run-daily-pipeline.sh:143-147 `check_submission_gate ... && ./venv/bin/python .../generate_submission.py ...`。
- 为什么算问题：上游把「环节退出码 = Task 成败」当作机器保障（fr2052a_daily_batch.py:15-16）。回执为 REJECTED 意味着监管未接收，但台账写了、退出码 0、DAG 全绿；后续 verify_submission 只核哈希与文件存在（python/validators/verify_submission.py:114-129 只把状态打印出来），也不会把它判成失败。
- 建议：至少按状态给非 0 退出码（例如 REJECTED → 返回 1），或在 verify_submission 里把 submission_status<>'ACCEPTED' 记为失败项。

**12. 导出作业的「表结构比对」被宽 except 吞掉，退化成「当作首次导出」**（中）
- 证据：python/exporters/export_gold_to_pg.py:81-86 `def target_columns(...): try: return spark.read.jdbc(url, f"(SELECT * FROM {table} WHERE 1 = 0) AS probe", properties).columns` / `except Exception:  # noqa: BLE001 - 表不存在是正常情况（首次导出）` / `return None`；调用点据返回值跳过结构比对：:112-124 `existing = target_columns(...)` / `if existing is not None: ... if missing or extra: ... continue`。
- 为什么算问题：「表不存在」与「表存在但读不到」（权限不足、连接瞬时失败、catalog 未刷新）被映射成同一个 None。后者会让下面这层防漂移/防元数据丢失的检查整体跳过，直接进入 `mode("overwrite").option("truncate","true")` 的数据覆盖。文件头大段论证的正是「不能让表结构漂移静默通过」，实现却留了一个能整体关闭它的入口。
- 建议：区分异常类型（UndefinedTable 才算首次），其他异常直接抛出；或先查 information_schema 判断表是否存在，再决定走比对还是首次导出。

**13. ADS 层「报表行数 = 交易实体数 + 1」的判据实际只要求 ≥ 2 行**（中）
- 证据：python/lakehouse/verify_gold.py:57（docstring）`核对报表行形状：每个实体一行，合并行只有一行`；:59-64 `entity_rows = ...filter("not is_consolidated").count()` / `consolidated_rows = ...` / `passed=entity_rows >= 2 and consolidated_rows == 1`。
- 为什么算问题：交易实体是 4 个（python/generators/ref_data.py:121 `TRADING_ENTITIES = ("ENT002", "ENT003", "ENT004", "ENT005")`，报表模型按 owd_deposits 的 distinct entity_code 生成行：dbt/models/marts/ads_fr2052a_report.sql:125-129）。若某个实体在某轮里整体丢失（上游过滤错、join 错），只要还剩 2 个实体行，闸照旧 PASS —— 判据比它自己声明的弱一个量级。
- 建议：与 ref.ref_entity_hierarchy（is_active 且参与记账的实体集）比对实体集合，或断言 entity_rows 等于期望实体数（可由 ref 推出而不是硬编码）。

**14. OWD 折算核对是 inner join，缺汇率的行不进校验却仍报通过**（中）
- 证据：python/lakehouse/verify_silver.py:94-111 `from {owd_table} o join {ods_table} s on s.source_system = o.source_system and s.source_record_id = o.source_record_id join ref.ref_exchange_rates f on f.from_currency = s.currency and f.rate_date = s.report_date`；判据 :112-118 `checked = row["checked"]` / `passed=checked > 0 and mismatched == 0`。
- 为什么算问题：join 不上的行（币种在 ref_exchange_rates 里没有当日汇率，正是最该报的情况）会从分母里消失；只要还有一条 join 上就 `checked > 0`，于是「覆盖率 99% 折算正确」与「全量折算正确」结果相同。注意这不是理论担忧：dbt 侧同样的 join 是 left join（dbt/models/staging/owd_deposits.sql:49-51），漏配汇率时 OWD 的 *usd 列会整行为 NULL，而核对脚本看不到这些行。
- 建议：同时统计未 join 上的行数（`count(*) from {ods_table} s left join ... where f.spot_rate is null`），非零即 FAIL；并把 checked 与本层行数比较。

**15. 重述登记不校验版本区间方向，PG 侧也没有对应约束（与 owd_scd2 的处理不对称）**（中）
- 证据：python/lakehouse/restate.py:136-141 `"UPDATE ads.ads_fr2052a_report_history " "SET end_date = date %s, is_active = false " "WHERE report_id = %s AND is_active"`（register 同形：:207-212），end_date 直接取 effective；对照 python/lakehouse/owd_scd2.py:262-267 `-- 但不早于该版本自己的生效日 ... greatest(date_sub(date '{effective_date}', 1), a.begin_date) AS end_date` 与写完自检 :293-297；库侧约束只管冗余列一致：sql/postgres/10_control_tables.sql:181-182 `CONSTRAINT ads_fr2052a_report_history_active_ck CHECK ((end_date IS NULL) = is_active)`。
- 为什么算问题：owd_scd2 的踩坑纪录（docs/business/KNOWN-ISSUE.md #scd2-reversed-interval：反向区间导致按日期过滤的查询静默漏数）说明这类数据是真实会发生的；restate 是同一语义的第二个实现，却既无兜底也无自检，PG 又能存下 end_date < begin_date 的行。两个调用方生效日约定一旦不一致（重述传处理日、日批传报告日），报表版本历史就会长出反向区间，且没有任何闸会报。
- 建议：restate 写入前校验 effective >= begin_date（或同样取 greatest），并在 sql/postgres 补 `CHECK (end_date IS NULL OR end_date >= begin_date)`。

**16. time_travel 用 f-string 拼表名、快照号与键值，构成注入面**（中）
- 证据：python/audit/time_travel.py:73-74 `keys_a = spark.sql(f"SELECT {args.key_column} AS k FROM {table} VERSION AS OF {snapshot_a}").cache()`；:97-101 `SELECT * FROM {table} VERSION AS OF {snapshot["snapshot_id"]}` / `WHERE {args.key_column} = '{args.trace_key}'`；参数全部来自 argparse 且无格式校验：:36-45 `parser.add_argument("--table", required=True, ...)` / `--trace-key` / `--key-column`。
- 为什么算问题：trace-key 是业务键值，被单引号包裹后直接进 SQL；键值里带单引号（或人为构造）即可改变语句结构。table/key-column 同样未做白名单校验，拿到 shell 调用权的人可让审计脚本对任意可解析表执行语句。红线「凭证、endpoint、路径与标识不得硬编码/不得裸拼」在此处失守。
- 建议：table/key_column 用正则白名单（^[a-z_]+\.[a-z_]+$）；trace_key 用参数化（Spark SQL 不支持绑定参数时用 `WHERE ... = '...'` 前先做单引号转义 / 改为 DataFrame API 的 filter）。

**17. CSV 导出把布尔列当金额格式化，与 XML 导出同一字段取值不一致**（低）
- 证据：python/exporters/generate_submission.py:122-126 `to_amount(row[name]) if isinstance(row[name], (int, float, Decimal)) else (row[name] or "")`（bool 是 int 子类，会走 to_amount → `f"{Decimal(value):.2f}"`，:109-113）；XML 分支对同一字段单独处理：:140 `"consolidated": "true" if row["is_consolidated"] else "false"`。
- 为什么算问题：同一份报表的 is_consolidated 在 XML 里是 true/false，在 CSV 里变成 1.00/0.00，人工复核与差异比对时两种格式无法逐字段对齐；也把「金额两位小数」这条格式化约定用在了非金额列上。
- 建议：先判 bool 再判数值（`isinstance(x, bool)` 分支输出 true/false），或按列类型清单决定格式化方式而不是按 Python 类型猜。

**18. 明细回溯核对把 NULL 与 0 合并，两侧都为 0 时空集也算通过**（低）
- 证据：python/lakehouse/verify_gold.py:100-107 `detail_total = (...).collect()[0]["total"] or 0` 与 `report_total = consolidated[report_column] or 0`，随后 :108-113 `variance = round(float(detail_total) - float(report_total), 2)` / `passed=abs(variance) <= 0.05`。
- 为什么算问题：sum 无匹配行时是 NULL（例如 section/line_item 过滤条件与明细模型的实际取值不一致），被 `or 0` 变成 0；只要报表侧该列也是 0（Section F/J 在小实体上就可能全为 0），差异 0 → PASS。判据的语义是「明细合计 = 报表合计」，实现却包含「查不到 = 没有」这一跳。
- 建议：对 NULL 单独处理：`if detail_total is None: 记为 FAIL 并说明过滤条件未命中`；或先断言该 section 在明细表里存在行。

**19. 巡检用 min(submission_status) 代表整批报送状态，REJECTED 会被 ACCEPTED 掩盖**（低）
- 证据：python/governance/pipeline_health.py:90-95 `"SELECT report_date, count(*) AS files, " "min(submission_status) AS status, max(submitted_at) AS submitted_at " "FROM ads.ads_fr2052a_submission GROUP BY report_date ORDER BY report_date DESC LIMIT 1"`，随后 :218-221 直接打印该 status 作为「状态」。
- 为什么算问题：min 对文本取字典序最小值，'ACCEPTED' < 'REJECTED'，因此一批里只要有一个文件回执成功，巡检就报 ACCEPTED，被拒的文件不出现在巡检结论里；而巡检恰好是唯一会跨批次汇总报送状态的观测入口。
- 建议：改为分别统计 `count(*) FILTER (WHERE submission_status='REJECTED')` 等，或 `string_agg(DISTINCT submission_status, '/')`。

**20. bronze 层 DDL 注释与入湖实现不符（etl_load_timestamp 的语义已改成数据时间线）**（低）
- 证据：sql/iceberg/02_create_ods_tables.sql:9-10 `-- 列序与 python/generators 产出的 CSV 表头逐列一致，多出的 etl_load_timestamp` / `-- 由入湖作业在写入时用 current_timestamp() 补齐。`；实际实现按数据时间线派生：python/consumers/kafka_to_iceberg.py:136-143 `# 入湖时间按数据自身的时间线打标（报告日 T+1 凌晨 2 点），而不是真实时钟：` / `load_timestamp = F.to_timestamp(F.concat(F.date_add(F.col("report_date"), 1), F.lit(" 02:00:00")))` / `.withColumn("etl_load_timestamp", load_timestamp)`。
- 为什么算问题：表定义文件是本项目声明的结构真源（AGENTS.md §9 SQL DDL「真源」）。注释留着一句被推翻的旧口径，下一个人排查 VDQ-016（T+1 08:00 时效规则）时会按注释去找 current_timestamp() 的行为，得出与实际相反的结论。属于「注释与实现不符」，也是变更未同步三处之一（另一处：fr2052a_daily_batch.py:125 的 publish_access 说明仍写 `Spark 的覆盖写会 DROP + CREATE 报表表`，而导出早已显式 truncate=true，见 python/exporters/export_gold_to_pg.py:126-128）。
- 建议：把注释改成「由入湖作业按报告日 +1 天 02:00 派生」，并同步修正 DAG 里那句 DROP + CREATE 的旧描述。

**21. 衍生品盯市按交易币种折算，mtm_currency 字段被忽略（两条链路同源同错，无判据可发现）**（低）
- 证据：生成器把 mtm_currency 固定写成 USD：python/generators/ods_data.py:383-384 `round(rng.uniform(-10_000_000, 10_000_000), 2),` / `"USD",`（对应列 `mark_to_market` / `mtm_currency`，见 :401-402）；而 GL 倒推时按行上的交易币种再折算一次：:477-488 `sum(_amount_usd(row, "mark_to_market") ...)`，:422-424 `return float(row[amount_column]) * FX_RATES.get(row["currency"], 1.0)`；dbt 侧同法：dbt/models/staging/owd_derivatives.sql:47 `round(d.mark_to_market * f.spot_rate, 2) as mtm_value_usd`（join 条件用 `f.currency_code = d.currency`，:68-70）。
- 为什么算问题：既然 ODS 明确声明该金额的币种是 USD（mtm_currency），再乘交易币种汇率就是把已折算的金额二次折算：`currency` 为 JPY 的交易会被乘 0.0067、EUR 会被乘 1.08。后果是 Section H（净盯市）与 GL 科目 1500/2200 同时被同一口径污染，GL 对账（dbt/models/marts/ads_gl_reconciliation.sql:89-99 与 42-48 两侧都读 mtm_value_usd 同源）仍然全 PASS，任何现有闸都抓不到。要么 mtm_currency 这一列标错，要么折算方式错，两者必有一错。
- 建议：二者取一：把生成器的 mtm_currency 改成与 `currency` 同值（表示盯市为原币金额），或让生成器/owd_derivatives 不再乘汇率（表示盯市已是 USD）。选定后在 verify_silver 加一条「金额币种与折算所用汇率取自同一列」的一致性检查。

**22. DAG 与文档的链条描述漂移：漏项、缺项、各说一套**（低）
- 证据：deploy/server1/airflow/dags/fr2052a_daily_batch.py:5-7（模块 docstring）`check_source_arrival → load_ref → replay_ods → load_bronze → dbt_run` / `→ pii_vault → lineage → owd_scd2 → dq_validate → export_pg → liquidity_monitor` / `→ verify_bronze → verify_silver → verify_ads → pipeline_health`，实际依赖链 :158-176 还包含 publish_access（:169）与 verify_rbac（:174）；脚本侧另有 verify-scd2 环节（deploy/server2/run-daily-pipeline.sh:59）而 DAG 里没有对应 task；docs/business/INTERFACE-DESIGN.md:210-224 的链条又与两者都不同（含 publish_access 顺序、不含 owd_scd2）。
- 为什么算问题：三处「链条」各自被当作此环路的说明使用（docstring 是新人第一读物、接口文档是契约、脚本是执行体），任何一处与实际依赖不符都会让下一次改动按错的图施工；这也是本项目历史上踩过的坑（同日批「分环节调度未生效」同类，见 fr2052a_daily_batch.py:11-14）。
- 建议：以 run-daily-pipeline.sh 的 STEPS 为唯一链源，DAG docstring 与 INTERFACE-DESIGN.md 改为引用它（或由脚本 `--list` 生成），并在 CI 里比对三者的环节集合。

</details>

**子代理声明未验证的点**：所有服务器运行态：本次只读本地仓库，未启动 Spark/PG/Kafka/Airflow，未执行任何 pipeline，所有「是否已经发生」的判断（例如 bronze 是否已被第二个报告日覆盖、ads_fr2052a_submission 是否已积累旧哈希行、report_his；restate.py 的快照哈希不等价：结论基于 PG 的 jsonb 文本输出会归一化（键序/空格）这一定义，本机无 PostgreSQL，未实跑两条 md5 比对确认两条序列化文本必然不同；deploy/server1/airflow/dags/*.py 的调度行为（06:30/07:00/15 分钟等 schedule 是否按声明生效、SSHOperator 的 cmd_timeout 与远端 shell 是否带 pipefail）：Airflow 未运行，che；pipeline_health.collect_kafka 对 kafka-consumer-groups.sh 输出的解析（表头/空行分块、lag 取第 6 列）未针对真实输出实测；实时扫描与入湖作业的 checkpoint 目录挂载（/opt/fr2052a-checkpoints 同时挂到 spark-master 与 worker）未验证；dbt 目标 schema（silver./gold. 命名）与列名已按 dbt/models 静态核对过一致，但未跑 dbt 验证实际物化结果；verify_* 脚本的表名依赖该约定；本地 sample_data/ 与服务器 /opt/fr2052a-app/sample_data 是否同一版未验证（本次用它复算生成器自检：39 项全通过、7 张 bronze 表列序与 CSV 表头逐列一致、行数全部吻合，结论仅对本地副本成立）；liquidity_thresholds.json 里「各实体实测 LCR 约 4.4 至 14.8 倍」的数字未复算（需 dbt 产物）；run_sql_file.py 的分号切分限制（docstring 自陈）未逐文件核对 sql/iceberg/*.sql 是否存在字符串字面量含分号或 --；sql/iceberg 下的迁移文件 05/06/07 为一次性脚本，是否已在目标环境执行过未知

## 二、dbt 与 SQL 口径审查（26 条）

| # | 严重度 | 发现 | 本轮处置 |
|---|---|---|---|
| 1 | 高 | 30 天预期流出漏掉全部活期/储蓄存款（非定期存款 bucket 落 OPEN 被报表过滤） | ✅ 已修（批次 C4a）：新增 `behavioral_bucket` 宏，无到期日的活期/储蓄按行为口径归 `O/N`（原来落 `OPEN`，被 30 天窗口 `where maturity_bucket in ('O/N','1-7D','8-30D')` 过滤掉，等于全部活期存款不进流出）。规则只写在宏一处，`owd_deposits` 与 `verify_silver` 的分桶核对共用同一份定义。实测（两期样本，Server 2）：PG `ads.ads_fr2052a_report.sec_k_total_outflows` 与独立复算逐期逐实体一致（差 ≤ 0.05）—— 独立脚本直接读 `sample_data/ods/ods_deposits.csv` 与 `ref/ref_behavior_assumptions.csv`，按流失率算出活期/储蓄 30 天流失额（2026-09-16：合并口径 58,054,209.10，ENT004 23,168,796.18、ENT002 15,022,186.82），这些金额现在都在流出合计里 |
| 2 | 高 | GL 对账 Section E 是自比对（两边同读 owd_gl_entries 的 1001/1100），恒 PASS 且零信息量 | ✅ 已修（批次 C6）：Section E 的基准侧换成**司库现金头寸**（新增 ODS 表 `bronze.ods_treasury_cash_position`：库存现金盘点数 + 各代理行对账单余额 + 未达账项，来源系统 `TREASURY_SYS`），与报送侧的账面口径分属两个来源；`gl_amount` 改名 `benchmark_amount`，新增 `benchmark_source` 与 `reconciling_item_usd` 两列，判定式改为 `基准 + 调节项 − 报送`；对账单口径的容差收到 1 分钱（旧 1% 会放行占该 Section 0.3% 的错误）。三条反向实测：① 改坏一个代理行对账单余额 +100,000 → 该实体行与合并行双双 FAIL（旧容差下这个量级会被放行）；② 把基准改回总账 → 对账 12 行**全 PASS**、缺陷完全隐形，只有新断言 `assert_recon_benchmark_independent` 报 FAIL 12 行；③ 把数字凑成同值、调节项置零 → 该行 PASS 而断言 FAIL 并给出「基准与报送同值」。两期样本整条日批 18/18 全绿、`dbt test` 33/33、`verify-ads` 26 项通过。**补注（批次 C3-2a）**：对账模型已重构为「按报告期 × 视角对账」（法人单体行对各自总账、合并行对 `Σ 实体总账 − 集团内往来`），但当时 Section E 两侧仍同读总账 1001/1100 —— 该次未动，本批消除 |
| 3 | 高 | 覆盖写（truncate=true）的前置闸只查了表结构，不查业务前提；且 Airflow DAG 把 publish-access 排在 export-pg 之后，与脚本声明的前置条件相反 | ⬜ 待你定（证据已核，未动） |
| 4 | 高 | 报表与对账模型都不按 report_date 限定：多报告期共存时金额跨期串加、合并行翻倍 | ✅ 已修（批次 C2）：报表 8 个口径 CTE 全部改为按 `(report_date, entity_code)` 分组、8 个 `left join` 补报告日匹配、合并行由 `max(report_date)` 改为按报告日分组；对账模型删掉单行日期构造（`max` + `cross join`），改为两边按期对上。生成器补 `--report-days N` 与按(表名,报告日)派生的随机源，使「加期不扰动已有期」成立。实测：1 期与 2 期输出的同一报告日逐字节一致、2 期行数 = 2 倍；上线跑整条日批 18/18 全绿，改造前快照的 PG `ads` 层 13 张表里报表/明细/对账/台账/版本历史/重述记录**内容 md5 逐字节相同**（另 5 张仅运行时刻戳变化，非时刻戳字段逐一对齐基线） | |
| 5 | 高 | ENT001 同时是真实法人实体与集团合并行代码，主键 (report_date, entity_code) 不含口径，无法承载声明的粒度 | ✅ 已修（批次 C3-1 数据侧 + C3-2a 模型侧）：合并行改用保留码 `GRP001`（不代表任何法人实体）；口径码扩成三档 `01` 全球合并 / `02` 法人实体单体 / `03` 母公司单体，由 `is_consolidated` + `entity_code` 推出；母公司 ENT001 开始记自己的账（小账，与第三方敞口一并生成），因此「母公司单体」这一档有真实数据而不是空行。实测（Server 2，两期数据）：报表每期 6 行 = GRP001(01) + ENT001(03) + 四家子公司(02)，`ads_liquidity_metrics` 主键不再有撞键可能 |
| 6 | 高 | 三张报表表（report/detail/gl_reconciliation）全库无主键或唯一约束，且注释声称存在的『补键列 DO 块』在本仓 sql/ 中并不存在 | ✅ 已修（批次 C5）：用幂等 DO 块补三条唯一约束（判存在全走 `pg_catalog`）—— `ads_fr2052a_report(report_id)`、`ads_fr2052a_detail(report_date, entity_code, is_intracompany, section_code, line_item, product_category, counterparty_type, currency_code, maturity_bucket)`、`ads_gl_reconciliation(report_date, entity_code, section_code)`。明细的键先按模型逐个 CTE 的 group by 核对过（五个 CTE 的分组维度都落在这九列里），不是照抄。实测：`pg_constraint` 列出三条约束名；三条负向探针各插一行重复数据全部被拒（报错带约束名与重复键值），行数保持 12 / 1220 / 96 未被污染 |
| 7 | 高 | 汇率 LEFT JOIN 无缺行保护：折算失败时金额静默变 NULL，而 ref 汇率表只有报告日一行 | ✅ 已修（批次 C1）：OWD 的 join 形态不动，改为**在批次级把缺汇率变成失败** —— 新增 singular test `dbt/tests/assert_fx_covered.sql`（ODS 六张明细表出现的（报告日, 币种）缺 MID 汇率即失败），并让 `dbt-run` 环节在建模后跑断言、不过即整条日批红。同时把「汇率与数据自洽」写成契约 + 生成器自检项（比对写出的汇率 CSV），新增 `--inject-missing-fx` 让缺汇率只能来自「故意制造的缺陷数据」。实测：注入 JPY 后探针确认缺陷状态（汇率表 9 币种无 JPY、bronze 有 82 行 JPY），同态跑日批断言 ERROR=1、环节退 1；恢复正常数据后退 0。留存问题（下一批处理）：汇率表仍只有报告日一天，多报告期需要 C2/C4 一起把日期维度补上 |
| 8 | 中 | stg_fx_rates 未限定 to_currency='USD'，OWD 各表 join 只用 currency，多目标币种时会行放大 | ✅ 已修（批次 C1）：`stg_fx_rates` 加 `and to_currency = 'USD'`，注释写明多目标币种会行放大、本项目只折 USD；生成器侧汇率行的折算目标恒为 USD |
| 9 | 中 | 行为假设 join 缺 customer_segment，且未命中时静默套用 10% 默认流失率 | ✅ 已修（批次 C4a）：① join 补 `customer_segment`；② `ref_behavior_assumptions` 从 6 行手工样本改为按规则派生的**全覆盖矩阵**（4 产品类别 × 5 客户分段 × 到期桶 = 80 行，流失率 = 基础率 × 分段系数 × 分桶系数，夹在 [0,1]）；③ 删掉 `coalesce(a.runoff_rate, 0.1)` 静默兜底。新增两道覆盖闸：生成器自检 `check_behavior_coverage`（生成阶段，没有 Spark 也能拦）+ `dbt/tests/assert_behavior_covered.sql`（转换后，返回行即失败；`dbt-run` 环节串跑断言，不过则整条日批红）。实测：正常数据生成器自检 PASS「ODS 探查组合 3656，假设表行数 80，全部命中」；A/B 探针删掉 `(DEMAND, RETAIL, O/N)` 一行 → FAIL「缺 1 个组合，例如 [('DEMAND', 'RETAIL', 'O/N')]」 |
| 10 | 中 | is_encumbered 为 NULL 时三层三种处理方式，导致 Section I（非受限+受限）加不回 Section G | ✅ 已修（批次 C5）：`owd_securities` 统一成布尔 `coalesce(pledged_flag = 'Y', false)` —— Spark 里 `pledged_flag = 'Y'` 遇 NULL 返回 NULL 而非 false，三层各判一套的结果是 NULL 行同时掉出「非受限」与「已受限」两侧。口径定为「只有明确 Y 才算受限，NULL 按未受限计」，源系统缺报另由新增 **VDQ-021**（ODS / WARNING）报出来，生成器留 4% 缺报以形成真实场景。实测：VDQ-021 报 `bronze.ods_securities:17 行违规`（WARNING 不阻断环节，日批仍 18/18 绿）；PG 恒等式「非受限四项 + 已受限 = Section G 合计」**10/10 实体行成立** —— 若仍按三值逻辑处理，这 17 行两边都不计、恒等式不成立 |
| 11 | 中 | 受保存款限额在『原币』上截断 25 万美元，外币存款的受保金额量级错误 | ✅ 已修（批次 C4a）：截断改在折算之后 `least(principal_amount * spot_rate, 限额)`，并且限额按**「客户 × 法人实体」聚合后**截断（客户受保总额超限时，各笔按占该客户受保总额的比例等比分摊）—— 逐笔截断等于给同一客户的多笔存款各发一次额度。限额单源进 `dbt_project.yml` 的 `deposit_insurance_limit_usd`，新增 `dbt/tests/assert_insured_within_limit.sql` 守「同一客户受保合计 ≤ 限额」。实测：独立复算（读 CSV，按客户聚合、按报告日汇率折算）与 PG `sec_c_insured_total` 十行逐实体一致（差 ≤ 0.05）；样本里有 3 个客户受保总额超限且持多笔，逐笔口径会比按客户口径多算 532,555.04 —— 若实现成逐笔，本次比对会不一致 |
| 12 | 中 | insured_amount_usd 全仓无消费者，而 marts/schema.yml 声称 sec_c_total 已按受保限额截断 | ✅ 已修（批次 C4a）：报表新增 `sec_c_insured_total`（受保存款合计）让这一列有真实消费者，`dbt/models/marts/schema.yml` 的描述改成与实现一致：`sec_c_total` 是**全额**、受保部分单列。PG 侧走显式迁移加列（判存在用 `pg_catalog`）。实测：PG 报表该列存在，值与独立复算逐实体一致 |
| 13 | 中 | Section E 列名与语义错配：库存现金映射到 sec_e_central_bank_dep，同业存放列整列丢失，total_cash 重复计两次 | ✅ 已修（批次 C4a）：库存现金（总账 1001）与同业存放（1100）各自成列 —— `sec_e_cash_on_hand` / `sec_e_due_from_banks`，央行存款列 `sec_e_central_bank_dep` 保持 NULL 并在模型注释里写明「本演示总账里没有央行准备金科目，NULL 表示无此业务」，删掉与 `sec_e_cash_total` 同值的 `sec_e_cash_equiv_total`（同一事实不写两列）。PG 侧显式迁移加两列、删一列。实测：PG 的 `sec_e%` 列恰为四项、无 `sec_e_cash_equiv_total`；四列值与独立复算（读 `ods_gl_balances.csv` 的 1001/1100 净额）逐实体一致 |
| 14 | 低 | sec_k_cumulative_30d_gap 与 sec_k_net_funding_gap 是同一个表达式，累计口径名不副实 | ⬜ 待你定（证据已核，未动） |
| 15 | 中 | LCR 分子不含现金与央行准备金，与 DATA-DESIGN §3.2 的 Level 1 定义不一致 | ✅ 已修（批次 C4a）：`liquidity_monitor` 的 L1 = 非受限一级证券 + `sec_e_cash_total`（取 Section E 合计，不分别相加，避免同一笔算两次），L2 的 40% 上限基数同步纳入这个 L1。实测：`ads.ads_liquidity_metrics.hqla_l1_unencumbered_usd` 与报表「非受限一级证券 + 现金」逐行一致（6 行差 ≤ 0.05）；合并口径 L1 由 2,384,335,189 升到 2,522,498,378 |
| 16 | 中 | 到期证券市值同时进 HQLA 与 30 天流入，形成双向计量 | ⬜ 待你定（证据已核，未动） |
| 17 | 中 | DATA-DESIGN §2.1/§4 与模型三层漂移：ows_hqla_summary 声称含 40% 截断、ows_collateral_summary 来源写错、血缘图声称 ows_funding_summary 喂报表 | ✅ 已修（批次 C5）：① DATA-DESIGN §2 的 `ows_hqla_summary` 去掉「含二级资产 40% 上限截断」（截断在报表层与 `liquidity_monitor`），`ows_collateral_summary` 来源由 `owd_secured_financing` 改为 `owd_securities`；② §4 血缘图改为真实链路（`owd_deposits → ads_fr2052a_report`，不经 OWS）并写明三张 OWS 表无消费者；③ 三张无消费者的 OWS 表在 `KNOWN-ISSUE` 登记为待下线（新锚点 `#dead-ows-tables`），三份表契约与报表契约的上游/下游清单同步改正。实测：全仓搜这三个表名，只剩说明性引用 |
| 18 | 中 | owd_derivatives 折算用交易币种 currency 而非 ODS 声明的 mtm_currency，后者在 OWD 层被丢弃 | ✅ 已修（批次 C5）：`owd_derivatives` 的盯市值改为按 ODS 声明的 `mtm_currency` 折算（名义本金与抵押品仍按交易币种），并把 `mtm_currency`、`mtm_exchange_rate` 输出成列，口径可核；**生成器总账侧 1500/2200 同步改用同一币种折算** —— 只改模型不改总账会让两边打架，差异还会以「对账不平」的形式出现、看不出是币种用错；历史表补列走一次性迁移 `sql/iceberg/09_*`。实测：GL 对账含 H/H2 两期各 48 项全 PASS |
| 19 | 中 | dbt 层零 schema 测试：dbt_project.yml 声明了 test-paths 但目录不存在，marts/schema.yml 只有描述没有断言 | ✅ 已修（批次 C5）：`marts/schema.yml` 与 `staging/schema.yml` 补键与枚举列断言（`not_null` / `unique` / `accepted_values`），另加两条 singular test 守合并行唯一与口径码自洽。实测：`dbt ls --resource-type test` 列出 **23 条**（5 条 singular + 18 条 schema），日批 dbt-test 环节 `PASS=23 WARN=0 ERROR=0` |
| 20 | 中 | SCD2：row_hash 的排除列不含 etl_source_file，且写入后自检不校验『每键至多一个有效版本』 | ⬜ 待你定（证据已核，未动） |
| 21 | 中 | Airflow 日批 DAG 缺 verify-scd2 环节，与 run-daily-pipeline.sh 的环节清单不一致 | ✅ 已修：DAG 新增 verify_scd2 任务并接入链条 |
| 22 | 低 | Section F 的 30 天过滤无下界，已过期贷款也计入流入 | ✅ 已修（批次 C5）：报表与明细两处 30 天窗口改为 `days_to_maturity between 0 and 30`（下界取 0：当天到期的贷款就是 O/N，属窗口内）。实测：样本 658 行贷款的最早到期日是报告日 +5 天、已过期 **0 行**，所以这条是**防御性修正，不改变当前数值** —— 判据在，等脏数据出现才见效果 |
| 23 | 低 | 明细表把逆回购也写成 outflow_amount 非零，需依赖 line_item 过滤才与报表对得上 | ✅ 已修（批次 C5）：明细 `secured_detail` 的方向归位 —— 正回购记 `outflow_amount`、逆回购记 `inflow_amount`，不再让下游必须靠 `line_item` 过滤才说得通。实测（PG）：`B-REVERSE` 129 行全部 `inflow_amount > 0 AND outflow_amount = 0`；`B-REPO` 127 行全部 `outflow_amount > 0 AND inflow_amount = 0`；明细汇总 Section B 仍与报表一致 |
| 24 | 低 | 监管上限的『独立核对』复刻被校验方同一公式，属自证循环 | ✅ 已修（批次 C5）：`verify_gold.check_l2_cap` 的输入改为从 silver 明细独立复算（`owd_securities` 按 `hqla_classification` 汇总、合并口径只取非集团内行），不再读报表自己的 `sec_g_*` 列再套同一个公式。实测：两期 PASS 并打印独立算出的份额与占比（09-15 认列总额 8,762,567,726.60、二级占比 34.32%；09-16 7,992,261,539.85、32.28%） |
| 25 | 低 | sql/iceberg 下的 05/06/07 是一次性破坏性脚本，却被 deploy/reset-demo.sh 直接引用执行 | ✅ 已修（批次 C5）：`05/06/07` 三个一次性破坏性脚本移到 `sql/iceberg/oneoff/`（内容一字未改），引用方全部更新（`deploy/reset-demo.sh` 三处、`PROJECT-STRUCTURE` 新增目录职责并写明「常规路径不得引用」、`KNOWN-ISSUE` 的路径）。实测：排除历史文档（build-log / 本审计报告）后全仓搜 `iceberg/05_|06_|07_` **零命中** |
| 26 | 低 | reset_demo.sql 会 TRUNCATE 被声明为『历史不可变』的报表版本历史表 | ✅ 已修（批次 C5）：`sql/admin/reset_demo.sql` 移除 `TRUNCATE ads.ads_fr2052a_report_history`，原位写明理由（历史不可变是硬性质：允许清空就等于承认历史可抹掉，重述登记会留下悬空引用）；确需清空走单独人工步骤，已在 `KNOWN-ISSUE` 登记为有意例外（新锚点 `#report-history-reset-exception`）。实测：`grep report_history reset_demo.sql` 只剩说明注释 |

<details><summary>展开：二、dbt 与 SQL 口径审查 的逐条证据与建议</summary>

**1. 30 天预期流出漏掉全部活期/储蓄存款（非定期存款 bucket 落 OPEN 被报表过滤）**（高）
- 证据：python/generators/ods_data.py:184 `maturity_date = _business_day_after(rng, 15, 400) if deposit_type in TERM_DEPOSIT_TYPES else ''`，同文件 47 行 `TERM_DEPOSIT_TYPES = ('CD', 'TIME')`，即 CHK/SAV/MMDA 到期日为空；dbt/macros/fr2052a_rules.sql:11 `when {{ days_expr }} is null then 'OPEN'`；dbt/models/marts/ads_fr2052a_report.sql:120 `where maturity_bucket in ('O/N', '1-7D', '8-30D')`；而 python/generators/ref_data.py:215 `('DEMAND', 'RETAIL', 'O/N', 0.05, 0.00, ...)` 的假设行因产品落 OPEN 桶永不匹配。
- 为什么算问题：FR 2052a / LCR 的流出主体是活期与储蓄存款（行为假设给的 5%~20% 流失率就是给它们的），现在这批存款一个币值都不进 30 天流出：sec_k_total_outflows 只剩定期存款（TIME 8-30D 0.30、CD 未命中走默认 0.1）与正回购。分母被系统性低估 → net_cash_outflow 变小 → LCR 被高估 → 熔断闸（CB-LCR-001/002）在真实资金外流下不会跳，报送照旧放行。这是『静默失真』，不是报错。
- 建议：在 OWD 层把无到期日的活期/储蓄按业务口径归入 O/N（例如 maturity_bucket 宏增加『days 为 NULL 且产品为即期 → O/N』的分支，或 owd_deposits 里对非定期产品显式落 bucket）；同时补一条断言：sec_k_total_outflows 必须覆盖存款类产品的全部余额×流失率。

**2. GL 对账 Section E 是自比对（两边同读 owd_gl_entries 的 1001/1100），恒 PASS 且零信息量**（高）
- 证据：dbt/models/marts/ads_gl_reconciliation.sql:19-20 映射 `select '1001', 'E', '现金头寸' union all select '1100', 'E', '现金头寸'`；同文件 67-71 报送侧 `select 'E' as section_code, round(sum(net_balance_usd), 2) as report_amount from {{ ref('owd_gl_entries') }} where gl_account_id in ('1001', '1100')`；总账侧 28-39 行 `from {{ ref('owd_gl_entries') }} g join account_section m on m.gl_account_id = g.gl_account_id`。
- 为什么算问题：『对账』的意义在于两边来源独立、一方出错另一方仍正确。E 项两边是同一张表、同一批科目、同一 net_balance_usd 口径，差异恒为 0，只可能因 abs() 的符号处理产生假差异。这条 PASS 会让 check_submission_gate（python/validators/check_submission_gate.py:52 起『熔断或对账未平即不放行』）误以为 E 项已被独立验证。
- 建议：报送侧 E 项改为从业务来源取数（现金头寸来自 owd_gl_entries 是设计使然，则至少换一个可见的独立来源，如 ODS 现金科目快照），或把 E 项在结果里标注为『同源不可判』而不是 PASS。
- 处置（批次 C6）：采纳「换独立来源」，但**不采纳建议里举的例子** —— ODS 现金科目快照仍是同一份总账数据的上游，换表不换源、独立度为零。真独立要换来源系统 + 口径 + 时点：基准换成司库系统的对账单/盘点口径（`benchmark_source = TREASURY_CASH_POSITION`），报送侧保持账面口径，差额由在途存款与未兑现支票逐项解释（`reconciling_item_usd`）。演示环境的独立度边界（对账单余额由账面倒推）已登记 `#recon-benchmark-synthetic`。

**3. 覆盖写（truncate=true）的前置闸只查了表结构，不查业务前提；且 Airflow DAG 把 publish-access 排在 export-pg 之后，与脚本声明的前置条件相反**（高）
- 证据：python/exporters/export_gold_to_pg.py:112-124 只做列结构比对（`schema_diff(frame, existing)`），128 行 `.option('truncate', 'true')` 直接覆盖，130-134 行只用『源行数 = 回读行数』验证；deploy/server2/run-daily-pipeline.sh:122 明确注释『必须在 export-pg 之前：先施加结构迁移与授权』，其 STEPS 顺序 45-62 行是 publish-access → export-pg；但 deploy/server1/airflow/dags/fr2052a_daily_batch.py:158-176 的链是 `>> export_pg >> publish_access >> liquidity_monitor`，与脚本相反。
- 为什么算问题：① 覆写没有任何业务前置条件：不检查该报告日是否已有 SUBMITTED/ACCEPTED 的报送（ads_fr2052a_submission）、不检查熔断闸状态、不检查 gold 是否为空；源行为 0 时同样回读 0，报『OK』并把库里的报表清空。② DAG 顺序反了：导出先 TRUNCATE 再写，publish-access 的迁移与授权在其后补——若导出失败或列结构变更，原子性与授权保护在 DAG 路径上不成立（只有手工全量跑 run-daily-pipeline.sh 才正确）。③ gse 失败于写入中途时 TRUNCATE 已提交，旧报表已不可得（Spark JDBC 的 truncate 与批量 insert 不是同一事务）。
- 建议：在 export 前加显式前置检查：目标报告日是否已报送、熔断闸是否 HALTED、源行数是否为 0；DAG 里把 publish_access 放到 export_pg 之前，与 run-daily-pipeline.sh 对齐（DAG 与脚本必须同一顺序，否则『编排逻辑只有一份』的纪律失守）。

**4. 报表与对账模型都不按 report_date 限定：多报告期共存时金额跨期串加、合并行翻倍**（高）
- 证据：dbt/models/marts/ads_fr2052a_report.sql:127 `select distinct report_date, entity_code from {{ ref('owd_deposits') }}`，而 13-123 的全部聚合 CTE 只 `group by entity_code`，214-222 行的 8 个 left join 只有 `on d.entity_code = e.entity_code`，没有 report_date；298 行合并行取 `max(report_date)`。dbt/models/marts/ads_gl_reconciliation.sql:107-112 `select max(report_date) ... from {{ ref('owd_gl_entries') }}`，而 30-39 与 43-101 的聚合同样不带 report_date 过滤。
- 为什么算问题：ODS 按 days(report_date) 分区且 deploy/server1/airflow/dags/fr2052a_backfill_and_restate.py:77/92/107 以 `REPORT_DATE={{ params.report_date }}` 支持多日回填（ODS 设计就是按日快照、SOURCE 注释也写『按日重跑幂等』）。一旦湖里出现第二个报告日：实体行的金额会取『两个报告日之和』并复制到每个报告日；合并行会把它再翻一倍、却只贴一个 max 日期标签。verify_gold 的合并口径校验（python/lakehouse/verify_gold.py:70-91）在此形态下仍全绿，异常不可见。当前样本只有 2026-09-16 一天，属潜伏缺陷。
- 建议：所有 OWD/OWS 聚合与 marts join 显式带 report_date（`group by report_date, entity_code`，join 条件补 `and f.report_date = e.report_date`），对账模型用报告日参数而非 max()；并给 verify_gold 增加『报表行数 = 报告日数 ×（实体数 + 1）』的断言。

**5. ENT001 同时是真实法人实体与集团合并行代码，主键 (report_date, entity_code) 不含口径，无法承载声明的粒度**（高）
- 证据：python/generators/ref_data.py:47-61 中 ENT001 = Global Bank Holding Inc.（entity_level 1，is_material_entity True，FULL）；python/generators/ods_data.py:85 `GL_ENTITY = 'ENT001'` 使 owd_gl_entries 的唯一实体也是 ENT001；dbt/models/marts/ads_fr2052a_report.sql:231 `'ENT001' as entity_code` 把合并行也写成 ENT001；sql/postgres/10_control_tables.sql:32 `CONSTRAINT ads_liquidity_metrics_pkey PRIMARY KEY (report_date, entity_code)`，而 docs/business/DATA-DESIGN.md:152 声明的粒度是『报告日 + 实体 + 口径』。
- 为什么算问题：① 合并行与母公司单体行在 report_id 之外完全同码，任何按 entity_code='ENT001' 的关联（例如把 ads_liquidity_metrics 与总账、与控制表 join）都会静默把集团口径当法人口径。② 指标表主键不含 is_consolidated，实测若母公司也出单体报表会直接撞主键（现在因为 entities 取自存款、而 ENT001 不记账才侥幸不撞）——即约束表达不了声明的粒度，脏数据拦不住。
- 建议：合并行改用保留码（如 GRP001/CONSOLIDATED），或在所有含 is_consolidated 的表的唯一键里加上该列；GL_ENTITY 与合并码必须不同值，否则总账与合并报表的身份无法区分。

**6. 三张报表表（report/detail/gl_reconciliation）全库无主键或唯一约束，且注释声称存在的『补键列 DO 块』在本仓 sql/ 中并不存在**（高）
- 证据：sql/postgres/10_control_tables.sql:7-8 `全部语句幂等。唯一的非 CREATE 语句是对 ads.ads_fr2052a_report 补键列 ——`，但全文只有 ads_fr2052a_report_history 的 DO 块（190-214 行）；全仓 sql/ 搜索无 `ALTER TABLE ads.ads_fr2052a_report`，也无该表的 CREATE；docs/business/DATA-DESIGN.md:150 声明『主键 report_id，四段区位码』；python/exporters/export_gold_to_pg.py:7-8 说明 PG 侧表结构由导出作业自动创建（Spark JDBC 不建 PK）。
- 为什么算问题：报表主表、明细表、对账表的唯一性只靠模型自身的 group by 保证（ads_fr2052a_report.sql:24/35/46 group by entity_code），库侧零约束：同一 (report_date, entity_code, 口径) 可以落多行而不报错，report_id 重复无人拦。报表是被监管读取并进入报送台账的对象，缺唯一约束意味着下游 JOIN 可能静默放大金额。
- 建议：在 sql/postgres/ 增加对 ads.ads_fr2052a_report 的显式 DO 块：列存在则 `ADD CONSTRAINT ... PRIMARY KEY (report_id)` 或 `UNIQUE (report_date, entity_code, is_consolidated)`；detail 加 `UNIQUE (report_date, entity_code, section_code, line_item, product_category, counterparty_type, currency_code, maturity_bucket)`；gl_reconciliation 加 `UNIQUE (report_date, section_code)`。约束不存在时补键列这一步要么补上、要么把注释改掉（自称存在而实际没有，比没有更危险）。

**7. 汇率 LEFT JOIN 无缺行保护：折算失败时金额静默变 NULL，而 ref 汇率表只有报告日一行**（高）
- 证据：dbt/models/staging/owd_deposits.sql:29 `round(d.principal_amount * f.spot_rate, 2) as principal_amount_usd` 配 49-51 行 `left join fx f on f.currency_code = d.currency and f.rate_date = d.report_date`（owd_loans.sql:34-58、owd_securities.sql:37-60、owd_secured_financing.sql:34-76、owd_derivatives.sql:40-70、owd_off_bs.sql:32-46 同形）；dbt/models/staging/stg_fx_rates.sql:5-12 只过滤 `rate_type = 'MID'`；python/generators/ref_data.py:531-534 只生成 `REPORT_DATE.isoformat()` 一天的汇率。
- 为什么算问题：任何非 2026-09-16 的报告日（回填 DAG 参数化了 report_date）都会让 spot_rate 为 NULL → 金额列全为 NULL → 上游 sum() 静默跳过 NULL 行，报表出来是一堆 0/NULL 且退出码 0。VDQ-009（折算精度）在 python/validators/run_dq_rules.py:79 被登记为跨表、由 verify_silver 覆盖，单表引擎不兜。
- 建议：把汇率 join 从 left join 改为 inner join（缺汇率即报错），或在 OWD 层加 `spot_rate is not null` 的断言并让批次失败；ref 侧应按报告日生成/加载多日汇率而不是只有一天。

**8. stg_fx_rates 未限定 to_currency='USD'，OWD 各表 join 只用 currency，多目标币种时会行放大**（中）
- 证据：dbt/models/staging/stg_fx_rates.sql:5-12 `select from_currency as currency_code, to_currency as report_currency, spot_rate, rate_date from {{ source('ref', 'ref_exchange_rates') }} where rate_type = 'MID'`，report_currency 取出后没有任何地方使用；sql/iceberg/01_create_ref_tables.sql:69-76 `ref_exchange_rates(rate_date, from_currency, to_currency, spot_rate, rate_type, rate_source)` 是通用币对表；join 形如 dbt/models/staging/owd_deposits.sql:49-51 只带 `f.currency_code = d.currency`。
- 为什么算问题：表结构允许同一 (rate_date, from_currency) 存在多个 to_currency；一旦加载了 EUR/USD 与 EUR/JPY 两条，deposits 每行会被复制成两行，合计金额翻倍且不报错。当前样本 ref_data.py:532 只写 to_currency='USD'，属潜伏缺陷，但约束不在代码里而在数据里。
- 建议：stg_fx_rates 加 `where to_currency = 'USD'`（折算基准是 USD，就该在唯一入口处写死），并给 ref_exchange_rates 的 (rate_date, from_currency, to_currency, rate_type) 加唯一性校验。

**9. 行为假设 join 缺 customer_segment，且未命中时静默套用 10% 默认流失率**（中）
- 证据：dbt/models/intermediate/ows_cashflow_projection.sql:21-25 `round(sum(d.principal_amount_usd * coalesce(a.runoff_rate, 0.1)), 2) ... left join {{ source('ref', 'ref_behavior_assumptions') }} a on a.product_category = d.product_category and a.maturity_bucket = d.maturity_bucket`；被 join 的表在 sql/iceberg/01_create_ref_tables.sql:96-104 声明的粒度是 (product_category, customer_segment, maturity_bucket)；python/generators/ref_data.py:214-221 的 6 行假设确实带 customer_segment。
- 为什么算问题：① 参考表粒度含 customer_segment，join 却只用 product+bucket —— 同一对出现两个客户细分的假设（零售 5%、对公 20% 这类）就会扇出，把存款余额重复计入流出，且不报错。② `coalesce(a.runoff_rate, 0.1)` 把『没有行为假设』和『假设流失率 10%』变成同一件事，模型头注释（第 1-10 行）只说明 cap 不在本层应用，未声明这个默认值。当前样本中 CD 在 8 -30D 桶未命中假设、直接套用 10%；实measure 出来的是『假配合』而不是被记录的口径。
- 建议：join 条件补上 customer_segment（或明确降级为不含细分并同步改参考表主键）；把默认流失率提为模型变量并在 schema.yml 里声明，未命中假设的产品应显式报错或落审计，不允许静默取默认值。

**10. is_encumbered 为 NULL 时三层三种处理方式，导致 Section I（非受限+受限）加不回 Section G**（中）
- 证据：dbt/models/staging/owd_securities.sql:47 `s.pledged_flag = 'Y' as is_encumbered`（NULL 入参产出 NULL，布尔三值逻辑）；dbt/models/marts/ads_fr2052a_report.sql:74 `case when hqla_classification = 'LEVEL_1' and not is_encumbered then market_value_usd else 0 end`（NULL 既不计非受限，78 行 `case when is_encumbered then ...` 也不计受限）；dbt/models/intermediate/ows_hqla_summary.sql:19 `case when s.is_encumbered then 0 else s.market_value_usd end`（NULL 被算作非受限）；dbt/models/intermediate/ows_cashflow_projection.sql:72 `where not s.is_encumbered`（NULL 被排除）。
- 为什么算问题：同一列 NULL 在 ADS 被『两边都不算』、在 OWS 被当非受限、在现金流预测被丢弃 —— 任一层变更质押标记的生成逻辑（当前 python/generators/ods_data.py:342 只产 Y/N，属潜伏）都会让 LCR 分子与 Section G/I 相互矛盾，且三条路径的结论不同、无人对账。
- 建议：在 OWD 层把质押标记强制二值化（`coalesce(s.pledged_flag, 'N') = 'Y'` 或 `s.pledged_flag = 'Y' or s.pledged_flag is null` 择一并写进注释），并加一条断言：sec_i 的非受限 + 受限 = sec_g_total_mv。

**11. 受保存款限额在『原币』上截断 25 万美元，外币存款的受保金额量级错误**（中）
- 证据：dbt/models/staging/owd_deposits.sql:39-43 `-- 受保金额按存款保险上限（25 万美元/客户）截断 ... case when d.insured_flag = 'Y' then least(d.principal_amount, 250000) * f.spot_rate else 0 end`，d.principal_amount 是原币（同文件 28-29 行 `d.principal_amount as principal_amount_lc` / `round(d.principal_amount * f.spot_rate, 2)` 证明单位）。
- 为什么算问题：限额是美元限额，必须先折算再截断（least(principal*rate, 250000)）。现在拿原币跟 250000 比：JPY（ref_data.py:37 汇率 0.0067）的 3,000 万日元存款会被截到 250,000 再乘汇率 → 受保额约 1,675 美元，比真实值差两个量级；EUR/GBP 则被放宽。另外限额是按客户聚合而非按单笔，逐笔截断也会高估。
- 建议：改为 `least(d.principal_amount * f.spot_rate, 250000)`，并按 customer_id 聚合后再按限额截断（window 或子查询）；补一条单测/核对：insured_amount_usd 必须 ≤ 250000。

**12. insured_amount_usd 全仓无消费者，而 marts/schema.yml 声称 sec_c_total 已按受保限额截断**（中）
- 证据：dbt/models/marts/schema.yml:23-28 `- name: sec_c_total  description: Section C 存款合计，受保存款按限额截断后计入`；实际 dbt/models/marts/ads_fr2052a_report.sql:17 `round(sum(principal_amount_usd), 2) as total_deposits_usd`（未做任何受保处理，154 行 `d.total_deposits_usd as sec_c_total`）；`insured_amount_usd` 全仓仅出现在 dbt/models/staging/owd_deposits.sql:43 定义处，无任何下游引用。
- 为什么算问题：契约（schema.yml 是列级契约，也是 render_lineage/build_pii_vault 的输入）声明了一个报表没有实施的口径，读契约的人会以为 C 项已按保险限额扣减；同时那列受保金额算完就丢，说明该口径在整条链路里悬空。
- 建议：二选一：报表真的按受保限额调整 C 项并更新明细；或把 schema.yml 的描述改成事实（『按存款全额计入，不区分受保』），并说明 insured_amount_usd 的去向或删除。

**13. Section E 列名与语义错配：库存现金映射到 sec_e_central_bank_dep，同业存放列整列丢失，total_cash 重复计两次**（中）
- 证据：dbt/models/intermediate/ows_cash_position.sql:18-20 产出 `cash_on_hand_usd`(1001) / `due_from_banks_usd`(1100) / `total_cash_usd`；dbt/models/marts/ads_fr2052a_report.sql:158-160 `c.total_cash as sec_e_cash_total, c.cash_on_hand as sec_e_central_bank_dep, c.total_cash as sec_e_cash_equiv_total` —— due_from_banks_usd 在全仓无任何下游引用；requirements/[99]详细材料.md:939-952 的原始口径是 `total_cash_usd = cash_on_hand_usd + central_bank_deposits_usd + correspondent_bank_usd + overnight_lending_usd`，即央行存款是独立列。
- 为什么算问题：行项目名字承载监管含义：把 GL 1001『库存现金』填进『央行存款』，报送文件里就是错报科目；1100 同业存放只在合计里出现、自身无行项目；且 sec_e_cash_total 与 sec_e_cash_equiv_total 是同一列值（同一事实写两列）。三者都指向 E 项口径未真正装配。
- 建议：要么把 due_from_banks_usd 映射到对应的行项目、并把 cash_on_hand 填到『库存现金』而非央行存款；要么明确本项目 E 项只有两项并同步改列注释与 schema.yml（禁止同行事实写两列）。

**14. sec_k_cumulative_30d_gap 与 sec_k_net_funding_gap 是同一个表达式，累计口径名不副实**（低）
- 证据：dbt/models/marts/ads_fr2052a_report.sql:203-206 `round(least(coalesce(cf.raw_inflow, 0), 0.75 * coalesce(cf.total_outflow, 0)) - coalesce(cf.total_outflow, 0), 2) as sec_k_net_funding_gap` 与 207-210 完全相同的表达式 `... as sec_k_cumulative_30d_gap`（合并口径 290-297 行同样成对复制）。
- 为什么算问题：列名声明了两个不同指标（净缺口 vs 30 天累计缺口），实现是同一式子。下游按列名取数会得到两个永远相等的值，任何『累计』相关的判断都变成假的。
- 建议：累计缺口要么按多桶逐日累加真正实现（并写进 schema.yml 说明），要么删掉该列；不要留一个名字与语义不符的列。

**15. LCR 分子不含现金与央行准备金，与 DATA-DESIGN §3.2 的 Level 1 定义不一致**（中）
- 证据：docs/business/DATA-DESIGN.md:214 `| Level 1 | 现金、央行准备金、国债 | 0% |`；dbt/models/marts/ads_fr2052a_report.sql:189-193 的 sec_i_unencumbered_hqla_l1 来自 65-80 行 hqla CTE，而该 CTE 只 `from {{ ref('owd_securities') }}`（第 79 行）；python/alerts/liquidity_monitor.py:96-102 `l1 = number(row['sec_i_unencumbered_hqla_l1']) ... hqla_capped = l1 + min(unencumbered_l2, 0.40 * unencumbered_total)`。
- 为什么算问题：现金/存放同业（Section E，来自 GL 1001+1100）没有进入 LCR 分子，等于把最高等级、零折扣的流动性资产漏掉；LCR 因此被低估，阈值判断与 headroom_usd 都跟着偏。DATA-DESIGN §3.2 写了『现金、央行准备金』是 Level 1，实现没做，属文档与代码漂移且是监管口径级差异。
- 建议：把 Section E 的现金头寸按未受限口径加进 HQLA Level 1（或在文档里明确声明本项目 LCR 分子只含证券类 HQLA 并说明与 §3.2 的偏差与理由）。

**16. 到期证券市值同时进 HQLA 与 30 天流入，形成双向计量**（中）
- 证据：dbt/models/intermediate/ows_cashflow_projection.sql:61-73 `security_maturity ... round(sum(s.market_value_usd), 2) as expected_inflow_usd ... from {{ ref('owd_securities') }} s where not s.is_encumbered`（市值全额作流入，未剔除已计入 HQLA 的证券）；dbt/models/marts/ads_fr2052a_report.sql:119-120 该流入经 `where maturity_bucket in ('O/N', '1-7D', '8-30D')` 计入 raw_inflow，并在 201 行进入 sec_k_total_inflows；同一批证券在 167-171 行又计入 Section G。
- 为什么算问题：LCR 口径下计入 HQLA 存量的资产不得再作现金流入，否则同一笔资产被计两次（分子抬高 + 分母压低），LCR 双向偏离。
- 建议：security_maturity 增补条件：排除计入 HQLA 等级的证券（或在 30 天窗口内已作流入的证券从 HQLA 中扣除），并在 DATA-DESIGN §3.3 写明该剔重规则。

**17. DATA-DESIGN §2.1/§4 与模型三层漂移：ows_hqla_summary 声称含 40% 截断、ows_collateral_summary 来源写错、血缘图声称 ows_funding_summary 喂报表**（中）
- 证据：docs/business/DATA-DESIGN.md:109 `silver.ows_hqla_summary | HQLA 汇总（含二级资产 40% 上限截断）` 但 dbt/models/intermediate/ows_hqla_summary.sql:16-22 只算市值/折扣后价值，无任何 40% 截断（截断在 marts 与 liquidity_monitor.py:102）；DATA-DESIGN.md:110 写 ows_collateral_summary 来源 `owd_secured_financing`，实际 dbt/models/intermediate/ows_collateral_summary.sql:9 `select * from {{ ref('owd_securities') }}`；DATA-DESIGN.md:245-251 血缘写 `owd_deposits → ows_funding_summary → ads_fr2052a_report.sec_c_retail_demand`，实际 ads_fr2052a_report.sql:18-23 直接读 owd_deposits（全仓 `ref('ows_` 只有 2 处：ows_cash_position、ows_cashflow_projection，即 ows_hqla_summary / ows_collateral_summary / ows_funding_summary 三张表无任何下游消费者）。
- 为什么算问题：DATA-DESIGN 自称与 sql/iceberg 同为真源，血缘又是 render_lineage/DataHub 替代品的输入；三处不符会让审计与影响面分析建立在错的关系上。另外三张 OWS 表无人消费（死表），却每天全量物化，属成本与认知负担。
- 建议：逐条改正 DATA-DESIGN 的描述与血缘图；三张无消费者的 OWS 表要么接入报表（例如让 report 用 ows_funding_summary 取 Section C，与血缘一致），要么登记为待下线并说明保留原因。

**18. owd_derivatives 折算用交易币种 currency 而非 ODS 声明的 mtm_currency，后者在 OWD 层被丢弃**（中）
- 证据：sql/iceberg/02_create_ods_tables.sql:144-145 `mark_to_market DECIMAL(18,4) COMMENT '盯市价值，可正可负', mtm_currency STRING COMMENT '盯市价值币种'`；dbt/models/staging/owd_derivatives.sql:47 `round(d.mark_to_market * f.spot_rate, 2) as mtm_value_usd`，而 68-70 行 join 的是 `f.currency_code = d.currency`；mtm_currency 在 OWD 层没有任何输出列。python/generators/ods_data.py:384 使样本恒为 'USD'。
- 为什么算问题：盯市值的币种与名义本金币种可以不同（跨币种 IRS 的 MTM 通常折成报告币种）。用 currency 折算在样本里巧合正确，真实数据里会让衍生品资产/负债（Section H、GL 1500/2200 对账）系统性偏错，且没有列能让人发现口径被替换过。
- 建议：OWD 层保留 mtm_currency 列，并用它 join 汇率（或对 mtm_currency 非 USD 的行单独折算）；补 VDQ 规则校验 mtm_currency 非空。

**19. dbt 层零 schema 测试：dbt_project.yml 声明了 test-paths 但目录不存在，marts/schema.yml 只有描述没有断言**（中）
- 证据：dbt/dbt_project.yml:9 `test-paths: ['tests']`，但仓库中不存在 dbt/tests/ 目录（`ls dbt/` 只有 macros、models、dbt_project.yml、profiles.yml）；dbt/models/marts/schema.yml 与 staging/schema.yml 通篇只有 description/meta，无 `tests:`、unique、not_null、relationships 任何断言。
- 为什么算问题：口径规则全靠模型内部实现与后置 python 核对脚本兜（verify_gold/verify_silver/run_dq_rules），单表级的不变量（主键唯一、金额非空、桶枚举合法、report_id 与 report_date 拼装一致）在建模层无人守。VDQ 里已声明的规则也是靠 SQL 字符串（ref_validation_rules）执行，与 dbt 的列契约没有机器关联，改列名不会让规则失败而是被标 SKIPPED（python/validators/run_dq_rules.py:171 `check_result='SKIPPED'`）。
- 建议：至少在 marts/staging 的 schema.yml 补上最小断言集：report_id/（report_date+entity_code+is_consolidated）unique、amount 列 not_null、maturity_bucket accepted_values、hqla_classification accepted_values；并给 dbt run 接 `dbt test` 环节。

**20. SCD2：row_hash 的排除列不含 etl_source_file，且写入后自检不校验『每键至多一个有效版本』**（中）
- 证据：python/lakehouse/owd_scd2.py:78 `EXCLUDED_FROM_HASH = ('etl_load_timestamp', 'etl_batch_id')`（同文件 33-34 行注释称『ETL 元数据列（etl_load_timestamp 等）不参与』），而 ODS 表还有 etl_source_file 列（sql/iceberg/02_create_ods_tables.sql:35/63/94/123/152/172/194，取值含报告日：python/generators/config.py:88-91 `source_file()`）；87 行 EXCLUDED 之外的 etl_source_file 进 hash（177-180 行 `hash_expression`）；289-297 行的写后自检只查『end_date < begin_date』一条不变式；207 行 `active = ... WHERE is_active` 的后续连接全部假定每键一个有效版本。
- 为什么算问题：① etl_source_file 若带上批次/文件名变化（例如同数据换名重放），每行 row_hash 都变 → 全表被判定为『已变更』，版本号狂涨、last_modified_reason 被写成一堆本不存在的 CORRECTION，而审计正是靠这两列。② 若历史表里同一键出现两行 is_active（脏数据、或有人手工改表），207 行的 active 会扇出、版本号与变更原因随之算错，现有自检抓不住 —— 而作者自己在 188-195 行注释里描述了同类事故（『数据行数对，但版本号与变更原因是错的』），说明这正是已知的失败模式。
- 建议：把 etl_source_file 列入 EXCLUDED_FROM_HASH（或在 hash 里只用业务列白名单）；写后自检增设 `SELECT key, count(*) FROM history WHERE is_active GROUP BY key HAVING count(*) > 1`，非空即抛错退出。

**21. Airflow 日批 DAG 缺 verify-scd2 环节，与 run-daily-pipeline.sh 的环节清单不一致**（中）
- 证据：deploy/server2/run-daily-pipeline.sh:59 与 89 行把 `verify-scd2`（『核对版本历史：END_DATE 为空 ⇔ 当前有效、版本号连续、无重复』）列入 STEPS；deploy/server1/airflow/dags/fr2052a_daily_batch.py:158-176 的依赖链只有 verify_bronze、verify_silver、verify_ads、verify_rbac、pipeline_health，没有 verify_scd2 这个 task。
- 为什么算问题：生产按 Airflow 定时触发，按 DAG 跑就永远不会执行 SCD2 区间/版本号一致性核对 —— 而 KNOWN-ISSUE #scd2-reversed-interval（docs/business/KNOWN-ISSUE.md:18）的修法明确写了『verify-scd2 纳入日批环节』，结果只在手工全量跑时生效。这正是 DAG 文件自己 10-14 行警告过的『改了一处忘另一处必然漂移』。
- 建议：在 DAG 里补 verify_scd2 task 并接进链条（放在 owd_scd2 之后、dq_validate 之前或与 verify_* 同级），并把『DAG 任务集合必须等于脚本 STEPS』做成机器校验。

**22. Section F 的 30 天过滤无下界，已过期贷款也计入流入**（低）
- 证据：dbt/models/marts/ads_fr2052a_report.sql:50-60 `where days_to_maturity <= 30`（无 `>= 0`）；dbt/models/marts/ads_fr2052a_detail.sql:69-70 同样 `where days_to_maturity <= 30`；dbt/macros/fr2052a_rules.sql:12 `when {{ days_expr }} <= 0 then 'O/N'` 说明负数天被归为 O/N 且被 30 天窗口接收。
- 为什么算问题：到期日为报告日之前的贷款（逾期未偿）会被当成『30 天内到期流入』。虽然 O/N 归并是既有约定，但把逾期本金算作可实现的流动性流入在监管口径上偏高。
- 建议：明确取舍并落到代码与文档：要么加 `days_to_maturity between 0 and 30`（逾期单独成项），要么在模型头注明『含逾期贷款，按 O/N 处理』。

**23. 明细表把逆回购也写成 outflow_amount 非零，需依赖 line_item 过滤才与报表对得上**（低）
- 证据：dbt/models/marts/ads_fr2052a_detail.sql:33-35 `-- 回购与逆回购分开列示... case when transaction_type = 'REPO' then 'B-REPO' else 'B-REVERSE' end as line_item` 与 41-44 行 `round(sum(cash_amount_usd), 2) as outflow_amount ... round(sum(cash_amount_usd), 2) as net_amount`（对 REPO 与 REVERSE_REPO 同值赋 outflow）；核对脚本必须带过滤才正确：python/lakehouse/verify_gold.py:29 `'B': ('sec_b_repo_outstanding', 'outstanding_amount', 'B-REPO')`。
- 为什么算问题：注释自己说『逆回购是资金运用（资产），混在一起会让明细与报表合计对不上』，但实现只把 line_item 拆开、没把方向修对：按 section_code='B' 直接 sum(outflow_amount) 的消费者会把逆回购当流出，金额量级很大（样本里两者相当）。VDQ-013 靠 line_item 过滤侥幸通过，掩盖了明细列本身的口径错误。
- 建议：B-REVERSE 行的 outflow_amount 置 0、inflow_amount 置 cash_amount（或新增一个 direction 列），让『不额外过滤也能正确聚合』成为明细表的基本性质。

**24. 监管上限的『独立核对』复刻被校验方同一公式，属自证循环**（低）
- 证据：python/lakehouse/verify_gold.py:136-137 `hqla_before_cap = level_1 + level_2` / `expected = round(level_1 + min(level_2, 0.40 * hqla_before_cap), 2)` 与 dbt/models/marts/ads_fr2052a_report.sql:173-179 `coalesce(h.l1_mv, 0) + least(coalesce(h.l2a_mv,0) + coalesce(h.l2b_mv,0), 0.40 * (...))` 是同一式子；VDQ-018 的规则文本 python/generators/ref_data.py:403 `sec_k_total_inflows <= 0.75 * sec_k_total_outflows` 是模型构造出来的恒真式（报表 201 行本身就用 least(…, 0.75 * outf lows) 生成该列）。
- 为什么算问题：核对脚本若与被核对实现共享同一公式，能发现的只有『计算没跑』，发现不了『公式本身错了』（例如 40% 上限是否应作用在未折算前的市值、是否应改用 2/3×L1 口径）。审计证据的强度取决于两边是否独立。
- 建议：核对侧改成从原始数据独立推演（例如由 owd_securities 逐笔按监管定义重算并输出两个口径的差值），或在文档里明确标注『该核对为自证，不能作为口径正确性证据』。

**25. sql/iceberg 下的 05/06/07 是一次性破坏性脚本，却被 deploy/reset-demo.sh 直接引用执行**（低）
- 证据：sql/iceberg/05_align_owd_history.sql:12 `本文件是历史迁移，**只应执行一次**；重复执行会报列不存在，属预期行为`；06_rebuild_owd_history.sql:24 `本文件是历史修复，**只应执行一次**` 且 26-32 行 DROP 7 张历史表；07_fix_reversed_intervals.sql:14 `幂等：再跑一次影响 0 行`；deploy/reset-demo.sh:40 `HISTORY_SQL="$PROJECT_ROOT/sql/iceberg/06_rebuild_owd_history.sql"`、112 行由 run_sql_file.py 执行它。
- 为什么算问题：同一目录里 00/01/02 全是 `IF NOT EXISTS` 幂等 DDL，05/06/07 却是会 DROP 审计历史的破坏性脚本，只在文件头用中文说明区分（06 自己第 3 行还要求『先确认没有别处依赖这些表』，调用脚本没有做这个确认）。任何人按目录批量应用 sql/iceberg/*.sql 都会静默删掉全部 OWD 版本历史。
- 建议：破坏性脚本移出 sql/iceberg/（例如 sql/iceberg/oneshot/ 或 sql/admin/），目录级隔离取代口头约定；reset-demo.sh 执行前加『历史表无下游消费者 + 仅演示环境』的显式确认。

**26. reset_demo.sql 会 TRUNCATE 被声明为『历史不可变』的报表版本历史表**（低）
- 证据：sql/admin/reset_demo.sql:37 `TRUNCATE TABLE ads.ads_fr2052a_report_history;`；对照 sql/postgres/10_control_tables.sql:217 `8. 审计三表。变更留痕用触发器归档，不原地覆盖 —— 历史不可变。`，且 165-183 行给该表建了 PK、唯一约束与 CHECK 以保版本完整性。
- 为什么算问题：报表版本历史是重述登记（ads_restatement_log 的 original_report_id/new_report_id）唯一能对上的实体；清掉它之后重述记录会指向不存在的版本，且审计线索不可恢复。reset_demo 自述是『演示环境清理』（第 1-20 行），范围可以接受，但没有任何机器限制阻止它在非演示库上被执行。
- 建议：在 reset_demo.sql 开头加环境判定（例如校验库名/存在演示标记表，不匹配即中止），或把版本历史表排除在外、只清派生报表。

</details>

**子代理声明未验证的点**：本次为只读静态审查：未执行 dbt run / dbt test、未跑 pipeline、未连 PG 或 Iceberg 实测数据，所有金额类结论均由 SQL 与生成器代码推导，未用实际结果集验证（例如 30 天流出漏活期存款的最终数值、现金流入是否真的触发 75% 上限）。；非定期存款的 maturity_date 空串在 Kafka→Iceberg 路径上最终落成 NULL 还是空字符串未实测；两种情况下 datediff 均为 NULL（bucket=OPEN），结论不受影响，但落地形态影响后续排查方式。；Iceberg 对 `INSERT OVERWRITE <history> (列清单)` 的语法支持与 Spark 3.5.9 行为未实测（owd_scd2.py:237），仅确认写法存在。；sql/iceberg/05、06、07 三个一次性脚本在目标环境是否已执行过、当前历史表是否已是新结构，未核实（无法只读判定）。；PG 侧 ads.ads_fr2052a_report / ads_fr2052a_detail / ads_gl_reconciliation 的实际落库结构（是否有库侧补的约束、列）未查库确认，结论基于 sql/postgres 与 export_gold_to_pg.py ；python 侧 LCR 计算与熔断判定的完整逻辑（liquidity_monitor.py 全文、config/liquidity_thresholds.json 的 regulatory_min 取值）只看了关键片段，未逐行审；该文件不在本任务的 dbt/models 与 s；deploy/ 下 Airflow DAG 与 run-daily-pipeline.sh 的执行顺序差异已按文件内容确认，但线上实际部署文件是否与仓库一致（sync-deploy.sh 的漂移检查结果）未核实。

## 三、文档体系审查（32 条）

| # | 严重度 | 发现 | 本轮处置 |
|---|---|---|---|
| 1 | 高",
      "fix": "把 DAG 链改为 `export_pg` 之前插 `publish_access`（与 STEPS 一致），或在 INTERFACE-DESIGN 明确 DAG 与脚本顺序不同的理由；顺序是契约，不能有两份。 | DAG 里 export_pg → publish_access 的顺序与文档和跑批脚本相反，授权/迁移滞后于导出 | ✅ 已修（本轮） |
| 2 | 高",
      "fix": "要么补一个 pre-commit/CI 检查（校验 commit message 含 [AI]、改动 .py 首行含头注），写进 .githooks/ 与 lint.yml；要么把 AGENTS.md 该两处改成「靠评审」，并登记为偏离。 | AGENTS.md 声称的「机器门禁」不存在：[AI] commit 标记与 [AI-GENERATED] 头注都没有任何检查实现 | 🟡 部分修：`[AI]` 提交标记已由新增的 `.githooks/commit-msg` 落地（三种情形已单测）；头注告警与引用存在性检查未实现，措辞已更正并登记 KNOWN-ISSUE |
| 3 | 高",
      "fix": "去掉 `--list`（该脚本默认行为就是列目录），或给脚本加 `--list`；并核对其余 README 命令（同页 78-81 行）。 | README 给出的取数命令 `inspect_catalog.py --list` 不存在该参数 | ✅ 已修（本轮） |
| 4 | 高",
      "fix": "按 dbt/macros/ 实况重写 MODULE-DESIGN 宏表（名字、参数、返回），脱敏一列改为「加盐 SHA-256，前 16 位 hex，加 `h_` 前缀」；与 INTERFACE-DESIGN 只保留一份（另一份引用）。 | MODULE-DESIGN 的 dbt macro 契约表四个宏名在代码里都不存在，脱敏算法也与实现、与术语表矛盾 | ✅ 已修（本轮） |
| 5 | 高",
      "fix": "以各脚本 argparse/头注为准重写两张表（含退出码与默认值），MODULE-DESIGN 只留「有哪些脚本」的索引，签名放 INTERFACE-DESIGN 单源。 | MODULE-DESIGN 的 Python CLI 契约表四项签名与实现不符，且与 INTERFACE-DESIGN 互相打架 | ✅ 已修（本轮） |
| 6 | 高",
      "fix": "删掉该注，宏表补 `mask_pii(column_name)` 一行；`fr2052a_fx_convert` 若确实未实现，保留单独一句说明（并登记在 KNOWN-ISSUE 偏离表）。 | INTERFACE-DESIGN 明确写「mask_pii 尚未实现」，与 E7 已交付的脱敏机制矛盾 | ✅ 已修（本轮） |
| 7 | 高",
      "fix": "改成计数并断言（如 `test \"$(ls -1 .../*.csv | wc -l)\" -eq 7`），失败以非 0 退出。 | check_source_arrival 任务实际恒成功，与「确认 7 张源文件到位」的声称不符（静默失败） | ✅ 已修（本轮） |
| 8 | 中",
      "fix": "PROJECT.md 改为「10 个 Topic（7 个有生产者）」，并只保留 config 为唯一声明来源。 | PROJECT.md 的 Topic 数量写成 7，与配置、DATA-DESIGN、MODULE-DESIGN 的 10 条冲突 | ✅ 已修（本轮） |
| 9 | 中",
      "fix": "端口表补 9094，验证命令改成 `--bootstrap-server 192.168.17.24:9094 --list`，并标注 9092 仅供容器内使用。 | PROJECT.md 的 Kafka 验证命令用了内网监听端口 9092，从 dev 机/Server 1 不可用；文档未提 9094 | ✅ 已修（本轮） |
| 10 | 中",
      "fix": "DATA-DESIGN §2.3 Ref 行改为「整表 INSERT OVERWRITE（覆盖写）」。 | Ref 层落库契约写「按主键 MERGE」，实现是整表覆盖写 | ✅ 已修（本轮） |
| 11 | 中",
      "fix": "把表契约位置改为真实落点（sql/iceberg/*.sql + DATA-DESIGN §2），或在 docs/tables/ 建最小契约；DEVELOP-FLOW 补 §4 条目模板（或把引用改到实际章节号）。 | 失效引用：AGENTS.md 指向不存在的 `docs/tables/` 与 DEVELOP-FLOW §4 | ✅ 已修（本轮） |
| 12 | 中",
      "fix": "删掉该句，或把技能名校验真的写进 pre-commit（技能目录可枚举）。 | AGENTS.md 引用的 `pre_commit_gate.py` 不存在 | ✅ 已修（本轮） |
| 13 | 中",
      "fix": "二选一：补条目文件并在 PROJECT/AGENTS/README 的目录清单登记；或删掉该机制，改为只追加 docs/build-log.md，并在 AGENTS 说明。 | 变更留痕机制三处口径不一：目录为空、路径未被项目地图承认 | 🟡 部分修：PROJECT-STRUCTURE 已注明该目录当前为空、E6/E7 未按格式留痕；是否补条目待定 |
| 14 | 低",
      "      "fix": "AGENTS.md 改为「E0–E7 构建日志」，或按实际保留范围写「构建日志（原始路径，用户要求保留）」。 | AGENTS.md 把构建日志写成「E0-E5」，与实际含 E0–E7 冲突 | ✅ 已修（本轮） |
| 15 | 中",
      "fix": "两处路径改为 deploy/server1/airflow/dags/，删「待实现」。 | PROJECT-STRUCTURE 把 DAG 写成就地目录且标注「待实现」，与实现和 README 的 E5 ✅ 矛盾 | ✅ 已修（本轮） |
| 16 | 低",
      "fix": "示例改为 Dockerfile/compose 的真实文件名，Python 一列按 snake_case 写（与 CODING-STANDARD 对齐）。 | PROJECT-STRUCTURE 的部署命名示例文件不存在，文件命名规范与 Python 实况相反 | ✅ 已修（本轮） |
| 17 | 中",
      "fix": "INTERFACE-DESIGN 补 5.4 fr2052a_gl_reconciliation、5.5 fr2052a_submission（触发时刻、任务链、参数、失败语义）。 | 5 个 DAG 只在接口文档里登记了 3 个，gl_reconciliation 与 submission 无契约条目 | ✅ 已修（本轮） |
| 18 | 中",
      "fix": "以 fr2052a_daily_batch.py 的 `>>` 链为唯一来源重写 INTERFACE-DESIGN §5.1 与该 DAG 的 docstring。 | INTERFACE-DESIGN 的日批任务流漏登 owd_scd2、verify_rbac，DAG 自带 docstring 也漏 publish_access 与 verify_rbac | ✅ 已修（本轮） |
| 19 | 低",
      "fix": "要么把脚本改成必填（required=True），要么文档把「必填」改为「默认值」。 | INTERFACE-DESIGN 把 run_dq_rules 的 --batch-id 标为必填、replay 两个参数标为必填，代码里都有默认值 | ⬜ 待你定（证据已核，未动） |
| 20 | 中",
      "fix": "标注「节选，全集见 ref.ref_validation_rules」，或补全 20 条并按 apply_layer 分组。 | DATA-DESIGN 的 VDQ 表只列 20 条规则中的 9 条且未标注节选 | ✅ 已修（本轮） |
| 21 | 低",
      "fix": "该行补「3 判不了，按不放行处理」。 | DOMAIN-LANGUAGE 对放行闸退出码漏写 3，与同文档退出码表和实现不符 | ✅ 已修（本轮） |
| 22 | 低",
      "fix": "把状态流转写成「生成即按回执落 ACCEPTED/REJECTED；GENERATED/SUBMITTED 为保留值，当前无写入方」，或让脚本先写 GENERATED 再按回执更新。 | DOMAIN-LANGUAGE 描述的 submission_status 状态流转在代码里没有实现路径 | ✅ 已修（本轮） |
| 23 | 低",
      "fix": "改为「10 次 spark-submit」或直接贴 `--list` 与日志摘要。 | 验收清单的性能证据「8 次 Spark 作业」与实跑环节数不符 | ✅ 已修（本轮） |
| 24 | 中",
      "fix": "性能 #3 换成可执行动作（如 `time bash deploy/server2/run-daily-pipeline.sh gate health` 并记录耗时），CR #2 改成机器判据或标人工项，:46 补真实输出或改标 🔍，签署栏留空则对应判据标 ⬜。 | 验收清单存在不可实核/自循环判据，且签名栏全空却给「独立复核」判 ✅ | 🟡 部分修：性能 #3 自循环判据改为可实核的耗时判据（标未实核）；「独立复核 ✅」降为 ⬜ 并写明只有同一执行方留日志、签名栏为空 |
| 25 | 中",
      "fix": "在偏离表补两行（监控：Grafana/Prometheus → 用 pipeline_health.py 直出结论；BI：Superset → 无），并按本节要求补代价与回退汇总行。 | 需求侧未落地的监控与 BI 选型没有在偏离登记表登记 | ⬜ 未修，登记待定：监控栈（Prometheus/Grafana）与 BI（Superset）未落地且未登记偏离 |
| 26 | 低",
      "fix": "把规则 2 改成与本文件实际锚点格式一致的写法（指向锚点表行），或给锚点补 `### #slug` 标题并让偏离表逐行引用。 | 偏离登记表自己定的格式规则与该文件实际格式、表内容三方不一致 | ⬜ 未修，登记待定：偏离登记表自定的「指向 `### #slug`」与文件实际格式不符 |
| 27 | 低",
      "fix": "以 pipeline_health.py 的实际输出为准统一四处（或改为「不在此抄清单，见脚本」，符合单源纪律）。 | 巡检覆盖清单三处不一致（代码 8 项，两份文档只写 6 项） | ✅ 已修（本轮） |
| 28 | 低",
      "fix": "统一写「七份核心文档（九文档体系的本项目子集）」，或补齐缺失两型文档。 | 「九项核心文档」与实际七份不符，且未见登记 | ✅ 已修（本轮） |
| 29 | 低",
      "fix": "§3.6 改为「脚本全集见 python/ 目录树」+ 按 python/ 子目录分组列全，或补一张 script → 用途 → 退出码 的表。 | INTERFACE-DESIGN「其他脚本」清单漏掉半数已交付脚本，与质量检查点表对不上 | ⬜ 未修：INTERFACE「其他脚本」清单仍只列 6 个（实际 33 个） |
| 30 | 低",
      "fix": "修正两处锚点（PROJECT 改为 #运行环境--验证命令 或删自引用；DATA-DESIGN 改为 #6-血缘与监管映射接口）。 | 两处 Markdown 锚点失效（自引用错锚、指向不存在的标题） | ✅ 已修（本轮） |
| 31 | 低",
      "fix": "该行拆成「单跑接入 | ods-replay」「单跑实时扫描 | realtime-scan」，对账另写一行指向 verify-ads 或 GL 对账 DAG。 | README 常见任务表把「对账」与实时扫描混成一条命令，且对账无独立环节 | ✅ 已修（本轮） |
| 32 | 低",
      "fix": "在 README 目录速览与 PROJECT-STRUCTURE 根目录清单登记该文件（或注明它是派生产物、可重建）。 | 仓库根多出一份未登记在教学文档体系之外的《教学文档-小白版.md》 | 🟡 按设计处理：教学文档是本机产物，已 `.gitignore` 并在 PROJECT-STRUCTURE 根目录清单注明 |

<details><summary>展开：三、文档体系审查 的逐条证据与建议</summary>

**1. DAG 里 export_pg → publish_access 的顺序与文档和跑批脚本相反，授权/迁移滞后于导出**（高",
      "fix": "把 DAG 链改为 `export_pg` 之前插 `publish_access`（与 STEPS 一致），或在 INTERFACE-DESIGN 明确 DAG 与脚本顺序不同的理由；顺序是契约，不能有两份。）
- 证据：deploy/server1/airflow/dags/fr2052a_daily_batch.py:167-170 `>> export_pg >> publish_access >> liquidity_monitor`；deploy/server2/run-daily-pipeline.sh:54-55 STEPS 顺序为 `publish-access` 再 `export-pg`（同文件对应 case 内注释：\"必须在 export-pg 之前：先施加结构迁移与授权，导出用 truncate=true 保住它们\"）；docs/business/INTERFACE-DESIGN.md:219-220 也写 `publish_access 施加库侧迁移与授权 → export_pg 导出到报送服务层`。
- 为什么算问题：按 Airflow 路径执行时，结构迁移与授权在导出之后才施加，与「零授权对象」核对和「truncate=true 保住授权」的机制前提冲突；同一事实在 DAG 代码、跑批脚本、接口文档三处不一致。

**2. AGENTS.md 声称的「机器门禁」不存在：[AI] commit 标记与 [AI-GENERATED] 头注都没有任何检查实现**（高",
      "fix": "要么补一个 pre-commit/CI 检查（校验 commit message 含 [AI]、改动 .py 首行含头注），写进 .githooks/ 与 lint.yml；要么把 AGENTS.md 该两处改成「靠评审」，并登记为偏离。）
- 证据：AGENTS.md:66 \"机器门禁会拦下缺少 `[AI]` 的 agent 会话提交，也会对缺头注的改动代码文件告警。这条规则不靠自觉。\"；AGENTS.md:96 \"机器门禁会检查 docs/、sql/、dbt/、python/、deploy/、config/ 下被引用的文件，以及它点名的根文件\"。但事实：.githooks/pre-commit:11 只有 `if ! make -s lint; then`，.github/workflows/lint.yml:28-29 只有 `run: make lint`，全仓无 [AI]/头注检查逻辑（grep '[AI]\\|AI-GENERATED' 仅命中 AGENTS.md 与代码头注本体）。
- 为什么算问题：红线明说「不靠自觉」，实际只靠自觉；同一句话在两处被当成已落地的机器保障，验收时会被当证据用。

**3. README 给出的取数命令 `inspect_catalog.py --list` 不存在该参数**（高",
      "fix": "去掉 `--list`（该脚本默认行为就是列目录），或给脚本加 `--list`；并核对其余 README 命令（同页 78-81 行）。）
- 证据：README.md:78 `bash spark-submit-fr2052a.sh /opt/fr2052a-app/python/lakehouse/inspect_catalog.py --list`；python/lakehouse/inspect_catalog.py:24,30 实际只注册 `--describe`（nargs=\"*\"）与 `--sample`（type=int），无 `--list`。
- 为什么算问题：人类入口文档里的命令直接跑不通（argparse error），属可实核的假断言；README 的「5 秒上手」路径被破坏。

**4. MODULE-DESIGN 的 dbt macro 契约表四个宏名在代码里都不存在，脱敏算法也与实现、与术语表矛盾**（高",
      "fix": "按 dbt/macros/ 实况重写 MODULE-DESIGN 宏表（名字、参数、返回），脱敏一列改为「加盐 SHA-256，前 16 位 hex，加 `h_` 前缀」；与 INTERFACE-DESIGN 只保留一份（另一份引用）。）
- 证据：docs/business/MODULE-DESIGN.md:28-31 列 `fr2052a_hqla_classification` / `fr2052a_maturity_bucket` / `fr2052a_mask_pii`（返回 `h_xxxx` (HMAC 脱敏)）/ `fr2052a_fx_convert`；代码实际宏为 dbt/macros/fr2052a_rules.sql:9 `maturity_bucket`、:24 `hqla_level`、:36 `hqla_haircut`、:47 `customer_segment`、:59 `deposit_product_category` 与 dbt/macros/pii.sql:16 `mask_pii`，没有任何 `fr2052a_*` 前缀宏；脱敏模板见 dbt/dbt_project.yml:32 `sha2(concat_ws('|', '{salt}', cast({column} as string)), 256)`（加盐 SHA-256，非 HMAC），docs/business/DOMAIN-LANGUAGE.md:70 也写「加盐 SHA-256」。
- 为什么算问题：宏名是跨模块契约，四个名字里三个凭空捏造、第四个算法写错；与 INTERFACE-DESIGN 的宏表（:18-22，与代码一致）直接冲突，读者无法判断哪份是真的。

**5. MODULE-DESIGN 的 Python CLI 契约表四项签名与实现不符，且与 INTERFACE-DESIGN 互相打架**（高",
      "fix": "以各脚本 argparse/头注为准重写两张表（含退出码与默认值），MODULE-DESIGN 只留「有哪些脚本」的索引，签名放 INTERFACE-DESIGN 单源。）
- 证据：docs/business/MODULE-DESIGN.md:37-41：`generate_sample_data.py | 无 | ...`、`load_ref_tables.py | --target iceberg|postgres`、`run_dq_rules.py | --layer owd|ows|ads`、`replay_ods_to_kafka.py | --topic <name> --file <path>`。代码：python/generators/generate_sample_data.py:266-278 有 `--out/--gl-break-amount/--correct-deposit-record/--correct-deposit-amount`；python/lakehouse/load_ref_tables.py:50-54 只取位置参数 `argv[1]`（目录不存在返回 2），无 `--target`；python/validators/run_dq_rules.py:135 只有 `--batch-id`；python/producers/replay_ods_to_kafka.py:53-54 是 `--data-dir/--config` 且均有 default。INTERFACE-DESIGN.md:65,78,99,111 的描述与前两者又都不同。
- 为什么算问题：接口契约是本类项目的对外约定，三份文档对同一个 CLI 给出三套签名，等于没有契约；照文档调用会直接失败。

**6. INTERFACE-DESIGN 明确写「mask_pii 尚未实现」，与 E7 已交付的脱敏机制矛盾**（高",
      "fix": "删掉该注，宏表补 `mask_pii(column_name)` 一行；`fr2052a_fx_convert` 若确实未实现，保留单独一句说明（并登记在 KNOWN-ISSUE 偏离表）。）
- 证据：docs/business/INTERFACE-DESIGN.md:24 \"注：原设计中的 `fr2052a_mask_pii` 和 `fr2052a_fx_convert` 尚未实现，将在后续阶段补充。\"；实际 dbt/macros/pii.sql:16-18 已实现 `mask_pii` 且被 OWD 模型使用，docs/rules/ACCEPTANCE-CHECKLIST.md:65 安全 #2 以「OWD 层用加盐 SHA-256；明文唯一落点 secure.fr2052a_pii_map」判 ✅。
- 为什么算问题：同一机制在同一套文档里一处说没实现、一处当已验收；会让评审者误判 E7 完备性。

**7. check_source_arrival 任务实际恒成功，与「确认 7 张源文件到位」的声称不符（静默失败）**（高",
      "fix": "改成计数并断言（如 `test \"$(ls -1 .../*.csv | wc -l)\" -eq 7`），失败以非 0 退出。）
- 证据：deploy/server1/airflow/dags/fr2052a_daily_batch.py:72-76 命令为 `ls -1 /opt/fr2052a-app/sample_data/ods/*.csv | wc -l`，doc_md=\"确认 7 张 ODS 源文件全部到位，避免空跑一整轮\"；docs/business/INTERFACE-DESIGN.md:211 也写 `check_source_arrival 确认 7 张 ODS 源文件到位，避免空跑一整轮`。`wc -l` 恒退出 0，文件缺失或不足 7 个都不会失败。
- 为什么算问题：正是术语表 docs/business/DOMAIN-LANGUAGE.md:69 定义的「静默失败」：零命中与读不到分不出来；文档声称的防空跑机制没有落地。

**8. PROJECT.md 的 Topic 数量写成 7，与配置、DATA-DESIGN、MODULE-DESIGN 的 10 条冲突**（中",
      "fix": "PROJECT.md 改为「10 个 Topic（7 个有生产者）」，并只保留 config 为唯一声明来源。）
- 证据：docs/business/PROJECT.md:97 \"| Kafka Topic | `config/pipeline_topics.json` | 7 个 Topic 契约 |\"；config/pipeline_topics.json 实际 `topics` 数组 10 条；docs/business/DATA-DESIGN.md:66 \"10 个主题，其中 7 个有生产者的落 bronze\"；docs/business/MODULE-DESIGN.md:45-56 列出 10 行。
- 为什么算问题：单源被抄成三份后出现两个数；「7」是被淘汰的旧值（ACCEPTANCE-CHECKLIST.md:21 自述本轮把主题清单 7 条→10 条），说明校正只改了部分文档。

**9. PROJECT.md 的 Kafka 验证命令用了内网监听端口 9092，从 dev 机/Server 1 不可用；文档未提 9094**（中",
      "fix": "端口表补 9094，验证命令改成 `--bootstrap-server 192.168.17.24:9094 --list`，并标注 9092 仅供容器内使用。）
- 证据：docs/business/PROJECT.md:108 \"| Kafka | 9092 | `kafka-topics --bootstrap-server 192.168.17.24:9092 --list` |\"；config/pipeline_topics.json:5 声明 `\"bootstrap_servers_external\": \"192.168.17.24:9094\"`；deploy/server2/docker-compose.yml:28-29 `KAFKA_LISTENERS: INTERNAL://:9092,EXTERNAL://:9094` / `KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka:9092,EXTERNAL://${SERVER2_HOST}:9094`（9092 的广告地址是容器名 `kafka`）。
- 为什么算问题：跨机客户端连 9092 会被回给不可解析的 `kafka:9092`，验证命令不可用；端口表漏掉真正对外监听 9094，属机制漏写。

**10. Ref 层落库契约写「按主键 MERGE」，实现是整表覆盖写**（中",
      "fix": "DATA-DESIGN §2.3 Ref 行改为「整表 INSERT OVERWRITE（覆盖写）」。）
- 证据：docs/business/DATA-DESIGN.md:187 \"| Ref | `load_ref_tables.py` 批加载 | 所有层 | 按主键 MERGE |\"；python/lakehouse/load_ref_tables.py:3-5 docstring \"用整表覆盖写（INSERT OVERWRITE）保证幂等：重复跑任意多次，结果一致，不会翻倍\"；docs/business/INTERFACE-DESIGN.md:80 也写 \"**幂等**：INSERT OVERWRITE，重复运行不重复写入\"。
- 为什么算问题：幂等策略是审计关注点，两份文档给出两种机制，其中一份与代码不符；「MERGE」还会让读者以为有主键去重逻辑。

**11. 失效引用：AGENTS.md 指向不存在的 `docs/tables/` 与 DEVELOP-FLOW §4**（中",
      "fix": "把表契约位置改为真实落点（sql/iceberg/*.sql + DATA-DESIGN §2），或在 docs/tables/ 建最小契约；DEVELOP-FLOW 补 §4 条目模板（或把引用改到实际章节号）。）
- 证据：AGENTS.md:35 \"4. 涉及数据变更 → 读对应 `docs/tables/{table}.md` 表契约\"（`ls -d docs/tables` → No such file or directory）；AGENTS.md:74 \"条目模板见 `docs/rules/DEVELOP-FLOW.md` §4\"，而 docs/rules/DEVELOP-FLOW.md 的章节只有「阶段定义 / 阶段 4 子步骤 / 质量检查点（§5.3、§5.4）/ 变更流程 / 回滚策略」，无 §4。
- 为什么算问题：AGENTS §2 是「动手前必做」的上下文加载清单，第 4 项指向空目录；变更留痕模板不存在，条目无法按规落。

**12. AGENTS.md 引用的 `pre_commit_gate.py` 不存在**（中",
      "fix": "删掉该句，或把技能名校验真的写进 pre-commit（技能目录可枚举）。）
- 证据：AGENTS.md:116 \"下表只接**技能库里真实存在**的技能，名字必须可查，`pre_commit_gate.py` 会检查。\"；全仓 find/grep 无 `pre_commit_gate.py`（.githooks/ 下只有 pre-commit 壳脚本，调用 make lint）。
- 为什么算问题：用「有个脚本会检查」给规则背书，而脚本不存在；同一段文字同时声称技能名可机器校验，削弱 §9/§10 两张表的可信度。

**13. 变更留痕机制三处口径不一：目录为空、路径未被项目地图承认**（中",
      "fix": "二选一：补条目文件并在 PROJECT/AGENTS/README 的目录清单登记；或删掉该机制，改为只追加 docs/build-log.md，并在 AGENTS 说明。）
- 证据：AGENTS.md:74 \"功能或契约变更，在 `docs/changes/{module}.md` **追加**一条目\"、AGENTS.md:105 \"新增模块变更追加到 `docs/changes/{module}.md`\"、docs/rules/PROJECT-STRUCTURE.md:12 \"| `docs/changes/` | 变更留痕（追加式） | 全员 |\"；实际 `docs/changes/` 为空目录（0 文件），docs/business/PROJECT.md:29-43 目录树、AGENTS.md §9 项目地图、README.md:92-95 目录速览均未出现该目录。
- 为什么算问题：E6/E7 属功能变更却无任何条目；三份「定义目录职责」的文档里两份承认它、两份不承认，路径成了孤儿。

**14. AGENTS.md 把构建日志写成「E0-E5」，与实际含 E0–E7 冲突**（低",
      "      "fix": "AGENTS.md 改为「E0–E7 构建日志」，或按实际保留范围写「构建日志（原始路径，用户要求保留）」。）
- 证据：AGENTS.md:105 \"| 变更留痕 | `docs/build-log.md` | E0-E5 构建日志（原始路径，用户要求保留）\"；docs/build-log.md:646 `## E6 合规演示剧本`、:699 `## E7 治理收口`、:727 `## 收口：一键初始化与文档校正`；README.md:129-130 也记 E6/E7 为 ✅ 完成。
- 为什么算问题：同一份文件覆盖范围两说，读者会以为 E6/E7 未入日志。

**15. PROJECT-STRUCTURE 把 DAG 写成就地目录且标注「待实现」，与实现和 README 的 E5 ✅ 矛盾**（中",
      "fix": "两处路径改为 deploy/server1/airflow/dags/，删「待实现」。）
- 证据：docs/rules/PROJECT-STRUCTURE.md:52 \"| 调度定义 | `airflow/dags/` | DAG 定义（待实现）|\"；实际 DAG 在 deploy/server1/airflow/dags/（5 个文件），README.md:128 记「E5 编排接管（Airflow DAG + 数据质量引擎）✅ 完成」，docs/business/PROJECT.md:98 也写「| Airflow DAG | `airflow/dags/` | 5 个 DAG 定义 |」（路径同样错）。
- 为什么算问题：收口点表是「东西在哪」的单源，路径错会让人找不到 DAG；「待实现」与已完成状态直接冲突。

**16. PROJECT-STRUCTURE 的部署命名示例文件不存在，文件命名规范与 Python 实况相反**（低",
      "fix": "示例改为 Dockerfile/compose 的真实文件名，Python 一列按 snake_case 写（与 CODING-STANDARD 对齐）。）
- 证据：docs/rules/PROJECT-STRUCTURE.md:31 \"| 部署 | `docker-compose-core.yml`、`docker-compose-compute.yml` |\"，实际只有 deploy/server1/docker-compose.yml 与 deploy/server2/docker-compose.yml；同文件:23 \"文件：PascalCase 或 kebab-case\"，而 python/ 下 33 个文件全为 snake_case（如 generate_sample_data.py），docs/rules/CODING-STANDARD.md:9-13 也规定 snake_case。
- 为什么算问题：命名规范自相矛盾且与实际文件不符；引用不存在的文件名会让 `sync-deploy`/部署核对看起来像缺件。

**17. 5 个 DAG 只在接口文档里登记了 3 个，gl_reconciliation 与 submission 无契约条目**（中",
      "fix": "INTERFACE-DESIGN 补 5.4 fr2052a_gl_reconciliation、5.5 fr2052a_submission（触发时刻、任务链、参数、失败语义）。）
- 证据：docs/business/INTERFACE-DESIGN.md §5 只含 5.1 fr2052a_daily_batch、5.2 fr2052a_realtime_alert、5.3 fr2052a_backfill_and_restate；实际有 5 个 DAG：fr2052a_gl_reconciliation.py（dag_id=\"fr2052a_gl_reconciliation\"，schedule=\"0 7 * * *\"）、fr2052a_submission.py（dag_id=\"fr2052a_submission\"，schedule=\"30 7 * * *\"，任务 check_gate → generate_and_submit → verify_submission）；docs/business/PROJECT.md:98 亦称「5 个 DAG 定义」。
- 为什么算问题：报送 DAG 与 GL 对账 DAG 是放行链条的关键一环（gate 与 verify-submission 只在这里跑），契约未登记等于重要机制漏写；ACCEPTANCE-CHECKLIST.md:21 却声称「DAG 任务流按实现校正」已完成。

**18. INTERFACE-DESIGN 的日批任务流漏登 owd_scd2、verify_rbac，DAG 自带 docstring 也漏 publish_access 与 verify_rbac**（中",
      "fix": "以 fr2052a_daily_batch.py 的 `>>` 链为唯一来源重写 INTERFACE-DESIGN §5.1 与该 DAG 的 docstring。）
- 证据：docs/business/INTERFACE-DESIGN.md:210-224 的任务流为 check_source_arrival → load_ref → replay_ods → load_bronze → dbt_run → pii_vault → lineage → dq_validate → publish_access → export_pg → liquidity_monitor → verify_* → pipeline_health（无 owd_scd2，无 verify_rbac 明细）；实际 fr2052a_daily_batch.py:107-111 有 owd_scd2、:152-156 有 verify_rbac，而该文件:5-7 的 docstring 链又写成 \"... → dq_validate → export_pg → liquidity_monitor → verify_bronze → verify_silver → verify_ads → pipeline_health\"（漏 publish_access、verify_rbac）。
- 为什么算问题：同一份任务链在文档、代码 docstring、代码实现三处三个版本；SCD2 归并属于「重要机制」，在接口契约里无席位。

**19. INTERFACE-DESIGN 把 run_dq_rules 的 --batch-id 标为必填、replay 两个参数标为必填，代码里都有默认值**（低",
      "fix": "要么把脚本改成必填（required=True），要么文档把「必填」改为「默认值」。）
- 证据：docs/business/INTERFACE-DESIGN.md:99 \"| `--batch-id` | string | 是 | ETL 批次号 |\"；python/validators/run_dq_rules.py:135 `parser.add_argument(\"--batch-id\", default=\"UNKNOWN\", help=...)`。docs/business/INTERFACE-DESIGN.md:111-112 把 `--data-dir` 与 `--config` 标「是」；python/producers/replay_ods_to_kafka.py:53-54 两者均有 default（DEFAULT_DATA_DIR / DEFAULT_CONFIG）。
- 为什么算问题：必填性是调用方契约；实际缺省会静默用默认值，与文档声明的严格性不符（也正是本项目定义的静默失败形态）。

**20. DATA-DESIGN 的 VDQ 表只列 20 条规则中的 9 条且未标注节选**（中",
      "fix": "标注「节选，全集见 ref.ref_validation_rules」，或补全 20 条并按 apply_layer 分组。）
- 证据：docs/business/DATA-DESIGN.md:229-241 列 VDQ-001/002/003/006/010/013/016/017/018 共 9 条；实际规则集 sample_data/ref/ref_validation_rules.csv 有 20 条（VDQ-001…VDQ-020，含 VDQ-004/007/009/014/015/020 等 7 条 WARNING），且代码按 apply_layer 分 ODS/OWD/OWS/ADS 四层执行（python/validators/run_dq_rules.py）。
- 为什么算问题：「哪些规则在管事」是质量闸的判据来源；文档给出的是子集却不说明，漏掉的全部 WARNING 级规则无法被评审看到。

**21. DOMAIN-LANGUAGE 对放行闸退出码漏写 3，与同文档退出码表和实现不符**（低",
      "fix": "该行补「3 判不了，按不放行处理」。）
- 证据：docs/business/DOMAIN-LANGUAGE.md:61 \"| 放行闸 | submission gate | 决定「这次报送能不能生成文件」的那道闸 | `check_submission_gate.py`，退出码 0 放行 / 2 阻断 |\"；同文件:134-139 §3.3 列出 0/1/2/3（3=无法判定）；python/validators/check_submission_gate.py:27-29 定义 EXIT_PASS=0 / EXIT_HALTED=2 / EXIT_UNKNOWN=3，:64 在连不上库时 `return EXIT_UNKNOWN`；docs/business/INTERFACE-DESIGN.md:255-259 也列 0/2/3。
- 为什么算问题：「判不了」这一支恰恰是最危险的状态（按不放行处理），术语表却把它从放行闸定义里删掉了。

**22. DOMAIN-LANGUAGE 描述的 submission_status 状态流转在代码里没有实现路径**（低",
      "fix": "把状态流转写成「生成即按回执落 ACCEPTED/REJECTED；GENERATED/SUBMITTED 为保留值，当前无写入方」，或让脚本先写 GENERATED 再按回执更新。）
- 证据：docs/business/DOMAIN-LANGUAGE.md:113 \"| `submission_status` | `GENERATED` / `SUBMITTED` / `ACCEPTED` / `REJECTED` | ... | 生成即 `GENERATED`，提交后按回执更新 |\"；python/exporters/generate_submission.py:322 写入时直接 `\"submission_status\": \"ACCEPTED\" if receipt.get(\"accepted\") else \"REJECTED\"`，全仓无任何写入 GENERATED/SUBMITTED 的代码；这两个值只出现在 sql/postgres/10_control_tables.sql:126 的列注释里。
- 为什么算问题：同一文档的变更记录（:220）自称已「修正报送枚举与实现不一致」，但修正方向是给文档加值，而不是对齐实现；状态流转描述与代码相反。

**23. 验收清单的性能证据「8 次 Spark 作业」与实跑环节数不符**（低",
      "fix": "改为「10 次 spark-submit」或直接贴 `--list` 与日志摘要。）
- 证据：docs/rules/ACCEPTANCE-CHECKLIST.md:55 \"重置后重跑实测 **3 分 11 秒**（16 个环节，含 dbt 16 个模型与 8 次 Spark 作业）\"；默认 16 环节里经 `bash spark-submit-fr2052a.sh` 的是 10 个：deploy/server2/run-daily-pipeline.sh 的 ref-load/ods-replay/bronze-load/dq-rules/export-pg/owd-scd2/verify-bronze/verify-silver/verify-scd2/verify-ads（其余 6 个走 `./venv/bin/python` 直跑）。
- 为什么算问题：证据列里的数字是可核对的事实，写少两个会让「性能达标」的推理链失真；也说明证据是凭记忆写的而不是从日志抄的。

**24. 验收清单存在不可实核/自循环判据，且签名栏全空却给「独立复核」判 ✅**（中",
      "fix": "性能 #3 换成可执行动作（如 `time bash deploy/server2/run-daily-pipeline.sh gate health` 并记录耗时），CR #2 改成机器判据或标人工项，:46 补真实输出或改标 🔍，签署栏留空则对应判据标 ⬜。）
- 证据：docs/rules/ACCEPTANCE-CHECKLIST.md:57 性能 #3 标准「放行闸与巡检响应 端到端 ≤ 30 秒」、状态 🔍、证据 \"不适用「API p95」：本项目无对外接口。替代判据即本条\"（用本条替代本条）；:14 CR #2 标准「注释解释「为什么」，出现与实现不符的注释即打回」无机器判据；:46 数据层 #3 的「证据」栏填的是替代标准文字（\"不适用：Iceberg 不支持外键...\"）而该表表头自述「『证据』一列写的是本轮实核时看到的输出」；:70-77 签名确认表四行（架构师/数据工程师/测试工程师/合规官）姓名、日期、签名全空，而 :30 发布就绪 #3 判 ✅「由未参与编写的一方实跑关键判据后给结论」。
- 为什么算问题：清单自己定了两条纪律（判据必须可执行、没有证据的勾选不算通过），却在四处破例；签署栏空白使「独立复核」这一最重的判据没有责任人。

**25. 需求侧未落地的监控与 BI 选型没有在偏离登记表登记**（中",
      "fix": "在偏离表补两行（监控：Grafana/Prometheus → 用 pipeline_health.py 直出结论；BI：Superset → 无），并按本节要求补代价与回退汇总行。）
- 证据：requirements/[01]架构设计.md、requirements/[04]环境设计.md 提到 Grafana 与 Prometheus；requirements/[98]补充资料 提到 Superset。docs/business/KNOWN-ISSUE.md:44-59 的「规范偏离（本项目 vs 上游）」表只有 13 行（双轨制、存储分工、requirements 归档、报表主键、明细外键、版本历史、报送文件、覆盖写 truncate、自研 DQ/GE、血缘/DataHub、DQ 日志粒度、lint 排除、提交闸、单元测试），无监控栈与 BI 两条；全仓无 Grafana/Prometheus/Superset 任何部署或代码。
- 为什么算问题：该表自述是偏离记录的「唯一落点」且「禁止无登记地默默降标准」；监控与可视化是上游明确要求的能力，缺失却未登记，等于无登记降标准。

**26. 偏离登记表自己定的格式规则与该文件实际格式、表内容三方不一致**（低",
      "fix": "把规则 2 改成与本文件实际锚点格式一致的写法（指向锚点表行），或给锚点补 `### #slug` 标题并让偏离表逐行引用。）
- 证据：docs/business/KNOWN-ISSUE.md:38 \"**处置列指向真实锚点**：写成本文件里已有的 `### #slug` 定义；只写「已说明」不合格。\"；但本文件的锚点是表格行（:11-18 `| #pg18-data-dir-change | ...`），不存在任何 `### #slug` 标题；且偏离表 :46-59 的处置列全部指向外部文档（如 \"见 `[01]架构设计.md` §1.5\"、\"见 `[04]环境设计.md` §4.3\"），无一行指锚点。
- 为什么算问题：登记处定的四条前提自己没满足，后续条目没有可依据的写法；引用外部 §号还引入了未在本仓校验的锚点。

**27. 巡检覆盖清单三处不一致（代码 8 项，两份文档只写 6 项）**（低",
      "fix": "以 pipeline_health.py 的实际输出为准统一四处（或改为「不在此抄清单，见脚本」，符合单源纪律）。）
- 证据：python/governance/pipeline_health.py:205-239 输出 8 项：熔断闸、校验（质量）、预警（含阻断级）、报送、重述、实时事件、PG 连接、Kafka 滞后、磁盘；docs/business/PROJECT.md:19 与 AGENTS.md:26 写「熔断 / 质量 / 报送 / 滞后 / 连接 / 磁盘」（漏重述、实时事件），docs/business/DATA-DESIGN.md:31 同样只列 6 项，而 docs/rules/ACCEPTANCE-CHECKLIST.md:37 列了 8 项。
- 为什么算问题：巡检面写窄会让评审认为重述与实时事件无人观测；同一机制四种枚举口径。

**28. 「九项核心文档」与实际七份不符，且未见登记**（低",
      "fix": "统一写「七份核心文档（九文档体系的本项目子集）」，或补齐缺失两型文档。）
- 证据：README.md:93 \"│   ├── business/              # 工程文档（九项核心）\"、docs/business/PROJECT.md:30 \"│   ├── business/              # 九项核心文档\"、AGENTS.md:103 \"| 业务文档 | `docs/business/` | 项目说明 / 模块 / 数据 / 接口 / 术语 / 变更 / 已知问题 |\"、docs/rules/ACCEPTANCE-CHECKLIST.md:31 \"README + 九项核心文档 + 构建日志齐全\"（证据列为 \"`docs/business/` 七份 + `docs/rules/` 四份 + `docs/build-log.md`\"）与 :22；实际 docs/business/ 为 7 个文件。
- 为什么算问题：验收条目名字与实际计数不一致（九 vs 七），容易被当成缺件；若确有取舍，按 KNOWN-ISSUE 纪律应登记。

**29. INTERFACE-DESIGN「其他脚本」清单漏掉半数已交付脚本，与质量检查点表对不上**（低",
      "fix": "§3.6 改为「脚本全集见 python/ 目录树」+ 按 python/ 子目录分组列全，或补一张 script → 用途 → 退出码 的表。）
- 证据：docs/business/INTERFACE-DESIGN.md:114-123 只列 6 个脚本（run_sql_file.py、verify_bronze.py、verify_silver.py、verify_gold.py、verify_ods_schema.py、export_gold_to_pg.py）；实际 python/ 下 33 个 .py，未登记的有 maintain_tables.py、build_pii_vault.py、clear_dq_batch.py、summarize_realtime_alerts.py、verify_scd2.py、verify_rbac.py、inspect_catalog.py、time_travel.py、restate.py、liquidity_monitor.py 等；docs/rules/DEVELOP-FLOW.md:24,44,58 又把 verify-scd2、verify-rbac 当作质量检查点判据引用。
- 为什么算问题：核对脚本是验收判据的载体，接口文档索引不到它们，读者只能靠猜；与 DEVELOP-FLOW 的引用形成单边死链。

**30. 两处 Markdown 锚点失效（自引用错锚、指向不存在的标题）**（低",
      "fix": "修正两处锚点（PROJECT 改为 #运行环境--验证命令 或删自引用；DATA-DESIGN 改为 #6-血缘与监管映射接口）。）
- 证据：docs/business/PROJECT.md:115 \"详细验证清单见 [docs/business/PROJECT.md](PROJECT.md#运行环境) 与 [docs/build-log.md](../build-log.md)。\" —— 该文件对应标题是 :100 \"## 运行环境 + 验证命令\"（锚点应为 #运行环境--验证命令），且链接指向自身；docs/business/DATA-DESIGN.md:254 \"见 [INTERFACE-DESIGN.md](INTERFACE-DESIGN.md#血缘渲染)。\" —— INTERFACE-DESIGN.md 无「血缘渲染」标题（:242 为 \"## 6. 血缘与监管映射接口\"）。
- 为什么算问题：文档互链是九文档体系的可达性基础，锚点错会让「读完再动手」的链路断在第一跳。

**31. README 常见任务表把「对账」与实时扫描混成一条命令，且对账无独立环节**（低",
      "fix": "该行拆成「单跑接入 | ods-replay」「单跑实时扫描 | realtime-scan」，对账另写一行指向 verify-ads 或 GL 对账 DAG。）
- 证据：README.md:77 \"| 单跑接入或对账 | `bash run-daily-pipeline.sh ods-replay realtime-scan` |\"；run-daily-pipeline.sh 的 STEP_DESC 中 `realtime-scan`=\"实时扫描：消费核心存款主题，识别大额未保险敞口\"（无 gl-recon 环节）；GL 对账结论由 dbt 模型 ads_gl_reconciliation 产出、由 verify-ads（verify_gold.py:177 check_gl_reconciliation）与 fr2052a_gl_reconciliation DAG 核对。
- 为什么算问题：命令与用途不符，会让执行者以为跑 realtime-scan 就完成了对账；「对账」这一重要机制在跑批入口无对应环节说明。

**32. 仓库根多出一份未登记在教学文档体系之外的《教学文档-小白版.md》**（低",
      "fix": "在 README 目录速览与 PROJECT-STRUCTURE 根目录清单登记该文件（或注明它是派生产物、可重建）。）
- 证据：仓库根目录存在 `教学文档-小白版.md`（19.6K），而 README.md:87-117 的目录速览、docs/business/PROJECT.md:24-72 的目录树、AGENTS.md §9 项目地图（:98-111）与 docs/rules/PROJECT-STRUCTURE.md:35 的根目录清单（\"README.md + AGENTS.md + .gitignore + .gitattributes\"，实际根目录还有 Makefile、pyproject.toml、教学文档-小白版.md）均未收录。
- 为什么算问题：根目录清单与实况不一致，新人按 PROJECT-STRUCTURE 的清单核对会认为多出文件；教学文档属于「人类入口」，未登记就没有维护责任人。

</details>

**子代理声明未验证的点**：所有依赖运行态的结论均未验证：服务器 192.168.17.22/24 上的容器、Kafka 主题、Iceberg 表、PG 表内容与行数（禁 docker/ssh），因此「16 个环节全绿」「3 分 11 秒 / 4 分 04 秒」「单批次固定 20 行」「基线 8/8 PAS；ACCEPTANCE-CHECKLIST.md:38 的 CI 证据（运行 35237994950，head_sha 37a5d26，success；反向验证 35238108100 failure）未在 GitHub 侧复核；本地 git log 确有 37a5d26 提交，但；ACCEPTANCE-CHECKLIST.md:34 「两台服务器均无漂移」需在 dev 机跑 `bash deploy/sync-deploy.sh --check`（涉及 ssh 到两台服务器），未执行。；ACCEPTANCE-CHECKLIST.md:44 「16/16 表结构匹配」需在 Server 2 跑 verify_ods_schema.py；静态核对显示 LAYOUT=((ref, ref),(bronze, ods)) 对应 9 张 ref + 7 张 ODS = 1；requirements/ 只做了技术选型关键词检索（DataHub / Great Expectations / Grafana / Prometheus / Superset / BIGSERIAL / 80% / Flink / Hive 等），未逐篇通读 6934 行需求；docs/build-log.md 仅核对了标题层级与 E0–E7 存在，未逐段核对日志内的数字与命令。；make lint 已实跑通过（ruff check + ruff format --check + mypy，`39 files already formatted`、`Success: no issues found in 39 source files`），未加 --fix；INTERFACE-DESIGN §5.3 报送 DAG 参数（report_date/entity_code/reason/requested_by/approved_by/effective_date）与 fr2052a_backfill_and_restate.py 的 p

---

## 后续处置（2026-09-18）

本报告第 2 条发现曾经以「补机器闸」的方式部分收口：新增 `.githooks/commit-msg`，要求 agent 会话的提交信息带 `[AI]` 前缀。用户随后裁决撤销该机制，理由是不该在提交信息里体现执行者是谁，agent 提交即用户提交。钩子已删除，`AGENTS.md` §5 与 `docs/rules/CODING-STANDARD.md` 的措辞同步更正，代码文件头的 `[AI-GENERATED]` 与 `reviewed_by` 保留。本报告其余处置结论不变，撤销过程见 `docs/changes/engineering.md` 的 `commit-message-policy` 条目。
