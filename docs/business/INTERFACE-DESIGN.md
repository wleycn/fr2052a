# 接口设计

> 接口契约唯一真源。只写对外/跨边界的契约，模块内部函数划分见 MODULE-DESIGN.md。

## 1. 通用约定

| 项 | 约定 |
|---|---|
| 幂等 | 写操作必须声明幂等键与语义 |
| 超时 | 调用方超时 30s，服务端处理时限 5min |
| 版本策略 | 破坏性变更 = 升版本并行；旧版本下线需调用量归零佐证 |
| 错误码 | 4xx 客户端（参数/权限/冲突）、5xx 服务端；业务错误码在 §4 列全 |

## 2. dbt Macro 接口

| # | 宏名 | 参数 | 返回类型 | 用途 | 状态 |
|---|------|------|----------|------|------|
| 1 | `hqla_level` | `security_type VARCHAR, rating VARCHAR` | `VARCHAR` | HQLA 等级分类 | ✅ 已实现 |
| 2 | `hqla_haircut` | `hqla_level VARCHAR` | `NUMERIC` | HQLA 折扣率 | ✅ 已实现 |
| 3 | `maturity_bucket` | `days_expr` | `VARCHAR` | 到期分桶计算 | ✅ 已实现 |
| 4 | `customer_segment` | `customer_type_expr` | `VARCHAR` | 客户细分归一 | ✅ 已实现 |
| 5 | `deposit_product_category` | `deposit_type_expr` | `VARCHAR` | 存款产品归类 | ✅ 已实现 |
| 6 | `is_affiliate_counterparty` | `counterparty_type_expr` | `BOOLEAN` | 集团内往来标记 | ✅ 已实现 |

> 注：宏名不带 `fr2052a_` 前缀。脱敏已实现为 `mask_pii`（加盐 SHA-256，见 `dbt/macros/pii.sql`）；汇率折算没有对应宏，由 OWD 模型 join `stg_fx_rates` 完成。

### 2.1 `hqla_level`

- **输入**：`security_type`（如 'TREASURY', 'AGENCY_DEBT', 'CORP_BOND'）、`rating`（如 'AAA', 'AA', 'A'）
- **输出**：`LEVEL_1` / `LEVEL_2A` / `LEVEL_2B` / `NON_HQLA`
- **规则**：见 DATA-DESIGN.md §3.2

### 2.2 `hqla_haircut`

- **输入**：`hqla_level`（LEVEL_1/LEVEL_2A/LEVEL_2B/NON_HQLA）
- **输出**：折扣率（0.00 / 0.15 / 0.50 / 1.00）

### 2.3 `maturity_bucket`

- **输入**：剩余天数表达式
- **输出**：`O/N` / `1-7D` / `8-30D` / `31-90D` / `91-180D` / `181D-1Y` / `>1Y` / `OPEN`
- **规则**：见 DATA-DESIGN.md §3.1
- **同族宏 `behavioral_bucket`**：输入产品类别 + 剩余天数表达式，活期/储蓄不看天数直接归 `O/N`。存款表走它，其余五张明细走 `maturity_bucket`

### 2.4 `customer_segment`

- **输入**：客户类型原始值（IND/CORP/FI/GOV/AFFIL/OTHER）
- **输出**：`RETAIL` / `CORPORATE` / `FINANCIAL` / `SOVEREIGN` / `AFFILIATE` / `OTHER`

### 2.5 `deposit_product_category`

- **输入**：存款产品类型（CHK/SAV/MMDA/CD/TIME）
- **输出**：`DEMAND` / `SAVINGS` / `CD` / `TIME` / `OTHER`

## 3. Python CLI 接口

> 以下接口定义基于实际代码。签名以 `--help` 输出为准。

### 3.1 `generate_sample_data.py`

```bash
python -m generators.generate_sample_data --out <dir> [--report-days <N>] [--gl-break-amount <N>]
```

| 参数 | 类型 | 必填 | 默认 | 说明 |
|------|------|------|------|------|
| `--out` | path | 否 | `../sample_data` | 输出目录 |
| `--report-days` | int | 否 | `1` | 生成几个连续日历日的多期数据，末尾一期是锚定报告日（`2026-09-16`）|
| `--gl-break-amount` | int | 否 | `0` | GL 对账缺口（用于演示对账失败场景）|
| `--correct-deposit-record` | string | 否 | 空 | 重述剧本：要修正的存款记录号，与下一个参数必须同时给出 |
| `--correct-deposit-amount` | float | 否 | 空 | 重述剧本：把该记录的本金改成这个数（记账币种原币金额）|
| `--inject-missing-fx` | string | 否 | 空 | 故意不写这些币种的汇率行（逗号分隔），用于演示「缺汇率必须失败」|

`--inject-missing-fx` 造的是**刻意的缺陷数据**：汇率表少这些行，但业务数据仍会照常抽到这些币种，于是 dbt 的汇率覆盖断言报红、日批在这一环停住。它不是一个「宽容模式」，用完必须重新生成正常数据。

`--report-days N` 的语义有两层：

- **加期不扰动已有期**：每期用 `(表名, 报告日)` 派生的独立随机源，同一个报告日的数据与「本次生成了几期」无关。回归检查因此可以拿 1 期与 2 期的同一报告日逐字节比对。
- **锚定时点**：`--correct-deposit-record` 只作用于锚定报告日那一行。同一账户在不同报告日是同一个源记录号（bronze 主键是 `source_system + source_record_id + report_date`），不限定日期会静默改掉其他期的同号记录。

汇率表按报告日逐期出行（每期 10 个币种）：多期数据里每一期都要能折算出 USD，缺了会被汇率覆盖断言判为缺汇率。

**退出码**：`0` = 成功，`1` = 失败

### 3.2 `load_ref_tables.py`

```bash
python lakehouse/load_ref_tables.py <csv_dir>
```

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `csv_dir` | path | 是 | 参考数据 CSV 目录 |

**幂等**：INSERT OVERWRITE，重复运行不重复写入

**退出码**：`0` 成功 / `1` 有表失败或目录下无 CSV / `2` 目录不存在

### 3.3 `export_gold_to_pg.py`

```bash
python export_gold_to_pg.py --batch-id BATCH-20260916-001
```

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `--batch-id` | string | 是 | 本批批次号，用于核对运行上下文；也可由环境变量 `BATCH_ID` 提供 |
| `--allow-after-submission` | flag | 否 | 允许覆盖「已报送且内容已变」的报告期（重述流程用）；等价环境变量 `ALLOW_EXPORT_AFTER_SUBMISSION=1` |

- 连接信息从环境变量取：`SERVER1_HOST`、`POSTGRES_DB`、`POSTGRES_USER`、`POSTGRES_PASSWORD`
- **幂等**：`truncate=true` 覆盖写 —— 只换数据，保留表上的授权、触发器与库侧迁移列
- **写前业务前提**（任一不过即整批失败、一张表都不碰）：gold 无空表 / 本批运行上下文成立 / 对账无 FAIL / 报表与对账的报告期集合一致 / 已报送期的内容未变
- 「已报送期的内容未变」的判据是**内容指纹**而不是「有没有报送过」：日批本身可重跑，拿「已报送」直接拦会挡掉正常重跑；真出现内容变化（上游修正、口径调整、模型改造后重跑）必须走重述流程，或显式加 `--allow-after-submission` 并在报送说明里写明原因

### 3.4 `run_dq_rules.py`

```bash
python validators/run_dq_rules.py --batch-id <id>
```

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `--batch-id` | string | 否 | ETL 批次号，不传时默认 `UNKNOWN`，仅影响审计表的追溯字段 |

**退出码**：`0` = 无 ERROR 级违规（WARNING 级只提示、不阻断），`1` = 有 ERROR 级违规（阻断）

**覆盖面判定**：ERROR 级规则若因列名不存在跳过部分目标表，覆盖面缩水即判 FAIL，不再因违规数为 0 而 PASS。WARNING 级规则跳过表仍可 PASS，但 detail 会写出跳过了哪些表。

### 3.5 `replay_ods_to_kafka.py`

```bash
python producers/replay_ods_to_kafka.py --data-dir <dir> --config <json>
```

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `--data-dir` | path | 否 | ODS CSV 数据目录，默认 `/opt/fr2052a-app/sample_data/ods` |
| `--config` | path | 否 | Topic 配置文件路径，默认 `/opt/fr2052a-app/config/pipeline_topics.json` |

两个参数都有默认值，跑批脚本按「可不传」调用；默认值指向容器内的挂载路径，所以只在 Server 2 的容器里可用，从别处调用必须显式指定。

### 3.6 其余脚本的全集

脚本全集共 34 个 `.py`，在 `python/` 下按子目录归口。**脚本清单只在本文维护一处**，新增脚本时改这里；有 CLI 契约的脚本已在上面 §3.1–3.5 与 `MODULE-DESIGN.md` 的 Python CLI 表里给出参数与退出码，本表不重复。

| 子目录 | 职责 | 脚本 |
|--------|------|------|
| `python/alerts/` | 预警与熔断判定 | `liquidity_monitor.py`、`realtime_scanner.py`、`summarize_realtime_alerts.py` |
| `python/audit/` | Iceberg 时间旅行审计 | `time_travel.py` |
| `python/consumers/` | Kafka 消费入湖 | `kafka_to_iceberg.py` |
| `python/exporters/` | gold 导出与报送文件生成 | `export_gold_to_pg.py`、`generate_submission.py` |
| `python/generators/` | 样本数据生成（固定种子） | `__init__.py`、`config.py`、`ref_data.py`、`ods_data.py`、`generate_sample_data.py` |
| `python/governance/` | 治理：运行上下文、权限、PII、血缘、巡检 | `run_context.py`、`publish_access.py`、`verify_rbac.py`、`build_pii_vault.py`、`render_lineage.py`、`pipeline_health.py` |
| `python/lakehouse/` | 入湖、建模、核对、重述、表维护 | `load_ref_tables.py`、`run_sql_file.py`、`owd_scd2.py`、`verify_bronze.py`、`verify_silver.py`、`verify_ods_schema.py`、`verify_gold.py`、`verify_scd2.py`、`restate.py`、`maintain_tables.py`、`inspect_catalog.py` |
| `python/producers/` | ODS 重放到 Kafka | `replay_ods_to_kafka.py` |
| `python/validators/` | 质量规则、放行闸、回执核对 | `run_dq_rules.py`、`clear_dq_batch.py`、`check_submission_gate.py`、`verify_submission.py` |

> `verify_*` 系列在核对对象为零时退出 `1`（零命中不算通过）。

## 4. Kafka Topic 契约

主题清单的**唯一声明来源**是 `config/pipeline_topics.json`：它含内网与外网地址、分区数、源系统、落点表与生产者标志。建主题脚本、重放生产者与入湖消费者都读这一份。

载荷的统一约定是「**投递的就是落点表的行**」：`python/producers/replay_ods_to_kafka.py` 用 `to_json(struct(整行列))` 把整行序列化成消息 value，key 取主键；消费者 `python/consumers/kafka_to_iceberg.py` 反序列化后 MERGE 进落点表。因此**每个主题的字段清单不在本节重复**，看落点表契约即可（`docs/tables/{表名}.md`）。

| 主题 | 源系统 | 落点表（字段见该表契约） | 有源系统生产者 |
|------|--------|--------------------------|----------------|
| `core_banking_txns` | `CORE_BANKING` | `bronze.ods_deposits` | 是 |
| `loan_book` | `LOAN_SYS` | `bronze.ods_loans` | 是 |
| `treasury_deals` | `TREASURY_SYS` | `bronze.ods_repo_transactions` | 是 |
| `custody_positions` | `CUSTODY_SYS` | `bronze.ods_securities` | 是 |
| `derivatives_trades` | `DERIV_SYS` | `bronze.ods_derivatives` | 是 |
| `gl_entries` | `FINANCE_SYS` | `bronze.ods_gl_balances` | 是 |
| `treasury_cash_position` | `TREASURY_SYS` | `bronze.ods_treasury_cash_position` | 是 |
| `off_bs_commitments` | `OFFBS_SYS` | `bronze.ods_off_bs_commitments` | 是 |
| `market_data_prices` | `MARKET_DATA` | 无：声明保留，本演示未生成对应 ODS 表 | 否 |
| `reference_data_updates` | `REF_DATA` | 无：引用数据走批加载直入 `ref` 命名空间 | 否 |
| `fr2052a_alerts` | `ALERTING` | 无：本项目自己产出的输出主题，载荷见 §4.1 | 否 |

### 4.1 输出主题 `fr2052a_alerts` 的载荷

这个主题不与落点表对应。它有两个写入方，载荷都是 JSON 但形状不同，订阅方按 `alert_code` 是否存在来区分：

| 写入方 | 载荷字段 | 说明 |
|--------|----------|------|
| `python/alerts/liquidity_monitor.py` | `report_date` / `entity_code` / `alert_code` / `severity` / `metric_name` / `metric_value` / `message` / `blocks_submission` | 指标越线预警。投递失败只打 WARN、不改闸状态：闸的判据是库里的记录，Kafka 只是通知通道 |
| `python/alerts/realtime_scanner.py` | 实时扫描的候选明细行（存款明细字段加扫描计算列） | 大额未保险存款与未提取承诺事件 |

## 5. Airflow DAG 接口

### 5.1 `fr2052a_daily_batch`

DAG 只做编排：每个任务 ssh 到 Server 2 调用跑批脚本的同一个环节，编排逻辑只有一份。

```text
run_context_open             运行上下文开口：登记本次跑批的报告日/处理日/生效日
  → check_source_arrival    确认 ODS 源文件张数与主题声明一致（现为 8 张），避免空跑一整轮
  → load_ref                REF 字典表入 Iceberg
  → replay_ods              样本明细按主题重放进 Kafka
  → load_bronze             消费 Kafka 入 bronze，按主键 MERGE 去重
  → dbt_run                 OWD → OWS → ADS 三层建模，再跑 singular test 守汇率覆盖
  → pii_vault               建脱敏对照表
  → lineage                 渲染血缘与监管映射
  → owd_scd2                OWD 版本历史归并（SCD2）
  → dq_validate             执行 ref 层声明的质量规则
  → publish_access          施加库侧迁移与授权（必须在 export_pg 之前）
  → export_pg               导出到报送服务层
  → liquidity_monitor       算 LCR、产预警、翻转熔断闸
  → verify_bronze → verify_silver → verify_scd2 → verify_ads → verify_rbac
  → pipeline_health         巡检收口
  → run_context_close       运行上下文收口：把本次跑批标为成功
```

**调度**：每日 06:00 触发，为 T+1 08:00 截止留余量；SLA 2 小时。

报送放行闸与报送文件生成不在本 DAG 内 —— 它们由 `fr2052a_submission` 触发，否则日批会因熔断整体变红，看不出是哪一环出的问题。

### 5.2 `fr2052a_realtime_alert`

- **触发**：定时每 15 分钟扫描一次
- **逻辑**：消费核心存款主题，识别未保险、USD 计价、单笔超过门槛的存款敞口
- **输出**：写入 `ads.ads_fr2052a_realtime_alerts`，同时投递告警主题

### 5.3 `fr2052a_backfill_and_restate`

- **参数**：`report_date`、`entity_code`、`reason`、`requested_by`、`approved_by`、`effective_date`
- **逻辑**：先留取重跑前的报表快照，再重跑链路，最后登记新版本并关闭旧版本
- **输出**：`ads.ads_restatement_log` 与 `ads.ads_fr2052a_report_history`

### 5.4 `fr2052a_gl_reconciliation`

- **触发**：每日 07:00
- **任务流**：`refresh_report → check_reconciliation`；后者比对 8 个 Section（基准侧：非现金取总账科目余额，Section E 取司库现金头寸），结论落 `ads.ads_gl_reconciliation`
- **退出码**：对平 0，未对平非 0，由 DAG 记为失败并触发告警

### 5.5 `fr2052a_submission`

- **触发**：每日 07:30，`retries=0`（熔断中重试没有意义）
- **任务流**：`check_gate → generate_and_submit → verify_submission`
- **放行约定**：`check_gate` 退出码 0 才继续；2（熔断）与 3（判不了）都视为不放行
- **判据（按检查顺序）**：① 本报告日最近一次日批状态为 SUCCEEDED（读 `ads.ads_pipeline_run_context`，取最近一行）② 本报告日有流动性判定痕迹（`ads.ads_liquidity_metrics` 行数 > 0）③ 熔断闸行存在 ④ 本报告日无阻断级预警。前两条缺失即判 UNKNOWN（退出码 3），区分「今天没有预警」与「今天根本没判」
- **产物**：每个实体各一份 XBRL / XML / CSV，落 `ads.ads_fr2052a_submission` 台账（按 report_id + file_format 唯一，一个文件一行）

## 6. 血缘与监管映射接口

| 层级 | 来源 | 产出 |
|------|------|------|
| 表级血缘 | dbt 元数据里的模型依赖 | `audit.audit_data_lineage` 的边 |
| 列级血缘 | SQL 解析 | 列级边 |
| 监管映射 | 模型 `schema.yml` 的 meta 声明 | 列到 Section / Line Item 的绑定 |
| 人读报告 | 以上三者渲染 | `LINEAGE.md` |

## 7. 退出码与规则编码

本项目没有对外 HTTP 接口，对外的是**退出码**：跑批环境靠退出码判断放行与否，比读日志可靠。

| 退出码 | 来源 | 含义 | 建议动作 |
|--------|------|------|----------|
| 0 | `check_submission_gate` | 放行 | 继续生成报送文件 |
| 2 | `check_submission_gate` | 熔断中，存在阻断级预警 | 人工排查后重新判定，不得绕过 |
| 3 | `check_submission_gate` | 判不了（数据库不可达等） | 按不放行处理，先恢复环境 |

预警规则编码（写进 `ads.ads_fr2052a_alerts`，同时投递 Kafka 告警主题）：

| 规则编码 | 严重度 | 触发条件 | 阻断报送 |
|----------|--------|----------|----------|
| `CB-LCR-001` | CRITICAL | LCR 低于监管红线 | 是 |
| `CB-LCR-002` | WARNING | LCR 低于内部预警线 | 否 |
| `CB-LCR-003` | WARNING | 净现金流出为零，LCR 算不出来 | 否 |
| `CB-L2CAP-001` | WARNING | 二级资产占比超上限，认列额已被截断 | 否 |
| `CB-INFLOW-001` | INFO | 预期流入占流出超上限比例，认列额已被截断 | 否 |
| `CB-GL-001` | CRITICAL | 总账对账有 Section 未通过 | 是 |
| `CB-DQ-001` | CRITICAL | 数据质量有 ERROR 级规则失败 | 是 |
| `RT-LARGE-UNINSURED-001` | WARNING | 单笔未保险存款敞口超过门槛 | 否 |

是否阻断由 `config/liquidity_thresholds.json` 的 `breaker.blocking_severities` 决定，默认只有 CRITICAL 阻断。
