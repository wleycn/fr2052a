# 数据设计

> 真源：`sql/iceberg/`（DDL）+ `docs/business/DATA-DESIGN.md`（本文档）。两者必须同步更新。

## §1 数据流图

### 端到端数据流

```text
源系统
  核心银行 / 资金交易 / 衍生品 / 托管 / 贷款 / 财务总账
        │ 文件 / CDC / API / Kafka
        ▼
接入层
  Kafka Topics + 批文件网关
        │
        ▼
数据湖 Bronze / ODS
  Iceberg on MinIO：原始镜像、追加、Schema 演进、Time Travel
        │
        ▼
数据湖 Silver / OWD + OWS
  清洗、标准化、汇率转换、引用数据关联、到期分桶、HQLA 分类、现金流预测
        │
        ▼
Gold / ADS
  PostgreSQL：FR 2052a 报表、明细、校验日志、GL 对账、报送状态
        │
        ├── 报送文件生成：XBRL / XML / CSV
        ├── 血缘与监管映射：dbt meta 声明 + render_lineage.py 渲染
        └── 巡检：pipeline_health.py（8 项：熔断 / 校验 / 预警 / 报送 / 重述 / 实时事件 / 连接 / 滞后 / 磁盘）
```

### 真源划分

| 数据 | 真源 | 说明 |
|------|------|------|
| 表结构 DDL | `sql/iceberg/*.sql` | 唯一真源，改代码必同步本节 |
| 接口契约 | `docs/business/INTERFACE-DESIGN.md` | dbt macro / Python API / Kafka Topic |
| 业务规则 | `dbt/macros/fr2052a_rules.sql` | HQLA、到期分桶、现金流 Cap |
| 元数据 | `config/pipeline_topics.json` | Topic 配置 |
| 派生视图 | 无真源 | Iceberg snapshot / dbt 物化视图可随时重建 |

## §2 数据结构设计

### 2.1 分层表清单

> 每张表的完整契约（粒度 / 主键 / 去重 / 分区 / 金额口径 / PII 与脱敏 / 生命周期 / 新鲜度与 owner / 依赖 / 质量规则）住 `docs/tables/`，一表一份，共 56 张。本节只列清单与落点，表级细节不在这里复制一份。
> 不入契约的两类：dbt 的连通性自检模型（`pg_smoke`、`spark_smoke`）产出的是探针表，随跑批丢弃；`market_data_prices` 等三个只作声明的 Kafka 主题没有对应表。

#### 引用数据（Ref，Iceberg）

9 张字典表，由批加载写入，不参与流式接入。

| 表 | 说明 |
|----|------|
| `ref.ref_entity_hierarchy` | 法人实体层级（并表口径与重要子公司） |
| `ref.ref_counterparty` | 交易对手主数据 |
| `ref.ref_maturity_bucket` | 到期分桶定义 |
| `ref.ref_fr2052a_line_items` | FR 2052a 行项目映射 |
| `ref.ref_exchange_rates` | 汇率（报告日即期，折算目标恒为 USD） |
| `ref.ref_regulatory_mapping` | 监管映射（字段 → Section / Line Item） |
| `ref.ref_behavior_assumptions` | 行为假设（流出率、HQLA 分类） |
| `ref.ref_calendar` | 银行营业日 |
| `ref.ref_validation_rules` | 数据质量规则定义（规则引擎读它执行） |

#### 接入层（Kafka）

11 个主题，其中 8 个有生产者的落 bronze，3 个只作声明保留。

| Topic | 落点 | 生产者 |
|-------|------|--------|
| `core_banking_txns` | `bronze.ods_deposits` | 有 |
| `loan_book` | `bronze.ods_loans` | 有 |
| `treasury_deals` | `bronze.ods_repo_transactions` | 有 |
| `custody_positions` | `bronze.ods_securities` | 有 |
| `derivatives_trades` | `bronze.ods_derivatives` | 有 |
| `gl_entries` | `bronze.ods_gl_balances` | 有 |
| `treasury_cash_position` | `bronze.ods_treasury_cash_position` | 有 |
| `off_bs_commitments` | `bronze.ods_off_bs_commitments` | 有 |
| `market_data_prices` | 不落表 | 暂作声明保留（本演示未生成对应 ODS 表） |
| `reference_data_updates` | 不落表 | 暂作声明保留（引用数据走批加载直入 ref） |
| `fr2052a_alerts` | 不落表 | 熔断判定写入，供告警下游订阅 |

主题清单、落点与是否有生产者的唯一声明在 `config/pipeline_topics.json`。

#### Bronze 层（Iceberg）

8 张 ODS 表，字段与源 CSV 表头一一对应（对不上时由 `verify_ods_schema.py` 报错），按 `days(report_date)` 分区。

| 表 | 加载方式 |
|----|----------|
| `bronze.ods_deposits` | Kafka → MERGE |
| `bronze.ods_repo_transactions` | Kafka → MERGE |
| `bronze.ods_loans` | Kafka → MERGE |
| `bronze.ods_securities` | Kafka → MERGE |
| `bronze.ods_derivatives` | Kafka → MERGE |
| `bronze.ods_gl_balances` | Kafka → MERGE |
| `bronze.ods_treasury_cash_position` | Kafka → MERGE |
| `bronze.ods_off_bs_commitments` | Kafka → MERGE |

#### 数据规模与视角覆盖

ODS 各表的行数由三类来源组成：子公司配额、母公司追加（小账）、集团内往来配对腿。子公司配额是原有四家法人实体（ENT002–ENT005）的业务量，母公司追加是 ENT001 自己也记账（母公司单体口径需要数据），配对腿是集团内往来的成对行。

母公司是小账：金额类字段（本金、票面、市值、敞口、承诺额、盯市）的抽取区间上限乘以 0.08（演示假设），显著小于子公司。母公司不只有内部往来，还有对第三方对手方（CP0001 这一类）的存款、贷款、证券、衍生品和表外承诺。

集团内往来：每对（母公司 ↔ 一家子公司）生成两条腿，金额来自同一个固定常量，因此天然相等。存款腿记在母公司账上（子公司在母公司的存款，母公司的负债），贷款腿记在子公司账上（母公司对子公司的放款，子公司的资产）。合并口径双边抵销这两条腿。真实源数据靠 `ref_counterparty` 里 `counterparty_type = 'AFFILIATE'` 识别集团内对手方。

三档报送视角：
- 全球合并（`01`，`entity_code = 'GRP001'`，`is_consolidated = true`）：把集团当一家看，集团内互相的存款与放款都不存在。合并行只汇总 `is_intracompany = false` 的明细行。
- 法人实体单体（`02`，`entity_code <> 'ENT001'`，`is_consolidated = false`）：各实体自己的账，内部往来不抵销。
- 母公司单体（`03`，`entity_code = 'ENT001'`，`is_consolidated = false`）：母公司自己的账，不抵销。

`GRP001` 是保留码，不代表任何法人实体，只用于合并行。口径码由 `is_consolidated` + `entity_code` 推出，不另加常量列。

总账按法人实体各记一本账：对每个 BOOKING_ENTITIES（ENT001–ENT005）各生成一组科目余额，由该实体自己的业务明细倒推。每本账各自借贷平衡（权益是轧差项）。该实体没有业务的科目记 0（0 = 查过且确实为零）。演示对账缺口（`gl_break_amount`）落在 ENT002 的 2100 科目（贷款），其它实体不受影响。

#### Silver 层（Iceberg）

| 表 | 说明 | 来源 |
|----|------|------|
| `silver.stg_fx_rates` | 汇率取数 | `ref.ref_exchange_rates` |
| `silver.owd_deposits` | 标准化存款（客户标识已脱敏） | `bronze.ods_deposits` 清洗 + 汇率换算 |
| `silver.owd_secured_financing` | 标准化有担保融资 | `bronze.ods_repo_transactions` |
| `silver.owd_loans` | 标准化贷款（借款人标识已脱敏） | `bronze.ods_loans` |
| `silver.owd_securities` | 标准化证券 | `bronze.ods_securities` |
| `silver.owd_derivatives` | 标准化衍生品 | `bronze.ods_derivatives` |
| `silver.owd_gl_entries` | 标准化总账余额 | `bronze.ods_gl_balances` |
| `silver.owd_treasury_cash_position` | 标准化司库现金头寸（对账单余额与未达账项） | `bronze.ods_treasury_cash_position` |
| `silver.owd_off_bs` | 标准化表外承诺 | `bronze.ods_off_bs_commitments` |
| `silver.ows_hqla_summary` | HQLA 汇总（按等级与受限状态汇总市值、折扣后价值；二级资产的 40% 上限在**报表层**应用，本层不截断） | `owd_securities` |
| `silver.ows_collateral_summary` | 担保品汇总（按证券类型、等级、发行国汇总） | `owd_securities` |
| `silver.ows_cash_position` | 现金头寸 | `owd_*` |
| `silver.ows_cashflow_projection` | 现金流预测 | `owd_*` 到期分桶 |
| `silver.ows_funding_summary` | 融资汇总 | `owd_*` 聚合 |
| `silver.owd_*_history` | 8 张 OWD 的版本历史（SCD2） | `owd_scd2.py` 归并 |

#### Gold 层（Iceberg）与 ADS 层（PostgreSQL）

同一份报表数据的两个落点：Gold 在湖里，ADS 由导出作业写进 PostgreSQL，供报送与放行闸读取。

| 表 | 说明 | 来源 |
|----|------|------|
| `ads_fr2052a_report` | FR 2052a 报表主表，一个报送主体一行 | `ows_*` 汇总 |
| `ads_fr2052a_detail` | 报表明细行，用于回溯报表数字 | `owd_*` 明细展开 |
| `ads_gl_reconciliation` | GL 对账结果，8 个 Section 逐项 PASS / FAIL。基准侧分两类：非现金 Section 取总账科目余额，Section E 取司库现金头寸（对账单 + 盘点口径），差额由调节项解释 | 报表 vs 独立基准 |

#### 控制与审计（PostgreSQL）

| 表 | 说明 |
|----|------|
| `ads.ads_liquidity_metrics` | 每家主体的 LCR 等流动性指标 |
| `ads.ads_fr2052a_alerts` | 规则状态（一行 = 一条规则的当前状态，重复命中累加次数） |
| `ads.ads_fr2052a_realtime_alerts` | 实时敞口事件流水（一行 = 一笔事件） |
| `ads.ads_circuit_breaker` | 熔断闸（全局一行，OPEN / HALTED） |
| `ads.ads_fr2052a_submission` | 报送台账（一行 = 一个文件当前状态，含哈希与回执） |
| `ads.ads_fr2052a_submission_audit` | 报送重生成审计（一行 = 一次生成，记录哈希变化） |
| `ads.ads_restatement_log` | 重述登记（原报表 ↔ 新报表） |
| `ads.ads_fr2052a_report_history` | 报表版本历史 |
| `ads.ads_fr2052a_validation_log` | 数据质量结论（一行 = 一个批次的一条规则） |
| `ads.ads_pipeline_run_context` | 运行上下文（一行 = 一次跑批，日期与状态的单源） |
| `secure.fr2052a_pii_map` | 脱敏对照表，明文唯一落点 |
| `audit.audit_data_lineage` | 血缘边（表级与列级） |
| `audit.audit_change_log` / `audit.audit_access_log` | 变更审计与访问审计 |

### 2.2 关键表结构

#### `ads.ads_fr2052a_report`

列由 dbt 模型产出，完整列清单见 `dbt/models/marts/ads_fr2052a_report.sql`。本文档不复制一份：复制出来的列清单必然在报表加列时漏改一处。

| 项 | 内容 |
|---|---|
| 主键 | `report_id`，四段区位码「机构-报表-报告期-口径」，例 `GRP001-FR2052A-20260916-01` |
| 末段口径码 | `01` = 全球合并（`is_consolidated` 为真，`entity_code = 'GRP001'`）；`02` = 法人实体单体（`is_consolidated` 为假且 `entity_code <> 'ENT001'`）；`03` = 母公司单体（`is_consolidated` 为假且 `entity_code = 'ENT001'`） |
| 粒度 | 一个报送主体一行，即（报告日 + 实体 + 口径） |
| 合并抵销 | 合并行只汇总 `is_intracompany = false` 的明细行。集团内往来的存款腿与贷款腿都带 `is_intracompany = true`，两侧同时排除，双边抵销 |
| 金额列 | Section A–K 的金额与监管上限后的认列额，另有三项汇总指标 |

为什么不用自增序列做主键：本表每轮导出是全量覆盖，序列值属于数据库状态而不是数据，同一个业务报表在不同批次会拿到不同的号。而重述登记要跨批次引用「原报表 / 新报表」，键一旦会变，这层对应关系就不成立。

#### `ads.ads_fr2052a_alerts`（流动性预警，一行一个规则状态）

```sql
CREATE TABLE ads.ads_fr2052a_alerts (
    alert_id BIGSERIAL PRIMARY KEY,                   -- 预警主键，交给序列生成
    report_date DATE NOT NULL,                        -- 报告日
    entity_code TEXT NOT NULL,                        -- 法人实体编码
    alert_code TEXT NOT NULL,                         -- 规则编码，如 CB-LCR-001
    severity TEXT NOT NULL,                           -- CRITICAL / WARNING / INFO
    metric_name TEXT,                                 -- 触发指标名
    metric_value NUMERIC(20, 4),                      -- 触发时指标实际值
    threshold_value NUMERIC(20, 4),                   -- 判定阈值
    message TEXT NOT NULL,                            -- 人读说明
    blocks_submission BOOLEAN NOT NULL DEFAULT FALSE, -- 是否阻断报送
    occurrence_count INTEGER NOT NULL DEFAULT 1,      -- 同一规则重复触发的累计次数
    status TEXT NOT NULL DEFAULT 'OPEN',              -- OPEN / CLOSED
    first_detected_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_detected_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT ads_fr2052a_alerts_uk UNIQUE (report_date, entity_code, alert_code),
    CONSTRAINT ads_fr2052a_alerts_severity_ck CHECK (severity IN ('CRITICAL', 'WARNING', 'INFO')),
    CONSTRAINT ads_fr2052a_alerts_status_ck CHECK (status IN ('OPEN', 'CLOSED'))
);
```

规则状态与事件流水分两张表。本表是「规则当前状态」，同一规则重复命中只累加 `occurrence_count`；「一笔事件一行」的流水另住 `ads.ads_fr2052a_realtime_alerts`。混在一张表里会让「一条记录代表什么」说不清。

### 2.3 落库契约

| 层 | 写入方 | 读取方 | 幂等策略 |
|----|--------|--------|----------|
| Ref | `load_ref_tables.py` 批加载 | 所有层 | 整表覆盖写（`INSERT OVERWRITE`）：字典表量小，重跑任意次结果一致 |
| Bronze | `kafka_to_iceberg.py` 消费 Kafka | OWD 模型 | 按主键 MERGE 去重，同一条消息重放不产生第二行 |
| Silver | dbt 模型（table 物化） | OWS / ADS 模型 | 整表重建，先建后换，不留半成品 |
| Silver 版本历史 | `owd_scd2.py` | 重述登记与审计 | 全表重算，同一主键的版本号连续 |
| Gold（Iceberg） | dbt marts 模型 | 导出作业 | 整表重建 |
| ADS（PG） | `export_gold_to_pg.py` | 报送、预警、对账 | 先清后写，写时开 `truncate=true` 以保留表上的授权与触发器 |
| 控制与审计（PG） | 各环节脚本 | 放行闸、巡检、审计 | 按业务键 upsert；按批次累积的表先清本批次再追加 |

## §3 业务规则

### 3.1 到期分桶

| Bucket | 最小天数 | 最大天数 |
|--------|----------|----------|
| O/N | 0 | 0 |
| 1-7D | 1 | 7 |
| 8-30D | 8 | 30 |
| 31-90D | 31 | 90 |
| 91-180D | 91 | 180 |
| 181D-1Y | 181 | 365 |
| >1Y | 366 | 99999 |
| OPEN | NULL | NULL |

存款表的分桶走**行为口径**（`behavioral_bucket` 宏）：活期与储蓄存款没有到期日，按业务行为归 `O/N`（可随时提取，行为上等同隔夜），不落 `OPEN`；`OPEN` 只对「有到期日概念却没填到期日」的行生效。其余五张明细表按剩余天数分桶（`maturity_bucket` 宏）。

为什么必须这么分：30 天现金流预测与报表只取 `O/N` / `1-7D` / `8-30D` 三个桶，活期与储蓄一旦落 `OPEN` 就被整体排除在流出之外 —— 而这两类存款正是 30 天流出的主体。

### 3.2 HQLA 分类

| 等级 | 示例 | Haircut |
|------|------|---------|
| Level 1 | 现金、央行准备金、国债 | 0% |
| Level 2A | 机构债、GSE | 15% |
| Level 2B | 投资级公司债、部分股票 | 50% |
| Non-HQLA | 其他 | 不纳入 |

Level 1 在本演示里由两块组成：

- 非受限的一级证券 —— `sec_i_unencumbered_hqla_l1`，来自 `owd_securities` 里分级为 `LEVEL_1` 的行，按未质押口径
- 现金与同业存放 —— `sec_e_cash_total`，即总账 `1001` 库存现金 + `1100` 同业存放。现金没有「已质押」概念，全额计入

LCR 分子的 L1 取这两块之和，见 `python/alerts/liquidity_monitor.py`。只算证券会把分子系统性压低，LCR 偏低时分不清是资产结构差还是口径漏算。

HQLA 存量还有一道**到期窗口**：剩余期限 30 天以内的证券不计入存量，它们已经按 100% 计入 30 天预期流入。两边都算就是同一笔资产被计两次 —— 分子抬高、分母压低，LCR 双向偏离。依据两份互相独立的权威口径：

- 美联储 LCR 最终规则（Federal Register, 2013）：「this exclusion also includes all HQLA that mature within 30 days」
- Basel III LCR 及各国实施稿（如 OSFI LAR、FSRA 指南）：「Inflows from securities maturing within 30 days not included in the stock of HQLA should receive 100% inflow」

被排除的那一块不消失：单列在 `sec_i_unencumbered_near_maturity`，并进 Section G 的构成，`Section I/G 恒等式` 因此仍然成立。

演示数据里约两成证券持仓是 30 天以内到期的短期票据（国债与机构债，见生成器的 `SHORT_DATED_SECURITY_RATIO`）。这批持仓专门留着：没有它们，这条规则在样本上无对象可判 —— 全是 60 天以上持仓时，改不改代码结果都一样，规则等于没验证过。

两个口径不要混：Section G 的 `sec_g_hqla_l1_mv` / `l2a` / `l2b` 是**组合按 HQLA 分类的构成**（含已质押与 30 天内到期的证券，回答「我们持有多少合格资产」）；Section I 的 `sec_i_unencumbered_*` 是**可计入存量的部分**（未受限 + 剩余期限 30 天以上，回答「现在能用来扛 30 天流出的有多少」）。LCR 分子取 Section I 与现金，不取 Section G。

### 3.3 关键约束

| 规则 | 约束 |
|------|------|
| 现金流 Cap | 预期流入 ≤ 总流出 × 75%，超出按 75% 截断并记录 WARNING |
| HQLA 二级资产上限 | Level 2A + Level 2B 的认列额 ≤ 一级资产 × 2/3。这是 Basel LCR30「不得超过扣除后 HQLA 的 40%」的等价式；按 0.40 × (一级 + 二级) 算会把二级自己也算进基数，上限偏高 |
| HQLA 到期窗口 | 剩余期限 30 天以内的证券不计入 HQLA 存量（改按 100% 流入计入），避免同一笔资产双向计量。排除部分单列 `sec_i_unencumbered_near_maturity` |
| Section I/G 恒等式 | 未受限五项（L1、L2A、L2B、非 HQLA、30 天内到期）+ 已受限 = Section G 合计。由 `verify_gold.check_section_i_identity` 独立复算，任一项漏算即不平 |
| 累计缺口不再单列 | 需求文档曾列出 `sec_k_cumulative_30d_gap`，实测它与 `sec_k_net_funding_gap` 是同一个数（逐桶累计净现金流单调递减时，最低点 = 窗口末累计值 = 净缺口；两期 6/6 行相等），因此删除而不是改名或硬凑一个不同算法。真需要时间维度就按到期桶出向量，不是挤成一个标量。见 KNOWN-ISSUE `#cumulative-gap-column-removed` |
| Operational 存款 | 流出率低于 Non-Operational |
| 币种转换 | 报告日即期汇率转 USD；汇率表必须覆盖业务数据里出现的全部（报告日, 币种）组合，缺行由 `dbt/tests/assert_fx_covered.sql` 断言失败拦住（折算失败必须出声，不允许静默变 NULL） |
| 行为假设覆盖 | ODS 存款里出现的每个 (product_category, customer_segment, maturity_bucket) 组合都必须在 ref_behavior_assumptions 里有行。缺行由两道拦住：生成器自检在生成阶段验（`check_behavior_coverage`），`dbt/tests/assert_behavior_covered.sql` 在转换后验。未命中不再静默兜底 10%，缺假设是缺陷 |
| 受保金额截断 | 存款保险限额是美元限额，且按「客户 × 法人实体」聚合后截断：同一客户在同一家银行的多笔存款合计受保，超限时各笔按占该客户受保总额的比例等比缩到限额。必须先折算 USD 再截断 —— 在原币上截断会让外币存款的受保金额量级错误。`sec_c_total` 是全额，受保部分单列在 `sec_c_insured_total`。限额单源在 `dbt_project.yml` 的 `deposit_insurance_limit_usd`，上限断言见 `dbt/tests/assert_insured_within_limit.sql` |
| 报告期隔离 | 跨期不混算：所有聚合与关联都按 `report_date` 分组与匹配（金额列、对账、核对脚本同理）。多个报告期共存时，每期只汇总自己的明细 |
| 净额结算 | 仅有有效净额协议时允许 |

### 3.4 数据质量规则（VDQ）

规则集共 21 条（VDQ-001 至 VDQ-021），真源是生成器产出的 `sample_data/ref/ref_validation_rules.csv`，
下表只列其中 9 条作为示例，完整清单以该 CSV 与 `ref.ref_validation_rules` 表为准。

| 编号 | 层级 | 规则 | 严重度 |
|------|------|------|--------|
| VDQ-001 | ODS | 文件完整到达 | ERROR |
| VDQ-002 | ODS | 关键字段非空 | ERROR |
| VDQ-003 | OWD | 金额 ≥ 0 | ERROR |
| VDQ-006 | OWD | outstanding ≤ facility | ERROR |
| VDQ-010 | OWS | 汇总 = SUM 明细 | ERROR |
| VDQ-013 | ADS | Section 合计 = 行项目合计 | ERROR |
| VDQ-016 | ODS | T+1 08:00 ET 前加载 | ERROR |
| VDQ-017 | ADS | 二级资产认列额 = min(原始二级市值, 一级市值 × 2/3)，1 分表示精度容差 | WARNING |
| VDQ-018 | ADS | 认列流入 = min(原始流入, 总流出 × 75%)，1 分表示精度容差 | ERROR |

### 3.5 行为假设派生规则

ref_behavior_assumptions 从原来的 6 行手工样本改为由规则派生的全覆盖矩阵。维度取值按 ODS 数据里实际出现的组合取，不凭空造维度。

组合 = product_category × customer_segment × maturity_bucket。流失率 = 基础流失率 × 客户分段调整 × 到期分桶调整，clamp [0, 1]。

活期与储蓄存款没有到期日，按行为口径归到 O/N（活期可随时提取，行为上等同隔夜），只有 O/N 一个桶。定期类（CD、TIME）按到期日覆盖全部到期桶。

基础流失率（活期低于定期：活期随时可取但行为上不会全走，定期到期不续约概率更高）：

| 产品类别 | 基础流失率 |
|---|---|
| DEMAND | 5% |
| SAVINGS | 8% |
| CD | 30% |
| TIME | 25% |

客户分段调整（零售低于对公）：

| 客户分段 | 调整系数 |
|---|---|
| RETAIL | 0.5 |
| CORPORATE | 1.0 |
| SOVEREIGN | 0.8 |
| FINANCIAL | 1.5 |
| AFFILIATE | 0.3 |

到期分桶调整（短桶流失率高）：

| 分桶 | 调整系数 |
|---|---|
| O/N | 1.2 |
| 1-7D | 1.1 |
| 8-30D | 1.0 |
| 31-90D | 0.8 |
| 91-180D | 0.6 |
| 181D-1Y | 0.4 |
| >1Y | 0.2 |

派生关系的真源是 `python/generators/ref_data.py` 的 `_derive_behavior_rows`，本节与之同步更新。

## §4 数据血缘

```text
Core Banking Deposit Module
  → ods_deposits (Bronze)
  → owd_deposits (Silver)
  → ads_fr2052a_report.sec_c_retail_demand (Gold)   ← 报表直接读 OWD 明细，不经 OWS
  → FR 2052a Section C / Line Item
```

Section E（现金）的对账血缘是两条独立链路，这是它有意义的前提：

```text
Treasury Cash Position（司库系统对账单与盘点）
  → ods_treasury_cash_position (Bronze)
  → owd_treasury_cash_position (Silver)
  → ads_gl_reconciliation.benchmark_amount (Gold, Section E 的基准侧)

总账 1001/1100 → owd_gl_entries → ads_fr2052a_report.sec_e_cash_total (Gold, 报送侧)
```

两侧如果都读总账，差异恒为零、任何错误都查不出来；司库口径是外部可见的事实，
两侧的差由在途存款与未兑现支票逐项解释（见 `docs/tables/ads_gl_reconciliation.md`）。

三张 OWS 表（`ows_hqla_summary`、`ows_collateral_summary`、`ows_funding_summary`）当前无下游消费者：
报表读的是 OWD 明细，OWS 只有 `ows_cash_position` 与 `ows_cashflow_projection` 被引用。
它们已登记为待下线（见 [KNOWN-ISSUE.md](KNOWN-ISSUE.md)），保留原因与下线条件写在那里。

完整血缘由 `render_lineage.py` 从 dbt 元数据与 SQL 解析生成，见 [INTERFACE-DESIGN.md](INTERFACE-DESIGN.md#6-血缘与监管映射接口)。
