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
| LZ | Landing Zone | 数据落地区, 类似 ODS 与 Bronze| 原始数据表 | ods |
| DZ | data Zone | 处理后数据区, Fact, Dim 表 类似 OWD 与 Gold| 处理后数据表 | OWD,OWS |
| HZ | history dataq Zone | 处理后数据区, Fact, Dim 表历史数据, 类似 OWD 与 Gold| 处理后历史数据表 | OWD,OWS |
| RPT | Report Zone | 数据落地区, 类似 ODS 与 Bronze| 原始数据表 | ADS |

### 1.3 技术术语

| 术语（中文） | 英文 / 代码标识 | 定义 | 禁止说法 |
|---|---|---|---|
| SCD2 | Slowly Changing Dimension Type 2 | 类型 2 缓慢变化维，保留历史版本 | "历史表" |
| Time Travel | Iceberg Time Travel | 按 snapshot 查询历史数据 | "历史查询" |
| Kafka KRaft | Kafka Raft Consensus | Kafka 无 ZooKeeper 模式 | "Kafka 集群"（模糊）|
| dbt model | dbt Transformation Model | dbt 转换模型，SQL 定义数据转换 | "SQL 文件" |
| Great Expectations | GE 数据质量框架 | 校验规则引擎 | "校验脚本" |
| Airflow DAG | Directed Acyclic Graph | 调度依赖图 | "任务"（缺少依赖语义）|
| DataHub | LinkedIn DataHub | 元数据治理平台 | "血缘工具" |
| MinIO | 对象存储 | S3 兼容对象存储 | "文件存储" |
| Iceberg | Apache Iceberg | 开放表格式 | "表格式" |

## 2. 口径类术语（数值/时间/状态）

| 术语 | 口径 | 单位 | 精度/舍入 | 示例 |
|---|---|---|---|---|
| 金额 | 以**美元**为单位 | USD | 2 位小数，half-up | `1234567.89` |
| 比率 | 小数表示 | 无 | 6 位小数 | `0.123456` = 12.3456% |
| 时间戳 | **UTC** | — | 毫秒 | `2026-09-17T08:00:00Z` |
| 报告日 | 报表基准日 | — | DATE | `2026-09-16` |
| 日分区 | `ds` 字符串 | — | `YYYY-MM-DD` | `2026-09-16` |
| 到期日 | 合同到期日 | — | DATE | `2027-09-16` |
| 剩余天数 | 报告日到到期日的天数 | 天 | 整数 | `365` |

> 🔴 **口径混用**（美元/本币、UTC/本地、小数/百分数）是本类**最高频事故来源**，必须逐项写明。

## 3. 状态与枚举

### 3.1 报表状态

| 枚举 | 取值 | 含义 | 状态流转 | 代码位置 |
|---|---|---|---|---|
| `ReportStatus` | `DRAFT` / `VALIDATED` / `SUBMITTED` / `RESTATED` | 报表状态 | `DRAFT → VALIDATED → SUBMITTED`；`SUBMITTED → RESTATED` | `ads/fr2052a_submission` |
| `AlertSeverity` | `CRITICAL` / `WARNING` / `INFO` | 告警严重度 | — | `ads/fr2052a_alerts` |
| `ReconciliationStatus` | `PASS` / `FAIL` / `PENDING` | 对账状态 | `PENDING → PASS/FAIL` | `ads/gl_reconciliation` |

### 3.2 数据质量状态

| 枚举 | 取值 | 含义 |
|---|---|---|
| `DQResult` | `PASS` / `FAIL` / `WARN` | 校验结果 |
| `ValidationError` | `ERROR` / `WARNING` | 错误级别 |

### 3.3 报送状态

| 枚举 | 取值 | 含义 |
|---|---|---|
| `SubmissionStatus` | `PENDING` / `SUCCESS` / `FAILED` / `REJECTED` | 报送结果 |
| `FilingMethod` | `XBRL` / `XML` / `CSV` | 报送文件格式 |

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
| 数据质量 | GE 校验规则引擎 | 校验日志 | 数据纠错、阻断报送 |
| 合规熔断 | `fr2052a_alerts` 检查 | 告警表 | 告警发送、修复 |
| 元数据治理 | DataHub 摄取与血缘 | 元数据目录 | 数据质量、业务映射 |
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

## 7. 变更记录

| 日期 | 术语 | 变更 | 影响面 |
|---|---|---|---|
| 2026-09-17 | 新增 | 完整术语表 | 本文档 |
