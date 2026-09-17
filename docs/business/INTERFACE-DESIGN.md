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

> 注：原设计中的 `fr2052a_mask_pii` 和 `fr2052a_fx_convert` 尚未实现，将在后续阶段补充。

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

### 2.4 `customer_segment`

- **输入**：客户类型原始值（IND/CORP/FI/GOV/OTHER）
- **输出**：`RETAIL` / `CORPORATE` / `FINANCIAL` / `SOVEREIGN` / `OTHER`

### 2.5 `deposit_product_category`

- **输入**：存款产品类型（CHK/SAV/MMDA/CD/TIME）
- **输出**：`DEMAND` / `SAVINGS` / `CD` / `TIME` / `OTHER`

## 3. Python CLI 接口

> 以下接口定义基于实际代码。签名以 `--help` 输出为准。

### 3.1 `generate_sample_data.py`

```bash
python -m generators.generate_sample_data --out <dir> [--gl-break-amount <N>]
```

| 参数 | 类型 | 必填 | 默认 | 说明 |
|------|------|------|------|------|
| `--out` | path | 否 | `../sample_data` | 输出目录 |
| `--gl-break-amount` | int | 否 | `0` | GL 对账缺口（用于演示对账失败场景）|

**退出码**：`0` = 成功，`1` = 失败

### 3.2 `load_ref_tables.py`

```bash
python lakehouse/load_ref_tables.py <csv_dir>
```

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `csv_dir` | path | 是 | 参考数据 CSV 目录 |

**幂等**：INSERT OVERWRITE，重复运行不重复写入

### 3.3 `export_gold_to_pg.py`

```bash
python export_gold_to_pg.py
```

- 配置全部通过环境变量注入（`PG_HOST`, `PG_DB`, `REPORT_DATE` 等）
- **幂等**：先 `TRUNCATE` 目标表，再批量插入

### 3.4 `run_dq_rules.py`

```bash
python validators/run_dq_rules.py --batch-id <id>
```

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `--batch-id` | string | 是 | ETL 批次号 |

**退出码**：`0` = 全部 PASS，`1` = 有 WARNING，`2` = 有 ERROR（阻断）

### 3.5 `replay_ods_to_kafka.py`

```bash
python producers/replay_ods_to_kafka.py --data-dir <dir> --config <json>
```

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `--data-dir` | path | 是 | ODS CSV 数据目录 |
| `--config` | path | 是 | Topic 配置文件路径 |

### 3.6 其他脚本

| 脚本 | 说明 |
|------|------|
| `python/lakehouse/run_sql_file.py` | 执行 Iceberg DDL |
| `python/lakehouse/verify_bronze.py` | 验证 Bronze 层数据 |
| `python/lakehouse/verify_silver.py` | 验证 Silver 层数据 |
| `python/lakehouse/verify_gold.py` | 验证 Gold 层数据 |
| `python/lakehouse/verify_ods_schema.py` | 验证 ODS 表结构 |
| `python/exporters/export_gold_to_pg.py` | Gold → PG 导出 |

## 4. Kafka Topic 契约

### 4.1 `core_banking_txns`

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `source_system` | VARCHAR(20) | 是 | 来源系统编码 |
| `source_record_id` | VARCHAR(50) | 是 | 源系统记录主键 |
| `txn_type` | VARCHAR(20) | 是 | `DEPOSIT` / `WITHDRAWAL` / `TRANSFER` |
| `amount` | NUMERIC(18,4) | 是 | 金额 |
| `currency` | CHAR(3) | 是 | ISO 4217 币种 |
| `event_time` | TIMESTAMPTZ | 是 | 事件时间 |

### 4.2 `treasury_deals`

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `deal_id` | VARCHAR(30) | 是 | 交易编号 |
| `deal_type` | VARCHAR(20) | 是 | `REPO` / `REVERSE_REPO` / `SEC_LENDING` |
| `notional` | NUMERIC(18,4) | 是 | 名义本金 |
| `currency` | CHAR(3) | 是 | 币种 |
| `counterparty_id` | VARCHAR(30) | 是 | 交易对手 ID |
| `event_time` | TIMESTAMPTZ | 是 | 事件时间 |

### 4.3 `derivatives_trades`

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `trade_id` | VARCHAR(30) | 是 | 交易编号 |
| `instrument_type` | VARCHAR(20) | 是 | `IRS` / `CDS` / `FX_FWD` / `FX_SWAP` / `OPTION` |
| `notional` | NUMERIC(18,4) | 是 | 名义本金 |
| `currency` | CHAR(3) | 是 | 币种 |
| `counterparty_id` | VARCHAR(30) | 是 | 交易对手 ID |
| `event_time` | TIMESTAMPTZ | 是 | 事件时间 |

### 4.4 `market_data_prices`

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `price_type` | VARCHAR(20) | 是 | `FX_RATE` / `SEC_PRICE` |
| `instrument_id` | VARCHAR(30) | 是 | 工具编号 |
| `price` | NUMERIC(18,6) | 是 | 价格 |
| `currency` | CHAR(3) | 是 | 币种 |
| `timestamp` | TIMESTAMPTZ | 是 | 价格时间 |

### 4.5 `gl_entries`

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `gl_account_id` | VARCHAR(30) | 是 | 总账科目编号 |
| `debit` | NUMERIC(20,2) | 是 | 借方余额 |
| `credit` | NUMERIC(20,2) | 是 | 贷方余额 |
| `currency` | CHAR(3) | 否 | 默认 USD |
| `entry_date` | DATE | 是 | 分录日期 |
| `event_time` | TIMESTAMPTZ | 是 | 事件时间 |

### 4.6 `reference_data_updates`

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `ref_type` | VARCHAR(30) | 是 | `ENTITY` / `COUNTERPARTY` / `CURRENCY` |
| `ref_id` | VARCHAR(30) | 是 | 主键 |
| `action` | VARCHAR(10) | 是 | `INSERT` / `UPDATE` / `DELETE` |
| `data_json` | JSONB | 是 | 变更数据 |
| `timestamp` | TIMESTAMPTZ | 是 | 变更时间 |

### 4.7 `fr2052a_alerts`

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `alert_id` | UUID | 是 | 全局唯一标识 |
| `report_date` | DATE | 是 | 受影响的报表日 |
| `source_model` | VARCHAR(255) | 是 | 触发告警的 dbt 模型名 |
| `severity` | VARCHAR(20) | 是 | `CRITICAL` / `WARNING` / `INFO` |
| `message` | TEXT | 是 | 人类可读描述 |
| `created_at` | TIMESTAMPTZ | 是 | 创建时间 |
| `is_resolved` | BOOLEAN | 否 | 是否已解决 |
| `resolution_note` | TEXT | 否 | 解决说明 |

## 5. Airflow DAG 接口

### 5.1 `fr2052a_daily_batch`

DAG 只做编排：每个任务 ssh 到 Server 2 调用跑批脚本的同一个环节，编排逻辑只有一份。

```text
check_source_arrival          确认 7 张 ODS 源文件到位，避免空跑一整轮
  → load_ref                  REF 字典表入 Iceberg
  → replay_ods                样本明细按主题重放进 Kafka
  → load_bronze               消费 Kafka 入 bronze，按主键 MERGE 去重
  → dbt_run                   OWD → OWS → ADS 三层建模
  → pii_vault                 建脱敏对照表
  → lineage                   渲染血缘与监管映射
  → dq_validate               执行 ref 层声明的质量规则
  → publish_access            施加库侧迁移与授权
  → export_pg                 导出到报送服务层
  → liquidity_monitor         算 LCR、产预警、翻转熔断闸
  → verify_*                  三层核对与权限核对
  → pipeline_health           巡检收口
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
