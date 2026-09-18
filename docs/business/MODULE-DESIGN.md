# 模块设计

> 模块划分、职责边界、接口契约、依赖关系。

## 模块划分

| 模块 | 职责 | 不做什么 |
|------|------|----------|
| 数据接入 | 文件/CDC/Kafka 接入源系统数据 | 不做数据清洗、不做业务校验 |
| ODS 原始镜像 | 1:1 保留源数据，加 ETL 元数据 | 不做标准化、不做转换 |
| OWD 标准化 | 清洗、去重、编码统一、汇率转换 | 不做聚合、不做报表对齐 |
| OWS 汇总 | 到期分桶、现金流计算、HQLA 分类 | 不做明细展开、不做 GL 对账 |
| ADS 报表 | FR 2052a 对齐、报送文件生成 | 不做数据采集、不做实时预警 |
| GL 对账 | 总账与报表对账，差异阻断 | 不做数据修正、不做重述 |
| 重述 | SCD2 版本管理、迟到数据处理 | 不做历史数据回滚、不做审计 |
| 数据质量 | 规则引擎执行 `ref.ref_validation_rules` 里的规则 | 不做数据纠错、不做阻断 |
| 合规熔断 | `ads.ads_fr2052a_alerts` 检查、报送放行闸阻断 | 不做告警发送、不做修复 |
| 调度编排 | Airflow DAG 管理 | 不做业务逻辑、不做数据访问 |
| 元数据/血缘 | dbt meta 声明 + `render_lineage.py` 渲染血缘与监管映射 | 不做数据质量校验、不做业务映射 |
| PII 脱敏 | 动态脱敏、权限控制 | 不做数据加密、不做审计日志 |

## 接口契约

### dbt macro 接口

宏名不带前缀，`fr2052a_` 只在文件名上（`dbt/macros/fr2052a_rules.sql`）。

| 宏名 | 参数 | 返回 | 用途 |
|------|------|------|------|
| `maturity_bucket` | `days_expr` | `VARCHAR`（`O/N`、`1-7D`……）| 到期分桶 |
| `hqla_level` | `security_type_expr, rating_expr` | `LEVEL_1` / `LEVEL_2A` / `LEVEL_2B` / `NON_HQLA` | HQLA 分类 |
| `hqla_haircut` | `hqla_level_expr` | 折扣率（小数）| 按分级给折扣 |
| `customer_segment` | `customer_type_expr` | `VARCHAR` | 客户细分归一 |
| `deposit_product_category` | `deposit_type_expr` | `VARCHAR` | 存款产品归类 |
| `mask_pii` | `column_name` | `h_` + 16 位十六进制 | PII 脱敏，加盐 SHA-256，模板在 `dbt_project.yml` 的 `vars.pii_mask_template` |
| `generate_schema_name` | `custom_schema_name, node` | `VARCHAR` | 决定模型落到哪个 schema |

汇率折算没有对应宏：由 OWD 模型 join `stg_fx_rates` 完成。

### Python CLI 接口

| 命令 | 参数 | 行为 | 退出码 |
|------|------|------|--------|
| `generate_sample_data.py` | `--out`（默认 `sample_data/`）、`--report-days`（默认 1，多期样本）、`--gl-break-amount`、`--correct-deposit-record`、`--correct-deposit-amount`、`--inject-missing-fx` | 生成 ref/ods CSV，随后自检（含汇率覆盖与逐期总账平衡自检） | 0 全过 / 1 自检不通过 |
| `load_ref_tables.py` | 位置参数：ref 目录（默认 `/opt/fr2052a-app/sample_data/ref`）| 覆盖写入 Iceberg 的 ref 命名空间 | 0 / 1 有表失败或目录下无 CSV / 2 目录不存在 |
| `export_gold_to_pg.py` | 无参数 | 把 gold 层三张表导出到 PG 的 ads 层，覆盖写 | 0 / 1 |
| `run_dq_rules.py` | `--batch-id`（默认 `UNKNOWN`）| 执行规则集并把结论落审计表；ERROR 级规则若因列名不存在跳过部分目标表，覆盖面缩水即判 FAIL | 0 无 ERROR / 1 有 ERROR |
| `replay_ods_to_kafka.py` | `--data-dir`、`--config`（均有默认）| 重放 ODS 数据到 Kafka | 0 / 1 |
| `generate_submission.py` | `--report-date`、`--output-dir`、`--receipt-file` | 生成三种格式的报送文件并登记台账；回执被拒退 1 | 0 / 1 |
| `time_travel.py` | `--table`、`--list-snapshots`、`--diff`、`--trace-key`、`--key-column`、`--columns` | Iceberg 时间旅行审计；参数经白名单校验，非法退 2 | 0 / 1 / 2 |

### Kafka Topic 契约

| Topic | 源系统 | 落点 | 生产者 |
|-------|--------|------|--------|
| `core_banking_txns` | CORE_BANKING | `bronze.ods_deposits` | 有 |
| `loan_book` | LOAN_SYS | `bronze.ods_loans` | 有 |
| `treasury_deals` | TREASURY_SYS | `bronze.ods_repo_transactions` | 有 |
| `custody_positions` | CUSTODY_SYS | `bronze.ods_securities` | 有 |
| `derivatives_trades` | DERIV_SYS | `bronze.ods_derivatives` | 有 |
| `gl_entries` | FINANCE_SYS | `bronze.ods_gl_balances` | 有 |
| `off_bs_commitments` | OFFBS_SYS | `bronze.ods_off_bs_commitments` | 有 |
| `market_data_prices` | MARKET_DATA | 不落表 | 暂作声明保留（本演示未生成对应的 ODS 表） |
| `reference_data_updates` | REF_DATA | 不落表 | 暂作声明保留（引用数据走批加载直入 ref） |
| `fr2052a_alerts` | ALERTING | 不落表 | 熔断判定写入，供告警下游订阅 |

主题清单、落点与是否有生产者的唯一声明在 `config/pipeline_topics.json`，本表不另抄一份。载荷字段同理：生产者的 JSON 键就是 ODS 表头，字段清单见 `sql/iceberg/02_create_ods_tables.sql`。

## 依赖关系

```text
数据接入 → ODS → OWD → OWS → ADS → 报送
              ↓        ↓        ↓
           数据质量  数据质量  数据质量
              ↓        ↓        ↓
           GL 对账 ←←←←←←←←←←←←
              ↓
           合规熔断 → 报送阻断/放行
              ↓
        血缘与监管映射（dbt meta + 自研渲染）
```

**单向依赖**：上层依赖下层，禁止反向依赖。

## 版本契约

- 文档标题版本 + 代码 docstring + CLI description 三处一致
- 契约升级（枚举/唯一键/规则级别）后必走 drift 检查清单
