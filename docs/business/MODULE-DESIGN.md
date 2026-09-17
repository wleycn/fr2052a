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
| 数据质量 | GE 校验、规则引擎 | 不做数据纠错、不做阻断 |
| 合规熔断 | `fr2052a_alerts` 检查、Airflow 阻断 | 不做告警发送、不做修复 |
| 调度编排 | Airflow DAG 管理 | 不做业务逻辑、不做数据访问 |
| 元数据治理 | DataHub 摄取、血缘生成 | 不做数据质量校验、不做业务映射 |
| PII 脱敏 | 动态脱敏、权限控制 | 不做数据加密、不做审计日志 |

## 接口契约

### dbt macro 接口

| 宏名 | 参数 | 返回 | 用途 |
|------|------|------|------|
| `fr2052a_hqla_classification` | `security_type, rating` | `LEVEL_1\|LEVEL_2A\|LEVEL_2B\|NON_HQLA` | HQLA 分类 |
| `fr2052a_maturity_bucket` | `maturity_date, report_date` | `O/N\|1-7D\|...` | 到期分桶 |
| `fr2052a_mask_pii` | `field_value` | `h_xxxx` (HMAC 脱敏) | PII 脱敏 |
| `fr2052a_fx_convert` | `amount, currency, report_date` | `amount_usd` | 汇率转换 |

### Python CLI 接口

| 命令 | 参数 | 行为 | 退出码 |
|------|------|------|--------|
| `generate_sample_data.py` | 无 | 生成 ref/ods CSV 到 `sample_data/` | 0/1 |
| `load_ref_tables.py` | `--target iceberg\|postgres` | 加载引用数据 | 0/1 |
| `export_gold_to_pg.py` | `--date YYYY-MM-DD` | 导出 Gold 到 PG | 0/1 |
| `run_dq_rules.py` | `--layer owd\|ows\|ads` | 运行数据质量校验 | 0/1/2 (1=WARNING, 2=ERROR) |
| `replay_ods_to_kafka.py` | `--topic <name> --file <path>` | 重放 ODS 数据到 Kafka | 0/1 |

### Kafka Topic 契约

| Topic | schema | 生产者 | 消费者 |
|-------|--------|--------|--------|
| `core_banking_txns` | `{source_system, source_record_id, txn_type, amount, currency, event_time}` | Core Banking | Spark Streaming |
| `treasury_deals` | `{deal_id, deal_type, notional, currency, counterparty_id, event_time}` | Treasury | Spark Streaming |
| `derivatives_trades` | `{trade_id, instrument_type, notional, currency, counterparty_id, event_time}` | Derivatives | Spark Streaming |
| `market_data_prices` | `{price_type, instrument_id, price, currency, timestamp}` | Market Data | Spark Streaming |
| `gl_entries` | `{gl_account_id, debit, credit, currency, entry_date, event_time}` | GL System | 批处理 |
| `reference_data_updates` | `{ref_type, ref_id, action, data_json, timestamp}` | MDM | 维表更新 |
| `fr2052a_alerts` | `{alert_id, report_date, source_model, severity, message, created_at}` | 校验引擎 | 告警服务 |

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
           DataHub 血缘
```

**单向依赖**：上层依赖下层，禁止反向依赖。

## 版本契约

- 文档标题版本 + 代码 docstring + CLI description 三处一致
- 契约升级（枚举/唯一键/规则级别）后必走 drift 检查清单
