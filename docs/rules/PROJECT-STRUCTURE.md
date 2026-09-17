# PROJECT-STRUCTURE.md — 项目结构规范

> 本文件定义项目的目录结构与职责划分。所有开发者必须遵守。

## 目录职责

| 目录 | 职责 | 谁可写 | 禁止行为 |
|------|------|--------|----------|
| `requirements/` | 原始需求文档（参考） | 架构师 | 修改（应走变更流程）|
| `docs/business/` | 九项核心文档（真源） | 全员 | 删除、重命名 |
| `docs/rules/` | 跨项目复用规范 | 架构师 | 未经评审修改 |
| `docs/changes/` | 变更留痕（追加式） | 全员 | 修改历史条目 |
| `deploy/` | 部署清单（不含凭据） | DevOps | 硬编码密码 |
| `sql/` | DDL（真源） | DBA/架构师 | 直接改线上库 |
| `dbt/models/` | 转换逻辑 | 数据工程师 | 绕过 dbt 直接写 SQL |
| `python/` | 脚本工具（薄壳） | 数据工程师 | 承载业务逻辑 |
| `config/` | 配置契约 | 架构师 | 运行时修改 |
| `sample_data/` | 样本数据（可重建） | 测试人员 | 纳入 git |

## 命名规范

- 目录：kebab-case（如 `docs/business/`）
- 文件：PascalCase 或 kebab-case，见下表
- 表名：`{层}_{表名}`（如 `ods_deposits`、`owd_loans`）

| 文件类型 | 命名示例 |
|----------|----------|
| 文档 | `PROJECT.md`、`DATA-DESIGN.md` |
| SQL | `01_create_namespaces.sql`、`91_drop_smoke_tables.sql` |
| Python | `generate_sample_data.py`、`export_gold_to_pg.py` |
| 部署 | `docker-compose-core.yml`、`docker-compose-compute.yml` |

## 分层纪律

1. **根目录**：`README.md` + `AGENTS.md` + `.gitignore` + `.gitattributes`
2. **docs/**：工程文档（rules + business + changes）
3. **deploy/**：部署清单（YAML + shell 脚本）
4. **sql/**：DDL（按层分目录）
5. **dbt/**：dbt 项目（models + macros + config）
6. **python/**：脚本工具（按功能分子目录）
7. **config/**：配置契约（JSON/YAML）
8. **sample_data/**：样本数据（不落 git）

## 收口点

| 类型 | 收口位置 | 说明 |
|------|----------|------|
| 表结构 | `sql/iceberg/*.sql` | 唯一真源 |
| 接口契约 | `docs/business/INTERFACE-DESIGN.md` | dbt macro / Python CLI / Kafka Topic |
| 业务规则 | `dbt/macros/fr2052a_rules.sql` | HQLA、到期分桶、现金流 Cap |
| 部署配置 | `deploy/server{1,2}/` | docker-compose + env |
| 调度定义 | `airflow/dags/` | DAG 定义（待实现）|
