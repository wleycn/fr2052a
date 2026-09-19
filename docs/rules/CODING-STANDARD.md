# CODING-STANDARD.md — 编码规范

> 本文件定义项目的编码规范。所有开发者必须遵守。

## 通用规范

### 命名

- 变量：snake_case（如 `report_date`、`total_funding`）
- 函数：snake_case（如 `load_ref_tables()`、`run_dq_rules()`）
- 类：PascalCase（如 `DataGenerator`、`KafkaProducer`）
- 常量：UPPER_SNAKE_CASE（如 `REPORT_DATE`、`MAX_RETRIES`）
- 表名：`{层}_{表名}`（如 `ods_deposits`、`owd_loans`）

### 注释

- 注释解释「**为什么**」，不复述「是什么」
- 函数头部加 docstring（Google 风格）
- 复杂逻辑加行内注释

### 错误处理

- 禁止空 pass
- 异常必须记录日志
- 退出码语义明确（0=成功，1=失败，2=阻断）

## Python 规范

### 依赖管理

- 虚拟环境在 Server 2 的 `~/fr2052a-infra/venv`，由 `deploy/server2/setup-venv.sh` 用 uv 创建，Python 3.11。
  为什么是 3.11：dbt-spark 与 PySpark 3.5.9 的官方支持区间到 3.11，理由写在那个脚本的头部注释里。
- 依赖在那个脚本里声明，本项目不维护 `requirements.txt`：环境只有一个，两处声明必然漂移。
- Spark 侧第三方 jar 由 `deploy/server2/fetch-deps.sh` 下载并校验，不进版本库。

### 代码风格

三条都由机器闸强制，配置只此一份（项目根 `pyproject.toml`），跑 `make lint` 即全部生效。

| 要求 | 由谁强制 | 现状 |
|------|----------|------|
| ruff 检查与格式化 | `ruff check` 与 `ruff format --check` | 41 个 Python 文件零告警、格式已统一 |
| 类型注解必须添加 | `mypy`，开了「禁未注解函数」与「禁裸泛型」 | 41 个文件零错误 |
| docstring 必须添加（Google 风格） | `ruff` 的 `D` 规则，`pydocstyle` 走 google 约定 | 全仓零告警 |

被排除的规则与理由都写在 `pyproject.toml` 里，改口径只能改那一处：

| 规则 | 命中 | 排除理由 |
|------|------|----------|
| `D415` | 281 | 只认 `.` `?` `!` 结尾。本项目 docstring 用中文，句末是「。」，规则表达不了 |
| `N812` | 6 | 禁止 `import functions as F`，而这是 PySpark 全生态的写法 |
| `RUF001` | 693 | 把字符串里的中文全角标点判成「歧义字符」 |
| `RUF002` | 1546 | 同 `RUF001`，对象是 docstring |
| `RUF003` | 550 | 同 `RUF001`，对象是注释 |

只排除有命中的规则：`D401` 与 `D202` 在本仓当前 0 命中，就不列进排除表，留着它们继续管事。

提交闸：执行一次 `git config core.hooksPath .githooks` 后，`.githooks/pre-commit` 会在每次提交前跑 `make lint`，不过就拦下提交。

### 示例

```python
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""FR 2052a 测试数据生成器。

生成 ref/ods CSV 数据，保证引用完整性。
"""

from __future__ import annotations

import csv
from pathlib import Path
from datetime import date


def generate_sample_data(
    output_dir: Path,
    report_date: date,
    seed: int = 42,
) -> None:
    """生成测试数据。

    Args:
        output_dir: 输出目录
        report_date: 报告日期
        seed: 随机种子（默认 42）
    """
    # 实现...
```

## SQL 规范

### 命名

- 表名：小写 + 下划线（如 `ods_deposits`）
- 字段名：小写 + 下划线（如 `report_date`）
- 索引名：`idx_{表名}_{字段}`（如 `idx_alerts_unresolved_critical`）

### 约束

- 主键必须显式声明
- 外键必须显式声明
- CHECK 约束用于枚举校验
- NOT NULL 用于必填字段

### 示例

```sql
CREATE TABLE ads.fr2052a_alerts (
    alert_id BIGSERIAL PRIMARY KEY,
    report_date DATE NOT NULL,
    severity VARCHAR(20) NOT NULL CHECK (severity IN ('CRITICAL', 'WARNING', 'INFO')),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_alerts_unresolved_critical
ON ads.fr2052a_alerts (report_date, severity, is_resolved)
WHERE severity = 'CRITICAL' AND is_resolved = FALSE;
```

## dbt 规范

### 模型命名

- 模型文件名：`{层}_{表名}.sql`（如 `owd_deposits.sql`）
- 模型别名：与表名一致

### macro 命名

- 宏名不带前缀，用功能名（如 `hqla_level`、`maturity_bucket`、`mask_pii`）
- 文件名以 `fr2052a_` 开头（如 `dbt/macros/fr2052a_rules.sql`），一个文件放一类规则
- 参数名：snake_case

### 测试

- 每个模型必须有 `schema.yml` 定义测试
- 必填字段：`not_null`
- 唯一字段：`unique`
- 自定义测试放 `tests/` 目录

## Git 规范

### 分支命名

本项目单人加 agent，默认**直接提交到 `main`**，不开 feature 分支也不提 MR（见 `AGENTS.md` §4）。确需并行试验时按下表命名，用完即删：

- feature: `feature/{功能}`（如 `feature/gl-reconciliation`）
- fix: `fix/{问题}`（如 `fix/pg18-data-dir`）
- hotfix: `hotfix/{问题}`（如 `hotfix/circuit-breaker`）

### Commit 消息

- 格式：`type: subject`，不区分提交者（agent 代用户执行，它提交的即用户提交）
- type: feat/fix/docs/chore
- subject: ≤ 72 字符

### 示例

```bash
git commit -m "feat: add GL reconciliation logic"
git commit -m "fix: apply HQLA 40% cap in ADS model"
```
