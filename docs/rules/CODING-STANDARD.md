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

```bash
# 使用 venv，不用系统 Python
python3 -m venv ~/fr2052a_venv
source ~/fr2052a_venv/bin/activate
pip install -r requirements.txt
```

### 代码风格

- 使用 ruff 格式化（config 见项目根）
- 类型注解必须添加
- docstring 必须添加（Google 风格）

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
    alert_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
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

- 宏名：`fr2052a_{功能}`（如 `fr2052a_hqla_classification`）
- 参数名：snake_case

### 测试

- 每个模型必须有 `schema.yml` 定义测试
- 必填字段：`not_null`
- 唯一字段：`unique`
- 自定义测试放 `tests/` 目录

## Git 规范

### 分支命名

- feature: `feature/{功能}`（如 `feature/gl-reconciliation`）
- fix: `fix/{问题}`（如 `fix/pg18-data-dir`）
- hotfix: `hotfix/{问题}`（如 `hotfix/circuit-breaker`）

### Commit 消息

- 格式：`[AI] type: subject`（agent 提交）或 `type: subject`（人工提交）
- type: feat/fix/docs/chore
- subject: ≤ 72 字符

### 示例

```bash
# Agent 提交
git commit -m "[AI] feat: add GL reconciliation logic"

# 人工提交
git commit -m "fix: apply HQLA 40% cap in ADS model"
```
