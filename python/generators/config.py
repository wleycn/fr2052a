"""FR 2052a 演示数据生成器 —— 共用配置与工具。

本模块只放常量与通用 IO 帮助函数，不含业务逻辑。
"""

from __future__ import annotations

import csv
import random
from collections.abc import Iterable, Sequence
from datetime import date
from pathlib import Path

# python/generators/config.py -> 上溯两级得到项目根目录
PROJECT_ROOT = Path(__file__).resolve().parents[2]

DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "sample_data"
REF_SUBDIR = "ref"
ODS_SUBDIR = "ods"

# 报告日：T+1 批次统一锚定这一天，所有 ODS 记录归属于它
REPORT_DATE = date(2026, 9, 16)
CALENDAR_START = date(2026, 9, 1)
CALENDAR_END = date(2026, 12, 31)

# 固定种子保证可复现；按表名派生独立种子，新增表不会打乱既有表的数据
RANDOM_SEED = 42

BATCH_ID = "BATCH-20260916-001"

# 各表行数（演示规模，与 [97] 方案一致）
VOLUMES: dict[str, int] = {
    "ods_deposits": 500,
    "ods_repo_transactions": 200,
    "ods_loans": 300,
    "ods_securities": 200,
    "ods_derivatives": 150,
    "ods_gl_balances": 50,
    "ods_off_bs_commitments": 100,
}

# 各法人实体的记账币种权重：伦敦分行以英镑为主、东京分行以日元为主，
# 保证后续汇率折算与合并报表口径不是空跑。
ENTITY_CURRENCY_WEIGHTS: dict[str, dict[str, float]] = {
    "ENT002": {"USD": 0.70, "EUR": 0.10, "GBP": 0.08, "JPY": 0.05, "CNY": 0.04, "HKD": 0.03},
    "ENT003": {"USD": 0.80, "EUR": 0.08, "GBP": 0.06, "JPY": 0.04, "CHF": 0.02},
    "ENT004": {"GBP": 0.50, "USD": 0.25, "EUR": 0.20, "CHF": 0.05},
    "ENT005": {"JPY": 0.55, "USD": 0.30, "EUR": 0.10, "CNY": 0.05},
}

# ODS 各表对应的源系统与源文件名（供 Kafka 重放与血缘标注使用）
ODS_SOURCE_FILES: dict[str, tuple[str, str]] = {
    "ods_deposits": ("CORE_BANKING", "core_deposits_{ymd}.csv"),
    "ods_repo_transactions": ("TREASURY_SYS", "treasury_repo_{ymd}.csv"),
    "ods_loans": ("LOAN_SYS", "loan_book_{ymd}.csv"),
    "ods_securities": ("CUSTODY_SYS", "custody_positions_{ymd}.csv"),
    "ods_derivatives": ("DERIV_SYS", "derivatives_book_{ymd}.csv"),
    "ods_gl_balances": ("FINANCE_SYS", "gl_balances_{ymd}.csv"),
    "ods_off_bs_commitments": ("OFFBS_SYS", "off_bs_commitments_{ymd}.csv"),
}

# ODS 表统一样式：前缀列 + 业务列 + ETL 尾部列
ODS_COLUMNS_HEAD = ["source_system", "source_record_id", "report_date", "entity_code"]
ODS_COLUMNS_TAIL = ["event_time", "etl_batch_id", "etl_source_file"]


def table_rng(table_name: str) -> random.Random:
    """按表名派生独立随机源，保证单表数据可复现且互不干扰。"""
    return random.Random(f"{RANDOM_SEED}:{table_name}")


def write_csv(path: Path, header: Sequence[str], rows: Iterable[Sequence[object]]) -> int:
    """写出 CSV（UTF-8、LF），返回行数。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    materialized = list(rows)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(header)
        writer.writerows(materialized)
    return len(materialized)


def weighted_choice(rng: random.Random, weights: dict[str, float]) -> str:
    """按权重抽取键，权重之和须为 1。"""
    return rng.choices(list(weights), weights=list(weights.values()), k=1)[0]


def source_file(table_name: str, report_date: date) -> str:
    """取该表在本报告日的源文件名。"""
    _, pattern = ODS_SOURCE_FILES[table_name]
    return pattern.format(ymd=report_date.strftime("%Y%m%d"))
