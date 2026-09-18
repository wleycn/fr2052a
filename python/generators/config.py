"""FR 2052a 演示数据生成器 —— 共用配置与工具。

本模块只放常量与通用 IO 帮助函数，不含业务逻辑。
"""

from __future__ import annotations

import csv
import random
from collections.abc import Iterable, Sequence
from datetime import date, timedelta
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
    "ods_gl_balances": 55,
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
    """按表名派生独立随机源，保证单表数据可复现且互不干扰。

    本函数用于 REF 层（引用数据不随报告日变化），ODS 层请用 ods_rng。
    """
    return random.Random(f"{RANDOM_SEED}:{table_name}")


def report_dates(count: int, anchor: date = REPORT_DATE) -> list[date]:
    """返回连续日历日列表，末尾是锚定报告日。

    Args:
        count: 要生成几个报告日。
        anchor: 锚定报告日，列表的最后一个元素。默认为 REPORT_DATE。

    Returns:
        连续日历日列表，末尾是 anchor。count=1 时返回 [anchor]，
        count=2 时返回 [anchor-1天, anchor]。

    Raises:
        ValueError: count < 1 时直接报错。
    """
    if count < 1:
        raise ValueError(f"report_dates count 必须 >= 1，收到 {count}")
    return [anchor - timedelta(days=count - 1 - i) for i in range(count)]


def ods_rng(table_name: str, report_date: date) -> random.Random:
    """按 (表名, 报告日) 派生独立随机源，供 ODS 层生成器使用。

    同一天的数据必须与「本次共生成了几期」无关。如果随机源只按表名派生
    （像 table_rng 那样），加期时已有期的随机序列会被推后，导致同一报告日
    的数据悄悄变化，多期回归测试就无法拿 1 期与 2 期的同一报告日逐字节比对。

    本函数把报告日纳入种子，使每个 (表名, 报告日) 组合得到独立且确定的随机源：
    不管共生成了几期，同一天的种子相同、数据相同。

    Args:
        table_name: ODS 表名。
        report_date: 该期的报告日。

    Returns:
        确定性的随机源，同一 (表名, 报告日) 每次返回相同的随机序列。
    """
    return random.Random(f"{RANDOM_SEED}:{table_name}:{report_date.isoformat()}")


def write_csv(
    path: Path,
    header: Sequence[str],
    rows: Iterable[Sequence[object]],
    append: bool = False,
) -> int:
    """写出 CSV（UTF-8、LF），返回**本次写入**的行数。

    append=True 时只追加数据行、不重复写表头：多期生成按报告日逐期追加，
    每期写出的行与表头都与单期生成时一致。
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    materialized = list(rows)
    mode = "a" if append and path.exists() else "w"
    with path.open(mode, newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        if mode == "w":
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
