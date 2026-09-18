#!/usr/bin/env python3
"""FR 2052a 演示数据生成入口：产出 REF 与 ODS 两套 CSV，并做完整性自检。

用法（在 python/ 目录下）：
    python -m generators.generate_sample_data --out ../sample_data
    python -m generators.generate_sample_data --gl-break-amount 5000000
    python -m generators.generate_sample_data --inject-missing-fx JPY
    python -m generators.generate_sample_data --report-days 2

产出：
    <out>/ref/*.csv   9 张引用数据表，走批加载入 Iceberg 的 ref 命名空间
    <out>/ods/*.csv   7 张业务明细表，经 Kafka 流入 Iceberg 的 bronze 命名空间

自检不通过时以退出码 1 结束，避免把坏数据带进下游。

--inject-missing-fx 故意不写指定币种的汇率行，用于验证「缺汇率必须失败」。
传入后汇率表少行，ODS 生成器将对应币种兜底为 USD，自检放行但打印明示。

--report-days N 生成 N 个连续日历日的多期数据，末尾一期是锚定报告日。
加期不扰动已有期：每期用独立随机源，同一报告日的数据与共生成了几期无关。

[AI-GENERATED] model=qianfan-code-latest date=2026-09-18 reviewed_by=pending
"""

from __future__ import annotations

import argparse
import csv
import sys
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date
from pathlib import Path

if __package__ in (None, ""):
    # 支持 `python python/generators/generate_sample_data.py` 直接执行（PEP 366）
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    __package__ = "generators"

from . import ods_data
from .config import (
    BANKS_PER_ENTITY,
    BOOKING_ENTITIES,
    DEFAULT_OUTPUT_DIR,
    INTRACOMPANY_PAIRS,
    ODS_SUBDIR,
    PARENT_VOLUMES,
    REF_SUBDIR,
    REPORT_DATE,
    VOLUMES,
    report_dates,
)
from .ods_data import GL_ACCOUNTS, GL_ROWS_PER_ACCOUNT, TERM_DEPOSIT_TYPES
from .ref_data import BEHAVIOR_ASSUMPTION_ROWS, FX_RATES, ReferenceData
from .ref_data import generate_all as generate_ref

# 外部对手方数（CP0001–CP0050）+ 集团内对手方数（CP9001–CP9005，每实体一个）
EXTERNAL_COUNTERPARTIES = 50
AFFILIATE_COUNTERPARTIES = len(BOOKING_ENTITIES)  # ENT001–ENT005 各一个

# 各表参与外键校验的交易对手类字段
COUNTERPARTY_FIELDS: dict[str, tuple[str, ...]] = {
    "ods_repo_transactions": ("counterparty_id",),
    "ods_loans": ("borrower_id",),
    "ods_securities": ("issuer_id",),
    "ods_derivatives": ("counterparty_id",),
    "ods_off_bs_commitments": ("counterparty_id",),
}

# 各表的日期先后规则：le = 不晚于报告日，gt = 严格晚于报告日
DATE_RULES: dict[str, tuple[tuple[str, str], ...]] = {
    "ods_deposits": (("open_date", "le"), ("maturity_date", "gt")),
    "ods_repo_transactions": (("start_date", "le"), ("end_date", "gt")),
    "ods_loans": (("origination_date", "le"), ("maturity_date", "gt")),
    "ods_securities": (("purchase_date", "le"), ("maturity_date", "gt")),
    "ods_derivatives": (("trade_date", "le"), ("maturity_date", "gt")),
    "ods_off_bs_commitments": (("maturity_date", "gt"),),
}

EXPECTED_REF_ROWS: dict[str, int] = {
    "ref_entity_hierarchy": 5,
    # 派生：外部对手方数 + 集团内对手方数（每实体一个）
    "ref_counterparty": EXTERNAL_COUNTERPARTIES + AFFILIATE_COUNTERPARTIES,
    "ref_maturity_bucket": 8,
    "ref_fr2052a_line_items": 19,
    # 由 FX_RATES 币种数 + 1（USD→USD）派生，加币种不会因硬编码挡路
    "ref_exchange_rates": len(FX_RATES) + 1,
    "ref_regulatory_mapping": 5,
    "ref_behavior_assumptions": len(BEHAVIOR_ASSUMPTION_ROWS),
    "ref_calendar": 122,
    "ref_validation_rules": 21,
}

# 派生 ODS 单期期望行数：子公司配额 + 母公司追加 + 配对腿追加
# ods_gl_balances 由 GL_ROWS_PER_ACCOUNT × len(GL_ACCOUNTS) × len(BOOKING_ENTITIES) 派生
PAIRS_PER_PERIOD = len(INTRACOMPANY_PAIRS)
EXPECTED_ODS_ROWS: dict[str, int] = {
    "ods_deposits": VOLUMES["ods_deposits"] + PARENT_VOLUMES["ods_deposits"] + PAIRS_PER_PERIOD,
    "ods_repo_transactions": VOLUMES["ods_repo_transactions"] + PARENT_VOLUMES["ods_repo_transactions"],
    "ods_loans": VOLUMES["ods_loans"] + PARENT_VOLUMES["ods_loans"] + PAIRS_PER_PERIOD,
    "ods_securities": VOLUMES["ods_securities"] + PARENT_VOLUMES["ods_securities"],
    "ods_derivatives": VOLUMES["ods_derivatives"] + PARENT_VOLUMES["ods_derivatives"],
    "ods_gl_balances": GL_ROWS_PER_ACCOUNT * len(GL_ACCOUNTS) * len(BOOKING_ENTITIES),
    # 每实体 1 行库存现金 + BANKS_PER_ENTITY 行代理行账户
    "ods_treasury_cash_position": (1 + BANKS_PER_ENTITY) * len(BOOKING_ENTITIES),
    "ods_off_bs_commitments": VOLUMES["ods_off_bs_commitments"] + PARENT_VOLUMES["ods_off_bs_commitments"],
}


@dataclass
class CheckResult:
    """一条自检结论。"""

    name: str
    passed: bool
    detail: str


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    """读一张样本 CSV，按表头取成字典行。"""
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def check_row_counts(
    output_dir: Path,
    inject_missing_fx: set[str] | None = None,
    num_periods: int = 1,
) -> list[CheckResult]:
    """核对每张表的行数与设定值是否一致。

    汇率表在注入缺汇率时行数会少，按注入数调整期望值。
    多期时 ODS 各表期望行数 = 单期行数 × N；汇率表期望行数 = 10 × N。
    单期 ODS 期望行数 = 子公司配额 + 母公司追加 + 配对腿追加（全部派生，无魔数）。
    """
    inject = inject_missing_fx or set()
    expected_ref = dict(EXPECTED_REF_ROWS)
    # 汇率表每期一组（10 行），注入缺汇率时每期少 len(inject) 行
    fx_per_period = len(FX_RATES) + 1 - len(inject)
    expected_ref["ref_exchange_rates"] = fx_per_period * num_periods
    results = []
    # ODS 行数期望用派生值（子公司 + 母公司 + 配对腿）
    all_expected = {**expected_ref, **EXPECTED_ODS_ROWS}
    for table_name, expected in all_expected.items():
        subdir = REF_SUBDIR if table_name.startswith("ref_") else ODS_SUBDIR
        path = output_dir / subdir / f"{table_name}.csv"
        actual = len(read_csv_rows(path)) if path.exists() else -1
        # ODS 表多期时期望 = 单期 × N
        adjusted_expected = expected * num_periods if not table_name.startswith("ref_") else expected
        results.append(
            CheckResult(
                name=f"行数 {table_name}",
                passed=actual == adjusted_expected,
                detail=f"期望 {adjusted_expected}，实际 {actual}",
            )
        )
    return results


def check_report_date(output_dir: Path, valid_dates: list[date]) -> list[CheckResult]:
    """核对每行的报告日都在允许的报告日集合内。"""
    results = []
    valid_set = {d.isoformat() for d in valid_dates}
    for table_name in EXPECTED_ODS_ROWS:
        rows = read_csv_rows(output_dir / ODS_SUBDIR / f"{table_name}.csv")
        offenders = {row["report_date"] for row in rows if row["report_date"] not in valid_set}
        results.append(
            CheckResult(
                name=f"报告日一致 {table_name}",
                passed=not offenders,
                detail="全部在允许集合内" if not offenders else f"异常值 {sorted(offenders)}",
            )
        )
    return results


def check_references(output_dir: Path, ref: ReferenceData) -> list[CheckResult]:
    """核对交易对手、币种等字段都能在 ref 层找到。"""
    results: list[CheckResult] = []
    entity_set = set(ref.entity_codes)
    currency_set = set(ref.currencies)
    # 集团内对手方也算合法引用：它们只在刻意构造的内部往来配对腿里出现，见 ref_data 的说明
    counterparty_set = set(ref.counterparties) | set(ref.affiliate_counterparties)

    entity_offenders: list[str] = []
    currency_offenders: list[str] = []
    counterparty_offenders: list[str] = []

    for table_name in EXPECTED_ODS_ROWS:
        rows = read_csv_rows(output_dir / ODS_SUBDIR / f"{table_name}.csv")
        for row in rows:
            if row["entity_code"] not in entity_set:
                entity_offenders.append(row["source_record_id"])
            if row["currency"] and row["currency"] not in currency_set:
                currency_offenders.append(row["source_record_id"])
            for field in COUNTERPARTY_FIELDS.get(table_name, ()):
                if row[field] and row[field] not in counterparty_set:
                    counterparty_offenders.append(row["source_record_id"])

    results.append(
        CheckResult(
            "实体引用",
            not entity_offenders,
            f"悬空 {len(entity_offenders)} 条" if entity_offenders else "全部命中 REF",
        )
    )
    results.append(
        CheckResult(
            "币种引用",
            not currency_offenders,
            f"悬空 {len(currency_offenders)} 条" if currency_offenders else "全部命中 REF 汇率表",
        )
    )
    results.append(
        CheckResult(
            "交易对手引用",
            not counterparty_offenders,
            f"悬空 {len(counterparty_offenders)} 条" if counterparty_offenders else "全部命中 REF",
        )
    )
    return results


def check_fx_coverage(
    output_dir: Path,
    ref: ReferenceData,
    inject_missing_fx: set[str] | None,
) -> list[CheckResult]:
    """核对 ODS 各表出现的 (report_date, currency) 在汇率表里都有 MID 汇率。

    比对基准是**写出的汇率 CSV**（不是内存里的币种池）：断言要验的是「落盘的数据与
    落盘的汇率自洽」，读产物才算数。汇率表缺行会让 OWD 折算 left join 落空、
    金额列变 NULL、sum() 跳过，报表出来是 0 而不是报错 —— 这条自检把「缺汇率」变显式。
    --inject-missing-fx 指定的币种属于故意注入，自检放行但打印明示。
    """
    inject = inject_missing_fx or set()
    fx_pairs = {
        (row["rate_date"], row["from_currency"])
        for row in read_csv_rows(output_dir / REF_SUBDIR / "ref_exchange_rates.csv")
        if row.get("rate_type") == "MID"
    }
    results: list[CheckResult] = []

    # 六张业务明细表的 currency 列都有值；gl_balances 的 currency 恒为 USD，不在检查范围
    ods_tables_with_currency = [
        "ods_deposits",
        "ods_repo_transactions",
        "ods_loans",
        "ods_securities",
        "ods_derivatives",
        "ods_off_bs_commitments",
    ]

    ods_pairs: set[tuple[str, str]] = set()
    for table_name in ods_tables_with_currency:
        rows = read_csv_rows(output_dir / ODS_SUBDIR / f"{table_name}.csv")
        for row in rows:
            currency = row.get("currency", "")
            if currency:
                ods_pairs.add((row["report_date"], currency))

    missing_pairs: list[tuple[str, str]] = []
    injected_seen: set[str] = set()
    for report_date, currency in sorted(ods_pairs):
        if currency in inject:
            injected_seen.add(currency)
            continue
        if (report_date, currency) not in fx_pairs:
            missing_pairs.append((report_date, currency))

    if injected_seen:
        print(f"  [INJECT] 数据里抽到了故意注入缺汇率的币种（自检放行）：{', '.join(sorted(injected_seen))}")
        print("  [INJECT] 这份数据就是「缺汇率」缺陷数据，dbt 汇率覆盖断言应当报红")

    results.append(
        CheckResult(
            "汇率覆盖",
            not missing_pairs,
            "全部覆盖" if not missing_pairs else f"缺汇率 {len(missing_pairs)} 个组合，例如 {missing_pairs[:3]}",
        )
    )
    return results


def check_date_ordering(output_dir: Path) -> list[CheckResult]:
    """核对日期先后：业务日期不晚于该行报告日，到期日不早于该行报告日。

    多期数据下用每行自己的 report_date 作基准，而不是用全局 REPORT_DATE。
    """
    results = []
    for table_name, rules in DATE_RULES.items():
        rows = read_csv_rows(output_dir / ODS_SUBDIR / f"{table_name}.csv")
        bad: list[str] = []
        for row in rows:
            row_report_date = date.fromisoformat(row["report_date"])
            for field, rule in rules:
                raw = row.get(field, "")
                if not raw:
                    continue  # 活期存款等无到期日，允许为空
                value = date.fromisoformat(raw)
                too_late = rule == "le" and value > row_report_date
                too_early = rule == "gt" and value <= row_report_date
                if too_late or too_early:
                    bad.append(f"{row['source_record_id']}.{field}={raw}")
        results.append(
            CheckResult(
                name=f"日期先后 {table_name}",
                passed=not bad,
                detail="符合业务含义" if not bad else f"异常 {len(bad)} 处，例如 {bad[:3]}",
            )
        )
    return results


def check_gl_balanced(output_dir: Path, valid_dates: list[date]) -> list[CheckResult]:
    """核对总账借贷平衡：逐实体 × 逐报告期，每个实体每期各自平衡。

    旧实现只按报告期判平衡（整本总账一起算）。逐实体记账后，必须每个实体各自平衡，
    否则某个实体的借贷缺口会被其它实体的盈余掩盖。
    """
    rows = read_csv_rows(output_dir / ODS_SUBDIR / "ods_gl_balances.csv")
    results = []
    for rd in valid_dates:
        for entity_code in BOOKING_ENTITIES:
            period_rows = [
                row for row in rows if row["report_date"] == rd.isoformat() and row["entity_code"] == entity_code
            ]
            debit_total = sum(float(row["debit_balance"]) for row in period_rows)
            credit_total = sum(float(row["credit_balance"]) for row in period_rows)
            gap = round(debit_total - credit_total, 2)
            if abs(gap) < 0.01:
                gap = 0.0  # 抹掉浮点尾差，避免报告里出现 "-0.00"
            results.append(
                CheckResult(
                    name=f"总账借贷平衡 {entity_code} {rd.isoformat()}",
                    passed=gap == 0.0,
                    detail=f"借方 {debit_total:,.2f}，贷方 {credit_total:,.2f}，差额 {gap:,.2f}",
                )
            )
    return results


def check_cash_position_reconciles(output_dir: Path, valid_dates: list[date]) -> list[CheckResult]:
    """司库现金头寸与总账的勾稽自检：逐实体 × 逐报告期核对两条判据。

    判据一：对账单余额 + 在途存款 − 未兑现支票 == 该实体总账 1001 + 1100。
    这是 GL 对账 Section E 要判的等式，生成阶段先自证一次，免得等式不成立时
    下游只能看到「对账不平」这一句结论。

    判据二：两侧金额不相等 —— 对账单口径与账面口径真的不同源。
    相等说明基准侧退化成了报送侧的副本，对账变成自己跟自己比（审计点名过的缺陷形态），
    必须在这里报出来，而不是留到对账表里静静 PASS。
    """
    gl_rows = read_csv_rows(output_dir / ODS_SUBDIR / "ods_gl_balances.csv")
    position_rows = read_csv_rows(output_dir / ODS_SUBDIR / "ods_treasury_cash_position.csv")
    results: list[CheckResult] = []
    for rd in valid_dates:
        stamp = rd.isoformat()
        for entity_code in BOOKING_ENTITIES:
            book = round(
                sum(
                    float(row["debit_balance"])
                    for row in gl_rows
                    if row["report_date"] == stamp
                    and row["entity_code"] == entity_code
                    and row["gl_account_id"] in ("1001", "1100")
                ),
                2,
            )
            positions = [
                row for row in position_rows if row["report_date"] == stamp and row["entity_code"] == entity_code
            ]
            statement = round(sum(float(row["balance_amount"]) for row in positions), 2)
            reconciling = round(
                sum(
                    float(row["in_transit_deposits_amount"]) - float(row["outstanding_checks_amount"])
                    for row in positions
                ),
                2,
            )
            gap = round(statement + reconciling - book, 2)
            results.append(
                CheckResult(
                    name=f"现金头寸勾稽 {entity_code} {stamp}",
                    passed=abs(gap) < 0.01,
                    detail=f"对账单 {statement:,.2f} + 调节项 {reconciling:,.2f} vs 账面 {book:,.2f}，差 {gap:,.2f}",
                )
            )
            results.append(
                CheckResult(
                    name=f"现金头寸独立基准 {entity_code} {stamp}",
                    passed=abs(statement - book) >= 0.01 and abs(reconciling) >= 0.01,
                    detail=(
                        f"对账单与账面差 {round(statement - book, 2):,.2f}，调节项 {reconciling:,.2f}"
                        "（两侧相等或调节项为零都会让对账失去意义）"
                    ),
                )
            )
    return results


def check_intracompany_pairs(output_dir: Path) -> list[CheckResult]:
    """内部往来配对自检：逐对核对存款腿本金 == 贷款腿 outstanding，且两侧实体与对手方编号互相对得上。

    判据：
    1. 存款腿：ods_deposits 里 customer_type_raw='AFFIL' 的行，entity_code 应为母公司 ENT001，
       customer_id 应为对应子公司的集团内对手方编号，principal_amount 应等于 INTRACOMPANY_PAIRS 里的金额。
    2. 贷款腿：ods_loans 里 source_record_id 以 'ICA-L-' 开头的行，entity_code 应为对应子公司，
       borrower_id 应为母公司的集团内对手方编号 CP9001，outstanding_amount 应等于同一金额。
    3. 两条腿金额必须相等（它们共用同一个常量，所以天然相等）。
    """
    from .config import INTRACOMPANY_PAIRS as PAIRS

    deposits = read_csv_rows(output_dir / ODS_SUBDIR / "ods_deposits.csv")
    loans = read_csv_rows(output_dir / ODS_SUBDIR / "ods_loans.csv")

    # 按 customer_id 索引存款腿（AFFIL 类型的行）
    dep_by_customer: dict[str, dict[str, str]] = {}
    for row in deposits:
        if row.get("customer_type_raw") == "AFFIL":
            dep_by_customer[row["customer_id"]] = row

    # 按 source_record_id 索引贷款腿（ICA-L- 开头）
    loan_by_id: dict[str, dict[str, str]] = {}
    for row in loans:
        if row["source_record_id"].startswith("ICA-L-"):
            loan_by_id[row["source_record_id"]] = row

    results: list[CheckResult] = []
    all_match = True
    details: list[str] = []

    for pair_index, (parent_code, sub_code, expected_amount) in enumerate(PAIRS):
        # 存款腿：母公司账上，customer_id = 子公司的对手方编号
        sub_cp = f"CP900{int(sub_code[-1])}"  # ENT002 → CP9002
        dep_row = dep_by_customer.get(sub_cp)
        if dep_row is None:
            all_match = False
            details.append(f"对 {pair_index + 1}：找不到存款腿（customer_id={sub_cp}）")
            continue
        dep_principal = float(dep_row["principal_amount"])
        dep_entity_ok = dep_row["entity_code"] == parent_code

        # 贷款腿：子公司账上，borrower_id = 母公司的对手方编号 CP9001
        loan_id = f"ICA-L-{pair_index:06d}"
        loan_row = loan_by_id.get(loan_id)
        if loan_row is None:
            all_match = False
            details.append(f"对 {pair_index + 1}：找不到贷款腿（source_record_id={loan_id}）")
            continue
        loan_outstanding = float(loan_row["outstanding_amount"])
        loan_entity_ok = loan_row["entity_code"] == sub_code
        loan_cp_ok = loan_row["borrower_id"] == "CP9001"

        amounts_equal = abs(dep_principal - loan_outstanding) < 0.01
        amount_ok = abs(dep_principal - expected_amount) < 0.01

        if not (dep_entity_ok and loan_entity_ok and loan_cp_ok and amounts_equal and amount_ok):
            all_match = False
            details.append(
                f"对 {pair_index + 1} ({parent_code}↔{sub_code}, 预期 {expected_amount}): "
                f"存款腿 entity={dep_row['entity_code']}({'OK' if dep_entity_ok else 'BAD'}), "
                f"贷款腿 entity={loan_row['entity_code']}({'OK' if loan_entity_ok else 'BAD'}), "
                f"loan_cp={'OK' if loan_cp_ok else 'BAD'}, "
                f"dep_principal={dep_principal}, loan_outstanding={loan_outstanding}, "
                f"金额相等={'YES' if amounts_equal else 'NO'}"
            )

    results.append(
        CheckResult(
            name="内部往来配对自检",
            passed=all_match,
            detail="全部匹配" if all_match else f"{len(details)} 处不匹配：{details[0] if details else ''}",
        )
    )
    return results


def check_amount_signs(output_dir: Path) -> list[CheckResult]:
    """金额字段不得为负：借贷两侧、本金与额度都只能是正数或零。

    衍生品的盯市价值可正可负，因此不在此列。
    """
    sign_fields: dict[str, tuple[str, ...]] = {
        "ods_deposits": ("principal_amount", "accrued_interest"),
        "ods_repo_transactions": ("cash_amount", "collateral_market_value"),
        "ods_loans": ("facility_amount", "outstanding_amount", "undrawn_amount"),
        "ods_securities": ("face_amount", "market_value", "book_value"),
        "ods_gl_balances": ("debit_balance", "credit_balance"),
        "ods_treasury_cash_position": (
            "balance_amount",
            "in_transit_deposits_amount",
            "outstanding_checks_amount",
        ),
        "ods_off_bs_commitments": ("facility_amount", "undrawn_amount"),
    }
    results = []
    for table_name, fields in sign_fields.items():
        rows = read_csv_rows(output_dir / ODS_SUBDIR / f"{table_name}.csv")
        bad = [
            f"{row['source_record_id']}.{field}={row[field]}"
            for row in rows
            for field in fields
            if float(row[field]) < 0
        ]
        results.append(
            CheckResult(
                name=f"金额非负 {table_name}",
                passed=not bad,
                detail="全部非负" if not bad else f"出现负值 {len(bad)} 处，例如 {bad[:2]}",
            )
        )
    return results


# 行为假设覆盖率自检用的维度归一表：与 dbt/macros/fr2052a_rules.sql 的
# deposit_product_category 与 customer_segment 两个宏同源。
# 复现一份是为了在生成阶段（还没有 Spark）就拦住「假设表缺组合」；权威判定在转换后的
# 数据上做（dbt/tests/assert_behavior_covered.sql）。两边若漂移，dbt 侧断言会失败。
DEPOSIT_CATEGORY: dict[str, str] = {
    "CHK": "DEMAND",
    "SAV": "SAVINGS",
    "MMDA": "SAVINGS",
    "CD": "CD",
    "TIME": "TIME",
}
CUSTOMER_SEGMENT: dict[str, str] = {
    "IND": "RETAIL",
    "CORP": "CORPORATE",
    "FI": "FINANCIAL",
    "GOV": "SOVEREIGN",
    "AFFIL": "AFFILIATE",
}
# 行为分桶的可能取值：活期/储蓄按行为口径恒归 O/N，定期类按剩余天数落到任一到期桶。
# 只列取值不列阈值 —— 阈值由 behavioral_bucket 宏定义，自检不复现阈值，避免两处各写一份。
DEMAND_BUCKETS: tuple[str, ...] = ("O/N",)
TERM_BUCKETS: tuple[str, ...] = ("O/N", "1-7D", "8-30D", "31-90D", "91-180D", "181D-1Y", ">1Y")


def check_behavior_coverage(output_dir: Path) -> list[CheckResult]:
    """行为假设覆盖率自检：ODS 存款里出现的每个组合都必须在假设表里有行。

    组合 = (product_category, customer_segment, maturity_bucket)。

    定期存款不逐笔算剩余天数，只要求「可能落到的每个到期桶」都有假设行 ——
    具体落哪个桶由 behavioral_bucket 宏算，自检不复现它的阈值。

    缺行即 FAIL。这条自检在生成阶段跑（那时还没有 Spark），与转换后的
    dbt/tests/assert_behavior_covered.sql 一前一后守同一件事。
    """
    assumptions = read_csv_rows(output_dir / REF_SUBDIR / "ref_behavior_assumptions.csv")
    assumption_keys = {
        (row["product_category"], row["customer_segment"], row["maturity_bucket"]) for row in assumptions
    }

    deposits = read_csv_rows(output_dir / ODS_SUBDIR / "ods_deposits.csv")
    missing: set[tuple[str, str, str]] = set()
    probes = 0
    # 定期存款必须带到期日：到期日为空会掉进 OPEN 桶，而行为假设矩阵只有 O/N 与到期桶
    term_without_maturity: list[str] = []
    for row in deposits:
        deposit_type = row.get("deposit_type", "")
        category = DEPOSIT_CATEGORY.get(deposit_type)
        segment = CUSTOMER_SEGMENT.get(row.get("customer_type_raw", ""))
        if category is None or segment is None:
            missing.add((deposit_type or "?", row.get("customer_type_raw", "?"), "维度未归一"))
            continue
        if deposit_type in TERM_DEPOSIT_TYPES:
            if not row.get("maturity_date"):
                term_without_maturity.append(row["source_record_id"])
            buckets = TERM_BUCKETS
        else:
            buckets = DEMAND_BUCKETS
        probes += len(buckets)
        missing |= {
            (category, segment, bucket) for bucket in buckets if (category, segment, bucket) not in assumption_keys
        }

    return [
        CheckResult(
            name="行为假设覆盖",
            passed=not missing,
            detail=(
                f"ODS 探查组合 {probes}，假设表行数 {len(assumption_keys)}，全部命中"
                if not missing
                else f"缺 {len(missing)} 个组合，例如 {sorted(missing)[:3]}"
            ),
        ),
        CheckResult(
            name="定期存款到期日非空",
            passed=not term_without_maturity,
            detail=(
                "全部有到期日"
                if not term_without_maturity
                else f"{len(term_without_maturity)} 行缺到期日，例如 {term_without_maturity[:2]}"
            ),
        ),
    ]


def run_checks(
    output_dir: Path,
    ref: ReferenceData,
    inject_missing_fx: set[str] | None = None,
    valid_dates: list[date] | None = None,
) -> list[CheckResult]:
    """跑完全部自检项，返回结论清单。"""
    if valid_dates is None:
        valid_dates = [REPORT_DATE]
    results: list[CheckResult] = []
    results.extend(check_row_counts(output_dir, inject_missing_fx, len(valid_dates)))
    results.extend(check_report_date(output_dir, valid_dates))
    results.extend(check_references(output_dir, ref))
    results.extend(check_fx_coverage(output_dir, ref, inject_missing_fx))
    results.extend(check_date_ordering(output_dir))
    results.extend(check_amount_signs(output_dir))
    results.extend(check_gl_balanced(output_dir, valid_dates))
    results.extend(check_cash_position_reconciles(output_dir, valid_dates))
    results.extend(check_intracompany_pairs(output_dir))
    results.extend(check_behavior_coverage(output_dir))
    return results


def print_report(results: Iterable[CheckResult]) -> bool:
    """打印自检结论，并返回是否全部通过。"""
    all_passed = True
    for result in results:
        mark = "PASS" if result.passed else "FAIL"
        all_passed = all_passed and result.passed
        print(f"  [{mark}] {result.name:<28} {result.detail}")
    return all_passed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """解析命令行参数。"""
    parser = argparse.ArgumentParser(description="生成 FR 2052a 演示数据（REF + ODS）")
    parser.add_argument("--out", type=Path, default=DEFAULT_OUTPUT_DIR, help="输出根目录，默认 sample_data/")
    parser.add_argument(
        "--gl-break-amount",
        type=float,
        default=0.0,
        help="让 ENT002 的 2100 科目（贷款）故意少记这么多（USD），供合规剧本演示对账阻断；默认 0 表示严格平衡",
    )
    parser.add_argument(
        "--correct-deposit-record",
        default=None,
        help="演示重述剧本：指定要修正的存款记录号（source_record_id）",
    )
    parser.add_argument(
        "--correct-deposit-amount",
        type=float,
        default=None,
        help="演示重述剧本：把该记录的本金改为这个数（USD 记账币种原币金额）",
    )
    parser.add_argument(
        "--inject-missing-fx",
        default="",
        help="故意不写这些币种的汇率行，用于验证「缺汇率必须失败」；逗号分隔多个，如 JPY,EUR",
    )
    parser.add_argument(
        "--report-days",
        type=int,
        default=1,
        help="生成几个连续日历日的多期数据，末尾一期是锚定报告日；默认 1 表示单期",
    )
    args = parser.parse_args(argv)
    if args.report_days < 1:
        parser.error("--report-days 必须 >= 1")
    if (args.correct_deposit_record is None) != (args.correct_deposit_amount is None):
        parser.error(
            "--correct-deposit-record 与 --correct-deposit-amount 必须同时给出："
            "只给一个不知道要改成多少，也不知道改哪一条。"
        )
    return args


def main(argv: list[str] | None = None) -> int:
    """生成 ref 与 ODS 两套 CSV，随后自检；不通过则以退出码 1 结束。"""
    args = parse_args(argv)
    ref_dir = args.out / REF_SUBDIR
    ods_dir = args.out / ODS_SUBDIR

    inject_missing_fx: set[str] = {c.strip() for c in args.inject_missing_fx.split(",") if c.strip()}

    valid_dates = report_dates(args.report_days)

    print("=" * 72)
    print(f"生成 REF 层引用数据 → {ref_dir}")
    print("=" * 72)
    ref = generate_ref(ref_dir, inject_missing_fx=inject_missing_fx or None, report_dates=valid_dates)

    for table_name, count in ref.row_counts.items():
        print(f"  [WRITE] {table_name:<28} {count} 行")

    print()
    print("=" * 72)
    print(f"生成 ODS 层业务数据 → {ods_dir}")
    print("=" * 72)
    ods_counts = ods_data.generate_all(
        ods_dir,
        ref,
        args.gl_break_amount,
        correction=(
            None if args.correct_deposit_record is None else (args.correct_deposit_record, args.correct_deposit_amount)
        ),
        report_dates_list=valid_dates,
    )
    for table_name, count in ods_counts.items():
        print(f"  [WRITE] {table_name:<28} {count} 行")

    print()
    print("=" * 72)
    print("完整性自检")
    print("=" * 72)
    passed = print_report(
        run_checks(args.out, ref, inject_missing_fx=inject_missing_fx or None, valid_dates=valid_dates)
    )

    print()
    if passed:
        print(f"全部自检通过。数据目录：{args.out.resolve()}")
        return 0
    print("自检未通过，请勿将本批数据送入下游。")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
