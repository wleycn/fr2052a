#!/usr/bin/env python3
"""FR 2052a 演示数据生成入口：产出 REF 与 ODS 两套 CSV，并做完整性自检。

用法（在 python/ 目录下）：
    python -m generators.generate_sample_data --out ../sample_data
    python -m generators.generate_sample_data --gl-break-amount 5000000
    python -m generators.generate_sample_data --inject-missing-fx JPY

产出：
    <out>/ref/*.csv   9 张引用数据表，走批加载入 Iceberg 的 ref 命名空间
    <out>/ods/*.csv   7 张业务明细表，经 Kafka 流入 Iceberg 的 bronze 命名空间

自检不通过时以退出码 1 结束，避免把坏数据带进下游。

--inject-missing-fx 故意不写指定币种的汇率行，用于验证「缺汇率必须失败」。
传入后汇率表少行，ODS 生成器将对应币种兜底为 USD，自检放行但打印明示。
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
    DEFAULT_OUTPUT_DIR,
    ODS_SUBDIR,
    REF_SUBDIR,
    REPORT_DATE,
    VOLUMES,
)
from .ref_data import FX_RATES, ReferenceData
from .ref_data import generate_all as generate_ref

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
    "ref_counterparty": 50,
    "ref_maturity_bucket": 8,
    "ref_fr2052a_line_items": 19,
    # 由 FX_RATES 币种数 + 1（USD→USD）派生，加币种不会因硬编码挡路
    "ref_exchange_rates": len(FX_RATES) + 1,
    "ref_regulatory_mapping": 5,
    "ref_behavior_assumptions": 6,
    "ref_calendar": 122,
    "ref_validation_rules": 20,
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


def check_row_counts(output_dir: Path, inject_missing_fx: set[str] | None = None) -> list[CheckResult]:
    """核对每张表的行数与设定值是否一致。

    汇率表在注入缺汇率时行数会少，按注入数调整期望值。
    """
    inject = inject_missing_fx or set()
    expected_ref = dict(EXPECTED_REF_ROWS)
    if inject:
        expected_ref["ref_exchange_rates"] = len(FX_RATES) + 1 - len(inject)
    results = []
    for table_name, expected in {**expected_ref, **VOLUMES}.items():
        subdir = REF_SUBDIR if table_name.startswith("ref_") else ODS_SUBDIR
        path = output_dir / subdir / f"{table_name}.csv"
        actual = len(read_csv_rows(path)) if path.exists() else -1
        results.append(
            CheckResult(
                name=f"行数 {table_name}",
                passed=actual == expected,
                detail=f"期望 {expected}，实际 {actual}",
            )
        )
    return results


def check_report_date(output_dir: Path) -> list[CheckResult]:
    """核对每行的报告日都等于约定的报告日。"""
    results = []
    expected = REPORT_DATE.isoformat()
    for table_name in VOLUMES:
        rows = read_csv_rows(output_dir / ODS_SUBDIR / f"{table_name}.csv")
        offenders = {row["report_date"] for row in rows if row["report_date"] != expected}
        results.append(
            CheckResult(
                name=f"报告日一致 {table_name}",
                passed=not offenders,
                detail="全部落在报告日" if not offenders else f"异常值 {sorted(offenders)}",
            )
        )
    return results


def check_references(output_dir: Path, ref: ReferenceData) -> list[CheckResult]:
    """核对交易对手、币种等字段都能在 ref 层找到。"""
    results: list[CheckResult] = []
    entity_set = set(ref.entity_codes)
    currency_set = set(ref.currencies)
    counterparty_set = set(ref.counterparties)

    entity_offenders: list[str] = []
    currency_offenders: list[str] = []
    counterparty_offenders: list[str] = []

    for table_name in VOLUMES:
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
    """核对日期先后：业务日期不晚于报告日，到期日不早于起始日。"""
    results = []
    for table_name, rules in DATE_RULES.items():
        rows = read_csv_rows(output_dir / ODS_SUBDIR / f"{table_name}.csv")
        bad: list[str] = []
        for row in rows:
            for field, rule in rules:
                raw = row.get(field, "")
                if not raw:
                    continue  # 活期存款等无到期日，允许为空
                value = date.fromisoformat(raw)
                too_late = rule == "le" and value > REPORT_DATE
                too_early = rule == "gt" and value <= REPORT_DATE
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


def check_gl_balanced(output_dir: Path) -> CheckResult:
    """核对总账借贷平衡：借方合计等于贷方合计。"""
    rows = read_csv_rows(output_dir / ODS_SUBDIR / "ods_gl_balances.csv")
    debit_total = sum(float(row["debit_balance"]) for row in rows)
    credit_total = sum(float(row["credit_balance"]) for row in rows)
    gap = round(debit_total - credit_total, 2)
    if abs(gap) < 0.01:
        gap = 0.0  # 抹掉浮点尾差，避免报告里出现 "-0.00"
    return CheckResult(
        name="总账借贷平衡",
        passed=gap == 0.0,
        detail=f"借方 {debit_total:,.2f}，贷方 {credit_total:,.2f}，差额 {gap:,.2f}",
    )


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


def run_checks(output_dir: Path, ref: ReferenceData, inject_missing_fx: set[str] | None = None) -> list[CheckResult]:
    """跑完全部自检项，返回结论清单。"""
    results: list[CheckResult] = []
    results.extend(check_row_counts(output_dir, inject_missing_fx))
    results.extend(check_report_date(output_dir))
    results.extend(check_references(output_dir, ref))
    results.extend(check_fx_coverage(output_dir, ref, inject_missing_fx))
    results.extend(check_date_ordering(output_dir))
    results.extend(check_amount_signs(output_dir))
    results.append(check_gl_balanced(output_dir))
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
        help="让总账故意少记这么多（USD），供合规剧本演示对账阻断；默认 0 表示严格平衡",
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
    args = parser.parse_args(argv)
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

    print("=" * 72)
    print(f"生成 REF 层引用数据 → {ref_dir}")
    print("=" * 72)
    ref = generate_ref(ref_dir, inject_missing_fx=inject_missing_fx or None)
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
    )
    for table_name, count in ods_counts.items():
        print(f"  [WRITE] {table_name:<28} {count} 行")

    print()
    print("=" * 72)
    print("完整性自检")
    print("=" * 72)
    passed = print_report(run_checks(args.out, ref, inject_missing_fx=inject_missing_fx or None))

    print()
    if passed:
        print(f"全部自检通过。数据目录：{args.out.resolve()}")
        return 0
    print("自检未通过，请勿将本批数据送入下游。")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
