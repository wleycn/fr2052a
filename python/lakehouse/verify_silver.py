"""核对 OWD 层：行数不丢不重、汇率折算正确、到期分桶合法。

OWD 是明细标准化的第一层，出错的方式很隐蔽：行数对了但折算率用错、
分桶落到未定义的值上，都要在这里拦住。

用法（Server 2，经 spark-submit 包装脚本执行）：
    bash spark-submit-fr2052a.sh /opt/fr2052a-app/python/lakehouse/verify_silver.py
"""

from __future__ import annotations

import sys
from dataclasses import dataclass

from pyspark.sql import SparkSession

# OWD 表与其 ODS 上游的对应关系
OWD_TO_ODS = {
    "silver.owd_deposits": "bronze.ods_deposits",
    "silver.owd_secured_financing": "bronze.ods_repo_transactions",
    "silver.owd_loans": "bronze.ods_loans",
    "silver.owd_securities": "bronze.ods_securities",
    "silver.owd_derivatives": "bronze.ods_derivatives",
    "silver.owd_off_bs": "bronze.ods_off_bs_commitments",
    "silver.owd_gl_entries": "bronze.ods_gl_balances",
}

# 没有到期日概念的 OWD 表，不参与分桶核对
TABLES_WITHOUT_MATURITY = ("silver.owd_gl_entries",)

# 折算核对：(OWD 表, OWD 的 USD 列, ODS 上游表, 上游原币列)
FX_CHECKS = (
    ("silver.owd_deposits", "principal_amount_usd", "bronze.ods_deposits", "principal_amount"),
    ("silver.owd_secured_financing", "cash_amount_usd", "bronze.ods_repo_transactions", "cash_amount"),
    ("silver.owd_loans", "outstanding_usd", "bronze.ods_loans", "outstanding_amount"),
    ("silver.owd_securities", "market_value_usd", "bronze.ods_securities", "market_value"),
    ("silver.owd_derivatives", "notional_usd", "bronze.ods_derivatives", "notional_amount"),
    ("silver.owd_off_bs", "undrawn_amount_usd", "bronze.ods_off_bs_commitments", "undrawn_amount"),
)

TOLERANCE = 0.02


@dataclass
class CheckResult:
    """一条核对结论：名称、是否通过、以及给人看的证据。"""

    name: str
    passed: bool
    detail: str


def check_row_counts(spark: SparkSession) -> list[CheckResult]:
    """核对 OWD 表与其上游 ODS 表的行数是否一致。"""
    results = []
    for owd_table, ods_table in OWD_TO_ODS.items():
        upstream = spark.table(ods_table).count()
        current = spark.table(owd_table).count()
        results.append(
            CheckResult(
                name=f"行数 {owd_table}",
                passed=upstream == current,
                detail=f"上游 {upstream} 行，本层 {current} 行",
            )
        )
    return results


def check_maturity_buckets(spark: SparkSession) -> list[CheckResult]:
    """分桶取值必须落在 ref_maturity_bucket 的定义内。"""
    defined = {row["bucket_code"] for row in spark.table("ref.ref_maturity_bucket").collect()}
    results = []
    for owd_table in OWD_TO_ODS:
        if owd_table in TABLES_WITHOUT_MATURITY:
            continue
        observed = {
            row["maturity_bucket"] for row in spark.table(owd_table).select("maturity_bucket").distinct().collect()
        }
        unknown = observed - defined
        results.append(
            CheckResult(
                name=f"到期分桶 {owd_table}",
                passed=not unknown,
                detail=f"{len(observed)} 种取值，全部有定义" if not unknown else f"未定义取值 {sorted(unknown)}",
            )
        )
    return results


def check_fx_conversion(spark: SparkSession) -> list[CheckResult]:
    """折算核对：拿上游原币金额按 ref 汇率重算一遍，与本层 USD 金额逐行比对。"""
    results = []
    for owd_table, usd_column, ods_table, lc_column in FX_CHECKS:
        row = spark.sql(
            f"""
            select count(*) as checked,
                   sum(
                       case
                           when abs(o.{usd_column} - round(s.{lc_column} * f.spot_rate, 2)) > {TOLERANCE}
                           then 1 else 0
                       end
                   ) as mismatched
            from {owd_table} o
            join {ods_table} s
              on s.source_system = o.source_system
             and s.source_record_id = o.source_record_id
            join ref.ref_exchange_rates f
              on f.from_currency = s.currency
             and f.rate_date = s.report_date
            """
        ).collect()[0]
        checked = row["checked"]
        mismatched = row["mismatched"] or 0
        results.append(
            CheckResult(
                name=f"折算 {owd_table}",
                passed=checked > 0 and mismatched == 0,
                detail=f"逐行重算 {checked} 条，偏差超限 {mismatched} 条",
            )
        )
    return results


def main() -> int:
    """跑 OWD 层全部核对项：行数、到期分桶、汇率重算。"""
    spark = SparkSession.builder.appName("fr2052a-verify-silver").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    results: list[CheckResult] = []
    results.extend(check_row_counts(spark))
    results.extend(check_maturity_buckets(spark))
    results.extend(check_fx_conversion(spark))

    for result in results:
        print(f"  [{'PASS' if result.passed else 'FAIL'}] {result.name:<38} {result.detail}")

    failed = [result.name for result in results if not result.passed]
    print()
    if failed:
        print(f"未通过 {len(failed)} 项：{failed}")
        return 1
    print(f"OWD 层 {len(results)} 项核对全部通过")
    return 0


if __name__ == "__main__":
    sys.exit(main())
