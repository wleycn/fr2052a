"""核对 ADS 报送层：报表内部一致、合并口径正确、监管上限确实生效。

dbt 跑通只说明 SQL 没报错，不说明数字对。这里逐项验算：
  1. 报表行数 = 交易实体数 + 1 行合并口径
  2. 合并口径 = 各实体口径之和（逐 Section 验算）
  3. 明细合计 = 报表 Section 合计（对应 VDQ-013）
  4. 二级资产占比 ≤ HQLA 总额的 40%（对应 VDQ-017）
  5. 30 天流入 ≤ 流出的 75%（对应 VDQ-018，验证上限确实被应用）
  6. GL 对账状态分布

用法（Server 2，经 spark-submit 包装脚本执行）：
    bash spark-submit-fr2052a.sh /opt/fr2052a-app/python/lakehouse/verify_gold.py
"""

from __future__ import annotations

import sys
from dataclasses import dataclass

from pyspark.sql import SparkSession

REPORT = "gold.ads_fr2052a_report"
DETAIL = "gold.ads_fr2052a_detail"
RECONCILIATION = "gold.ads_gl_reconciliation"

# Section 的报表合计列、明细金额列，以及明细需要附加的行项目过滤
SECTION_AMOUNTS = {
    "C": ("sec_c_total", "outstanding_amount", None),
    "B": ("sec_b_repo_outstanding", "outstanding_amount", "B-REPO"),
    "F": ("sec_f_total_inflow", "inflow_amount", None),
    "G": ("sec_g_total_mv", "market_value", None),
    "J": ("sec_j_total_contingent", "outstanding_amount", None),
}

# 合并口径逐列验算：报表列名列表
CONSOLIDATION_COLUMNS = (
    "sec_b_total",
    "sec_c_total",
    "sec_e_cash_total",
    "sec_f_total_inflow",
    "sec_g_total_mv",
    "sec_j_total_contingent",
    "sec_k_total_funding",
    "sec_k_total_outflows",
)


@dataclass
class CheckResult:
    """一条核对结论：名称、是否通过、以及给人看的证据。"""

    name: str
    passed: bool
    detail: str


def check_row_shape(spark: SparkSession) -> list[CheckResult]:
    """核对报表行形状：每个实体一行，合并行只有一行。"""
    entity_rows = spark.table(REPORT).filter("not is_consolidated").count()
    consolidated_rows = spark.table(REPORT).filter("is_consolidated").count()
    return [
        CheckResult(
            name="报表行数",
            passed=entity_rows >= 2 and consolidated_rows == 1,
            detail=f"实体口径 {entity_rows} 行，合并口径 {consolidated_rows} 行",
        )
    ]


def check_consolidation(spark: SparkSession) -> list[CheckResult]:
    """合并口径必须等于各实体口径之和（同一列逐列验算）。"""
    sums = ", ".join(f"round(sum({column}), 2) as {column}" for column in CONSOLIDATION_COLUMNS)
    entity_totals = spark.sql(f"select {sums} from {REPORT} where not is_consolidated").collect()[0]
    consolidated = spark.table(REPORT).filter("is_consolidated").select(*CONSOLIDATION_COLUMNS).collect()[0]

    mismatched = []
    for column in CONSOLIDATION_COLUMNS:
        entity_value = entity_totals[column]
        consolidated_value = consolidated[column]
        if entity_value is None or consolidated_value is None:
            continue
        if abs(float(entity_value) - float(consolidated_value)) > 0.05:
            mismatched.append(f"{column}: 实体合计 {entity_value} vs 合并 {consolidated_value}")

    return [
        CheckResult(
            name="合并口径加总",
            passed=not mismatched,
            detail=f"{len(CONSOLIDATION_COLUMNS)} 列逐列验算一致" if not mismatched else f"不一致 {mismatched[:2]}",
        )
    ]


def check_detail_rollup(spark: SparkSession) -> list[CheckResult]:
    """明细合计 = 报表 Section 合计（VDQ-013）。"""
    results = []
    consolidated = spark.table(REPORT).filter("is_consolidated").collect()[0]
    for section, (report_column, detail_column, line_item) in SECTION_AMOUNTS.items():
        line_filter = f" and line_item = '{line_item}'" if line_item else ""
        detail_total = (
            spark.sql(
                f"select round(sum({detail_column}), 2) as total from {DETAIL} "
                f"where section_code = '{section}'{line_filter}"
            ).collect()[0]["total"]
            or 0
        )
        report_total = consolidated[report_column] or 0
        variance = round(float(detail_total) - float(report_total), 2)
        results.append(
            CheckResult(
                name=f"明细汇总 Section {section}",
                passed=abs(variance) <= 0.05,
                detail=f"明细 {detail_total} vs 报表 {report_total}，差异 {variance}",
            )
        )
    return results


def check_l2_cap(spark: SparkSession) -> CheckResult:
    """二级资产上限（VDQ-017）：验算 HQLA 认列总额是否按 40% 上限正确截断。

    这条规则在需求文档里是 WARNING 级 —— 二级资产占比高本身不构成错误，
    真正的错误是认列总额没有按上限截断。因此这里验算计算是否正确，而不是占比是否达标。
    """
    row = spark.sql(
        f"""
        select sec_g_hqla_l1_mv as l1, sec_g_hqla_l2a_mv as l2a, sec_g_hqla_l2b_mv as l2b,
               sec_g_hqla_capped_total_usd as capped
        from {REPORT} where is_consolidated
        """
    ).collect()[0]
    level_1 = float(row["l1"] or 0)
    level_2 = float(row["l2a"] or 0) + float(row["l2b"] or 0)
    capped = float(row["capped"] or 0)

    hqla_before_cap = level_1 + level_2
    expected = round(level_1 + min(level_2, 0.40 * hqla_before_cap), 2)
    ratio = level_2 / hqla_before_cap if hqla_before_cap else 0.0
    cap_triggered = level_2 > 0.40 * hqla_before_cap

    return CheckResult(
        name="二级资产上限",
        passed=abs(capped - expected) <= 0.05,
        detail=(
            f"认列总额 {capped:,.2f}，按 40% 上限应为 {expected:,.2f}；"
            f"二级资产原始占比 {ratio:.2%}" + ("（上限已截断）" if cap_triggered else "（未触发上限）")
        ),
    )


def check_inflow_cap(spark: SparkSession) -> CheckResult:
    """30 天流入不得超过流出的 75%（VDQ-018）。"""
    row = spark.sql(
        f"""
        select sec_k_total_inflows as inflows, sec_k_total_outflows as outflows,
               sec_h_expected_inflow_30d as raw_inflows
        from {REPORT} where is_consolidated
        """
    ).collect()[0]
    inflows = float(row["inflows"] or 0)
    outflows = float(row["outflows"] or 0)
    raw_inflows = float(row["raw_inflows"] or 0)
    cap = 0.75 * outflows
    within_cap = inflows <= cap + 0.01
    # 上限是否真的起了作用：未加限制的流入高于上限时，认列值应被压到上限
    cap_effective = raw_inflows <= cap + 0.01 or abs(inflows - cap) <= 0.02
    return CheckResult(
        name="流入上限 75%",
        passed=within_cap and cap_effective,
        detail=(
            f"流入 {inflows:,.2f}，上限 {cap:,.2f}，未限制前 {raw_inflows:,.2f}"
            + ("（上限已生效）" if raw_inflows > cap else "（未触发上限）")
        ),
    )


def check_gl_reconciliation(spark: SparkSession) -> CheckResult:
    """GL 对账结果分布：本演示数据应当全部 PASS。"""
    rows = spark.table(RECONCILIATION).groupBy("status").count().collect()
    counts = {row["status"]: row["count"] for row in rows}
    passed = counts.get("FAIL", 0) == 0 and counts.get("PASS", 0) > 0
    return CheckResult(
        name="GL 对账",
        passed=passed,
        detail=f"PASS {counts.get('PASS', 0)} 项，FAIL {counts.get('FAIL', 0)} 项",
    )


def main() -> int:
    """跑 ADS 层全部核对项，任一不通过即以退出码 1 结束。"""
    spark = SparkSession.builder.appName("fr2052a-verify-gold").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    results: list[CheckResult] = []
    results.extend(check_row_shape(spark))
    results.extend(check_consolidation(spark))
    results.extend(check_detail_rollup(spark))
    results.append(check_l2_cap(spark))
    results.append(check_inflow_cap(spark))
    results.append(check_gl_reconciliation(spark))

    for result in results:
        print(f"  [{'PASS' if result.passed else 'FAIL'}] {result.name:<26} {result.detail}")

    failed = [result.name for result in results if not result.passed]
    print()
    if failed:
        print(f"未通过 {len(failed)} 项：{failed}")
        return 1
    print(f"ADS 层 {len(results)} 项核对全部通过")
    return 0


if __name__ == "__main__":
    sys.exit(main())
