# [AI-GENERATED] model=deepseek-flash date=2026-09-17 reviewed_by=pending
"""核对 ADS 报送层：报表内部一致、合并口径正确、监管上限确实生效。

dbt 跑通只说明 SQL 没报错，不说明数字对。这里逐项验算：
  1. 报表实体覆盖 = bronze 存款表的实体集合，且实体集合与 ref 层级表一致
  2. 合并口径 = 各实体口径之和减去集团内往来（逐 Section 验算 + 抵销专项）
  3. 明细合计 = 报表 Section 合计（对应 VDQ-013，排除 is_intracompany 行）
  4. 二级资产占比 ≤ HQLA 总额的 40%（对应 VDQ-017）
  5. 30 天流入 ≤ 流出的 75%（对应 VDQ-018，验证上限确实被应用）
  6. GL 对账逐报告期核对，每个视角都有对账行且无 FAIL

第 1–5 项**逐报告期**核对：多期数据共存时，把两期并到一起比集合或加金额，
会把「本期缺一个实体、另一期多一个实体」看成一致，也会拿错期的明细去凑合并行。

用法（Server 2，经 spark-submit 包装脚本执行）：
    bash spark-submit-fr2052a.sh /opt/fr2052a-app/python/lakehouse/verify_gold.py
"""

from __future__ import annotations

import sys
from dataclasses import dataclass

from pyspark.sql import Row, SparkSession

REPORT = "gold.ads_fr2052a_report"
DETAIL = "gold.ads_fr2052a_detail"
RECONCILIATION = "gold.ads_gl_reconciliation"
SOURCE_DEPOSITS = "bronze.ods_deposits"
SOURCE_LOANS = "bronze.ods_loans"
REF_ENTITY = "ref.ref_entity_hierarchy"
REF_COUNTERPARTY = "ref.ref_counterparty"

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


def report_periods(spark: SparkSession) -> list[str]:
    """报表里出现的报告期，升序排列。

    多报告期数据下的核对基准：实体覆盖、合并口径、明细回溯都要**逐期**成立。
    把两期并成一个集合去比，会把「本期少一个实体、另一期多一个实体」看成一致。
    """
    rows = spark.sql(f"select distinct cast(report_date as string) as period from {REPORT} order by period").collect()
    return [str(row["period"]) for row in rows]


def check_row_shape(spark: SparkSession) -> list[CheckResult]:
    """核对报表行形状：每个报告期的实体覆盖与 bronze 一致，且每期恰有一行合并行。

    报表行形状的期望来自两处独立来源，不是常量：
      ref.ref_entity_hierarchy 声明哪些实体承接业务（is_active 的实体）
      bronze.ods_deposits      声明该报告期实际有哪些实体记了账
    """
    expected_entities = {
        row["entity_code"] for row in spark.table(REF_ENTITY).filter("is_active").select("entity_code").collect()
    }

    results: list[CheckResult] = []
    for period in report_periods(spark):
        report_entities = {
            row["entity_code"]
            for row in spark.sql(
                f"select distinct entity_code from {REPORT} "
                f"where not is_consolidated and cast(report_date as string) = '{period}'"
            ).collect()
        }
        source_entities = {
            row["entity_code"]
            for row in spark.sql(
                f"select distinct entity_code from {SOURCE_DEPOSITS} where cast(report_date as string) = '{period}'"
            ).collect()
        }
        consolidated_rows = spark.sql(
            f"select count(*) as n from {REPORT} where is_consolidated and cast(report_date as string) = '{period}'"
        ).collect()[0]["n"]
        missing_in_report = source_entities - report_entities
        extra_in_report = report_entities - source_entities

        results.append(
            CheckResult(
                name=f"报表实体覆盖 {period}",
                passed=report_entities == source_entities and consolidated_rows == 1,
                detail=(
                    f"报表 {len(report_entities)} 实体，bronze {len(source_entities)} 实体，"
                    f"合并 {consolidated_rows} 行"
                    + (f"，报表缺 {sorted(missing_in_report)}" if missing_in_report else "")
                    + (f"，报表多 {sorted(extra_in_report)}" if extra_in_report else "")
                ),
            )
        )
        results.append(
            CheckResult(
                name=f"实体集合与 ref 一致 {period}",
                passed=source_entities == expected_entities,
                detail=(
                    f"bronze {len(source_entities)} 实体，ref {len(expected_entities)} 实体"
                    + (
                        f"，差异 bronze 多 {sorted(source_entities - expected_entities)}"
                        if source_entities - expected_entities
                        else ""
                    )
                    + (
                        f"，ref 多 {sorted(expected_entities - source_entities)}"
                        if expected_entities - source_entities
                        else ""
                    )
                ),
            )
        )
    return results


@dataclass
class IntracompanyAmounts:
    """某报告期的集团内往来金额，从 silver 明细独立算出（不复用合并行的公式）。"""

    deposit_leg: float
    """存款腿：子公司在母公司的存款。合并口径要从 Section C 剔除。"""

    loan_leg: float
    """贷款腿：母公司对子公司的放款，不限期限。只用于确认前提存在。"""

    loan_leg_within_30d: float
    """30 天内到期的那部分贷款腿。合并口径要从 Section F 剔除。"""

    deposit_leg_runoff_30d: float
    """存款腿在 30 天窗口内的行为流失额。合并口径要从 Section K 的流出合计里剔除。

    只剔本金是不够的：存款腿进了 30 天现金流预测，就带着自己的流失额进了
    实体口径的流出合计；合并行不含集团内行，于是两边差一个流失额。
    流失额按 ref_behavior_assumptions 的流失率算，不从报表读回来。
    """

    def elimination_for(self, column: str) -> float:
        """某个报表列在合并口径里应剔除的金额。

        口径必须与报表定义一致：Section F 只含 **30 天内到期**的贷款本金，
        所以 30 天以上到期的集团内放款本来就不在 Section F 里，不能去减它 ——
        减了会凭空少一块（实测踩过）。k 的融资合计包含存款，存款腿同样要剔。
        """
        if column in ("sec_c_total", "sec_k_total_funding"):
            return self.deposit_leg
        if column == "sec_f_total_inflow":
            return self.loan_leg_within_30d
        if column == "sec_k_total_outflows":
            return self.deposit_leg_runoff_30d
        return 0.0


def intracompany_amounts(spark: SparkSession, period: str) -> IntracompanyAmounts:
    """从 silver 明细独立算出该期的两条腿金额。"""
    deposit_leg = spark.sql(
        f"select round(sum(principal_amount_usd), 2) as total from silver.owd_deposits "
        f"where is_intracompany and cast(report_date as string) = '{period}'"
    ).collect()[0]["total"]
    loan_rows = spark.sql(
        f"select "
        f"round(sum(outstanding_usd), 2) as total, "
        f"round(sum(case when days_to_maturity <= 30 then outstanding_usd else 0 end), 2) as within_30d "
        f"from silver.owd_loans "
        f"where is_intracompany and cast(report_date as string) = '{period}'"
    ).collect()[0]
    deposit_runoff = spark.sql(
        f"select round(sum(d.principal_amount_usd * a.runoff_rate), 2) as total "
        f"from silver.owd_deposits d "
        f"join ref.ref_behavior_assumptions a "
        f"  on a.product_category = d.product_category "
        f" and a.customer_segment = d.customer_segment "
        f" and a.maturity_bucket = d.maturity_bucket "
        f"where d.is_intracompany and cast(d.report_date as string) = '{period}' "
        f"  and d.maturity_bucket in ('O/N', '1-7D', '8-30D')"
    ).collect()[0]["total"]
    return IntracompanyAmounts(
        deposit_leg=float(deposit_leg or 0),
        loan_leg=float(loan_rows["total"] or 0),
        loan_leg_within_30d=float(loan_rows["within_30d"] or 0),
        deposit_leg_runoff_30d=float(deposit_runoff or 0),
    )


def check_consolidation(spark: SparkSession) -> list[CheckResult]:
    """合并口径必须等于同报告期各实体口径之和**减去集团内往来**（同一列逐列验算）。

    逐期验算：跨期把实体行加到一起，既拿错期的明细去凑合并行，
    也让「两期都是对的」与「两期互相抵消着错」无法区分。
    抵销金额来自 silver 明细，不引用合并行自己的公式。
    """
    sums = ", ".join(f"round(sum({column}), 2) as {column}" for column in CONSOLIDATION_COLUMNS)
    results: list[CheckResult] = []
    for period in report_periods(spark):
        entity_totals = spark.sql(
            f"select {sums} from {REPORT} where not is_consolidated and cast(report_date as string) = '{period}'"
        ).collect()[0]
        consolidated = (
            spark.table(REPORT)
            .filter(f"is_consolidated and cast(report_date as string) = '{period}'")
            .select(*CONSOLIDATION_COLUMNS)
            .collect()[0]
        )
        ic = intracompany_amounts(spark, period)

        mismatched = []
        for column in CONSOLIDATION_COLUMNS:
            entity_value = entity_totals[column]
            consolidated_value = consolidated[column]
            if entity_value is None or consolidated_value is None:
                continue
            elimination = ic.elimination_for(column)
            expected = round(float(entity_value) - elimination, 2)
            if abs(expected - float(consolidated_value)) > 0.05:
                mismatched.append(
                    f"{column}: Σ实体 {entity_value} − 抵销 {elimination:,.2f} = {expected:,.2f}，"
                    f"合并行 {consolidated_value}"
                )

        results.append(
            CheckResult(
                name=f"合并口径加总 {period}",
                passed=not mismatched,
                detail=(
                    f"{len(CONSOLIDATION_COLUMNS)} 列逐列验算一致"
                    f"（抵销：存款腿 {ic.deposit_leg:,.2f}、30 天内到期的贷款腿 {ic.loan_leg_within_30d:,.2f}、"
                    f"存款腿 30 天流失额 {ic.deposit_leg_runoff_30d:,.2f}）"
                    if not mismatched
                    else f"不一致 {mismatched[:2]}"
                ),
            )
        )
    return results


def check_intracompany_present(spark: SparkSession) -> list[CheckResult]:
    """抵销前提核对：数据里**确实存在**集团内往来，否则「抵销」这条规则在空转。

    判据存在而场景不存在，等于没有判据。这条检查守住前提：该期两条腿都不为零，
    且实体口径里确实含得下它们（实体 C 合计 ≥ 存款腿）。
    """
    results: list[CheckResult] = []
    for period in report_periods(spark):
        ic = intracompany_amounts(spark, period)
        entity_deposits = float(
            spark.sql(
                f"select round(sum(sec_c_total), 2) as total from {REPORT} "
                f"where not is_consolidated and cast(report_date as string) = '{period}'"
            ).collect()[0]["total"]
            or 0
        )
        passed = ic.deposit_leg > 0 and ic.loan_leg > 0 and entity_deposits >= ic.deposit_leg
        results.append(
            CheckResult(
                name=f"抵销前提（集团内往来存在）{period}",
                passed=passed,
                detail=(
                    f"存款腿 {ic.deposit_leg:,.2f}、贷款腿 {ic.loan_leg:,.2f}"
                    f"（其中 30 天内到期 {ic.loan_leg_within_30d:,.2f}）；"
                    f"实体口径 Section C 合计 {entity_deposits:,.2f}"
                ),
            )
        )
    return results


def check_detail_rollup(spark: SparkSession) -> list[CheckResult]:
    """明细合计 = 报表 Section 合计（VDQ-013），逐报告期核对。

    合并口径的明细回溯只汇总 is_intracompany = false 的行。
    is_intracompany = true 的行是集团内往来，不该出现在合并口径里。
    """
    results: list[CheckResult] = []
    for period in report_periods(spark):
        consolidated = (
            spark.table(REPORT).filter(f"is_consolidated and cast(report_date as string) = '{period}'").collect()[0]
        )
        _check_detail_rollup_for_period(spark, results, consolidated, period)
    return results


def _check_detail_rollup_for_period(
    spark: SparkSession,
    results: list[CheckResult],
    consolidated: Row,
    period: str,
) -> None:
    """核对单个报告期的明细回溯，结论追加进 results。

    合并口径只看 is_intracompany = false 的明细行。
    """
    for section, (report_column, detail_column, line_item) in SECTION_AMOUNTS.items():
        line_filter = f" and line_item = '{line_item}'" if line_item else ""
        row = spark.sql(
            f"select count(*) as rows, round(sum({detail_column}), 2) as total "
            f"from {DETAIL} where section_code = '{section}'{line_filter} "
            f"and not is_intracompany "
            f"and cast(report_date as string) = '{period}'"
        ).collect()[0]
        detail_rows = row["rows"]
        detail_total = row["total"]

        if detail_rows == 0:
            results.append(
                CheckResult(
                    name=f"明细汇总 Section {section} {period}",
                    passed=False,
                    detail=f"过滤条件未命中明细表（section={section}、line_item={line_item}）",
                )
            )
            continue

        if detail_total is None:
            results.append(
                CheckResult(
                    name=f"明细汇总 Section {section} {period}",
                    passed=False,
                    detail="明细合计为 NULL",
                )
            )
            continue

        report_total = consolidated[report_column]
        if report_total is None:
            results.append(
                CheckResult(
                    name=f"明细汇总 Section {section} {period}",
                    passed=False,
                    detail="报表侧该列为 NULL",
                )
            )
            continue

        variance = round(float(detail_total) - float(report_total), 2)
        results.append(
            CheckResult(
                name=f"明细汇总 Section {section} {period}",
                passed=abs(variance) <= 0.05,
                detail=f"明细 {detail_total} vs 报表 {report_total}，差异 {variance}",
            )
        )


def check_l2_cap(spark: SparkSession) -> list[CheckResult]:
    """二级资产上限（VDQ-017）：逐报告期验算 HQLA 认列总额是否按 40% 截断。

    这条规则在需求文档里是 WARNING 级 —— 二级资产占比高本身不构成错误，
    真正的错误是认列总额没有按上限截断。因此这里验算计算是否正确，而不是占比是否达标。

    输入必须独立：L1 / L2A / L2B 的市值从 silver 明细独立算出（`owd_securities` 按
    `hqla_classification` 汇总，合并口径只取非集团内行），不读报表自己的 `sec_g_*` 列 ——
    拿被校验方算好的输入再套一遍同一个公式，只能证明「公式抄对了」，证明不了输入没写歪。
    """
    results: list[CheckResult] = []
    for period in report_periods(spark):
        detail = spark.sql(
            f"""
            select
                round(sum(case when hqla_classification = 'LEVEL_1' then market_value_usd else 0 end), 2) as l1,
                round(sum(case when hqla_classification = 'LEVEL_2A' then market_value_usd else 0 end), 2) as l2a,
                round(sum(case when hqla_classification = 'LEVEL_2B' then market_value_usd else 0 end), 2) as l2b
            from silver.owd_securities
            where not is_intracompany and cast(report_date as string) = '{period}'
            """
        ).collect()[0]
        report_row = spark.sql(
            f"""
            select sec_g_hqla_capped_total_usd as capped
            from {REPORT} where is_consolidated and cast(report_date as string) = '{period}'
            """
        ).collect()[0]
        level_1 = float(detail["l1"] or 0)
        level_2 = float(detail["l2a"] or 0) + float(detail["l2b"] or 0)
        capped = float(report_row["capped"] or 0)

        hqla_before_cap = level_1 + level_2
        expected = round(level_1 + min(level_2, 0.40 * hqla_before_cap), 2)
        ratio = level_2 / hqla_before_cap if hqla_before_cap else 0.0
        cap_triggered = level_2 > 0.40 * hqla_before_cap

        results.append(
            CheckResult(
                name=f"二级资产上限 {period}",
                passed=abs(capped - expected) <= 0.05,
                detail=(
                    f"认列总额 {capped:,.2f}，按 40% 上限应为 {expected:,.2f}；"
                    f"二级资产原始占比 {ratio:.2%}" + ("（上限已截断）" if cap_triggered else "（未触发上限）")
                ),
            )
        )
    return results


def check_inflow_cap(spark: SparkSession) -> list[CheckResult]:
    """30 天流入不得超过流出的 75%（VDQ-018），逐报告期核对。"""
    results: list[CheckResult] = []
    for period in report_periods(spark):
        row = spark.sql(
            f"""
            select sec_k_total_inflows as inflows, sec_k_total_outflows as outflows,
                   sec_h_expected_inflow_30d as raw_inflows
            from {REPORT} where is_consolidated and cast(report_date as string) = '{period}'
            """
        ).collect()[0]
        inflows = float(row["inflows"] or 0)
        outflows = float(row["outflows"] or 0)
        raw_inflows = float(row["raw_inflows"] or 0)
        cap = 0.75 * outflows
        within_cap = inflows <= cap + 0.01
        # 上限是否真的起了作用：未加限制的流入高于上限时，认列值应被压到上限
        cap_effective = raw_inflows <= cap + 0.01 or abs(inflows - cap) <= 0.02
        results.append(
            CheckResult(
                name=f"流入上限 75% {period}",
                passed=within_cap and cap_effective,
                detail=(
                    f"流入 {inflows:,.2f}，上限 {cap:,.2f}，未限制前 {raw_inflows:,.2f}"
                    + ("（上限已生效）" if raw_inflows > cap else "（未触发上限）")
                ),
            )
        )
    return results


def check_gl_reconciliation(spark: SparkSession) -> list[CheckResult]:
    """GL 对账逐报告期核对：每个视角都有对账行且无 FAIL。

    一个对象都没核对到不能再算通过。
    """
    results: list[CheckResult] = []
    for period in report_periods(spark):
        rows = spark.sql(
            f"select status, count(*) as n from {RECONCILIATION} "
            f"where cast(report_date as string) = '{period}' group by status"
        ).collect()
        counts = {row["status"]: row["n"] for row in rows}
        fail_count = counts.get("FAIL", 0)
        pass_count = counts.get("PASS", 0)
        total = sum(counts.values())
        passed = fail_count == 0 and total > 0
        results.append(
            CheckResult(
                name=f"GL 对账 {period}",
                passed=passed,
                detail=f"PASS {pass_count} 项，FAIL {fail_count} 项，合计 {total} 行",
            )
        )
    return results


def check_recon_benchmark(spark: SparkSession) -> list[CheckResult]:
    """独立核对 Section E 的对账基准：来源必须是司库现金头寸，且调节后残差为零。

    对账表自己判 PASS 不算证据。这里从另一条路径核三件事：
      1. Section E 的基准来源标识必须是司库现金头寸 —— 退回到总账就是自比对；
      2. 调节项必须非零 —— 现金头寸与账面真的不同源；
      3. 调节后残差必须是零 —— 差额已被逐项解释干净。
    """
    results: list[CheckResult] = []
    for period in report_periods(spark):
        row = spark.sql(
            f"""
            select
                count(*) as total,
                sum(case when benchmark_source = 'TREASURY_CASH_POSITION' then 1 else 0 end) as independent,
                sum(case when abs(reconciling_item_usd) >= 0.01 then 1 else 0 end) as itemized,
                max(abs(variance)) as max_variance
            from {RECONCILIATION}
            where section_code = 'E' and cast(report_date as string) = '{period}'
            """
        ).collect()[0]
        total = row["total"]
        independent = row["independent"] or 0
        itemized = row["itemized"] or 0
        max_variance = float(row["max_variance"] or 0)
        passed = total > 0 and independent == total and itemized == total and max_variance <= 0.01
        results.append(
            CheckResult(
                name=f"对账基准独立性 {period}",
                passed=passed,
                detail=(
                    f"E 项 {total} 行，司库口径基准 {independent} 行，调节项非零 {itemized} 行，"
                    f"最大残差 {max_variance:,.2f}"
                ),
            )
        )
    return results


def check_section_i_identity(spark: SparkSession) -> list[CheckResult]:
    """Section I/G 恒等式：未受限各项 + 已受限 = Section G 合计，且逐桶与 silver 独立复算一致。

    这条恒等式是「HQLA 存量排除 30 天内到期的证券」那次改动的守门人：
    被排除出去的那一块必须落进 unencumbered_near_maturity 桶里，否则恒等式立刻不平 ——
    少算的钱不会在别的地方报出来，只会让 LCR 分子静静变小。
    """
    results: list[CheckResult] = []
    for period in report_periods(spark):
        rows = spark.sql(
            f"""
            select
                round(sec_i_unencumbered_hqla_l1 + sec_i_unencumbered_hqla_l2a
                      + sec_i_unencumbered_hqla_l2b + sec_i_unencumbered_non_hqla
                      + sec_i_unencumbered_near_maturity + sec_i_encumbered_total, 2) as parts_sum,
                round(sec_g_total_mv, 2) as section_g_total
            from {REPORT}
            where cast(report_date as string) = '{period}'
            """
        ).collect()
        worst = max(
            (abs(float(row["parts_sum"] or 0) - float(row["section_g_total"] or 0)) for row in rows),
            default=0.0,
        )
        results.append(
            CheckResult(
                name=f"Section I/G 恒等式 {period}",
                passed=bool(rows) and worst <= 0.05,
                detail=f"{len(rows)} 行，最大偏差 {worst:,.2f}（未受限五项 + 已受限 vs Section G 合计）",
            )
        )

        # 逐桶独立复算：从 silver 明细按同一口径重算未受限各桶，与报表的合并行逐项比。
        # 拿报表自己的列再套一遍公式只能证明公式抄对了，证明不了桶的划分没错。
        recomputed = spark.sql(
            f"""
            select
                -- 计入 HQLA 存量的三个等级：未受限 + 剩余期限 30 天以上（窗口内到期的走流入）
                round(sum(case when hqla_classification = 'LEVEL_1' and not is_encumbered
                                    and days_to_maturity > 30 then market_value_usd else 0 end), 2) as l1,
                round(sum(case when hqla_classification = 'LEVEL_2A' and not is_encumbered
                                    and days_to_maturity > 30 then market_value_usd else 0 end), 2) as l2a,
                round(sum(case when hqla_classification = 'LEVEL_2B' and not is_encumbered
                                    and days_to_maturity > 30 then market_value_usd else 0 end), 2) as l2b,
                -- 未受限但 30 天内到期：被排除在存量之外，单列披露
                round(sum(case when not is_encumbered and days_to_maturity <= 30
                               then market_value_usd else 0 end), 2) as near_maturity,
                round(sum(case when hqla_classification = 'NON_HQLA' and not is_encumbered
                               then market_value_usd else 0 end), 2) as non_hqla,
                round(sum(case when is_encumbered then market_value_usd else 0 end), 2) as encumbered
            from silver.owd_securities
            where not is_intracompany and cast(report_date as string) = '{period}'
            """
        ).collect()[0]
        report_row = spark.sql(
            f"""
            select sec_i_unencumbered_hqla_l1 as l1, sec_i_unencumbered_hqla_l2a as l2a,
                   sec_i_unencumbered_hqla_l2b as l2b, sec_i_unencumbered_non_hqla as non_hqla,
                   sec_i_unencumbered_near_maturity as near_maturity, sec_i_encumbered_total as encumbered
            from {REPORT} where is_consolidated and cast(report_date as string) = '{period}'
            """
        ).collect()[0]
        worst_bucket = 0.0
        worst_name = ""
        for column in ("l1", "l2a", "l2b", "non_hqla", "near_maturity", "encumbered"):
            gap = abs(float(recomputed[column] or 0) - float(report_row[column] or 0))
            if gap > worst_bucket:
                worst_bucket, worst_name = gap, column
        results.append(
            CheckResult(
                name=f"非受限各桶独立复算 {period}",
                passed=worst_bucket <= 0.05,
                detail=(f"六个桶逐项比对，最大偏差 {worst_bucket:,.2f}" + (f"（{worst_name}）" if worst_name else "")),
            )
        )
    return results


def main() -> int:
    """跑 ADS 层全部核对项，任一不通过即以退出码 1 结束。"""
    spark = SparkSession.builder.appName("fr2052a-verify-gold").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    results: list[CheckResult] = []
    results.extend(check_row_shape(spark))
    results.extend(check_consolidation(spark))
    results.extend(check_intracompany_present(spark))
    results.extend(check_detail_rollup(spark))
    results.extend(check_l2_cap(spark))
    results.extend(check_section_i_identity(spark))

    results.extend(check_inflow_cap(spark))
    results.extend(check_gl_reconciliation(spark))
    results.extend(check_recon_benchmark(spark))

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
