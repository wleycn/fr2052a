"""REF 层（引用数据）生成。

REF 是本批数据的字典：ODS 中出现的法人实体、交易对手、币种、日期都必须取自这里，
生成结束后的完整性自检会逐条核对，杜绝悬空引用。

汇率契约：ref_exchange_rates 是全量币种来源，ref.currencies 由它派生。
ODS 各表的币种只能取汇率表里有的；两者不一致即数据缺陷，
由 generate_sample_data.py 自检与 dbt/tests/assert_fx_covered.sql 共同守住。

[AI-GENERATED] model=qianfan-code-latest date=2026-09-18 reviewed_by=pending
"""

from __future__ import annotations

import random
import string
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path

from .config import (
    CALENDAR_END,
    CALENDAR_START,
    REPORT_DATE,
    TRADING_ENTITIES,
    table_rng,
    write_csv,
)

USD_RATE_SOURCE = "BLOOMBERG"

# 交易日历中的休市日（美国口径，演示用）
HOLIDAYS: dict[date, str] = {
    date(2026, 9, 7): "Labor Day",
    date(2026, 11, 26): "Thanksgiving",
    date(2026, 12, 25): "Christmas",
}

# 汇率（1 单位外币 = 多少 USD）
FX_RATES: dict[str, float] = {
    "EUR": 1.0800,
    "GBP": 1.2600,
    "JPY": 0.0067,
    "CHF": 1.1200,
    "CNY": 0.1400,
    "HKD": 0.1280,
    "AUD": 0.6600,
    "CAD": 0.7400,
    "SGD": 0.7500,
}

# 法人实体：层级、注册地、是否重要实体、本位币（用于交易币种加权）
ENTITY_ROWS: list[tuple[str, str, str, str, int, str, str, bool, str, bool, str, str]] = [
    (
        "ENT001",
        "Global Bank Holding Inc.",
        "LEI0000000000000001",
        "",
        1,
        "US",
        "HOLDING",
        True,
        "FULL",
        True,
        "2020-01-01",
        "",
    ),
    (
        "ENT002",
        "Global Bank NA",
        "LEI0000000000000002",
        "ENT001",
        2,
        "US",
        "BANK",
        True,
        "FULL",
        True,
        "2020-01-01",
        "",
    ),
    (
        "ENT003",
        "Global Broker Dealer LLC",
        "LEI0000000000000003",
        "ENT001",
        2,
        "US",
        "BROKER",
        False,
        "FULL",
        True,
        "2021-01-01",
        "",
    ),
    (
        "ENT004",
        "Global Bank London Branch",
        "LEI0000000000000004",
        "ENT002",
        3,
        "GB",
        "BANK",
        True,
        "FULL",
        True,
        "2021-06-01",
        "",
    ),
    (
        "ENT005",
        "Global Bank Tokyo Branch",
        "LEI0000000000000005",
        "ENT002",
        3,
        "JP",
        "BANK",
        False,
        "FULL",
        True,
        "2022-01-01",
        "",
    ),
]

# 承接业务的实体（母公司不直接记账）——此常量已移至 config.py，此处从 config 导入。
# 承接业务记账实体与全部记账实体（含母公司）的定义都在 config.py，保持单一来源。

MATURITY_BUCKET_ROWS = [
    ("O/N", "Overnight", 0, 0, 1, 1),
    ("1-7D", "1 to 7 days", 1, 7, 2, 2),
    ("8-30D", "8 to 30 days", 8, 30, 3, 3),
    ("31-90D", "31 to 90 days", 31, 90, 4, 4),
    ("91-180D", "91 to 180 days", 91, 180, 5, 5),
    ("181D-1Y", "181 days to 1yr", 181, 365, 6, 6),
    (">1Y", "Over 1 year", 366, 99999, 7, 7),
    ("OPEN", "Open-ended", "", "", 8, 8),
]

LINE_ITEM_ROWS = [
    ("A-01", "A", "Commercial Paper Outstanding", "", False, "", "AMOUNT", "POSITIVE", True, 1),
    ("A-02", "A", "Large Time Deposits", "", False, "", "AMOUNT", "POSITIVE", True, 2),
    ("B-01", "B", "Repo Outstanding", "", False, "", "AMOUNT", "POSITIVE", True, 3),
    ("B-02", "B", "Reverse Repo", "", False, "", "AMOUNT", "POSITIVE", True, 4),
    ("C-01", "C", "Retail Demand Deposits", "", False, "", "AMOUNT", "POSITIVE", True, 5),
    ("C-02", "C", "Retail Savings Deposits", "", False, "", "AMOUNT", "POSITIVE", True, 6),
    ("C-03", "C", "Wholesale Demand Deposits", "", False, "", "AMOUNT", "POSITIVE", True, 7),
    ("C-04", "C", "Brokered Deposits", "", False, "", "AMOUNT", "POSITIVE", True, 8),
    ("D-01", "D", "Other Funding", "", False, "", "AMOUNT", "POSITIVE", True, 9),
    ("E-01", "E", "Cash and Due from Banks", "", False, "", "AMOUNT", "POSITIVE", True, 10),
    ("F-01", "F", "Commercial Loan Inflows", "", False, "", "AMOUNT", "POSITIVE", True, 11),
    ("G-01", "G", "HQLA Level 1 Market Value", "", False, "", "AMOUNT", "POSITIVE", True, 12),
    ("G-02", "G", "HQLA Level 2A Market Value", "", False, "", "AMOUNT", "POSITIVE", True, 13),
    ("G-03", "G", "HQLA Level 2B Market Value", "", False, "", "AMOUNT", "POSITIVE", True, 14),
    ("H-01", "H", "Net MTM Asset", "", False, "", "AMOUNT", "EITHER", True, 15),
    ("I-01", "I", "Unencumbered HQLA L1", "", False, "", "AMOUNT", "POSITIVE", True, 16),
    ("J-01", "J", "Credit Commitments", "", False, "", "AMOUNT", "POSITIVE", True, 17),
    ("K-01", "K", "Total Funding", "", True, "A+B+C+D", "AMOUNT", "POSITIVE", True, 18),
    ("K-02", "K", "Net Funding Gap", "", True, "Inflow - Outflow", "AMOUNT", "EITHER", True, 19),
]

REGULATORY_MAPPING_ROWS = [
    (
        "G",
        "G-01",
        "hqla_level_1_mv_usd",
        "FR 2052a Instructions Page 42",
        "FR2052A-G-01",
        0.00,
        "Treasury Liquidity Team",
        "2024-01-01",
        "",
    ),
    (
        "G",
        "G-02",
        "hqla_level_2a_mv_usd",
        "FR 2052a Instructions Page 43",
        "FR2052A-G-02",
        0.15,
        "Treasury Liquidity Team",
        "2024-01-01",
        "",
    ),
    (
        "G",
        "G-03",
        "hqla_level_2b_mv_usd",
        "FR 2052a Instructions Page 44",
        "FR2052A-G-03",
        0.50,
        "Treasury Liquidity Team",
        "2024-01-01",
        "",
    ),
    (
        "C",
        "C-01",
        "retail_demand_usd",
        "FR 2052a Instructions Page 20",
        "FR2052A-C-01",
        0.00,
        "Treasury Liquidity Team",
        "2024-01-01",
        "",
    ),
    (
        "F",
        "F-01",
        "commercial_inflow_usd",
        "FR 2052a Instructions Page 30",
        "FR2052A-F-01",
        0.00,
        "Treasury Liquidity Team",
        "2024-01-01",
        "",
    ),
]

# 行为假设派生规则：由基础流失率 × 客户分段调整 × 到期分桶调整组合出全覆盖矩阵。
# 维度取值从 ODS 生成器的实际维度派生（不凭空造维度）：
#   产品类别 deposit_product_category 宏的取值里 ODS 实际产生的（CHK→DEMAND, SAV/MMDA→SAVINGS, CD, TIME）
#   客户分段 customer_segment 宏的取值（IND→RETAIL, CORP→CORPORATE, GOV→SOVEREIGN, FI→FINANCIAL, AFFIL→AFFILIATE）
#   到期分桶 behavioral_bucket 宏：活期/储蓄恒归 O/N；定期类按到期日归桶
# 派生关系：活期低于定期（活期随时可取但行为上不会全走，定期到期不续约概率更高）、零售低于对公
# 详见 docs/business/DATA-DESIGN.md §3.5 行为假设派生规则
_BEHAVIOR_PRODUCT_CATEGORIES = ("DEMAND", "SAVINGS", "CD", "TIME")
_BEHAVIOR_CUSTOMER_SEGMENTS = ("RETAIL", "CORPORATE", "SOVEREIGN", "FINANCIAL", "AFFILIATE")
# 活期/储蓄按行为口径恒归 O/N；定期类按到期日可落到以下桶
_TERM_BUCKETS = ("O/N", "1-7D", "8-30D", "31-90D", "91-180D", "181D-1Y", ">1Y")
_DEMAND_BUCKETS = ("O/N",)

# 基础流失率
_BASE_RUNOFF: dict[str, float] = {
    "DEMAND": 0.05,
    "SAVINGS": 0.08,
    "CD": 0.30,
    "TIME": 0.25,
}
# 客户分段调整系数（零售低于对公）
_SEGMENT_FACTOR: dict[str, float] = {
    "RETAIL": 0.5,
    "CORPORATE": 1.0,
    "SOVEREIGN": 0.8,
    "FINANCIAL": 1.5,
    "AFFILIATE": 0.3,
}
# 到期分桶调整系数（短桶流失率高）
_BUCKET_FACTOR: dict[str, float] = {
    "O/N": 1.2,
    "1-7D": 1.1,
    "8-30D": 1.0,
    "31-90D": 0.8,
    "91-180D": 0.6,
    "181D-1Y": 0.4,
    ">1Y": 0.2,
}


def _derive_behavior_rows() -> list[tuple[str, str, str, float, float, str, str]]:
    """派生全覆盖行为假设矩阵。

    组合 = product_category × customer_segment × maturity_bucket。
    流失率 = 基础流失率 × 客户分段调整 × 到期分桶调整，clamp [0, 1]。
    活期/储蓄只有 O/N 桶（行为口径），定期类覆盖全部到期桶。
    """
    rows: list[tuple[str, str, str, float, float, str, str]] = []
    for cat in _BEHAVIOR_PRODUCT_CATEGORIES:
        buckets = _DEMAND_BUCKETS if cat in ("DEMAND", "SAVINGS") else _TERM_BUCKETS
        for seg in _BEHAVIOR_CUSTOMER_SEGMENTS:
            for bucket in buckets:
                rate = _BASE_RUNOFF[cat] * _SEGMENT_FACTOR[seg] * _BUCKET_FACTOR[bucket]
                rate = min(max(rate, 0.0), 1.0)
                rows.append((cat, seg, bucket, round(rate, 4), 0.0, "2024-01-01", ""))
    return rows


BEHAVIOR_ASSUMPTION_ROWS = _derive_behavior_rows()

# 校验规则：与 [02] §2.7 的 20 条 VDQ 逐条对应。
# 表达式里的列名必须与 silver/gold 层的实际列名一致 —— 规则与实现两张皮的话，
# 数据质量引擎会以"列不存在"跳过，等于没校验。
VALIDATION_RULE_ROWS = [
    (
        "VDQ-001",
        "Source file complete arrival",
        "COMPLETENESS",
        "ODS",
        "ERROR",
        "row_count > 0",
        "源文件整批到达，无空文件",
        True,
    ),
    (
        "VDQ-002",
        "Key fields not null",
        "COMPLETENESS",
        "ODS",
        "ERROR",
        "source_record_id IS NOT NULL AND currency IS NOT NULL",
        "主键与币种不得为空",
        True,
    ),
    (
        "VDQ-003",
        "Amount non-negative",
        "ACCURACY",
        "OWD",
        "ERROR",
        "principal_amount_usd >= 0",
        "金额不得为负",
        True,
    ),
    (
        "VDQ-004",
        "Interest rate in range",
        "ACCURACY",
        "OWD",
        "WARNING",
        "interest_rate BETWEEN -0.10 AND 1.0",
        "利率落在合理区间",
        True,
    ),
    (
        "VDQ-005",
        "Currency is ISO 4217",
        "ACCURACY",
        "OWD",
        "ERROR",
        "LENGTH(currency_code) = 3",
        "币种须为三位 ISO 4217 代码",
        True,
    ),
    (
        "VDQ-006",
        "Outstanding <= facility",
        "CONSISTENCY",
        "OWD",
        "ERROR",
        "outstanding_usd <= facility_amount_usd",
        "已用额度不得超过授信额度",
        True,
    ),
    (
        "VDQ-007",
        "Repo collateral value sane",
        "CONSISTENCY",
        "OWD",
        "WARNING",
        "collateral_mv_usd BETWEEN cash_amount_usd AND cash_amount_usd * 1.5",
        "回购抵押品市值相对现金金额合理",
        True,
    ),
    (
        "VDQ-008",
        "Maturity not before report date",
        "CONSISTENCY",
        "OWD",
        "ERROR",
        "maturity_date >= report_date",
        "到期日不得早于报告日",
        True,
    ),
    (
        "VDQ-009",
        "FX conversion error under 1%",
        "ACCURACY",
        "OWD",
        "WARNING",
        "ABS(amount_usd - amount_lc * spot_rate) / NULLIF(amount_usd, 0) < 0.01",
        "折算金额与本地金额×汇率误差小于 1%（需跨表 join，由核对脚本执行）",
        True,
    ),
    (
        "VDQ-010",
        "Summary equals detail sum",
        "CONSISTENCY",
        "OWS",
        "ERROR",
        "ABS(summary_amount - detail_sum) < 0.01",
        "汇总数必须等于明细求和（跨表比对）",
        True,
    ),
    (
        "VDQ-011",
        "Unencumbered <= total",
        "CONSISTENCY",
        "OWS",
        "ERROR",
        "unencumbered_amount <= total_amount",
        "非受限资产不得超过总资产（跨表比对）",
        True,
    ),
    (
        "VDQ-012",
        "Pledged <= market value",
        "CONSISTENCY",
        "OWS",
        "ERROR",
        "pledged_amount <= market_value",
        "已质押金额不得超过总市值（跨表比对）",
        True,
    ),
    (
        "VDQ-013",
        "Section total equals line items",
        "CONSISTENCY",
        "ADS",
        "ERROR",
        "ABS(section_total - line_items_total) < 0.01",
        "Section 合计等于其行项目合计（跨表比对）",
        True,
    ),
    (
        "VDQ-014",
        "Total funding vs balance sheet",
        "BUSINESS",
        "ADS",
        "WARNING",
        "ABS(total_funding - balance_sheet_total) / NULLIF(balance_sheet_total, 0) < 0.05",
        "总融资与资产负债表口径偏差小于 5%（跨表比对）",
        True,
    ),
    (
        "VDQ-015",
        "Period-over-period move",
        "BUSINESS",
        "ADS",
        "WARNING",
        "ABS(current_amount - prior_amount) / NULLIF(prior_amount, 0) < 0.20",
        "环比波动小于 20%（需上一期数据）",
        True,
    ),
    (
        "VDQ-016",
        "Loaded before T+1 08:00 ET",
        "TIMELINESS",
        "ODS",
        "ERROR",
        "etl_load_timestamp <= report_date + INTERVAL '1' DAY + INTERVAL '8' HOUR",
        "T+1 早八点前完成加载",
        True,
    ),
    (
        "VDQ-017",
        "L2A+2B within 40% of HQLA",
        "BUSINESS",
        "ADS",
        "WARNING",
        "(sec_g_hqla_l2a_mv + sec_g_hqla_l2b_mv) <= 0.40 * (sec_g_hqla_l1_mv + sec_g_hqla_l2a_mv + sec_g_hqla_l2b_mv)",
        "二级资产不得超过 HQLA 总额 40%（按认列额截断，见报表模型）",
        True,
    ),
    (
        "VDQ-018",
        "Inflow capped at 75% of outflow",
        "BUSINESS",
        "ADS",
        "ERROR",
        "sec_k_total_inflows <= 0.75 * sec_k_total_outflows",
        "现金流入上限为流出的 75%",
        True,
    ),
    (
        "VDQ-019",
        "Mandatory line items present",
        "COMPLETENESS",
        "ADS",
        "ERROR",
        "sec_k_total_funding IS NOT NULL AND sec_c_total IS NOT NULL",
        "必填行项目不得缺失",
        True,
    ),
    (
        "VDQ-020",
        "LEI format",
        "ACCURACY",
        "OWD",
        "WARNING",
        "lei_code RLIKE '^[A-Z0-9]{20}$'",
        "LEI 须为 20 位大写字母数字",
        True,
    ),
    (
        "VDQ-021",
        "Pledged flag value domain",
        "COMPLETENESS",
        "ODS",
        "WARNING",
        "coalesce(trim(pledged_flag), '') IN ('Y', 'N')",
        "质押标记须为 Y 或 N；空值或非法值按未受限处理，但源系统缺报状态要报出来",
        True,
    ),
]


@dataclass
class ReferenceData:
    """本批 REF 的外键集合，供 ODS 生成与完整性自检复用。

    currencies 与 spot_rates 由 _generate_exchange_rates 派生，
    是 ODS 币种的唯一合法来源：ODS 只能取这里有的币种。
    """

    entity_codes: list[str] = field(default_factory=list)
    trading_entities: list[str] = field(default_factory=list)
    counterparties: list[str] = field(default_factory=list)
    counterparty_types: dict[str, str] = field(default_factory=dict)
    # 集团内对手方映射：法人实体码 → 对手方编号（CP9001 ↔ ENT001，依此类推）
    affiliate_by_entity: dict[str, str] = field(default_factory=dict)
    # 集团内对手方编号清单（counterparties 只放外部对手方，见 _generate_counterparty 的说明）
    affiliate_counterparties: list[str] = field(default_factory=list)
    currencies: list[str] = field(default_factory=list)
    spot_rates: dict[str, float] = field(default_factory=dict)
    business_dates: list[str] = field(default_factory=list)
    row_counts: dict[str, int] = field(default_factory=dict)


def _random_lei(rng: random.Random) -> str:
    return "".join(rng.choices(string.ascii_uppercase + string.digits, k=20))


def _generate_entity_hierarchy(ref_dir: Path, ref: ReferenceData) -> None:
    header = [
        "entity_code",
        "entity_name",
        "lei_code",
        "parent_entity_code",
        "entity_level",
        "jurisdiction",
        "entity_type",
        "is_material_entity",
        "consolidation_method",
        "is_active",
        "effective_date",
        "expiry_date",
    ]
    rows = [list(row) for row in ENTITY_ROWS]
    ref.row_counts["ref_entity_hierarchy"] = write_csv(ref_dir / "ref_entity_hierarchy.csv", header, rows)
    ref.entity_codes = [str(row[0]) for row in ENTITY_ROWS]
    ref.trading_entities = list(TRADING_ENTITIES)


def _generate_counterparty(ref_dir: Path, ref: ReferenceData) -> None:
    rng = table_rng("ref_counterparty")
    types = ["BANK", "BROKER", "CORPORATE", "SOVEREIGN", "CENTRAL_BANK"]
    countries = ["US", "GB", "DE", "JP", "CH", "CN", "HK", "AU", "CA", "SG"]
    ratings = ["AAA", "AA", "A", "BBB", "BB", "B"]
    industries = ["6200", "6010", "6020", "6030", "6040"]

    rows = []
    for index in range(1, 51):
        counterparty_id = f"CP{index:04d}"
        counterparty_type = rng.choice(types)
        rows.append(
            [
                counterparty_id,
                f"Counterparty {index} {rng.choice(['Inc', 'LLC', 'Ltd', 'Corp'])}",
                _random_lei(rng),
                counterparty_type,
                rng.choice(countries),
                rng.choice(ratings),
                rng.choice(industries),
            ]
        )
        ref.counterparty_types[counterparty_id] = counterparty_type

    # 追加集团内实体作为对手方（CP9001 ↔ ENT001，依此类推）。
    # lei_code 取该实体在 ref_entity_hierarchy 里的同一个 LEI（全局法人标识，两表同源）。
    # credit_rating 与 industry_code 给固定值，是演示假设。
    affiliate_credit_rating = "AA"
    affiliate_industry_code = "6010"
    for entity_index, entity_row in enumerate(ENTITY_ROWS, start=1):
        entity_code = entity_row[0]
        entity_name = entity_row[1]
        entity_lei = entity_row[2]
        entity_jurisdiction = entity_row[5]
        cp_id = f"CP900{entity_index}"
        rows.append(
            [
                cp_id,
                entity_name,
                entity_lei,
                "AFFILIATE",
                entity_jurisdiction,
                affiliate_credit_rating,
                affiliate_industry_code,
            ]
        )
        ref.counterparty_types[cp_id] = "AFFILIATE"
        ref.affiliate_by_entity[entity_code] = cp_id

    header = [
        "counterparty_id",
        "counterparty_name",
        "lei_code",
        "counterparty_type",
        "country_code",
        "credit_rating",
        "industry_code",
    ]
    ref.row_counts["ref_counterparty"] = write_csv(ref_dir / "ref_counterparty.csv", header, rows)

    # 随机抽取池只放**外部**对手方：集团内对手方只在刻意构造的内部往来配对腿里出现。
    # 若把它们混进随机池，数据里会出现「只存在一条腿」的内部头寸（例如一笔对母公司的回购，
    # 却没有对应的另一腿），合并抵销就会只抵掉一边、把差额留在合并口径里 —— 抵销必须成对。
    ref.counterparties = [row[0] for row in rows if row[3] != "AFFILIATE"]
    ref.affiliate_counterparties = [row[0] for row in rows if row[3] == "AFFILIATE"]


def _generate_maturity_bucket(ref_dir: Path, ref: ReferenceData) -> None:
    header = ["bucket_code", "bucket_description", "min_days", "max_days", "sort_order", "fr2052a_display_order"]
    rows = [list(row) for row in MATURITY_BUCKET_ROWS]
    ref.row_counts["ref_maturity_bucket"] = write_csv(ref_dir / "ref_maturity_bucket.csv", header, rows)


def _generate_line_items(ref_dir: Path, ref: ReferenceData) -> None:
    header = [
        "line_item_code",
        "section_code",
        "line_description",
        "parent_line_item",
        "is_calculated",
        "calculation_formula",
        "data_type",
        "sign_convention",
        "mandatory_flag",
        "sort_order",
    ]
    rows = [list(row) for row in LINE_ITEM_ROWS]
    ref.row_counts["ref_fr2052a_line_items"] = write_csv(ref_dir / "ref_fr2052a_line_items.csv", header, rows)


def _generate_exchange_rates(
    ref_dir: Path,
    ref: ReferenceData,
    inject_missing_fx: set[str] | None = None,
    report_dates: Sequence[date] | None = None,
) -> None:
    """生成汇率表，并把「全量币种集合」交给 ODS 抽取使用。

    契约：
        - 汇率表是**正常状态下**的全量币种来源：ODS 各表抽到的币种必须都在汇率表里。
        - 唯一例外是**故意注入**：ODS 仍会照常抽到该币种，只是汇率表里没有这一行 ——
          这正是「缺汇率」这类缺陷数据的样子，由 dbt 断言把它变成显式失败。
        - 唯一键 (rate_date, from_currency, to_currency, rate_type) 不得重复。

    Args:
        ref_dir: REF 输出目录
        ref: 引用数据容器，本函数写入 currencies 与 spot_rates
        inject_missing_fx: 故意不写汇率行的币种集合，用于验证「缺汇率必须失败」。
            正常生产时不传。传入后：汇率表少这些行，但 ODS 的抽币种池不收缩
            （否则 ODS 会退化成不抽该币种，缺汇率的场景反而造不出来）。
        report_dates: 要出汇率行的报告日列表；None 时只出锚定报告日那一组。
            多期样本里每一期都要能折算出 USD，缺了就会被汇率覆盖断言判为缺汇率。
    """
    inject_missing_fx = inject_missing_fx or set()
    if inject_missing_fx:
        print(f"  [INJECT] 本次故意注入缺汇率：{', '.join(sorted(inject_missing_fx))}")
        print("  [INJECT] 这些币种不会出现在 ref_exchange_rates，但数据里仍可能抽到它们")
        print("  [INJECT] 预期效果：dbt 的汇率覆盖断言报红（这才是缺陷数据该有的样子）")

    header = ["rate_date", "from_currency", "to_currency", "spot_rate", "rate_type", "rate_source"]
    dates = list(report_dates) if report_dates else [REPORT_DATE]
    rows: list[list[object]] = []
    for rate_date in dates:
        rows.extend(
            [rate_date.isoformat(), currency, "USD", rate, "MID", USD_RATE_SOURCE]
            for currency, rate in FX_RATES.items()
            if currency not in inject_missing_fx
        )
        if "USD" not in inject_missing_fx:
            rows.append([rate_date.isoformat(), "USD", "USD", 1.0, "MID", "INTERNAL"])
    ref.row_counts["ref_exchange_rates"] = write_csv(ref_dir / "ref_exchange_rates.csv", header, rows)

    # 唯一键断言：(rate_date, from_currency, to_currency, rate_type) 不得重复
    seen: set[tuple[str, str, str, str]] = set()
    for row in rows:
        key = (str(row[0]), str(row[1]), str(row[2]), str(row[4]))
        assert key not in seen, f"汇率表唯一键重复：{key}"
        seen.add(key)

    # 抽币种池取**全量**（FX_RATES + USD），不随注入收缩 —— 注入要让「数据里抽到该币种、
    # 汇率表里没有」这种缺陷数据可复现，池子一起收缩就复现不出来了。
    ref.currencies = [*FX_RATES.keys(), "USD"]
    ref.spot_rates = {**{str(code): float(rate) for code, rate in FX_RATES.items()}, "USD": 1.0}


def _generate_regulatory_mapping(ref_dir: Path, ref: ReferenceData) -> None:
    header = [
        "section_code",
        "line_item_code",
        "field_name",
        "regulatory_reference",
        "rule_id",
        "haircut_rate",
        "owner",
        "effective_date",
        "expiry_date",
    ]
    rows = [list(row) for row in REGULATORY_MAPPING_ROWS]
    ref.row_counts["ref_regulatory_mapping"] = write_csv(ref_dir / "ref_regulatory_mapping.csv", header, rows)


def _generate_behavior_assumptions(ref_dir: Path, ref: ReferenceData) -> None:
    header = [
        "product_category",
        "customer_segment",
        "maturity_bucket",
        "runoff_rate",
        "inflow_rate",
        "effective_date",
        "expiry_date",
    ]
    rows = [list(row) for row in BEHAVIOR_ASSUMPTION_ROWS]
    ref.row_counts["ref_behavior_assumptions"] = write_csv(ref_dir / "ref_behavior_assumptions.csv", header, rows)


def _generate_calendar(ref_dir: Path, ref: ReferenceData) -> None:
    header = ["calendar_date", "is_business_day", "holiday_name", "jurisdiction"]
    rows = []
    business_dates: list[str] = []
    current = CALENDAR_START
    while current <= CALENDAR_END:
        is_business_day = current.weekday() < 5
        holiday_name = HOLIDAYS.get(current, "")
        if holiday_name:
            is_business_day = False
        iso = current.isoformat()
        rows.append([iso, is_business_day, holiday_name, "US"])
        if is_business_day and current <= REPORT_DATE:
            business_dates.append(iso)
        current += timedelta(days=1)

    ref.row_counts["ref_calendar"] = write_csv(ref_dir / "ref_calendar.csv", header, rows)
    ref.business_dates = business_dates


def _generate_validation_rules(ref_dir: Path, ref: ReferenceData) -> None:
    header = [
        "rule_id",
        "rule_name",
        "rule_category",
        "apply_layer",
        "severity",
        "sql_expression",
        "description",
        "is_active",
    ]
    rows = [list(row) for row in VALIDATION_RULE_ROWS]
    ref.row_counts["ref_validation_rules"] = write_csv(ref_dir / "ref_validation_rules.csv", header, rows)


def generate_all(
    ref_dir: Path,
    inject_missing_fx: set[str] | None = None,
    report_dates: Sequence[date] | None = None,
) -> ReferenceData:
    """生成全部 9 张 REF 表，返回可复用的外键集合。

    Args:
        ref_dir: REF 输出目录
        inject_missing_fx: 故意不写汇率行的币种集合，传给 _generate_exchange_rates。
        report_dates: 要出汇率行的报告日列表；None 时只出锚定报告日那一组。
            多期样本下每期都参与折算，汇率表缺了非锚定期的行就会被 dbt 的
            汇率覆盖断言判为缺汇率。
    """
    ref = ReferenceData()
    _generate_entity_hierarchy(ref_dir, ref)
    _generate_counterparty(ref_dir, ref)
    _generate_maturity_bucket(ref_dir, ref)
    _generate_line_items(ref_dir, ref)
    _generate_exchange_rates(ref_dir, ref, inject_missing_fx, report_dates)
    _generate_regulatory_mapping(ref_dir, ref)
    _generate_behavior_assumptions(ref_dir, ref)
    _generate_calendar(ref_dir, ref)
    _generate_validation_rules(ref_dir, ref)
    return ref
