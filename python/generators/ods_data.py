"""ODS 层（业务交易数据）生成。

这七张表是送入 Kafka 的业务明细，最终流入 Iceberg bronze 层。
所有外键（法人实体、交易对手、币种、日期）都取自 REF 层，
生成后由 generate_sample_data.py 统一做完整性自检。

相比原始方案（requirements/[97]）的三处修正：
  1. 每行统一带 report_date，日报批次可按日分区、可重跑；
  2. 每行统一带 entity_code，合并口径与子公司口径都能切；
  3. 日期字段按业务含义生成（开户日必早于报告日、到期日必晚于报告日），
     不会出现"未来开户"这类反常识数据。

多期能力：generate_all 接受 report_dates 列表，逐期生成各表。
每期用 ods_rng(table_name, report_date) 派生独立随机源，
保证同一报告日的数据与共生成了几期无关 —— 加期不扰动已有期。

[AI-GENERATED] model=qianfan-code-latest date=2026-09-18 reviewed_by=pending
"""

from __future__ import annotations

import csv
import random
import string
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path

from .config import (
    BATCH_ID,
    BOOKING_ENTITIES,
    ENTITY_CURRENCY_WEIGHTS,
    INTRACOMPANY_PAIRS,
    ODS_COLUMNS_HEAD,
    ODS_COLUMNS_TAIL,
    ODS_SOURCE_FILES,
    PARENT_AMOUNT_SCALE,
    PARENT_ENTITY,
    PARENT_VOLUMES,
    REPORT_DATE,
    VOLUMES,
    ods_rng,
    source_file,
    weighted_choice,
    write_csv,
)
from .ref_data import FX_RATES, ReferenceData

# 产品代码与存款类型的对应关系：避免出现"活期产品挂在定期类型下"这类不实数据
PRODUCT_DEPOSIT_TYPES: dict[str, str] = {
    "SAV_PREMIUM": "SAV",
    "CHK_BASIC": "CHK",
    "CD_12M": "CD",
    "MMDA": "MMDA",
    "TIME_DEP": "TIME",
}
TERM_DEPOSIT_TYPES = ("CD", "TIME")

CUSTOMER_TYPES = ("IND", "CORP", "GOV", "FI")
COLLATERAL_TYPES = ("UST", "AGENCY", "MBS", "CORP")
LOAN_TYPES = ("COMMERCIAL", "RETAIL", "MORTGAGE", "REVOLVING", "SYNDICATED")
# 证券类型与其权重。真实银行的 HQLA 以一级资产（国债）为主，
# 二级资产（机构债/MBS/公司债）占比通常低于 40% —— 若等概率抽取，二级资产会占八成，
# 报表的二级资产占比规则必然长期告警，与真实银行业务结构不符。
SECURITY_TYPE_WEIGHTS: dict[str, float] = {
    "TREASURY": 0.60,
    "AGENCY_DEBT": 0.15,
    "MBS": 0.10,
    "CORP_BOND": 0.08,
    "EQUITY": 0.04,
    "ABS": 0.03,
}
PORTFOLIO_CODES = ("HTP", "AFS", "HFT")
INSTRUMENT_TYPES = ("IRS", "CDS", "FX_FWD", "FX_SWAP", "OPTION", "FUTURES")
COMMITMENT_TYPES = ("CREDIT_COMMITMENT", "LETTER_OF_CREDIT", "GUARANTEE")
CREDIT_RATINGS = ("AAA", "AA", "A", "BBB", "BB")

# 集团总账科目：(科目号, 科目名, 余额方向)。
# 科目金额由业务明细倒推（见 generate_gl_balances），权益是轧差项 —— 这正是资产负债表的
# 平衡关系，因此借贷天然相等，不需要用"待清算"科目兜底。
GL_ACCOUNTS: tuple[tuple[str, str, str], ...] = (
    ("1001", "Cash and Cash Equivalents", "DEBIT"),
    ("1100", "Due from Banks", "DEBIT"),
    ("1200", "Investment Securities", "DEBIT"),
    ("1300", "Reverse Repo Receivable", "DEBIT"),
    ("1500", "Derivative Assets", "DEBIT"),
    ("2100", "Loans and Leases", "DEBIT"),
    ("2001", "Demand Deposits", "CREDIT"),
    ("2002", "Time Deposits", "CREDIT"),
    ("2010", "Securities Sold under Repo", "CREDIT"),
    ("2200", "Derivative Liabilities", "CREDIT"),
    ("5001", "Shareholders Equity", "CREDIT"),
)
GL_ROWS_PER_ACCOUNT = 5

# 现金与同业存放没有对应的业务明细表，按存款余额的一个比例设定（演示假设）
CASH_TO_DEPOSIT_RATIO = 0.08
DUE_FROM_BANKS_TO_DEPOSIT_RATIO = 0.04


@dataclass(frozen=True)
class GlEntry:
    """一行总账余额，借贷两侧字段齐全（非余额方向记 0）。"""

    account_id: str
    account_name: str
    debit: float
    credit: float
    currency: str = "USD"


class EventClock:
    """按报告日派生确定性递增的事件时间，保证同一份种子每次得到相同结果。"""

    def __init__(self, report_date: date, step_seconds: int = 3) -> None:
        """记下报告日与时间步长，事件时间由这两者推出。"""
        self._base = datetime.combine(report_date, time(2, 0, 0))
        self._step_seconds = step_seconds
        self._index = 0

    def next(self) -> str:
        """返回下一个事件时间戳，每调一次前进一个步长。"""
        stamp = self._base + timedelta(seconds=self._index * self._step_seconds)
        self._index += 1
        return stamp.strftime("%Y-%m-%d %H:%M:%S")


def _token(rng: random.Random, length: int = 8) -> str:
    return "".join(rng.choices(string.ascii_uppercase + string.digits, k=length))


def _business_day_before(rng: random.Random, report_date: date, min_days: int, max_days: int) -> str:
    """报告日往前推若干天的最近工作日。"""
    day = report_date - timedelta(days=rng.randint(min_days, max_days))
    while day.weekday() >= 5:
        day -= timedelta(days=1)
    return day.isoformat()


def _business_day_after(rng: random.Random, report_date: date, min_days: int, max_days: int) -> str:
    """报告日往后推若干天的最近工作日（所有到期日均晚于报告日）。"""
    day = report_date + timedelta(days=rng.randint(min_days, max_days))
    while day.weekday() >= 5:
        day += timedelta(days=1)
    return day.isoformat()


def _pick_entity_currency(rng: random.Random, ref: ReferenceData) -> tuple[str, str]:
    """抽一个记账实体，再按该实体的币种权重抽币种。"""
    entity_code = rng.choice(ref.trading_entities)
    currency = weighted_choice(rng, ENTITY_CURRENCY_WEIGHTS[entity_code])
    if currency not in ref.spot_rates:
        currency = "USD"
    return entity_code, currency


def _assemble(
    table_name: str,
    source_record_id: str,
    entity_code: str,
    business_values: Sequence[object],
    event_time: str,
    report_date: date,
) -> list[object]:
    """拼一行 ODS 记录：前缀列 + 业务列 + ETL 尾部列（列序与表头一致）。"""
    return [
        ODS_SOURCE_FILES[table_name][0],
        source_record_id,
        report_date.isoformat(),
        entity_code,
        *business_values,
        event_time,
        BATCH_ID,
        source_file(table_name, report_date),
    ]


def _header(business_columns: list[str]) -> list[str]:
    return [*ODS_COLUMNS_HEAD, *business_columns, *ODS_COLUMNS_TAIL]


def generate_deposits(ods_dir: Path, ref: ReferenceData, report_date: date, append: bool = False) -> int:
    """存款头寸：零售/对公/同业存款，含活期与定期。"""
    rng = ods_rng("ods_deposits", report_date)
    clock = EventClock(report_date)
    rows = []
    for index in range(1, VOLUMES["ods_deposits"] + 1):
        entity_code, currency = _pick_entity_currency(rng, ref)
        product_code = rng.choice(list(PRODUCT_DEPOSIT_TYPES))
        deposit_type = PRODUCT_DEPOSIT_TYPES[product_code]
        principal = round(rng.uniform(1_000, 5_000_000), 2)
        interest_rate = round(rng.uniform(0.0005, 0.0525), 6)
        accrued_interest = round(principal * interest_rate * rng.uniform(0.05, 1.0), 2)
        maturity_date = _business_day_after(rng, report_date, 15, 400) if deposit_type in TERM_DEPOSIT_TYPES else ""

        business = [
            f"ACC-{rng.randint(100000, 999999)}",
            f"CUST-{rng.randint(1000, 9999)}",
            product_code,
            deposit_type,
            currency,
            principal,
            accrued_interest,
            interest_rate,
            _business_day_before(rng, report_date, 30, 3650),
            maturity_date,
            f"BR-{entity_code}",
            rng.choice(CUSTOMER_TYPES),
            rng.choice(("Y", "N")),
        ]
        rows.append(_assemble("ods_deposits", f"DEP-{index:06d}", entity_code, business, clock.next(), report_date))

    business_columns = [
        "account_number",
        "customer_id",
        "product_code",
        "deposit_type",
        "currency",
        "principal_amount",
        "accrued_interest",
        "interest_rate",
        "open_date",
        "maturity_date",
        "branch_code",
        "customer_type_raw",
        "insured_flag",
    ]
    return write_csv(ods_dir / "ods_deposits.csv", _header(business_columns), rows, append=append)


def generate_repo_transactions(ods_dir: Path, ref: ReferenceData, report_date: date, append: bool = False) -> int:
    """回购与逆回购：质押式融资的存量交易。"""
    rng = ods_rng("ods_repo_transactions", report_date)
    clock = EventClock(report_date)
    rows = []
    for index in range(1, VOLUMES["ods_repo_transactions"] + 1):
        entity_code, currency = _pick_entity_currency(rng, ref)
        cash_amount = round(rng.uniform(100_000, 50_000_000), 2)
        haircut_pct = round(rng.uniform(0.01, 0.10), 4)
        collateral_market_value = round(cash_amount * (1 + haircut_pct + rng.uniform(0.0, 0.03)), 2)

        business = [
            f"RPO-{_token(rng)}",
            rng.choice(ref.counterparties),
            rng.choice(("REPO", "REVERSE_REPO")),
            currency,
            cash_amount,
            collateral_market_value,
            haircut_pct,
            round(rng.uniform(0.01, 0.08), 6),
            _business_day_before(rng, report_date, 0, 60),
            _business_day_after(rng, report_date, 1, 90),
            f"US{rng.randint(1000000000, 9999999999)}",
            rng.choice(COLLATERAL_TYPES),
            f"GMRA-{_token(rng, 6)}",
        ]
        rows.append(
            _assemble("ods_repo_transactions", f"REPO-{index:06d}", entity_code, business, clock.next(), report_date)
        )

    business_columns = [
        "deal_id",
        "counterparty_id",
        "repo_type",
        "currency",
        "cash_amount",
        "collateral_market_value",
        "haircut_pct",
        "interest_rate",
        "start_date",
        "end_date",
        "collateral_isin",
        "collateral_type_raw",
        "netting_agreement_id",
    ]
    return write_csv(ods_dir / "ods_repo_transactions.csv", _header(business_columns), rows, append=append)


def generate_loans(ods_dir: Path, ref: ReferenceData, report_date: date, append: bool = False) -> int:
    """贷款台账：已用额度与未提取额度，支撑表内外融资口径。"""
    rng = ods_rng("ods_loans", report_date)
    clock = EventClock(report_date)
    rows = []
    for index in range(1, VOLUMES["ods_loans"] + 1):
        entity_code, currency = _pick_entity_currency(rng, ref)
        facility_amount = round(rng.uniform(10_000, 20_000_000), 2)
        outstanding_amount = round(facility_amount * rng.uniform(0.10, 0.95), 2)

        business = [
            f"LN-{_token(rng)}",
            rng.choice(ref.counterparties),
            rng.choice(LOAN_TYPES),
            facility_amount,
            outstanding_amount,
            round(facility_amount - outstanding_amount, 2),
            currency,
            round(rng.uniform(0.01, 0.12), 6),
            rng.choice(("FIXED", "FLOAT")),
            _business_day_before(rng, report_date, 90, 1500),
            # 到期日从 1 天起：贷款簿里必然有 30 天内到期的余额，
            # 否则 Section F（30 天流入）恒为 0，报表会失真
            _business_day_after(rng, report_date, 1, 1800),
            _business_day_after(rng, report_date, 1, 120),
            rng.choice(("Y", "N")),
            rng.choice(("CORP", "IND", "FI")),
            rng.choice(CREDIT_RATINGS),
        ]
        rows.append(_assemble("ods_loans", f"LOAN-{index:06d}", entity_code, business, clock.next(), report_date))

    business_columns = [
        "loan_id",
        "borrower_id",
        "loan_type",
        "facility_amount",
        "outstanding_amount",
        "undrawn_amount",
        "currency",
        "interest_rate",
        "rate_type",
        "origination_date",
        "maturity_date",
        "next_payment_date",
        "collateral_flag",
        "borrower_type_raw",
        "credit_grade_raw",
    ]
    return write_csv(ods_dir / "ods_loans.csv", _header(business_columns), rows, append=append)


def generate_securities(ods_dir: Path, ref: ReferenceData, report_date: date, append: bool = False) -> int:
    """证券持仓：HQLA 分级的原始依据，含质押标记。"""
    rng = ods_rng("ods_securities", report_date)
    clock = EventClock(report_date)
    rows = []
    for index in range(1, VOLUMES["ods_securities"] + 1):
        entity_code, currency = _pick_entity_currency(rng, ref)
        face_amount = round(rng.uniform(100_000, 100_000_000), 2)

        business = [
            f"SEC-{_token(rng)}",
            f"US{rng.randint(1000000000, 9999999999)}",
            f"{rng.randint(100000000, 999999999)}",
            weighted_choice(rng, SECURITY_TYPE_WEIGHTS),
            rng.choice(PORTFOLIO_CODES),
            rng.choice(ref.counterparties),
            currency,
            face_amount,
            round(face_amount * rng.uniform(0.95, 1.05), 2),
            round(face_amount * rng.uniform(0.98, 1.02), 2),
            round(rng.uniform(0.01, 0.08), 6),
            _business_day_before(rng, report_date, 30, 1200),
            _business_day_after(rng, report_date, 60, 3650),
            rng.choice(("AAA", "AA", "A", "BBB")),
            rng.choice(("Y", "N")),
        ]
        rows.append(_assemble("ods_securities", f"SEC-{index:06d}", entity_code, business, clock.next(), report_date))

    business_columns = [
        "security_id",
        "isin",
        "cusip",
        "security_type",
        "portfolio_code",
        "issuer_id",
        "currency",
        "face_amount",
        "market_value",
        "book_value",
        "coupon_rate",
        "purchase_date",
        "maturity_date",
        "credit_rating_raw",
        "pledged_flag",
    ]
    return write_csv(ods_dir / "ods_securities.csv", _header(business_columns), rows, append=append)


def generate_derivatives(ods_dir: Path, ref: ReferenceData, report_date: date, append: bool = False) -> int:
    """衍生品交易：盯市价值与双边抵押品，支撑衍生品融资口径。"""
    rng = ods_rng("ods_derivatives", report_date)
    clock = EventClock(report_date)
    rows = []
    for index in range(1, VOLUMES["ods_derivatives"] + 1):
        entity_code, currency = _pick_entity_currency(rng, ref)

        business = [
            f"TRD-{_token(rng)}",
            rng.choice(ref.counterparties),
            rng.choice(INSTRUMENT_TYPES),
            round(rng.uniform(1_000_000, 500_000_000), 2),
            currency,
            f"{currency}/USD",
            _business_day_before(rng, report_date, 1, 60),
            _business_day_after(rng, report_date, 30, 1800),
            round(rng.uniform(-10_000_000, 10_000_000), 2),
            "USD",
            rng.choice(("Y", "N")),
            f"CSA-{_token(rng, 6)}",
            round(rng.uniform(0, 5_000_000), 2),
            round(rng.uniform(0, 5_000_000), 2),
        ]
        rows.append(_assemble("ods_derivatives", f"DRV-{index:06d}", entity_code, business, clock.next(), report_date))

    business_columns = [
        "trade_id",
        "counterparty_id",
        "instrument_type",
        "notional_amount",
        "currency",
        "currency_pair",
        "trade_date",
        "maturity_date",
        "mark_to_market",
        "mtm_currency",
        "is_central_cleared",
        "csa_agreement_id",
        "collateral_posted",
        "collateral_received",
    ]
    return write_csv(ods_dir / "ods_derivatives.csv", _header(business_columns), rows, append=append)


def _split_amount(rng: random.Random, total: float, parts: int) -> list[float]:
    """把总额随机拆成 parts 份，各份非负、合计精确等于 total。"""
    if parts <= 0:
        return []
    cuts = sorted(rng.uniform(0.0, 1.0) for _ in range(parts - 1))
    bounds = [0.0, *cuts, 1.0]
    shares = [round(total * (bounds[index + 1] - bounds[index]), 2) for index in range(parts)]
    shares[-1] = round(total - sum(shares[:-1]), 2)
    return shares


def _amount_usd(row: dict[str, str], amount_column: str) -> float:
    """把一行明细的指定金额按其币种折算成 USD。"""
    return float(row[amount_column]) * FX_RATES.get(row["currency"], 1.0)


def _read_ods_rows(ods_dir: Path, table_name: str, report_date: date | None = None) -> list[dict[str, str]]:
    """读回已经写出的 ODS 明细，供总账倒推使用。

    Args:
        ods_dir: ODS 输出目录。
        table_name: 表名（不含 .csv 后缀）。
        report_date: 给定时只返回该报告日的行；不给时返回全部行。
    """
    with (ods_dir / f"{table_name}.csv").open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    if report_date is not None:
        target = report_date.isoformat()
        rows = [row for row in rows if row["report_date"] == target]
    return rows


def generate_gl_balances(
    ods_dir: Path,
    ref: ReferenceData,
    report_date: date,
    gl_break_amount: float = 0.0,
    append: bool = False,
) -> int:
    """逐实体一本账：对每个 BOOKING_ENTITIES 各生成一组科目余额，各自借贷平衡。

    为什么从「一本总账挂在 ENT001」改为逐实体：旧实现整本总账挂在母公司名下、
    却由全部实体的明细倒推，导致总账里的现金（Section E）无法归属到任何法人实体。
    逐实体记账后，每个实体的 Section E 都有自己的现金行。

    倒推关系（与旧实现一致，只是按实体过滤明细）：
        资产  1001 现金、1100 同业存放（按存款余额比例设定，属演示假设）
              1200 证券、1300 逆回购、1500 衍生品资产、2100 贷款
        负债  2001/2002 存款、2010 正回购、2200 衍生品负债
        权益  5001 = 资产 - 负债（轧差项）

    资产负债表本就是"资产 = 负债 + 权益"，权益作轧差后借贷天然相等。

    gl_break_amount 给正值时故意让 ENT002 的 2100 科目少记这么多（USD），制造受控缺口：
    总账整体借贷仍然平衡（权益是轧差项，替它吸收），但 ENT002 的 2100 科目余额
    小于该实体贷款明细合计，与 Section F 对不上，供合规剧本演示 GL 对账阻断。

    为什么缺口落在科目侧而不是权益侧：权益不参与任何 Section 对账，
    少记权益只会让「资产 = 负债 + 权益」不成立，而分科目对账照旧全 PASS。
    异常要造在能被判据碰到的地方。

    本函数按单个报告日生成总账：只读该期的业务明细来倒推该期余额，
    因此多期数据下每期总账各自平衡，不会把两期的借贷混在一起算。

    Args:
        ods_dir: ODS 输出目录。
        ref: 引用数据（用于 affiliate_by_entity 识别内部往来存款腿）。
        report_date: 该期的报告日。
        gl_break_amount: 总账故意少记的金额，落在 ENT002 的 2100 科目。
        append: 追加模式。

    Returns:
        本次写入的总账行数。
    """
    rng = ods_rng("ods_gl_balances", report_date)
    clock = EventClock(report_date)

    deposits = _read_ods_rows(ods_dir, "ods_deposits", report_date)
    repo = _read_ods_rows(ods_dir, "ods_repo_transactions", report_date)
    loans = _read_ods_rows(ods_dir, "ods_loans", report_date)
    securities = _read_ods_rows(ods_dir, "ods_securities", report_date)
    derivatives = _read_ods_rows(ods_dir, "ods_derivatives", report_date)

    all_rows: list[list[object]] = []
    row_index = 0

    for entity_code in BOOKING_ENTITIES:
        # 按实体过滤明细：该实体自己的业务倒推该实体的总账
        dep_entity = [row for row in deposits if row["entity_code"] == entity_code]
        repo_entity = [row for row in repo if row["entity_code"] == entity_code]
        loans_entity = [row for row in loans if row["entity_code"] == entity_code]
        sec_entity = [row for row in securities if row["entity_code"] == entity_code]
        deriv_entity = [row for row in derivatives if row["entity_code"] == entity_code]

        demand_deposits = round(
            sum(
                _amount_usd(row, "principal_amount")
                for row in dep_entity
                if row["deposit_type"] in ("CHK", "SAV", "MMDA")
            ),
            2,
        )
        time_deposits = round(
            sum(_amount_usd(row, "principal_amount") for row in dep_entity if row["deposit_type"] in ("CD", "TIME")),
            2,
        )
        total_deposits = round(demand_deposits + time_deposits, 2)

        balances = {
            "1001": round(total_deposits * CASH_TO_DEPOSIT_RATIO, 2),
            "1100": round(total_deposits * DUE_FROM_BANKS_TO_DEPOSIT_RATIO, 2),
            "1200": round(sum(_amount_usd(row, "market_value") for row in sec_entity), 2),
            "1300": round(
                sum(_amount_usd(row, "cash_amount") for row in repo_entity if row["repo_type"] == "REVERSE_REPO"), 2
            ),
            "1500": round(
                sum(_amount_usd(row, "mark_to_market") for row in deriv_entity if float(row["mark_to_market"]) > 0), 2
            ),
            "2100": round(sum(_amount_usd(row, "outstanding_amount") for row in loans_entity), 2),
            "2001": demand_deposits,
            "2002": time_deposits,
            "2010": round(sum(_amount_usd(row, "cash_amount") for row in repo_entity if row["repo_type"] == "REPO"), 2),
            "2200": round(
                -sum(_amount_usd(row, "mark_to_market") for row in deriv_entity if float(row["mark_to_market"]) < 0), 2
            ),
        }

        # gl_break_amount 只落在 ENT002 的 2100 科目
        if entity_code == "ENT002" and gl_break_amount > 0:
            balances["2100"] = round(balances["2100"] - gl_break_amount, 2)

        assets = round(sum(balances[account] for account, _, side in GL_ACCOUNTS if side == "DEBIT"), 2)
        liabilities = round(
            sum(balances[account] for account, _, side in GL_ACCOUNTS if side == "CREDIT" and account in balances), 2
        )
        equity = round(assets - liabilities, 2)
        if equity < 0:
            raise ValueError(f"倒推出的权益为负（实体 {entity_code}，权益 {equity}），请检查业务明细规模与现金比例假设")
        balances["5001"] = equity

        # 该实体没有业务的科目记 0（0 = 查过且确实为零）
        for account_id, _, _ in GL_ACCOUNTS:
            if account_id not in balances:
                balances[account_id] = 0.0

        for account_id, account_name, normal_side in GL_ACCOUNTS:
            for amount in _split_amount(rng, balances[account_id], GL_ROWS_PER_ACCOUNT):
                entry = GlEntry(
                    account_id=account_id,
                    account_name=account_name,
                    debit=amount if normal_side == "DEBIT" else 0.0,
                    credit=amount if normal_side == "CREDIT" else 0.0,
                )
                all_rows.append(
                    _assemble(
                        "ods_gl_balances",
                        f"GL-{row_index:06d}",
                        entity_code,
                        [entry.account_id, entry.account_name, entry.debit, entry.credit, entry.currency],
                        clock.next(),
                        report_date,
                    )
                )
                row_index += 1

    business_columns = ["gl_account_id", "account_name", "debit_balance", "credit_balance", "currency"]
    return write_csv(ods_dir / "ods_gl_balances.csv", _header(business_columns), all_rows, append=append)


def generate_off_bs_commitments(ods_dir: Path, ref: ReferenceData, report_date: date, append: bool = False) -> int:
    """表外承诺：授信承诺、信用证、担保，进 Section J。"""
    rng = ods_rng("ods_off_bs_commitments", report_date)
    clock = EventClock(report_date)
    rows = []
    for index in range(1, VOLUMES["ods_off_bs_commitments"] + 1):
        entity_code, currency = _pick_entity_currency(rng, ref)
        facility_amount = round(rng.uniform(100_000, 50_000_000), 2)

        business = [
            f"CMT-{_token(rng)}",
            rng.choice(ref.counterparties),
            rng.choice(COMMITMENT_TYPES),
            facility_amount,
            round(facility_amount * rng.uniform(0.10, 0.90), 2),
            currency,
            _business_day_after(rng, report_date, 30, 720),
        ]
        rows.append(
            _assemble("ods_off_bs_commitments", f"OFFBS-{index:06d}", entity_code, business, clock.next(), report_date)
        )

    business_columns = [
        "commitment_id",
        "counterparty_id",
        "commitment_type",
        "facility_amount",
        "undrawn_amount",
        "currency",
        "maturity_date",
    ]
    return write_csv(ods_dir / "ods_off_bs_commitments.csv", _header(business_columns), rows, append=append)


def apply_deposit_correction(ods_dir: Path, record_id: str, new_amount: float) -> None:
    """演示用：对锚定报告日的单笔存款本金做一笔修正，用于重述剧本。

    为什么做成生成器的一个开关，而不是手工改 CSV：
    手工改出来的样本与生成器的输出不再一致，下一次重跑生成器就被抹掉了，剧本不可复现。
    修正必须发生在总账倒推之前 —— 总账余额是由业务明细倒推的，
    若在总账生成之后再改明细，就会凭空造出一个对账缺口，重述剧本会误报成对账失败。

    多期数据下的语义：source_record_id 跨期会重号（同一个账户在不同报告日是同一个源记录号，
    这是有意的、也符合 bronze 主键 (source_system, source_record_id, report_date)）。
    因此修正只作用于锚定报告日（REPORT_DATE = 2026-09-16）那一行，
    不会静默改掉其他报告日的同号记录。
    """
    target_date = REPORT_DATE.isoformat()
    path = ods_dir / "ods_deposits.csv"
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        fieldnames = list(reader.fieldnames or [])
        rows = list(reader)

    matched = [row for row in rows if row["source_record_id"] == record_id and row["report_date"] == target_date]
    if not matched:
        raise ValueError(f"ods_deposits 里没有 source_record_id = {record_id} 且 report_date = {target_date} 的记录")
    for row in matched:
        row["principal_amount"] = f"{new_amount:.4f}"

    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def generate_parent_deposits(ods_dir: Path, ref: ReferenceData, report_date: date, append: bool = False) -> int:
    """母公司对第三方的存款（负债）：追加行，与子公司互不干扰。

    母公司是小账：金额区间上限乘以 PARENT_AMOUNT_SCALE（演示假设）。
    customer_type_raw 不取 AFFIL——那只有配对腿用。
    """
    rng = ods_rng("ods_deposits_parent", report_date)
    clock = EventClock(report_date)
    rows = []
    for index in range(1, PARENT_VOLUMES["ods_deposits"] + 1):
        currency = weighted_choice(rng, ENTITY_CURRENCY_WEIGHTS[PARENT_ENTITY])
        if currency not in ref.spot_rates:
            currency = "USD"
        product_code = rng.choice(list(PRODUCT_DEPOSIT_TYPES))
        deposit_type = PRODUCT_DEPOSIT_TYPES[product_code]
        principal = round(rng.uniform(1_000, 5_000_000 * PARENT_AMOUNT_SCALE), 2)
        interest_rate = round(rng.uniform(0.0005, 0.0525), 6)
        accrued_interest = round(principal * interest_rate * rng.uniform(0.05, 1.0), 2)
        maturity_date = _business_day_after(rng, report_date, 15, 400) if deposit_type in TERM_DEPOSIT_TYPES else ""

        business = [
            f"ACC-{rng.randint(100000, 999999)}",
            f"CUST-{rng.randint(1000, 9999)}",
            product_code,
            deposit_type,
            currency,
            principal,
            accrued_interest,
            interest_rate,
            _business_day_before(rng, report_date, 30, 3650),
            maturity_date,
            f"BR-{PARENT_ENTITY}",
            rng.choice(CUSTOMER_TYPES),
            rng.choice(("Y", "N")),
        ]
        rows.append(_assemble("ods_deposits", f"PDEP-{index:06d}", PARENT_ENTITY, business, clock.next(), report_date))

    business_columns = [
        "account_number",
        "customer_id",
        "product_code",
        "deposit_type",
        "currency",
        "principal_amount",
        "accrued_interest",
        "interest_rate",
        "open_date",
        "maturity_date",
        "branch_code",
        "customer_type_raw",
        "insured_flag",
    ]
    return write_csv(ods_dir / "ods_deposits.csv", _header(business_columns), rows, append=append)


def generate_parent_repo(ods_dir: Path, ref: ReferenceData, report_date: date, append: bool = False) -> int:
    """母公司对第三方的回购交易：追加行。"""
    rng = ods_rng("ods_repo_parent", report_date)
    clock = EventClock(report_date)
    external_cps = [cp for cp in ref.counterparties if ref.counterparty_types.get(cp) != "AFFILIATE"]
    rows = []
    for index in range(1, PARENT_VOLUMES["ods_repo_transactions"] + 1):
        currency = weighted_choice(rng, ENTITY_CURRENCY_WEIGHTS[PARENT_ENTITY])
        if currency not in ref.spot_rates:
            currency = "USD"
        cash_amount = round(rng.uniform(100_000, 50_000_000 * PARENT_AMOUNT_SCALE), 2)
        haircut_pct = round(rng.uniform(0.01, 0.10), 4)
        collateral_market_value = round(cash_amount * (1 + haircut_pct + rng.uniform(0.0, 0.03)), 2)

        business = [
            f"RPO-{_token(rng)}",
            rng.choice(external_cps),
            rng.choice(("REPO", "REVERSE_REPO")),
            currency,
            cash_amount,
            collateral_market_value,
            haircut_pct,
            round(rng.uniform(0.01, 0.08), 6),
            _business_day_before(rng, report_date, 0, 60),
            _business_day_after(rng, report_date, 1, 90),
            f"US{rng.randint(1000000000, 9999999999)}",
            rng.choice(COLLATERAL_TYPES),
            f"GMRA-{_token(rng, 6)}",
        ]
        rows.append(
            _assemble("ods_repo_transactions", f"PREPO-{index:06d}", PARENT_ENTITY, business, clock.next(), report_date)
        )

    business_columns = [
        "deal_id",
        "counterparty_id",
        "repo_type",
        "currency",
        "cash_amount",
        "collateral_market_value",
        "haircut_pct",
        "interest_rate",
        "start_date",
        "end_date",
        "collateral_isin",
        "collateral_type_raw",
        "netting_agreement_id",
    ]
    return write_csv(ods_dir / "ods_repo_transactions.csv", _header(business_columns), rows, append=append)


def generate_parent_loans(ods_dir: Path, ref: ReferenceData, report_date: date, append: bool = False) -> int:
    """母公司对第三方的贷款：追加行。"""
    rng = ods_rng("ods_loans_parent", report_date)
    clock = EventClock(report_date)
    external_cps = [cp for cp in ref.counterparties if ref.counterparty_types.get(cp) != "AFFILIATE"]
    rows = []
    for index in range(1, PARENT_VOLUMES["ods_loans"] + 1):
        currency = weighted_choice(rng, ENTITY_CURRENCY_WEIGHTS[PARENT_ENTITY])
        if currency not in ref.spot_rates:
            currency = "USD"
        facility_amount = round(rng.uniform(10_000, 20_000_000 * PARENT_AMOUNT_SCALE), 2)
        outstanding_amount = round(facility_amount * rng.uniform(0.10, 0.95), 2)

        business = [
            f"LN-{_token(rng)}",
            rng.choice(external_cps),
            rng.choice(LOAN_TYPES),
            facility_amount,
            outstanding_amount,
            round(facility_amount - outstanding_amount, 2),
            currency,
            round(rng.uniform(0.01, 0.12), 6),
            rng.choice(("FIXED", "FLOAT")),
            _business_day_before(rng, report_date, 90, 1500),
            _business_day_after(rng, report_date, 1, 1800),
            _business_day_after(rng, report_date, 1, 120),
            rng.choice(("Y", "N")),
            rng.choice(("CORP", "IND", "FI")),
            rng.choice(CREDIT_RATINGS),
        ]
        rows.append(_assemble("ods_loans", f"PLOAN-{index:06d}", PARENT_ENTITY, business, clock.next(), report_date))

    business_columns = [
        "loan_id",
        "borrower_id",
        "loan_type",
        "facility_amount",
        "outstanding_amount",
        "undrawn_amount",
        "currency",
        "interest_rate",
        "rate_type",
        "origination_date",
        "maturity_date",
        "next_payment_date",
        "collateral_flag",
        "borrower_type_raw",
        "credit_grade_raw",
    ]
    return write_csv(ods_dir / "ods_loans.csv", _header(business_columns), rows, append=append)


def generate_parent_securities(ods_dir: Path, ref: ReferenceData, report_date: date, append: bool = False) -> int:
    """母公司的证券持仓：追加行。"""
    rng = ods_rng("ods_securities_parent", report_date)
    clock = EventClock(report_date)
    external_cps = [cp for cp in ref.counterparties if ref.counterparty_types.get(cp) != "AFFILIATE"]
    rows = []
    for index in range(1, PARENT_VOLUMES["ods_securities"] + 1):
        currency = weighted_choice(rng, ENTITY_CURRENCY_WEIGHTS[PARENT_ENTITY])
        if currency not in ref.spot_rates:
            currency = "USD"
        face_amount = round(rng.uniform(100_000, 100_000_000 * PARENT_AMOUNT_SCALE), 2)

        business = [
            f"SEC-{_token(rng)}",
            f"US{rng.randint(1000000000, 9999999999)}",
            f"{rng.randint(100000000, 999999999)}",
            weighted_choice(rng, SECURITY_TYPE_WEIGHTS),
            rng.choice(PORTFOLIO_CODES),
            rng.choice(external_cps),
            currency,
            face_amount,
            round(face_amount * rng.uniform(0.95, 1.05), 2),
            round(face_amount * rng.uniform(0.98, 1.02), 2),
            round(rng.uniform(0.01, 0.08), 6),
            _business_day_before(rng, report_date, 30, 1200),
            _business_day_after(rng, report_date, 60, 3650),
            rng.choice(("AAA", "AA", "A", "BBB")),
            rng.choice(("Y", "N")),
        ]
        rows.append(
            _assemble("ods_securities", f"PSEC-{index:06d}", PARENT_ENTITY, business, clock.next(), report_date)
        )

    business_columns = [
        "security_id",
        "isin",
        "cusip",
        "security_type",
        "portfolio_code",
        "issuer_id",
        "currency",
        "face_amount",
        "market_value",
        "book_value",
        "coupon_rate",
        "purchase_date",
        "maturity_date",
        "credit_rating_raw",
        "pledged_flag",
    ]
    return write_csv(ods_dir / "ods_securities.csv", _header(business_columns), rows, append=append)


def generate_parent_derivatives(ods_dir: Path, ref: ReferenceData, report_date: date, append: bool = False) -> int:
    """母公司对第三方的衍生品交易：追加行。"""
    rng = ods_rng("ods_derivatives_parent", report_date)
    clock = EventClock(report_date)
    external_cps = [cp for cp in ref.counterparties if ref.counterparty_types.get(cp) != "AFFILIATE"]
    rows = []
    for index in range(1, PARENT_VOLUMES["ods_derivatives"] + 1):
        currency = weighted_choice(rng, ENTITY_CURRENCY_WEIGHTS[PARENT_ENTITY])
        if currency not in ref.spot_rates:
            currency = "USD"

        business = [
            f"TRD-{_token(rng)}",
            rng.choice(external_cps),
            rng.choice(INSTRUMENT_TYPES),
            round(rng.uniform(1_000_000, 500_000_000 * PARENT_AMOUNT_SCALE), 2),
            currency,
            f"{currency}/USD",
            _business_day_before(rng, report_date, 1, 60),
            _business_day_after(rng, report_date, 30, 1800),
            round(rng.uniform(-10_000_000, 10_000_000) * PARENT_AMOUNT_SCALE, 2),
            "USD",
            rng.choice(("Y", "N")),
            f"CSA-{_token(rng, 6)}",
            round(rng.uniform(0, 5_000_000) * PARENT_AMOUNT_SCALE, 2),
            round(rng.uniform(0, 5_000_000) * PARENT_AMOUNT_SCALE, 2),
        ]
        rows.append(
            _assemble("ods_derivatives", f"PDRV-{index:06d}", PARENT_ENTITY, business, clock.next(), report_date)
        )

    business_columns = [
        "trade_id",
        "counterparty_id",
        "instrument_type",
        "notional_amount",
        "currency",
        "currency_pair",
        "trade_date",
        "maturity_date",
        "mark_to_market",
        "mtm_currency",
        "is_central_cleared",
        "csa_agreement_id",
        "collateral_posted",
        "collateral_received",
    ]
    return write_csv(ods_dir / "ods_derivatives.csv", _header(business_columns), rows, append=append)


def generate_parent_off_bs(ods_dir: Path, ref: ReferenceData, report_date: date, append: bool = False) -> int:
    """母公司的表外承诺：追加行。"""
    rng = ods_rng("ods_off_bs_parent", report_date)
    clock = EventClock(report_date)
    external_cps = [cp for cp in ref.counterparties if ref.counterparty_types.get(cp) != "AFFILIATE"]
    rows = []
    for index in range(1, PARENT_VOLUMES["ods_off_bs_commitments"] + 1):
        currency = weighted_choice(rng, ENTITY_CURRENCY_WEIGHTS[PARENT_ENTITY])
        if currency not in ref.spot_rates:
            currency = "USD"
        facility_amount = round(rng.uniform(100_000, 50_000_000 * PARENT_AMOUNT_SCALE), 2)

        business = [
            f"CMT-{_token(rng)}",
            rng.choice(external_cps),
            rng.choice(COMMITMENT_TYPES),
            facility_amount,
            round(facility_amount * rng.uniform(0.10, 0.90), 2),
            currency,
            _business_day_after(rng, report_date, 30, 720),
        ]
        rows.append(
            _assemble(
                "ods_off_bs_commitments",
                f"POFFBS-{index:06d}",
                PARENT_ENTITY,
                business,
                clock.next(),
                report_date,
            )
        )

    business_columns = [
        "commitment_id",
        "counterparty_id",
        "commitment_type",
        "facility_amount",
        "undrawn_amount",
        "currency",
        "maturity_date",
    ]
    return write_csv(ods_dir / "ods_off_bs_commitments.csv", _header(business_columns), rows, append=append)


def generate_intracompany_pairs(ods_dir: Path, ref: ReferenceData, report_date: date, append: bool = False) -> int:
    """集团内往来的成对腿：每对生成存款腿（母公司负债）和贷款腿（子公司资产）。

    两条腿的金额来自 INTRACOMPANY_PAIRS 里的同一个常量，因此天然相等。
    不是两边各抽一个随机数碰巧相等。

    存款腿：子公司在母公司的存款 → ods_deposits，entity_code=ENT001，
    customer_id = 该子公司的集团内对手方编号，customer_type_raw='AFFIL'。
    贷款腿：母公司对子公司的放款 → ods_loans，entity_code=该子公司，
    counterparty_id=CP9001（母公司的集团内对手方编号），loan_type='COMMERCIAL'。

    Returns:
        本次写入的行数（存款腿 + 贷款腿合计）。
    """
    rng = ods_rng("ods_intracompany", report_date)
    clock = EventClock(report_date)
    parent_cp = ref.affiliate_by_entity[PARENT_ENTITY]

    dep_rows: list[list[object]] = []
    loan_rows: list[list[object]] = []

    for pair_index, (parent_code, sub_code, amount) in enumerate(INTRACOMPANY_PAIRS):
        sub_cp = ref.affiliate_by_entity[sub_code]

        # 存款腿：母公司的负债（子公司在母公司存钱）
        dep_business = [
            f"ICA-DEP-{pair_index + 1:03d}",
            sub_cp,  # customer_id = 子公司的集团内对手方编号
            "CHK_INTRACOMPANY",
            "CHK",
            "USD",
            amount,
            0.0,
            0.0,
            _business_day_before(rng, report_date, 30, 365),
            "",
            f"BR-{parent_code}",
            "AFFIL",
            "N",
        ]
        dep_rows.append(
            _assemble("ods_deposits", f"ICA-D-{pair_index:06d}", parent_code, dep_business, clock.next(), report_date)
        )

        # 贷款腿：母公司对子公司放款（子公司的资产）
        loan_business = [
            f"ICA-LOAN-{pair_index + 1:03d}",
            parent_cp,  # counterparty_id = 母公司的集团内对手方编号
            "COMMERCIAL",
            amount,
            amount,
            0.0,
            "USD",
            round(rng.uniform(0.02, 0.08), 6),
            "FIXED",
            _business_day_before(rng, report_date, 90, 365),
            _business_day_after(rng, report_date, 180, 1800),
            _business_day_after(rng, report_date, 30, 90),
            "N",
            "FI",
            "AA",
        ]
        loan_rows.append(
            _assemble("ods_loans", f"ICA-L-{pair_index:06d}", sub_code, loan_business, clock.next(), report_date)
        )

    dep_columns = [
        "account_number",
        "customer_id",
        "product_code",
        "deposit_type",
        "currency",
        "principal_amount",
        "accrued_interest",
        "interest_rate",
        "open_date",
        "maturity_date",
        "branch_code",
        "customer_type_raw",
        "insured_flag",
    ]
    loan_columns = [
        "loan_id",
        "borrower_id",
        "loan_type",
        "facility_amount",
        "outstanding_amount",
        "undrawn_amount",
        "currency",
        "interest_rate",
        "rate_type",
        "origination_date",
        "maturity_date",
        "next_payment_date",
        "collateral_flag",
        "borrower_type_raw",
        "credit_grade_raw",
    ]
    dep_count = write_csv(ods_dir / "ods_deposits.csv", _header(dep_columns), dep_rows, append=append)
    loan_count = write_csv(ods_dir / "ods_loans.csv", _header(loan_columns), loan_rows, append=append)
    return dep_count + loan_count


# 6 张业务明细表的生成器函数（不含总账，总账在存款修正之后生成）
_BUSINESS_GENERATORS = [
    ("ods_deposits", generate_deposits),
    ("ods_repo_transactions", generate_repo_transactions),
    ("ods_loans", generate_loans),
    ("ods_securities", generate_securities),
    ("ods_derivatives", generate_derivatives),
    ("ods_off_bs_commitments", generate_off_bs_commitments),
]

# 母公司追加生成器：在子公司数据之后追加，不干扰既有行
_PARENT_GENERATORS = [
    ("ods_deposits", generate_parent_deposits),
    ("ods_repo_transactions", generate_parent_repo),
    ("ods_loans", generate_parent_loans),
    ("ods_securities", generate_parent_securities),
    ("ods_derivatives", generate_parent_derivatives),
    ("ods_off_bs_commitments", generate_parent_off_bs),
]


def generate_all(
    ods_dir: Path,
    ref: ReferenceData,
    gl_break_amount: float = 0.0,
    correction: tuple[str, float] | None = None,
    report_dates_list: list[date] | None = None,
) -> dict[str, int]:
    """生成全部 7 张 ODS 表，返回各表行数（各期行数之和）。

    生成顺序（每期）：
    1. 子公司业务明细（_BUSINESS_GENERATORS）—— 既有四家子公司，一行不改
    2. 母公司追加业务（_PARENT_GENERATORS）—— 对第三方的存款/贷款/证券/衍生品/表外
    3. 集团内往来配对腿（generate_intracompany_pairs）—— 存款腿 + 贷款腿
    4. 存款修正（如有）
    5. 总账逐实体倒推（generate_gl_balances）—— 每个 BOOKING_ENTITIES 各一本账

    多期生成：report_dates_list 给多个报告日时，按期顺序逐期生成各表。
    每期用 ods_rng(table_name, report_date) 派生独立随机源，
    保证加期不扰动已有期。返回的 counts 是各期合计。

    correction 给定时，先修正锚定报告日的单笔存款本金再倒推总账，保证总账与业务明细仍然自洽。

    Args:
        ods_dir: ODS 输出目录。
        ref: 引用数据。
        gl_break_amount: 总账故意少记的金额，落在 ENT002 的 2100 科目。
        correction: (record_id, new_amount) 或 None。
        report_dates_list: 报告日列表；None 时默认 [REPORT_DATE]
            （单期行为与改造前完全一致）。
    """
    if report_dates_list is None:
        report_dates_list = [REPORT_DATE]

    counts: dict[str, int] = {
        "ods_deposits": 0,
        "ods_repo_transactions": 0,
        "ods_loans": 0,
        "ods_securities": 0,
        "ods_derivatives": 0,
        "ods_gl_balances": 0,
        "ods_off_bs_commitments": 0,
    }

    # 逐期生成：第一期覆盖建文件，后续期追加数据行（表头只出现一次）
    for i, rd in enumerate(report_dates_list):
        append = i > 0
        # 1. 子公司业务明细
        for table_name, gen_func in _BUSINESS_GENERATORS:
            counts[table_name] += gen_func(ods_dir, ref, rd, append=append)
        # 2. 母公司追加业务（对第三方）
        for table_name, gen_func in _PARENT_GENERATORS:
            counts[table_name] += gen_func(ods_dir, ref, rd, append=True)
        # 3. 集团内往来配对腿（存款腿追加到 ods_deposits，贷款腿追加到 ods_loans）
        pairs = len(INTRACOMPANY_PAIRS)
        generate_intracompany_pairs(ods_dir, ref, rd, append=True)
        counts["ods_deposits"] += pairs  # 存款腿行数
        counts["ods_loans"] += pairs  # 贷款腿行数

    if correction is not None:
        apply_deposit_correction(ods_dir, correction[0], correction[1])

    # 总账在存款修正之后逐期生成，每期各自平衡
    for i, rd in enumerate(report_dates_list):
        counts["ods_gl_balances"] += generate_gl_balances(ods_dir, ref, rd, gl_break_amount, append=i > 0)

    return counts
