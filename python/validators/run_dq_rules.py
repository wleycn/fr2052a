"""数据质量引擎：把 ref 层声明的校验规则逐条执行，结果落审计表。

规则不写在代码里，而是从 `ref.ref_validation_rules` 读 —— 那是规则的唯一定义处。
本引擎只负责"把规则应用到具体表上并统计违规数"，应用范围（哪条规则管哪张表）
写在下面的 RULE_TARGETS 里，因为这部分是工程绑定，不是业务规则。

规则分两类：
  单表断言   形如 `principal_amount_usd >= 0`，可直接对表求违规行数 → 本引擎执行
  跨表比对   需要 join 或与报表比对 → 本引擎标记为 SKIPPED，
             由 python/lakehouse/verify_*.py 三个核对脚本覆盖（日批 DAG 两处都会跑）

结果写入 PostgreSQL 的 `ads.ads_fr2052a_validation_log`，并按批次追加，保留历史。

用法（Server 2，经 spark-submit 包装脚本执行）：
    bash spark-submit-fr2052a.sh \\
        /opt/fr2052a-app/python/validators/run_dq_rules.py --batch-id BATCH-20260916-001
"""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

RULES_TABLE = "ref.ref_validation_rules"
LOG_TABLE = "ads.ads_fr2052a_validation_log"

ALL_BRONZE = (
    "bronze.ods_deposits",
    "bronze.ods_repo_transactions",
    "bronze.ods_loans",
    "bronze.ods_securities",
    "bronze.ods_derivatives",
    "bronze.ods_gl_balances",
    "bronze.ods_off_bs_commitments",
)
ALL_OWD = (
    "silver.owd_deposits",
    "silver.owd_secured_financing",
    "silver.owd_loans",
    "silver.owd_securities",
    "silver.owd_derivatives",
    "silver.owd_off_bs",
    "silver.owd_gl_entries",
)

# 规则 → 应用到的表。VDQ-001 是结构性规则（表非空），在引擎里单独处理，不在此列；
# 未列出的规则按"跨表比对"处理，由核对脚本覆盖。
RULE_TARGETS: dict[str, tuple[str, ...]] = {
    "VDQ-002": ALL_BRONZE,  # 关键字段非空
    "VDQ-003": ("silver.owd_deposits",),
    "VDQ-004": ("silver.owd_deposits", "silver.owd_secured_financing", "silver.owd_loans"),
    "VDQ-005": ALL_OWD,
    "VDQ-006": ("silver.owd_loans",),
    "VDQ-007": ("silver.owd_secured_financing",),
    "VDQ-008": (
        "silver.owd_deposits",
        "silver.owd_loans",
        "silver.owd_securities",
        "silver.owd_derivatives",
        "silver.owd_off_bs",
    ),
    "VDQ-016": ALL_BRONZE,  # T+1 加载时效：入湖时间戳不得晚于报告日 +1 天 08:00
    "VDQ-017": ("gold.ads_fr2052a_report",),
    "VDQ-018": ("gold.ads_fr2052a_report",),
    "VDQ-019": ("gold.ads_fr2052a_report",),
    "VDQ-020": ("ref.ref_counterparty",),  # LEI 格式：主数据质量
}

# 需要跨表比对、无法用单表断言表达的规则，及其覆盖位置
CROSS_TABLE_RULES: dict[str, str] = {
    "VDQ-009": "汇率折算精度：verify_silver.py 逐行重算覆盖",
    "VDQ-010": "汇总等于明细求和：verify_gold.py 明细回溯覆盖",
    "VDQ-011": "非受限资产不超过总资产：verify_gold.py 覆盖",
    "VDQ-012": "已质押不超过总市值：verify_gold.py 覆盖",
    "VDQ-013": "Section 合计等于行项目合计：verify_gold.py 明细回溯覆盖",
    "VDQ-014": "总融资与资产负债表偏差：GL 对账模型覆盖",
    "VDQ-015": "环比波动：需要上一期报表，本演示只跑单期",
}


@dataclass
class RuleResult:
    rule_id: str
    description: str
    category: str
    severity: str
    apply_layer: str
    check_result: str
    detail: str
    violations: int


def evaluate_rule(
    spark: SparkSession, rule_id: str, expression: str, targets: tuple[str, ...]
) -> tuple[int, list[str], int]:
    """对规则涉及的表统计违规行数。

    返回 (违规总数, 明细, 实际参与评估的表数)。

    某张表若不含规则表达式引用的列，Spark 会报 UNRESOLVED_COLUMN —— 这说明这条规则
    对该表不适用（例如 `LENGTH(currency) = 3` 遇到总账表，列名是 currency_code），
    此时跳过该表而不是让整批失败。用 Spark 的报错来判断，比在代码里用正则猜列名可靠。
    """
    total = 0
    details: list[str] = []
    evaluated = 0
    for table in targets:
        try:
            violations = spark.sql(f"select count(*) as c from {table} where not ({expression})").collect()[0]["c"]
        except Exception as error:  # noqa: BLE001 - 只吞"列不存在"，其余错误照常抛出
            message = str(error)
            if "UNRESOLVED_COLUMN" in message or "cannot be resolved" in message:
                continue
            raise
        evaluated += 1
        total += violations
        if violations:
            details.append(f"{table}:{violations} 行违规")
    return total, details, evaluated


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="执行 FR 2052a 数据质量规则")
    parser.add_argument("--batch-id", default="UNKNOWN", help="批次号，写入审计表用于追溯")
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv[1:])
    spark = SparkSession.builder.appName("fr2052a-dq-rules").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    rules = spark.table(RULES_TABLE).filter("is_active").collect()
    print(f"从 {RULES_TABLE} 读到 {len(rules)} 条启用规则")

    results: list[RuleResult] = []
    for rule in rules:
        rule_id = rule["rule_id"]
        if rule_id == "VDQ-001":
            # 文件完整到达是结构性规则：判据是"表里有数据"，不是列断言，
            # 因此不能把 row_count > 0 当表达式去解析列名
            empty = [table for table in ALL_BRONZE if spark.table(table).count() == 0]
            violations = len(empty)
            details = [f"{table} 无数据" for table in empty]
            passed = violations == 0
        elif rule_id in RULE_TARGETS:
            violations, details, evaluated = evaluate_rule(spark, rule_id, rule["sql_expression"], RULE_TARGETS[rule_id])
            if evaluated == 0:
                # 规则引用的列在所有目标表里都不存在 → 这条规则对本层不适用
                results.append(
                    RuleResult(
                        rule_id=rule_id,
                        description=rule["rule_name"],
                        category=rule["rule_category"],
                        severity=rule["severity"],
                        apply_layer=rule["apply_layer"],
                        check_result="SKIPPED",
                        detail="目标表中不存在该规则引用的列",
                        violations=0,
                    )
                )
                continue
            passed = violations == 0
        else:
            reason = CROSS_TABLE_RULES.get(rule_id, "未绑定应用表")
            results.append(
                RuleResult(
                    rule_id=rule_id,
                    description=rule["rule_name"],
                    category=rule["rule_category"],
                    severity=rule["severity"],
                    apply_layer=rule["apply_layer"],
                    check_result="SKIPPED",
                    detail=reason,
                    violations=0,
                )
            )
            continue

        results.append(
            RuleResult(
                rule_id=rule_id,
                description=rule["rule_name"],
                category=rule["rule_category"],
                severity=rule["severity"],
                apply_layer=rule["apply_layer"],
                check_result="PASS" if passed else "FAIL",
                detail="；".join(details) if details else "无违规",
                violations=violations,
            )
        )

    for result in results:
        mark = {"PASS": "PASS", "FAIL": "FAIL", "SKIPPED": "SKIP"}[result.check_result]
        print(f"  [{mark}] {result.rule_id} {result.description:<28} {result.detail}")

    # 结果写 PostgreSQL 审计表（追加，保留历史）
    url = f"jdbc:postgresql://{os.environ['SERVER1_HOST']}:5432/{os.environ['POSTGRES_DB']}"
    properties = {
        "user": os.environ["POSTGRES_USER"],
        "password": os.environ["POSTGRES_PASSWORD"],
        "driver": "org.postgresql.Driver",
    }
    log_frame = spark.createDataFrame(
        [
            (
                args.batch_id,
                result.rule_id,
                result.description,
                result.category,
                result.severity,
                result.check_result,
                str(result.violations),
                "0",
                result.detail,
                result.apply_layer,
                None,
            )
            for result in results
        ],
        "batch_id string, validation_rule_id string, rule_description string, rule_category string, "
        "severity string, check_result string, actual_value string, expected_value string, "
        "detail string, apply_layer string, affected_line_item string",
    ).withColumn("created_at", F.current_timestamp())

    log_frame.write.jdbc(url, LOG_TABLE, mode="append", properties=properties)
    written = spark.read.jdbc(url, LOG_TABLE, properties=properties).count()

    failures = [result for result in results if result.check_result == "FAIL"]
    errors = [result for result in failures if result.severity == "ERROR"]
    skipped = [result for result in results if result.check_result == "SKIPPED"]

    print()
    print(f"规则执行：PASS {len(results) - len(failures) - len(skipped)}，FAIL {len(failures)}，"
          f"SKIPPED {len(skipped)}")
    print(f"审计表 {LOG_TABLE} 现有 {written} 行（批次 {args.batch_id} 已追加）")

    if errors:
        print(f"ERROR 级违规：{[result.rule_id for result in errors]}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
