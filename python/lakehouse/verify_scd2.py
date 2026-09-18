# [AI-GENERATED] model=deepseek-flash date=2026-09-17 reviewed_by=pending
"""核对 OWD 版本历史（SCD2）—— 版本区间语义必须机器可查，不能靠人看。

七项检查，每项都对应一条「一旦不成立，下游就会被骗」的约束：

    1. 业务列与 OWD 当前结构一致      历史表少列 = 那次变更的内容没被记录
    2. 当前有效版本的 end_date 为空    空值就是「有效」的表达方式，这里错则状态判据全错
    3. end_date 非空的版本 is_active 必须为假   冗余列与区间不能分叉
    4. 每个业务键最多一条有效版本      两条有效 = 「现在是什么」有两个答案
    5. record_version 从 1 起且不跳号   跳号说明有版本被删除或写入被跳过
    6. (业务键, record_version) 不重复  重复 = 同一版本写了两遍
    7. 每条记录都在「有效」或「已失效」中恰好占一边   两边都不占 = 悬空版本

运行（Server 2，经 spark-submit 包装脚本执行）：
    bash spark-submit-fr2052a.sh /opt/fr2052a-app/python/lakehouse/verify_scd2.py
"""

from __future__ import annotations

import sys

from pyspark.sql import SparkSession

OWD_TABLES = (
    "owd_deposits",
    "owd_secured_financing",
    "owd_loans",
    "owd_securities",
    "owd_derivatives",
    "owd_off_bs",
    "owd_gl_entries",
    "owd_treasury_cash_position",
)

KEY_COLUMNS = ("source_system", "source_record_id", "report_date")
VERSION_COLUMNS = ("begin_date", "end_date", "is_active", "last_modified_reason", "record_version", "row_hash")


def check_table(spark: SparkSession, table: str) -> list[str]:
    """返回该表的失败项列表（空表示全过）。"""
    failures: list[str] = []
    odb = f"silver.{table}"
    history = f"silver.{table}_history"

    live_columns = spark.table(odb).columns
    history_columns = spark.table(history).columns
    version_set = set(VERSION_COLUMNS)
    history_business = [name for name in history_columns if name not in version_set]
    # 按集合比较，不按列表顺序比较：列顺序对 SCD2 语义没有影响。
    # 历史表新增列是 ALTER TABLE ADD COLUMN 追在末尾，与 OWD 模型里的位置不同，
    # 用有序列表比较会把这种情况误判成「业务列不一致」，且差异清单是空的 —— 看的人无从下手。
    if set(history_business) != set(live_columns):
        missing = [name for name in live_columns if name not in history_business]
        extra = [name for name in history_business if name not in live_columns]
        failures.append(f"业务列与 {odb} 不一致（缺 {missing}，多 {extra}）")

    key_text = ", ".join(KEY_COLUMNS)
    spark.sql(
        f"""
        SELECT {key_text}, begin_date, end_date, is_active, record_version
        FROM {history}
        """
    ).createOrReplaceTempView("history_rows")

    total = spark.sql("SELECT count(*) AS n FROM history_rows").first()["n"]

    active_with_end = spark.sql(
        "SELECT count(*) AS n FROM history_rows WHERE is_active AND end_date IS NOT NULL"
    ).first()["n"]
    if active_with_end:
        failures.append(f"有效版本带失效日：{active_with_end} 行（有效版本的 end_date 必须为空）")

    inactive_without_end = spark.sql(
        "SELECT count(*) AS n FROM history_rows WHERE NOT is_active AND end_date IS NULL"
    ).first()["n"]
    if inactive_without_end:
        failures.append(f"失效版本没有失效日：{inactive_without_end} 行（冗余列与区间分叉）")

    duplicate_active = spark.sql(
        f"""
        SELECT count(*) AS n FROM (
            SELECT {key_text}, count(*) AS c FROM history_rows WHERE is_active
            GROUP BY {key_text} HAVING count(*) > 1
        )
        """
    ).first()["n"]
    if duplicate_active:
        failures.append(f"同一业务键有多条有效版本：{duplicate_active} 个键")

    duplicate_version = spark.sql(
        f"""
        SELECT count(*) AS n FROM (
            SELECT {key_text}, record_version, count(*) AS c FROM history_rows
            GROUP BY {key_text}, record_version HAVING count(*) > 1
        )
        """
    ).first()["n"]
    if duplicate_version:
        failures.append(f"(业务键, 版本号) 重复：{duplicate_version} 组")

    version_gaps = spark.sql(
        f"""
        SELECT count(*) AS n FROM (
            SELECT {key_text}, max(record_version) AS max_version, count(*) AS versions
            FROM history_rows GROUP BY {key_text}
        ) WHERE max_version <> versions
        """
    ).first()["n"]
    if version_gaps:
        failures.append(f"版本号不连续（最大版本号 ≠ 版本条数）：{version_gaps} 个键")

    no_begin = spark.sql("SELECT count(*) AS n FROM history_rows WHERE begin_date IS NULL").first()["n"]
    if no_begin:
        failures.append(f"缺生效日：{no_begin} 行")

    # 区间方向必须正确：失效版本的失效日不得早于生效日。
    # 校验这一条是因为「用报告日当区间边界」会算出 begin > end 的反向区间 ——
    # 反向区间不会报错，只会让按区间过滤的查询悄悄漏掉数据。
    reversed_interval = spark.sql(
        "SELECT count(*) AS n FROM history_rows WHERE end_date IS NOT NULL AND end_date < begin_date"
    ).first()["n"]
    if reversed_interval:
        failures.append(f"区间反向（失效日早于生效日）：{reversed_interval} 行")

    if total and not failures:
        print(f"  [OK]   {table:<24} {total} 条版本，全部约束成立")
    elif total == 0:
        failures.append("历史表为空（版本化作业没有写入过）")
    return failures


def main() -> int:
    """核对历史表的 SCD2 不变式：区间、唯一性、版本连续。"""
    spark = SparkSession.builder.appName("fr2052a-verify-scd2").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print("核对 OWD 版本历史（SCD2 语义）")
    all_failures: list[str] = []
    for table in OWD_TABLES:
        try:
            failures = check_table(spark, table)
        except Exception as error:  # noqa: BLE001 - 单表失败不中断，末尾统一汇总
            failures = [f"检查失败：{type(error).__name__}: {error}"]
        for item in failures:
            print(f"  [FAIL] {table:<24} {item}")
        all_failures.extend(f"{table}: {item}" for item in failures)

    print()
    if all_failures:
        print(f"未通过 {len(all_failures)} 项：")
        for item in all_failures:
            print(f"  - {item}")
        return 1
    print(f"完成：{len(OWD_TABLES)} 张历史表全部通过（END_DATE 为空 ⇔ 当前有效）")
    return 0


if __name__ == "__main__":
    sys.exit(main())
