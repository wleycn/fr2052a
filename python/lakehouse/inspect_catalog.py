# [AI-GENERATED] model=deepseek-flash date=2026-09-17 reviewed_by=pending
"""巡检数据湖目录：列出命名空间与各命名空间下的表。

用途：确认建表结果，以及排查"表建了但列不出来"这类元数据可见性问题。

用法（Server 2，经 spark-submit 包装脚本执行）：
    bash spark-submit-fr2052a.sh /opt/fr2052a-app/python/lakehouse/inspect_catalog.py
    bash spark-submit-fr2052a.sh .../inspect_catalog.py --describe silver.owd_deposits
"""

from __future__ import annotations

import argparse
import sys

from pyspark.sql import SparkSession

NAMESPACES = ("ref", "bronze", "silver")
SPOT_CHECK_TABLE = "ref.ref_calendar"


def parse_args() -> argparse.Namespace:
    """解析命令行参数。"""
    parser = argparse.ArgumentParser(description="巡检数据湖目录")
    parser.add_argument(
        "--describe",
        nargs="*",
        default=None,
        help="只打印指定表的列清单，如 --describe silver.owd_deposits",
    )
    parser.add_argument(
        "--sample",
        type=int,
        default=0,
        help="配合 --describe，额外打印前 N 行（用于肉眼确认脱敏等形态）",
    )
    return parser.parse_args()


def describe_tables(spark: SparkSession, tables: list[str], sample: int = 0) -> None:
    """打印每张表的列结构与若干样例行，供人工巡检。"""
    for table in tables:
        try:
            columns = spark.table(table).columns
        except Exception as error:  # noqa: BLE001 - 探测用，失败要如实打印
            print(f"{table} 读不到结构：{type(error).__name__}: {error}")
            continue
        print(f"\n{table}（{len(columns)} 列）")
        for index, name in enumerate(columns, start=1):
            print(f"  {index:>3}. {name}")
        try:
            print(f"  行数：{spark.table(table).count()}")
        except Exception as error:  # noqa: BLE001
            print(f"  行数读取失败：{type(error).__name__}: {error}")
        if sample > 0:
            try:
                for row in spark.table(table).limit(sample).collect():
                    print(f"  样本：{row.asDict()}")
            except Exception as error:  # noqa: BLE001
                print(f"  取样失败：{type(error).__name__}: {error}")


def main() -> int:
    """列出命名空间与表，或按 --describe 细看某张表。"""
    args = parse_args()
    spark = SparkSession.builder.appName("fr2052a-inspect-catalog").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    if args.describe is not None:
        describe_tables(spark, args.describe, args.sample)
        return 0

    print(f"当前 catalog: {spark.catalog.currentCatalog()}    当前 database: {spark.catalog.currentDatabase()}")

    shown = sorted(row[0] for row in spark.sql("SHOW NAMESPACES").collect())
    print(f"SHOW NAMESPACES: {shown}")

    for namespace in NAMESPACES:
        try:
            via_sql = sorted(row[1] for row in spark.sql(f"SHOW TABLES IN {namespace}").collect())
        except Exception as error:  # noqa: BLE001 - 探测用，任何失败都要如实打印
            via_sql = [f"<{type(error).__name__}: {error}>"]
        try:
            via_api = sorted(table.name for table in spark.catalog.listTables(namespace))
        except Exception as error:  # noqa: BLE001
            via_api = [f"<{type(error).__name__}: {error}>"]

        print(f"\n命名空间 {namespace}")
        print(f"  SHOW TABLES     : {len(via_sql)} 张 {via_sql}")
        print(f"  listTables API  : {len(via_api)} 张 {via_api}")

    try:
        count = spark.sql(f"SELECT COUNT(*) AS c FROM {SPOT_CHECK_TABLE}").collect()[0]["c"]
        print(f"\n抽检 {SPOT_CHECK_TABLE}: 当前 {count} 行")
    except Exception as error:  # noqa: BLE001
        print(f"\n抽检 {SPOT_CHECK_TABLE} 失败: {type(error).__name__}: {error}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
