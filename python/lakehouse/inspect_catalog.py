"""巡检数据湖目录：列出命名空间与各命名空间下的表。

用途：确认建表结果，以及排查"表建了但列不出来"这类元数据可见性问题。

用法（Server 2，经 spark-submit 包装脚本执行）：
    bash spark-submit-fr2052a.sh /opt/fr2052a-app/python/lakehouse/inspect_catalog.py
"""

from __future__ import annotations

import sys

from pyspark.sql import SparkSession

NAMESPACES = ("ref", "bronze", "silver")
SPOT_CHECK_TABLE = "ref.ref_calendar"


def main() -> int:
    spark = SparkSession.builder.appName("fr2052a-inspect-catalog").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

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
