"""把 Iceberg gold 层的报送报表导出到 Server 1 的 PostgreSQL（ADS 服务层）。

为什么需要这一步：架构上 Iceberg 承载数据湖（ref / ODS / OWD / OWS / Gold），
PostgreSQL 只承载报送服务层。dbt 在数据湖里算出报表，再由本作业导出到关系库，
供报送服务与下游查询使用。

PG 侧表结构由 Spark 按 gold 模型的结构自动创建（overwrite 模式），不手写一份 DDL ——
两处各写一份结构定义必然漂移。

导出后逐表回读行数，作为对"确实写进去了"的独立验证。

用法（Server 2，经 spark-submit 包装脚本执行，环境变量由包装脚本透传）：
    bash spark-submit-fr2052a.sh \\
        /opt/fr2052a-app/python/exporters/export_gold_to_pg.py
"""

from __future__ import annotations

import os
import sys

from pyspark.sql import SparkSession

GOLD_SCHEMA = "gold"
ADS_SCHEMA = "ads"

TABLES = (
    "ads_fr2052a_report",
    "ads_fr2052a_detail",
    "ads_gl_reconciliation",
)


def jdbc_url() -> str:
    host = os.environ["SERVER1_HOST"]
    database = os.environ["POSTGRES_DB"]
    return f"jdbc:postgresql://{host}:5432/{database}"


def connection_properties() -> dict[str, str]:
    return {
        "user": os.environ["POSTGRES_USER"],
        "password": os.environ["POSTGRES_PASSWORD"],
        "driver": "org.postgresql.Driver",
    }


def main() -> int:
    url = jdbc_url()
    properties = connection_properties()

    spark = SparkSession.builder.appName("fr2052a-export-gold").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print(f"导出 gold 层报表到 PostgreSQL（{url.split('@')[-1]}）")
    failures: list[str] = []
    for table in TABLES:
        target = f"{ADS_SCHEMA}.{table}"
        try:
            frame = spark.table(f"{GOLD_SCHEMA}.{table}")
            source_rows = frame.count()
            frame.write.jdbc(url, target, mode="overwrite", properties=properties)
            # 独立回读：写成功不等于写对了
            written_rows = spark.read.jdbc(url, target, properties=properties).count()
            ok = source_rows == written_rows
            if not ok:
                failures.append(target)
            print(f"  [{'OK' if ok else 'FAIL'}] {target:<28} gold {source_rows} 行，PG 回读 {written_rows} 行")
        except Exception as error:  # noqa: BLE001 - 逐表失败不中断整批，最后统一汇总
            failures.append(target)
            print(f"  [FAIL] {target:<28} {type(error).__name__}: {error}")

    print()
    if failures:
        print(f"导出失败：{failures}")
        return 1
    print(f"导出完成：{len(TABLES)} 张表全部回读一致")
    return 0


if __name__ == "__main__":
    sys.exit(main())
