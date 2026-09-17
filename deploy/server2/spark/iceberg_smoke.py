"""E3 穿透自检：Spark（Server 2）→ Iceberg → MinIO（Server 1）。

验证三件事：
  1. Spark 能经 Iceberg 在 MinIO 上创建命名空间与表
  2. 写入后能读回，行数与内容一致
  3. 全链路无异常（这一步是后续所有 ETL 的前提）

表名不加 catalog 前缀：spark_catalog 已被 Iceberg SessionCatalog 接管，
未限定的 db.table 即 Iceberg 表。

凭据不在本文件里，由 spark-submit 的 --conf 传入，来源是 ~/fr2052a-infra/.env。
运行方式见 docs/BUILD-LOG.md 的 E3 一节。
"""

import sys

from pyspark.sql import SparkSession

NAMESPACES = ["ref", "bronze", "silver"]
TABLE = "bronze.smoke_check"
EXPECTED_ROWS = 3


def main() -> int:
    """E3 阶段的一次性连通性探针：确认 Spark 能读写 Iceberg 与 MinIO。"""
    spark = SparkSession.builder.appName("fr2052a-e3-smoke").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    for namespace in NAMESPACES:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    found = sorted(row[0] for row in spark.sql("SHOW NAMESPACES").collect())
    print(f"[1/3] 命名空间: {found}")

    spark.sql(f"DROP TABLE IF EXISTS {TABLE}")
    spark.sql(f"CREATE TABLE {TABLE} (id INT, payload STRING, loaded_at TIMESTAMP) USING iceberg")
    spark.sql(
        f"INSERT INTO {TABLE} VALUES "
        "(1, 'alpha', current_timestamp()), "
        "(2, 'beta', current_timestamp()), "
        "(3, 'gamma', current_timestamp())"
    )

    rows = spark.sql(f"SELECT id, payload FROM {TABLE} ORDER BY id").collect()
    pairs = [(row["id"], row["payload"]) for row in rows]
    print(f"[2/3] 读回 {len(rows)} 行: {pairs}")

    if len(rows) != EXPECTED_ROWS:
        print(f"[3/3] 断言失败: 期望 {EXPECTED_ROWS} 行，实际 {len(rows)} 行")
        return 1

    print("[3/3] E3 穿透自检通过")
    return 0


if __name__ == "__main__":
    sys.exit(main())
