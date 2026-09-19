# [AI-GENERATED] model=deepseek-flash date=2026-09-17 reviewed_by=pending
r"""把 REF 层 CSV 批量加载进 Iceberg 的 ref 命名空间。

REF 是静态字典表，量小、变动少，用整表覆盖写（INSERT OVERWRITE）保证幂等：
重复跑任意多次，结果一致，不会翻倍。

用法（Server 2，经 spark-submit 包装脚本执行）：
    bash spark-submit-fr2052a.sh \
        /opt/fr2052a-app/python/lakehouse/load_ref_tables.py \
        /opt/fr2052a-app/sample_data/ref
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import TYPE_CHECKING

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

if TYPE_CHECKING:  # pyspark 只在 Server 2 上装，dev 侧静态检查时用类型占位
    from pyspark.sql import DataFrame

NAMESPACE = "ref"


def align_columns(spark: SparkSession, csv_path: Path, table: str) -> DataFrame:
    """读 CSV 成 DataFrame，把列名、列序与列类型一并对齐到目标表。

    不按列做特殊处理：CSV 是文本载体，Spark 的类型推断并不可靠 —— 整列全空时
    （例如 ref_entity_hierarchy 的 expiry_date）会被推成 STRING，写入 DATE 列直接报
    CANNOT_SAFELY_CAST。因此统一以目标表结构为准逐列 cast，谁不匹配就转谁。
    """
    frame = spark.read.csv(str(csv_path), header=True, inferSchema=True, nullValue="")
    target_schema = spark.table(table).schema

    missing = [field.name for field in target_schema.fields if field.name not in frame.columns]
    if missing:
        raise ValueError(f"{csv_path.name} 缺少目标表 {table} 的列：{missing}")

    projected = []
    for field in target_schema.fields:
        column = F.col(field.name)
        if frame.schema[field.name].dataType != field.dataType:
            column = column.cast(field.dataType)
        projected.append(column.alias(field.name))

    return frame.select(*projected)


def main(argv: list[str]) -> int:
    """把 ref 目录下每张 CSV 覆盖写入 Iceberg 的 ref 命名空间。"""
    ref_dir = Path(argv[1]) if len(argv) > 1 else Path("/opt/fr2052a-app/sample_data/ref")
    if not ref_dir.is_dir():
        print(f"REF 目录不存在: {ref_dir}", file=sys.stderr)
        return 2

    spark = SparkSession.builder.appName("fr2052a-load-ref").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {NAMESPACE}")

    csv_files = sorted(ref_dir.glob("*.csv"))
    if not csv_files:
        print(f"REF 目录下没有 CSV：{ref_dir}", file=sys.stderr)
        return 1

    loaded: dict[str, int] = {}
    failures: list[str] = []
    for csv_path in csv_files:
        table = f"{NAMESPACE}.{csv_path.stem}"
        try:
            frame = align_columns(spark, csv_path, table)
            frame.writeTo(table).overwritePartitions()
            rows = spark.table(table).count()
            loaded[table] = rows
            print(f"  [OK]   {table:<34} {rows} 行")
        except Exception as error:  # noqa: BLE001 - 逐表失败不要中断整批，最后统一汇总
            failures.append(table)
            print(f"  [FAIL] {table:<34} {type(error).__name__}: {error}")

    print()
    print(f"加载完成：成功 {len(loaded)} 张，失败 {len(failures)} 张")
    if failures:
        print(f"失败清单：{failures}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
