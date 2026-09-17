r"""核对数据湖表结构与生成器产出的 CSV 表头是否逐列一致。

这是一道防漂移的机器闸。CSV 是数据的源头，表结构必须与它对齐；一旦错位，
入湖时会静默丢列或串列，而且不会报错。

规则：
  1. CSV 表头必须与表列**同序同名**地构成前缀；
  2. 表里多出来的列只允许是 loader 侧补齐的列（白名单）。

用法（Server 2，经 spark-submit 包装脚本执行）：
    bash spark-submit-fr2052a.sh \
        /opt/fr2052a-app/python/lakehouse/verify_ods_schema.py \
        /opt/fr2052a-app/sample_data
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path

from pyspark.sql import SparkSession

# 入湖作业在写入时补齐、因此不出现在 CSV 表头里的列，按层区分：
# ref 是批加载的静态字典表，没有入湖时间戳；bronze 的 ODS 表由作业写入时补 etl_load_timestamp。
LOADER_MANAGED_COLUMNS: dict[str, frozenset[str]] = {
    "ref": frozenset(),
    "bronze": frozenset({"etl_load_timestamp"}),
}

LAYOUT = (("ref", "ref"), ("bronze", "ods"))


def read_csv_header(path: Path) -> list[str]:
    """读出 CSV 表头，作为表结构比对的基准。"""
    with path.open(newline="", encoding="utf-8") as handle:
        return next(csv.reader(handle))


def verify_table(spark: SparkSession, namespace: str, table: str, csv_path: Path) -> list[str]:
    """返回该表的问题列表，空列表表示通过。"""
    expected = read_csv_header(csv_path)
    actual = [field.name for field in spark.table(table).schema.fields]
    loader_columns = LOADER_MANAGED_COLUMNS[namespace]
    problems: list[str] = []

    if actual[: len(expected)] != expected:
        problems.append(f"表头与表列不一致：CSV {expected} vs 表前 {len(expected)} 列 {actual[: len(expected)]}")

    unexpected = set(actual[len(expected) :]) - loader_columns
    if unexpected:
        problems.append(f"表里出现未经声明的多余列：{sorted(unexpected)}")

    missing = loader_columns - set(actual)
    if missing:
        problems.append(f"表里缺少 loader 侧列：{sorted(missing)}")

    return problems


def main(argv: list[str]) -> int:
    """把湖表的列名与列序跟生成器产出的 CSV 表头逐列比对。"""
    data_root = Path(argv[1]) if len(argv) > 1 else Path("/opt/fr2052a-app/sample_data")
    if not data_root.is_dir():
        print(f"样本数据目录不存在: {data_root}", file=sys.stderr)
        return 2

    spark = SparkSession.builder.appName("fr2052a-verify-schema").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    checked = 0
    failed: list[str] = []
    for namespace, subdir in LAYOUT:
        for csv_path in sorted((data_root / subdir).glob("*.csv")):
            table = f"{namespace}.{csv_path.stem}"
            checked += 1
            try:
                problems = verify_table(spark, namespace, table, csv_path)
            except Exception as error:  # noqa: BLE001 - 表不存在也要算作失败项
                problems = [f"读取表结构失败：{type(error).__name__}: {error}"]

            if problems:
                failed.append(table)
                print(f"  [FAIL] {table}")
                for problem in problems:
                    print(f"         {problem}")
            else:
                print(f"  [PASS] {table}")

    print()
    print(f"共核对 {checked} 张表，失败 {len(failed)} 张")
    if failed:
        print(f"失败清单：{failed}")
        return 1
    print("表结构与 CSV 表头全部一致")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
