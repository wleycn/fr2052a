r"""核对 bronze 层行数与样本 CSV 行数是否一致。

入湖是"看得见才敢用"的一步：CSV 有多少行，bronze 就必须有多少行，
多一行少一行都要在进 OWD 之前暴露出来。

用法（Server 2，经 spark-submit 包装脚本执行）：
    bash spark-submit-fr2052a.sh \
        /opt/fr2052a-app/python/lakehouse/verify_bronze.py \
        --data-dir /opt/fr2052a-app/sample_data/ods \
        --config   /opt/fr2052a-app/config/pipeline_topics.json
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

from pyspark.sql import SparkSession

DEFAULT_DATA_DIR = Path("/opt/fr2052a-app/sample_data/ods")
DEFAULT_CONFIG = Path("/opt/fr2052a-app/config/pipeline_topics.json")


def count_csv_rows(path: Path) -> int:
    """数 CSV 的数据行数，不含表头。"""
    with path.open(newline="", encoding="utf-8") as handle:
        return sum(1 for _ in csv.DictReader(handle))


def parse_args(argv: list[str]) -> argparse.Namespace:
    """解析命令行参数。"""
    parser = argparse.ArgumentParser(description="核对 bronze 层与样本 CSV 的行数")
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    """核对 bronze 层每张表的行数与样本 CSV 是否一致。"""
    args = parse_args(argv[1:])
    config = json.loads(args.config.read_text(encoding="utf-8"))
    targets = [topic for topic in config["topics"] if topic.get("has_producer")]

    if not targets:
        print("核对对象为空：配置里没有声明生产者的主题", file=sys.stderr)
        return 1

    spark = SparkSession.builder.appName("fr2052a-verify-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    mismatch: list[str] = []
    csv_total = 0
    bronze_total = 0
    for target in targets:
        table = target["target_table"]
        csv_path = args.data_dir / f"{table.split('.')[-1]}.csv"
        if not csv_path.is_file():
            mismatch.append(table)
            print(f"  [FAIL] {table:<34} 源文件不存在：{csv_path}")
            continue
        expected = count_csv_rows(csv_path)
        actual = spark.table(table).count()
        csv_total += expected
        bronze_total += actual

        mark = "PASS" if expected == actual else "FAIL"
        if expected != actual:
            mismatch.append(table)
        print(f"  [{mark}] {table:<34} CSV {expected:>5} 行，bronze {actual:>5} 行")

    print()
    print(f"核对对象 {len(targets)} 张表")
    print(f"合计：CSV {csv_total} 行，bronze {bronze_total} 行")
    if mismatch:
        print(f"不一致的表：{mismatch}")
        return 1
    print("bronze 层与样本数据行数一致")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
