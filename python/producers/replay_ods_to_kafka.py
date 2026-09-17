"""把 ODS 样本 CSV 重放进 Kafka，模拟各源系统的实时上报。

为什么用 Spark 而不是 Python Kafka 客户端：消费侧本来就要 Spark（Structured Streaming），
复用同一套 Kafka 连接器 jar 即可，不必在服务器上再维护一套 Python 客户端依赖。

按 config/pipeline_topics.json 的声明逐主题重放：一张 ODS 表 → 一个主题。
消息一律按字符串写入，类型转换统一在入湖侧做（与 ref 批加载同一套规则）。

用法（Server 2，经 spark-submit 包装脚本执行）：
    bash spark-submit-fr2052a.sh \\
        /opt/fr2052a-app/python/producers/replay_ods_to_kafka.py \\
        --data-dir /opt/fr2052a-app/sample_data/ods \\
        --config   /opt/fr2052a-app/config/pipeline_topics.json
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

DEFAULT_DATA_DIR = Path("/opt/fr2052a-app/sample_data/ods")
DEFAULT_CONFIG = Path("/opt/fr2052a-app/config/pipeline_topics.json")


def load_config(config_path: Path) -> dict:
    return json.loads(config_path.read_text(encoding="utf-8"))


def csv_path_for(data_dir: Path, target_table: str) -> Path:
    """bronze.ods_deposits → <data_dir>/ods_deposits.csv"""
    return data_dir / f"{target_table.split('.')[-1]}.csv"


def build_messages(spark: SparkSession, csv_path: Path) -> DataFrame:
    """把 CSV 读成 Kafka 消息：key 取主键，value 取整行 JSON。"""
    frame = spark.read.csv(str(csv_path), header=True, inferSchema=False)
    return frame.select(
        F.col("source_record_id").alias("key"),
        F.to_json(F.struct(*[F.col(column) for column in frame.columns])).alias("value"),
    )


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="重放 ODS 样本数据到 Kafka")
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv[1:])
    config = load_config(args.config)
    bootstrap_servers = config["kafka"]["bootstrap_servers_internal"]
    targets = [topic for topic in config["topics"] if topic.get("has_producer")]

    spark = SparkSession.builder.appName("fr2052a-replay-ods").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print(f"重放 {len(targets)} 张 ODS 表到 Kafka ({bootstrap_servers})")
    produced: dict[str, int] = {}
    failures: list[str] = []
    for target in targets:
        topic = target["name"]
        csv_path = csv_path_for(args.data_dir, target["target_table"])
        if not csv_path.is_file():
            failures.append(topic)
            print(f"  [FAIL] {topic:<22} 缺少源文件 {csv_path}")
            continue

        try:
            messages = build_messages(spark, csv_path)
            row_count = messages.count()
            (
                messages.write.format("kafka")
                .option("kafka.bootstrap.servers", bootstrap_servers)
                .option("topic", topic)
                .save()
            )
            produced[topic] = row_count
            print(f"  [OK]   {topic:<22} {row_count} 条")
        except Exception as error:  # noqa: BLE001 - 单主题失败不中断整批，最后统一汇总
            failures.append(topic)
            print(f"  [FAIL] {topic:<22} {type(error).__name__}: {error}")

    print()
    print(f"重放完成：成功 {len(produced)} 个主题、共 {sum(produced.values())} 条消息，失败 {len(failures)} 个")
    if failures:
        print(f"失败清单：{failures}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
