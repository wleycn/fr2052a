"""把 Kafka 各主题的 ODS 消息流入 Iceberg bronze 层。

投递语义：Kafka 是至少一次，因此这里按主键做 MERGE 去重
（source_system + source_record_id），重复消息不会把 bronze 层写重。
这样重放生产者、重跑消费者都是幂等的，不需要删主题或清偏移量。

批次切分：用 trigger(availableNow=True)，把积压消息一口气消费完即退出，
行为可预期、可验证；改成连续消费只需去掉这个 trigger。

用法（Server 2，经 spark-submit 包装脚本执行）：
    bash spark-submit-fr2052a.sh \\
        /opt/fr2052a-app/python/consumers/kafka_to_iceberg.py \\
        --config /opt/fr2052a-app/config/pipeline_topics.json
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

DEFAULT_CONFIG = Path("/opt/fr2052a-app/config/pipeline_topics.json")
CHECKPOINT_ROOT = "/opt/fr2052a-checkpoints"

# 入湖作业在写入时补齐、CSV 表头里没有的列
LOADER_MANAGED_COLUMNS = ("etl_load_timestamp",)

MERGE_TEMPLATE = """
MERGE INTO {table} AS target
USING staging AS source
ON target.source_system = source.source_system
   AND target.source_record_id = source.source_record_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
"""


def load_config(config_path: Path) -> dict:
    return json.loads(config_path.read_text(encoding="utf-8"))


def payload_schema(spark: SparkSession, table: str) -> StructType:
    """消息体的结构 = bronze 表结构去掉 loader 侧列。"""
    return StructType(
        [field for field in spark.table(table).schema.fields if field.name not in LOADER_MANAGED_COLUMNS]
    )


def upsert_batch(batch: DataFrame, batch_id: int, table: str) -> None:
    """一个微批：解析失败的消息丢弃并留痕，其余按主键 MERGE。"""
    if batch.isEmpty():
        return
    spark = batch.sparkSession

    valid = batch.filter(F.col("source_record_id").isNotNull())
    dropped = batch.count() - valid.count()
    if dropped:
        print(f"    [{table}] 微批 {batch_id}：丢弃 {dropped} 条无法解析的消息")
    if valid.isEmpty():
        return

    valid.createOrReplaceTempView("staging")
    spark.sql(MERGE_TEMPLATE.format(table=table))
    print(f"    [{table}] 微批 {batch_id}：本批 {valid.count()} 行，表内合计 {spark.table(table).count()} 行")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="把 Kafka 的 ODS 消息流入 Iceberg bronze 层")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv[1:])
    config = load_config(args.config)
    bootstrap_servers = config["kafka"]["bootstrap_servers_internal"]
    targets = [topic for topic in config["topics"] if topic.get("has_producer")]

    spark = SparkSession.builder.appName("fr2052a-kafka-to-iceberg").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print(f"订阅 {len(targets)} 个主题，写入 bronze 层（bootstrap: {bootstrap_servers}）")
    queries = []
    for target in targets:
        topic = target["name"]
        table = target["target_table"]
        frame = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", bootstrap_servers)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            # 重放时主题可能被重建，偏移量对不上也不要让作业直接挂掉
            .option("failOnDataLoss", "false")
            .load()
        )
        parsed = frame.select(F.from_json(F.col("value").cast("string"), payload_schema(spark, table)).alias("payload"))

        def write_batch(batch: DataFrame, batch_id: int, table_name: str = table) -> None:
            upsert_batch(batch, batch_id, table_name)

        # 入湖时间按数据自身的时间线打标（报告日 T+1 凌晨 2 点），而不是真实时钟：
        # 演示数据的报告日是虚拟的，用真实时钟会让入湖时间与数据时间线脱节，
        # 并使 T+1 时效规则（VDQ-016）产生假阳性 —— 那是演示前提造成的，不是数据缺陷。
        load_timestamp = F.to_timestamp(F.concat(F.date_add(F.col("report_date"), 1), F.lit(" 02:00:00")))

        query = (
            parsed.select("payload.*")
            .withColumn("etl_load_timestamp", load_timestamp)
            .writeStream.format("iceberg")
            .outputMode("append")
            .option("checkpointLocation", f"{CHECKPOINT_ROOT}/{table}")
            .trigger(availableNow=True)
            .foreachBatch(write_batch)
            .start()
        )
        print(f"  [启动] {topic:<22} → {table}")
        queries.append((topic, query))

    for topic, query in queries:
        query.awaitTermination()
        print(f"  [完成] {topic:<22} 状态 {query.status}")

    print()
    print("全部主题消费完成，bronze 层行数：")
    for target in sorted({topic["target_table"] for topic in targets}):
        # 计数前刷新元数据：本会话早先解析过的表可能还握着旧快照，
        # 不刷新会打印出与事实不符的行数（实测曾整列打印 0，而表里其实有数据）。
        spark.catalog.refreshTable(target)
        print(f"  {target:<34} {spark.table(target).count()} 行")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
