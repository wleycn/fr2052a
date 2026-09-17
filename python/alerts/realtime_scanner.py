# [AI-GENERATED] model=deepseek-flash date=2026-09-17 reviewed_by=pending
"""实时敞口扫描 —— 消费 Kafka 主题，识别大额未保险存款敞口并告警。

与 liquidity_monitor.py 的分工：
    那个看的是「已经算完的报表」，是批次内的判定；
    这个看的是「正在进来的源记录」，是批次之间的预警。
    一个客户在一天里开出一笔大额未保险存款，批次跑批要到 T+1 才看见，
    这笔存款却可能当天就被提走 —— 这就是实时扫描存在的理由。

判定规则（阈值见 config/liquidity_thresholds.json 的 large_exposure）：
    未保险（insured_flag = N）、USD 计价、且本金 ≥ 门槛的存款头寸 → WARNING 级事件预警。
    只报警不阻断：单笔敞口不构成报送阻断条件，阻断由批次侧的规则决定。

只在 USD 头寸上判定：
    ODS 的金额是原币金额，汇率折算发生在 OWD 层，本脚本读不到折算后的值。
    拿 JPY 4,941,215 去和一个以 USD 表示的门槛比较，等于把 3 万美金的敞口报成 490 万，
    所以这里显式限定 USD，而不是「约等于」地放过。
    非 USD 头寸的敞口监控应放到 OWD 之后，那是另一件事。

解析 schema 与生产者对齐，且**数值字段按字符串收**：
    生产者（python/producers/replay_ods_to_kafka.py）把 ODS 整行按列名序列化成 JSON，
    而 CSV 读进来全是字符串，所以消息里连金额也是带引号的 "3144856.76"。
    若 schema 直接写 DOUBLE，这个字段会解析成 NULL —— 而 insured_flag 这类真字符串
    字段照常解析成功，于是表现为「解析没报错、判定一条都不命中」，作业照常打印
    「无新增大额敞口」。实测踩过这一轮空转，所以这里按 STRING 收、再显式 cast 成数值，
    并用「转出来的数值是否为空」当作解析成功的判据（见 write_batch 的盲区检查）。

落点：
    ads.ads_fr2052a_realtime_alerts（事件流水，一行一笔）—— 经 Spark JDBC 追加写入。
    为什么不用 psycopg2：本作业跑在 Spark 容器里，容器内没有 psycopg2；
    而实时写入本来就没有 upsert 语义（每笔事件都该独立留痕），JDBC 追加正合适。

配套：checkpoint 目录记录 Kafka 消费位点，且必须同时挂到 spark-master 与 spark-worker。
      执行器侧要写状态存储，只挂驱动侧时会在执行器上报
      「mkdir of file:/opt/fr2052a-checkpoints/... failed」，看着像权限问题，实为挂载缺失。
      删掉 checkpoint 会让作业从头重放，而事件表主键会拦下重复写入并让作业失败 ——
      这是有意的：宁可红，也不重复报。

运行（Server 2，经 spark-submit 包装脚本执行）：
    bash spark-submit-fr2052a.sh /opt/fr2052a-app/python/alerts/realtime_scanner.py \
        --report-date 2026-09-16
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

SOURCE_TOPIC = "core_banking_txns"
ALERT_TOPIC = "fr2052a_alerts"
ALERT_CODE = "RT-LARGE-UNINSURED-001"
CHECKPOINT_ROOT = "/opt/fr2052a-checkpoints"

# 解析 Kafka 消息用的字段清单，键名必须与生产者写进消息的 JSON 一致（即 ODS 的列名）。
# 金额按 STRING 收：消息里它是带引号的字符串，直接声明成 DOUBLE 会解析成 NULL。
PAYLOAD_SCHEMA = (
    "report_date STRING, entity_code STRING, source_record_id STRING, currency STRING, "
    "customer_type_raw STRING, insured_flag STRING, principal_amount STRING"
)
# 解析成功的判据字段；它为空即说明载荷与 schema 对不上。
AMOUNT_FIELD = "principal_amount"
# 判定只覆盖 USD：ODS 的金额是原币金额，汇率折算在 OWD 层。
COMPARABLE_CURRENCY = "USD"


def parse_args() -> argparse.Namespace:
    """解析命令行参数。"""
    parser = argparse.ArgumentParser(description="实时大额敞口扫描")
    parser.add_argument("--report-date", required=True, help="报告日，随预警一并落库")
    parser.add_argument("--config", default="/opt/fr2052a-app/config/liquidity_thresholds.json")
    parser.add_argument("--topics-config", default="/opt/fr2052a-app/config/pipeline_topics.json")
    return parser.parse_args()


def load_config(path: str) -> dict[str, Any]:
    """读扫描配置：大额敞口阈值等。"""
    return json.loads(Path(path).read_text(encoding="utf-8"))


def jdbc_options() -> tuple[str, dict[str, str]]:
    """拼 PostgreSQL 的 JDBC 连接串与凭据，供 Spark 直接写控制表。"""
    host = os.environ["SERVER1_HOST"]
    database = os.environ["POSTGRES_DB"]
    url = f"jdbc:postgresql://{host}:5432/{database}"
    return url, {
        "user": os.environ["POSTGRES_USER"],
        "password": os.environ["POSTGRES_PASSWORD"],
        "driver": "org.postgresql.Driver",
    }


def detect(frame: DataFrame, report_date: str, threshold: float) -> DataFrame:
    """标出大额未保险存款，并留下「解析是否成功」的痕迹。

    不在这里 filter 掉未命中的行：过滤之后，「一条都没命中」与「解析全为空」
    结果完全一样，后者会让扫描长期空转而不报错。
    因此改为打 is_candidate 标记，另加 payload_parsed 标记，由写入端做盲区检查。
    """
    payload = F.from_json(F.col("value").cast("string"), PAYLOAD_SCHEMA)
    parsed = frame.select(payload.alias("p")).select("p.*")

    return (
        parsed.withColumn("amount_num", F.col(AMOUNT_FIELD).cast("double"))
        .withColumn("payload_parsed", F.col("amount_num").isNotNull())
        .withColumn(
            "is_candidate",
            (F.col("insured_flag") == F.lit("N"))
            & (F.col("currency") == F.lit(COMPARABLE_CURRENCY))
            & (F.col("amount_num") >= F.lit(threshold)),
        )
        .withColumn("entity_code", F.coalesce(F.col("entity_code"), F.lit("UNKNOWN")))
        .withColumn("segment", F.coalesce(F.col("customer_type_raw"), F.lit("UNKNOWN")))
        .withColumn("amount_usd", F.round(F.col("amount_num"), 2))
        .withColumn(
            "event_id",
            F.md5(
                F.concat_ws(
                    "|",
                    F.lit(ALERT_CODE),
                    F.col("source_record_id"),
                    F.lit(report_date),
                )
            ),
        )
        .withColumn("report_date", F.to_date(F.lit(report_date)))
        .withColumn("alert_code", F.lit(ALERT_CODE))
        .withColumn("severity", F.lit("WARNING"))
        .withColumn("source_topic", F.lit(SOURCE_TOPIC))
        .withColumn(
            "message",
            F.concat(
                F.lit("未保险存款单笔敞口 "),
                F.col("amount_usd").cast("string"),
                F.lit(" USD（客户类别 "),
                F.col("segment"),
                F.lit("），超过监控门槛 "),
                F.lit(str(threshold)),
                F.lit(" USD"),
            ),
        )
        .select(
            "event_id",
            "report_date",
            "entity_code",
            "alert_code",
            "severity",
            "source_topic",
            "source_record_id",
            "amount_usd",
            "segment",
            "message",
            "is_candidate",
            "payload_parsed",
        )
    )


def main() -> int:
    """扫描入口：从交易主题识别大额未保险存款，落库并投递预警。"""
    args = parse_args()
    config = load_config(args.config)
    topics = load_config(args.topics_config)
    threshold = float(config["large_exposure"]["uninsured_deposit_min_usd"])
    bootstrap = topics["kafka"]["bootstrap_servers_internal"]
    jdbc_url, properties = jdbc_options()

    spark = SparkSession.builder.appName("fr2052a-realtime-scan").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", SOURCE_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )
    alerts = detect(raw, args.report_date, threshold)

    def write_batch(batch: DataFrame, batch_id: int) -> None:
        """落一个微批并投递预警。先报读入条数与命中条数，让「零命中」与「读不到」分得开。"""
        total = batch.count()
        if total == 0:
            print(f"  批次 {batch_id}：无新消息")
            return

        # 盲区检查：消息读到了，但金额字段一条都没转成数值 —— 只可能是载荷格式或字段名变了。
        # 没有这道检查，扫描会安静地一条都报不出来，还照常打印「无新增大额敞口」。
        if batch.filter(F.col("payload_parsed")).count() == 0:
            raise ValueError(
                f"批次 {batch_id}：读到 {total} 条消息，但 {AMOUNT_FIELD} 转数值后全为空，"
                "消息载荷与解析 schema 对不上 —— 先核对生产者写进 Kafka 的字段名与取值格式"
            )

        # 去重放在批内做，不用流上的 dropDuplicates：
        # 流上的 dropDuplicates 是有状态算子，会把历史见过的键长期留在 checkpoint 状态里，
        # 结果是「重放同样的样本数据全部被静默丢掉、一条都不入表」——
        # 偏移量照常前进，输出却永远是空的，排查时非常难看出来。
        # 批内去重 + 事件表主键，就是完整的去重：跨批次重复会撞主键让作业报错，
        # 这是有意的（宁可红，也不重复报）。
        candidates = (
            batch.filter(F.col("is_candidate")).drop("is_candidate", "payload_parsed").dropDuplicates(["event_id"])
        )
        rows = candidates.collect()
        if not rows:
            print(f"  批次 {batch_id}：读到 {total} 条消息，无新增大额敞口")
            return

        candidates.write.jdbc(jdbc_url, "ads.ads_fr2052a_realtime_alerts", mode="append", properties=properties)
        print(f"  批次 {batch_id}：写入 {len(rows)} 条敞口预警")
        for row in rows[:5]:
            print(f"    {row['entity_code']}  {row['amount_usd']:>16,.2f}  {row['source_record_id']}")
        # 同时发到告警主题，供告警平台订阅
        candidates.select(
            F.to_json(F.struct(*[F.col(name) for name in candidates.columns])).alias("value")
        ).write.format("kafka").option("kafka.bootstrap.servers", bootstrap).option("topic", ALERT_TOPIC).save()

    query = (
        alerts.writeStream.foreachBatch(write_batch)
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/realtime_alerts")
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()

    total = spark.read.jdbc(jdbc_url, "ads.ads_fr2052a_realtime_alerts", properties=properties).count()
    print(f"\n实时敞口事件表累计 {total} 条")
    return 0


if __name__ == "__main__":
    sys.exit(main())
