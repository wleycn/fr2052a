# [AI-GENERATED] model=deepseek-flash date=2026-09-17 reviewed_by=pending
"""流动性指标计算与预警判定 —— 报送熔断机制的输入端。

分三段，顺序固定：

1. 读 PostgreSQL 的报送服务层（ads.ads_fr2052a_report 与 ads.ads_gl_reconciliation），
   算出每个实体的流动性指标。
2. 按 config/liquidity_thresholds.json 的规则逐条判定，另叠加两条非指标类规则：
   总账对账差异与数据质量 ERROR 级失败（读 ads.ads_fr2052a_validation_log）。
3. 落库并翻转熔断闸：指标写 ads.ads_liquidity_metrics，预警写 ads.ads_fr2052a_alerts
   （按「报告日 + 实体 + 规则」去重，重复触达累加次数），阻断级预警把
   ads.ads_circuit_breaker 置为 HALTED；预警同时发到 Kafka 主题 fr2052a_alerts。

为什么这里不用 Spark：
   输入是已导出的报表服务层，量级是「几个实体 × 几十个 Section 字段」。
   为一个 5 行的聚合去起一个 Spark 会话，换来的只是多一层可能失效的依赖 ——
   而且 Spark 容器里没有 psycopg2，作业会在 import 阶段就挂。放在有 psycopg2 的
   venv 里直接跑，链路更短。真正需要 Spark 的是实时扫描（要读 Kafka），那部分
   单独放在 realtime_scanner.py，且只用 Spark 侧自带的 JDBC 写库。

两处口径说明（都与 [99] 的表述不同，理由写在代码里而不是留在口头）：

* LCR 分子用「未质押 HQLA」而不是报表的 sec_g_hqla_capped_total_usd。
  后者含已质押资产，而已质押资产不能自由变现，不计入 LCR 分子。两个口径都留在
  指标表里，便于回溯核对。
* 二级资产上限（一级资产 × 2/3，即 LCR30 的 40% 上限）的判定沿用报表的做法（含已质押），
  与 VDQ-017 保持一致；LCR 分子另按未质押口径重新截断一次。两个截断点各自独立，不共用中间结果。

运行（Server 2，用 venv 里的 python 直接跑，不需要 Spark）：
    ./venv/bin/python /opt/fr2052a-app/python/alerts/liquidity_monitor.py \
        --report-date 2026-09-16 --batch-id BATCH-20260916-001
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg2
import psycopg2.extras

ALERT_TOPIC = "fr2052a_alerts"
# Kafka 生产用命令行工具，不引入新的 Python 依赖。
# 主题的创建同样走 Kafka 自带的 kafka-topics.sh（见 deploy/server2/create-topics.sh），
# 口径一致：这个演示项目不为一次投递去装一个客户端库。
KAFKA_CONTAINER = "fr2052a_kafka"
KAFKA_PRODUCER = "/opt/kafka/bin/kafka-console-producer.sh"


def parse_args() -> argparse.Namespace:
    """解析命令行参数：报告日与阈值配置文件位置。"""
    parser = argparse.ArgumentParser(description="流动性指标与预警判定")
    parser.add_argument("--report-date", required=True, help="报告日，格式 YYYY-MM-DD")
    parser.add_argument("--batch-id", required=True, help="批次号，用于关联数据质量结果")
    parser.add_argument("--config", default="/opt/fr2052a-app/config/liquidity_thresholds.json")
    parser.add_argument("--topics-config", default="/opt/fr2052a-app/config/pipeline_topics.json")
    parser.add_argument("--no-publish", action="store_true", help="不投递 Kafka，只落库")
    return parser.parse_args()


def load_thresholds(path: str) -> dict[str, Any]:
    """读阈值配置。阈值只此一份，监控与放行闸都从这里取，避免两处各写一套。"""
    return json.loads(Path(path).read_text(encoding="utf-8"))


def kafka_bootstrap(path: str) -> str:
    """取容器内可用的 broker 地址 —— 命令行生产者跑在 Kafka 容器里。"""
    config = json.loads(Path(path).read_text(encoding="utf-8"))
    return config["kafka"]["bootstrap_servers_internal"]


def pg_connection() -> psycopg2.extensions.connection:
    """连接 Server 1 的 PostgreSQL：报表与控制表都在那一侧。"""
    return psycopg2.connect(
        host=os.environ["SERVER1_HOST"],
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


def number(value: Any) -> float:
    """把 NUMERIC 列读出来的 Decimal/None 统一成 float。"""
    return 0.0 if value is None else float(value)


def compute_metrics(row: dict[str, Any], regulatory_min: float) -> dict[str, Any]:
    """把一条报表行换算成 LCR 口径的指标。"""
    # Level 1 不只是非受限的一级证券：现金与同业存放同样是最优质的流动性资产
    # （DATA-DESIGN §3.2 的 Level 1 定义里就有它们）。只算证券会把分子系统性压低，
    # LCR 偏低时无法判断是「资产结构差」还是「口径少算了一块」。
    # sec_e_cash_total = 总账 1001 + 1100，取合计而不是分别相加，避免同一笔算两次。
    l1 = number(row["sec_i_unencumbered_hqla_l1"]) + number(row["sec_e_cash_total"])
    l2a = number(row["sec_i_unencumbered_hqla_l2a"])
    l2b = number(row["sec_i_unencumbered_hqla_l2b"])
    unencumbered_l2 = l2a + l2b

    # 二级资产认列上限 = 未质押一级资产的 2/3。Basel LCR30 的原文是「不得超过扣除后 HQLA 的
    # 40%」，把它写成 0.40 * (一级 + 二级) 会连二级自己也算进基数，上限偏高。
    # 算式必须与报表模型 sec_g_hqla_capped_total_usd 一致，否则同一套资产在报表上是一个数、
    # 在 LCR 分子上是另一个数。
    hqla_capped = l1 + min(unencumbered_l2, 2.0 / 3 * l1)

    inflow_raw = number(row["sec_h_expected_inflow_30d"])
    outflow = number(row["sec_k_total_outflows"])
    inflow_capped = min(inflow_raw, 0.75 * outflow)
    nco = outflow - inflow_capped

    # 报表口径的 HQLA 构成，用于二级资产占比判定（与 VDQ-017 同口径）
    g1 = number(row["sec_g_hqla_l1_mv"])
    g2a = number(row["sec_g_hqla_l2a_mv"])
    g2b = number(row["sec_g_hqla_l2b_mv"])
    total_hqla = g1 + g2a + g2b

    return {
        "report_date": row["report_date"],
        "entity_code": row["entity_code"],
        "is_consolidated": row["is_consolidated"],
        "hqla_l1_unencumbered_usd": Decimal(f"{l1:.2f}"),
        "hqla_l2a_unencumbered_usd": Decimal(f"{l2a:.2f}"),
        "hqla_l2b_unencumbered_usd": Decimal(f"{l2b:.2f}"),
        "hqla_unencumbered_capped_usd": Decimal(f"{hqla_capped:.2f}"),
        "hqla_encumbered_usd": Decimal(f"{number(row['sec_i_encumbered_total']):.2f}"),
        "expected_inflow_30d_usd": Decimal(f"{inflow_raw:.2f}"),
        "expected_inflow_capped_usd": Decimal(f"{inflow_capped:.2f}"),
        "expected_outflow_30d_usd": Decimal(f"{outflow:.2f}"),
        "net_cash_outflow_30d_usd": Decimal(f"{nco:.2f}"),
        "lcr_ratio": None if nco <= 0 else Decimal(f"{hqla_capped / nco:.4f}"),
        "l2_cap_ratio": (None if total_hqla <= 0 else Decimal(f"{(g2a + g2b) / total_hqla:.4f}")),
        "inflow_cap_ratio": None if outflow <= 0 else Decimal(f"{inflow_raw / outflow:.4f}"),
        "regulatory_min_ratio": Decimal(f"{regulatory_min:.4f}"),
        "headroom_usd": Decimal(f"{hqla_capped - regulatory_min * nco:.2f}"),
    }


def metric_alerts(metrics: list[dict[str, Any]], thresholds: dict[str, Any]) -> list[dict[str, Any]]:
    """逐条评估指标类规则。"""
    lcr_config = thresholds["lcr"]
    regulatory_min = float(lcr_config["regulatory_min"])
    warning_min = float(lcr_config["internal_warning_min"])
    l2_max = float(thresholds["l2_cap_ratio_max"])
    inflow_max = float(thresholds["inflow_cap_ratio_max"])
    blocking = set(thresholds["breaker"]["blocking_severities"])

    alerts: list[dict[str, Any]] = []

    def add(
        row: dict[str, Any],
        code: str,
        severity: str,
        metric: str,
        value: float | None,
        threshold: float | None,
        message: str,
    ) -> None:
        """追加一条预警候选，连判定用的实际值与阈值一起记下，便于事后复核。"""
        alerts.append(
            {
                "report_date": row["report_date"],
                "entity_code": row["entity_code"],
                "alert_code": code,
                "severity": severity,
                "metric_name": metric,
                "metric_value": None if value is None else Decimal(f"{value:.4f}"),
                "threshold_value": None if threshold is None else Decimal(f"{threshold:.4f}"),
                "message": message,
                "blocks_submission": severity in blocking,
            }
        )

    for row in metrics:
        lcr = row["lcr_ratio"]
        entity = row["entity_code"]
        lcr_value = None if lcr is None else float(lcr)
        if lcr_value is None:
            add(
                row,
                "CB-LCR-003",
                "WARNING",
                "lcr_ratio",
                None,
                regulatory_min,
                f"{entity} 净现金流出为零，无法计算 LCR，需人工确认现金流出预测是否缺失。",
            )
        elif lcr_value < regulatory_min:
            add(
                row,
                "CB-LCR-001",
                "CRITICAL",
                "lcr_ratio",
                lcr_value,
                regulatory_min,
                f"{entity} LCR {lcr_value:.4f} 低于监管红线 {regulatory_min:.2f}，禁止报送。",
            )
        elif lcr_value < warning_min:
            add(
                row,
                "CB-LCR-002",
                "WARNING",
                "lcr_ratio",
                lcr_value,
                warning_min,
                f"{entity} LCR {lcr_value:.4f} 低于内部预警线 {warning_min:.2f}，尚未触及监管红线。",
            )

        l2_ratio = row["l2_cap_ratio"]
        if l2_ratio is not None and float(l2_ratio) > l2_max:
            add(
                row,
                "CB-L2CAP-001",
                "WARNING",
                "l2_cap_ratio",
                float(l2_ratio),
                l2_max,
                f"{entity} 二级资产占比 {float(l2_ratio):.2%} 超过 {l2_max:.0%} 上限，认列额已被截断。",
            )

        inflow_ratio = row["inflow_cap_ratio"]
        if inflow_ratio is not None and float(inflow_ratio) > inflow_max:
            add(
                row,
                "CB-INFLOW-001",
                "INFO",
                "inflow_cap_ratio",
                float(inflow_ratio),
                inflow_max,
                f"{entity} 预期流入占流出 {float(inflow_ratio):.2%}，认列额被 75% 上限截断，"
                "报送口径与账面口径存在差额。",
            )
    return alerts


def fetch_gl_alerts(
    connection: psycopg2.extensions.connection, report_date: str, blocking: set[str]
) -> list[dict[str, Any]]:
    """总账对账未通过的行，汇总成一条 CRITICAL 预警。"""
    with connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
        cursor.execute(
            "SELECT section_code, variance FROM ads.ads_gl_reconciliation "
            "WHERE report_date = %s AND status <> 'PASS' ORDER BY section_code",
            (report_date,),
        )
        rows = cursor.fetchall()
    if not rows:
        return []
    sections = ", ".join(f"{row['section_code']}(差异 {row['variance']})" for row in rows)
    return [
        {
            "report_date": report_date,
            "entity_code": "GLOBAL",
            "alert_code": "CB-GL-001",
            "severity": "CRITICAL",
            "metric_name": "gl_reconciliation_fail_count",
            "metric_value": Decimal(len(rows)),
            "threshold_value": Decimal(0),
            "message": f"总账对账 {len(rows)} 个 Section 未通过：{sections}。未对平不得报送。",
            "blocks_submission": "CRITICAL" in blocking,
        }
    ]


def fetch_dq_alerts(
    connection: psycopg2.extensions.connection, report_date: str, batch_id: str, blocking: set[str]
) -> list[dict[str, Any]]:
    """数据质量 ERROR 级失败，汇总成一条 CRITICAL 预警。"""
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT count(*),
                   coalesce(string_agg(validation_rule_id, ', ' ORDER BY validation_rule_id), '')
            FROM ads.ads_fr2052a_validation_log
            WHERE batch_id = %s AND check_result = 'FAIL' AND severity = 'ERROR'
            """,
            (batch_id,),
        )
        count, rule_ids = cursor.fetchone()
    if not count:
        return []
    return [
        {
            "report_date": report_date,
            "entity_code": "GLOBAL",
            "alert_code": "CB-DQ-001",
            "severity": "CRITICAL",
            "metric_name": "dq_error_fail_count",
            "metric_value": Decimal(count),
            "threshold_value": Decimal(0),
            "message": f"数据质量 ERROR 级失败 {count} 条：{rule_ids}。质量不达标不得报送。",
            "blocks_submission": "CRITICAL" in blocking,
        }
    ]


def write_metrics(connection: psycopg2.extensions.connection, metrics: list[dict[str, Any]]) -> None:
    """指标按 (报告日, 实体) 覆盖写，保持一报告日一张快照。"""
    columns = [
        "report_date",
        "entity_code",
        "is_consolidated",
        "hqla_l1_unencumbered_usd",
        "hqla_l2a_unencumbered_usd",
        "hqla_l2b_unencumbered_usd",
        "hqla_unencumbered_capped_usd",
        "hqla_encumbered_usd",
        "expected_inflow_30d_usd",
        "expected_inflow_capped_usd",
        "expected_outflow_30d_usd",
        "net_cash_outflow_30d_usd",
        "lcr_ratio",
        "l2_cap_ratio",
        "inflow_cap_ratio",
        "regulatory_min_ratio",
        "headroom_usd",
    ]
    assignments = ", ".join(f"{name} = EXCLUDED.{name}" for name in columns)
    placeholders = ", ".join(["%s"] * len(columns))
    sql = (
        f"INSERT INTO ads.ads_liquidity_metrics ({', '.join(columns)}) "
        f"VALUES ({placeholders}) "
        f"ON CONFLICT (report_date, entity_code) DO UPDATE SET {assignments}, "
        f"computed_at = CURRENT_TIMESTAMP"
    )
    with connection.cursor() as cursor:
        for row in metrics:
            cursor.execute(sql, [row[name] for name in columns])
    connection.commit()


def write_alerts(connection: psycopg2.extensions.connection, alerts: list[dict[str, Any]], report_date: str) -> None:
    """预警按 (报告日, 实体, 规则) 去重；本轮未命中的关闭，避免残留阻断。"""
    with connection.cursor() as cursor:
        for row in alerts:
            cursor.execute(
                """
                INSERT INTO ads.ads_fr2052a_alerts
                    (report_date, entity_code, alert_code, severity, metric_name,
                     metric_value, threshold_value, message, blocks_submission, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'OPEN')
                ON CONFLICT (report_date, entity_code, alert_code) DO UPDATE SET
                    severity = EXCLUDED.severity,
                    metric_name = EXCLUDED.metric_name,
                    metric_value = EXCLUDED.metric_value,
                    threshold_value = EXCLUDED.threshold_value,
                    message = EXCLUDED.message,
                    blocks_submission = EXCLUDED.blocks_submission,
                    status = 'OPEN',
                    occurrence_count = ads.ads_fr2052a_alerts.occurrence_count + 1,
                    last_detected_at = CURRENT_TIMESTAMP
                """,
                (
                    row["report_date"],
                    row["entity_code"],
                    row["alert_code"],
                    row["severity"],
                    row["metric_name"],
                    row["metric_value"],
                    row["threshold_value"],
                    row["message"],
                    row["blocks_submission"],
                ),
            )

        if alerts:
            entity_codes = [row["entity_code"] for row in alerts]
            alert_codes = [row["alert_code"] for row in alerts]
            cursor.execute(
                """
                UPDATE ads.ads_fr2052a_alerts
                SET status = 'CLOSED'
                WHERE report_date = %s
                  AND status = 'OPEN'
                  AND (entity_code, alert_code) NOT IN (
                      SELECT * FROM unnest(%s::text[], %s::text[])
                  )
                """,
                (report_date, entity_codes, alert_codes),
            )
        else:
            cursor.execute(
                "UPDATE ads.ads_fr2052a_alerts SET status = 'CLOSED' WHERE report_date = %s AND status = 'OPEN'",
                (report_date,),
            )
    connection.commit()


def update_breaker(
    connection: psycopg2.extensions.connection, alerts: list[dict[str, Any]], scope: str
) -> tuple[str, str]:
    """按阻断级预警翻转熔断闸。只记状态，不做动作 —— 动作由 gate 负责。"""
    blocking = [row for row in alerts if row["blocks_submission"]]
    state = "HALTED" if blocking else "OPEN"
    if blocking:
        reason = "；".join(row["message"] for row in blocking[:3])
        trigger = blocking[0]["alert_code"]
    else:
        reason = "无阻断级预警"
        trigger = None

    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO ads.ads_circuit_breaker
                (scope, state, reason, triggered_by_alert_code, triggered_at, trip_count)
            VALUES (%s, %s, %s, %s,
                    CASE WHEN %s = 'HALTED' THEN CURRENT_TIMESTAMP END,
                    CASE WHEN %s = 'HALTED' THEN 1 ELSE 0 END)
            ON CONFLICT (scope) DO UPDATE SET
                state = EXCLUDED.state,
                reason = EXCLUDED.reason,
                triggered_by_alert_code = EXCLUDED.triggered_by_alert_code,
                triggered_at = CASE
                    WHEN EXCLUDED.state = 'HALTED'
                         AND ads.ads_circuit_breaker.state <> 'HALTED'
                    THEN CURRENT_TIMESTAMP
                    ELSE ads.ads_circuit_breaker.triggered_at
                END,
                cleared_at = CASE
                    WHEN EXCLUDED.state = 'OPEN' THEN CURRENT_TIMESTAMP
                    ELSE ads.ads_circuit_breaker.cleared_at
                END,
                trip_count = ads.ads_circuit_breaker.trip_count + CASE
                    WHEN EXCLUDED.state = 'HALTED'
                         AND ads.ads_circuit_breaker.state <> 'HALTED'
                    THEN 1 ELSE 0
                END,
                updated_at = CURRENT_TIMESTAMP
            """,
            (scope, state, reason, trigger, state, state),
        )
    connection.commit()
    return state, reason


def publish_alerts(alerts: list[dict[str, Any]], bootstrap: str) -> None:
    """把预警发到 Kafka。消费端可以是告警平台，也可以是下一轮跑批的前置检查。"""
    if not alerts:
        print("  无预警，跳过 Kafka 投递")
        return
    lines = []
    for row in alerts:
        # 报告日有两个来源：指标类预警取自 PG（date 对象），GL 与数据质量类预警
        # 取自命令行参数（字符串）。序列化前统一成 ISO 字符串，
        # 否则会出现「一种预警能投递、另一种一投就崩」——而且只在阻断级预警上崩。
        report_date = row["report_date"]
        payload = {
            "report_date": report_date.isoformat() if hasattr(report_date, "isoformat") else str(report_date),
            "entity_code": row["entity_code"],
            "alert_code": row["alert_code"],
            "severity": row["severity"],
            "metric_name": row["metric_name"],
            "metric_value": None if row["metric_value"] is None else float(row["metric_value"]),
            "message": row["message"],
            "blocks_submission": row["blocks_submission"],
        }
        lines.append(json.dumps(payload, ensure_ascii=False))
    command = [
        "docker",
        "exec",
        "-i",
        KAFKA_CONTAINER,
        KAFKA_PRODUCER,
        "--bootstrap-server",
        bootstrap,
        "--topic",
        ALERT_TOPIC,
    ]
    completed = subprocess.run(
        command,
        input="\n".join(lines) + "\n",
        text=True,
        capture_output=True,
        check=False,
    )
    if completed.returncode != 0:
        # 投递失败不改判闸状态：闸的判定依据是数据库里的记录，Kafka 只是通知通道。
        # 但必须让人看见，不能静默 —— 通知断了也是故障。
        print(f"  [WARN] Kafka 投递失败（退出码 {completed.returncode}）：{completed.stderr.strip()[:200]}")
        return
    print(f"  已投递 {len(alerts)} 条预警到 Kafka 主题 {ALERT_TOPIC}")


def main() -> int:
    """算 LCR 与各口径比例，判定预警、翻熔断，并把预警投到 Kafka 主题。"""
    args = parse_args()
    thresholds = load_thresholds(args.config)
    blocking = set(thresholds["breaker"]["blocking_severities"])
    scope = thresholds["breaker"]["scope"]

    connection = pg_connection()
    try:
        with connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            cursor.execute(
                "SELECT * FROM ads.ads_fr2052a_report WHERE report_date = %s ORDER BY entity_code",
                (args.report_date,),
            )
            report_rows = cursor.fetchall()
        if not report_rows:
            print(f"ads.ads_fr2052a_report 无 {args.report_date} 的数据，先跑上游导出")
            return 1

        metrics = [compute_metrics(dict(row), float(thresholds["lcr"]["regulatory_min"])) for row in report_rows]
        write_metrics(connection, metrics)

        alerts = metric_alerts(metrics, thresholds)
        alerts += fetch_gl_alerts(connection, args.report_date, blocking)
        alerts += fetch_dq_alerts(connection, args.report_date, args.batch_id, blocking)

        print(f"流动性指标（{args.report_date}）：")
        for row in metrics:
            lcr = row["lcr_ratio"]
            print(
                f"  {row['entity_code']:<7} "
                f"认列 HQLA {float(row['hqla_unencumbered_capped_usd']):>20,.2f} "
                f"净现金流出 {float(row['net_cash_outflow_30d_usd']):>18,.2f} "
                f"LCR {('N/A' if lcr is None else f'{float(lcr):.4f}'):>9} "
                f"距红线余量 {float(row['headroom_usd']):>20,.2f}"
            )

        print(f"\n预警判定：命中 {len(alerts)} 条")
        for row in alerts:
            print(f"  [{row['severity']:<8}] {row['alert_code']:<14} {row['entity_code']:<7} {row['message']}")

        write_alerts(connection, alerts, args.report_date)
        state, reason = update_breaker(connection, alerts, scope)
        print(f"\n熔断闸状态：{state}（{reason}）")

        if not args.no_publish:
            publish_alerts(alerts, kafka_bootstrap(args.topics_config))
    finally:
        connection.close()

    # 熔断本身不是作业失败：闸的状态由 gate 环节解释并阻断报送。
    # 这里返回非零会让日批在预警环节就断掉，后面看不到闸的状态变化。
    return 0


if __name__ == "__main__":
    sys.exit(main())
