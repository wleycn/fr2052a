# [AI-GENERATED] model=deepseek-flash date=2026-09-17 reviewed_by=pending
"""跑批健康巡检 —— 一次把「链路现在还活着吗」查清楚。

巡检项与判据（每项都写清楚「什么算正常」，否则报出来的数字没人能判断）：

    熔断闸        状态与累计熔断次数。HALTED 是重点，不是失败。
    数据质量      最近一批的 ERROR 级失败数。> 0 即为异常。
    预警          未关闭的阻断级预警数。> 0 说明有规则没过。
    报送          最近报告日的文件台账与回执状态。
    重述          累计重述次数（信息项，不是异常）。
    实时事件      实时敞口事件表累计条数与最近一条时间。
    Kafka 滞后    各消费组的 lag。持续增长说明下游跟不上上游。
    PG 连接数     与 max_connections 的比例，> 80% 视为告警。
    磁盘          数据卷占用。> 80% 视为告警。

退出码恒为 0：巡检是观测手段，不是流水线闸门。
用它当闸会让「监控自己挂了」和「系统真出问题」这两种情况变得无法区分。

用法（Server 2，用 venv 里的 python 直接跑）：
    ./venv/bin/python /opt/fr2052a-app/python/governance/pipeline_health.py
    ./venv/bin/python .../pipeline_health.py --json     # 供告警平台消费
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from typing import Any

import psycopg2
import psycopg2.extras

KAFKA_CONTAINER = "fr2052a_kafka"
PG_CONTAINER = "fr2052a_postgres"
WARN_CONNECTION_RATIO = 0.8
WARN_DISK_PERCENT = 80


def parse_args() -> argparse.Namespace:
    """解析命令行参数。"""
    parser = argparse.ArgumentParser(description="跑批健康巡检")
    parser.add_argument("--json", action="store_true", help="以 JSON 输出")
    return parser.parse_args()


def pg_connection() -> psycopg2.extensions.connection:
    """连接 Server 1 的 PostgreSQL。"""
    return psycopg2.connect(
        host=os.environ["SERVER1_HOST"],
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


def shell(command: list[str]) -> tuple[int, str]:
    """执行一条命令，返回退出码与合并后的输出。"""
    completed = subprocess.run(command, capture_output=True, text=True, check=False)
    return completed.returncode, (completed.stdout or completed.stderr).strip()


def collect_pg(connection: psycopg2.extensions.connection) -> dict[str, Any]:
    """收集库侧信号：熔断状态、质量结果、预警、报送台账、重述与连接数。"""
    with connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
        cursor.execute("SELECT state, trip_count, updated_at FROM ads.ads_circuit_breaker WHERE scope = 'GLOBAL'")
        breaker = cursor.fetchone()

        cursor.execute(
            "SELECT batch_id, count(*) AS total, "
            "count(*) FILTER (WHERE check_result = 'FAIL' AND severity = 'ERROR') AS errors, "
            "count(*) FILTER (WHERE check_result = 'FAIL' AND severity = 'WARNING') AS warnings, "
            "max(created_at) AS last_run "
            "FROM ads.ads_fr2052a_validation_log "
            "GROUP BY batch_id ORDER BY max(created_at) DESC LIMIT 1"
        )
        quality = cursor.fetchone()

        cursor.execute(
            "SELECT count(*) AS blocking FROM ads.ads_fr2052a_alerts WHERE status = 'OPEN' AND blocks_submission"
        )
        blocking_alerts = cursor.fetchone()["blocking"]

        cursor.execute("SELECT count(*) AS open_total FROM ads.ads_fr2052a_alerts WHERE status = 'OPEN'")
        open_alerts = cursor.fetchone()["open_total"]

        cursor.execute(
            "SELECT report_date, count(*) AS files, "
            "count(*) FILTER (WHERE submission_status = 'ACCEPTED') AS accepted, "
            "count(*) FILTER (WHERE submission_status = 'REJECTED') AS rejected, "
            "string_agg(DISTINCT submission_status, '/') AS status, "
            "max(submitted_at) AS submitted_at "
            "FROM ads.ads_fr2052a_submission GROUP BY report_date "
            "ORDER BY report_date DESC LIMIT 1"
        )
        submission = cursor.fetchone()

        cursor.execute("SELECT count(*) AS total FROM ads.ads_restatement_log")
        restatements = cursor.fetchone()["total"]

        cursor.execute("SELECT count(*) AS total, max(detected_at) AS last_event FROM ads.ads_fr2052a_realtime_alerts")
        realtime = cursor.fetchone()

        cursor.execute("SELECT count(*) AS n FROM pg_stat_activity")
        active = cursor.fetchone()["n"]
        cursor.execute("SELECT setting::int AS max_connections FROM pg_settings WHERE name = 'max_connections'")
        max_connections = cursor.fetchone()["max_connections"]

    return {
        "breaker": breaker,
        "quality": quality,
        "blocking_alerts": blocking_alerts,
        "open_alerts": open_alerts,
        "submission": submission,
        "restatements": restatements,
        "realtime": realtime,
        "connections": active,
        "max_connections": max_connections,
    }


def collect_kafka() -> dict[str, Any]:
    """收集 Kafka 侧信号：消费滞后。"""
    code, output = shell(
        [
            "docker",
            "exec",
            KAFKA_CONTAINER,
            "/opt/kafka/bin/kafka-consumer-groups.sh",
            "--bootstrap-server",
            "localhost:9092",
            "--describe",
            "--all-groups",
        ]
    )
    if code != 0:
        return {"error": output[:200]}

    groups: dict[str, int] = {}
    header = True
    for line in output.splitlines():
        if header:
            if line.startswith("GROUP"):
                header = False
            continue
        if not line.strip():
            # 新消费组的开始，表头会再次出现
            header = True
            continue
        parts = line.split()
        if len(parts) >= 6:
            try:
                lag = int(parts[5])
            except ValueError:
                continue
            groups[parts[0]] = groups.get(parts[0], 0) + lag
    return {"lag": groups, "max_lag": max(groups.values(), default=0)}


def collect_disk() -> dict[str, Any]:
    """收集磁盘占用。"""
    code, output = shell(["df", "-P", "/home/hermes"])
    if code != 0:
        return {"error": output[:200]}
    parts = output.splitlines()[-1].split()
    return {"mount": parts[-1], "used_percent": int(parts[4].rstrip("%")), "available": parts[3]}


def main() -> int:
    """跑全部巡检项并打印；退出码恒为 0，观察不做闸。"""
    args = parse_args()
    connection = pg_connection()
    try:
        pg = collect_pg(connection)
    finally:
        connection.close()
    kafka = collect_kafka()
    disk = collect_disk()

    report = {
        "breaker": pg["breaker"],
        "quality": pg["quality"],
        "blocking_alerts": pg["blocking_alerts"],
        "open_alerts": pg["open_alerts"],
        "submission": pg["submission"],
        "restatements": pg["restatements"],
        "realtime": pg["realtime"],
        "connections": {"active": pg["connections"], "max": pg["max_connections"]},
        "kafka": kafka,
        "disk": disk,
    }

    if args.json:
        print(json.dumps(report, default=str, ensure_ascii=False, indent=2))
        return 0

    breaker = pg["breaker"] or {}
    quality = pg["quality"] or {}
    submission = pg["submission"] or {}
    realtime = pg["realtime"] or {}

    findings: list[str] = []
    print("FR 2052a 跑批健康巡检")
    print()
    print(f"  熔断闸        {breaker.get('state', '无记录')}（累计熔断 {breaker.get('trip_count', 0)} 次）")
    if breaker.get("state") == "HALTED":
        findings.append("熔断闸处于 HALTED，报送被阻断")
    print(
        f"  最近批次      {quality.get('batch_id', '无记录')}  校验 {quality.get('total', 0)} 条"
        f"（ERROR {quality.get('errors', 0)} / WARNING {quality.get('warnings', 0)}）"
        f"  跑于 {quality.get('last_run')}"
    )
    if quality.get("errors"):
        findings.append(f"最近批次有 {quality['errors']} 条 ERROR 级校验失败")
    print(f"  预警          未关闭 {pg['open_alerts']} 条，其中阻断级 {pg['blocking_alerts']} 条")
    if pg["blocking_alerts"]:
        findings.append(f"存在 {pg['blocking_alerts']} 条未关闭的阻断级预警")
    print(
        f"  报送          {submission.get('report_date', '无记录')}  {submission.get('files', 0)} 个文件"
        f"  状态 {submission.get('status', '-')}  提交于 {submission.get('submitted_at')}"
    )
    if submission.get("rejected"):
        findings.append(
            f"{submission.get('report_date', '未知')} 有 {submission['rejected']} 个文件回执被拒（REJECTED）"
        )
    print(f"  重述          累计 {pg['restatements']} 次")
    print(f"  实时事件      {realtime.get('total', 0)} 条，最近 {realtime.get('last_event')}")

    ratio = pg["connections"] / max(pg["max_connections"], 1)
    print(f"  PG 连接       {pg['connections']}/{pg['max_connections']}（{ratio:.0%}）")
    if ratio > WARN_CONNECTION_RATIO:
        findings.append(f"PG 连接使用率 {ratio:.0%} 超过 {WARN_CONNECTION_RATIO:.0%}")

    if "error" in kafka:
        print(f"  Kafka 滞后    检查失败：{kafka['error']}")
        findings.append("Kafka 消费组滞后检查失败")
    else:
        print(f"  Kafka 滞后    {kafka['lag']}（最大 {kafka['max_lag']}）")

    if "error" in disk:
        print(f"  磁盘          检查失败：{disk['error']}")
    else:
        print(f"  磁盘          {disk['mount']} 已用 {disk['used_percent']}%，可用 {disk['available']}")
        if disk["used_percent"] > WARN_DISK_PERCENT:
            findings.append(f"磁盘已用 {disk['used_percent']}% 超过 {WARN_DISK_PERCENT}%")

    print()
    if findings:
        print(f"发现 {len(findings)} 项需要注意：")
        for item in findings:
            print(f"  - {item}")
    else:
        print("各项检查均正常。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
