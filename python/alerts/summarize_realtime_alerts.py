# [AI-GENERATED] model=deepseek-flash date=2026-09-17 reviewed_by=pending
"""实时敞口事件汇总 —— 给「有没有异常敞口」这个问题一个一眼能看的答案。

为什么不直接在 DAG 里 `docker exec psql`：
    PostgreSQL 跑在 Server 1 的容器里，而 DAG 的任务经 ssh_default 落到 Server 2。
    在 Server 2 上 exec 一个不存在的容器，报错是 `No such container: fr2052a_postgres`，
    看着像容器挂了，其实是任务落错了机器 —— 实测踩过一次。
    这里改成用 venv 里的 psycopg2 直连（连接信息从 .env 来），
    与 liquidity_monitor、pipeline_health 走同一条路：跨机器只走网络，不做跨机器 exec。

用法（Server 2，用 venv 里的 python 直接跑）：
    ./venv/bin/python /opt/fr2052a-app/python/alerts/summarize_realtime_alerts.py \
        --report-date 2026-09-16
"""

from __future__ import annotations

import argparse
import os
import sys

import psycopg2

TABLE = "ads.ads_fr2052a_realtime_alerts"


def parse_args() -> argparse.Namespace:
    """解析命令行参数。"""
    parser = argparse.ArgumentParser(description="实时敞口事件汇总")
    parser.add_argument("--report-date", required=True, help="报告日，格式 YYYY-MM-DD")
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


def main() -> int:
    """汇总某报告日的实时敞口事件：条数、金额与最大单笔。"""
    args = parse_args()
    connection = pg_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT alert_code, severity, count(*) AS events,
                       min(amount_usd) AS min_amount, max(amount_usd) AS max_amount,
                       max(detected_at) AS latest
                FROM {TABLE}
                WHERE report_date = %s
                GROUP BY alert_code, severity
                ORDER BY alert_code
                """,
                (args.report_date,),
            )
            rows = cursor.fetchall()
    finally:
        connection.close()

    print(f"实时敞口事件汇总（报告日 {args.report_date}）")
    if not rows:
        print(f"  无事件。{TABLE} 里没有该报告日的记录。")
        return 0

    for alert_code, severity, events, min_amount, max_amount, latest in rows:
        print(
            f"  [{severity:<7}] {alert_code:<24} {events:>4} 条  "
            f"{min_amount:>16,.2f} ~ {max_amount:>16,.2f} USD  最近 {latest}"
        )
    total = sum(row[2] for row in rows)
    print(f"  合计 {total} 条")
    return 0


if __name__ == "__main__":
    sys.exit(main())
