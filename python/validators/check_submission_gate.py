# [AI-GENERATED] model=deepseek-flash date=2026-09-17 reviewed_by=pending
"""报送放行闸 —— 只有它说通过，报送环节才允许生成文件。

为什么单独做成一道闸而不并进 liquidity_monitor：
    monitor 负责「判定」并记录状态，闸负责「拦停」。判定与拦停分开有两个好处：
    monitor 挂掉或漏判时不会静默放行（闸读不到状态就报错退出，而不是当成通过），
    且闸可以被任何下游环节复用，不依赖 Spark 是否在跑。

退出码约定（上游 Airflow DAG 依赖它做阻断）：
    0  放行
    2  熔断中，禁止报送
    3  状态缺失或依赖表不可读，无法判定 —— 按「不通过」处理，不放行

用法（Server 2，用 venv 里的 python 直接跑，不需要 Spark）：
    ./venv/bin/python /opt/fr2052a-app/python/validators/check_submission_gate.py \
        --report-date 2026-09-16
"""

from __future__ import annotations

import argparse
import os
import sys

import psycopg2

EXIT_PASS = 0
EXIT_HALTED = 2
EXIT_UNKNOWN = 3


def parse_args() -> argparse.Namespace:
    """解析命令行参数。"""
    parser = argparse.ArgumentParser(description="报送放行闸")
    parser.add_argument("--report-date", required=True, help="报告日，格式 YYYY-MM-DD")
    parser.add_argument("--scope", default="GLOBAL", help="熔断范围")
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
    """报送放行闸：熔断或对账未平即不放行。退出码 0 放行、2 阻断、3 取不到状态。"""
    args = parse_args()
    connection = pg_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT state, reason, trip_count, updated_at FROM ads.ads_circuit_breaker WHERE scope = %s",
                (args.scope,),
            )
            breaker = cursor.fetchone()
            if breaker is None:
                print(f"[UNKNOWN] 熔断闸无 {args.scope} 的记录，说明预警环节还没跑过。无法判定是否放行，按不放行处理。")
                return EXIT_UNKNOWN

            state, reason, trip_count, updated_at = breaker

            cursor.execute(
                """
                SELECT alert_code, entity_code, severity, message, occurrence_count
                FROM ads.ads_fr2052a_alerts
                WHERE report_date = %s AND status = 'OPEN' AND blocks_submission
                ORDER BY severity, entity_code, alert_code
                """,
                (args.report_date,),
            )
            blocking = cursor.fetchall()
    finally:
        connection.close()

    print(f"熔断闸 {args.scope}：{state}（累计熔断 {trip_count} 次，更新于 {updated_at}）")

    if state == "OPEN" and not blocking:
        print(f"[PASS] 报告日 {args.report_date} 无阻断级预警，允许生成报送文件。")
        return EXIT_PASS

    print(f"[HALTED] 报告日 {args.report_date} 存在阻断级预警，禁止生成报送文件：")
    for alert_code, entity_code, severity, message, count in blocking:
        print(f"  [{severity}] {alert_code} {entity_code}（第 {count} 次）：{message}")
    if not blocking:
        print(f"  闸状态为 {state}，原因：{reason}")
    return EXIT_HALTED


if __name__ == "__main__":
    sys.exit(main())
