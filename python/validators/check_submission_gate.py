# [AI-GENERATED] model=deepseek-flash date=2026-09-17 reviewed_by=pending
"""报送放行闸 —— 只有它说通过，报送环节才允许生成文件。

为什么单独做成一道闸而不并进 liquidity_monitor：
    monitor 负责「判定」并记录状态，闸负责「拦停」。判定与拦停分开有两个好处：
    monitor 挂掉或漏判时不会静默放行（闸读不到状态就报错退出，而不是当成通过），
    且闸可以被任何下游环节复用，不依赖 Spark 是否在跑。

判据（按检查顺序，先不通过即短路返回）：
    1. 本报告日最近一次日批必须跑成功（读 ads.ads_pipeline_run_context，取最近一行）。
       没有行 = 从未跑过日批；最新状态不是 SUCCEEDED = 当前批次没跑成。两者都判 UNKNOWN。
       必须取「最近一次」而不是「存在过某次成功」：后者会被更早的成功掩盖掉当前失败的批次，
       那就是本条判据要修的缺陷本身的翻版。
    2. 本报告日必须有流动性判定痕迹（读 ads.ads_liquidity_metrics，count > 0）。
       为 0 = 判定没跑过，等于没判，判 UNKNOWN。
    3. 熔断闸行存在（读 ads.ads_circuit_breaker）。行不存在判 UNKNOWN。
    4. 本报告日无阻断级预警（读 ads.ads_fr2052a_alerts）。

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
    """报送放行闸：按四条判据依次检查，任一不通过即短路返回。退出码 0 放行、2 阻断、3 取不到状态。"""
    args = parse_args()
    connection = pg_connection()
    try:
        with connection.cursor() as cursor:
            # 判据 1：本报告日最近一次日批必须跑成功。
            # 必须取最近一行而不是「存在过某次成功」：
            # 只看存在过会被更早的成功掩盖当前失败的批次，那就是本条判据要修的缺陷的翻版。
            cursor.execute(
                """
                SELECT status FROM ads.ads_pipeline_run_context
                WHERE report_date = %s
                ORDER BY started_at DESC LIMIT 1
                """,
                (args.report_date,),
            )
            run_ctx = cursor.fetchone()
            if run_ctx is None:
                print(
                    f"[UNKNOWN] 报告日 {args.report_date} 在运行上下文表里没有行，"
                    "说明日批从未跑过，无法判定是否放行，按不放行处理。"
                )
                return EXIT_UNKNOWN
            run_status = run_ctx[0]
            if run_status != "SUCCEEDED":
                print(
                    f"[UNKNOWN] 报告日 {args.report_date} 最近一次日批状态为 {run_status}，"
                    "不是 SUCCEEDED，无法判定是否放行，按不放行处理。"
                )
                return EXIT_UNKNOWN

            # 判据 2：本报告日必须有流动性判定痕迹。
            # 没有指标行 = 判定环节没跑过，等于没判，与「判了但没有预警」不同。
            cursor.execute(
                "SELECT count(*) FROM ads.ads_liquidity_metrics WHERE report_date = %s",
                (args.report_date,),
            )
            metrics_row = cursor.fetchone()
            metrics_count = metrics_row[0] if metrics_row is not None else 0
            if metrics_count == 0:
                print(
                    f"[UNKNOWN] 报告日 {args.report_date} 在流动性指标表里没有行，"
                    "说明判定环节没跑过，无法判定是否放行，按不放行处理。"
                )
                return EXIT_UNKNOWN

            # 判据 3：熔断闸行存在。
            cursor.execute(
                "SELECT state, reason, trip_count, updated_at FROM ads.ads_circuit_breaker WHERE scope = %s",
                (args.scope,),
            )
            breaker = cursor.fetchone()
            if breaker is None:
                print(f"[UNKNOWN] 熔断闸无 {args.scope} 的记录，说明预警环节还没跑过。无法判定是否放行，按不放行处理。")
                return EXIT_UNKNOWN

            state, reason, trip_count, updated_at = breaker

            # 判据 4：本报告日无阻断级预警。
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
