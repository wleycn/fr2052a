# [AI-GENERATED] model=qianfan-code-latest date=2026-09-18 reviewed_by=pending
r"""运行上下文管理 —— 把「本次跑批处理哪一天」变成库里的一行状态。

为什么需要这张表：
    报告日此前是跑批脚本里的一个默认值 ``REPORT_DATE="${REPORT_DATE:-2026-09-16}"``，
    各脚本、各 DAG 各拿各的。放行闸只读熔断闸那一行全局状态，分不清
    「今天没有预警」与「今天根本没判」。本脚本在跑批开口时写一行上下文，
    收口时更新状态，让所有环节与闸都读同一个日期来源。

三个子命令：
    --open   按 batch_id upsert 一行，status=RUNNING
    --close  更新该行 status 与 finished_at；行不存在即报错退出非 0
    --show   打印上下文行，供人查看

退出码：0 成功；2 参数错误或行不存在；3 连库失败。

用法（Server 2，用 venv 里的 python 直接跑）：
    ./venv/bin/python /opt/fr2052a-app/python/governance/run_context.py \\
        --open --batch-id BATCH-20260916-001 \\
        --report-date 2026-09-16 --processing-date 2026-09-17 \\
        --effective-date 2026-09-17 --run-type DAILY
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from datetime import date

import psycopg2

EXIT_OK = 0
EXIT_ARG = 2
EXIT_DB = 3

DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")
VALID_RUN_TYPES = ("DAILY", "MANUAL")
VALID_STATUSES = ("RUNNING", "SUCCEEDED", "FAILED")


def parse_date(value: str, label: str) -> date:
    """解析并校验 YYYY-MM-DD 格式的日期。

    Args:
        value: 日期字符串。
        label: 参数名，用于报错提示。

    Returns:
        解析后的 date 对象。

    Raises:
        SystemExit: 格式不合法时退出码 2。
    """
    if not DATE_PATTERN.match(value):
        print(f"参数错误：{label} 必须是 YYYY-MM-DD 格式，实际收到 {value!r}", file=sys.stderr)
        sys.exit(EXIT_ARG)
    return date.fromisoformat(value)


def parse_args() -> argparse.Namespace:
    """解析命令行参数。"""
    parser = argparse.ArgumentParser(description="运行上下文管理：开口 / 收口 / 查看")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--open", action="store_true", help="开口：写一行 RUNNING 状态")
    group.add_argument("--close", action="store_true", help="收口：更新状态与 finished_at")
    group.add_argument("--show", action="store_true", help="打印上下文行")
    parser.add_argument("--batch-id", help="批次号")
    parser.add_argument("--report-date", help="报告日，YYYY-MM-DD")
    parser.add_argument("--processing-date", help="处理日，YYYY-MM-DD")
    parser.add_argument("--effective-date", help="生效日，YYYY-MM-DD")
    parser.add_argument("--run-type", default="MANUAL", help="DAILY 或 MANUAL，默认 MANUAL")
    parser.add_argument("--status", help="收口时的状态：SUCCEEDED 或 FAILED")
    args = parser.parse_args()

    if args.open:
        if not args.batch_id:
            print("参数错误：--open 需要 --batch-id", file=sys.stderr)
            sys.exit(EXIT_ARG)
        if not args.report_date:
            print("参数错误：--open 需要 --report-date", file=sys.stderr)
            sys.exit(EXIT_ARG)
        if not args.processing_date:
            print("参数错误：--open 需要 --processing-date", file=sys.stderr)
            sys.exit(EXIT_ARG)
        if not args.effective_date:
            print("参数错误：--open 需要 --effective-date", file=sys.stderr)
            sys.exit(EXIT_ARG)
        if args.run_type not in VALID_RUN_TYPES:
            print(f"参数错误：--run-type 只接受 {VALID_RUN_TYPES}，实际收到 {args.run_type!r}", file=sys.stderr)
            sys.exit(EXIT_ARG)

    if args.close:
        if not args.batch_id:
            print("参数错误：--close 需要 --batch-id", file=sys.stderr)
            sys.exit(EXIT_ARG)
        if args.status not in ("SUCCEEDED", "FAILED"):
            print(f"参数错误：--status 只接受 SUCCEEDED / FAILED，实际收到 {args.status!r}", file=sys.stderr)
            sys.exit(EXIT_ARG)

    return args


def pg_connection() -> psycopg2.extensions.connection:
    """连接 Server 1 的 PostgreSQL。"""
    return psycopg2.connect(
        host=os.environ["SERVER1_HOST"],
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


def do_open(args: argparse.Namespace) -> int:
    """开口：按 batch_id upsert 一行 RUNNING 状态。"""
    report_date = parse_date(args.report_date, "--report-date")
    processing_date = parse_date(args.processing_date, "--processing-date")
    effective_date = parse_date(args.effective_date, "--effective-date")

    connection = pg_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO ads.ads_pipeline_run_context
                    (batch_id, report_date, processing_date, effective_date,
                     run_type, status, started_at, finished_at)
                VALUES (%s, %s, %s, %s, %s, 'RUNNING', CURRENT_TIMESTAMP, NULL)
                ON CONFLICT (batch_id) DO UPDATE SET
                    report_date = EXCLUDED.report_date,
                    processing_date = EXCLUDED.processing_date,
                    effective_date = EXCLUDED.effective_date,
                    run_type = EXCLUDED.run_type,
                    status = 'RUNNING',
                    started_at = CURRENT_TIMESTAMP,
                    finished_at = NULL
                """,
                (args.batch_id, report_date, processing_date, effective_date, args.run_type),
            )
        connection.commit()
    finally:
        connection.close()

    print(
        f"[OPEN] batch_id={args.batch_id} report_date={report_date} "
        f"processing_date={processing_date} effective_date={effective_date} "
        f"run_type={args.run_type} status=RUNNING"
    )
    return EXIT_OK


def do_close(args: argparse.Namespace) -> int:
    """收口：更新该行 status 与 finished_at；行不存在即报错退出非 0。"""
    connection = pg_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT report_date, status FROM ads.ads_pipeline_run_context
                WHERE batch_id = %s
                """,
                (args.batch_id,),
            )
            row = cursor.fetchone()
            if row is None:
                print(
                    f"参数错误：batch_id={args.batch_id} 在运行上下文表里不存在，"
                    "说明开口那步没跑。这是真异常，不许静默。",
                    file=sys.stderr,
                )
                return EXIT_ARG

            cursor.execute(
                """
                UPDATE ads.ads_pipeline_run_context
                SET status = %s, finished_at = CURRENT_TIMESTAMP
                WHERE batch_id = %s
                """,
                (args.status, args.batch_id),
            )
        connection.commit()
    finally:
        connection.close()

    print(f"[CLOSE] batch_id={args.batch_id} status={args.status}")
    return EXIT_OK


def do_show(args: argparse.Namespace) -> int:
    """打印上下文行，供人查看。"""
    connection = pg_connection()
    try:
        with connection.cursor() as cursor:
            if args.report_date:
                cursor.execute(
                    """
                    SELECT batch_id, report_date, processing_date, effective_date,
                           run_type, status, started_at, finished_at
                    FROM ads.ads_pipeline_run_context
                    WHERE report_date = %s
                    ORDER BY started_at DESC
                    """,
                    (args.report_date,),
                )
            else:
                cursor.execute(
                    """
                    SELECT batch_id, report_date, processing_date, effective_date,
                           run_type, status, started_at, finished_at
                    FROM ads.ads_pipeline_run_context
                    ORDER BY started_at DESC
                    """
                )
            rows = cursor.fetchall()
    finally:
        connection.close()

    if not rows:
        print("运行上下文表无记录")
        return EXIT_OK

    print(
        f"{'batch_id':<28} {'report_date':<12} {'processing':<12} "
        f"{'effective':<12} {'run_type':<8} {'status':<10} {'started_at':<28} {'finished_at'}"
    )
    for batch_id, report_date, processing_date, effective_date, run_type, status, started_at, finished_at in rows:
        finished_str = finished_at if finished_at is not None else "-"
        print(
            f"{batch_id:<28} {report_date}    {processing_date}    "
            f"{effective_date}    {run_type:<8} {status:<10} {started_at}   {finished_str}"
        )
    return EXIT_OK


def main() -> int:
    """入口：按子命令分发。"""
    args = parse_args()
    try:
        if args.open:
            return do_open(args)
        if args.close:
            return do_close(args)
        return do_show(args)
    except psycopg2.Error as error:
        print(f"连库失败：{str(error).strip()}", file=sys.stderr)
        return EXIT_DB


if __name__ == "__main__":
    sys.exit(main())
