# [AI-GENERATED] model=deepseek-flash date=2026-09-17 reviewed_by=pending
"""Iceberg 时间旅行审计 —— 回答「这一行改之前长什么样」。

为什么这件事不能靠版本历史表单独完成：
    silver.owd_*_history 记的是「按业务键的版本」，它看不见被删掉的整张表、
    也看不见某次写入是怎么落盘的。Iceberg 快照是存储层的事实，两者互补：
    版本历史回答「业务上改了什么」，快照回答「物理上写过什么」。

三个用途：

    --list-snapshots            列出快照，看这张表被写过几次、每次是什么操作
    --diff A B                  比较两个快照的行数与键集合差异，定位「哪次改动动了什么」
    --trace-key <值>            把一个键在各快照里的取值逐版列出来，用于重述取证

用法（Server 2，经 spark-submit 包装脚本执行）：

    bash spark-submit-fr2052a.sh /opt/fr2052a-app/python/audit/time_travel.py \
        --table silver.owd_deposits --list-snapshots

    bash spark-submit-fr2052a.sh /opt/fr2052a-app/python/audit/time_travel.py \
        --table silver.owd_deposits --trace-key DEP-000001 \
        --key-column source_record_id --columns principal_amount_lc,principal_amount_usd
"""

from __future__ import annotations

import argparse
import sys

from pyspark.sql import SparkSession


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Iceberg 时间旅行审计")
    parser.add_argument("--table", required=True, help="表名，如 silver.owd_deposits")
    parser.add_argument("--list-snapshots", action="store_true", help="列出快照")
    parser.add_argument("--diff", nargs=2, metavar=("SNAPSHOT_A", "SNAPSHOT_B"), help="比较两个快照")
    parser.add_argument("--key-column", default="source_record_id", help="业务键列名")
    parser.add_argument("--trace-key", help="要追溯的键值")
    parser.add_argument(
        "--columns",
        default=None,
        help="追溯时打印的列，逗号分隔；默认打印全部列（只对单键，行数可控）",
    )
    return parser.parse_args()


def list_snapshots(spark: SparkSession, table: str) -> None:
    frame = spark.sql(
        f"""
        SELECT snapshot_id, parent_id, committed_at, operation
        FROM {table}.snapshots
        ORDER BY committed_at
        """
    )
    rows = frame.collect()
    print(f"{table} 共有 {len(rows)} 个快照：")
    for row in rows:
        print(
            f"  {row['committed_at']}  id={row['snapshot_id']}  "
            f"operation={row['operation']}  parent={row['parent_id']}"
        )
    current = spark.sql(f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1")
    current_rows = current.collect()
    if current_rows:
        print(f"\n当前快照：{current_rows[0]['snapshot_id']}")


def diff_snapshots(spark: SparkSession, table: str, args: argparse.Namespace) -> None:
    snapshot_a, snapshot_b = args.diff
    keys_a = spark.sql(
        f"SELECT {args.key_column} AS k FROM {table} VERSION AS OF {snapshot_a}"
    ).cache()
    keys_b = spark.sql(
        f"SELECT {args.key_column} AS k FROM {table} VERSION AS OF {snapshot_b}"
    ).cache()

    count_a, count_b = keys_a.count(), keys_b.count()
    added = keys_b.subtract(keys_a).count()
    removed = keys_a.subtract(keys_b).count()

    print(f"{table} 快照 {snapshot_a} → {snapshot_b}")
    print(f"  行数：{count_a} → {count_b}")
    print(f"  新增键：{added}    消失键：{removed}    两侧都在：{count_b - added}")
    print(
        "  注意：键集合相同不代表内容相同。内容级差异看 *_history 表的 row_hash，"
        "快照层只回答行数与键集合。"
    )
    keys_a.unpersist()
    keys_b.unpersist()


def trace_key(spark: SparkSession, table: str, args: argparse.Namespace) -> None:
    columns = None if args.columns is None else [name.strip() for name in args.columns.split(",")]
    snapshots = spark.sql(
        f"SELECT snapshot_id, committed_at, operation FROM {table}.snapshots ORDER BY committed_at"
    ).collect()
    print(f"{table} 中 {args.key_column} = {args.trace_key} 的逐版取值：")
    previous = None
    for snapshot in snapshots:
        frame = spark.sql(
            f"""
            SELECT * FROM {table} VERSION AS OF {snapshot['snapshot_id']}
            WHERE {args.key_column} = '{args.trace_key}'
            """
        )
        rows = frame.collect()
        if not rows:
            print(f"  {snapshot['committed_at']}  id={snapshot['snapshot_id']}  <该快照中没有这条记录>")
            continue
        row = rows[0].asDict()
        shown = {name: row.get(name) for name in (columns or row)} if columns else row
        marker = "  " if shown == previous else "→ "
        print(f"{marker}{snapshot['committed_at']}  id={snapshot['snapshot_id']}  {shown}")
        previous = shown


def main() -> int:
    args = parse_args()
    spark = SparkSession.builder.appName("fr2052a-time-travel").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    actions = [args.list_snapshots, args.diff is not None, args.trace_key is not None]
    if not any(actions):
        print("至少给一个动作：--list-snapshots / --diff / --trace-key")
        return 2

    try:
        if args.list_snapshots:
            list_snapshots(spark, args.table)
        if args.diff is not None:
            print()
            diff_snapshots(spark, args.table, args)
        if args.trace_key is not None:
            print()
            trace_key(spark, args.table, args)
    except Exception as error:  # noqa: BLE001 - 审计工具失败要如实报出，不能吞
        print(f"查询失败：{type(error).__name__}: {error}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
