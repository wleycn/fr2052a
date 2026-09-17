# [AI-GENERATED] model=deepseek-flash date=2026-09-17 reviewed_by=pending
"""重述登记 —— 把「同一天重跑一次」变成有版本、有原因、有审批痕迹的动作。

为什么需要它：日批重跑与监管重述是两件不同的事。前者是运维动作（数据没变、结果应该一样），
后者是对已报送数字的修正（数据变了、必须留痕、必须能回答「改了什么、谁批的」）。
本作业把后者显式化：报文版本化、登记原报表与新报表的对应关系。

SCD2 语义（与 OWD 层 owd_scd2.py 同一套，只是对象换成了报表）：
    版本 N     begin_date = 该版本的产生日，end_date 为空（空 = 当前有效），is_active = true
    被重述时   旧版本 end_date = 重述日，is_active = false
               新版本 record_version = N + 1，begin_date = 重述日，end_date 为空，is_active = true

    当前有效用「end_date 为空」表达，不用 9999-12-31 这种哨兵日期 ——
    哨兵值会被按日期过滤的查询当成真实日期算进去。

两个模式，由流水线分两处调用（顺序不能颠倒）：

    --mode capture    在重跑之前，把「当前生效版本」的快照存进版本历史。
                      必须在重跑之前执行 —— 重跑会覆盖 ads_fr2052a_report，
                      覆盖之后再取就取不到原报表了。
    --mode register   在重跑之后，关闭旧版本、登记新版本、写重述登记表。

不把两个模式合成一次调用：中间夹着重跑（dbt 全量重建 + 导出），
而重跑是流水线的事，不该由本脚本代劳。

运行（Server 2，用 venv 里的 python 直接跑）：
    ./venv/bin/python /opt/fr2052a-app/python/lakehouse/restate.py \
        --mode capture --report-date 2026-09-16 --entity-code ENT001 \
        --reason "发现某笔回购到期日录错" --requested-by treasury_analyst
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date
from decimal import Decimal

import psycopg2
import psycopg2.extras


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="重述登记")
    parser.add_argument("--mode", required=True, choices=("capture", "register"))
    parser.add_argument("--report-date", required=True, help="报告日，格式 YYYY-MM-DD")
    parser.add_argument("--entity-code", required=True, help="法人实体编码")
    parser.add_argument("--effective-date", help="版本生效日（重述日），默认同报告日")
    parser.add_argument("--reason", default="数据修正", help="重述原因")
    parser.add_argument("--requested-by", default="unknown", help="申请人")
    parser.add_argument("--approved-by", default=None, help="审批人")
    return parser.parse_args()


def pg_connection():
    return psycopg2.connect(
        host=os.environ["SERVER1_HOST"],
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


def json_default(value):
    """JSONB 不接受 Decimal 与 date，转成字符串/浮点。"""
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, date):
        return value.isoformat()
    raise TypeError(f"无法序列化的类型：{type(value).__name__}")


def fetch_report_row(cursor, report_date: str, entity_code: str) -> dict | None:
    cursor.execute(
        """
        SELECT *, md5(row_to_json(t)::text) AS snapshot_hash
        FROM ads.ads_fr2052a_report t
        WHERE report_date = %s AND entity_code = %s
        """,
        (report_date, entity_code),
    )
    row = cursor.fetchone()
    return None if row is None else dict(row)


def capture(connection, args: argparse.Namespace) -> int:
    """把当前生效的报表存进版本历史（若尚未登记过）。"""
    with connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
        report = fetch_report_row(cursor, args.report_date, args.entity_code)
    if report is None:
        print(
            f"ads.ads_fr2052a_report 里没有 {args.report_date} / {args.entity_code} 的报文，"
            "无从登记。先跑一次完整批把基线做出来。"
        )
        return 1

    report_id = report["report_id"]
    snapshot_hash = report.pop("snapshot_hash")

    with connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
        cursor.execute(
            "SELECT record_version, begin_date, md5(snapshot::text) AS snapshot_hash "
            "FROM ads.ads_fr2052a_report_history WHERE report_id = %s AND is_active",
            (report_id,),
        )
        existing = cursor.fetchone()

    if existing is not None:
        if existing["snapshot_hash"] == snapshot_hash:
            print(
                f"{report_id} 当前版本已登记且内容一致（v{existing['record_version']}），无需重复登记。"
            )
            return 0
        # 内容变了但没走重述流程 —— 说明有人直接改了数据。留痕并继续登记新版本，
        # 不能静默覆盖，否则版本历史会说谎。
        print(
            f"[WARN] {report_id} 当前报文与已登记的 v{existing['record_version']} 不一致，"
            "但未走重述流程。按 CORRECTION 登记新版本并留痕。"
        )
        next_version = existing["record_version"] + 1
        reason = "CORRECTION"
    else:
        next_version = 1
        reason = "ORIGINAL"

    effective = args.effective_date or args.report_date
    report_version_id = f"{report_id}-v{next_version}"

    with connection.cursor() as cursor:
        if existing is not None:
            cursor.execute(
                "UPDATE ads.ads_fr2052a_report_history "
                "SET end_date = date %s, is_active = false "
                "WHERE report_id = %s AND is_active",
                (effective, report_id),
            )
        cursor.execute(
            """
            INSERT INTO ads.ads_fr2052a_report_history
                (report_version_id, report_id, record_version, report_date, entity_code,
                 begin_date, end_date, is_active, last_modified_reason,
                 snapshot, snapshot_hash)
            VALUES (%s, %s, %s, %s, %s, date %s, NULL, true, %s, %s, %s)
            """,
            (
                report_version_id,
                report_id,
                next_version,
                args.report_date,
                args.entity_code,
                effective,
                reason,
                json.dumps(report, ensure_ascii=False, default=json_default),
                snapshot_hash,
            ),
        )
    connection.commit()

    print(f"[capture] {report_version_id} 已登记（reason={reason}，生效日 {effective}）")
    if existing is not None:
        print(f"          旧版本 v{existing['record_version']} 已置为失效")
    return 0


def register(connection, args: argparse.Namespace) -> int:
    """重跑完成后：关闭旧版本、写入新版本、登记重述记录。"""
    with connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
        report = fetch_report_row(cursor, args.report_date, args.entity_code)
    if report is None:
        print(f"重跑后找不到 {args.report_date} / {args.entity_code} 的报文，无法登记新版本。")
        return 1

    report_id = report["report_id"]
    snapshot_hash = report.pop("snapshot_hash")
    effective = args.effective_date or args.report_date

    with connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
        cursor.execute(
            "SELECT record_version, report_version_id, md5(snapshot::text) AS snapshot_hash "
            "FROM ads.ads_fr2052a_report_history "
            "WHERE report_id = %s AND is_active "
            "ORDER BY record_version DESC LIMIT 1",
            (report_id,),
        )
        previous = cursor.fetchone()

    if previous is None:
        print(f"{report_id} 没有已登记的历史版本，请先跑一次 --mode capture。")
        return 1
    if previous["snapshot_hash"] == snapshot_hash:
        print(
            f"{report_id} 重跑前后报文完全一致（仍是 v{previous['record_version']}）。"
            "数字没变，不构成重述，未登记新版本。"
        )
        return 0

    next_version = previous["record_version"] + 1
    report_version_id = f"{report_id}-v{next_version}"
    original_report_version_id = previous["report_version_id"]

    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE ads.ads_fr2052a_report_history "
            "SET end_date = date %s, is_active = false "
            "WHERE report_version_id = %s",
            (effective, original_report_version_id),
        )
        cursor.execute(
            """
            INSERT INTO ads.ads_fr2052a_report_history
                (report_version_id, report_id, record_version, report_date, entity_code,
                 begin_date, end_date, is_active, last_modified_reason,
                 snapshot, snapshot_hash)
            VALUES (%s, %s, %s, %s, %s, date %s, NULL, true, 'RESTATEMENT', %s, %s)
            """,
            (
                report_version_id,
                report_id,
                next_version,
                args.report_date,
                args.entity_code,
                effective,
                json.dumps(report, ensure_ascii=False, default=json_default),
                snapshot_hash,
            ),
        )
        cursor.execute(
            """
            INSERT INTO ads.ads_restatement_log
                (original_report_id, new_report_id, report_date, entity_code,
                 reason, requested_by, approved_by, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, 'APPLIED')
            """,
            (
                original_report_version_id,
                report_version_id,
                args.report_date,
                args.entity_code,
                args.reason,
                args.requested_by,
                args.approved_by,
            ),
        )
    connection.commit()

    print(f"[register] {original_report_version_id} → {report_version_id} 重述已登记（APPLIED）")
    print(f"           原因：{args.reason}（申请人 {args.requested_by}）")
    return 0


def main() -> int:
    args = parse_args()
    connection = pg_connection()
    try:
        if args.mode == "capture":
            return capture(connection, args)
        return register(connection, args)
    finally:
        connection.close()


if __name__ == "__main__":
    sys.exit(main())
