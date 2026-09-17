# [AI-GENERATED] model=deepseek-flash date=2026-09-17 reviewed_by=pending
"""报送文件完整性核对 —— 重新计算文件哈希与台账比对。

为什么不能只看「生成脚本返回 0」：
    脚本返回 0 只说明它跑完了。文件被截断、被覆盖、路径写错、哈希算错，
    都可能发生在返回 0 之后或之后的使用里。监管核验的是文件本身，
    所以这里从磁盘重新读一遍、重新算哈希，与台账逐条比对。

四类核对（按报送主体分组）：
    1. 每个报送主体三种格式齐全（XBRL / XML / CSV），缺一即交付不完整
    2. 台账里每个文件都真实存在，且重算的 SHA-256 与字节数与台账一致
    3. 同一主体的台账 entity_code 唯一，不会出现一份文件挂两个报送主体
    4. report_id 里的报告期段与台账的报告日一致，防止「标识与数据对不上」

用法（Server 2，用 venv 里的 python 直接跑）：
    ./venv/bin/python /opt/fr2052a-app/python/validators/verify_submission.py \
        --report-date 2026-09-16
"""

from __future__ import annotations

import argparse
import hashlib
import os
import sys
from pathlib import Path

import psycopg2
import psycopg2.extras

EXPECTED_FORMATS = ("XBRL", "XML", "CSV")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="报送文件完整性核对")
    parser.add_argument("--report-date", required=True, help="报告日，格式 YYYY-MM-DD")
    return parser.parse_args()


def pg_connection():
    return psycopg2.connect(
        host=os.environ["SERVER1_HOST"],
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> int:
    args = parse_args()
    connection = pg_connection()
    try:
        with connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            cursor.execute(
                """
                SELECT report_id, entity_code, file_format, file_path, file_hash,
                       file_size_bytes, submission_status, receipt_id
                FROM ads.ads_fr2052a_submission
                WHERE report_date = %s
                ORDER BY report_id, file_format
                """,
                (args.report_date,),
            )
            rows = [dict(row) for row in cursor.fetchall()]
    finally:
        connection.close()

    if not rows:
        print(f"[FAIL] {args.report_date} 没有任何报送记录，先跑 submission 环节")
        return 1

    by_report: dict[str, list[dict]] = {}
    for row in rows:
        by_report.setdefault(row["report_id"], []).append(row)

    # 报告日在区位码里的形态是 YYYYMMDD，用来验证标识本身与台账日期一致。
    expected_period = args.report_date.replace("-", "")
    failures: list[str] = []
    print(f"报送文件核对（{args.report_date}）：{len(by_report)} 个报送主体，共 {len(rows)} 条台账")

    for report_id in sorted(by_report):
        report_rows = by_report[report_id]
        print(f"  {report_id}")

        formats = {row["file_format"] for row in report_rows}
        missing = [fmt for fmt in EXPECTED_FORMATS if fmt not in formats]
        if missing:
            print(f"    [FAIL] 缺少格式 {missing}")
            failures.append(f"{report_id} 缺少格式 {missing}")

        entities = {row["entity_code"] for row in report_rows}
        if len(entities) != 1:
            print(f"    [FAIL] entity_code 不唯一：{sorted(entities)}")
            failures.append(f"{report_id} 的 entity_code 不唯一")

        if expected_period not in report_id:
            print(f"    [FAIL] report_id 未包含报告期 {expected_period}")
            failures.append(f"{report_id} 的 report_id 与报告日不一致")

        for row in report_rows:
            path = Path(row["file_path"])
            if not path.is_file():
                print(f"    [FAIL] {row['file_format']:<4} 文件不存在：{path}")
                failures.append(f"{report_id} {row['file_format']} 文件缺失")
                continue
            actual_hash = sha256_of(path)
            actual_size = path.stat().st_size
            if actual_hash != row["file_hash"] or actual_size != row["file_size_bytes"]:
                print(f"    [FAIL] {row['file_format']:<4} 哈希或字节数与台账不一致：{path.name}")
                failures.append(f"{report_id} {row['file_format']} 哈希或大小不一致")
                continue
            print(
                f"    [OK  ] {row['file_format']:<4} {actual_size:>7} 字节  "
                f"哈希 {actual_hash[:16]}…  状态 {row['submission_status']}  回执 {row['receipt_id']}"
            )

    print()
    if failures:
        print(f"核对未通过（{len(failures)} 项）：")
        for item in failures:
            print(f"  - {item}")
        return 1
    print(f"完成：{len(by_report)} 个报送主体、{len(rows)} 个文件与台账逐条一致（哈希重算相符）")
    return 0


if __name__ == "__main__":
    sys.exit(main())
