# [AI-GENERATED] model=deepseek-flash date=2026-09-17 reviewed_by=pending
"""报送文件生成 —— 把报送服务层的报表装成监管要的格式并留下回执。

生成三种格式，各有各的用途，不是同一份内容换三个后缀：

    XBRL  监管接收系统解析用。本演示生成的是结构合法的 XBRL 2.1 实例文档
          （xbrli 上下文 + 单位 + 事实元素），但**不是** Federal Reserve 的真实分类标准：
          真实报送需要按官方 taxonomy 打标签，那套 taxonomy 不在本仓库也不在演示范围。
          文件头有显式声明，避免被误当成可直接提交监管的产物。
    XML   内部系统与对账工具读取用，结构简单、字段名即列名。
    CSV   人工复核与差异比对用，一行一个实体。

一个报数主体一份文件：
    FR 2052a 按法人实体分别报送，集团合并口径由母公司另报一份，所以本演示对
    ads_fr2052a_report 的每一行生成三个文件，不是整批一个文件。
    文件名直接用 report_id —— 区位码本身已说明是谁、什么报表、哪一期、什么口径，
    加上扩展名即可自解释：ENT001-FR2052A-20260916-01.xbrl

落库与幂等：
    ads.ads_fr2052a_submission 按（报告日, 实体, 格式, 文件哈希）唯一，一行一个文件。
    重复跑同一批不会堆重复行 —— 内容没变就只是刷新时间戳。

为什么读 PostgreSQL 而不是数据湖：
    报送服务读的是报送服务层（PG 的 ads 层），不是湖里的中间态。而且这样能在
    venv 里直接跑，不必为了拼几个字符串起 Spark 会话。

运行（Server 2，用 venv 里的 python 直接跑）：
    ./venv/bin/python /opt/fr2052a-app/python/exporters/generate_submission.py \
        --report-date 2026-09-16 --output-dir /home/hermes/fr2052a-infra/submissions
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import sys
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from xml.etree import ElementTree as ET

import psycopg2
import psycopg2.extras

XBRL_NAMESPACE = "http://www.xbrl.org/2003/instance"
ISO4217_NAMESPACE = "http://www.xbrl.org/2003/iso4217"

# 送监管的格式集合。三种格式同批生成，缺任何一种是交付不完整。
FILE_FORMATS = ("XBRL", "XML", "CSV")


def parse_args() -> argparse.Namespace:
    """解析命令行参数。"""
    parser = argparse.ArgumentParser(description="报送文件生成")
    parser.add_argument("--report-date", required=True, help="报告日，格式 YYYY-MM-DD")
    parser.add_argument(
        "--output-dir",
        default="/home/hermes/fr2052a-infra/submissions",
        help="报送文件落盘根目录，按报告日分子目录",
    )
    parser.add_argument(
        "--receipt-file",
        default=None,
        help="外部回执 JSON 路径；不给则生成模拟回执（演示环境无真实监管网关）",
    )
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


def report_columns(cursor: psycopg2.extensions.cursor) -> list[str]:
    """报表的列清单取自库本身，不写死在代码里 —— 报表加列时不必改这里。

    调用方传的是 RealDictCursor，所以按列名取值，不能用 row[0]。
    """
    cursor.execute(
        """
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'ads' AND table_name = 'ads_fr2052a_report'
        ORDER BY ordinal_position
        """
    )
    return [row["column_name"] for row in cursor.fetchall()]


def fetch_report_rows(cursor: psycopg2.extensions.cursor, report_date: str) -> list[dict[str, Any]]:
    """取某报告日全部实体的报表行，一个实体一套文件。"""
    with cursor:
        cursor.execute(
            "SELECT * FROM ads.ads_fr2052a_report WHERE report_date = %s ORDER BY entity_code",
            (report_date,),
        )
        return [dict(row) for row in cursor.fetchall()]


def to_amount(value: Any) -> str:
    """金额统一两位小数。None 表示「本演示无此业务」，导出成空而不是 0。"""
    if value is None:
        return ""
    return f"{Decimal(value):.2f}"


def write_csv_file(path: Path, columns: list[str], rows: list[dict[str, Any]]) -> None:
    """按给定列序写出 CSV。"""
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(columns)
        for row in rows:
            writer.writerow(
                [
                    to_amount(row[name]) if isinstance(row[name], (int, float, Decimal)) else (row[name] or "")
                    for name in columns
                ]
            )


def write_xml_file(path: Path, report_date: str, columns: list[str], rows: list[dict[str, Any]]) -> None:
    """写出 XML 格式的报送文件。"""
    root = ET.Element("FR2052aReport", {"reportDate": report_date, "reportingCurrency": "USD"})
    for row in rows:
        entity = ET.SubElement(
            root,
            "Entity",
            {
                "entityCode": str(row["entity_code"]),
                "reportId": str(row["report_id"]),
                "consolidated": "true" if row["is_consolidated"] else "false",
            },
        )
        for name in columns:
            if name in ("entity_code", "is_consolidated", "report_date", "report_id"):
                continue
            value = row[name]
            # 无数据的 Section 不出元素，而不是出空元素：报表里 NULL 的含义是
            # 「本机构没有这类业务」，与「金额为零」是两回事。
            if value is None:
                continue
            ET.SubElement(entity, "LineItem", {"code": name}).text = to_amount(value)
    ET.indent(root, space="  ")
    ET.ElementTree(root).write(path, encoding="utf-8", xml_declaration=True)


def write_xbrl_file(path: Path, report_date: str, rows: list[dict[str, Any]]) -> None:
    """生成 XBRL 2.1 实例文档。

    结构遵循 XBRL 2.1：每个实体一个 context（期间为报告日），金额事实带 unitRef 指向 USD。
    分类标准用本项目的占位命名空间，不是官方 taxonomy —— 文件头有明确声明。
    """
    ET.register_namespace("xbrli", XBRL_NAMESPACE)
    ET.register_namespace("iso4217", ISO4217_NAMESPACE)
    ET.register_namespace("fr2052a", "http://example.org/fr2052a/demo")

    root = ET.Element(f"{{{XBRL_NAMESPACE}}}xbrl")

    unit = ET.SubElement(root, f"{{{XBRL_NAMESPACE}}}unit", {"id": "usd"})
    ET.SubElement(unit, f"{{{XBRL_NAMESPACE}}}measure").text = f"{{{ISO4217_NAMESPACE}}}USD"

    contexts: list[tuple[str, dict[str, Any]]] = []
    for row in rows:
        # context 用 report_id 命名：一个报送主体一个 context，
        # 名字里带上报表身份，读 XBRL 的人不必回头查 entity_code 对应哪份报表。
        context_id = f"ctx-{row['report_id']}"
        context = ET.SubElement(root, f"{{{XBRL_NAMESPACE}}}context", {"id": context_id})
        entity = ET.SubElement(context, f"{{{XBRL_NAMESPACE}}}entity")
        ET.SubElement(
            entity,
            f"{{{XBRL_NAMESPACE}}}identifier",
            {"scheme": "http://example.org/fr2052a/entity-id"},
        ).text = str(row["entity_code"])
        period = ET.SubElement(context, f"{{{XBRL_NAMESPACE}}}period")
        ET.SubElement(period, f"{{{XBRL_NAMESPACE}}}instant").text = report_date
        contexts.append((context_id, row))

    for context_id, row in contexts:
        for name in sorted(row):
            if not name.startswith("sec_") or row[name] is None:
                continue
            ET.SubElement(
                root,
                "{http://example.org/fr2052a/demo}" + name,
                {"contextRef": context_id, "unitRef": "usd", "decimals": "2"},
            ).text = to_amount(row[name])

    ET.indent(root, space="  ")
    tree = ET.ElementTree(root)
    tree.write(path, encoding="utf-8", xml_declaration=True)

    # ElementTree 不支持写注释声明，改为追加处理指令式的说明行，供阅读者一眼看清来源。
    notice = (
        "<!-- 本文件为 FR 2052a 演示项目生成的 XBRL 2.1 结构实例，"
        "使用占位分类标准命名空间 http://example.org/fr2052a/demo；"
        "非 Federal Reserve 官方 taxonomy，不可直接提交监管。 -->\n"
    )
    content = path.read_text(encoding="utf-8")
    header, _, body = content.partition("\n")
    path.write_text(f"{header}\n{notice}{body}", encoding="utf-8")


def sha256_of(path: Path) -> str:
    """算文件摘要，作为台账里可比对的指纹。"""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def simulated_receipt(report_id: str, file_hash: str) -> dict[str, Any]:
    """演示环境的模拟回执，一个报送主体一份。

    真实环境这里应接监管网关并以它返回的回执为准：回执号由监管系统编，
    不是报送方自己算出来的。这里用文件哈希派生只是在没有网关时的替代做法。
    """
    return {
        "receipt_id": f"SIM-{file_hash[:12].upper()}",
        "report_id": report_id,
        "accepted": True,
        "message": "演示环境模拟回执：未连接 Federal Reserve 真实报送网关，回执号由文件哈希派生。",
        "received_at": datetime.now(UTC).isoformat(),
    }


def upsert_submission(cursor: psycopg2.extensions.cursor, rows: list[dict[str, Any]]) -> None:
    """把报送台账按报告日、实体、格式、摘要四个字段幂等写入。"""
    for row in rows:
        cursor.execute(
            """
            INSERT INTO ads.ads_fr2052a_submission
                (report_id, report_date, entity_code, file_format, file_path,
                 file_hash, file_size_bytes, submitted_at, submission_status,
                 receipt_id, receipt_message)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (report_date, entity_code, file_format, file_hash) DO UPDATE SET
                file_path = EXCLUDED.file_path,
                file_size_bytes = EXCLUDED.file_size_bytes,
                submitted_at = EXCLUDED.submitted_at,
                submission_status = EXCLUDED.submission_status,
                receipt_id = EXCLUDED.receipt_id,
                receipt_message = EXCLUDED.receipt_message
            """,
            (
                row["report_id"],
                row["report_date"],
                row["entity_code"],
                row["file_format"],
                row["file_path"],
                row["file_hash"],
                row["file_size_bytes"],
                row["submitted_at"],
                row["submission_status"],
                row["receipt_id"],
                row["receipt_message"],
            ),
        )


def main() -> int:
    """为每个实体各生成三种格式的报送文件，并把台账登记落库。"""
    args = parse_args()
    output_dir = Path(args.output_dir) / args.report_date
    output_dir.mkdir(parents=True, exist_ok=True)

    connection = pg_connection()
    try:
        with connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            columns = report_columns(cursor)
            rows = fetch_report_rows(cursor, args.report_date)
        if not rows:
            print(f"ads.ads_fr2052a_report 无 {args.report_date} 的数据，无从生成报送文件。")
            return 1

        # 一个报数主体一份文件。report_id 做文件名，三份文件同一个前缀，
        # 人工翻目录时不必打开文件就知道哪几份属于同一次报送。
        external_receipt = None
        if args.receipt_file:
            external_receipt = json.loads(Path(args.receipt_file).read_text(encoding="utf-8"))

        submitted_at = datetime.now(UTC).replace(tzinfo=None)
        submission_rows: list[dict[str, Any]] = []
        written: list[tuple[str, str, Path]] = []
        receipts: list[tuple[str, dict[str, Any]]] = []

        for row in rows:
            report_id = row["report_id"]
            files: list[tuple[str, Path]] = [
                ("XBRL", output_dir / f"{report_id}.xbrl"),
                ("XML", output_dir / f"{report_id}.xml"),
                ("CSV", output_dir / f"{report_id}.csv"),
            ]
            write_xbrl_file(files[0][1], args.report_date, [row])
            write_xml_file(files[1][1], args.report_date, columns, [row])
            write_csv_file(files[2][1], columns, [row])

            # 外部回执是整批一份的演示替身，按同一状态套用到本批每个报送主体。
            receipt = external_receipt or simulated_receipt(report_id, sha256_of(files[0][1]))
            receipts.append((report_id, receipt))

            for file_format, path in files:
                submission_rows.append(
                    {
                        "report_id": report_id,
                        "report_date": args.report_date,
                        "entity_code": row["entity_code"],
                        "file_format": file_format,
                        "file_path": str(path),
                        "file_hash": sha256_of(path),
                        "file_size_bytes": path.stat().st_size,
                        "submitted_at": submitted_at,
                        "submission_status": "ACCEPTED" if receipt.get("accepted") else "REJECTED",
                        "receipt_id": receipt.get("receipt_id"),
                        "receipt_message": receipt.get("message"),
                    }
                )
                written.append((report_id, file_format, path))

        with connection.cursor() as cursor:
            upsert_submission(cursor, submission_rows)
        connection.commit()

        print(f"报送文件（报告日 {args.report_date}，{len(rows)} 个报送主体，{len(written)} 个文件）：")
        for report_id, file_format, path in written:
            print(f"  [{file_format:<4}] {path.name:<32} {path.stat().st_size:>7} 字节  {report_id}")
        print()
        for report_id, receipt in receipts:
            accepted = "ACCEPTED" if receipt.get("accepted") else "REJECTED"
            print(f"回执 {report_id}：{receipt.get('receipt_id')}  状态 {accepted}")
        if receipts:
            print(f"      {receipts[0][1].get('message')}")
    finally:
        connection.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
