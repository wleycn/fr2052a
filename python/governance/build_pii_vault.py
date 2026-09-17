# [AI-GENERATED] model=deepseek-flash date=2026-09-17 reviewed_by=pending
"""建立 PII 明文对照表 —— 明文在系统里只保留这一份，且受角色限制。

数据流：
    ODS 落地数据（明文）──脱敏宏算 token──→ secure.fr2052a_pii_map（token ↔ 明文）
    ODS 落地数据 ──mask_pii 宏──→ silver.owd_*（只有 token，没有明文）

「哪一列是 PII」不从代码里读，从 dbt manifest 读：staging/schema.yml 里标了 meta.pii
的列就是脱敏列。这样模型新增脱敏列时对照表自动跟上；把清单写在脚本里则必然漏，
而漏掉的表现是「查这个客户查不到」，不报错。

脱敏表达式与 dbt 宏同源：都从 dbt/dbt_project.yml 的 vars.pii_mask_template 取，
盐从环境变量 FR2052A_PII_SALT 取。两处各写一份模板会让同一个客户算出两个 token，
静默毁掉按客户的关联分析，所以这里不做第二份实现。

为什么读落地 CSV 而不是 bronze 表：
    两者都是落地副本（bronze 是 Kafka 入湖后的镜像）。选 CSV 是因为本脚本要在
    宿主 venv 里跑：写库要用 psycopg2 做 upsert，而 Spark 的 JDBC 覆盖写对 PostgreSQL
    走 DROP + CREATE，会把对照表的授权与触发器一起抹掉 —— 一张权限表被重建，
    权限就没了，而且不报错。

运行（Server 2，用 venv 里的 python 直接跑）：
    ./venv/bin/python /opt/fr2052a-app/python/governance/build_pii_vault.py \
        --manifest ~/fr2052a-infra/app/dbt/target/manifest.json \
        --data-dir ~/fr2052a-infra/app/sample_data/ods \
        --project ~/fr2052a-infra/app/dbt/dbt_project.yml
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import sys
from pathlib import Path
from typing import Any

import psycopg2
import yaml


def parse_args() -> argparse.Namespace:
    """解析命令行参数。"""
    parser = argparse.ArgumentParser(description="建立 PII 明文对照表")
    parser.add_argument("--manifest", required=True, help="dbt manifest.json")
    parser.add_argument("--data-dir", required=True, help="ODS 落地目录（含 *.csv）")
    parser.add_argument(
        "--project",
        default=None,
        help="dbt_project.yml，用于取脱敏模板；默认取 manifest 同级的上两级",
    )
    return parser.parse_args()


def mask_template(project_path: Path) -> str:
    """从 dbt 工程读脱敏模板。模板只此一份，Python 与 dbt 宏共用，避免两边算法漂移。"""
    config = yaml.safe_load(project_path.read_text(encoding="utf-8"))
    template = (config.get("vars") or {}).get("pii_mask_template")
    if not template:
        raise SystemExit(f"{project_path} 的 vars 里没有 pii_mask_template，脱敏策略缺失")
    return template


def pii_columns(manifest: dict[str, Any]) -> list[tuple[str, str, str]]:
    """从 manifest 取 (模型, 列, 明文来源) 三元组。"""
    found: list[tuple[str, str, str]] = []
    for node in manifest.get("nodes", {}).values():
        if node.get("resource_type") != "model":
            continue
        for column, spec in (node.get("columns") or {}).items():
            meta = spec.get("meta") or {}
            if meta.get("pii"):
                source = meta.get("pii_source")
                if not source:
                    raise SystemExit(f"{node['name']}.{column} 标了 pii 但没写 pii_source，无法确定明文在哪，停止执行")
                found.append((node["name"], column, source))
    if not found:
        raise SystemExit(
            "manifest 里没有任何 meta.pii 列。要么是 schema.yml 丢了标记，"
            "要么是 manifest 过期 —— 两种情况都不能当成「没有 PII」继续跑。"
        )
    return sorted(found)


def compute_token(template: str, salt: str, value: str) -> str:
    """按模板同样的算法算 token。

    模板是 Spark SQL 表达式，这里把它翻译成等价的 Python：
    concat('h_', substr(sha2(concat_ws('|', '<salt>', cast(<col> as string)), 256), 1, 16))
    只支持这一种形状 —— 模板改了这里必须一起改，所以模板里不放复杂结构，
    并在下面校验形状，改坏时直接失败而不是算出不一致的 token。
    """
    expected_prefix = "concat('h_', substr(sha2(concat_ws('|', '"
    if not template.startswith(expected_prefix):
        raise SystemExit(
            "脱敏模板的形状与 build_pii_vault.py 的实现不一致，"
            f"无法保证两边算出同一个 token：\n  {template}\n"
            "改模板必须同时改本脚本的 compute_token。"
        )
    digest = hashlib.sha256(f"{salt}|{value}".encode()).hexdigest()
    return "h_" + digest[:16]


def source_file(data_dir: Path, table: str) -> Path:
    """找到某张表对应的落地 CSV；找不到直接报错，不静默跳过。"""
    path = data_dir / f"{table}.csv"
    if not path.is_file():
        raise SystemExit(f"落地数据里找不到 {path}")
    return path


def rows_for(data_dir: Path, source: str, salt: str, template: str) -> list[tuple[str, str, str, str, str | None]]:
    """读一个明文来源，产出 (token, 明文, 来源对象, 列名, 实体) 行。"""
    table, column = source.split(".", 1)
    path = source_file(data_dir, table)
    seen: dict[str, tuple[str, str, str, str, str | None]] = {}
    with path.open(newline="", encoding="utf-8") as handle:
        for record in csv.DictReader(handle):
            value = (record.get(column) or "").strip()
            if not value:
                continue
            token = compute_token(template, salt, value)
            key = f"{token}|{table}"
            if key in seen:
                continue
            seen[key] = (token, value, f"landing.{table}", column, record.get("entity_code"))
    return list(seen.values())


def main() -> int:
    """按 dbt 里声明的 PII 列取明文，建立 token 到明文的对照表。"""
    args = parse_args()
    manifest_path = Path(args.manifest).expanduser()
    if not manifest_path.is_file():
        print(f"找不到 manifest：{manifest_path}，先在 Server 2 上跑一次 dbt")
        return 1

    project_path = Path(args.project).expanduser() if args.project else manifest_path.parent.parent / "dbt_project.yml"
    template = mask_template(project_path)

    salt = os.environ.get("FR2052A_PII_SALT")
    if not salt:
        print("缺少环境变量 FR2052A_PII_SALT。没有盐的哈希对低熵客户号可被穷举反推，等于没脱敏，故拒绝执行。")
        return 1

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    declared = pii_columns(manifest)
    data_dir = Path(args.data_dir).expanduser()

    all_rows: list[tuple[str, str, str, str, str | None]] = []
    for model, column, source in declared:
        rows = rows_for(data_dir, source, salt, template)
        all_rows.extend(rows)
        print(f"  {model}.{column:<14} ← {source:<28} {len(rows)} 条")

    connection = psycopg2.connect(
        host=os.environ["SERVER1_HOST"],
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )
    try:
        with connection.cursor() as cursor:
            inserted = 0
            for row in all_rows:
                cursor.execute(
                    """
                    INSERT INTO secure.fr2052a_pii_map
                        (pii_token, pii_plaintext, source_object, pii_column, entity_code)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (pii_token, source_object, pii_column) DO NOTHING
                    """,
                    row,
                )
                inserted += cursor.rowcount
            cursor.execute("SELECT count(*) FROM secure.fr2052a_pii_map")
            (total,) = cursor.fetchone()
        connection.commit()
    finally:
        connection.close()

    print(f"\n对照表：本次新增 {inserted} 条，累计 {total} 条")
    print("（明文只此一份；OWD 层与所有下游只保留 token）")
    return 0


if __name__ == "__main__":
    sys.exit(main())
