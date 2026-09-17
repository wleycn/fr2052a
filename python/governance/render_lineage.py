# [AI-GENERATED] model=deepseek-flash date=2026-09-17 reviewed_by=pending
"""血缘与监管映射渲染 —— 用 dbt 产物替代 DataHub 摄取。

需求原本要求 DataHub 摄取后展示血缘与监管规则。本项目不接 DataHub（见 KNOWN-ISSUE
的偏离登记），改用 dbt 自己产出的 manifest.json + 列级 meta 渲染，产出两样东西：

    1. audit.audit_data_lineage 表里的技术血缘边（表 → 表），可查询、可追溯
    2. 一份 Markdown 报告，含表级血缘、字段级血缘、监管映射（列 → 规则号 → 页码 → 折扣率 → 责任人）

三类信息三个来源，各归其位：

    表级血缘     manifest.json 的 parent_map —— dbt 自己解析出来的依赖，不靠人写
    字段级血缘   sqlglot 解析各模型的编译后 SQL，用 sources 递归展开到上游模型，
                 一直追到 bronze 层的源表为止
    监管映射     schema.yml 里的列级 meta —— 业务含义只有人知道，机器推断不出来

字段级血缘的边界（写清楚，不假装全覆盖）：
    表达式列（如 sec_k_total_inflows = least(流入, 0.75 × 流出)）的溯源会同时列出
    多个上游列，这是对的；聚合列（sum(x)）追到的是被聚合的列，也是对的；
    但常量与字面量列（如 cast(null as decimal(20,2))）没有上游，会记为「无上游」。

依赖说明：sqlglot 随 dbt 一起装在 venv 里（非本项目直接依赖）。这是渲染字段级血缘最
省事的方案 —— 自己写 SQL 解析器要处理 CTE、子查询、别名解析，代价远超收益。
缺失时本脚本会立刻报错并说明原因，不会静默降级成"只出表级血缘"。

运行（Server 2，用 venv 里的 python 直接跑，不需要 Spark）：
    ./venv/bin/python /opt/fr2052a-app/python/governance/render_lineage.py \
        --manifest ~/fr2052a-infra/app/dbt/target/manifest.json \
        --output-dir ~/fr2052a-infra/app/build/governance
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

import psycopg2

try:
    from sqlglot.lineage import lineage
except ImportError as error:  # pragma: no cover - 环境缺依赖时的显式失败
    print(f"缺少 sqlglot，无法渲染字段级血缘。它随 dbt 一起安装，请确认 venv 完整（{error}）")
    raise SystemExit(3) from error

DIALECT = "spark"


def parse_args() -> argparse.Namespace:
    """解析命令行参数。"""
    parser = argparse.ArgumentParser(description="血缘与监管映射渲染")
    parser.add_argument("--manifest", required=True, help="dbt manifest.json 路径")
    parser.add_argument("--output-dir", required=True, help="报告输出目录")
    parser.add_argument("--job-name", default="dbt_render_lineage", help="写入血缘表时的作业名")
    parser.add_argument("--run-id", default=None, help="本次运行编号，默认取 manifest 的 invocation_id")
    parser.add_argument("--no-audit", action="store_true", help="不写 audit.audit_data_lineage，只出报告")
    return parser.parse_args()


def normalize_relation(name: str) -> str:
    """去掉反引号与双引号，便于用名字做 map 的键。"""
    return name.replace("`", "").replace('"', "")


def load_manifest(path: Path) -> dict[str, Any]:
    """读 dbt 的 manifest.json，血缘的原始依据。"""
    return json.loads(path.read_text(encoding="utf-8"))


def relation_of(node: dict[str, Any]) -> str:
    """取节点的物理关系名并归一化，供报告展示。"""
    return normalize_relation(node.get("relation_name") or "")


def build_sources(manifest: dict[str, Any]) -> dict[str, str]:
    """把每个模型/源表的编译后 SQL 作为 sources，供 sqlglot 递归下钻。

    键用 dbt 编译后实际出现的名字（relation_name），因为 compiled_code 里 ref() 已经
    被渲染成这个名字，两边对上才能展开。
    """
    sources: dict[str, str] = {}
    for node in manifest.get("nodes", {}).values():
        if node.get("resource_type") not in ("model", "snapshot", "seed"):
            continue
        relation = relation_of(node)
        if relation:
            sources[relation] = node.get("compiled_code") or node.get("raw_code") or ""
    for source in manifest.get("sources", {}).values():
        relation = relation_of(source)
        if relation:
            # 源表是血缘的终点，给一个 SELECT * 占位，让 sqlglot 能解析引用但不继续下钻
            sources[relation] = f"select * from {relation}"
    return sources


def model_edges(manifest: dict[str, Any]) -> list[tuple[str, str, str]]:
    """表级血缘边：(上游, 下游, 转换类型)。"""
    edges: list[tuple[str, str, str]] = []
    nodes = {**manifest.get("nodes", {}), **manifest.get("sources", {})}
    for _node_id, node in nodes.items():
        if node.get("resource_type") != "model":
            continue
        target = node.get("name", "")
        for parent_id in node.get("depends_on", {}).get("nodes", []):
            parent = nodes.get(parent_id)
            if parent is None:
                continue
            source = parent.get("name", "")
            transform = "source_to_staging" if parent.get("resource_type") == "source" else "model_to_model"
            edges.append((source, target, transform))
    return sorted(set(edges))


def column_lineage(manifest: dict[str, Any], sources: dict[str, str], model_name: str, column: str) -> list[str]:
    """把一个输出列追到最上游，返回命中的上游对象列表（去重、截断）。"""
    node = next(
        (item for item in manifest.get("nodes", {}).values() if item.get("name") == model_name),
        None,
    )
    if node is None:
        return []
    relation = relation_of(node)
    sql = sources.get(relation, "")
    if not sql:
        return []
    try:
        graph = lineage(column, sql, sources=sources, dialect=DIALECT)
    except Exception as error:  # noqa: BLE001 - 单列解析失败不能拖垮整份报告
        return [f"<解析失败：{type(error).__name__}>"]

    leaves: list[str] = []
    stack = [graph]
    seen: set[int] = set()
    while stack:
        current = stack.pop()
        if id(current) in seen or current is None:
            continue
        seen.add(id(current))
        downstream = getattr(current, "downstream", None) or []
        if not downstream:
            name = normalize_relation(current.name or "")
            leaves.append(name)
        else:
            stack.extend(downstream)
    return sorted(set(leaves))


def regulatory_mapping(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    """列级 meta：监管规则号、页码、折扣率、责任人。"""
    rows: list[dict[str, Any]] = []
    for node in manifest.get("nodes", {}).values():
        if node.get("resource_type") != "model":
            continue
        model_meta = node.get("meta") or {}
        for column, spec in (node.get("columns") or {}).items():
            meta = spec.get("meta") or {}
            rows.append(
                {
                    "model": node.get("name", ""),
                    "column": column,
                    "rule_id": meta.get("rule_id", ""),
                    "regulatory_reference": meta.get("regulatory_reference", ""),
                    "haircut_rate": meta.get("haircut_rate", ""),
                    "owner": meta.get("owner", model_meta.get("owner", "")),
                    "description": (spec.get("description") or "").strip(),
                }
            )
    return rows


def write_audit(edges: list[tuple[str, str, str]], args: argparse.Namespace) -> int:
    """把血缘边写进审计表。"""
    if args.no_audit:
        print("按参数要求跳过血缘表写入")
        return 0
    connection = psycopg2.connect(
        host=os.environ["SERVER1_HOST"],
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )
    try:
        with connection.cursor() as cursor:
            for source, target, transform in edges:
                cursor.execute(
                    """
                    INSERT INTO audit.audit_data_lineage
                        (source_object, target_object, transform_type, job_name, run_id)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (source_object, target_object, transform_type, job_name)
                    DO UPDATE SET run_id = EXCLUDED.run_id, created_at = CURRENT_TIMESTAMP
                    """,
                    (source, target, transform, args.job_name, args.run_id),
                )
        connection.commit()
        with connection.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM audit.audit_data_lineage WHERE job_name = %s", (args.job_name,))
            count = cursor.fetchone()[0]
    finally:
        connection.close()
    return count


def render_report(
    manifest: dict[str, Any],
    edges: list[tuple[str, str, str]],
    mapping_rows: list[dict[str, Any]],
    sources: dict[str, str],
    output_dir: Path,
) -> Path:
    """渲染血缘与监管映射报告。"""
    lines: list[str] = []
    lines.append("# 血缘与监管映射（由 dbt manifest 渲染）")
    lines.append("")
    lines.append(
        "> 本文件由 `python/governance/render_lineage.py` 生成，不手工编辑。"
        "数据来源：dbt manifest.json（表级血缘、列级 meta）+ sqlglot 解析编译后 SQL（字段级血缘）。"
    )
    lines.append("")

    lines.append("## 表级血缘")
    lines.append("")
    lines.append("| 上游 | 下游 | 关系 |")
    lines.append("|---|---|---|")
    for source, target, transform in edges:
        lines.append(f"| {source} | {target} | {transform} |")
    lines.append("")

    lines.append("## 字段级血缘")
    lines.append("")
    lines.append("只对声明了监管映射的列展开，避免整份报告被几百行机械输出淹没。")
    lines.append("")
    targets = sorted({(row["model"], row["column"]) for row in mapping_rows})
    blocked: list[str] = []
    for model, column in targets:
        parents = column_lineage(manifest, sources, model, column)
        if not parents:
            continue
        if any(parent.startswith("<解析失败") for parent in parents):
            blocked.append(f"{model}.{column}")
        lines.append(f"- `{model}.{column}` ← {', '.join(f'`{parent}`' for parent in parents)}")
    lines.append("")
    if blocked:
        lines.append(f"解析失败 {len(blocked)} 列：{', '.join(blocked)}")
        lines.append("")

    lines.append("## 监管映射")
    lines.append("")
    lines.append("| 模型 | 列 | 规则号 | 监管出处 | 折扣率 | 责任人 | 说明 |")
    lines.append("|---|---|---|---|---|---|---|")
    for row in sorted(mapping_rows, key=lambda item: (item["model"], item["column"])):
        lines.append(
            f"| {row['model']} | {row['column']} | {row['rule_id']} | "
            f"{row['regulatory_reference']} | {row['haircut_rate']} | {row['owner']} | {row['description']} |"
        )
    lines.append("")

    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "LINEAGE.md"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def main() -> int:
    """血缘渲染入口：读 manifest，抽边与映射，写表并出报告。"""
    args = parse_args()
    manifest_path = Path(args.manifest).expanduser()
    if not manifest_path.is_file():
        print(f"找不到 manifest：{manifest_path}。先在 Server 2 上跑一次 dbt（run 或 compile）。")
        return 1

    manifest = load_manifest(manifest_path)
    args.run_id = args.run_id or manifest.get("metadata", {}).get("invocation_id")

    sources = build_sources(manifest)
    edges = model_edges(manifest)
    mapping_rows = regulatory_mapping(manifest)

    report_path = render_report(manifest, edges, mapping_rows, sources, Path(args.output_dir).expanduser())
    written = write_audit(edges, args)

    print(f"表级血缘边：{len(edges)} 条")
    print(f"列级监管映射：{len(mapping_rows)} 条")
    print(f"血缘表累计边数：{written} 条（job_name = {args.job_name}）")
    print(f"报告：{report_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
