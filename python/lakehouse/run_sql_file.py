r"""执行一个 SQL 文件：逐条语句交给 Spark SQL 跑，任一失败即退出非零。

用途：把建表脚本（sql/iceberg/*.sql）应用到数据湖，以及跑临时诊断查询。
之所以单独写一个执行器而不是直接把 DDL 塞进 Python，是为了让语句保持在可读、
可 diff、可 Git 管理的 .sql 文件里。

用法（Server 2，经 spark-submit 包装脚本执行）：
    bash spark-submit-fr2052a.sh \
        /opt/fr2052a-app/python/lakehouse/run_sql_file.py \
        /opt/fr2052a-app/sql/iceberg/01_create_ref_tables.sql

    # 诊断查询：把结果打出来（SELECT/EXPLAIN/SHOW/DESCRIBE 会打印，写语句不会）
    bash spark-submit-fr2052a.sh .../run_sql_file.py /tmp/q.sql --show

限制：按 `--` 剥注释后以分号切分语句。当前脚本里没有字符串字面量含分号或 `--`，
因此这个切分方式够用；将来若出现，需要换成真正的 SQL 解析。
"""

from __future__ import annotations

import sys
from pathlib import Path

from pyspark.sql import SparkSession

# 只有这些开头的语句会把结果打出来。写语句没有结果行，打印它们只会刷屏。
RESULT_HEAD_KEYWORDS = ("select", "with", "explain", "show", "describe", "desc ")


def load_statements(sql_path: Path) -> list[str]:
    """剥掉行注释后按分号切分，返回非空语句列表。"""
    kept_lines = []
    for raw_line in sql_path.read_text(encoding="utf-8").splitlines():
        without_comment = raw_line.split("--", 1)[0]
        if without_comment.strip():
            kept_lines.append(without_comment)
    return [statement.strip() for statement in "\n".join(kept_lines).split(";") if statement.strip()]


def summarize(statement: str) -> str:
    """取语句前 60 个字符做单行摘要，便于日志里对号入座。"""
    return " ".join(statement.split())[:60]


def main(argv: list[str]) -> int:
    """把 SQL 文件里的语句逐条交给 Spark SQL 执行，任一失败即退出非零。"""
    arguments = [item for item in argv[1:] if item != "--show"]
    show_results = "--show" in argv

    if len(arguments) != 1:
        print(f"用法: {argv[0]} <sql 文件路径> [--show]", file=sys.stderr)
        return 2

    sql_path = Path(arguments[0])
    if not sql_path.is_file():
        print(f"SQL 文件不存在: {sql_path}", file=sys.stderr)
        return 2

    statements = load_statements(sql_path)
    if not statements:
        print(f"SQL 文件里没有可执行语句: {sql_path}", file=sys.stderr)
        return 2

    spark = SparkSession.builder.appName("fr2052a-run-sql").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print(f"执行 {sql_path.name}，共 {len(statements)} 条语句")
    for index, statement in enumerate(statements, start=1):
        try:
            frame = spark.sql(statement)
        except Exception as error:  # noqa: BLE001 - 需要把原始错误完整打出来再退出
            print(f"  [{index}/{len(statements)}] 失败: {summarize(statement)}")
            print(f"       {type(error).__name__}: {error}", file=sys.stderr)
            return 1
        print(f"  [{index}/{len(statements)}] OK: {summarize(statement)}")
        if show_results and statement.strip().lower().startswith(RESULT_HEAD_KEYWORDS):
            rows = frame.collect()
            if not rows:
                print("        （无结果行）")
            for row in rows[:50]:
                print(f"        {row.asDict() if hasattr(row, 'asDict') else row}")
            if len(rows) > 50:
                print(f"        … 共 {len(rows)} 行，只打印前 50 行")

    for namespace in ("ref", "bronze"):
        try:
            tables = sorted(row[1] for row in spark.sql(f"SHOW TABLES IN {namespace}").collect())
        except Exception:  # noqa: BLE001 - 命名空间不存在时只提示，不影响退出码
            print(f"命名空间 {namespace} 不存在")
            continue
        print(f"命名空间 {namespace}：{len(tables)} 张表 {tables}")

    print("SQL 文件执行完成")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
