# [AI-GENERATED] model=deepseek-flash date=2026-09-17 reviewed_by=pending
"""重新应用 PostgreSQL 侧的库对象定义（建表 / 迁移 / 授权 / 触发器）。

它为什么还在（导出已经用 truncate=true 保住元数据了）：

    Spark 的 JDBC 覆盖写默认是 DROP + CREATE（文档里 truncate 选项默认 false），
    会把表上的授权、触发器与库侧列一起抹掉且不报错。导出作业已经显式打开
    truncate=true 绕开这条路。但**模型结构变化时 truncate 走不通**（文档明说
    「will not work ... when the new data has a different schema」），
    那种情况下导出会失败并提示补迁移，而不是继续用删除重建。

    所以这个环节的职责从「每次补回丢失的授权」变成了两件事：
      1. 在导出之前施加结构迁移与授权 —— 迁移必须先于灌数，否则灌数会因结构不齐失败；
      2. 核对没有任何对象是「零授权」的 —— 万一哪天有人把 truncate 选项去掉或
         走了删除重建的老路，这一条会立刻让流水线变红，而不是等到某天有人读不到数据。

为什么由本脚本施加而不是 shell 调 psql：
    Server 2 上没有 psql 客户端，而 PostgreSQL 又跑在 Server 1 的容器里。
    psycopg2 直连即可，不必为了执行几条 DDL 去装客户端。
    多语句文本 psycopg2 可以直接执行（服务端自己解析，含 DO $$ ... $$ 块）。

单一来源：本脚本不写任何 DDL，只按文件名顺序执行 sql/postgres/*.sql。
DDL 与授权永远只有那一份，脚本只负责「什么时候施加」。

用法（Server 2，用 venv 里的 python 直接跑）：
    ./venv/bin/python /opt/fr2052a-app/python/governance/publish_access.py
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import psycopg2

DEFAULT_SQL_DIR = "/opt/fr2052a-app/sql/postgres"


def parse_args() -> argparse.Namespace:
    """解析命令行参数。"""
    parser = argparse.ArgumentParser(description="重新应用库对象定义")
    parser.add_argument("--sql-dir", default=DEFAULT_SQL_DIR, help="SQL 文件目录")
    return parser.parse_args()


def pg_connection() -> psycopg2.extensions.connection:
    """连接 Server 1 的 PostgreSQL。"""
    connection = psycopg2.connect(
        host=os.environ["SERVER1_HOST"],
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )
    # DDL 不需要事务包裹，逐条生效；失败即中断并报出是哪个文件。
    connection.autocommit = True
    return connection


def main() -> int:
    """重新应用建表与授权脚本，并检查有没有对象一条授权都没有。"""
    args = parse_args()
    sql_dir = Path(args.sql_dir)
    if not sql_dir.is_dir():
        print(f"找不到 SQL 目录：{sql_dir}（需要先把 app/ 同步到服务器）")
        return 1

    files = sorted(sql_dir.glob("*.sql"))
    if not files:
        print(f"{sql_dir} 下没有 .sql 文件")
        return 1

    connection = pg_connection()
    try:
        for path in files:
            statements = path.read_text(encoding="utf-8")
            with connection.cursor() as cursor:
                try:
                    cursor.execute(statements)
                except psycopg2.Error as error:
                    print(f"  [FAIL] {path.name}：{str(error).strip().splitlines()[0]}")
                    return 1
            print(f"  [OK]   {path.name} 已应用")

        # 复核：授权不是写进文件就算生效，读一次 pg_class 的权限位才算。
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT c.relname, count(a.privilege_type) AS privileges
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                LEFT JOIN information_schema.role_table_grants a
                       ON a.table_schema = n.nspname AND a.table_name = c.relname
                WHERE n.nspname IN ('ads', 'audit', 'secure') AND c.relkind = 'r'
                GROUP BY c.relname
                ORDER BY c.relname
                """
            )
            rows = cursor.fetchall()
    finally:
        connection.close()

    print()
    print("对象授权条数（0 表示该表没有任何角色被授权）：")
    unsecured = [name for name, count in rows if count == 0]
    for name, count in rows:
        print(f"  {name:<34} {count}")

    print()
    if unsecured:
        print(f"以下对象没有任何授权，需检查 sql/postgres/ 的授权语句：{unsecured}")
        return 1
    print(f"完成：{len(files)} 个 SQL 文件已应用，{len(rows)} 个对象均有授权")
    return 0


if __name__ == "__main__":
    sys.exit(main())
