#!/usr/bin/env bash
# [AI-GENERATED] model=deepseek-flash date=2026-09-17 reviewed_by=pending
# 把 sql/postgres/ 下的建表脚本按文件名顺序应用到 Server 1 的 PostgreSQL。
#
# 为什么不在 docker-compose 里挂 /docker-entrypoint-initdb.d：
#   那份目录只在数据卷为空时执行一次。本项目的表结构会随功能演进（E6/E7 要加控制表），
#   需要可反复执行的入口，而不是「首次建库才生效」。
#
# 幂等性由 SQL 自己保证（全部 CREATE ... IF NOT EXISTS 与 DO 块判存在），
# 本脚本只负责按顺序投喂并让错误立刻冒出来（ON_ERROR_STOP）。
#
# 用法（在开发机或 Server 1 上执行，需能免密 ssh 到 SERVER1_HOST）：
#   bash deploy/server1/apply-sql.sh              # 应用全部
#   bash deploy/server1/apply-sql.sh 10_control_tables.sql   # 只应用指定文件

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SQL_DIR="$PROJECT_ROOT/sql/postgres"

SERVER1_HOST="${SERVER1_HOST:-192.168.17.22}"
POSTGRES_USER="${POSTGRES_USER:-fr2052a}"
POSTGRES_DB="${POSTGRES_DB:-fr2052a_db}"
PG_CONTAINER="${PG_CONTAINER:-fr2052a_postgres}"

if [ ! -d "$SQL_DIR" ]; then
  echo "找不到 SQL 目录：$SQL_DIR" >&2
  exit 1
fi

if [ "$#" -gt 0 ]; then
  files=("$@")
else
  files=()
  while IFS= read -r file; do
    files+=("$(basename "$file")")
  done < <(find "$SQL_DIR" -maxdepth 1 -name '*.sql' | sort)
fi

if [ "${#files[@]}" -eq 0 ]; then
  echo "没有待应用的 SQL 文件" >&2
  exit 1
fi

for file in "${files[@]}"; do
  path="$SQL_DIR/$file"
  if [ ! -f "$path" ]; then
    echo "找不到 $path" >&2
    exit 1
  fi
  echo "──── 应用 $file"
  # 逐字透传文件内容，不在本地做任何替换 —— SQL 里的 $$ 等符号不能被 shell 碰到。
  ssh "$SERVER1_HOST" \
    "docker exec -i $PG_CONTAINER psql -v ON_ERROR_STOP=1 -q -U $POSTGRES_USER -d $POSTGRES_DB" \
    < "$path"
  echo "  [OK] $file"
done

echo
echo "完成：${#files[@]} 个 SQL 文件已应用（库 $POSTGRES_DB）"
