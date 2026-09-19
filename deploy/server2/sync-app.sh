#!/usr/bin/env bash
# [AI-GENERATED] model=deepseek-flash date=2026-09-17 reviewed_by=pending
# 把 dev 侧的应用代码与样本数据同步到 Server 2，供 Spark 容器内的作业读取。
#
# 为什么需要它：Spark 容器只挂载了 ~/fr2052a-infra 下的目录，dev 侧仓库里的
# python/ 与 sql/ 不会自己出现在容器里。
#
# 用法（在 dev 的仓库根目录执行）：
#   bash deploy/server2/sync-app.sh
#
# 环境变量（可选）：
#   SERVER2_HOST  默认 192.168.17.24
#   REMOTE_DIR    默认 /home/hermes/fr2052a-infra/app
#
# 同步后如需让容器看到新挂载点，在 Server 2 上执行一次
#   cd ~/fr2052a-infra && docker compose --env-file .env up -d

set -euo pipefail

SERVER2_HOST="${SERVER2_HOST:-192.168.17.24}"
REMOTE_DIR="${REMOTE_DIR:-/home/hermes/fr2052a-infra/app}"

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$REPO_ROOT"

if [ ! -d sample_data/ods ]; then
  echo "缺少 sample_data/ods，请先运行：python3 python/generators/generate_sample_data.py" >&2
  exit 1
fi

echo "同步到 ${SERVER2_HOST}:${REMOTE_DIR}"
ssh "$SERVER2_HOST" "mkdir -p '$REMOTE_DIR'"

# app 目录完全由本脚本托管：先清空再逐树同步。
# 这样既不会留下上次同步的残骸，也避开了 rsync 多源时 --delete 不覆盖目标根目录的语义坑。
ssh "$SERVER2_HOST" "find '$REMOTE_DIR' -mindepth 1 -maxdepth 1 -exec rm -rf {} +"

for tree in python sql dbt config sample_data; do
  rsync -az --delete \
    --exclude '__pycache__' \
    --exclude '*.pyc' \
    "$tree" "${SERVER2_HOST}:${REMOTE_DIR}/"
done

echo "同步完成，远端目录树："
ssh "$SERVER2_HOST" "find '$REMOTE_DIR' -maxdepth 3 -type d | sort"
