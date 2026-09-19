#!/usr/bin/env bash
# [AI-GENERATED] model=deepseek-flash date=2026-09-17 reviewed_by=pending
# 把 deploy/ 下的部署清单同步到两台服务器，并提供漂移检查。
#
# 为什么需要它：
#   deploy/server2/sync-app.sh 只同步 python/sql/dbt/config/sample_data 到 app/ 目录，
#   部署脚本本身（run-daily-pipeline.sh、run-dbt.sh、DAG、compose 等）不在同步范围内。
#   结果是「仓库里改了、服务器上还是旧版」：实测踩过 —— E4.5 把跑批脚本改成分环节调度，
#   改动没到 Server 2，Airflow 每个任务都跑了一整条链路却全部显示成功。
#   本脚本把同步这件事变成一条命令，并让漂移可查。
#
# 用法（在开发机仓库根或 deploy/ 下设执行）：
#   bash deploy/sync-deploy.sh            # 同步到两台服务器
#   bash deploy/sync-deploy.sh --check    # 只比对，不改动；有漂移时退出码 1
#
# 有意不同步的东西：
#   .env / .env.orig  服务器上的真实凭据，仓库里只有模板
#   airflow/logs     运行日志
#   airflow/keys     SSH 私钥
#   以上路径在 rsync 里排除，且不加 --delete（远端还有 venv、jars、jdk17、app 等
#   由其他脚本托管的目录，误删代价远高于收益）。

set -euo pipefail

DEPLOY_DIR="$(cd "$(dirname "$0")" && pwd)"
SERVER1_HOST="${SERVER1_HOST:-192.168.17.22}"
SERVER2_HOST="${SERVER2_HOST:-192.168.17.24}"
REMOTE_DIR="${REMOTE_DIR:-/home/hermes/fr2052a-infra}"

MODE="${1:-sync}"
EXCLUDES=(
  --exclude='.env'
  --exclude='.env.orig'
  --exclude='airflow/logs'
  --exclude='airflow/keys'
)

# 需要纳入比对的文件：仓库里有、服务器上也应该有。用仓库侧清单当基准，
# 避免把服务器上运行时产生的目录（checkpoints、venv、jars）误判成漂移。
tracked_files() {
  local side="$1"
  (cd "$DEPLOY_DIR/$side" && find . -type f \
    -not -path './airflow/logs/*' \
    -not -path './airflow/keys/*' \
    -not -name '.env' -not -name '.env.orig' | sed 's|^\./||' | sort)
}

check_side() {
  local side="$1" host="$2" drift=0
  echo "──── 检查 $side → $host:$REMOTE_DIR"
  while IFS= read -r rel; do
    local local_sum remote_sum
    local_sum="$(md5sum "$DEPLOY_DIR/$side/$rel" | cut -d' ' -f1)"
    # ssh 必须带 -n：它会读走 stdin，而这里的 stdin 正是 while 循环的输入，
    # 不隔离的话循环只处理第一个文件就结束（实测踩过，会报出假的"无漂移"）。
    remote_sum="$(ssh -n "$host" "md5sum '$REMOTE_DIR/$rel' 2>/dev/null | cut -d' ' -f1" || true)"
    if [ -z "$remote_sum" ]; then
      echo "  [远端缺失] $rel"
      drift=$((drift + 1))
    elif [ "$local_sum" != "$remote_sum" ]; then
      echo "  [内容不同] $rel"
      drift=$((drift + 1))
    fi
  done < <(tracked_files "$side")
  if [ "$drift" -eq 0 ]; then
    echo "  [一致] 仓库里的部署文件与远端逐字节相同"
  fi
  return "$drift"
}

sync_side() {
  local side="$1" host="$2"
  echo "──── 同步 $side → $host:$REMOTE_DIR"
  ssh "$host" "mkdir -p '$REMOTE_DIR'"
  rsync -a "${EXCLUDES[@]}" "$DEPLOY_DIR/$side/" "$host:$REMOTE_DIR/"
  echo "  [OK] $side 已同步"
}

case "$MODE" in
  --check)
    drift=0
    check_side server1 "$SERVER1_HOST" || drift=$((drift + $?))
    check_side server2 "$SERVER2_HOST" || drift=$((drift + $?))
    echo
    if [ "$drift" -gt 0 ]; then
      echo "发现 $drift 处漂移。跑 bash deploy/sync-deploy.sh 修正。"
      exit 1
    fi
    echo "无漂移。"
    ;;
  sync)
    sync_side server1 "$SERVER1_HOST"
    sync_side server2 "$SERVER2_HOST"
    echo
    echo "同步完成。跑 bash deploy/sync-deploy.sh --check 复核。"
    ;;
  *)
    echo "用法：bash deploy/sync-deploy.sh [--check]" >&2
    exit 2
    ;;
esac
