#!/usr/bin/env bash
# [AI-GENERATED] model=deepseek-flash date=2026-09-17 reviewed_by=pending
# 为 Airflow 准备 Server 1 上的运行前提：元数据库、DAG/日志目录、SSH 私钥。
#
# 为什么需要单独的密钥副本：DAG 通过 SSH 触发 Server 2 上的跑批作业，
# Airflow 容器内的运行用户是 uid 50000，直接挂载宿主机的 ~/.ssh 会因为属主与权限
# 不匹配而无法使用（SSH 对私钥权限有硬要求）。因此复制一份并改成容器用户的属主。
#
# 用法（在 Server 1 的 ~/fr2052a-infra 下执行）：
#   bash prepare-airflow.sh

set -euo pipefail

BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
ENV_FILE="$BASE_DIR/.env"

if [ -f "$ENV_FILE" ]; then
  set -a
  # shellcheck disable=SC1090
  . "$ENV_FILE"
  set +a
fi

POSTGRES_USER="${POSTGRES_USER:-fr2052a}"
POSTGRES_DB="${POSTGRES_DB:-fr2052a_db}"
AIRFLOW_UID="${AIRFLOW_UID:-50000}"

echo "1/3 创建 Airflow 元数据库（已存在则跳过）"
if docker exec fr2052a_postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc \
  "select 1 from pg_database where datname='airflow'" | grep -q 1; then
  echo "    已存在：airflow"
else
  docker exec fr2052a_postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "CREATE DATABASE airflow OWNER $POSTGRES_USER"
fi

echo "2/3 创建 DAG / 日志 / 密钥目录"
mkdir -p "$BASE_DIR/airflow/logs" "$BASE_DIR/airflow/dags" "$BASE_DIR/airflow/keys"
# 容器内进程需要写日志目录
sudo chown -R "${AIRFLOW_UID}:0" "$BASE_DIR/airflow/logs"

echo "3/3 复制 SSH 私钥并调整属主"
if [ -f "$BASE_DIR/airflow/keys/id_rsa" ]; then
  echo "    已存在，跳过复制"
else
  cp "$HOME/.ssh/id_rsa" "$BASE_DIR/airflow/keys/id_rsa"
fi
sudo chown "${AIRFLOW_UID}:${AIRFLOW_UID}" "$BASE_DIR/airflow/keys/id_rsa"
chmod 600 "$BASE_DIR/airflow/keys/id_rsa"
# 容器内 SSH 首次连接需要 known_hosts，预先写入避免 Host key verification failed。
# keys 目录属主已改为容器用户，这里用 sudo 写入。
if [ ! -f "$BASE_DIR/airflow/keys/known_hosts" ]; then
  ssh-keyscan -T 5 "${SERVER2_HOST:-192.168.17.24}" 2>/dev/null | sudo tee "$BASE_DIR/airflow/keys/known_hosts" >/dev/null
  sudo chmod 644 "$BASE_DIR/airflow/keys/known_hosts"
fi

echo
echo "准备完成："
ls -ld "$BASE_DIR/airflow" "$BASE_DIR/airflow/logs" "$BASE_DIR/airflow/keys"
ls -l "$BASE_DIR/airflow/keys"
