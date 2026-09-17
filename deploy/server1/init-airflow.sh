#!/usr/bin/env bash
# 建 Airflow 连接与变量（幂等，可重复执行）。
#
# 为什么要脚本化：连接与变量是编排的配置面，手敲一遍既容易漏，也无法复现。
# 值取自同目录 .env，不写死在本文件里。
#
# 用法（在 Server 1 的 ~/fr2052a-infra 下执行，Airflow 容器已启动）：
#   bash init-airflow.sh

set -euo pipefail

BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
ENV_FILE="$BASE_DIR/.env"
if [ ! -f "$ENV_FILE" ]; then
  echo "缺少 $ENV_FILE" >&2
  exit 1
fi

set -a
# shellcheck disable=SC1090
. "$ENV_FILE"
set +a

CONTAINER="${AIRFLOW_CONTAINER:-fr2052a_airflow_webserver}"
SERVER2_HOST="${SERVER2_HOST:-192.168.17.24}"

airflow_cmd() {
  docker exec "$CONTAINER" airflow "$@"
}

add_connection() {
  local conn_id="$1"
  shift
  # 幂等：先删后建，避免"已存在"直接失败
  airflow_cmd connections delete "$conn_id" >/dev/null 2>&1 || true
  airflow_cmd connections add "$conn_id" "$@"
  echo "  连接就绪：$conn_id"
}

echo "1/2 配置连接"
add_connection postgres_default \
  --conn-type postgres \
  --conn-host postgres \
  --conn-port 5432 \
  --conn-login "$POSTGRES_USER" \
  --conn-password "$POSTGRES_PASSWORD" \
  --conn-schema "$POSTGRES_DB" \
  --conn-description "FR 2052a 报送服务库（Server 1）"

# SSH 连接用参数化私钥：Airflow 任务在容器内跑，私钥由 prepare-airflow.sh
# 复制到 ./airflow/keys 并改成容器用户属主后挂载进来。
add_connection ssh_default \
  --conn-type ssh \
  --conn-host "$SERVER2_HOST" \
  --conn-port 22 \
  --conn-login "${SERVER2_SSH_USER:-hermes}" \
  --conn-extra "{\"key_file\": \"/opt/airflow/keys/id_rsa\", \"look_for_keys\": false}" \
  --conn-description "Server 2 计算节点（跑批脚本在其上执行）"

echo "2/2 配置变量"
airflow_cmd variables set fr2052a_report_currency "USD"
airflow_cmd variables set fr2052a_deadline "08:00"
airflow_cmd variables set fr2052a_gl_tolerance "0.01"
airflow_cmd variables set fr2052a_minio_endpoint "http://${SERVER1_HOST:-192.168.17.22}:9000"
echo "  变量已就绪"

echo
# 注意：`airflow connections list` 会把口令明文打印出来，不能直接用于日志。
# 这里改从元数据库只取连接标识与类型。
echo "连接清单（不打印口令）："
docker exec fr2052a_postgres psql -U "$POSTGRES_USER" -d airflow -tAc \
  "select conn_id || '  (' || conn_type || ')' from connection order by conn_id"
