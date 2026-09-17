#!/usr/bin/env bash
# 按 config/pipeline_topics.json 建 Kafka 主题。
#
# 为什么用脚本而不是手敲：主题名与落点的唯一声明在 JSON 里，
# 手敲一遍就等于多了一份可能漂移的副本。
#
# 用法（在 Server 2 的 ~/fr2052a-infra 下执行）：
#   bash create-topics.sh
#
# 幂等：全部 --if-not-exists，可重复执行。

set -euo pipefail

BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
CONFIG_FILE="${1:-$BASE_DIR/app/config/pipeline_topics.json}"
CONTAINER="${KAFKA_CONTAINER:-fr2052a_kafka}"

if [ ! -f "$CONFIG_FILE" ]; then
  echo "缺少主题声明文件：$CONFIG_FILE" >&2
  exit 1
fi

# 用 python3 读 JSON，避免依赖 jq 是否安装
read -r PARTITIONS REPLICATION <<<"$(python3 - "$CONFIG_FILE" <<'PY'
import json, sys
kafka = json.load(open(sys.argv[1]))["kafka"]
print(kafka["partitions"], kafka["replication_factor"])
PY
)"

mapfile -t TOPICS < <(python3 - "$CONFIG_FILE" <<'PY'
import json, sys
for topic in json.load(open(sys.argv[1]))["topics"]:
    print(topic["name"])
PY
)

echo "声明主题 ${#TOPICS[@]} 个，分区 ${PARTITIONS}，副本 ${REPLICATION}"
for topic in "${TOPICS[@]}"; do
  docker exec "$CONTAINER" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create --if-not-exists \
    --topic "$topic" \
    --partitions "$PARTITIONS" \
    --replication-factor "$REPLICATION" >/dev/null
  echo "  [OK] $topic"
done

echo
echo "当前主题清单："
docker exec "$CONTAINER" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
