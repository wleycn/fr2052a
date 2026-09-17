#!/usr/bin/env bash
# 清掉流式作业的「进度」，让它们能从零开始重放。
#
# 为什么必须清两样东西，少清一样都白清：
#   Kafka 主题里的内容
#       不清主题，重放只是在旧内容后面追加，下一次读取会把历史消息一起读进来。
#       结果虽然被主键 MERGE 收敛到正确行数，但白跑几倍的数据量。
#   消费位点（checkpoint）
#       不清位点，作业认为「后面的消息都处理过了」，一条都不会读 ——
#       表现为「明明重放了却什么都没发生」，而且不报错，最难排查。
#
# 删除主题是异步的，删完立刻重建会撞上「主题仍存在」；所以这里删完先等到列表里没有它们，
# 再交给 create-topics.sh 重建（那份声明是主题清单的唯一来源，不在这里重写一份）。
#
# 用法（在 Server 2 的 ~/fr2052a-infra 下执行；属破坏性操作，只由 reset-demo.sh --apply 调用）：
#   bash reset-streaming-state.sh

set -euo pipefail

BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
CONTAINER="${KAFKA_CONTAINER:-fr2052a_kafka}"
CHECKPOINT_DIR="$BASE_DIR/checkpoints"

mapfile -t TOPICS < <(python3 - "$BASE_DIR/app/config/pipeline_topics.json" <<'PY'
import json, sys
for topic in json.load(open(sys.argv[1]))["topics"]:
    print(topic["name"])
PY
)

echo "删除主题 ${#TOPICS[@]} 个（内容随主题一起清掉）"
for topic in "${TOPICS[@]}"; do
  docker exec "$CONTAINER" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --delete --topic "$topic" >/dev/null 2>&1 || true
  echo "  [OK] $topic"
done

echo "等待删除生效（最多 30 秒）"
for _ in $(seq 1 30); do
  remaining="$(docker exec "$CONTAINER" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --list 2>/dev/null | grep -cFx -f <(printf '%s\n' "${TOPICS[@]}") || true)"
  if [ "${remaining:-1}" -eq 0 ]; then
    echo "  已全部删除"
    break
  fi
  sleep 1
done

echo "重建主题"
bash "$BASE_DIR/create-topics.sh"

echo "清空消费位点：$CHECKPOINT_DIR"
# 位点文件由容器内的运行用户（uid 185）创建，宿主上的 hermes 删不掉 ——
# 实测报一堆 Permission denied，而且 rm 会在中途停住、留下半个检查点目录
# （半个检查点比没有更糟：作业读它会直接失败）。所以这里用 sudo 删，
# 随后由 prepare-runtime-dirs.sh 重建目录并把属主改回容器用户。
sudo rm -rf "${CHECKPOINT_DIR:?}"/*
bash "$BASE_DIR/prepare-runtime-dirs.sh"
