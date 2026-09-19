#!/usr/bin/env bash
# [AI-GENERATED] model=deepseek-flash date=2026-09-17 reviewed_by=pending
# 创建 Server 2 上容器需要的宿主侧运行时目录，并把属主改成容器内的运行用户。
#
# 为什么需要它：Docker 看到绑定挂载的源目录不存在时会自动创建，但属主是 root，
# 而 apache/spark 镜像里的进程以 uid 185(spark) 运行，写不进去。
# 症状是流式作业报 "mkdir of file:/opt/fr2052a-checkpoints/... failed"，与检查点本身毫无关系。
#
# 用法（在 Server 2 的 ~/fr2052a-infra 下执行）：
#   bash prepare-runtime-dirs.sh

set -euo pipefail

BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
CHECKPOINT_DIR="$BASE_DIR/checkpoints"

# apache/spark 镜像中的 spark 用户
CONTAINER_UID=185
CONTAINER_GID=185

mkdir -p "$CHECKPOINT_DIR"
sudo chown -R "${CONTAINER_UID}:${CONTAINER_GID}" "$CHECKPOINT_DIR"

echo "已准备运行时目录："
ls -ld "$CHECKPOINT_DIR"
