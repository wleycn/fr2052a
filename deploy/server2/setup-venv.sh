#!/usr/bin/env bash
# 在 Server 2 上创建 dbt 运行环境：uv 虚拟环境 + dbt 三件套 + PySpark。
#
# 为什么用 Python 3.11：
#   dbt-spark 与 PySpark 3.5.9 的官方支持区间到 3.11，再高没有把握；
#   dbt-core / dbt-postgres / great-expectations 也都覆盖 3.11。
#
# 前置：uv 二进制与一份 uv 管理的 Python 3.11。
#   uv 从 dev 机拷贝：   scp ~/.hermes/bin/uv .
#   Python 3.11 同理：   scp -r ~/.local/share/uv/python/cpython-3.11-linux-x86_64-gnu \
#                          ~/.local/share/uv/python/
# 若本地没有该解释器，脚本会回退到联网下载。
#
# 用法（在 Server 2 的 ~/fr2052a-infra 下执行）：
#   setsid nohup bash setup-venv.sh > /tmp/venv-setup.log 2>&1 < /dev/null &

set -euo pipefail

BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
UV="$BASE_DIR/uv"
PYTHON_VERSION="3.11"

# 走国内 PyPI 镜像：实测公网 PyPI 约 1.3 MB/s，阿里云镜像更快且稳定
export UV_INDEX_URL="https://mirrors.aliyun.com/pypi/simple/"
export UV_DEFAULT_INDEX="https://mirrors.aliyun.com/pypi/simple/"

if [ ! -x "$UV" ]; then
  echo "缺少 uv：$UV" >&2
  echo "从 dev 机拷贝：scp ~/.hermes/bin/uv ." >&2
  exit 1
fi

if ! "$UV" python list --only-installed 2>/dev/null | grep -q "cpython-${PYTHON_VERSION}"; then
  echo "本地无 Python ${PYTHON_VERSION}，联网安装"
  "$UV" python install "$PYTHON_VERSION"
fi

echo "创建虚拟环境"
"$UV" venv --python "$PYTHON_VERSION" "$BASE_DIR/venv"

echo "安装依赖"
VIRTUAL_ENV="$BASE_DIR/venv" "$UV" pip install \
  dbt-core \
  dbt-postgres \
  dbt-spark \
  "pyspark==3.5.9"

echo "安装完成，版本如下"
"$BASE_DIR/venv/bin/dbt" --version
