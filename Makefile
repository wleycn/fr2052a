# FR 2052a —— 本地质量闸的统一入口。
#   make lint       跑全部闸：ruff 检查 + 格式检查 + 类型检查
#   make lint-fix   自动修可修项并格式化
#   make deps-dev   按下面的版本装 ruff 与 mypy（CI 用）
#   make help       列出全部目标
#
# 工具版本只在本文件声明一处，CI 也只调用 make，不另写一份版本。
# 取用顺序：PATH 里已装 ruff / mypy 就用本地的，否则用 uvx 现取（需要 uv）。
# 要临时换版本或换工具，命令行覆盖即可：make lint RUFF_VERSION=0.15.0
RUFF_VERSION ?= 0.14.4
MYPY_VERSION ?= 1.18.2
PYTHON ?= python3
RUFF ?= $(shell command -v ruff 2>/dev/null || echo "uvx ruff@$(RUFF_VERSION)")
MYPY ?= $(shell command -v mypy 2>/dev/null || echo "uvx mypy@$(MYPY_VERSION)")

.PHONY: help lint lint-fix deps-dev

help:  ## 列出全部目标
	@grep -E '^[a-z-]+:.*?## ' $(MAKEFILE_LIST) | sed 's/:.*## /\t/'

deps-dev:  ## 把工具装进当前解释器（供 CI 使用）
	$(PYTHON) -m pip install --disable-pip-version-check "ruff==$(RUFF_VERSION)" "mypy==$(MYPY_VERSION)"

lint:  ## 跑全部质量闸（ruff 检查 + 格式检查 + 类型检查）
	$(RUFF) check python deploy
	$(RUFF) format --check python deploy
	$(MYPY)

lint-fix:  ## 自动修可修项并格式化
	$(RUFF) check python deploy --fix
	$(RUFF) format python deploy
