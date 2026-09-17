# FR 2052a —— 本地质量闸的统一入口。
#   make lint       跑全部闸：ruff 检查 + 格式检查 + 类型检查
#   make lint-fix   自动修可修项并格式化
#   make help       列出全部目标
#
# 工具取用顺序：PATH 里已装 ruff / mypy 就用本地的，否则用 uvx 现取（需要 uv）。
# 要固定版本或换工具，命令行覆盖即可：make lint RUFF="uvx ruff@0.14.4"
RUFF ?= $(shell command -v ruff 2>/dev/null || echo "uvx ruff@0.14.4")
MYPY ?= $(shell command -v mypy 2>/dev/null || echo "uvx mypy@1.18.2")

.PHONY: help lint lint-fix

help:  ## 列出全部目标
	@grep -E '^[a-z-]+:.*?## ' $(MAKEFILE_LIST) | sed 's/:.*## /\t/'

lint:  ## 跑全部质量闸（ruff 检查 + 格式检查 + 类型检查）
	$(RUFF) check python deploy
	$(RUFF) format --check python deploy
	$(MYPY)

lint-fix:  ## 自动修可修项并格式化
	$(RUFF) check python deploy --fix
	$(RUFF) format python deploy
