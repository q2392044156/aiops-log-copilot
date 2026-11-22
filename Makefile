# ============================================================================
# aiops-log-copilot —— 统一任务入口
#
# 为什么用 Makefile：计划里那些验收红线（`make pack-lint`、`make verify-collect`
# 132/132、`make collect-demo`）本身就是一个个目标名 —— 把它们做成真能敲的
# 命令，文档和实际动作才是同一件事。
#
# 可移植性约定：recipe 里只写 uv 命令和纯文本 echo，不用 test / rm / $(shell)
# 这些 Unix 才有的东西。Windows 的 cmd.exe 与 Linux 的 sh 都能跑，不需要额外装 sh。
# ============================================================================

UV      := uv
BACKEND := apps/backend
PY      := $(UV) run --project $(BACKEND) python

# `make test ARGS="-k roundtrip -x"` —— 追加参数不用改 Makefile
ARGS    ?=

.DEFAULT_GOAL := help
.PHONY: help install lint fmt type test pack-lint verify-collect ask

help:
	@echo 用法: make 目标   [ARGS=附加参数]
	@echo   install         建 .venv 并按 uv.lock 装依赖（含 editable 安装 logcopilot）
	@echo   lint            ruff 静态检查 + 格式检查（只查不改）
	@echo   fmt             ruff 格式化 + 自动修可修的 lint 问题（会改文件）
	@echo   type            pyright 类型检查（首次运行要下一份 node，较慢）
	@echo   test            pytest（当前 17 条契约测试）
	@echo   pack-lint       校验一个 CasePack 目录（第 4 步才有脚本）
	@echo   verify-collect  132/132 一致性核验（第 11 步才有脚本）
	@echo   ask             命令行问答入口（第 9 步才有脚本）

# ---------------------------------------------------------------------------
# 基础三件套
# ---------------------------------------------------------------------------
install:
	$(UV) sync --directory $(BACKEND)

lint:
	$(UV) run --directory $(BACKEND) ruff check .
	$(UV) run --directory $(BACKEND) ruff format --check .

# lint 只报告，fmt 负责改。分开是因为 CI 里跑 lint（必须只读），人手里跑 fmt。
fmt:
	$(UV) run --directory $(BACKEND) ruff format .
	$(UV) run --directory $(BACKEND) ruff check --fix .

# pyright 的配置在 apps/backend/pyproject.toml 的 [tool.pyright] 里；
# 不用 `uv sync` 装它（那是 node 包装器，进依赖会拖慢每次安装），用一次性 uvx。
type:
	$(UV) tool run --from pyright pyright --project $(BACKEND)/pyproject.toml

test:
	$(UV) run --directory $(BACKEND) pytest $(ARGS)

# ---------------------------------------------------------------------------
# 三个"还只有名字"的目标
#
# 计划里它们必须存在（文档/红线引用它们），但实现分别在后面几步。
# 这里用 $(wildcard) 判脚本在不在：没写就打印一句人话并失败，
# 而不是扔一句 "can't open file" 让人以为环境坏了。
# ---------------------------------------------------------------------------
pack-lint:
ifeq ($(wildcard scripts/pack_lint.py),)
	@echo [pack-lint] 尚未实现: scripts/pack_lint.py 在第 4 步落地
	@exit 1
else
	$(PY) scripts/pack_lint.py $(ARGS)
endif

verify-collect:
ifeq ($(wildcard scripts/verify_collect.py),)
	@echo [verify-collect] 尚未实现: scripts/verify_collect.py 在第 11 步落地
	@exit 1
else
	$(PY) scripts/verify_collect.py $(ARGS)
endif

ask:
ifeq ($(wildcard scripts/ask.py),)
	@echo [ask] 尚未实现: scripts/ask.py 在第 9 步落地
	@exit 1
else
	$(PY) scripts/ask.py $(ARGS)
endif
