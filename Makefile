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
	@echo   test            pytest（当前 178 条契约测试）
	@echo   pack-lint       校验 CasePack：make pack-lint CASE=inc_dnvol_01 CHECK=levels
	@echo   verify-collect  132/132 一致性核验（第 11 步才有脚本）
	@echo   ask             命令行问答入口（第 9 步才有脚本）

# ---------------------------------------------------------------------------
# 基础三件套
# ---------------------------------------------------------------------------
install:
	$(UV) sync --directory $(BACKEND)

# lint 只报告，fmt 负责改。分开是因为 CI 里跑 lint（必须只读），人手里跑 fmt。
# `../../scripts` 是仓库根的 scripts/（ruff 的 cwd 是 apps/backend）——
# pack_lint.py 这种"只被 make 调用"的脚本也必须进检查视野。
lint:
	$(UV) run --directory $(BACKEND) ruff check . ../../scripts
	$(UV) run --directory $(BACKEND) ruff format --check . ../../scripts

fmt:
	$(UV) run --directory $(BACKEND) ruff format . ../../scripts
	$(UV) run --directory $(BACKEND) ruff check --fix . ../../scripts

# pyright 的配置在 apps/backend/pyproject.toml 的 [tool.pyright] 里；
# 不用 `uv sync` 装它（那是 node 包装器，进依赖会拖慢每次安装），用一次性 uvx。
type:
	$(UV) tool run --from pyright pyright --project $(BACKEND)/pyproject.toml

test:
	$(UV) run --directory $(BACKEND) pytest $(ARGS)

# ---------------------------------------------------------------------------
# pack-lint：校验采集产物
#
# 变量名 `CASE= / CHECK=` 直接照抄 P-A §7 的验收命令，但 make 变量不会自己
# 变成 argv，所以这里显式转成 --case / --check。这样文档里那句
# `make pack-lint CASE=base_healthy_01 CHECK=levels` 才是真能敲的。
# ---------------------------------------------------------------------------
pack-lint:
	$(PY) scripts/pack_lint.py $(if $(CASE),--case $(CASE),) $(if $(CHECK),--check $(CHECK),) $(ARGS)

# ---------------------------------------------------------------------------
# 还只有名字的目标：脚本没写就打印一句人话，而不是扔 "can't open file" 让人
# 以为环境坏了。
# ---------------------------------------------------------------------------
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
