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
.PHONY: help install lint fmt type test pack-lint verify-collect verify-gates collect annotate annotate-check ask

help:
	@echo 用法: make 目标   [ARGS=附加参数]
	@echo   install         建 .venv 并按 uv.lock 装依赖（含 editable 安装 logcopilot）
	@echo   lint            ruff 静态检查 + 格式检查（只查不改）
	@echo   fmt             ruff 格式化 + 自动修可修的 lint 问题（会改文件）
	@echo   type            pyright 类型检查（首次运行要下一份 node，较慢）
	@echo   test            pytest（当前 431 条契约测试）
	@echo   pack-lint       校验 CasePack：make pack-lint CASE=inc_dnvol_01 CHECK=levels
	@echo   verify-collect  132/132 一致性核验（第 11 步才有脚本）
	@echo   verify-gates    收口闸门 V3/V4/V7：make verify-gates ARGS="--v4"
	@echo   collect         采一个病例：make collect CASE=base_healthy_01 ARGS="--minutes 5"
	@echo   annotate        补标注：make annotate CASE=inc_dnvol_01 ARGS="--from-measurements"
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
# verify-collect：V2 的 132/132 一致性闸门
#
# 它比的是 `tests/fixtures/verify/baseline.json` 里冻结的指纹，不是"能不能跑"。
# 改了输出而基准没更新 → 这里必须红。确认变化是有意的，再
# `make verify-collect ARGS="--update-baseline"` 并把基准文件一起提交。
# ---------------------------------------------------------------------------
verify-collect:
	$(PY) scripts/verify_collect.py $(if $(CASE),--case $(CASE),) $(ARGS)

# ---------------------------------------------------------------------------
# verify-gates：V3 / V4 / V7 三条跨对象闸门
#
# 与上面两条的区别：V1/V2 只看 pack 自己（自洽、没漂移），这三条问的是
# "两个 pack 是否等价"（V3）、"两条取数路径是否逐字节相同"（V4）、
# "注入之后是否复原"（V7）。它们需要集群在跑，所以单独一个目标。
# 默认不带参数时只跑 V7 的离线部分（不需要集群）。
# ---------------------------------------------------------------------------
verify-gates:
	$(PY) scripts/verify_gates.py $(ARGS)

# ---------------------------------------------------------------------------
# 还只有名字的目标：脚本没写就打印一句人话，而不是扔 "can't open file" 让人
# 以为环境坏了。
# ---------------------------------------------------------------------------
ask:
ifeq ($(wildcard scripts/ask.py),)
	@echo [ask] 尚未实现: scripts/ask.py 在第 9 步落地
	@exit 1
else
	$(PY) scripts/ask.py $(ARGS)
endif

# ---------------------------------------------------------------------------
# 现场采集（第 15、16 步落地）：collect 采一例，annotate 补人的判断
# ---------------------------------------------------------------------------
# 两个目标都要 CASE=：采集必须有 case_id（它同时是目录名），标注必须知道改哪个 pack。
collect:
	$(PY) scripts/collect_case.py --case $(CASE) $(ARGS)

annotate:
	$(PY) scripts/annotate_case.py --case $(CASE) $(ARGS)

# 把一例从"采完"推到"可验收"：先按实测填门槛，再按 V6 复查一遍。
# 两条命令一起做，是因为分开跑很容易只填门槛、忘了复查（或反过来）。
annotate-check:
	$(PY) scripts/annotate_case.py --case $(CASE) $(ARGS)
	$(PY) scripts/pack_lint.py --case $(CASE) --check levels