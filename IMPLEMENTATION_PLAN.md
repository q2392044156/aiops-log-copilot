# aiops-log-copilot 总体计划（全景）

> **版本** v1.0（全景版）｜ **日期** 2026-11-22 ｜ **状态** 方向已定，进入 P-A 详细设计
> **目标岗位** AI 应用开发 / LLM 应用工程
> **项目根** `E:\project\my_project1\aiops-log-copilot`
> **参照项目**
> - 领域语义与历史数据：`E:\project\my_project1\aiops-fault-diagnosis-agent`（只读复用数据，**代码不搬**）
> - 工程骨架与流程纪律：`E:\project\agent_py-release-2026-09-08`（apps/packages 布局、durable job、SSE 契约、OpenSpec）

---

## 0. 这份文档的角色

**它是地图，不是施工图。** 只回答四个问题：

1. 这是什么项目、为什么做（§1–§2）
2. 一共有哪几块工作（§3）
3. 每一块做什么、吃什么、吐什么、用什么技术（§4）
4. 怎么算做完（§7–§8）

**它不写**：字段定义、SSE 事件清单、状态机字段、逐日计划、文件级职责、实验矩阵、Makefile 目标。这些属于各阶段的设计文档（§12 文档地图），每份独立演进，**总计划只在阶段边界变化时才改**。

阅读顺序：先读 §1–§4 建立全局印象，动手前读对应阶段的设计文档。

---

## 1. 项目概述

### 1.1 一句话

> **aiops-log-copilot**：SRE 的日志诊断 Copilot。贴一段日志或说一句话，它**自己规划要查什么**，用工具查日志、查历史案例、查运维手册，输出**带行级引用**的根因假设与**只读**排查建议。计划可人审修改，关键结论必须人工确认后才落库，人工改判会**回流**成评测集与知识。

### 1.2 交付物形态

一个**本地优先**的单机 Web 应用 + 一套可复跑的评测 + 一份可校验的数据资产：

| 交付物 | 形态 | 判定标准 |
|---|---|---|
| 应用 | FastAPI + Vue3，`make dev` 起服务 | 浏览器里能走完"贴日志 → 出报告 → 人审改判" |
| 数据资产 | `data/cases/` 的 CasePack（新采 + 132 例迁移） | 指纹可重放、`132/132` 一致 |
| 评测资产 | `eval/` + 7 个数字 + 6 个对照实验 | `make eval` 一屏出全部数字 |
| 知识资产 | Milvus 2 万 chunk（5 类语料） | `recall@5 ≥ 0.80` |

### 1.3 三个不变量（所有设计让步于这三条）

1. **引用可回溯**：报告里每条结论都能定位到具体日志行/知识 chunk，否则视为幻觉。
2. **结论须人审**：系统只出假设与建议，`execution_allowed=false`；两次 interrupt 是产品语义，不是技术炫技。
3. **只读建议**：不执行任何变更操作。

### 1.4 规模一览

| 维度 | 数字 |
|---|---|
| 流水线阶段 | 5（P-A 采集 / P-B 索引 / P-C 诊断 / P-D 回流 / P-E 评测） |
| 诊断图节点 | 11 |
| 工具 | 6 个内置 + 若干 MCP 外部工具 |
| 知识库 | 2 万 chunk，5 类语料 |
| 案例 | 132 例旧数据迁移 + 3 例新采（S1）→ 12 例（S6 前） |
| 评测 | 7 个数字 + 2 条红线 + 6 个对照实验 |
| 工期 | 27.5 天（交付线）/ ≈42 天（学习线） |

---

## 2. 背景

### 2.1 起点：旧项目能跑，但学不进去

`aiops-fault-diagnosis-agent` 由 AI 一次性生成。它的**护城河是真的**（132 例真实案例、104 张根因卡、四层评测口径、severity-first 门禁），但结构与信心都有问题：

| 现象 | 证据 |
|---|---|
| 目录混装 | `src/aiops_diag/platform/` 一个包 17 个文件、约 4000 行：API + worker + SQLite + Streamlit + MCP + release 全塞一起 |
| 定义重复 | 全仓库 5 个 `release.py`、13 个 `__init__.py`；`CaseManifest`、`SourceAdapter` 两类双写 |
| 文档漂移 | 面试文档写的 `src/offline/capture`、`src/offline/detection/*` 在实际目录里不存在 |
| 关键能力没敢开 | 工具自由路由实测 **55%** 正确率（门槛 80%），故默认关闭；LangGraph 版本 `production_ready=false`，只作研究候选 |

**结论**：它的价值在"数据与规则"，不在"可学习的代码"。

### 2.2 参照项目给了什么

`agent_py-release-2026-09-08` 的价值不在功能，在**做法**：

- **骨架先行**：`docs/foundation.md` 只有 11 行——先建仓库结构与验证入口，产品行为由后续一个个 change 实现。
- **80 余个归档 change**：每块功能都有 proposal / design / tasks，可追溯"为什么这么写"。
- **硬约束写在 AGENTS.md**：包路径、类型注解、Protocol 边界、migration、durable job、SSE 契约同步。
- **契约三层**：`packages/api-contracts` 统一契约，前端 typed client，合同测试。

**结论**：它给的是"每天只长一块、每块都留记录"的节奏，正是旧项目缺的那一半。

### 2.3 岗位目标：每项能力都要落到代码

投的是 AI 应用岗，每一条 JD 高频词都要能在演示里被看见、在评测里被量化：

| 能力 | 落在哪个阶段 | 演示成什么 |
|---|---|---|
| LangChain / LCEL | P-C（链） | 追问时先改写 query 再检索，recall 提升 |
| 结构化输出 | P-C（plan / report） | 报告字段齐全，schema 违规率 0 |
| **Plan-and-Execute** | P-C（plan / 执行器） | 出计划 → 人审改一条 → 按计划执行 |
| **LangGraph** | P-C（图 / 检查点） | 节点时间线、暂停等人审、改判重跑 |
| 子图 + Send | P-C | 一次分诊并行派 3 路子任务 |
| 工具调用 | P-C | 一句话触发多个工具并行 |
| **MCP**（server + client） | P-C | Inspector 直连；内外工具同台 + 审计 |
| **RAG 全链路** | P-B | 引用抽屉展示各阶段 rank/score |
| **Milvus** | P-B | schema / HNSW / partition / 标量过滤 / hybrid |
| 数据采集（重写） | P-A | 现场采一例真实故障，指纹可重放 |
| 数据飞轮 | P-D | 改判 → 进评测集 → 再跑判对 |
| 成本路由 | P-D | 预算条、缓存命中标识 |
| 记忆 | P-C / P-D | 第二轮追问引用第一轮结论 |
| 流式 SSE | P-C | 工具卡逐个出现，P95 首事件 ≤1.5s |
| 评测 | P-E | 一屏 7 个数字 + 6 个实验 |

### 2.4 四条核心判断

| # | 判断 | 理由 | 后果 |
|---|---|---|---|
| **J1** | 主题不变，路线换掉 | 岗位考"LLM 系统怎么设计"，不考检测算法；但真实数据是最大稀缺项 | 保留 HDFS 日志诊断主题，砍掉检测算法重写 |
| **J2** | 旧项目当**数据**源，不当**代码**源 | 检测/窗口/门禁已做完且有 132 例产物 | 消费其 `incident.json` 与 `logs.parquet`；**采集器除外**（见 J4） |
| **J3** | 规划权给模型，执行权给代码 | 旧项目实测自由选工具仅 55%，SRE 场景错一次成本高 | Plan-and-Execute，不是 ReAct 自由发挥 |
| **J4** | **采集链路必须重写** | 旧采集器与检测耦合、无 manifest/指纹、不可续采，132 例无法确定性重放 | 新增 P-A，产出可校验的 CasePack；用 132 例做回归验证 |

> J4 的红利：新采集器可以用旧 132 例做**回归验证**（"新代码复现旧数据，行数与时间戳序列 100% 一致"）。有现成 ground truth 证明重构没跑偏，比"我写了个采集脚本"强得多。

---

## 3. 总体工作：五条流水线

```
              ┌─────────── 离线 ───────────┐        ┌──── 在线 ────┐
  真实环境 ──▶ │ P-A 采集 → CasePack         │        │              │
  旧 132 例 ─▶ │   （acquire→canonical→pack）│        │  P-C 诊断     │
              └────────────┬───────────────┘        │  11 节点图    │
                           │ CasePack               │  规划→执行→   │
              ┌────────────▼───────────────┐        │  推理→校验→   │
  语料 5 类 ─▶ │ P-B 索引 → Milvus + BM25    │──检索──▶│  人审→落库    │
  BGE ONNX  ─▶ │   （切分→embed→hybrid）     │        └──────┬───────┘
              └────────────────────────────┘               │ 报告/改判
                                                           ▼
              ┌──────────── 横切 ────────────────────────────────────┐
              │ P-D 回流：改判 → 评测集 + 知识提案 → 下次更准           │
              │ P-E 评测：7 个数字 + 6 个实验 + 回归护栏 → 证明有效      │
              └──────────────────────────────────────────────────────┘
```

| 阶段 | 一句话职责 | 关键输入 | 关键产出 | 工期 |
|---|---|---|---|---|
| **P-A** 采集 | 把真实环境的日志变成可校验、可续采、可重放的数据包 | docker HDFS 集群 / 旧 132 例 / 大日志分片 | CasePack（manifest + logs.jsonl + metrics + labels） | 3d |
| **P-B** 索引 | 把 5 类语料变成可混合检索的知识库 | 语料种子 + BGE ONNX 模型 | Milvus collection + 混合检索接口 + 引用锚点 | 4d |
| **P-C** 诊断 | 11 节点图：一句话/一段日志 → 带引用的报告 | CasePack + 知识库 + 6 个工具 + MCP | 报告 JSON + SSE 事件流 + runs 账本 | 11.5d |
| **P-D** 回流 | 人审改判变成评测集与知识提案 | feedback 表 + 采纳记录 | golden 增量 + 索引版本 +1 + 周报 | 6d |
| **P-E** 评测 | 把"我觉得好"变成可复现的数字 | golden 集 + 回放轨迹 | 7 个数字 + 6 份实验报告 | 横切 |

---

## 4. 阶段分述

每个阶段固定回答五件事：**做什么 / 输入 / 产出 / 技术栈 / 完成标志**。细节见各自设计文档。

### 4.1 P-A 采集

**做什么**：三段流水线——`acquire`（从容器/文件按时间窗增量拉日志与指标，支持断点续采）→ `canonicalize`（解析时间戳/级别/组件、归并续行、脱敏并计数）→ `package`（流式写 `logs.jsonl`，最后写 manifest，算两个哈希）。旁路两条：`migrate`（旧 132 例 → CasePack，用于回归验证）与 `inject`（4 类故障注入 + 恢复）。

**输入**：docker 起 1NN+3DN 的 HDFS 集群（`apache/hadoop:3.3.6`，本机已有）；旧项目 132 例产物；`HDFS.log` 大文件分片。

**产出**：`data/cases/<case_id>/{manifest.json, logs.jsonl, metrics.jsonl, labels.json, anchors/}`。其中 `manifest` 带 `input_fingerprint`（源不变则不变）与 `canonical_sha256`（代码/脱敏策略变了就变）+ `checkpoint`（续采状态）。

**技术栈**：`docker` CLI（subprocess，不引 SDK）· asyncio 并发拉取 · pydantic v2 · iptables / tmpfs 做故障注入 · uv。

**完成标志**：`make verify-collect` → **132/132 一致**；现场采出 3 例新案例（卷满 / 心跳 / 网络）；`make pack-lint` 全绿。**详细设计见 [P-A_COLLECT_PLAN.md](P-A_COLLECT_PLAN.md)。**

### 4.2 P-B 索引与检索

**做什么**：五段——语料构建（5 类）→ 4 种切分策略 → BGE 批量 embedding → Milvus 入库 → 混合检索（BM25 + dense + RRF + rerank），产出可被工具调用的检索接口与可回溯的引用锚点。

**输入**：语料种子（旧项目 154 chunk + 8 篇自写 runbook + 104 张根因卡 + 132 例案例报告 + 模板统计 + HDFS 文档）；`artifacts/releases/kb_v0.1/model/model.onnx`（BGE-small-zh，90MB）。

**产出**：Milvus collection `knowledge_chunks`（含 `source/kind/case_id/provenance/index_version` 等标量字段，支持按来源分区与过滤）；`rag/` 的检索接口；每条引用带 `(source, chunk_id, quote)`。

**技术栈**：Milvus standalone（`milvusdb/milvus:v2.6.4`）+ `pymilvus 3.0.3` · HNSW(M=16, efC=200, COSINE) · 原生稀疏 BM25 · RRF(k=60) · rerank（ONNX bge-reranker 或 Qwen rerank API，二选一）· `onnxruntime` · `jieba` · `rank-bm25`。

**完成标志**：2 万 chunk 入库；`recall@5 ≥ 0.80`；`make ask Q="..."` 能带出处回答；E1/E2/E3 三份对照实验成文。

### 4.3 P-C 诊断

**做什么**：11 节点图。`route`（4 类意图，规则优先）→ `parse_query` → `plan`（**唯一规划用 LLM**：出假设 + 步骤 + 反证步骤）→ `review_plan`（人审，可改/删/加）→ `execute_plan`（**确定性 DAG 执行**，Send 并行派 rag / tools / live 三路）→ `synthesize`（去重、打分、token 裁剪）→ `rank_root_cause`（**唯一写报告用 LLM**）→ `validate`（引用白名单 / 根因码闭集 / 动作安全 / 幻觉比对，失败回退 ≤2 次，证据不足回 plan）→ `human_review`（人审结论）→ `finalize`（落库 + 回流事件 + 成本记账）。

**输入**：CasePack（或一段贴进来的日志 → 临时 case）；知识库；6 个工具（`search_logs` / `get_event_evidence` / `search_cases` / `search_runbooks` / `get_template_stats` / `render_report`）；MCP 外部工具。

**产出**：`DiagnosisReport`（严格 JSON + hash + 引用锚点）；SSE 事件流（run_start / route / plan / plan_review / node / tool_call / tool_result / budget / token / citation / report / interrupt / done）；`runs` 表（成本与审计账本）。

**技术栈**：`langgraph 1.2.24`（`StateGraph` / reducer / 条件边 / `interrupt`+`Command` / 子图 + Send）· `langgraph-checkpoint-sqlite 3.1.3`（`AsyncSqliteSaver`，`thread_id = run_id`）· `langchain-openai 1.8.5`（LCEL + `with_structured_output(method="function_calling")` + `with_retry` + `with_fallbacks`）· DeepSeek（`deepseek-chat` 默认，`deepseek-reasoner` 对照）· `duckdb 2.0.1` 直查 11M 行 parquet · FastAPI 原生 `StreamingResponse` 推 SSE · `mcp 2.3.0`（server + client）· Vue3 + Vite + TS strict + Pinia。

**完成标志**：7 个数字达标；断点续跑（改一条证据 → resume → 报告变化可见）；一次诊断内外工具都用上；Vue 五页全流程走通。

### 4.4 P-D 回流

**做什么**：三条回路。①**评测集回路**：人审改判 → `feedback` 表 → 周任务生成候选 → 人工确认并入 golden（只增不减）→ 每次改动跑回归，翻车即红。②**知识回路**：人审补充 → 生成知识提案（`provenance` 必填、可 `supersedes` 旧条）→ 人工采纳后写 Milvus 且 `index_version + 1`（同时让语义缓存失效）。③**失败聚类周报**：按根因码 × 意图交叉统计，输出到 `docs/log/flywheel-YYYYWW.md`。

**输入**：`feedback` 表、`knowledge_proposals` 表、历史 run 轨迹。

**产出**：`eval/agent/golden.jsonl` 增量、Milvus 新版本索引、周报、成本账本聚合。

**技术栈**：SQLite + 手写 migration · `store/` 的 job 表 + 后台 worker（不裸 `asyncio.create_task`）· 模型分级路由 + 语义缓存（`index_version` 参与 key）· 预算预估与降级。

**完成标志**：一条改判能从"前端改判"走到"再跑判对"；**回归翻车数 = 0**；单次成本 -40% 且 task success 损失 ≤5pt。

### 4.5 P-E 评测

**做什么**：三个层次——RAG（recall@5 / nDCG@10）、Agent（tool-call F1 / plan coverage / task success）、E2E（多轮 + 信息不足场景）；外加 6 个对照实验与"新版 vs 旧版"回归护栏。

**输入**：golden 集（60 例 × 3 问 + 40 负例 + 60 条 agent 任务 + 20 条 E2E，其中 5 条故意信息不足）；录制的模型响应（回放模式，CI 不花一分钱）。

**产出**：`eval/reports/`（6 份，每份一表一结论）；`make eval` 一屏出全部数字。

**技术栈**：pytest + 自写 runner · 回放客户端（录一次真实响应，CI 全回放）· SQLite 存结果 · Markdown 报告。

**完成标志**：7 个数字可复现；回放模式下 CI 无网络也能跑；差异能逐条解释。

---

## 5. 阶段间契约：只有四条边

阶段之间**只允许通过类型与格式通信，不允许 import 彼此的实现**。

| 边 | 载体 | 定义处 | 谁写 | 谁读 |
|---|---|---|---|---|
| P-A → P-B / P-C / P-E | **CasePack 格式**（磁盘） | `domain/case.py` 类型 + `store/casepack.py` 读写 | P-A | P-B（建语料）、P-C（工具查日志）、P-E（评测） |
| P-B → P-C | **检索结果类型** + `Retriever` 接口 | `ports.py` + `domain/` | P-B | P-C |
| P-C → P-D | **`runs` / `feedback` 表** + 报告 JSON | `store/` | P-C | P-D |
| P-C → 前端 | **SSE 事件契约** | `packages/contracts` | P-C | Vue（typed client） |

两条由此推出的硬规则（写进测试，不靠自觉）：

- **L1 单向**：`api → services → agents/chains → domain`；`infra` 只实现 `ports`；`domain` 不 import 任何阶段。
- **L2 互斥**：`collect/` 不得 import `rag/`、`agents/`、`tools/`；`rag/` 不得 import `agents/`；`eval/` 可依赖一切，产品代码永不 import `eval`。

**目录形状**（一个阶段一个目录 + 契约集中三处）：

```
apps/backend/src/logcopilot/
├─ domain/      # 跨阶段类型：log / case / plan / report（零 IO、零框架）
├─ ports.py     # 跨阶段接口：LlmClient / Retriever / CaseStore / Clock
├─ store/       # 磁盘与 SQLite 契约：CasePack 读写、runs、feedback
├─ collect/     # P-A
├─ rag/         # P-B
├─ agents/ tools/ mcp/ memory/   # P-C（+ P-D 的会话记忆）
├─ llm/         # 横切：调用/重试/结构化/回放/路由/缓存/预算
├─ services/    # 用例编排（api 只调这里，将来 CLI/Cron 也调这里）
└─ api/ serve.py
apps/web/src/   # Vue3 五页
packages/contracts/   # 契约 → TS 类型
eval/           # P-E（在产品包之外，物理上就不可能被反向 import）
data/ models/ infra/ scripts/ docs/ openspec/
```

> 自检口径：**"删掉 `collect/` 后，P-C 依然能编译"**——说明耦合在数据上，不在代码上。

---

## 6. 技术栈总览

版本于 2026-11-21 实测，**全部 pin 死，`uv.lock` 进 git**。

| 层 | 选型 | 版本 | 备注 |
|---|---|---|---|
| 语言 / 包管理 | Python 3.12 + uv | uv 0.12.11 | PATH 上 python 是空壳，全程 `uv run` |
| Web | FastAPI + uvicorn | 最新稳定 | SSE 用原生 `StreamingResponse`，不引 `sse-starlette` |
| 校验 / 配置 | pydantic v2 + pydantic-settings | 最新稳定 | 契约唯一源；`.env` 只读一次 |
| LLM 客户端 | `langchain-openai`（`ChatOpenAI`） | **1.8.5** | **不装 `langchain` 元包** |
| 编排 | `langgraph` | **1.2.24** | 图 / interrupt / `astream_events(v2)` |
| 检查点 | `langgraph-checkpoint-sqlite` | **3.1.3** | 拉 `sqlite-vec`，装完即在 Windows 验 import；不行退 `MemorySaver` |
| 切分 | `langchain-text-splitters` | **1.1.3** | 仅作 E1 对照 baseline |
| 向量库 | Milvus standalone + `pymilvus` | 镜像 v2.6.4 / **3.0.3** | 原生 BM25 稀疏 + dense hybrid |
| Embedding | BGE-small-zh（ONNX 90MB） | 复用旧项目 | 零下载、离线可跑 |
| Rerank | bge-reranker ONNX 或 Qwen rerank API | 二选一 | 做 E2 对照；默认只对 top50 生效 |
| 关键词检索 | Milvus 原生 BM25 或自写 `jieba + rank-bm25` | — | 中文可能是坑，见 §13 R9 |
| 日志查询 | `duckdb` | **2.0.1** | 直查 635MB parquet（11M 行） |
| MCP | `mcp`（含 FastMCP） | **2.3.0** | 我暴露 server，同时当 client 接外部 server |
| 采集 | `docker` CLI（subprocess） | docker 29.8.2 | 不引 docker SDK，CLI 更透明 |
| 业务库 | SQLite + 手写 migration | — | 会话 / 报告 / 审计 / runs / 飞轮 |
| 前端 | Vue 3 + Vite + TS strict + Pinia | node v24.20.0 | 自写 typed SSE client |
| 质量 | ruff + pyright(strict) + pytest + pytest-asyncio | 最新稳定 | CI 门禁 |

**LangChain 的边界**（必须能讲清）：只允许出现在 5 个接缝——LLM 调用、链式组装、检索适配、图编排、回调观测。`domain/`、`tools/`、`collect/` 完全不认识 LangChain。

---

## 7. 里程碑总览

两条时间线，别混用：**交付线 27.5 天**（熟练工连续投入）／**学习线 ≈42 天**（每个 `.py` 读懂 + 单测 + 记一条日志）。投递最小闭环 = S0+S1+S2+S3+M6.7 ≈ **16.5 天（交付）/ 约 25 天（学习）**。

| 段 | 天 | 一句话交付 | 主阶段 |
|---|---|---|---|
| **S0** 基座 | 2.5 | clone 即跑：uv + Makefile + CI + Milvus + 模型 + 契约生成 + 样例数据 | 基础 |
| **S1** 采集 | 3 | CasePack 契约 + canonical/脱敏 + 两个 adapter + 132/132 迁移校验 + 现场采 3 例 | P-A |
| **S2** 索引 | 4 | 5 类语料 → 2 万 chunk → Milvus → 混合检索 → RAG golden | P-B |
| **S3** 诊断 | 6 | 6 工具 + ReAct baseline + 规划 + DAG 执行 + 11 节点图 + SSE/interrupt | P-C |
| **S4** MCP | 2.5 | FastMCP server（5 工具 + 2 resource + 1 prompt）+ client + 同台路由 + E4 | P-C |
| **S5** 入口 | 3.5 | 记忆两档 + 意图路由 + 日志自认领 case + Vue 五页 | P-C / P-D |
| **S6** 收口 | 6 | 飞轮三回路 + 成本路由 + eval 60 条 + E1–E6 成文 + ADR + 数字表 | P-D / P-E |

工期账：`2.5 + 3 + 4 + 6 + 2.5 + 3.5 + 6 = 27.5 天`。若需压到 24 天：砍 MCP 同台路由、砍 Eval/MCP 两页前端、砍 E3 实验、采集只做两个 adapter 且注入只做 2 类、成本路由只留接口（合计 -3.25d）。

> 每个 S 内的任务表（M0.1–M6.9）与文件级清单**不在本文件**，见各阶段设计文档；本文件只保证"段与段之间的边界不变"。

---

## 8. 成功标准

### 8.1 七个数字

| # | 指标 | 定义 | 目标 |
|---|---|---|---|
| 1 | `recall@5` | gold chunk 落在 top5 的占比 | ≥ 0.80 |
| 2 | `citation precision` | 能定位到真实行/chunk 且原文支持该断言的引用占比 | ≥ 0.95 |
| 3 | `tool-call F1` | 以 `(工具名, 关键参数)` 为集合的 micro-F1 | ≥ 0.85 |
| 4 | `plan coverage` | gold evidence 被 plan 步骤覆盖的比例 | ≥ 0.80 |
| 5 | `task success` | 根因码命中 且 报告 schema 合法 | ≥ 0.85 |
| 6 | `P95 TTFT` | 首个 SSE 事件到浏览器 | ≤ 1.5s |
| 7 | `¥/次诊断` | 15k 输入 + 2k 输出 | ≤ ¥0.05 |

### 8.2 两条红线（不是数字）

- **回归翻车数 = 0**：新版在 golden 集上不得比旧版判错更多。
- **采集一致性 132/132**：新采集器复现旧数据的行数、时间戳序列、level 分布、component 分布完全一致。

### 8.3 六个对照实验（只说比什么）

| 编号 | 比什么 |
|---|---|
| E1 | 4 种切分策略 |
| E2 | BM25 / dense / RRF / RRF+rerank |
| E3 | Milvus 原生 hybrid vs 自写 RRF |
| E4 | 同一工具：function calling vs MCP 暴露 |
| E5 | 无记忆 / 全上下文 / 摘要+检索 |
| **E6** | **ReAct 自由 loop vs Plan-and-Execute** |

> E6 与 E4 最稀缺——前者对应 LangChain 官方两种 pattern，后者对应"MCP 到底省不省"。

---

## 9. 工程约束（AGENTS.md 摘要）

1. 包路径一律 `from logcopilot...`，禁止 `from src...`。
2. 全量类型注解；`ruff` + `pyright --strict` 零告警。
3. 业务层只依赖 `ports.py` 的 Protocol，构造注入；禁止函数体内 new 客户端。
4. `domain/` 禁止 `open()` / `httpx` / `sqlite3` / `datetime.now()`；时间从 `Clock` 注入。
5. SQLite schema 变更必须新增 migration 文件。
6. 长任务走 job 表 + worker，**禁止裸 `asyncio.create_task`**。
7. SSE 事件改动必须同步三处：`packages/contracts` + 前端 `sse.ts` + 合同测试。
8. P-A 产出必须带指纹与 checkpoint，且采集器不得调用诊断侧任何模块。
9. 新增知识必须带 `provenance` 且经人工采纳（防知识污染）。
10. 每条性能/质量结论必须附可复现命令；一次 change 一次 commit。

---

## 10. 明确不做

| 不做 | 理由 |
|---|---|
| 重写检测算法 / 窗口 / 门禁 / 事件合并 | 旧项目已做完；**采集重写 ≠ 检测重写** |
| Streamlit 复核台 | 用 `interrupt` + Vue 页面替代 |
| 多租户 auth / 租户隔离 | 单用户 dev token 足够 |
| release 灰度 / 候选机制 | 旧项目最大的复杂度来源之一，刻意不要 |
| K8s / Milvus 集群模式 | standalone 够用 |
| 模型微调 | 性价比低；宁可做 prompt 版本化 + A/B |
| 自动写知识库 | 必须人工采纳 |

---

## 11. 决策记录

| # | 决策点 | 结论 |
|---|---|---|
| D1 | LLM 与密钥 | ✅ **DeepSeek**，key 在 `.env` 的 `DEEPSEEK_API_KEY`；CI 用回放模式，不需要 key |
| D2 | 语料规模 | ✅ **2 万 chunk**（Milvus 与 E1/E3 需统计意义） |
| D3 | 前端范围 | ✅ **五页**（Chat / Knowledge / Diagnose / Eval / MCP） |
| D4 | 方案档位 | ✅ 全量（规划权 + 飞轮 + 成本路由 + 零表单）+ 采集重写 + S0 工程门禁 |
| D5 | 采集源 | ✅ **docker 自建 1NN+3DN 现场采真实数据**，四类故障各 ≥2 例 |

> 无待拍板项。实施中冒出的新决策一律进 `docs/adr/`，不回填本表。

---

## 12. 文档地图

| 文档 | 角色 | 状态 |
|---|---|---|
| `IMPLEMENTATION_PLAN.md`（本文件） | **全景**：为什么做、做哪几块、怎么算完 | v1.0 |
| `P-A_COLLECT_PLAN.md` | P-A 详细设计：采什么、什么环境、哪些等级、文件级清单 | v0.1 |
| `P-B_RAG_PLAN.md` | P-B 详细设计：语料、切分、索引、混合检索、引用 | 待写 |
| `P-C_AGENT_PLAN.md` | P-C 详细设计：图、节点、工具契约、SSE、恢复 | 待写 |
| `P-D_FLYWHEEL_PLAN.md` | P-D 详细设计：三回路、表结构、护栏 | 待写 |
| `P-E_EVAL_PLAN.md` | P-E 详细设计：golden 造集、指标口径、实验 | 待写 |
| `docs/adr/` | 每个"为什么不选另一个"的决策 | 随做随写 |
| `docs/log/` | 每日"问题 / 决策 / 证据 / 复盘"，面试讲稿的原料 | 随做随写 |
| `docs/architecture.md` | 由脚本生成，杜绝手写漂移 | S6 |

**维护规则**：阶段内部怎么改，只改对应设计文档；**只有阶段边界（输入/产出/契约）变化时才动本文件**。

---

## 13. 风险

| # | 风险 | 概率 | 应对 |
|---|---|---|---|
| R1 | Milvus 镜像与 `pymilvus 3.0.3` 不匹配 | 低（镜像已在本机） | 起容器后先连一次；退 `milvus-lite`（**不支持 BM25 Function，E3 作废**），再退 Qdrant |
| R2 | 2 万 chunk 用 CPU ONNX 太慢 | 高 | 先测 1k 吞吐；分批 + 缓存；必要时改 Qwen embedding API |
| R3 | 现场采集起不来 docker HDFS 集群 | 中→低（镜像 + 旧脚本可参照） | 退路：`file` adapter 采大日志分片 + `migrate` 132 例 |
| R4 | 新采集器与旧数据对不齐 | 中 | 差异逐条列白名单，白名单外的必须修 |
| R5 | plan 一旦偏，执行全跑偏 | 高 | 反证步骤 `falsifies` + replan 触发条件 + 保留 ReAct fallback |
| R6 | 中文 BM25 跑不通 | 中 | 当天用 10 条中文试一次；不通改"自写 BM25(jieba) + dense"，E3 作废但主链路不受影响 |
| R7 | 成本数字自证（无真实流量） | 高 | 用 60 条录制轨迹回放算；面试讲设计而非硬报数字 |

---

## 14. 变更记录

| 版本 | 日期 | 变更 |
|---|---|---|
| v0.1 | 2026-11-21 | 首版：由"aiops 全链路重建"改为"AI 应用岗导向的日志诊断 Copilot" |
| v0.2 | 2026-11-21 | 叠加规划权 / 飞轮 / 成本路由 / 零表单；新增采集重写；9 → 11 节点；评测改 7 数字 + E6 |
| v0.3 | 2026-11-21 | D1（DeepSeek）、D5（现场采集）拍板；补密钥铁律与现场条件核对 |
| v0.4 | 2026-11-21 | 目录评审整改：补 `services/` 层与 L5；`CaseManifest`/`SourceAdapter` 单一来源；补 CI / LICENSE / 类型生成 / clone 可跑四项门禁 |
| v0.5 | 2026-11-21 | 合理性审计整改：文档一致性 9 处；拆交付线/学习线两条口径；实验落点显性化；意图分类改规则兜底；心跳注入改 iptables |
| **v1.0** | 2026-11-22 | **角色变更：从"施工总图"改为"全景地图"。** 移出所有实现细节（字段定义、SSE 事件清单、状态机、逐日计划、文件级职责、Makefile、实验矩阵、故障注入表）到各阶段设计文档，本文只保留：概述、背景、五条流水线与各阶段的「做什么/输入/产出/技术栈/完成标志」、阶段间四条边、技术栈总览、里程碑总览、成功标准、目录形状、文档地图 |
