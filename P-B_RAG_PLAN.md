# P-B 索引与检索 · 实现计划

> **版本**：v0.3（2026-11-23）—— O1（~2000 chunk）与 O5（`make ask` 归 P-B）已拍板；代码尚未开工，本文是施工图
> **上游文档**：`IMPLEMENTATION_PLAN.md` v1.0 的 §4.2（P-B 段落）/ §5（P-B → P-C 两条边）/ §7（S2 索引 4d）/ §8.1（`recall@5 ≥ 0.80`、`citation precision ≥ 0.95`）/ §8.3（E1/E2/E3）/ §9（十条工程约束）
> **本文边界**：只讲 P-B —— 语料从哪来、怎么切、怎么算向量、怎么进 Milvus、怎么混合检索、怎么验收。
> **不管**：诊断图与 LLM 编排（P-C）、知识提案审批与回流（P-D）、评测框架本身（P-E）、日志采集（P-A）。
> **代码落点**：下文相对路径都相对 `apps/backend/src/logcopilot/`；`scripts/`、`data/`、`models/`、`infra/`、`eval/` 相对仓库根。

---

## 0. 三句话

1. P-A 把日志变成了**可回放的证据**；P-B 把文字变成**可按相关性取回、且能指出出处的片段**。
2. 对外只交两样东西：一个 Milvus collection `knowledge_chunks`，和一个 `ports.Retriever` 接口。**P-C 从不直连 Milvus。**
3. 结项判定是三个可跑的数字 / 命令：`make kb-check` 全绿、`recall@5 ≥ 0.80`、`make ask Q="..."` 带出处。

---

## 1. 做什么 / 不做什么

| 做 | 不做什么（写清是为了防止范围蔓延） |
|---|---|
| 把 4 层语料规范化成统一 `CorpusDoc` | 不训练、不微调任何模型（只用现成 ONNX / API） |
| 4 种切分策略 + 引用 span | 不做知识图谱、不做实体抽取管线 |
| BGE ONNX 批量向量化（含缓存与断点） | 不做实时增量（`index_version` 手动切） |
| Milvus 建库（dense + sparse + 标量过滤 + 分区） | 不做多租户、不做集群模式（standalone 够） |
| BM25 / dense / RRF / rerank 四模式检索 | 不生成答案（生成是 P-C 的事） |
| `Citation` 引用与可回搜校验 | 不做 PDF OCR、不整库复制第三方手册 |
| 检索评测查询集 + E1/E2/E3 | 不改写 `data/cases/` 里的任何 pack（P-A 的产物只读） |

硬约束（来自 §9 与 P-A 的分层）：`rag/` **不得** import `agents/`、`tools/`、`memory/`；跨阶段只经 `ports.py` 的 Protocol；`domain/` 零 IO。

---

## 2. 输入：现在真实有什么（2026-11-23 实测）

### 2.1 CasePack（P-A 的产出，只读）

- **156 个 pack**：132 `migrated` + 24 `collected`，共 **78754 条**记录、35.8 MB。
- 非 INFO 记录 **4347 条**（ERROR 73 / WARN 4274），全部来自 5 个族：`NN_SAFE_MODE` 205、`DN_HEARTBEAT` 2388、`DN_VOLUME` 1350、`NETWORK_CONNECTIVITY` 892、`HEALTHY` 125（噪声）。
- 全部记录里只出现过 **14 种异常类**：
  ```
  IOException 251 · ConnectException 173 · SafeModeException 100 · DiskErrorException 63
  NotEnoughReplicasException 46 · NoRouteToHostException 20 · EOFException 16
  InterruptedIOException 9 · ConnectTimeoutException 9 · FileNotFoundException 8
  SocketTimeoutException 7 · UnknownHostException 5 · NotDirectoryException 2 · DiskOutOfSpaceException 2
  ```
- 非 INFO 的 logger 极集中：`BlockPlacementPolicy` 1863、`DFSUtil` 1368、`BlockStoragePolicy` 898、`DataNode` 156、`FsShell` 113、`DataStreamer` 63。
- `logs.jsonl` 的 `template_id` **全是 `null`**（P-A 留给 P-B 回填，见 O3）。

### 2.2 知识与语料素材（旧项目 + 本仓库）

| 素材 | 位置 | 实测规模 | 状态 |
|---|---|---|---|
| 旧知识库 chunk | `…\artifacts\releases\kb_v0.1\index\chunks.jsonl` | **154 条 / 104 篇文档**，字段全（`chunk_id/doc_id/section_path/root_cause_codes/evidence_unit_ids/risk_level/source_url/source_version`） | 可直接用 |
| 结构化 chunk | `…\artifacts\retrieval\work\structured\chunks.jsonl` | **416 条 / 同样 104 篇文档**，同字段 | 疑似超集，需确认（V1 的 gate） |
| 生成卡 | `…\runtime\knowledge_expansion\generated_cards.json` | **48 张**（`doc_id/title/category/component/product/version/content_origin`） | 与"104"不是一回事：104 是旧 KB 的 distinct `doc_id`（篇），48 是这里生成的卡；两者都要用 |
| 案例工单 | `…\ops\lab\*\evidence\tickets\*.md` | 132/132 例都有 | 能不能当语料见 O6 |
| 模板（Drain3 状态） | `…\artifacts\templates\{docker_container,wsl}\state.bin` | 10 KB / 8 KB，**二进制** | 需导出成可入库的模板清单 |
| runbook | — | **0 篇** | 要自己写 8–12 篇 |
| HDFS 文档 | — | 0 | 需按需抓 Apache 官方 HTML |

### 2.3 模型与运行时

| 项 | 实测 |
|---|---|
| Embedding | `artifacts\releases\kb_v0.1\model\model.onnx` = **90.4 MB**（BGE-small-zh，512 维） |
| Reranker | `artifacts\rag\reranker_onnx\model.onnx` = **1060.9 MB**（bge-reranker） |
| Python | 当前 venv 是 **3.14.7**；`onnxruntime 1.32.0` 与 `pymilvus 3.0.3` **在 3.14 上有轮子**（已 dry-run 解析通过），不必降 Python |
| Milvus | 本地 **一个镜像都没有**（`milvus` / `etcd` / `minio` 全缺）→ 第一步就得拉，见 R1 |

### 2.4 三个由数据推出来的结论（决定了后面的设计）

**结论 1：单行日志没有判别力。**
`Failed to place enough replicas, still in need of 1 to reach 3` 在 `DN_VOLUME` 出现 272 次、`DN_HEARTBEAT` **592 次**、`NN_SAFE_MODE` 19 次；`Unexpected value for data transfer bytes=45 duration=0` 是纯噪声（每族 390–423 条）。
→ 知识库**不能**做成"日志句 → 根因"的字典；判别力在**组合特征**（logger + 时间序 + 组件分布 + 指标），那是 P-C 的事。P-B 提供的是**解释**与**可比案例**。

**结论 2：真正稀缺的是"组合特征"的显式表述。**
14 个异常类 × 4 族 × 6 条链路只有几十个真实签名组合，而"`DiskErrorException` + `unavailableStorages=[DISK]` + `df 100%` 同时出现才叫卷坏"这句话**只在你自己的 156 例里有**，任何官网文档都不写。

**结论 3：语料量级不该由"2 万"倒推。**
文档类素材约 340 篇（154/416 chunk + 48 卡 + 132 工单 + 8 runbook），切分后 **1200–2000 chunk** 就能覆盖全部真实组合。要到 2 万只能把日志窗口也切成语料（≈3900 块起），而那样的语料服务的是"找相似案例"，不是"解释机制"。→ **O1，必须先拍板**。

---

## 3. 语料设计

### 3.1 三层，而不是沿用"五类"

总计划写"5 类语料"，但按 §2.4 的结论，按**用途**分三层更清楚（`kind` 是这一层的落码）：

| 层 | 用途 | `kind` | 谁消费 |
|---|---|---|---|
| **A 故障模式卡** | "这个现象组合意味着什么" | `case_card` | P-C 的 `search_cases` / 报告依据 |
| **B 机制与参数** | "为什么会这样"（含官方出处） | `mechanism` | P-C 的 `search_runbooks` / 解释段 |
| **C 处置步骤** | "现在该做什么、不能做什么" | `runbook` | P-C 的动作建议（只读） |
| 补充 1 | 旧知识库冷启动 | `legacy_kb` | 与 B 同权限 |
| 补充 2 | 模板簇（现象聚类） | `template` | 相似案例检索 |

`kind` 是**闭集**，进 Milvus 标量字段与 partition key；新增 kind 必须改本文与 `rag/corpus.py` 的常量。

### 3.2 每条语料必须带的东西（`CorpusDoc` 契约）

```python
@dataclass(frozen=True, slots=True)
class CorpusDoc:
    doc_id: str            # 稳定标识：case_card:DN_VOLUME:dn1 / mechanism:hdfs-default:dfs.datanode.failed.volumes.tolerated
    kind: str              # 闭集，见 3.1
    title: str
    text: str              # 原始正文（未切分）
    source: str            # 来源标识，见 3.4 的枚举
    source_url: str | None # 官方文档 / 旧 KB 的出处；自构造为 None
    version: str           # 版本或"取自哪一批数据"（如 casepack/v1+156pack@2026-11-23）
    root_cause_codes: tuple[str, ...]   # 闭集 11 码，可为空
    components: tuple[str, ...]         # nn/dn1..dn3，可为空
    evidence_ids: tuple[str, ...]       # 指回 pack 的 (case_id, line_id)，模式卡必填
    provenance: str        # 谁写的、依据什么写的（一条自然语言），**必填非空**
```

`provenance` 非空是硬校验：§9 第 9 条"新增知识必须带 provenance 且经人工采纳"。自构造语料的 `provenance` 写成"由 156 个 pack 中 `DN_VOLUME` 33 例的 `DiskErrorException` 统计生成（2026-11-23）"这种可复核的话。

### 3.3 来源决策表

| 来源 | 词面对齐 | 可引用性 | 成本 | 结论 |
|---|---|---|---|---|
| 156 例派生的模式卡 | **最高**（日志原句） | 可引到 `case_id + line_id` | 低（脚本生成骨架 + 人工补机制） | **主力，先做** |
| Apache 官方 HDFS 文档（HTML） | 中 | URL + 章节锚点 | 低（抓 20–30 页） | **必做，做"出处"** |
| `hdfs-default.xml` 参数说明 | 中高（参数名日志里真出现） | 版本 + 键名 | 极低 | 必做 |
| 自写 runbook | 高 | 自建文档 | 中（8–12 篇） | 必做 |
| 旧 154/416 chunk + 48 卡 | 中 | 有 `doc_id/section_path` | 零 | 冷启动基线 |
| **HDFS 排障 PDF** | 低 | **差**（无稳定锚点，页码随版本漂） | 中 | **不做主力（O7）** |
| 博客 / 公众号 / CSDN | 高但不可信 | 不可引 | 低 | **明确不要** |

理由：Apache Hadoop 官方只发布 **HTML 站点文档，没有 PDF 排障手册**；流通的"HDFS 运维手册 PDF"多为第三方整理稿。而第一不变量是"引用可回溯"——PDF 只能给"第 12 页第 3 段"，换版本即失效；HTML 给 `url#section`、`hdfs-default.xml` 给键名、模式卡给 `case_id:line_id`，这三种都能被程序校验。

### 3.4 A 层（模式卡）的生成规则

每张卡一个 `(root_cause, component)` 组合，目前实测只有 5 个族 × 4 个组件，**去重后 40–60 张**足够：

1. 从 156 例里按 `(labels.root_cause, component)` 分组；
2. 取该组**非 INFO 记录**的 top 5 消息形态（抹掉块号/路径/IP，见 §2.1 的统计口径）；
3. 取该组**指标极值**（`df.use_pct` max、`hdfs_report.dead_datanodes` max、`under_replicated_blocks` max）；
4. 取**反例**：该组出现但别组更常见的形态（用来写"别把这句当根因"）；
5. 人工补 3 段：机制（为什么）、判别（和谁易混）、处置（runbook 链接）。

脚本只生成 1–4 的骨架（`data/corpus/cards/*.md` 带 front-matter），第 5 段必须人手写——这是"经人工采纳"的落点。

### 3.5 配比与量级

**O1 已拍板（2026-11-23）：走 ~2000 chunk 的方案。** 构成与目标：

| 语料 | `kind` | 目标块数 |
|---|---|---:|
| 故障模式卡 | `case_card` | 400 |
| 案例三段 | `case_card`（`case_phase` 策略） | 468 |
| runbook | `runbook` | 30 |
| Apache 官方 + `hdfs-default.xml` | `mechanism` | 300 |
| 旧知识库（154 或 416 选一，见 V1） | `legacy_kb` | 600 |
| 模板簇 | `template` | 100 |
| **合计** | | **1800–2200** |

**被否决的备选**（记下来是为了以后不重新讨论）：把日志窗口切片（每 20 行一块，≈1.5 万块）凑到 2 万。否决理由：那些块的引用只能指到"某天某段日志"，`quote` 与机制无关，`citation precision` 会被稀释；而且 1.5 万块 × 512 维在单机 Milvus 上并不便宜（见 R2）。**代价**：E1/E3 的统计意义变弱，报告里要写明"chunk 总量 ~2000，切分策略差异的置信区间较宽"。

**与总计划的一处冲突**：总计划 §4.2 的完成标志写"2 万 chunk 入库"，§7 S2 也写"2 万 chunk"。按总计划自己 §12 的维护规则（"只有阶段边界变化时才动本文件"），这里**不改总计划**，由本文覆盖该数字；若要回填，只需把 §4.2 的完成标志与 §7 S2 的"2 万"改成"2 千"。

---

## 4. 切分

### 4.1 四种策略（对应 §8.3 的 E1）

| 策略 | 怎么做 | 主要服务 | 备注 |
|---|---|---|---|
| `fixed` | 固定 512 字符、不重叠 | baseline 对照组（E1） | 只为对照，不用作成品 |
| `section` | 按 `section_path` / Markdown 标题切 | 文档类（`mechanism` / `legacy_kb`） | 一块 = 一节，天然带锚点 |
| `template_cluster` | 按日志模板簇切：一簇一块，块内是"该模板的典型行 + 出现次数 + 涉及组件" | `template` | 需要先做模板挖掘（见 4.6） |
| `case_phase` | 按案例三段（baseline / fault / recovery）+ 证据行切 | `case_card` | 每块带 `case_id` 与 `phase` |

### 4.2 策略矩阵

| `kind` | 策略 | 目标块长 |
|---|---|---|
| `case_card` | `case_phase` | 200–400 字 |
| `mechanism` | `section` | 200–500 字 |
| `runbook` | `section`（按步骤切，一步一块） | 150–300 字 |
| `legacy_kb` | 原样（旧 chunk 本身就是切好的） | 沿用 |
| `template` | `template_cluster` | 100–200 字 |

### 4.3 `Chunk` 契约

```python
@dataclass(frozen=True, slots=True)
class Chunk:
    chunk_id: str          # {kind}:{doc_id}:{seq:04d}
    doc_id: str
    kind: str
    text: str              # 入库正文（= quote 的容器）
    quote: str             # 最能代表这一块的原文片段，**必须能在 text 里原样找到**
    span_start: int        # 相对 CorpusDoc.text 的字符偏移
    span_end: int
    case_id: str | None
    root_cause_codes: tuple[str, ...]
    components: tuple[str, ...]
    evidence_ids: tuple[str, ...]
```

### 4.4 `quote` 必须可回搜（机器校验，V2）

切分完立刻跑：`doc.text[span_start:span_end] == chunk.text` **且** `chunk.quote in chunk.text`。
这条不做，后面 `citation precision ≥ 0.95` 一定塌 —— 因为 P-C 会把 `quote` 直接贴进报告，而它是幻觉的最后一公里。

### 4.5 长度与重叠

- 中文按**字符**算：200–500 字对应 BGE 的 512 token 上限，留足余量。
- 同一文档内的相邻块**不重叠**（引用要精确到块，重叠会让"这段属于哪块"变成二义）。
- 超长节（> 800 字）在句号处强制断开，断点写进 `span_end`。

### 4.6 模板挖掘（`template` kind 的前置）

输入是 156 个 pack 的 `logs.jsonl`（78754 条）。用 Drain3（旧项目的 `state.bin` 可直接载入）产出 `(template_id, 模板文本, 出现次数, 组件分布, 代表行)`，落到 `data/corpus/templates.jsonl`。
**不回写 pack 的 `template_id`**（见 O3）。

---

## 5. 向量化

### 5.1 模型与文件

| 项 | 值 |
|---|---|
| 模型 | BGE-small-zh（ONNX，90.4 MB），拷到 `models/bge-small-zh/model.onnx` |
| 维度 | **512** |
| 运行时 | `onnxruntime` CPU，`intra_op_num_threads = min(8, cores)` |
| 归一化 | L2 normalize 后入 Milvus，度量用 **COSINE** |
| 分词 | 随模型带的 `tokenizer.json`（`tokenizers` 库），**不要**用 jieba 做 embedding 侧分词 |

### 5.2 批处理、缓存、断点

- 批大小 32；按 `(model_id, sha256(text))` 缓存向量到 `data/cache/emb/{sha256[:2]}/{sha256}.npy`。
- 断点：每 500 块写一次 `data/cache/emb/_progress.json`（记录已处理 chunk_id 列表的尾指针）。
- 重跑幂等：缓存命中直接跳过，**不重新算**。

### 5.3 吞吐 gate（V4）

先拿 1000 块跑一遍，记录 `条/秒` 与 RSS。判据：**1000 块 ≤ 3 分钟**（≈5.5 条/秒）。
达不到就走 R2 的退路：加大 batch、`intra_op` 调参、或改 Qwen embedding API（记住：换模型 = 换 collection，`index_version` 必须 +1）。

---

## 6. Milvus

### 6.1 部署

`infra/milvus/compose.yaml`：`milvusdb/milvus:v2.6.4` + `etcd` + `minio`，standalone 模式，端口 `19530`（gRPC）/ `9091`（metrics）。
数据卷落在 `infra/milvus/data/`（已 gitignore）。
**注意**：本地目前一个镜像都没有，`docker compose up` 会先拉 ~1.5 GB，见 R1。

### 6.2 collection schema：`knowledge_chunks`

| 字段 | 类型 | 说明 |
|---|---|---|
| `chunk_id` | VARCHAR(128) **主键** | `{kind}:{doc_id}:{seq:04d}` |
| `dense` | FLOAT_VECTOR(512) | BGE 向量，COSINE |
| `sparse` | SPARSE_FLOAT_VECTOR | 由 `text` 经 BM25 Function 生成（见 R6） |
| `text` | VARCHAR(8192)，`enable_analyzer=True` | 入库正文 |
| `quote` | VARCHAR(2048) | 引用片段 |
| `kind` | VARCHAR(32) | 闭集，见 3.1 |
| `source` | VARCHAR(64) | 来源枚举 |
| `doc_id` | VARCHAR(128) | 父文档 |
| `case_id` | VARCHAR(64) | 案例类语料才有；否则空串 |
| `root_cause_codes` | ARRAY(VARCHAR(32)) | 闭集 11 码，可空 |
| `components` | ARRAY(VARCHAR(8)) | nn/dn1..dn3 |
| `span_start` / `span_end` | INT64 | 回指原文 |
| `provenance` | VARCHAR(1024) | 必填非空 |
| `index_version` | INT64 | 每次重建 +1 |

### 6.3 索引

- `dense`：`HNSW(M=16, efConstruction=200)`，度量 `COSINE`，查询时 `ef=64`（可调）。
- `sparse`：`SPARSE_INVERTED_INDEX` + `BM25` 度量（Milvus 2.6 的 Function 机制）。
- 标量：`kind` / `case_id` / `index_version` 建 `INVERTED` 索引，供过滤与版本隔离。

### 6.4 分区与过滤

- 按 `kind` 建 5 个 partition；检索时默认跨分区，给了 `kinds` 就只查对应分区。
- 常见过滤：`root_cause_codes` 含某码、`components` 含某组件、`case_id == x`、`index_version == N`。
- **P-C 的六件工具里，只有 `search_cases` / `search_runbooks` 走这里**；`search_logs` 走 pack（P-A 的 `store/query.py`），不要混。

### 6.5 `index_version`、幂等、重建

- 写入一律 `upsert`（按主键），所以**重跑同一批语料不产生重复**（V6）。
- 重建流程：`index_version = N+1` 全量写入 → 校验条数与抽样 → `alias` 从 `knowledge_chunks_vN` 切到 `vN+1` → 保留上一代供回滚。
- 回滚：alias 切回去即可，不动数据。
- **换 embedding 模型必须换 collection**（维度不同），不允许在同 collection 里混两种向量。

### 6.6 Make 目标

| 目标 | 语义 |
|---|---|
| `make kb-build` | 语料 → 切分 → 向量 → 入库（幂等，可重跑） |
| `make kb-check` | 条数对账、`kind` 分布、抽查 20 条回搜、alias 指向当前版本 |
| `make kb-drop` | 删 collection（要二次确认） |
| `make ask Q="..."` | 走混合检索，打印 top-k 与引用（P-A 的目标已改名 `ask-case`，此名归 P-B） |

---

## 7. 混合检索与引用

### 7.1 四种模式（对应 §8.3 E2）

| 模式 | 怎么算 | 用途 |
|---|---|---|
| `bm25` | 只用 `sparse` 字段 | 关键词精确命中（异常类名、参数名） |
| `dense` | 只用 `dense` | 语义相近（"盘坏了" ↔ "volume failure"） |
| `rrf` | 两路各取 top50，RRF 融合 | **默认模式** |
| `rrf_rerank` | 融合后对 top50 重排，取 top-k | 质量优先 |

### 7.2 RRF

`score = Σ 1 / (k + rank_i)`，`k = 60`（Milvus 与 §4.2 的默认值一致）。
两路权重相等；不给权重参数——真要调就在 E2 里做，不进产品接口。

### 7.3 rerank

- 输入是 RRF 的 top50，输出重排后的 top-k。
- 两种实现：`bge-reranker` ONNX（1060.9 MB，CPU 上加载几十秒）或 Qwen rerank API。
- **O4 必须先定**：常驻进程（P-C 启动即加载，吃 ~1.2 GB 内存）还是按需加载（每次几十秒）还是走 API。这条直接决定 P-C 的服务形态。

### 7.4 `Retriever` / `Citation` 契约（唯一对外入口）

```python
# domain/retrieval.py（新增；零 IO）
@dataclass(frozen=True, slots=True)
class Citation:
    chunk_id: str
    source: str
    kind: str
    quote: str              # 报告里可原样展示的片段
    score: float
    doc_id: str
    case_id: str | None
    index_version: int

# ports.py 追加
class Retriever(Protocol):
    def retrieve(
        self,
        query: str,
        *,
        k: int = 5,
        kinds: Sequence[str] | None = None,
        filters: Mapping[str, object] | None = None,
    ) -> list[Citation]: ...
```

- 只读、同步、无副作用；失败（Milvus 连不上）抛 `RetrievalError`，**不返回空列表充数**——"检索挂了"与"没检索到"是两件事。
- 返回顺序即相关性顺序，上层不再排序。

### 7.5 空结果与阈值

- `k` 上限 50；`min_score` 可选参数，默认 0（不做截断，让上层决定）。
- 结果为空时返回 `[]`，并在日志里记 `query` 与命中的模式，便于 P-E 归因。

### 7.6 延迟目标

`P95`：`bm25`/`dense` ≤ 150 ms，`rrf` ≤ 300 ms，`rrf_rerank` ≤ 800 ms（不含模型加载）。这是 V10 的判据，实测后回填。

---

## 8. 评测

### 8.1 golden 查询集（P-B 自己造，P-E 复用）

- 规模：**60 例 × 3 问 = 180 条 query**，从 156 个 pack 的 `fault` 段出题。
- 三类问法，每条 query 标好 `gold_chunk_ids`（人工指定，不看检索结果反推）：
  1. **归属**："以下日志属于哪类故障？" （取 2–3 行非 INFO，去掉 case_id）
  2. **机制**："为什么 DataNode 报 `DiskErrorException` 且 `unavailableStorages=[DISK]`？"
  3. **处置**："卷坏但集群还在跑，应该先做什么？"
- 负例 20 条：与 HDFS 无关的问题（应返回低分或空），用来测"不硬答"。
- 落到 `eval/rag/queries.jsonl`，字段 `query_id / text / kind / gold_chunk_ids / case_id`。

### 8.2 指标

| 指标 | 定义 | 目标 |
|---|---|---|
| `recall@5` | gold 落在 top5 的占比 | **≥ 0.80** |
| `nDCG@10` | 分级相关性 | 记录，不设红线 |
| `citation precision` | 抽 100 条引用，`quote` 能在源文档/日志行里原样找到且支持该断言 | **≥ 0.95** |

### 8.3 E1 / E2 / E3

| 编号 | 比什么 | 记录 | 产出 |
|---|---|---|---|
| E1 | 4 种切分策略（fixed / section / template_cluster / case_phase） | recall@5、chunk 数、平均块长 | `eval/reports/E1-chunking.md` |
| E2 | bm25 / dense / rrf / rrf_rerank | recall@5、P95 延迟 | `eval/reports/E2-hybrid.md` |
| E3 | Milvus 原生 hybrid vs 自写 RRF | recall@5、实现复杂度 | `eval/reports/E3-hybrid-native.md` |

E3 若中文 BM25 跑不通（R6），写成"作废 + 原因"，不硬凑。

---

## 9. 文件级清单与工期

| 步骤 | 文件 | 行数 | 做什么 | gate |
|---|---|---:|---|---|
| **M2.1** 语料 | `rag/corpus.py` | 180 | `CorpusDoc` + 校验 + 读写 `data/corpus/*.jsonl` | V1 |
| | `rag/sources/legacy_kb.py` | 120 | 读旧 154/416 chunk、48 卡，映射成 `CorpusDoc` | 条数对得上 |
| | `rag/sources/case_cards.py` | 200 | 从 156 pack 生成模式卡骨架（§3.4 的 1–4 步） | 40–60 张 |
| | `data/corpus/cards/*.md` | — | 人手补第 5 段（机制/判别/处置） | provenance 非空 |
| | `data/corpus/runbooks/*.md` | 8–12 篇 | 自写处置步骤 | 每篇能回指日志句 |
| **M2.2** 切分 | `rag/chunking.py` | 260 | 4 种策略 + `Chunk` + 回搜校验 | V2 |
| | `rag/templates.py` | 120 | Drain3 模板挖掘 → `data/corpus/templates.jsonl` | 模板数、代表行 |
| **M2.3** 向量 | `rag/embedding.py` | 200 | ONNX 加载、批处理、缓存、断点 | V4 |
| **M2.4** 入库 | `rag/store_milvus.py` | 300 | schema、upsert、alias、版本 | V5 / V6 / V7 |
| **M2.5** 检索 | `rag/retriever.py` | 240 | 4 模式 + RRF + rerank 钩子 | V8 |
| | `domain/retrieval.py` | 60 | `Citation` | — |
| | `ports.py`（改） | +25 | `Retriever` 协议 | pyright 0 |
| **M2.6** CLI | `scripts/kb_build.py` | 150 | `make kb-build` 的入口 | 幂等 |
| | `scripts/kb_check.py` | 120 | `make kb-check` | 全绿 |
| | `scripts/ask_kb.py` | 100 | `make ask Q=` | 带引用 |
| **M2.7** 评测 | `eval/rag/queries.jsonl` | 180 条 | golden query | 人工标注 |
| | `eval/rag/run_retrieval.py` | 200 | 跑四种模式、出 recall/nDCG | 报告 |
| **M2.8** 基础设施 | `infra/milvus/compose.yaml` | 60 | milvus + etcd + minio | 起来能连 |
| | `models/bge-small-zh/*` | — | 90.4 MB 模型 + tokenizer | 能出 512 维 |

合计约 **2400 行**，**4 天**（与 §7 的 S2 一致）：

| 天 | 内容 |
|---|---|
| D1 | M2.1 语料 + M2.2 切分（含 V1/V2） |
| D2 | M2.3 向量 + M2.8 Milvus 起来（含 V4/V5） |
| D3 | M2.4 入库 + M2.5 检索（含 V6/V7/V8） |
| D4 | M2.6 CLI + M2.7 评测 + E1/E2/E3 初稿 |

---

## 10. 验收（每条都能跑）

| 编号 | 判据 | 命令 |
|---|---|---|
| **V1** | 每类语料有明确条数，且每条能追到来源（`source` + `provenance` 非空） | `make kb-check` 的语料段 |
| **V2** | `quote` 100% 能在 `text` 里原样找到，`text == doc[span_start:span_end]` | `make kb-check` 的回搜段 |
| **V3** | chunk 总数落在 **1500–2500**（O1 已定 ~2000） | `make kb-check` |
| **V4** | 1000 块 ≤ 3 分钟 | `scripts/kb_build.py --bench 1000` |
| **V5** | Milvus 条数 = 磁盘 chunk 数；按 `kind` 过滤结果数正确 | `make kb-check --milvus` |
| **V6** | 连续 build 两次，条数与主键集合不变 | 跑两遍 `make kb-build` |
| **V7** | `index_version` 从 N 切到 N+1 后 alias 生效，回滚能切回 | `make kb-build --bump` |
| **V8** | `recall@5 ≥ 0.80`（180 条 query） | `make eval-rag` |
| **V9** | `citation precision ≥ 0.95`（抽 100 条） | `make eval-rag --citation` |
| **V10** | `make ask Q="DataNode 报 DiskErrorException 但没 dead"` 输出 top-5 且每条带 `quote` 与 `chunk_id`；P95 ≤ 300 ms（rrf） | `make ask Q=...` |

---

## 11. 风险与坑

| 编号 | 风险 | 会以什么形式咬人 | 退路 |
|---|---|---|---|
| **R1** | Milvus 镜像不在本地 | 第一次 `compose up` 要拉 ~1.5 GB，网络差就卡住 | 先用 `milvus-lite` 跑通链路（**无原生 BM25，E3 作废**）；再退 Qdrant |
| **R2** | CPU 上跑 embedding 慢 | 2000 块可能十几分钟，2 万就几小时 | 加 batch、调 `intra_op_num_threads`、缓存；仍不行改 Qwen embedding API |
| **R3** | reranker 1.06 GB | 每次加载几十秒，P-C 每次诊断都加载不现实 | 常驻进程或换 API（O4） |
| **R4** | 噪声底污染召回 | `Unexpected value for data transfer` 这类噪声句会挤进 top-k | 切分时把噪声模板单列成 `template` 且默认降权；query 侧做"噪声句过滤" |
| **R5** | 判别力错觉 | 单行日志跨族出现（§2.4 结论 1），人看 recall@5 高会误判"检索能定根因" | 报告里写死：检索只提供证据，定根因在 P-C |
| **R6** | 中文 BM25 不通 | Milvus 原生 BM25 的 analyzer 对中文可能切不出词，`bm25` 模式全空 | 当天用 10 条中文试；不通就 `jieba + rank-bm25` 自写（E3 作废，主链路不受影响） |
| **R7** | `template_id` 回填位置 | 写进 pack 会破坏"pack 只读"契约，且动 `logs.jsonl` 没有代码保证安全 | 模板映射写 `data/index/templates.jsonl`（O3） |
| **R8** | 旧 chunk 的版权/隐私 | 416 chunk 里可能含旧项目内网路径 | 入库前跑 P-A 的 `redact` 一遍，命中进 `provenance` 备注 |
| **R9** | Windows 上 Milvus 依赖 | Docker Desktop 资源不足时 etcd 起不来 | 给 compose 配 `mem_limit`，或先验证 `pymilvus` 能连再谈语料 |

---

## 12. 待拍板

| 编号 | 问题 | 选项 | 我的建议 |
|---|---|---|---|
| ~~**O1**~~ | chunk 总量 | **已拍板：~2000**（见 §3.5 的构成表；2 万方案已否决并记下理由） | — |
| **O2** | "案例报告"语料形态 | ①用旧 132 张 `tickets/*.md` ②从 `manifest+labels+证据行` 合成结构化摘要 | **②**，可复算、无版权问题；①只作补充 |
| **O3** | `template_id` 回填到哪 | ①写回 `logs.jsonl` ②写 `data/index/templates.jsonl` | **②**，保持 pack 只读 |
| **O4** | rerank 形态 | ①常驻 ONNX ②按需加载 ③Qwen API | 先 ②（P-B 期），P-C 期再定 ①/③ |
| ~~**O5**~~ | `make ask` 目标名冲突 | **已拍板：P-A 的改名 `make ask-case`（脚本仍是 `scripts/ask.py`），`ask` 归 P-B**（已随本次提交落地） | — |
| **O6** | 132 张 ticket 能否作语料 | ①可 ②不可（内网信息/许可） | 先跑 `redact`，命中率高的不采 |
| **O7** | 要不要采 PDF | ①采 ②不采 | **②不采**，理由见 §3.3 |

---

## 13. 与下游的接口

| 下游 | 用什么 | 约束 |
|---|---|---|
| **P-C** | `ports.Retriever.retrieve()` → `Citation` | 只读；不得绕过 Retriever 直连 Milvus；六件工具里只有 `search_cases` / `search_runbooks` 走这里 |
| **P-D** | 知识提案采纳后写 Milvus，`index_version + 1` | 提案必须带 `provenance`；写库走同一个 `rag/store_milvus.py`，不另开通道 |
| **P-E** | `eval/rag/queries.jsonl` + `run_retrieval.py` | P-E 只调 CLI/脚本，不 import `rag/` 的内部函数 |

---

## 14. 变更记录

| 版本 | 日期 | 变更 |
|---|---|---|
| v0.3 | 2026-11-23 | **更正一处误读**：总计划里的"104 张根因卡"其实是旧 KB 的 **104 篇文档**（154/416 chunk），而 `generated_cards.json` 是 **48 张生成卡**，两者不是同一物；§2.2 表与 §3.5 已按实测改写，总计划同步回填 |
| v0.2 | 2026-11-23 | **O1 与 O5 拍板**：①chunk 总量定 **~2000**（六类语料的目标块数表），2 万方案否决并记下理由；②`make ask` 归 P-B，P-A 的目标改名 `make ask-case`（已落地）；③总计划 §4.2/§7 S2 的"2 万"由本文覆盖，不改总计划（遵其 §12 维护规则）；V3 判据改成 1500–2500 |
| v0.1 | 2026-11-23 | 首版：语料三层（`case_card` / `mechanism` / `runbook` + `legacy_kb` / `template`）、4 种切分、BGE ONNX、Milvus `knowledge_chunks`、RRF+rerank、V1–V10 验收；据实测数据提出 O1（2 千 vs 2 万）、O5（`make ask` 命名冲突）、O7（不采 PDF） |
