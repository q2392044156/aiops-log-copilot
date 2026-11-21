# P-A 采集流水线 · 实现计划

> **版本**：v0.2（2026-11-22）
> **上游文档**：`IMPLEMENTATION_PLAN.md` v1.0 的 §4.1（P-A 段落）/ §5（CasePack 契约与目录形状）/ §7（S1 里程碑）/ §8.2（红线）
> **本文边界**：只讲 **P-A（采集）** 这一段——采什么日志、用什么环境采、采哪些等级与窗口、A1/A2/A3 各做什么、怎么验收。
> **不管**：窗口切分 / 模板 / 门禁 / 事件合并（那是旧项目已有的检测侧，新项目不复现）、检索与诊断（P-B / P-C）。
> **与总计划的关系**：v0.1 的"本文会修订总计划一处（指纹拆分）"**已闭环**——总计划 v1.0 §4.1 已直接写成 `input_fingerprint` + `canonical_sha256` 两个哈希。本文只保留定义与不变式，不再有"待回填"项。
> **代码落点**：下文相对路径都相对 `apps/backend/src/logcopilot/`（总计划 v1.0 §5 目录形状）；`scripts/`、`data/`、`infra/` 相对仓库根。

---

## 1. 采集什么日志（结论先行）

### 1.1 决策：**只采 HDFS 生态，不做"通用日志采集器"**

| 采 | 日志 | 容器内路径 | 格式 | 为什么值得采 |
|---|---|---|---|---|
| ✅ | **NameNode 主日志** | `/opt/hadoop/logs/hadoop-hdfs-namenode-<host>.log` | log4j pattern | `NN_SAFE_MODE`、块副本不足、`SlowDisk`、`FSNamesystem` 异常的唯一来源 |
| ✅ | **DataNode 主日志** | `/opt/hadoop/logs/hadoop-hdfs-datanode-<host>.log` | log4j pattern | `DN_VOLUME`（卷失败）、`DN_HEARTBEAT`（注册/心跳）、`DataXceiver` 连接错误都在这 |
| ✅ | **HDFS 审计日志** | `/opt/hadoop/logs/hdfs-audit.log` | 空白分隔 KV | 谁删了块、谁触发了 safemode——用它补"人的动作"这条链 |
| ✅ | **NameNode GC 日志** | `/opt/hadoop/logs/hadoop-hdfs-namenode-*-gc.log`（可选） | JVM GC | 成本≈0，`NN_SAFE_MODE` 里"内存不足触发"的场景需要它 |
| ✅ | **容器 stdout/stderr** | `docker logs` | 混合 | 容器启动失败、OOMKilled、镜像内脚本报错**只在标准输出里**，日志文件里看不到 |
| ✅ | **指标快照**（不是日志） | `docker stats` + `hdfs dfsadmin -report` + `df -h` | JSON | `DN_VOLUME` 的"容量趋势"、`DN_HEARTBEAT` 的"DN 存活数"必须靠指标定论，日志只能证明"报错了" |
| ⬜ 扩展位 | syslog / journald / dmesg | — | syslog | **本期不做**：格式与 log4j 完全不同，canonical 规则要另写一套；`adapters/http.py` 留位 |
| ⬜ 扩展位 | YARN / Spark / Hive 日志 | — | log4j | **本期不做**：多一种组件 = 多一套领域知识，深度换广度不划算 |
| ❌ | K8s 事件 / 云厂商日志服务 | — | — | 本机没有集群，只为演示不值 |

**为什么不"什么日志都采"**：canonical 的核心前提是**一种格式一套规则**。HDFS 的 log4j pattern 是"时间 级别 类名: 消息"的固定形状，续行靠缩进/非时间戳首列识别。混入 syslog 和 JSON 日志后，A2 会从"一个解析器"变成"解析器分发器"，而后者正是旧项目 `ingestion/adapters` 膨胀成 6 个文件的原因。**广度留给 adapter 接口，深度留给 HDFS。**

### 1.2 每个组件的"信号密度"（为什么这三个文件就够）

| 故障类型 | 主证据在哪个日志 | 辅助证据 |
|---|---|---|
| `DN_VOLUME` | DataNode（`VolumeFailed` / `DiskChecker` / `No space left on device`） | `df -h`、`docker stats` |
| `DN_HEARTBEAT` | NameNode（`DatanodeRegistration` 超时 / stale / dead） + DataNode（心跳发送失败） | `dfsadmin -report` 的 Live/Dead 数 |
| `NN_SAFE_MODE` | NameNode（`SafeModeException` / `NameNode is in safe mode` / 进入退出日志） | `dfsadmin -safemode get`、GC 日志 |
| `NETWORK_CONNECTIVITY` | DataNode（`DataXceiver` / `SocketTimeout` / `Connection refused`）+ NameNode（`IOException` 连不上 DN） | `docker stats` 的 net I/O |

> 四类故障的**主证据**全部落在 NN/DN 两个文件里——这是"只采 HDFS"在证据层面的依据，不是拍脑袋。

---

## 2. 用什么环境采集（以及为什么不是别的）

### 2.1 决策：**docker compose，1 NameNode + 3 DataNode，`apache/hadoop:3.3.6`**

| 候选 | 结论 | 理由 |
|---|---|---|
| **docker compose（1NN+3DN）** | ✅ **采用** | 镜像本机已有（2.64GB，digest 可记）；四类故障都有可注入目标；`docker` 层能直接拿 `stats`/`logs`/`events`；旧项目同款拓扑已验证；重置成本 = `down -v && up` |
| 单容器（NN+DN 同进程） | ✗ | 无法做"某一个 DN 掉线而其他正常"（心跳/网络两类故障的核心场景） |
| WSL2 里装原生 Hadoop | ✗ | 环境不可复制（发行版/包管理器差异），且 WSL 的时钟与网络栈会让"心跳超时"测不准 |
| kind / K8s | ✗ | 为了采集引入集群编排，收益为负；本机 docker 资源也不够 |
| 云上 EMR | ✗ | 无账号、无预算、网络不可控 |

### 2.2 拓扑与资源

```
infra/hdfs/compose.yaml  (project name: logcopilot-hdfs)

  namenode      apache/hadoop:3.3.6   9870(HTTP) 9000(RPC)   heap 512m   vol: nn-data
  datanode1     同上                                          heap 512m   vol: tmpfs 128m  ← 专用卷满靶子
  datanode2     同上                                          heap 512m   vol: dn2-data
  datanode3     同上                                          heap 512m   vol: dn3-data
  network: hdfs (bridge)
```

| 项 | 值 | 说明 |
|---|---|---|
| 镜像 | `apache/hadoop:3.3.6` | **记录 digest**，写进每个 manifest 的 `source[].image_digest` |
| 用户 | `user: "0:0"` | 否则容器内 `dd` 写不进数据目录、`hdfs` 命令也没有权限 |
| 时区 | **`TZ=UTC`（推荐，见 §10 O3）** | 日志里是 UTC，宿主机是 +08:00——统一成 UTC，A2 就不用猜 |
| 日志落点 | 容器内 `/opt/hadoop/logs/`，同时 **bind mount** 到 `runtime/hdfs-logs/<container>/` | 让 `file` adapter 与 `docker` adapter 读同一份数据 → 两条路径产出必须逐字节相同（这本身是一个测试） |
| 数据盘 | dn1 用 `tmpfs size=128m`，dn2/dn3 用 volume | `dd 256MB` 必然写爆 dn1，注入稳定且不伤宿主磁盘 |
| 内存 | 4 容器 × 512m heap ≈ 2.5–3.5GB | 本机足够 |
| 端口 | 9870 / 9000（仅 NN） | 宿主机不暴露 DN 端口，减少干扰 |
| 启动 | `bin/start-namenode.sh` / `bin/start-datanode.sh`（`hdfs namenode -format` 只在首次） | 脚本化，不手工敲 |

**环境即代码**：`infra/hdfs/{compose.yaml,conf/core-site.xml,conf/hdfs-site.xml,bin/,inject/}` 全部进 git；`conf` 只改三处（`fs.defaultFS`、`dfs.replication=2`、`dfs.datanode.data.dir`），其余用默认。

### 2.3 为什么 `dfs.replication=2`（不是默认 3）

3 个 DN、副本 3 → 每个块三份，**任何一台 DN 挂掉都不会产生"副本不足"**，`DN_HEARTBEAT` 的现象会被掩盖。副本 2 → 3DN 里挂 1 台就立刻出现 `Under-replicated blocks`，现象更"响"、更好采。代价是 3DN 也只能容忍 1 台故障，但采集只需要"能看见"。

### 2.4 环境自检（起集群后必须跑）

```powershell
docker compose -f infra/hdfs/compose.yaml up -d
docker exec namenode hdfs dfsadmin -report          # 期望 Live datanodes (3)
docker exec namenode hdfs dfs -mkdir -p /lab/data
docker exec namenode hdfs dfs -put <本地1MB文件> /lab/data/   # 触发真实读写
docker exec namenode hdfs dfsadmin -safemode get     # 期望 OFF
docker exec namenode hdfs dfs -ls /lab/data          # 期望能看到文件
```

四条全过才允许开始采集——**"环境不干净"是 132/132 之外最常见的假数据来源**。

---

## 3. 采集哪些等级、哪些窗口（这一节回答"正常/fatal/error/warn"）

### 3.1 决策：**采集端不过滤 level，全量落盘**

| 做法 | 结论 | 理由 |
|---|---|---|
| 采集端按 `level >= WARN` 过滤 | ❌ | ①丢掉异常前后的 INFO 上下文，而"哪一步开始不对"全在 INFO 里；②过滤条件一变指纹就变，132/132 没法比；③下游要算"异常密度"，需要分母 |
| 采集端全量（`INFO` 及以上） | ✅ | 过滤是**分析的事**，不是**采集的事**。A2 只做格式归一，不做价值判断 |
| 调 log4j 到 `DEBUG` | ⬜ 仅 1 例对照 | DEBUG 会让日志量涨 10–50 倍，只采一例证明"我们的采集器扛得住量"，不进主数据集 |

**log4j 阈值语义要讲清**（这是面试常问的细节）：`rootLogger=INFO` 输出的是 **≥ INFO** 的全部级别，即 `INFO / WARN / ERROR / FATAL` 都会落盘，`DEBUG/TRACE` 不会。所以"要采 ERROR/WARN"不需要额外配置——默认就有。

### 3.2 各等级的"预期分布"，以及**不要伪造 FATAL**

| 等级 | 正常基线里 | 故障窗口里 | 说明 |
|---|---|---|---|
| `FATAL` | 0 | **通常仍是 0** | HDFS 极少打 FATAL（进程要死了才打）。**不要为了凑 FATAL 去改代码或造格式**——`severity` 用 `error` 标注即可，并在评测说明"FATAL 天然稀缺" |
| `ERROR` | **必须为 0** | ≥ 1（多数故障 5–50 条） | 正常例出现任何 ERROR = 环境不干净 → 该例作废、重采 |
| `WARN` | 允许少量（HDFS 正常也会 `Slow BlockReceiver`） | 明显增多 | 正常例的 WARN **数量本身**就是基线的组成部分 |
| `INFO` | 占 95%+ | 占多数，但异常消息密度上升 | 上下文的主要来源 |

> 一句话：**"采哪些等级"的正确答案是"全采"，然后靠 labels 与统计去分。**采集器替下游做价值判断，是旧项目把检测逻辑揉进 ingestion 的同一个错。

### 3.3 窗口结构：每个案例 20 分钟，三段

```
T-5min ────────── T0 ────────────────── T+10min ──── T+15min ──────── T+20min
   healthy 基线        故障持续（注入）        恢复动作          recovery 观察
   (5 min)             (10 min)              (脚本 restore)     (5 min)
```

| 段 | 长度 | 作用 | labels 里的标记 |
|---|---|---|---|
| `baseline` | 5 min | 证明"注入前是好的"——报告里"异常起点"要能对齐到这里 | `phase: baseline` |
| `fault` | 10 min | 主证据窗口；`DN_HEARTBEAT` 的检测阈值通常 10.5 min（`2*heartbeat.recheck.interval + 10*heartbeat.interval`），**10 分钟可能刚好差一点 → 这类故障窗口拉到 15 min** | `phase: fault` |
| `recovery` | 5 min | 恢复日志（`safemode leave`、卷重新注册）——证明"注入可逆" | `phase: recovery` |

> 关于心跳：HDFS 默认 `heartbeat.interval=3s`、`recheck.interval=5min` → 判 dead 约需 `2*300+10*3=630s ≈ 10.5min`。**`DN_HEARTBEAT` 若只采 10 分钟，很可能采不到 dead 判定**。两个办法：①该例窗口拉到 15 min；②在 `conf/hdfs-site.xml` 把 `heartbeat.recheck.interval` 调到 `60s` 加速（**推荐 ②，并在 manifest 记 `config_overrides`**，否则面试时会被问"你是不是改了参数才造出故障"）。这一条是本计划里最容易踩的坑。

### 3.4 采集矩阵：S1 只采 3 例，S6 前补齐 12 例

| ID | 故障 | 注入脚本 | 窗口 | S1 内采？ |
|---|---|---|---|---|
| `inc_dnvol_01` | 卷满 | `inject/dn_volume_full.sh`（dn1） | 20 min | ✅ |
| `inc_hb_01` | 心跳 | `inject/dn_heartbeat_drop.sh`（dn2，iptables DROP 8020） | 15 min | ✅ |
| `inc_net_01` | 网络 | `inject/net_partition.sh`（dn3，DROP 50010） | 20 min | ✅ |
| `inc_sf_01` | 安全模式 | `inject/nn_safemode.sh` | 20 min | ⬜ S2 后 |
| `inc_dnvol_02` / `inc_hb_02` / `inc_net_02` / `inc_sf_02` | 重复例 | 同上 | 同 | ⬜ S6 前 |
| `base_healthy_01..04` | **正常基线** | 无注入，只跑读写 | 20 min | 其中 1 例 ✅ |

**为什么 S1 只采 3 例**：S1 的目标是"管线通 + 端到端跑通一次"，不是"数据集齐"。采满 12 例 ≈ 4 小时纯采集 + 4 次环境重置，会把 S1 的 3 天吃掉半天。**先 3 例打通，剩下的按需补**——这也让"续采"能力有真实用武之地（同一 case 分两次采）。

### 3.5 正常基线为什么必须单独采

正常例不是"没故障的例子"，它有明确用途：

1. **FPR 分母**：报告说"这条日志异常"，正常例里必须一条都不报。
2. **模板基线**：`template_stat` 语料要用正常日志算"每个模板的正常频次"，异常 = 频次突变。
3. **说服力**：面试问"你怎么证明不是把正常当异常"，答案就是这 4 个 healthy 案例 + 一条命令。

---

## 4. 三种运行模式（同一条代码路径，三种参数）

| 模式 | 命令 | 采什么 | 何时用 |
|---|---|---|---|
| **baseline** | `collect run --case base_healthy_01 --minutes 20` | 全量日志 + 指标，无注入 | 建正常基线 |
| **inject** | `collect run --case inc_dnvol_01 --minutes 20 --inject inject/dn_volume_full.sh` | 先采 5min，到点执行注入脚本，采满 20min，自动跑 `_restore.sh` | 采故障 |
| **attach** | `collect run --case inc_x --resume` | 从 `checkpoint.adapter_state` 续采 | 中途中断 / 分两次采 |

**注入编排必须在采集器里，不能手工敲**：手工敲无法保证"注入时刻"与"日志时间"对齐，而 `labels.fault_injection.started` 正是报告里"异常起点"的 ground truth。采集器负责：`T0` 记录 → 执行脚本 → 记录 `started/ended` → 采满 → 执行 restore → 记录 `restored`。

---

## 5. 产物规格：CasePack v1（P-A 视角补全）

```
data/cases/<case_id>/
├─ manifest.json          # 身份 + 窗口 + 计数 + 两个哈希 + checkpoint（最后写，写了才算完成）
├─ logs.jsonl             # canonical LogRecord，一行一条；采集中按到达序追加，finalize 后按 (ts, id) 排序
├─ metrics.jsonl          # 指标快照，一行一个时间点
├─ labels.json            # fault_injection（自动写）+ root_cause / severity / reviewer（人工）
└─ anchors/index.json     # 指针 (file, offset, length, sha256) → runtime/ 下的原始日志，**不复制原文**
```

`logs.jsonl` 单行 = 一个 `LogRecord`（10 个字段，与 `domain/log.py` 一字不差）：

| 字段 | 规则 |
|---|---|
| `id` | `{component}:{line_no:05d}`。`line_no` 是**源文件内绝对物理行号**（由 offset 反算，不是读入顺序），所以续采不重号；id 里**不含 adapter**，换 adapter 不变 |
| `ts` | UTC，ISO8601 **一律 `+00:00` 偏移**（不写 `Z`），有毫秒则保留 |
| `level` | 闭集 `TRACE/DEBUG/INFO/WARN/ERROR/FATAL` |
| `component` | 闭集短名 `nn` / `dn1` / `dn2` / `dn3`。容器名 `namenode→nn`、`datanode1→dn1`；旧数据 `aiops-lab-dn1→dn1` |
| `logger` | `%c{1}` 短类名（`FSNamesystem`）或 `Class:行号` 原样；解析不出为 `null` |
| `message` | 脱敏后的消息体，续行用 `\n` 拼接；**不含**时间戳/级别/logger 前缀 |
| `source` | adapter 出处 `docker:<ref>` / `file:<path>`；**不进 canonical 投影**（否则 V4 永不成立） |
| `raw` | **仅 `parse_status != "ok"` 时出现**（原始整行）；ok 行为 `null` |
| `parse_status` | `ok` / `partial` / `failed` |
| `template_id` | 本期恒 `null`，P-B 回填 |

```json
{"id":"dn2:00123","ts":"2026-11-22T10:03:11.482+00:00","level":"ERROR","component":"dn2","logger":"DataNode",
 "message":"Volume /tmp/hadoop-root/dfs/data failed ...","source":"docker:datanode2","raw":null,
 "parse_status":"ok","template_id":null}
{"id":"dn2:00124","ts":"2026-11-22T10:03:11.482+00:00","level":"ERROR","component":"dn2","logger":null,
 "message":"java.io.IOException: No space left on device\n\tat org.apache.hadoop.hdfs...","source":"docker:datanode2",
 "raw":"2026-11-22 10:03:11,482 ERROR org.apache.hadoop.hdfs.server.datanode.DataNode: ...",
 "parse_status":"partial","template_id":null}
```

`manifest.json` 关键字段（★ = 相对总计划新增或明确）：

```jsonc
{
  "schema": "casepack/v1",
  "case_id": "inc_dnvol_01",
  "created_at": "2026-11-22T10:20:00+00:00",
  "pipeline_version": "collect/0.2.0",            // ★ 代码版本；只用于解释 canonical_sha256 为何变化，**不进任何一个哈希**
  "canonical_profile": "hdfs_log4j",              // ★ "hdfs_log4j" | "lab"，见 §6.2
  "redact_policy": {"version": "v1", "rules": ["host", "ip", "path", "token"]},  // ★ 也不进 input_fingerprint
  "tz": "UTC",                                     // ★ 日志时区，A2 不再猜
  "config_overrides": {"dfs.replication": "2", "heartbeat.recheck.interval": "60s"}, // ★ 改了参数必须记
  "source": [{"adapter": "docker", "ref": "datanode1", "component": "dn1",
              "image_digest": "sha256:...", "path": "/opt/hadoop/logs/..."}],
  "window": {"since": "...", "until": "...",
             "phases": {"baseline": ["...","..."], "fault": ["...","..."], "recovery": ["...","..."]}},  // ★ 可空（旧数据没有标注），空时见 §6.2 反推规则
  "origin": {"kind": "collected", "src": null},   // ★ "collected" | "migrated"；migrated 时带 incident_sha256
  "counters": {"raw_lines": 8443, "lines": 8421, "by_level": {"INFO": 8100, "WARN": 300, "ERROR": 20, "FATAL": 0},
               "by_component": {"dn1": 4000, "nn": 3000},
               "parse_failed": 22, "redacted": {"host": 0, "ip": 310, "path": 1204, "token": 0}},
  "hashes": {"input_fingerprint": "sha256:...", "canonical_sha256": "sha256:..."},   // ★ 两个，见 §6.4
  "checkpoint": {"adapter_state": {"dn1": {"inode": 1234, "offset": 8399, "line_no": 112}},
                 "last_ts": "..."}
}
```

> `counters.redacted.host` 在旧数据上是 0，不是漏采——那些日志**已经脱敏过**（`<:IP:>` / `<:PATH:>` 就是旧占位符），匹配到已有占位符时按 §6.2 的幂等规则保留原样、不计数。

`labels.json`——**谁写要分清**（v0.1 §5 说"人工标注"、§8 又说 `collect/labels.py` 自动写，此处定死）：

| 字段 | 谁写 | 说明 |
|---|---|---|
| `fault_injection` | `collect/labels.py`（自动） | `{script, started, ended, restored, restored_ok}`；baseline 例与迁移例为 `null` |
| `root_cause` / `severity` / `expect` / `reviewer` / `notes` | 人工（可留初值） | `expect` 由人先填，V5/V6 拿它当门禁；`reviewer` 初始 `null` |
| `phase_map` | `collect/labels.py` | 本期内联或 `null`，引用 `manifest.window.phases` |

```json
{"case_id":"inc_dnvol_01","root_cause":"DN_VOLUME","severity":"error","phase_map":null,
 "fault_injection":{"script":"inject/dn_volume_full.sh","started":"...","ended":"...","restored":"...","restored_ok":true},
 "expect":{"min_error_lines":1,"min_warn_lines":5,"error_components":["dn1"]},
 "reviewer":null,"notes":"dd 写满 tmpfs 128m，VolumeFailed 首次出现于 10:03:11"}
```

迁移例的 `labels.json` 由 `incident.json` 生成：`labels.root_cause_code → root_cause`，`confidence` / `review_status` / `reviewer_id` 拼进 `notes`；`fault_injection` 恒 `null`（旧数据没有注入记录）。

---

## 6. A1 / A2 / A3 各自做什么，关键规则是什么

### 6.1 A1 acquire（`collect/adapters/*.py` + `collect/collect.py`）

| 项 | 决策 |
|---|---|
| adapter 数量 | `file`、`docker`、`wsl`（本期只做前两个，`wsl` 留骨架，`http` 只留 Protocol 注释） |
| docker adapter 的两种取数 | ①`docker logs --since` 拿 stdout；②`docker exec cat <容器内路径>` 拿日志文件（**Hadoop 镜像把日志写文件，stdout 几乎是空的——这一点不搞清会白采一轮**）；同一 adapter 用 `--path` 区分 |
| 增量与续采 | 每次记录 `(inode, offset, line_no, tail_line_sha1)`；恢复时先 `tail` 校验最后一行 sha1 一致，再从 offset+1 读，**不重不漏**；`line_no` 用于恢复后继续给 `id` 编号 |
| 时间窗 | `--minutes` 相对窗口 + `--since/--until` 绝对窗口；时钟统一 UTC |
| 指标 | 每 5s 一次 `docker stats --no-stream`；每 30s 一次 `dfsadmin -report` 与 `df -h` |
| 并发 | 所有源并行拉（`nn` / `dn1–dn3` 主日志、`hdfs-audit.log`、GC、容器 stdout），各自独立 checkpoint（一个源失败不影响其他） |

### 6.2 A2 canonicalize（`collect/canonical.py` + `collect/redact.py`）

**先定 profile，再解析。** 新采的容器日志是 log4j 原生；旧 132 例**不是** log4j，而是实验台合成格式——行首多一个 `[component] ` 前缀。用同一套正则硬吃两种数据，旧数据会整批落进 `failed`。所以 `canonical_profile ∈ {hdfs_log4j, lab}`，写进 manifest。

| profile | 行首 | body 形态 | 用在哪 |
|---|---|---|---|
| `hdfs_log4j` | 时间戳 | `%d{ISO8601} %-5p %c{1}: %m%n`，时间形如 `2026-11-22 10:03:11,482`（逗号毫秒），logger 是短类名 `FSNamesystem` | 新采集（docker / file） |
| `lab` | `[component] ` | 下方语法 A / B | 旧 132 例迁移（M1.4） |

`lab` 的两种实测 body（132 例里约 2/3 是 A、1/4 是 B）：

```
# 语法 A —— real_batch_cases(60) / network_cases(21)：无毫秒、级别后双空格、logger 带行号
[aiops-lab-dn1] 2026-11-11 08:15:05 INFO  DataNode:750 - Receiving BP-... src: /<:IP:> dest: /<:IP:>

# 语法 B —— wsl_cases(30)：逗号毫秒、全限定 logger、无行号
[aiops-lab-dn1] 2026-10-29 01:12:46,453 INFO datanode.DataNode: STARTUP_MSG:
```

三步，顺序不能换：

1. **剥前缀 + 解析**。`lab` 先按 `^\[([^\]]+)\]\s?` 取 `component` 并归一（`aiops-lab-dn1 → dn1`、`aiops-lab-namenode → nn`），**剥完再进第 2 步**；`hdfs_log4j` 不剥。然后解析 `ts / level / logger / message`：时间三种形态（`,` 毫秒 / `.` 毫秒 / 无毫秒）都要吃；`logger` 可能是 `Class:行号` 也可能是 `pkg.Class:`；级别后可能有两个空格。
2. **续行归并**。判据（v0.1 写错了，以此为准）：**剥掉前缀后的 body**，首字符既不是 `YYYY-MM-DD` 也不是 `\tat`/缩进 → 判为续行。**旧数据的续行同样带 `[comp] ` 前缀**（`[aiops-lab-dn1]   getNumBytes()     = 18`、`[aiops-lab-namenode] It was turned on manually.`），所以"首列是否时间戳"必须判定在剥前缀之后，否则整批误判。续行**继承**上一行的 `ts/component/source/logger`，`message` 用 `\n` 拼接，`parse_status = partial`。
3. **脱敏**。`host / ip / path / token` 四类规则，逐条计数写 `counters.redacted`。两条硬约束：①**幂等**——匹配到已有占位符（`<:IP:>` / `<:PATH:>` / `<:HOST:>` / `<:TOKEN:>`）时原样保留、**不计数**，否则旧数据二次脱敏会改哈希，V2 与两条不变式同时破；②**不改源文件**——脱敏后的文本才进 `logs.jsonl`，原文不复制，`anchors/index.json` 用 `(file, offset, length, sha256)` 指回 `runtime/` 里的原文件。

`parse_status` 三态：`ok`（四字段全解析）/ `partial`（时间戳或级别有一个没解出来，靠继承补）/ `failed`（完全解析不了）。**failed 行不丢弃**——它们是"解析器缺陷"的证据，也是 132/132 比对时要解释的差异来源。`ok` 行 `raw = null`；`partial` / `failed` 行 `raw` 必留。

**`ts` 一律归一成 UTC、`+00:00` 偏移**；解析结果早于或晚于 `window` 超过 1 min 直接报错（A-R2 的运行时检查）。

**旧数据的窗口怎么来**：迁移例没有 `phases`、没有注入记录，但 `incident.json` 的 `timestamps.fault_start` 是可信的——`[日志首行, fault_start)` 就是**未标注的 baseline 段**（实测某例首行 12:52:12、`fault_start` 12:59:51，中间 7.6 分钟）。`recovery` 段**留空**：该例 `recovered` 与 `human_confirmed` 只差 90 ms，旧数据的恢复时刻不可信，不要拿它造假标注。

### 6.3 A3 package（`store/casepack.py`）

> 落点按总计划 v1.0 §5：CasePack 读写归 `store/casepack.py`，`domain/case.py` 只放类型。v0.1 的 `collect/pack.py` 与 `collect/manifest.py` 并入它——否则"CasePack 有两套读写"，P-B/P-C 会各写一遍。

| 项 | 决策 |
|---|---|
| 写入方式 | 采集期 `logs.jsonl` **按到达序流式追加**（中断不丢已采部分）；结束时进 `finalize`：外部归并排序 → 重算 `canonical_sha256` → 最后写 manifest |
| 排序 / 稳定键 | `finalize` 后按 `ts` 升序，同 ts 按 `id` 升序。`id` 只由 `component:line_no` 构成、**不含 adapter**，所以换 adapter 排序结果不变（V4 的前提） |
| canonical 投影 | `canonical_sha256` 只覆盖 `(id, ts, level, component, logger, message)` 的规范化 JSON；**排除 `source` / `raw` / `parse_status` / `template_id`**。否则 file 与 docker 两条路径永远不会相等 |
| 原子性 | 先写 `manifest.json.tmp` → `os.replace`；`schema` 不匹配一律拒读；**manifest 写了才算完成** |
| 校验 | `pack-lint`：字段完整性、`ts` 单调、`level` 闭集、`id` 唯一、`canonical_sha256` 重算一致、`counters` 与行数对账 |

### 6.4 指纹：为什么是两个哈希

总计划 v0.5 曾写单一公式 `sha256(case_id | source_refs | window | line_count | canonical_sha256)`；v1.0 已改成两个哈希。缺陷是原公式里 `canonical_sha256` 会随代码与脱敏策略变化——A2 改一个正则，所有历史指纹全变，"同一输入必得同一指纹"就废了。这里只留 v1.0 采纳后的定义：

| 哈希 | 覆盖什么 | 什么时候变 | 用途 |
|---|---|---|---|
| `input_fingerprint` | `case_id / source_refs / window / raw_line_count / raw_sha256(脱敏前原文)` | **只有源变了才变**（换日志、改窗口） | 证明"采的是同一份数据"；去重；续采校验 |
| `canonical_sha256` | 脱敏+归一后的 canonical 投影（见 §6.3） | 代码、profile 或脱敏策略变了就变 | 证明"处理结果一致"；132/132 比的是它 |

**排除项（必须写进契约）**：`pipeline_version`、`redact_policy.version`、`canonical_profile` 三个字段**都不进任何一个哈希**——它们的变化由 manifest 元数据解释，不能让"换了个版本号"把数据指纹弄脏。

两条不变式（进 `tests/contract/test_collect.py`）：

1. **同源重采** → `input_fingerprint` 相等，`canonical_sha256` 相等。
2. **同源 + A2 改了一处正则** → `input_fingerprint` 相等，`canonical_sha256` 不等，manifest 里 `pipeline_version` 不同（用来定位是谁改的）。

第 2 条是"可回归"的真正含义——**能证明"变了"和"为什么变"，比"永远不变"更值钱**。

---

## 7. 验收标准（跑不通不算完成）

| # | 验收项 | 命令 | 门槛 |
|---|---|---|---|
| V1 | 采样可通过 lint | `make pack-lint` | 1 个 sample CasePack 全字段合法 |
| V2 | 旧 132 例迁移一致 | `make verify-collect` | **132/132**：每例的 行数 / ts 序列 / level 分布 / component 分布 与**独立 oracle** 相等；差异白名单为空 |
| V3 | 续采不重不漏 | 采 → `Ctrl-C` → `--resume` | 合并后行数 == 一次性采满的行数；`id` 集合完全相同（无重复、无遗漏） |
| V4 | 两条读路径一致 | 同窗口分别用 `file` 与 `docker --path` 采 | `canonical_sha256` 相等（canonical 投影已排除 `source`，见 §6.3） |
| V5 | 正常例零 ERROR | `make pack-lint CASE=base_healthy_01 CHECK=levels` | `by_level.ERROR == 0` |
| V6 | 故障例有证据 | `make pack-lint CASE=inc_dnvol_01 CHECK=levels`（3 个 `inc_*` 各跑一次） | `by_level.ERROR >= 1` 且出现在 `expect.error_components` 里 |
| V7 | 可逆 | 每个 inject 脚本执行 `_restore.sh` 后跑 | `dfsadmin -report` 的 Live DN 数回到 3；`safemode get` 回 OFF |
| V8 | 脱敏有账 | 任意样例 | `counters.redacted` 四类字段齐全，`ip`/`path` 两类 > 0 且与 `grep` 抽查一致；旧数据命中已有占位符时为 0 且**不算失败** |
| V9 | 旧两种语法都能解析 | `pytest tests/collect/ -k lab` | 语法 A / 语法 B / 带前缀续行 三类 fixture 全部 `ok` 或 `partial`，`failed == 0` |

> **V2 的基准从哪来（v0.1 漏了）**：旧 132 例的 `evidence/logs/*.log` 实测只有 3–239 行，而 `data/processed/*/logs.parquet` 是旧项目的全量语料、**不是这 132 例的切片**。所以"行数 / ts 序列 / 分布全等"不能拿新解析器自己当参照——基准必须是 awk/grep 直接从 `.log` 数出来、冻结在 `tests/fixtures/migrate/` 里的期望值。V9 是 V2 的前置检查。

---

## 8. 文件级落地清单（S1 的 3 天 → 按学习线约 5–6 天）

> **路径前缀**：除 `scripts/`、`tests/`、`data/`、`infra/` 外，其余都在 `apps/backend/src/logcopilot/` 下（总计划 v1.0 §5 目录形状）。v0.1 写的是仓库根扁平路径，会让 S0 建骨架时二次搬家。

### 8.1 前置：S0 的采集侧子集（M0.x）

> 总计划 v1.0 §7 明写"每个 S 内的任务表（M0.1–M6.9）见各阶段设计文档，本文件不含"。P-A 是第一个阶段文档，S0 的采集侧落点必须写在这里，否则 M0 无处安放。

| 里程碑 | 文件 | 一句话职责 | 依赖 |
|---|---|---|---|
| M0.1 | `pyproject.toml` | uv 管理，`requires-python >= 3.11`，pytest / ruff / pyright 的配置都在这一个文件里 | — |
| M0.2 | `Makefile` | `install / lint / type / test / pack-lint / verify-collect / ask` 七个目标 | M0.1 |
| M0.3 | `apps/backend/src/logcopilot/{__init__,domain/__init__,collect/__init__,store/__init__}.py` | 包骨架（空文件；只建 P-A 需要的四个包，其余阶段再补） | M0.1 |
| M0.4 | `tests/{domain,collect,contract}/`、`tests/fixtures/migrate/` | 测试骨架 + V2 的冻结期望值 | M0.2 |
| M0.5 | `data/cases/.gitkeep`、`.gitignore`（`runtime/`、`data/cases/*/`） | 产物目录与忽略规则 | M0.3 |

### 8.2 采集本体

| 里程碑 | 文件 | 一句话职责 | 依赖 |
|---|---|---|---|
| M1.1 | `domain/log.py` | `LogRecord` / `LogLevel` / `ParseStatus` / `canonical_json`（10 字段） | 无 |
| M1.1 | `domain/case.py` | `CaseManifest` / `Case` / `RootCause` 闭集（4 码，**唯一定义处**） | `domain/log.py` |
| M1.1 | `store/casepack.py` | CasePack 读写 + `finalize` + 校验 + 两个哈希（v0.1 的 `collect/pack.py` 与 `collect/manifest.py` 并入此） | `domain/case.py` |
| M1.1 | `scripts/pack_lint.py` | CLI：校验一个 case 目录（`CASE= / CHECK=`） | `store/casepack.py` |
| M1.2 | `collect/canonical.py` | RawLine → LogRecord（profile 分发 + 剥前缀 + 续行归并） | `domain/log.py` |
| M1.2 | `collect/redact.py` | 四类规则 + 占位符幂等 + 计数 | 同上 |
| M1.2 | `tests/domain/test_log.py`、`tests/collect/test_canonical.py` | 10 + 24 条单测（含语法 A / B / 带前缀续行 三类 fixture） | — |
| M1.3 | `collect/adapters/base.py` | `SourceAdapter` Protocol（`open / read / resume`）**定义在这里**——它是 P-A 阶段内接口，不进 `ports.py`（总计划 §5 的 `ports.py` 只放跨阶段四件 `LlmClient / Retriever / CaseStore / Clock`） | `domain/log.py` |
| M1.3 | `ports.py`（`CaseStore` 段） | 跨阶段的案例读写 Protocol，P-A 只加这一个 | `domain/case.py` |
| M1.3 | `collect/adapters/file.py` | 本地文件 / 分片 / `offset + line_no` 续采 | `collect/adapters/base.py` |
| M1.3 | `collect/adapters/docker.py` | `docker logs` + `docker exec cat` 两种取数 | 同上 |
| M1.3 | `collect/collect.py` | 编排：多源并行、checkpoint、指标采样、注入调度 | 以上 |
| M1.3 | `scripts/ask.py` | **最小闭环**：CasePack → 关键词检索 → 模板报告 | `store/casepack.py` |
| M1.4 | `collect/migrate.py` + `scripts/verify_collect.py` | 旧 132 例 → CasePack（`canonical_profile=lab`）+ 四项比对 | `store/casepack.py` |
| M1.5 | `infra/hdfs/{compose.yaml,conf/,bin/}` | 集群环境即代码 | — |
| M1.5 | `collect/inject/*.sh`（4 类 + restore） | 故障注入与恢复 | `infra/hdfs` |
| M1.5 | `collect/labels.py` | 注入前后自动写 `labels.fault_injection`（**只写这一块**，`root_cause` 仍人工） | `store/casepack.py` |

**代码量预估**：`domain/` ≈ 180 行，`collect/` ≈ 700 行，`store/` ≈ 150 行，`scripts/` ≈ 200 行，测试 ≈ 500 行，`infra/hdfs/` ≈ 150 行。合计约 1900 行（比 v0.1 的 1600 多：canonical 要两个 profile、pack 要 `finalize` 与投影哈希）——**3 天交付线是紧的，学习线 5–6 天更实际**。

---

## 9. 风险与未知

| # | 风险 | 应对 |
|---|---|---|
| A-R1 | Hadoop 镜像把日志写文件而 stdout 为空，`docker logs` 采不到东西 | 先跑 `docker exec cat` 验证（§6.1），两种取数都实现 |
| A-R2 | 容器内 TZ 与日志 TZ 不一致，`ts` 错 8 小时 | 统一 `TZ=UTC`；A2 解析后与 `window` 做断言（超出窗口 ±1min 直接报错） |
| A-R3 | `DN_HEARTBEAT` 采不到 dead 判定（默认 10.5 min） | 调 `heartbeat.recheck.interval=60s` 并**写进 manifest.config_overrides** |
| A-R4 | 132 例里有 `parse_status=failed` 的行，新解析器救回 → 差异 | 逐条白名单（`verify_collect.py --whitelist`），白名单外必须修；白名单本身要写进 PR 说明，不能只留在代码里 |
| A-R5 | 日志滚动（`hadoop-*.log.1`）导致 offset 失效 | checkpoint 记 `inode`，inode 变了就从头读并向 manifest 记 `rotated: true` |
| A-R6 | tmpfs 卷满后容器内其他写操作（如日志本身）也被影响 | dn1 的 tmpfs 只挂数据目录，`logs/` 走 bind mount 到宿主机 |
| A-R7 | 采集中途 `docker compose down` 导致部分源丢数据 | 每源独立 checkpoint + 流式追加；manifest 未写 = 未完成 |
| A-R8 | 旧 132 例不是 log4j：行首带 `[component] ` 前缀、body 有 A/B 两种语法、**续行也带前缀** | profile 分离（§6.2）；V9 用三类 fixture 守住；不要在 `hdfs_log4j` 正则里塞兼容分支 |
| A-R9 | 旧数据 baseline 段未标注、`recovered` 不可信（实测 `recovered` 与 `human_confirmed` 只差 90 ms） | `phases` 由 `fault_start` 反推 `[日志首行, fault_start)`；`recovery` 段留空，不造假标注（§6.2） |

---

## 10. 仍待拍板（我的建议已写在括号里）

| # | 决策点 | 选项 | 我的建议 |
|---|---|---|---|
| **O1** | 是否采 HDFS **审计日志**（`hdfs-audit.log`） | 采 / 不采 | **采**：成本≈0，能回答"谁删的"，且它是唯一的 KV 格式——顺便证明 canonical 不只会解 log4j |
| **O2** | DN 数量 | 3 / 4 | **3**：四类故障都能造，且 `dfs.replication=2` 下 3 台能看见"副本不足" |
| **O3** | 日志时区 | UTC / Asia-Shanghai | **UTC**：宿主机是 +08:00，统一 UTC 最省心；显示层再转 |
| **O4** | 是否采 1 例 `DEBUG` 对照 | 采 / 不采 | **采 1 例**：证明"日志量 ×20 时采集器不炸"，也证明我们没为了好处理而限制输入 |
| **O5** | S1 内采 3 例够不够 | 3 / 6 / 12 | **3**：S1 求"管线通"，数据集在 S6 前补齐（§3.4） |
| **O6** | 指纹拆分（§6.4）是否采纳 | — | **已闭环**：总计划 v1.0 §4.1 已直接写成两个哈希，无需再拍板；本文只留定义 |

---

## 11. 与其他流水线的接口（只列契约，不展开）

| 对端 | P-A 给什么 | P-A 要什么 |
|---|---|---|
| P-B 索引 | `logs.jsonl`（喂 `case_report` 语料）、`labels.json` | 无 |
| P-C 诊断 | `CasePack`（工具 `search_logs` / `get_event_evidence` 读它）、`anchors/index.json`（行级引用靠它） | 无 |
| P-D 飞轮 | `labels.json` 的 `reviewer` 字段（人工标注 = 最初的金标准） | 无 |
| P-E 评测 | 12 例 + 132 例迁移结果 | 无 |

---

## 12. 变更记录

| 版本 | 日期 | 变更 |
|---|---|---|
| v0.2 | 2026-11-22 | 对齐总计划 v1.0：引用改指 §4.1 / §5 / §7 / §8.2，路径补 `apps/backend/src/logcopilot/` 前缀，补 M0.x 前置表。CasePack 读写归 `store/casepack.py`（并入原 `collect/pack.py` 与 `collect/manifest.py`）。`LogRecord` 定死 10 字段：`id = {component}:{line_no:05d}`、`ts` 用 `+00:00`、新增 `logger`、`raw` 仅非 ok 出现。canonical 拆 `hdfs_log4j` / `lab` 两个 profile，按实测语法 A/B 与"续行也带前缀"重写判据。redact 要求对已有占位符幂等。`canonical_sha256` 只覆盖 canonical 投影（排除 `source`/`raw`），排序键改 `(ts, id)`，新增 `finalize` 阶段。`pipeline_version` / `redact_policy.version` / `canonical_profile` 明确不入任何哈希。V2 改用独立 oracle + 冻结 fixture，新增 V9（两种旧语法），命令改 `CASE=`/`CHECK=`。`window.phases` 与 `fault_injection` 可空并给出反推规则；O6 标记闭环 |
| v0.1 | 2026-11-22 | 首版。定下：只采 HDFS 生态（NN/DN/audit/GC/stdout + 指标）；docker compose 1NN+3DN、`TZ=UTC`、`dfs.replication=2`；采集端不过滤 level、FATAL 不伪造；三段窗口（baseline/fault/recovery）；S1 只采 3 例；指纹拆成 `input_fingerprint` + `canonical_sha256` |
