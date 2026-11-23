# P-A 采集流水线 · 实现计划

> **版本**：v0.6（2026-11-23）—— **结项版**：P-A 的代码线已全部落地，本文此后只做"事实回填"
> **上游文档**：`IMPLEMENTATION_PLAN.md` v1.0 的 §4.1（P-A 段落）/ §5（CasePack 契约与目录形状）/ §7（S1 里程碑）/ §8.2（红线）
> **本文边界**：只讲 **P-A（采集）** 这一段——采什么日志、用什么环境采、采哪些等级与窗口、A1/A2/A3 各做什么、怎么验收。
> **不管**：窗口切分 / 模板 / 门禁 / 事件合并（那是旧项目已有的检测侧，新项目不复现）、检索与诊断（P-B / P-C）。
> **与总计划的关系**：v0.1 的"本文会修订总计划一处（指纹拆分）"**已闭环**——总计划 v1.0 §4.1 已直接写成 `input_fingerprint` + `canonical_sha256` 两个哈希。本文只保留定义与不变式，不再有"待回填"项。
> **代码落点**：下文相对路径都相对 `apps/backend/src/logcopilot/`（总计划 v1.0 §5 目录形状）；`scripts/`、`data/`、`infra/` 相对仓库根。

## 0. 结项状态（v0.8）

> 这一节是"结论先行"块：先说清 P-A **做到了什么、没做到什么**，后面各节的规则才不会被当成"待办"来读。

| 维度 | 现状（2026-11-23 实测） |
|---|---|
| 代码 | 源码 + 脚本约 **8900 行**，17 个测试文件、**457 条用例**全绿；`ruff` / `pyright` 0 问题 |
| 产物 | `data/cases/` 下 **154 个完整 pack**：**132 例迁移**（`origin=migrated`，旧实验台）+ **22 例真采**（`origin=collected`，本仓库集群，矩阵已按 §3.4 采满） |
| 闸门 | `pack-lint` 154/154、`verify-collect` 132/132（22 例新采不参与比对）、`verify-gates` V3/V4/V7 全通过 |
| 环境 | `infra/hdfs/` 一套 1NN+3DN 即代码；`make collect` 一条命令出 pack，9–14 分钟一例（2+5+2 或 2+10+2） |
| 未做 | ①`NN_SAFE_MODE` 无判别证据（四次尝试，O10 + O12），四族里它是唯一没有 level 级证据的；②`ports.py` 的 `CaseStore` 尚未落地（P-A 自己用不到）；③`anchors` 只有新采例有指针，迁移例没有（指向旧项目路径，换 clone 即悬空）；④`edits 满` 型安全模式没做 |
| 复现 | `make install && make test`（457 条）→ `make verify-collect`（132/132）→ `make pack-lint`（154/154）→ `make verify-gates ARGS="--v7 --live"`（要集群在跑） |

---

## 1. 采集什么日志（结论先行）

### 1.1 决策：**只采 HDFS 生态，不做"通用日志采集器"**

| 采 | 日志 | 容器内路径 | 格式 | 为什么值得采 |
|---|---|---|---|---|
| ✅ | **NameNode 主日志** | `/opt/hadoop/logs/hadoop-hdfs-namenode-<host>.log` | log4j pattern | `NN_SAFE_MODE`、块副本不足、`SlowDisk`、`FSNamesystem` 异常的唯一来源 |
| ✅ | **DataNode 主日志** | `/opt/hadoop/logs/hadoop-hdfs-datanode-<host>.log` | log4j pattern | `DN_VOLUME`（卷失败）、`DN_HEARTBEAT`（注册/心跳）、`DataXceiver` 连接错误都在这 |
| ✅ | **HDFS 审计日志** | `/opt/hadoop/logs/hdfs-audit.log` | 空白分隔 KV | 谁删了块、谁触发了 safemode——用它补"人的动作"这条链 |
| ✅ | **NameNode GC 日志** | `/opt/hadoop/logs/hadoop-hdfs-namenode-*-gc.log`（可选） | JVM GC | 成本≈0，`NN_SAFE_MODE` 里"内存不足触发"的场景需要它 |
| ✅ | **容器 stdout/stderr** | `/opt/hadoop/logs/<component>.out`（由 compose 的 `tee` 落盘） | 混合 | 容器启动失败、OOMKilled、镜像内脚本报错**只在标准输出里**，日志文件里看不到。**不用 `docker logs` 采**：它给不出字节偏移，续采会重读或漏读（O7）；落成文件后与主日志走同一条 `tail -c` 语义 |
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
| 数据盘 | dn1 用 `tmpfs size=2g`，dn2/dn3 用 volume | `dd 4GB` 必然写爆 dn1，注入稳定且不伤宿主磁盘。**128m 不够**：20 分钟负载自己就能写满它，故障不可控（见 §2.5） |
| 内存 | 4 容器 × 512m heap ≈ 2.5–3.5GB | 本机足够 |
| 端口 | 9870 / 9000（仅 NN） | 宿主机不暴露 DN 端口，减少干扰 |
| 启动 | `bin/start-namenode.sh` / `bin/start-datanode.sh`（`hdfs namenode -format` 只在首次） | 脚本化，不手工敲 |

**环境即代码**：`infra/hdfs/{compose.yaml,conf/core-site.xml,conf/hdfs-site.xml,bin/,inject/}` 全部进 git；`conf` 只改四处（`fs.defaultFS`、`dfs.replication=3`、`dfs.blocksize=32m`、`dfs.datanode.data.dir`），其余用默认。

### 2.3 为什么 `dfs.replication=3` + `dfs.blocksize=32m`（v0.4 改）

**block 太大，dn1 就永远不接块。** NameNode 选副本时第一道门槛是 `available >= blocksize`：dn1 的盘只有几百 MB，默认 128MB 的块它放不下，于是 `dfs.replication=2` 时 NN 会把两块都放 dn2/dn3 —— dn1 的 `DirectoryScanner` 常年 `Total blocks: 0`，把它的盘写满对集群**毫无影响**，日志里一行都不会有（2026-11-22 实采确认：20 分钟、原样注入、dn1 记录 0 条）。这等于"靶子不在射程内"。

改成 `blocksize=32m` + `replication=3` 之后，每个块都要落三台，dn1 必然接块（同一环境重采：dn1 记录 1127 条，盘占用从 1% 涨到 79%），盘满才会真的影响写管道与副本放置。

副本 3 在"挂一台"上的表现也比副本 2 更清楚：3 副本 1 台全丢 → 2 副本 < 3，NN 立刻报 `Under-replicated blocks: N`；同时 Live DN 从 3 掉到 2。原 v0.1 那句话（"副本 3 不会产生副本不足"）是写反了。

### 2.4 dn1 的盘到底多大（v0.4 改）

`128m` 太小、`512m` 也不够：workload 以约 40–70 MB/分钟的速度往 HDFS 写，20 分钟窗口光正常负载就能把 512m 写满 —— 于是"故障段"其实在 baseline 里就开始了，谁也没法证明盘是被注入写满的。现在取 `2g`，正常负载 20 分钟约占 40–60%，`dd` 用 4096MB 强制撞 ENOSPC。

配套要求（第 14 步起已实现）：**`dd` 的 `count_mb` 必须大于盘容量**，否则 `dd` 成功退出、盘却没满，而 `expect_ok=False` 会把这种"什么都没发生"吞掉。第一次实采就是这么白白跑了 20 分钟：盘 512m、`count=200`，`df` 从 1% 到 39% 就停了。

### 2.5 环境自检（起集群后必须跑）

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

### 3.4 采集矩阵：22 例已全部采满（v0.8 回填）

> v0.1 写的是 12 例，v0.6 按实测改成 22 例矩阵 —— 加列的每一例都对应"某个变体在日志里的表现不一样"，不是凑数。
> v0.8 真采到 22 例（`origin=collected`）。**唯一没采满的是 `NN_SAFE_MODE`**：四次尝试（手动安全模式 ×2、块缺失背景 + 安全模式、带负载安全模式）全部**没有一条可判别的非 INFO**，全部移到 `runtime/no-evidence/`（O10 + O12）。缺的 4 个安全模式槽位由"多打几遍其余三族"补齐 —— 这是有意的取舍：宁可同一族多两个变体，也不往数据集里塞 0 证据的样本。

| 族 | ID | 注入变体（`collect/inject.py` 的键） | 实测（记录数 / ERROR / WARN） |
|---|---|---|---|
| 正常 | `base_healthy_01/02/03` | 无注入 + 读写负载（空载集群不写日志，healthy 例必须有负载） | 1590 / 641 / 1669，**ERROR 全 0** |
| 正常 | `base_noise_01/02` | 良性扰动：窗口内 `docker restart` 一台 DN；负载尖峰再整体停 | 147 / 1127，**ERROR 全 0**（"重启不等于故障"，V5 的分母） |
| 卷 | `inc_dnvol_01/04/05` | `dn_volume`：dn1 的 2g tmpfs 写满 4g（fault 5 / 8 / 10 分钟） | 3200 / 4430 / 5306，**ERROR 全 0**，证据全在 WARN（`Failed to place enough replicas`） |
| 卷 | `inc_dnvol_02/03` | `dn_disk_broken`：卷里的 `current` 换成普通文件后重启（dn2 / dn3） | 335（**ERROR=1**）/ 2998（**ERROR=7**），`Initialization failed for Block pool` |
| 心跳 | `inc_hb_01/03/06` | `dn_heartbeat_pause`（dn2 / dn1 / dn3），dead≈30s | 1627（E3）/ 2459（E4）/ 2532（E4） |
| 心跳 | `inc_hb_02/04/05` | `dn_heartbeat_stop`（dn2 / dn1 / dn3，fault 5 / 5 / 8 分钟） | 2668（E2）/ 2710（E2）/ 3715（E3） |
| 网络 | `inc_net_01/03/04/05` | `network`（disconnect dn3 / dn1 / dn2 / dn3，fault 5/5/5/8 分钟） | 2480（E3）/ 2663（E2）/ 2494（E2）/ 3675（E3） |
| 网络 | `inc_net_02` | 同 `net_disconnect`，dn3 出网 + 客户端负载 | 1586（E3，含客户端 WARN 进 pack） |
| 网络 | `inc_net_06` | `network` 摘掉 **NameNode**（dn↔nn 全断） | 1376（**E0**，WARN 56 是真证据：`Failed to place enough replicas ... unavailableStorages`、`lease recovery`） |
| 安全模式 | `inc_safemode_01..04` | `nn_safemode`；带负载；块缺失背景 | **四次全部无判别证据** → `runtime/no-evidence/`（O10 / O12） |

**为什么当初只采 3 例（v0.1 的账）**：S1 的目标是"管线通 + 端到端跑通一次"，不是"数据集齐"。采满 22 例 ≈ 9 小时纯采集 + 22 次环境重置，会把 S1 的 3 天吃掉一天半。**先打通，再按矩阵补** —— v0.8 补完的 22 例共花约 4 小时（每例 9–14 分钟，含自动标注与 `pack-lint` 复查）。

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
└─ anchors/index.json     # 指针 (id, file, offset, length, sha256) → runtime/ 下的原始日志，**不复制原文**
```

`anchors/index.json` 的三个约定（v0.6 补，之前 139 个 pack 的 `entries` 一直是空数组）：

- `id` 指回 `logs.jsonl` 的某条记录（形如 `dn1:00042`）。没有它，指针只是"某个文件里的一段字节"，下游拿到一条记录无法反查原文。
- **只给证据行记指针**：非 INFO 或 `parse_status != ok` 的记录。5 分钟基线有上千条 INFO，逐条记指针会让 `index.json` 比 `logs.jsonl` 还大，而那里面一条也不值得回看。
- `length` 与 `sha256` 都是**原始字节**口径（含行尾换行），所以按 `(offset, length)` 读出来就是那行原话，`sha256` 能证明它没被换过（CRLF、非法 UTF-8 都在字节层，只有原始字节校验得住）。
- 中途被打断时，指针存在 `checkpoint.json` 里 —— `index.json` 只在 finalize 写一次，不这么做的话 attach 会把第一轮的证据**静默**丢掉（`entries: []` 仍然是合法 pack，不会报错）。

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
| docker adapter 的取数 | **只用一种语义：把容器当远端文件系统**。`stat -c '%i %s'` 拿 inode/size、`tail -c +N` 拿 offset 之后的字节、`sh -c 'tail -c +N … \| head -c L'` 拿 offset 前一行、`ls -1` 解析 glob。**不用 `docker logs --since`**：它只给时间戳不给字节偏移，续采只能猜，V3（不重不漏）当场失守；而 Hadoop 镜像把日志写文件，stdout 本来也几乎是空的。容器 stdout 用 §2 的 `tee` 落成 `<component>.out` 再走同一条 `tail -c` 路径（**O7 已闭环**） |
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
| V2 | 旧 132 例迁移一致 | `make verify-collect` | **132/132**：每例的 行数 / level 分布 / component 分布 / 两个哈希 与**独立第二实现**重算的结果相等，且与 `tests/fixtures/verify/baseline.json` 冻结的基准逐字段相等；差异白名单为空 |
| V3 | 续采不重不漏 | `make verify-gates ARGS="--v3 A B"` | 两侧**各自**没有重复 `id`；记录内容的多重集合相同（`(ts, level, component, logger, message, raw, parse_status)` 逐条对账）；`counters` 相同。**不比 `id` 序列与 `canonical_sha256`**（见下方 O11） |
| V4 | 两条读路径一致 | 同窗口分别用 `file` 与 `docker --path` 采 | `canonical_sha256` 相等（canonical 投影已排除 `source`，见 §6.3） |
| V5 | 正常例零 ERROR | `make pack-lint CASE=base_healthy_01 CHECK=levels` | `by_level.ERROR == 0` |
| V6 | 故障例有证据（**只对 `origin=collected` 的新采例成立**） | `make pack-lint CASE=inc_dnvol_02 CHECK=levels`（4 个 `inc_*` 各跑一次） | `min_error_lines + min_warn_lines >= 1`；实测 `ERROR`/`WARN` 各自不低于门槛；`expect.error_components` 非空且其中每个组件都真的出现过**非 INFO**（WARN 或 ERROR）记录。**不做反向断言**：expect 之外的组件跟着报错不算失败 |
| V7 | 可逆 | 每个 inject 脚本执行 `_restore.sh` 后跑 | `dfsadmin -report` 的 Live DN 数回到 3；`safemode get` 回 OFF |
| V8 | 脱敏有账 | 任意样例 | `counters.redacted` 四类字段齐全，`ip`/`path` 两类 > 0 且与 `grep` 抽查一致；旧数据命中已有占位符时为 0 且**不算失败** |
| V9 | 旧两种语法都能解析 | `pytest tests/collect/ -k lab` | 语法 A / 语法 B / 带前缀续行 三类 fixture 全部 `ok` 或 `partial`，`failed == 0` |
| V10 | 产物可被人查询（v0.6 新增） | `make ask CASE=inc_dnvol_02 ARGS="--level ERROR"` | 命中行按 `anchors` 回读出**原始字节**；指针不可用时必须打印原因（文件没了 / 偏移越界 / sha256 对不上），不许静默当成"没有原文"；`--json` 的字段集合稳定 |


> **V2 的基准从哪来（v0.1 漏了）**：旧 132 例的 `evidence/logs/*.log` 实测只有 2–993 行，而 `data/processed/*/logs.parquet` 是旧项目的全量语料、**不是这 132 例的切片**。所以"全等"不能拿新解析器自己当参照。现在的做法是两层：①`store/verify.py` 用**独立第二实现**只读 `logs.jsonl` 文本重算（不 import `domain.log`、不调 `casepack` 的计算函数），与生产者只共享契约；②把通过时的指纹冻结进 `tests/fixtures/verify/baseline.json`，此后任何一例的任何数字变化都算失败，必须显式 `--update-baseline` 并让基准 diff 出现在提交里。V9 是 V2 的前置检查。
>
> **V6 为什么只对新采例成立**：迁移的 132 例里 `ERROR` 只出现在 `DN_VOLUME`（28/28 例），心跳 37 例、网络 21 例、安全模式 46 例**一行 ERROR 都没有**（P-E 的"漏检"口径必须知道这件事）。拿旧数据卡 V6 只会逼人反填 `expect`，所以 `audit_levels()` 对 `origin=migrated` 直接放行；V6 的证据只认 `origin=collected`。

> **V6 v0.4：从"ERROR >= 1"改成"非 INFO >= 1"**。2026-11-22 在真实集群上采第一个 `DN_VOLUME` 故障例（把 dn1 的 128MB tmpfs 写满）时，`ERROR` 是 **0** —— HDFS 盘满的日志表现就是持续 WARN（`Not able to place enough replicas` 之类），ERROR 只在 DN **启动失败**时出现（旧语料 28 例的 ERROR 全是 `DiskChecker$DiskErrorException` / `Failed to acquire lock`，不是"盘满"本身）。把"ERROR >= 1"当红线，意味着四族里只有"能造出启动失败"的那一两个变体才合格，其余全被判死；而人能"凭实测填"的 expect 会因此被迫往上抬，校验就变成了自证。
>
> 现在的三条是**正向断言**：门槛不全 0、实测 ERROR/WARN 各自达标、`expect.error_components` 里点名的组件真的出过非 INFO 记录。**刻意不做反向断言**（"expect 之外的组件不许报错"）：故障会级联，上游出事、下游跟着 WARN 是正常的；拿反向去卡只会让真实样本过不了。`expect.error_components` 的语义也随之放宽成"哪个组件会出现非 INFO 记录"，字段名沿用是为了不动 schema。
> **O10（已知空洞）：`NN_SAFE_MODE` 造不出非 INFO 证据。** 两次实采都指向同一结论。2026-11-22 第一次（`inc_safemode_01`，`dfsadmin -safemode enter` 2 分钟，无负载）：故障段 212 条记录里 **211 条 INFO、1 条与故障无关的 WARN、0 条 ERROR**。2026-11-23 第二次（`inc_safemode_02`，同样 enter 5 分钟，**带读写负载**，1860 条记录）：**NN 侧 WARN 0 条、ERROR 0 条**；全局 34 条 WARN 全是每例都有的 `Unexpected value for data transfer bytes=..` 噪声，且都在 dn1/dn2/dn3 上，与安全模式无关。机制是：安全模式是 NN 的**保护动作**，NN 只在主日志记一条 INFO（`Safe mode is ON`）、在审计日志记 `safemode_get/enter/allowed=false`，DN 侧什么也不记。两次都移到 `runtime/no-evidence/`。**P-E 评测必须知道：这一族只有 INFO 判别信息**，不能按"有没有非 INFO"来算召回；要造出真 WARN 只能做"块缺失型"（删掉某个 block 的全部三份副本再进安全模式，NN 才会打 `Safe mode is ON. The reported blocks .. needs additional ..`），代价是破坏性操作 + 约 15 分钟环境重置，本期不做。

> **O12（已知失真）：客户端的 FsShell 纯文本错误会被"续行"吞掉。** `workload.out` 里真实存在这些行（`put: Cannot create file/workload/w_1.txt._COPYING_. Name node is in safe mode.`，`inc_safemode_02` 的窗口内就有几十条），但它们没有时间戳 → 按续行判据被并进上一条 log4j 记录的 `raw`，于是**既不算一条记录、也不进 `by_level`**。也就是说：客户端侧的失败在 level 分布里是**结构性不可见**的，只能从某条 partial 记录的 `raw` 里读出来。要修得给 `workload` 流加一条"命令行失败前缀（`put:` / `rm:` / `mv:` / `mkdir:` …）单独成条、级别记 WARN"的规则，属于 canonical 的口径变更，与 O11 一起留给以后。

> **O11（已知约束）：`id` 里的行号是"读的先后"，不是内容坐标。** `id` 形如 `dn1:00123`，其中 123 由 `LineAllocator` 全局自增发出，按"哪个流先被读"决定。一次采完与"崩了再续"的读法不同：崩点之后继续读的那条流会拿到比别的流更大的号，于是**同一毫秒**（几条流各写一行、ts 完全相同）的平局排序会变，`canonical_sha256` 跟着变 —— 内容一字不差，只是 id 在平局内部换了位置。
>
> 2026-11-23 做 V3 闸门时用真实集群打出来的：两侧各 194 条、id 集合与 counters 全等、内容多重集合全等，但 39 条的 id 落了不同的行，`canonical_sha256` 不同。**处置**：V3 只断言"不重不漏 + 内容一致"（`compare_packs`），不比 id 序列与哈希。要把这两个也稳定下来，得让 `id` 由「流名 + 流内偏移」派生而不是全局自增 —— 那是 id 格式的 schema 变更，132 个已迁移 pack 都要重迁，并同步改 `tests/fixtures/verify/baseline.json`。真要做时单开一步。


---

### 7.1 当前实况（v0.8 回填，2026-11-23 真跑）

| 验收项 | 命令 | 最近一次结果 |
|---|---|---|
| V1 | `make pack-lint` | **154 个 pack，0 个有问题** |
| V2 | `make verify-collect` | **132/132 一致**（22 例新采不参与比对：时间戳每次采集都变，冻进基准等于把"重采一次"变成回归失败） |
| V3 | `make verify-gates ARGS="--v3 A B"` | 通过（同一批起点、一次采完 vs 中断后 attach，内容多重集合与 counters 全等） |
| V4 | `make verify-gates ARGS="--v4"` | 通过：nn/dn1/dn2/dn3 四条主流**逐字节相同**（各 64 KiB 区间比对） |
| V5 | `make pack-lint CASE=base_healthy_01 CHECK=levels` | 通过（`ERROR == 0`）；负样本 `base_noise_01/02`（窗口内重启 dn3、负载尖峰）同样 `ERROR == 0` |
| V6 | `make pack-lint CASE=<每例> CHECK=levels` | **22 例逐例通过**：`inc_dnvol_02`（E1）、`inc_dnvol_03`（E7）、`inc_hb_0x`（E2–E4）、`inc_net_0x`（E2–E3）、`inc_dnvol_01/04/05`（E0，走 WARN 证据） |
| V7 | `make verify-gates ARGS="--v7 --live"` | 通过（容器 4 个在跑、安全模式 OFF、Live DN=3、无 `lab.fill` 残留） |
| V8 | 同上（`counters.redacted`） | 通过（新采例 `ip`/`path` 两类 > 0；迁移例命中已有占位符为 0，不算失败） |
| V9 | `pytest -k lab` | 通过（语法 A / B / 带前缀续行） |
| V10 | `make ask ...` | 通过（`base_healthy_02` 的 71 条指针逐条回读自洽；三种悬空原因都能打印） |

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
| M1.3 | `collect/adapters/docker.py` | 容器当远端文件系统：`stat -c` / `tail -c +N` / `ls -1` 还原同一套 offset 语义（不用 `docker logs --since`） | 同上 |
| M1.3 | `collect/collect.py` | 编排：多源并行、checkpoint、指标采样、注入调度 | 以上 |
| M1.3 | `scripts/ask.py` | **最小闭环**：CasePack → 关键词检索 → 模板报告 | `store/casepack.py` |
| M1.4 | `collect/migrate.py` + `store/verify.py` + `scripts/verify_collect.py` + `tests/fixtures/verify/baseline.json` | 旧 132 例 → CasePack（`canonical_profile=lab`）；V2 = 独立第二实现 + 冻结基准逐例对账 | `store/casepack.py` |
| M1.5 | `infra/hdfs/{compose.yaml,conf/,bin/}` | 集群环境即代码 | — |
| M1.5 | `collect/inject/*.sh`（4 类 + restore） | 故障注入与恢复 | `infra/hdfs` |
| M1.5 | `collect/labels.py` | 注入前后自动写 `labels.fault_injection`（**只写这一块**，`root_cause` 仍人工） | `store/casepack.py` |

**代码量预估**：`domain/` ≈ 180 行，`collect/` ≈ 700 行，`store/` ≈ 150 行，`scripts/` ≈ 200 行，测试 ≈ 500 行，`infra/hdfs/` ≈ 150 行。合计约 1900 行（比 v0.1 的 1600 多：canonical 要两个 profile、pack 要 `finalize` 与投影哈希）——**3 天交付线是紧的，学习线 5–6 天更实际**。

### 8.3 实际交付（v0.6 回填）

> 上表的估算是 1900 行，实际约 **8900 行源码 + 6600 行测试**。差 4 倍不是"写多了"，而是估算时只数了"主流程"，没数 docstring（每个文件开头都写"为什么这么做"）、错误分支与 CLI。如实记在这里，方便下个阶段估工时。

| 文件 | 行数 | 落地情况 |
|---|---:|---|
| `domain/log.py` | 285 | ✅ `LogRecord` 十字段 + 6 类校验 + `canonical_payload` |
| `domain/case.py` | 993 | ✅ manifest / labels 全套（枚举 6 个、类 13 个） |
| `store/casepack.py` | 705 | ✅ 读写 + `finalize` + `audit` / `audit_levels` + 两个哈希 + anchors + `checkpoint.json` sidecar |
| `store/verify.py` | 356 | ✅ V2 的独立第二实现 + 冻结基准（**v0.1 没规划**：原计划把核验塞在 `scripts/` 里，实测发现"逻辑要在库里才能被测试直接调"） |
| `store/query.py` | 429 | ✅ `make ask` 的检索与报告（**v0.1 没规划**，只写了 `scripts/ask.py`） |
| `collect/canonical.py` | 434 | ✅ 两个 profile + 剥前缀 + 续行归并 |
| `collect/redact.py` | 251 | ✅ 四类规则 + 哨兵幂等 + 计数 |
| `collect/adapters/base.py` | 399 | ✅ 协议 + `ComponentReader`（统一发号 + mid-stream checkpoint） |
| `collect/adapters/file.py` | 357 | ✅ glob 布局 + 半行不消费 + 三查续采 + spans |
| `collect/adapters/docker.py` | 408 | ✅ 远端文件系统四命令 + 与 file 逐字节对齐 |
| `collect/collect.py` | 871 | ✅ 三模式 + 跟随 + 指标 + 注入调度 + 窗口硬边界 + `resume_from` |
| `collect/metrics.py` | 421 | ✅ `docker stats` / `dfsadmin -report` / `safemode` / `df` 四类采样（**v0.1 没规划**） |
| `collect/inject.py` | 463 | ✅ 六种注入变体的编排（**v0.1 只写了"注入脚本"**；脚本本身在 `infra/hdfs/inject/`，因为要挂进容器） |
| `collect/labels.py` | 205 | ✅ `measure` / `annotate` / `describe`（机器写 `fault_injection`，人写其余） |
| `collect/gates.py` | 504 | ✅ V3 / V4 / V7 三条闸门（**v0.1 只在 §7 写了验收标准，没有实现位置**） |
| `collect/migrate.py` | 587 | ✅ 132 例迁移（registry + 时区平移 + `content_sha256` 校验） |
| `scripts/`（9 个） | 1311 | ✅ `pack_lint` / `verify_collect` / `verify_gates` / `collect_case` / `annotate_case` / `ask` / `migrate_cases` / `snapshot_metrics` |
| `infra/hdfs/`（20 个文件） | — | ✅ compose + 4 份 conf（含客户端 `client-log4j.properties`）+ 3 个 bin（含 `workload.sh`）+ 10 个 inject 脚本 |
| 测试（17 个文件） | 6589 | ✅ 457 条用例；仓库根 `tests/fixtures/verify/baseline.json`（2083 行）是 V2 的冻结基准 |

**与 §8.2 清单的三处偏差（都是有意的，记在这里免得下个人以为是漏项）**：

1. **`ports.py` 至今没有落地**。计划里写"P-A 只往 `ports.py` 加一个 `CaseStore`"，但 P-A 自己一次也没用到它——`store/casepack.py` 是具体实现，P-C 才需要 Protocol。**结论**：等 P-B/P-C 真的需要"可替换的案例存储"时再加，否则就是一个没人 import 的空接口。
2. **`tests/fixtures/migrate/` 没有建**。V2 的期望值最终落在仓库根 `tests/fixtures/verify/baseline.json`（132 例的指纹），比"每例一个 fixture 文件"更容易审阅 diff。
3. **`collect/inject/*.sh` 改成 `infra/hdfs/inject/*.sh`**。脚本要 `docker exec` 进容器执行（`dd` 写盘、`dfsadmin safemode`），必须被 compose 挂载；而采集链路调的是 Python 的 `collect/inject.py`（Windows 上不一定有 bash）。

---

## 9. 风险与未知

| # | 风险 | 应对 |
|---|---|---|
| A-R1 | Hadoop 镜像把日志写文件而 stdout 为空，`docker logs` 采不到东西 | **已落地**：docker 只走"远端文件系统"一条路（§6.1），stdout 由 §2 的 `tee` 落成文件；`O7` 闭环 |
| A-R2 | 容器内 TZ 与日志 TZ 不一致，`ts` 错 8 小时 | 统一 `TZ=UTC`；A2 解析后与 `window` 做断言（超出窗口 ±1min 直接报错） |
| A-R3 | `DN_HEARTBEAT` 采不到 dead 判定（默认 10.5 min） | 调 `heartbeat.recheck.interval=60s` 并**写进 manifest.config_overrides** |
| A-R4 | 132 例里有 `parse_status=failed` 的行，新解析器救回 → 差异 | **没有白名单**：V2 的差异一律红，解释清楚之后 `--update-baseline` 重新冻结，基准文件的 diff 就是审计记录（写进 PR 说明） |
| A-R5 | 日志滚动（`hadoop-*.log.1`）导致 offset 失效 | checkpoint 记 `inode`，inode 变了就从头读并向 manifest 记 `rotated: true` |
| A-R6 | tmpfs 卷满后容器内其他写操作（如日志本身）也被影响 | dn1 的 tmpfs 只挂数据目录，`logs/` 走 bind mount 到宿主机 |
| A-R7 | 采集中途 `docker compose down` 导致部分源丢数据 | 每源独立 checkpoint + 流式追加；manifest 未写 = 未完成 |
| A-R8 | 旧 132 例不是 log4j：行首带 `[component] ` 前缀、body 有 A/B 两种语法、**续行也带前缀** | profile 分离（§6.2）；V9 用三类 fixture 守住；不要在 `hdfs_log4j` 正则里塞兼容分支 |
| A-R9 | 旧数据 baseline 段未标注、`recovered` 不可信（实测 `recovered` 与 `human_confirmed` 只差 90 ms） | `phases` 由 `fault_start` 反推 `[日志首行, fault_start)`；`recovery` 段留空，不造假标注（§6.2） |

---

## 10. 决策台账（O1–O5 已落进代码，O6–O11 见下）

| # | 决策点 | 状态 |
|---|---|---|
| **O1** | 是否采 HDFS 审计日志 | **已定：采**。`hdfs-audit.log` 是 NN 的第 2 条流（`file.py::hdfs_streams` order=1），也是 canonical 里唯一非 log4j 的语法；`log4j.properties` 给审计 logger 单独 appender 且 `additivity=false`，否则同一行进两条流 |
| **O2** | DN 数量 | **已定：3**（`alcp-dn1..3`）。v0.4 起 `dfs.replication=3`，四类故障都能造 |
| **O3** | 日志时区 | **已定：UTC**。容器 `TZ=UTC` + `%d{ISO8601}`；迁移的 WSL 51 例在 migrate 里按来源 −8h 平移到 UTC |
| **O4** | 是否采 1 例 `DEBUG` 对照 | **未做**：`log4j.properties` 现在是 `rootLogger=INFO`。要做就在补样本矩阵时加一例（把它记进 `manifest.config_overrides`，否则同一批语料的 level 分布没法横向比） |
| **O5** | S1 内采 3 例够不够 | **已定并超出：6 例**（`base_healthy_01/02`、`inc_dnvol_01/02`、`inc_net_02`、`inc_hb_01`）。剩余 16 例见 §3.4 的 22 例矩阵 |

| # | 决策点 | 选项 | 我的建议 |
|---|---|---|---|
| **O1** | 是否采 HDFS **审计日志**（`hdfs-audit.log`） | 采 / 不采 | **采**：成本≈0，能回答"谁删的"，且它是唯一的 KV 格式——顺便证明 canonical 不只会解 log4j |
| **O2** | DN 数量 | 3 / 4 | **3**：四类故障都能造，且 `dfs.replication=2` 下 3 台能看见"副本不足" |
| **O3** | 日志时区 | UTC / Asia-Shanghai | **UTC**：宿主机是 +08:00，统一 UTC 最省心；显示层再转 |
| **O4** | 是否采 1 例 `DEBUG` 对照 | 采 / 不采 | **采 1 例**：证明"日志量 ×20 时采集器不炸"，也证明我们没为了好处理而限制输入 |
| **O5** | S1 内采 3 例够不够 | 3 / 6 / 12 | **3**：S1 求"管线通"，数据集在 S6 前补齐（§3.4） |
| **O6** | 指纹拆分（§6.4）是否采纳 | — | **已闭环**：总计划 v1.0 §4.1 已直接写成两个哈希，无需再拍板；本文只留定义 |
| **O7** | docker adapter 是否实现 `docker logs --since` 取 stdout | 实现 / 不实现 | **已闭环：不实现**。它给不出字节偏移，续采必猜，V3 失守；stdout 改由 §2 的 `tee` 落盘，复用同一条 `tail -c` 语义（一进 §6.1、A-R1） |
| **O8** | 是否采 `gc.log` | 采 / 不采 | **已闭环：不采**（v0.3 改）。Java 8 的 GC 行 `2026-11-22T11:46:27.166+0000: ...` 既不符 `hdfs_log4j` 也不符 `lab`，续行判据会把它整段拼进最后一条 DataNode 日志。要收它得先加 `jvm_gc` 档位 |
| **O9** | 服务端 ERROR 从哪来 | — | **已闭环**：HDFS 服务端 ERROR 基本只来自**启动期致命失败**（`DiskChecker$DiskErrorException` / `Failed to acquire lock` / `Initialization failed for Block pool`）。运行期故障只打 WARN，所以 V6 改成"非 INFO >= 1"（§7） |
| **O10** | `NN_SAFE_MODE` 造不出非 INFO | — | **已知空洞（四次实证）**：手动 enter（空载 / 带负载）、块缺失背景 + 安全模式、8 分钟带负载，四例的全局 WARN 全部是 `Unexpected value for data transfer` 噪声，NN 侧非 INFO 恒为 0。根因是安全模式是**保护动作**：NN 只在拒绝写时用 INFO 说"Safe mode is ON"，真正的错误发生在客户端且被 O12 吞掉。P-E 必须按"这一族只有 INFO"来算 |
| **O11** | `id` 的行号是"读的先后"而非内容坐标 | 现在改 / 以后改 | **以后改**：V3 因此只断言"不重不漏 + 内容一致"，不比 id 序列与 `canonical_sha256`。真要改得让 id 由「流名 + 流内偏移」派生，属于 id 格式的 schema 变更，132 个迁移 pack 与仓库根的 `tests/fixtures/verify/baseline.json` 都要重做（§7 的 O11 段有实测记录） |

---

## 11. 与其他流水线的接口（只列契约，不展开）

| 对端 | P-A 给什么 | P-A 要什么 |
|---|---|---|
| P-B 索引 | `logs.jsonl`（喂 `case_report` 语料）、`labels.json`；154 个 pack 即现成语料 | 无 |
| P-C 诊断 | `CasePack`（工具 `search_logs` / `get_event_evidence` 读它）、`anchors/index.json`（行级引用靠它）、`store/query.py` 的 `search()`（现成的确定性检索，别再造一遍） | 无 |
| P-D 飞轮 | `labels.json` 的 `reviewer` 字段（人工标注 = 最初的金标准） | 无 |
| P-E 评测 | **22 例真采 + 132 例迁移**（真采例有 anchors、有 metrics；迁移例两者为空） | 无 |

---

## 12. 变更记录

| 版本 | 日期 | 变更 |
|---|---|---|
| v0.8 | 2026-11-23 | **矩阵采满 22 例**：§3.4 重写成实测表（每族的 ID、注入变体、记录数 / ERROR / WARN），补 `inc_dnvol_03/04/05`、`inc_hb_03/04/05/06`、`inc_net_03/04/05/06`、`base_healthy_03`、`base_noise_02`；`NN_SAFE_MODE` 第四次尝试仍无判别证据（块缺失背景 + 安全模式，1826 条全是噪声 WARN），四例全部留 `runtime/no-evidence/`，**缺的 4 个槽位改由其余三族多采变体补齐**（理由写进 §3.4）。§0 计数回填（154 pack / 22 例真采 / 闸门 154-154、132-132）；§7.1 V6 改成"22 例逐例通过"；§11 接口表更正为 22 例 |
| v0.7 | 2026-11-23 | **补样本到 9 例**：`inc_net_01`（disconnect dn3）、`inc_hb_02`（stop dn2）、`base_noise_01`（无负载 + 窗口内重启 dn3 的负样本，`ERROR=0`）三例入 `data/cases/`，安全模式第二例 `inc_safemode_02`（带负载 5 分钟）**仍然 0 条真 WARN**、移到 `runtime/no-evidence/`。新增 O12（客户端 FsShell 纯文本错误被续行吞掉，level 分布结构性看不见）；O10 补第二次实测；§3.4 矩阵、§7.1 实况、§0 计数（141 pack / 9 例真采）回填 |
| v0.6 | 2026-11-23 | **结项版**：新增 §0 结项状态（8900 行源码 / 457 条用例 / 138 个 pack / 闸门实况 / 四件未做）；§7 新增 **V10（产物可被人查询）** 并把 V1–V10 的最近一次结果回填成 §7.1 实况表；新增 §8.3 实际交付清单（逐文件行数 + 三处有意偏差：`ports.py` 未落地、`tests/fixtures/migrate/` 改仓库根 `tests/fixtures/verify/baseline.json`、`collect/inject/*.sh` 改 `infra/hdfs/inject/`）；§10 拆成"已落进代码（O1–O5）"与"仍待拍板/已知约束（O6–O11）"；§11 接口表按实物更正（6 例真采 + 132 例迁移、向 P-C 指明 `store/query.py` 已有检索实现）。同时修正头部版本号停在 v0.3 的笔误 |
| v0.5 | 2026-11-23 | 收口（V3 / V4 / V7 从"演示过"变成命令）：新增 `collect/gates.py` + `scripts/verify_gates.py` + `make verify-gates`。V3 的口径按实测改写成"不重不漏 + 内容多重集合一致"，**不比 `id` 序列与 `canonical_sha256`**（新记 O11：`id` 里的行号是按"读的先后"发的，崩点会让同一毫秒的平局换位）。§10 补齐 O8–O11 的闭环状态 |
| v0.4 | 2026-11-22 | 实采回填：§2.2/§2.3/§2.4 把 dn1 数据盘从 `128m` 改成 `2g`，改用 `dfs.blocksize=32m` + `dfs.replication=3`（块比盘大时 dn1 永远不接块，盘满无痕）；V6 从"ERROR>=1"改成"非 INFO>=1"（真实故障的日志表现几乎都是 WARN，ERROR 只在 DN 启动失败时出现），`audit_levels` 改为三条正向断言、不做反向断言；`expect` 门槛允许为 0 但和必须 >=1 |
| v0.3 | 2026-11-22 | 实现回填（第 9 / 11 / 12 步）：§6.1 docker adapter 改成"把容器当远端文件系统、只用 `tail -c` 一套 offset 语义"，**不实现 `docker logs --since`**，stdout 改由 §2 的 `tee` 落盘（O7 闭环）。V6 收紧为"只对 `origin=collected` 成立"（迁移的 132 例里 104 例根本没有 ERROR），并补一段说明。V2 改成"独立第二实现 + `tests/fixtures/verify/baseline.json` 冻结基准"，差异白名单取消（A-R4 改为重新冻结基准并在提交里露 diff）。§8.2 M1.4 补 `store/verify.py` 与基准文件；A-R1 标已落地 |
| v0.2 | 2026-11-22 | 对齐总计划 v1.0：引用改指 §4.1 / §5 / §7 / §8.2，路径补 `apps/backend/src/logcopilot/` 前缀，补 M0.x 前置表。CasePack 读写归 `store/casepack.py`（并入原 `collect/pack.py` 与 `collect/manifest.py`）。`LogRecord` 定死 10 字段：`id = {component}:{line_no:05d}`、`ts` 用 `+00:00`、新增 `logger`、`raw` 仅非 ok 出现。canonical 拆 `hdfs_log4j` / `lab` 两个 profile，按实测语法 A/B 与"续行也带前缀"重写判据。redact 要求对已有占位符幂等。`canonical_sha256` 只覆盖 canonical 投影（排除 `source`/`raw`），排序键改 `(ts, id)`，新增 `finalize` 阶段。`pipeline_version` / `redact_policy.version` / `canonical_profile` 明确不入任何哈希。V2 改用独立 oracle + 冻结 fixture，新增 V9（两种旧语法），命令改 `CASE=`/`CHECK=`。`window.phases` 与 `fault_injection` 可空并给出反推规则；O6 标记闭环 |
| v0.1 | 2026-11-22 | 首版。定下：只采 HDFS 生态（NN/DN/audit/GC/stdout + 指标）；docker compose 1NN+3DN、`TZ=UTC`、`dfs.replication=2`；采集端不过滤 level、FATAL 不伪造；三段窗口（baseline/fault/recovery）；S1 只采 3 例；指纹拆成 `input_fingerprint` + `canonical_sha256` |
