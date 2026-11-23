# P-A 真实采集运行手册（runbook）

这份文档回答一个问题：**“要把一个新的病例采进 `data/cases/`，从开机到归档要敲哪些命令、按什么顺序、每一步验什么。”**

它不是设计文档。设计（为什么要三段窗口、为什么要 `start_at_end`、为什么窗口是硬边界）在
[`P-A_COLLECT_PLAN.md`](../P-A_COLLECT_PLAN.md)；这里只写**能照着做的操作**，以及 2026-11-22 ~ 11-23
那 24 例真采到底是怎么跑出来的。

权威顺序：**代码 > 本文件 > 计划文档的叙述**。计划文档描述的是意图，代码和本文件描述的是实际。

---

## 0. 前提

| 前提 | 检查 |
|---|---|
| Docker 在跑，且 1NN+3DN 镜像在本地 | `docker images apache/hadoop:3.3.6` |
| 仓库依赖装好（`uv sync` 过） | `make install` |
| 当前工作目录是仓库根 | `pwd` 出来是 `aiops-log-copilot` |
| 磁盘留 100 MB 以上 | 一个 5 分钟病例大约 0.1–1 MB 日志 + 0.1–0.3 MB 指标 |

时间成本（实测）：起集群 ~40 秒；采一例 baseline 2 + fault 5 + recovery 2 = **9 分钟**采集 +
约 1 分钟标注与验收。批量补矩阵时按“一例 10 分钟”排。

---

## 1. 采前体检（四步，任何一步不对就别采）

```bash
# ① 四个容器都得在跑。少一个，采出来的 pack 会缺组件，事后无法补。
docker ps --format "{{.Names}}\t{{.Status}}"

# ② 安全模式必须是 OFF。安全模式下 NN 不接任何写，采到的“故障证据”全是安全模式噪声。
docker exec alcp-nn hdfs dfsadmin -safemode get        # 期望：Safe mode is OFF

# ③ 三台 DataNode 都要 Live。少了就是上一轮注入没收尾。
docker exec alcp-nn hdfs dfsadmin -report | head -6    # 期望：Live datanodes (3)

# ④ 无注入残留 + 副本齐全 + 网络成员齐。
make verify-gates ARGS="--v7 --live"
```

第 ④ 步的判据（`collect/gates.py::ClusterStatus.check`）：容器齐 / 安全模式 OFF / Live DN=3 /
无 `lab.fill` 残留（dn1 走 exec、dn2–dn3 查卷）/ 网络成员齐 / `Under replicated blocks = 0` /
`Missing blocks = 0`。

它红了先修现场再采 —— 一条残留的 `Missing blocks = 1` 会让**之后每一个** healthy 例的 NN 日志
都持续冒 `Failed to place enough replicas`，于是故障证据变成噪声。修法：

```bash
make verify-gates ARGS="--v7 --live --repair"     # 内部跑 `fsck / -delete`
```

---

## 2. 一次真采的完整流程（8 步）

### 步骤 1 · 起集群

```bash
cd infra/hdfs
docker compose up -d
docker compose ps          # 四个都 Up 之后等 30–60 秒让 DN 完成块汇报
cd ../..
```

冷启动的 NN 主日志里有 22 KB 的 `STARTUP_MSG` classpath，**采集器会跳过它**（`start_at_end` 把每条流挪到 EOF），
所以“刚起来就采”不会把启动噪声算进窗口。但 DN 的块汇报发生在启动后几十秒内，等一会儿更稳。

### 步骤 2 · 体检

见 §1。**不要跳过。**

### 步骤 3 · 采集

正常样本（不注入故障）：

```bash
make collect CASE=base_healthy_04 ARGS="--minutes 2"
```

故障样本（注入 + 三段窗口）：

```bash
make collect CASE=inc_dnvol_06 \
  ARGS="--inject dn_volume --inject-target alcp-dn1 --root-cause DN_VOLUME \
        --minutes 2 --fault-minutes 5 --recovery-minutes 2"
```

参数选择规则（24 例实测出来的经验值）：

| 参数 | 取值 | 为什么 |
|---|---|---|
| `--minutes`（baseline 长度） | 正常例 2–5；故障例 **2** | baseline 越长，`canonical_sha256` 越容易被噪声带偏；2 分钟已够 10–20 次块汇报周期 |
| `--fault-minutes` | 3–10（默认 5） | 心跳类要 ≥3 分钟：dead 判定实测 ~30 秒，太短的话故障段里看不到 dead |
| `--recovery-minutes` | 1–2 | 覆盖一次 unpause/重建卷；DN 重启后重新注册块约 20–40 秒 |
| `--inject-target` | 见 §3 | 不写就用变体的默认目标；**写了会进 manifest**（见 §5） |
| `--config-override KEY=VALUE` | 现场与默认不同才写 | 例如 DEBUG 对照例 `--config-override log_level=DEBUG` |

`--dry-run` 只打印注入剧本、不碰集群，但**管线照样跑**（仍然会写出一个 pack），适合先确认参数。

### 步骤 4 · 验包（不验就等于没采）

```bash
make pack-lint CASE=inc_dnvol_06 CHECK=levels
```

两个检查：结构（字段、ts 单调、id 唯一、counters 对账、`canonical_sha256` 重算一致）+
级别门槛（`min_error_lines` / `min_warn_lines` / `error_components`）。**故障例在这一步之前先别急着标注** ——
`pack-lint` 不带 `CHECK=levels` 也能跑结构检查：

```bash
make pack-lint CASE=inc_dnvol_06
```

### 步骤 5 · 标注（人的那半）

先看实测数字，再定门槛：

```bash
make annotate CASE=inc_dnvol_06          # 只打印现状和实测值
```

写进去：

```bash
make annotate CASE=inc_dnvol_06 ARGS="--root-cause DN_VOLUME --severity error \
     --reviewer lead --notes '写清这一例注入的是什么、观察到了什么'"
```

批量补矩阵时用 `runtime/annotate_auto.py`（不进库）的口径：组件取所有出现非 INFO 的组件、
`min_error = 1 if 有 ERROR else 0`、`min_warn = max(5, 实测 WARN × 30%)`、和为 0 时兜底 1。

**`fault_injection` 不用人填** —— 采集器在故障发生时自己写（`script` / `started` / `ended` /
`restored` / `restored_ok`）。

### 步骤 6 · 复验

```bash
make annotate-check CASE=inc_dnvol_06    # = annotate + pack-lint CHECK=levels
```

门槛写错在这一步被抓（例如把 `min_warn_lines` 填得比实测还高）。

### 步骤 7 · 收尾复位

```bash
# 安全模式：注入脚本会自己 leave，但失败时要人工兜底
docker exec alcp-nn hdfs dfsadmin -safemode leave

# 残留 / 副本 / 网络
make verify-gates ARGS="--v7 --live --repair"
```

DEBUG 对照例额外一步：`Remove-Item Env:\HADOOP_ROOT_LOGGER` 然后 `docker compose down && up -d`，
否则后面所有例子的 level 分布都没法横向比。

### 步骤 8 · 记账

采集器已经把这些写进 `manifest.json`，不需要手工记：

- `window.since/until` + `window.phases`（三段时刻）
- `config_overrides`（`inject_variant` / `inject_target` / `log_level` 等）
- `counters`（行数、level/component 分布、解析失败、脱敏计数）
- `hashes.input_fingerprint` / `canonical_sha256`
- `checkpoint`（每流的 `offset` / `line_no` / `tail_line_sha1`）

需要**人记**的只有 `labels.json` 的 `root_cause` / `severity` / `expect` / `reviewer` / `notes`。

---

## 3. 六个注入变体

| 变体（`--inject`） | 默认目标 | 触发方式 | 恢复方式 | 期望证据 | 前置条件 |
|---|---|---|---|---|---|
| `dn_volume_fill` | `alcp-dn1` | `dd 4096MB` 到 2 GB tmpfs，必然 ENOSPC | 删 `lab.fill` | NN 持续 `Failed to place enough replicas`（WARN）；dn1 日志几乎不涨 | **只能打 dn1**（dn2/dn3 是 1.08 TB 命名卷，填不满） |
| `dn_disk_broken` | `alcp-dn2` | 停容器 → 把卷里 `current/` 换成普通文件 → 启容器 | 停 → 删文件 → 启（DN 重新格式化卷） | DN 启动即 **ERROR**：`Initialization failed for Block pool` / `NotDirectoryException` | 只能打 dn2/dn3（dn1 无卷）；helper 容器要 `--user 0:0` |
| `dn_heartbeat_pause` | `alcp-dn1` | `docker pause`（SIGSTOP） | `docker unpause` | `dead_datanodes=1`；其它 DN 的 `WRITE_BLOCK` ERROR + 客户端 `Exception in createBlockOutputStream` | 无 |
| `dn_heartbeat_stop` | `alcp-dn2` | `docker stop` | `docker start` | 同 pause，另加该 DN 自己的 shutdown WARN | 无 |
| `nn_safemode` | `alcp-nn` | `dfsadmin -safemode enter` | `-safemode leave` | **只有 WARN**：客户端 `FsShell` 的 `put: ... Name node is in safe mode.`（需要 O12 那条解析规则） | 无；但服务端 ERROR 在结构上不出现（O10） |
| `network` | `alcp-dn3` | `docker network disconnect aiops-log-copilot_hdfs <c>` | `docker network connect` | 其它 DN 的 `DataXceiver error` ERROR + 客户端 `Excluding datanode`（WARN） | 不用 iptables（避免 NET_ADMIN） |

`dn_disk_broken` **没有 bash 版**：它要“停容器 → 换卷内容 → 启容器”三步事务，用 Python 编排更稳
（`collect/inject.py::dn_disk_broken`）。其余五个在 `infra/hdfs/inject/` 都有可 `DRY_RUN=1` 空跑的 bash 脚本。

---

## 4. 已采的 24 例（2026-11-22 ~ 11-23 真采）

单位：分钟。`-` = 该例没有 phases（只采一段）。数字取自各 pack 的 `manifest.counters`。

| case | base | fault | rec | 记录 | ERROR | WARN | root_cause | 注入变体 |
|---|---:|---:|---:|---:|---:|---:|---|---|
| `base_healthy_01` | 5 | - | - | 1590 | 0 | 32 | - | - |
| `base_healthy_02` | 2 | - | - | 641 | 0 | 16 | - | - |
| `base_healthy_03` | 2 | - | - | 1669 | 0 | 35 | - | - |
| `base_healthy_debug_01` | 2 | - | - | 10638 | 0 | 7 | - | -（`log_level=DEBUG`） |
| `base_noise_01` | 4 | - | - | 147 | 0 | 4 | - | -（窗口内重启 dn3） |
| `base_noise_02` | 4 | - | - | 1127 | 0 | 31 | - | -（窗口内负载尖峰） |
| `inc_dnvol_01` | 2 | 5 | 2 | 3200 | 0 | 259 | `DN_VOLUME` | `dn_volume_fill` |
| `inc_dnvol_02` | 2 | 4 | 2 | 335 | 1 | 10 | `DN_VOLUME` | `dn_disk_broken` |
| `inc_dnvol_03` | 2 | 5 | 2 | 2998 | 7 | 112 | `DN_VOLUME` | `dn_disk_broken` |
| `inc_dnvol_04` | 2 | 8 | 2 | 4430 | 0 | 377 | `DN_VOLUME` | `dn_volume_fill` |
| `inc_dnvol_05` | 2 | 10 | 2 | 5306 | 0 | 478 | `DN_VOLUME` | `dn_volume_fill` |
| `inc_hb_01` | 2 | 3 | 1 | 1627 | 3 | 46 | `DN_HEARTBEAT` | `dn_heartbeat_pause` |
| `inc_hb_02` | 2 | 5 | 2 | 2668 | 2 | 83 | `DN_HEARTBEAT` | `dn_heartbeat_stop` |
| `inc_hb_03` | 2 | 5 | 2 | 2459 | 4 | 77 | `DN_HEARTBEAT` | `dn_heartbeat_pause` |
| `inc_hb_04` | 2 | 5 | 2 | 2710 | 2 | 81 | `DN_HEARTBEAT` | `dn_heartbeat_stop` |
| `inc_hb_05` | 2 | 8 | 2 | 3715 | 3 | 121 | `DN_HEARTBEAT` | `dn_heartbeat_stop` |
| `inc_hb_06` | 2 | 5 | 2 | 2532 | 4 | 83 | `DN_HEARTBEAT` | `dn_heartbeat_pause` |
| `inc_net_01` | 2 | 5 | 2 | 2480 | 3 | 72 | `NETWORK_CONNECTIVITY` | `network` |
| `inc_net_02` | 2 | 3 | 1 | 1586 | 3 | 65 | `NETWORK_CONNECTIVITY` | `network` |
| `inc_net_03` | 2 | 5 | 2 | 2663 | 2 | 87 | `NETWORK_CONNECTIVITY` | `network` |
| `inc_net_04` | 2 | 5 | 2 | 2494 | 2 | 79 | `NETWORK_CONNECTIVITY` | `network` |
| `inc_net_05` | 2 | 8 | 2 | 3675 | 3 | 140 | `NETWORK_CONNECTIVITY` | `network` |
| `inc_net_06` | 2 | 5 | 2 | 1376 | 0 | 56 | `NETWORK_CONNECTIVITY` | `network`（打 nn） |
| `inc_safemode_05` | 2 | 5 | 2 | 1912 | 0 | 148 | `NN_SAFE_MODE` | `nn_safemode` |

复现这张表：

```bash
uv run --project apps/backend python runtime/collect_inventory.py     # runtime/ 不进库
```

四例安全模式（`inc_safemode_01`–`04`）采完判定不合格，已移到 `runtime/no-evidence/`，不在上表里。

---

## 5. 留痕约定：什么进包、什么不进

| 信息 | 落在哪 | 说明 |
|---|---|---|
| 采集窗口与三段时刻 | `manifest.window` | 含 `phases`；迁移例可能只有 fault 段 |
| **注入变体 + 目标容器** | `manifest.config_overrides.inject_variant` / `.inject_target` | 2026-11-23 起自动写；此前的 24 例没有（见 §7） |
| 现场参数（如日志级别） | `manifest.config_overrides.log_level` | 事后解释数据差异的唯一依据 |
| 每个组件来自哪 | `manifest.source[]` | `adapter` / `ref` / `component` / `path` |
| 行数与分布 | `manifest.counters` | 与盘上内容对账过 |
| 两个指纹 | `manifest.hashes` | `input_fingerprint` 只随源变；`canonical_sha256` 随脱敏与归一化变 |
| 续采位置 | `manifest.checkpoint` | 每条流的 `offset` / `line_no` / `tail_line_sha1` |
| 故障起止与是否复原 | `labels.fault_injection` | 采集器自动写，**不记目标容器** |
| 金标准与门槛 | `labels.root_cause` / `severity` / `expect` / `reviewer` / `notes` | 人写 |
| 证据行原文在哪 | `anchors/index.json` | 只给非 INFO / 非 ok 的行记指针（`id` + 文件 + offset + length + sha256） |
| 指标曲线 | `metrics.jsonl` | 不进任何哈希 |

**故意不记的**：`input_fingerprint` 里的 `pipeline_version`、`redact_policy.version`、`canonical_profile`
（改代码不该让指纹变脏）；`config_overrides` 不进任何哈希（它是“解释”，不是“内容”）。

---

## 6. 收尾与复位清单

采完/中断之后，按顺序做：

1. `docker exec alcp-nn hdfs dfsadmin -safemode get` → 必须是 `OFF`
2. `docker exec alcp-dn1 rm -f /tmp/hadoop-root/dfs/data/lab.fill`（或 `make verify-gates --v7 --live` 会报残留）
3. `make verify-gates ARGS="--v7 --live --repair"` → `PASS 现场已复原`
4. DEBUG 跑过的话：`Remove-Item Env:\HADOOP_ROOT_LOGGER` + `docker compose down && up -d`
5. 中断的 pack 目录（有 `checkpoint.json` 无 `manifest.json`）默认**删掉重采**。
   续采能力在库里（`Collector.collect(mode=CollectMode.ATTACH)`，会读 `checkpoint.json`
   从断点接着采，语义由 `tests/collect/test_collect.py` 的 attach 用例钉住），
   但 `scripts/collect_case.py` **还没有暴露 `--attach`** 开关 —— 见 §7 第 4 条。
   别把半成品的 pack 留在 `data/cases/` 里：`pack-lint` 会把它报成“未完成”。

---

## 7. 已知留痕缺口（如实记录）

1. **24 例的注入目标没有进包**。`labels.fault_injection.script` 只记变体（`inject/network_disconnect`），
   不记打在哪台 DN 上；`config_overrides` 这项当时还没有。所以“`inc_net_03` 断的是 dn1 还是 dn3”
   只能从日志证据推断（哪个组件出现 `DataXceiver error`），无法从包里直接回答。
   **已修**：`scripts/collect_case.py` 现在把 `inject_variant` / `inject_target` 自动写进
   `manifest.config_overrides`（冲突时报错），下一批采集自带留痕。
2. **24 例的原始命令行没有被记录**。本文件 §4 的表格是从 pack 里反推的（`window.phases` 三段时长、
   `counters`、`labels.root_cause`、`labels.fault_injection.script`），参数与当时的实际命令行**一致但不保证逐字相同**
   （例如 `--poll-interval` 用过默认值也用过 1.0）。
3. **`data/cases/` 不进 git**（35.8 MB）。所以这 24 例不在仓库里，换台机器要重采或另行归档。
   `make verify-collect` 的 132/132 只覆盖迁移例，它不依赖 `data/cases` 的 collected 部分。
4. **续采没有 CLI 开关**。`CollectMode.ATTACH` 在库里实现且有测试（读 `checkpoint.json`、
   先验 `tail_line_sha1` 再从 `offset+1` 读、拒绝 `resume_from`），但 `scripts/collect_case.py`
   只跑 `BASELINE` / `INJECT` 两种模式 —— 中断一例只能删掉重采。要补的话是在 CLI 里加
   `--attach`，把 `mode=CollectMode.ATTACH` 传下去，其余参数不变。

---

## 8. P-A 核心脚本清单

**入口脚本（`scripts/`，全部可 `--help`）**

| 文件 | 行数 | 职责 | 典型入口 |
|---|---:|---|---|
| `scripts/collect_case.py` | 261 | 采一个病例：拼来源 → 建窗口 → 起指标 → 可选注入 → 跟随采集 → 打印报告 | `make collect CASE=… ARGS=…` |
| `scripts/pack_lint.py` | 109 | 校验一个/多个 pack（结构 + 级别门槛） | `make pack-lint CASE=… CHECK=levels` |
| `scripts/annotate_case.py` | 111 | 读实测值 → 写 `labels.json`（只动人填的字段） | `make annotate CASE=…` |
| `scripts/migrate_cases.py` | 139 | 旧项目 132 例 → CasePack v1 | `python scripts/migrate_cases.py` |
| `scripts/verify_collect.py` | 118 | V2：迁移例逐例与冻结基准对账 | `make verify-collect` |
| `scripts/verify_gates.py` | 141 | V3/V4/V7 闸门（续采一致 / 双路径逐字节 / 现场复原） | `make verify-gates ARGS="--v7 --live"` |
| `scripts/ask.py` | 101 | 用自然语言（关键词/级别/组件/时间）查一个 pack + 回读 anchors 原文 | `make ask-case CASE=… ARGS="-k Excluding"` |
| `scripts/snapshot_metrics.py` | 65 | 一次性指标快照（排障用，不进 pack） | `python scripts/snapshot_metrics.py` |

**环境脚本**

| 文件 | 行数 | 职责 |
|---|---:|---|
| `infra/hdfs/compose.yaml` | 163 | 1NN+3DN，`TZ=UTC`，日志 bind mount 到 `runtime/hdfs-logs/<c>/`，stdout 走 `tee` |
| `infra/hdfs/conf/log4j.properties` | 44 | 服务端日志格式（`%d{ISO8601} %-5p %c{1}:%L - %m%n`）+ 审计独立 appender |
| `infra/hdfs/conf/client-log4j.properties` | 15 | 客户端（`hdfs dfs`）日志格式，落 `nn/workload.out` |
| `infra/hdfs/conf/hdfs-site.xml` | 28 | `replication=3` / `blocksize=32m` / `recheck-interval=2000` / `du.reserved=0` |
| `infra/hdfs/bin/start-namenode.sh` | 16 | 首次 format，再 `exec hdfs namenode` |
| `infra/hdfs/bin/start-datanode.sh` | 18 | 等 NN 就绪（`safemode get` 探活）再 `exec hdfs datanode` |
| `infra/hdfs/bin/workload.sh` | 60 | 真读写负载（`put`/`cat`/`rm`），协作式停机 |
| `infra/hdfs/inject/_common.sh` | 26 | 注入脚本公共部分（`DRY_RUN` / 容器名 / `run()`） |
| `infra/hdfs/inject/*.sh`（5 对） | 4–6 | 五个变体的人工版：volume / heartbeat pause / heartbeat stop / safemode / network |

**库代码**（`apps/backend/src/logcopilot/`，行数见 `P-A_COLLECT_PLAN.md` §8.3）：
`domain/log.py`、`domain/case.py`、`ports.py`、`collect/adapters/{base,file,docker,wsl}.py`、
`collect/{canonical,redact,collect,metrics,inject,migrate,labels,gates}.py`、`store/{casepack,casestore,query,verify}.py`。

**Makefile 目标**：`install / lint / fmt / type / test / check / check-live / collect / annotate /
annotate-check / pack-lint / verify-collect / verify-gates / ask-case / help`。
`make check` 是离线闸门（lint + test + pack-lint + verify-collect），`make check-live` 再加
`verify-gates --v4` 与 `--v7 --live`（**要求集群在跑**）。
