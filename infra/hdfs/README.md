# HDFS 采集环境（1 NameNode + 3 DataNode）

这是 P-A 里"现场采集"那一路的环境，S1 阶段采的 3+1 个案例都从这里来。
和旧实验台（`aiops-fault-diagnosis-agent/ops/lab`）是同一套镜像与拓扑，但有四处关键改动，
目的只有一个：**让日志能被采集器读到、且读到的字节与 `docker exec cat` 完全一致**。

> 要照着一例一例采数据，看 [`docs/P-A_COLLECT_RUNBOOK.md`](../../docs/P-A_COLLECT_RUNBOOK.md)：
> 采前体检、8 步流程、六个注入变体、24 例实测清单、留痕约定都在那里。
> 本文件只讲"这个环境为什么长这样"。

## 起停

```bash
cd infra/hdfs
docker compose up -d          # 第一次会自动格式化 NameNode
docker compose ps
docker exec alcp-nn hdfs dfsadmin -report   # 应看到 Live datanodes (3)
docker compose down           # 只停容器，保留数据卷
docker compose down -v        # 连数据卷一起删（NN 的元数据也没了，下次重新格式化）
```

端口：`9870`（NN Web UI）、`9000`（HDFS RPC）。

## 日志落在哪

容器内 `/opt/hadoop/logs` 就是宿主机的 `runtime/hdfs-logs/<组件>/`（bind mount），
`runtime/` 已在 `.gitignore` 里 —— 日志不进版本库。

| 文件 | 谁写 | 采不采 | 说明 |
|---|---|---|---|
| `hadoop-hdfs-namenode-nn.log` | NameNode | ✅ main | 主日志 |
| `hadoop-hdfs-datanode-dn{1,2,3}.log` | DataNode | ✅ main | 主日志 |
| `hdfs-audit.log` | NameNode | ✅ audit | 只有 NN 有；`additivity=false` 保证不重复进主日志 |
| `nn.out` / `dn{1,2,3}.out` | 启动脚本 | ✅ stdout | 容器 stdout，通常只有一两行 |
| `nn/workload.out` | `hdfs dfs` 客户端 | ✅ workload（nn 的第 4 条流） | 客户端日志：断网时打 `Excluding datanode`，副本放不下时打 `Slow waitForAckedSeqno` —— 服务端不说"谁写失败了" |
| `gc.log` | JVM | ❌ **暂不采** | Java 8 GC 格式没有对应解析档位，收进来会被当成上一条日志的续行（O8） |
| `hdfs-security.log` | 安全日志 | ❌ | 权限关闭时为空 |

## 关键参数（都在 `conf/hdfs-site.xml`）

| 参数 | 值 | 为什么 |
|---|---|---|
| `dfs.replication` | 3 | 挂 1 台立刻 2 < 3 → Under-replicated（见下方"为什么改 3 + 32MB 块"） |
| `dfs.blocksize` | 33554432（32MB） | 块必须小于 dn1 的盘，否则 NN 永不选它 |
| `dfs.datanode.du.reserved` | 0 | 数据盘写满立刻报 `DiskOutOfSpaceException`，不用等预留空间 |
| `dfs.heartbeat.interval` | 2s | 心跳快，故障判定快 |
| `dfs.namenode.heartbeat.recheck-interval` | 2000ms | 判 dead ≈ **24 秒**（默认值是 630 秒，10 分钟的故障窗里等不到） |

这两个心跳参数必须原样写进 `manifest.config_overrides`，否则窗口与日志对不上。

## 为什么 dn1 用 tmpfs

`dn1` 的数据盘是 `tmpfs size=2g`，`dn2`/`dn3` 是普通卷。于是：

- 造 `DN_VOLUME` 只需要往 dn1 的数据盘里 `dd` 一个文件，写满立刻触发 volume failure；
- dn1 重启后数据盘清空，故障可重复，不留后遗症；
- dn2/dn3 正常，保证 `replication=3` 的副本能正常落下来。

## 为什么改 3 副本 + 32MB 块 + 2g 盘（2026-11-22 实采回填）

第一版是 `replication=2`、默认 128MB 块、dn1 盘 128m。实采 20 分钟之后发现两件事：

1. **dn1 一块都没接到**：NameNode 选副本的第一道门槛是 `available >= blocksize`，
   128MB 的盘装不下 128MB 的块，于是所有块都去了 dn2/dn3（dn1 的
   `DirectoryScanner: Total blocks: 0`）。把它的盘写满对集群**毫无影响** ——
   采集窗口里 dn1 一行日志都没写。
2. **512m 也不够**：workload 每分钟写 40–70MB，20 分钟光正常负载就能把 512m
   写满，"故障段"其实从 baseline 就开始了，无法证明盘是注入写满的。

现在：`blocksize=32m` + `replication=3` → 每个块三台都落，dn1 必然接块；
`tmpfs size=2g` → 正常负载 20 分钟占 40–60%，`dd 4096MB` 才撞 ENOSPC。

## 故障注入

`inject/` 下每个脚本都是一次可逆的注入，全部支持空跑：

```bash
DRY_RUN=1 bash inject/dn_volume_fill.sh      # 只打印要做什么
bash inject/dn_volume_fill.sh                # 真写满 dn1 的 2GB 数据盘（FILL_MB=4096）
bash inject/dn_volume_restore.sh             # 删掉填盘文件并确认 df 能读
```

| 根因 | 脚本 | 做了什么 |
|---|---|---|
| `DN_VOLUME` | `dn_volume_fill.sh` / `dn_volume_restore.sh` | 往 dn1 的 tmpfs 里 `dd` 4096MB（> 盘容量），必然 ENOSPC |
| `DN_HEARTBEAT` | `dn_heartbeat_pause.sh` / `dn_heartbeat_resume.sh` | `docker pause`（SIGSTOP）：进程在、心跳停 |
| `DN_HEARTBEAT` | `dn_heartbeat_stop.sh` / `dn_heartbeat_start.sh` | `docker stop`：进程没了，重启重新注册块 |
| `NN_SAFE_MODE` | `nn_safemode_enter.sh` / `nn_safemode_leave.sh` | `dfsadmin -safemode enter/leave` |
| `NETWORK_CONNECTIVITY` | `net_disconnect.sh` / `net_connect.sh` | `docker network disconnect/connect`（不需要 NET_ADMIN） |
| `DN_VOLUME`（重症） | 走 Python 编排，无 bash 版 | `dn_disk_broken`：把 dn2 卷里的 `current/` 换成普通文件 → DN 启动即失败 → **服务端 ERROR** |

**采集链路不调 bash**：`collect/inject.py` 自己拼同样的 docker 命令（`InjectionPlan`）。
这些脚本是给人在终端里复现用的，两边命令必须一致，改了这边记得改那边。

实测延迟（`recheck-interval=2000` + `heartbeat.interval=2`）：

- commit 之后 pause 一台 DN，**约 30 秒**后 `dfsadmin -report` 才把它算成 dead；
- unpause 之后 **8 秒内**回到 `Live datanodes (3)`；
- `replication=2` 时**单台 DN 挂掉不会产生 under-replicated 块**（另一份副本还在），
  所以 `DN_HEARTBEAT` 的指标证据是 `dead_datanodes=1`，不是副本不足。

## 读写负载（`bin/workload.sh`）

空载集群几乎不写日志 —— 没有负载的"正常样本"会是一段死水，P-B 挖不出模板，
P-C 也没有可检索的上下文。所以采健康样本前必须先起负载：

```bash
docker exec -d alcp-nn /lab/bin/workload.sh     # 后台：每秒 put/cat/rm 一个小文件
docker exec alcp-nn pkill -f /lab/bin/workload.sh
```

`scripts/collect_case.py` 会自己起停它（`--no-workload` 可关掉）。

停机是**协作式**的：`touch /opt/hadoop/logs/.workload-stop`，循环在两次迭代之间
自己退出，当前那次 `put` 会写完。别用 `pkill` —— 掐断写管线会让 DN 在故障窗口
之外凭空打 3 条 `DataXceiver error processing WRITE_BLOCK` 的 ERROR，V6 的门槛
就被噪声顶起来了（实测记录见 `collect_case.py` 的 `_workload`）。
负载是**真读写**：`put` 触发 DN 的 `BlockReceiver` 日志、`cat` 触发读、
`rm` 触发删除，三条路径都会在日志里留下可被采集到的痕迹 —— 不是往 `.log` 里
`echo` 假行（那是自己伪造语料）。

## 采一个病例（`scripts/collect_case.py`）

三步的完整版（含体检、验收、标注、复位）在 [`docs/P-A_COLLECT_RUNBOOK.md`](../../docs/P-A_COLLECT_RUNBOOK.md)；
这里只留最短的三条示例：

```bash
# 正常样本：从"现在"起跟随 5 分钟，指标 5s/30s 一起采
python scripts/collect_case.py --case base_healthy_01 --minutes 5

# 故障样本：基线 5 分钟 → 注入 10 分钟 → 恢复 5 分钟
python scripts/collect_case.py --case inc_dnvol_01 --inject dn_volume \
    --root-cause DN_VOLUME --minutes 5

# 只打印注入剧本、不碰集群（管线照样跑）
python scripts/collect_case.py --case inc_dnvol_01 --inject dn_volume --dry-run
```

三个开关对应三件必须做的事：`follow_until`（窗口内每 2 秒 drain，不是读一轮就收工）、
`MetricsSession`（`docker stats` 5s / `dfsadmin -report`+`df` 30s 写进 `metrics.jsonl`）、
`start_at_end`（起采前把每条流挪到 EOF，否则启动时 22KB 的 STARTUP_MSG classpath
会被算进这 5 分钟）。

## DEBUG 对照例（O4：日志量涨一个数量级时采集器不炸）

root level 从 `log4j.properties` 挪到了环境变量，所以换现场不用改仓库文件：

```powershell
$env:HADOOP_ROOT_LOGGER='DEBUG,RFA'          # 默认是 INFO,RFA
cd infra/hdfs; docker compose down; docker compose up -d
cd ../..; make collect CASE=base_healthy_debug_01 ARGS="--minutes 2 --config-override log_level=DEBUG"
# 采完一定切回来，否则后面所有例子的 level 分布都没法横向比
Remove-Item Env:\HADOOP_ROOT_LOGGER
cd infra/hdfs; docker compose down; docker compose up -d
```

`--config-override log_level=DEBUG` 会写进 `manifest.config_overrides` —— 它是**事后解释数据差异的唯一依据**，
所以它刻意不进任何哈希（改日志级别不该让指纹变脏）。

2026-11-23 实测（`base_healthy_debug_01`，2 分钟）：**10638 条**（DEBUG 9949 / INFO 682 / WARN 7）、
原始 15764 行、硬失败 **0** 条、`partial` 1162 条（`UserGroupInformation` 后面的 3–4 行 Java 栈被正确并进上一条）、
`pack-lint` PASS。同一份代码下 INFO 档是 318 条/分钟，DEBUG 档 **5319 条/分钟 —— 16.7 倍**。

两个必须知道的副作用：

- 审计 / 安全日志是**独立 logger**，仍是 INFO —— 对照例要证明的是"主日志涨多少"，审计流的行数不该跟着动。
- 主日志是**追加写**的：DEBUG 跑完后那 8 MB 已混进同一个 `hadoop-hdfs-namenode-nn.log`。
  想要一份干净语料就 `docker compose down -v` 重来（会清数据卷）。

## 已知缺口

- **`gc.log` 没接进采集链路**（O8）：要接得先加第三个解析档位 `jvm_gc`。
- **`df` 的读数只有 dn1 是"这块盘"**：dn2/dn3 的数据目录在 Docker 命名卷上，
  `df` 报的是宿主机磁盘（1.08TB），所以容量类判据要看 dn1。
- **副本不足类判据用不上**：`replication=3` 下把单台 DN 从网络里摘掉，只剩 2 份副本、
  仍满足最小复制数，不会产生 under-replicated 块，所以 `DN_HEARTBEAT` 只靠
  `dead_datanodes` 与日志本身。
- **故意删块做实验之后，记得 `fsck -delete`**：2026-11-23 删过 `blk_1073744860` 的三份副本
  去做"块缺失型安全模式"，收尾没清干净，于是每次 `verify-gates --v7 --live` 都报
  `Missing blocks = 1`，而每个 healthy 例的 NN 日志里也持续出现
  `Failed to place enough replicas`。修法是 `make verify-gates ARGS="--v7 --live --repair"`。
  这条闸门就是为这种"实验留伤"设计的 —— 它红了，先修现场再采数据。

