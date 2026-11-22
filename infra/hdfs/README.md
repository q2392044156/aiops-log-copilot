# HDFS 采集环境（1 NameNode + 3 DataNode）

这是 P-A 里"现场采集"那一路的环境，S1 阶段采的 3+1 个案例都从这里来。
和旧实验台（`aiops-fault-diagnosis-agent/ops/lab`）是同一套镜像与拓扑，但有四处关键改动，
目的只有一个：**让日志能被采集器读到、且读到的字节与 `docker exec cat` 完全一致**。

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
| `gc.log` | JVM | ❌ **暂不采** | Java 8 GC 格式没有对应解析档位，收进来会被当成上一条日志的续行（O8） |
| `hdfs-security.log` | 安全日志 | ❌ | 权限关闭时为空 |

## 关键参数（都在 `conf/hdfs-site.xml`）

| 参数 | 值 | 为什么 |
|---|---|---|
| `dfs.replication` | 2 | 只有 3 台 DN，挂 1 台就进 Under-replicated |
| `dfs.datanode.du.reserved` | 0 | 数据盘写满立刻报 `DiskOutOfSpaceException`，不用等预留空间 |
| `dfs.heartbeat.interval` | 2s | 心跳快，故障判定快 |
| `dfs.namenode.heartbeat.recheck-interval` | 2000ms | 判 dead ≈ **24 秒**（默认值是 630 秒，10 分钟的故障窗里等不到） |

这两个心跳参数必须原样写进 `manifest.config_overrides`，否则窗口与日志对不上。

## 为什么 dn1 用 tmpfs

`dn1` 的数据盘是 `tmpfs size=128m`，`dn2`/`dn3` 是普通卷。于是：

- 造 `DN_VOLUME` 只需要往 dn1 的数据盘里 `dd` 一个文件，写满立刻触发 volume failure；
- dn1 重启后数据盘清空，故障可重复，不留后遗症；
- dn2/dn3 正常，保证 `replication=2` 的副本能正常落下来。

## 故障注入

`inject/` 下每个脚本都是一次可逆的注入，全部支持空跑：

```bash
DRY_RUN=1 bash inject/dn_volume_fill.sh      # 只打印要做什么
bash inject/dn_volume_fill.sh                # 真写满 dn1 的 128MB 数据盘
bash inject/dn_volume_restore.sh             # 删掉填盘文件并确认 df 能读
```

| 根因 | 脚本 | 做了什么 |
|---|---|---|
| `DN_VOLUME` | `dn_volume_fill.sh` / `dn_volume_restore.sh` | 往 dn1 的 tmpfs 里 `dd` 200MB，必然 ENOSPC |
| `DN_HEARTBEAT` | `dn_heartbeat_pause.sh` / `dn_heartbeat_resume.sh` | `docker pause`（SIGSTOP）：进程在、心跳停 |
| `DN_HEARTBEAT` | `dn_heartbeat_stop.sh` / `dn_heartbeat_start.sh` | `docker stop`：进程没了，重启重新注册块 |
| `NN_SAFE_MODE` | `nn_safemode_enter.sh` / `nn_safemode_leave.sh` | `dfsadmin -safemode enter/leave` |
| `NETWORK_CONNECTIVITY` | `net_disconnect.sh` / `net_connect.sh` | `docker network disconnect/connect`（不需要 NET_ADMIN） |

**采集链路不调 bash**：`collect/inject.py` 自己拼同样的 docker 命令（`InjectionPlan`）。
这些脚本是给人在终端里复现用的，两边命令必须一致，改了这边记得改那边。

实测延迟（`recheck-interval=2000` + `heartbeat.interval=2`）：

- commit 之后 pause 一台 DN，**约 30 秒**后 `dfsadmin -report` 才把它算成 dead；
- unpause 之后 **8 秒内**回到 `Live datanodes (3)`；
- `replication=2` 时**单台 DN 挂掉不会产生 under-replicated 块**（另一份副本还在），
  所以 `DN_HEARTBEAT` 的指标证据是 `dead_datanodes=1`，不是副本不足。

## 已知缺口

- **`gc.log` 没接进采集链路**（O8）：要接得先加第三个解析档位 `jvm_gc`。
- **指标还没接进采集循环**：`collect/metrics.py` 与 `scripts/snapshot_metrics.py` 已可用，
  但 `collect.py` 还没有按 5s/30s 的节奏调用 `MetricsSession` —— 下一步（第 15 步）接。
- **`df` 的读数只有 dn1 是"这块盘"**：dn2/dn3 的数据目录在 Docker 命名卷上，
  `df` 报的是宿主机磁盘（1.08TB），所以容量类判据要看 dn1。

