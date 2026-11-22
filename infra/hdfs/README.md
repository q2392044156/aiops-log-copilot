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

## 已知缺口

- **`gc.log` 没接进采集链路**（O8）：要接得先加第三个解析档位 `jvm_gc`。
- **`metrics.jsonl` 还是空文件**：`docker stats`（5s）与 `dfsadmin -report` + `df -h`（30s）尚未实现。
- **注入脚本**：`inject/` 目前只有占位，第 14 步补齐 4 族（卷满 / 心跳 / 安全模式 / 网络）。
