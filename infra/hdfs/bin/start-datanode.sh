#!/usr/bin/env bash
# DataNode 启动脚本（在 alcp-dn1/dn2/dn3 容器内执行）。
#
# 和旧实验台的区别：旧脚本 sleep 5 硬等，这里改成探活 NameNode 的 RPC 端口——
# 集群重启时 DN 常常比 NN 先起来，硬等 5 秒不够，DN 会因为连不上 NN 直接退出。
set -euo pipefail

export HADOOP_HOME=/opt/hadoop
export HADOOP_LOG_DIR=/opt/hadoop/logs
mkdir -p "$HADOOP_LOG_DIR" /tmp/hadoop-root/dfs/data

# dfsadmin 能回答就说明 NN 的 RPC 已就绪；最多等 120 秒
for _ in $(seq 1 60); do
  if hdfs dfsadmin -safemode get >/dev/null 2>&1; then
    echo "[start-datanode] NameNode 就绪"
    break
  fi
  sleep 2
done

exec hdfs datanode
