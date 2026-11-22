#!/usr/bin/env bash
# NameNode 启动脚本（在 alcp-nn 容器内执行，由 compose 的 command 调起）。
#
# 只做两件事：把日志目录钉在 bind mount 上、首次启动时格式化。其余交给 hdfs 自己的启动器。
set -euo pipefail

export HADOOP_HOME=/opt/hadoop
# 必须和 compose 的 HADOOP_LOG_DIR、bind mount 目标一致，否则日志又跑回 /var/log/hadoop
export HADOOP_LOG_DIR=/opt/hadoop/logs
mkdir -p "$HADOOP_LOG_DIR" /tmp/hadoop-root/dfs/name

# VERSION 文件是"这个目录被格式化过"的唯一标志；用 -force -nonInteractive 免掉交互确认
if [ ! -f /tmp/hadoop-root/dfs/name/current/VERSION ]; then
  echo "[start-namenode] 未发现 VERSION，首次格式化 NameNode"
  hdfs namenode -format -force -nonInteractive
fi

# exec：让 namenode 成为 PID 1 的直接子进程，docker stop 能收到信号并进入优雅关闭
exec hdfs namenode
