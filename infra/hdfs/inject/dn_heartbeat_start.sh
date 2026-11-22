#!/usr/bin/env bash
# DN_HEARTBEAT 复原：把被杀掉的 DataNode 拉起来。
# 注意：DataNode 重新注册到 NameNode 需要几秒到几十秒，
# 判据看 `dfsadmin -report` 里的 Live datanodes，不要看容器状态。
source "$(dirname "$0")/_common.sh"
run docker start "$DN3"