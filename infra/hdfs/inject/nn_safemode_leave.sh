#!/usr/bin/env bash
# NN_SAFE_MODE 复原：退出安全模式，并确认状态是 OFF。
source "$(dirname "$0")/_common.sh"
run docker exec "$NN" hdfs dfsadmin -safemode leave
docker exec "$NN" hdfs dfsadmin -safemode get