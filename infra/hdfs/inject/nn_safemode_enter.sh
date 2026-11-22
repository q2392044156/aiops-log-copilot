#!/usr/bin/env bash
# NN_SAFE_MODE：让 NameNode 进安全模式（只能读，写全被拒）。
source "$(dirname "$0")/_common.sh"
run docker exec "$NN" hdfs dfsadmin -safemode enter