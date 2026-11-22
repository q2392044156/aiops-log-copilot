#!/bin/bash
# 顺序 5e · 读写负载：让 HDFS 在采集窗口里持续产出正常业务日志
#
# 为什么需要它：空载集群几乎不写日志，采出来的"正常样本"是一段死水 ——
# P-B 挖不出模板，P-C 也没有可检索的上下文。负载必须真实（put/get/rm），
# 而不是 echo 一行假日志到 .log 文件里（那等于自己伪造语料）。
#
# 用法（在 NN 容器内）：/lab/bin/workload.sh &
# 停止：pkill -f /lab/bin/workload.sh
#
# 注意：脚本自己把自己 mkdir/put/rm 掉的文件写进 /workload，靠 HDFS 自身
# 的审计日志（hdfs-audit.log）与 DN 的 BlockReceiver 日志留下轨迹。
set -u

HDFS=${HDFS:-hdfs}
DIR=${WORKLOAD_DIR:-/workload}
INTERVAL=${WORKLOAD_INTERVAL:-1}
MAX_FILES=${WORKLOAD_MAX_FILES:-100000}

log() { echo "[workload] $*"; }

trap 'log "收到终止信号，退出"; exit 0' TERM INT

$HDFS dfs -mkdir -p "$DIR" >/dev/null 2>&1 || true
log "开始：目录 $DIR，间隔 ${INTERVAL}s，pid $$"

i=0
while [ "$i" -lt "$MAX_FILES" ]; do
    i=$((i + 1))
    f="/tmp/w_${i}.txt"
    printf 'workload line %s at %s\n' "$i" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" > "$f"
    # put 触发写（DN 的 BlockReceiver 日志）、cat 触发读、rm 触发删除 ——
    # 三条路径都会留下可被采集到的痕迹。
    $HDFS dfs -put -f "$f" "$DIR/w_${i}.txt" >/dev/null 2>&1 || true
    $HDFS dfs -cat "$DIR/w_${i}.txt" >/dev/null 2>&1 || true
    $HDFS dfs -rm -f "$DIR/w_${i}.txt" >/dev/null 2>&1 || true
    if [ $((i % 50)) -eq 0 ]; then
        log "已写 $i 个文件"
    fi
    sleep "$INTERVAL"
done

log "达到上限 $MAX_FILES，退出"
