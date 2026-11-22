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
# 客户端日志单独落一个文件，采集器把它当 nn 的第四条流收走。
# 为什么值得收：客户端（`hdfs dfs`）是唯一直说"谁写失败了"的地方 ——
# 断网时它打 `Exception in createBlockOutputStream` + `Excluding datanode`，
# 副本放不下时打 `Slow waitForAckedSeqno`。实测这些是 WARN，但它带来了
# 服务端日志里没有的因果链（NN 只报"放不下副本"，不说被谁拖累）。
HDFS_LOG4J=${HDFS_LOG4J:-/opt/hadoop/etc/hadoop/client-log4j.properties}
CLIENT_LOG=${CLIENT_LOG:-/opt/hadoop/logs/workload.out}

log() { echo "[workload] $*"; }

# 用服务端那份 pattern 跑客户端日志，否则 `24/11/22` 这种格式解析不了。
#
# 必须是 `HADOOP_OPTS`，不是 `HADOOP_CLIENT_OPTS`：实测（2026-11-22）后者被
# `hdfs` 启动脚本丢掉，JVM 根本没收到 `-Dlog4j.configuration`，客户端一条
# log4j 日志都不打 —— 只有 FsShell 自己那行纯文本（`put: Cannot create ...`）。
export HADOOP_OPTS="-Dlog4j.configuration=file:${HDFS_LOG4J} -Dhadoop.root.logger=INFO,console ${HADOOP_OPTS:-}"
hdfs_() {
    "$HDFS" "$@"
}

# 停机方式：**协作式**，不要直接 kill。
# 直接 SIGTERM 会掐断正在进行的 `hdfs dfs -put`，DN 那边立刻打
# `DataXceiver error processing WRITE_BLOCK operation` 的 ERROR —— 实测这样
# 会在故障窗口外凭空多出 3 条 ERROR，V6 的门槛就被噪声顶起来了。
# 所以停机 = 建一个哨兵文件，循环在两次迭代之间自己退出，当前那次 put 写完。
STOP_FILE=${STOP_FILE:-/opt/hadoop/logs/.workload-stop}
rm -f "$STOP_FILE"
trap 'log "收到终止信号，退出"; exit 0' TERM INT

$HDFS dfs -mkdir -p "$DIR" >/dev/null 2>&1 || true
log "开始：目录 $DIR，间隔 ${INTERVAL}s，pid $$"

i=0
while [ "$i" -lt "$MAX_FILES" ] && [ ! -f "$STOP_FILE" ]; do
    i=$((i + 1))
    f="/tmp/w_${i}.txt"
    printf 'workload line %s at %s\n' "$i" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" > "$f"
    # put 触发写（DN 的 BlockReceiver 日志）、cat 触发读、rm 触发删除 ——
    # 三条路径都会留下可被采集到的痕迹。
    hdfs_ dfs -put -f "$f" "$DIR/w_${i}.txt" >/dev/null 2>>"$CLIENT_LOG" || true
    hdfs_ dfs -cat "$DIR/w_${i}.txt" >/dev/null 2>>"$CLIENT_LOG" || true
    hdfs_ dfs -rm -f "$DIR/w_${i}.txt" >/dev/null 2>>"$CLIENT_LOG" || true
    if [ $((i % 50)) -eq 0 ]; then
        log "已写 $i 个文件"
    fi
    sleep "$INTERVAL"
done

log "达到上限 $MAX_FILES，退出"
