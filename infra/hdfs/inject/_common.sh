#!/usr/bin/env bash
# 注入脚本的公共部分。
#
# 这些脚本是**人工版**：有人在终端里复现一次故障时用它们。采集链路（collect/inject.py）
# 不调 bash，它自己拼同样的 docker 命令 —— 这样 Windows 上没有 bash 也能采，
# 而这两边的命令必须保持一致，改了这边记得改那边。
#
# 所有脚本都支持 DRY_RUN=1 空跑：只打印要做什么，不动集群。
set -uo pipefail

DRY_RUN="${DRY_RUN:-0}"
NN="${NN:-alcp-nn}"
DN1="${DN1:-alcp-dn1}"
DN2="${DN2:-alcp-dn2}"
DN3="${DN3:-alcp-dn3}"
NETWORK="${NETWORK:-aiops-log-copilot_hdfs}"
DN_DATA_DIR="${DN_DATA_DIR:-/tmp/hadoop-root/dfs/data}"
FILL_NAME="${FILL_NAME:-lab.fill}"
FILL_MB="${FILL_MB:-200}"

run() {
  if [ "$DRY_RUN" = "1" ]; then
    echo "[dry-run] $*"
  else
    echo "[inject] $*"
    "$@"
  fi
}