#!/usr/bin/env bash
# DN_HEARTBEAT：SIGSTOP 一台 DataNode —— 进程在、心跳停、日志不写。
source "$(dirname "$0")/_common.sh"
run docker pause "$DN2"