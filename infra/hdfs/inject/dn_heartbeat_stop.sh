#!/usr/bin/env bash
# DN_HEARTBEAT：直接杀掉一台 DataNode（与 pause 的区别：重启会重新注册块）。
source "$(dirname "$0")/_common.sh"
run docker stop "$DN3"