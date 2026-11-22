#!/usr/bin/env bash
# DN_HEARTBEAT 复原：SIGCONT，并确认状态真的回到 running。
source "$(dirname "$0")/_common.sh"
run docker unpause "$DN2"
docker inspect -f '{{.State.Status}}' "$DN2"