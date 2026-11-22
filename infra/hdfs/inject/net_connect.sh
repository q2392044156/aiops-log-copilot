#!/usr/bin/env bash
# NETWORK_CONNECTIVITY 复原：接回网络，并确认容器名出现在网络里。
source "$(dirname "$0")/_common.sh"
run docker network connect "$NETWORK" "$DN2"
docker network inspect "$NETWORK" --format '{{range .Containers}}{{.Name}} {{end}}'