#!/usr/bin/env bash
# NETWORK_CONNECTIVITY：把一台容器从 HDFS 网络上摘下来。
# 用 docker network disconnect 而不是 iptables：不需要 NET_ADMIN 能力。
source "$(dirname "$0")/_common.sh"
run docker network disconnect "$NETWORK" "$DN2"