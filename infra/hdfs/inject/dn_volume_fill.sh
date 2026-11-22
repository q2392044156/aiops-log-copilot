#!/usr/bin/env bash
# DN_VOLUME：把 dn1 的数据盘写满（128MB tmpfs 放 200MB，必然 ENOSPC）。
# dd 的"失败"就是故障本身，所以这里不能 set -e。
source "$(dirname "$0")/_common.sh"
run docker exec "$DN1" dd if=/dev/zero "of=$DN_DATA_DIR/$FILL_NAME" bs=1M "count=$FILL_MB" status=none || true
run docker exec "$DN1" df -B1 "$DN_DATA_DIR"