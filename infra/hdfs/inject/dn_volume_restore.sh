#!/usr/bin/env bash
# DN_VOLUME 复原：删掉填盘文件，再确认数据盘能读（df 退出码 0 就是证据）。
source "$(dirname "$0")/_common.sh"
run docker exec "$DN1" rm -f "$DN_DATA_DIR/$FILL_NAME"
run docker exec "$DN1" df -B1 "$DN_DATA_DIR"