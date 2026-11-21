"""`domain/log.py` 的契约测试。

每条测试都对应 `P-A_COLLECT_PLAN.md` v0.2 里一条已经定死的规则。
它的作用是给这些规则做一份"可执行的副本"：谁改了规则而没改文档，
或者改了文档而代码没跟上，这里就会变红。
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta, timezone
from typing import Any

import pytest

from logcopilot.domain.log import (
    CANONICAL_FIELDS,
    FIELDS,
    LogLevel,
    LogRecord,
    ParseStatus,
)

#: 测试基准时刻：带毫秒、UTC。
TS = datetime(2026, 11, 22, 10, 3, 11, 482_000, tzinfo=UTC)

#: 逐字节冻结的期望值。字段顺序、`+00:00`、`null`、紧凑分隔符都在里面，
#: 动了任何一个都会让这条测试变红 —— 这正是我们想要的（哈希稳定性靠它）。
EXPECTED_JSON = (
    '{"id":"dn2:00123","ts":"2026-11-22T10:03:11.482+00:00","level":"ERROR",'
    '"component":"dn2","logger":"FsDatasetImpl",'
    '"message":"Receiving BP-1 blk_1 src: 10.0.0.2:50010",'
    '"source":"file:dn2","raw":null,"parse_status":"ok","template_id":null}'
)


def make(**overrides: Any) -> LogRecord:
    """构造一条合法记录；测试只覆盖自己关心的字段。"""
    base: dict[str, Any] = {
        "id": "dn2:00123",
        "ts": TS,
        "level": LogLevel.ERROR,
        "component": "dn2",
        "logger": "FsDatasetImpl",
        "message": "Receiving BP-1 blk_1 src: 10.0.0.2:50010",
        "source": "file:dn2",
        "raw": None,
        "parse_status": ParseStatus.OK,
        "template_id": None,
    }
    base.update(overrides)
    return LogRecord(**base)


# ---------------------------------------------------------------- 1. id 生成


def test_build_id_pads_line_no_to_five_digits() -> None:
    assert LogRecord.build_id("dn1", 12) == "dn1:00012"
    # 行号超过 5 位不截断：长文件不能因为位数变化就重号
    assert LogRecord.build_id("nn", 100_000) == "nn:100000"


# ------------------------------------------------- 2. 序列化逐字节 + 字段顺序


def test_to_json_line_is_byte_stable() -> None:
    record = make()
    assert record.to_json_line() == EXPECTED_JSON
    # 顺序漂了会让 canonical_sha256 在两次运行之间不一样
    assert tuple(record.to_dict()) == FIELDS


# --------------------------------------------- 3. canonical 投影（V4 的核心）


def test_canonical_projection_excludes_source() -> None:
    payload = make(source="file:dn2").canonical_payload()

    assert tuple(payload) == CANONICAL_FIELDS
    assert "source" not in payload
    # 同一条日志走 file 还是 docker，投影必须完全一样；
    # 否则 V4「两条路径 canonical_sha256 相等」永远过不了。
    assert make(source="docker:dn2").canonical_payload() == payload


# ---------------------------------------------------------------- 4. 往返


def test_round_trip_ok_line() -> None:
    record = make()
    assert LogRecord.from_json_line(record.to_json_line()) == record


def test_round_trip_partial_line_keeps_raw() -> None:
    record = make(
        parse_status=ParseStatus.PARTIAL,
        message="",
        raw="  getNumBytes()     = 18",
    )
    restored = LogRecord.from_json_line(record.to_json_line())
    assert restored == record
    assert restored.raw == "  getNumBytes()     = 18"


# ------------------------------------------------------------ 5. 闭集校验


@pytest.mark.parametrize("bad_level", ["WARNING", "warn", ""])
def test_invalid_level_rejected(bad_level: str) -> None:
    with pytest.raises(ValueError, match="level 非法"):
        make(level=bad_level)


def test_invalid_parse_status_rejected() -> None:
    with pytest.raises(ValueError, match="parse_status 非法"):
        make(parse_status="done")


# ------------------------------------------------------------ 6. 时区（A-R2）


def test_naive_ts_rejected() -> None:
    with pytest.raises(ValueError, match="必须带时区"):
        make(ts=datetime(2026, 11, 22, 10, 3, 11))


def test_non_utc_ts_rejected() -> None:
    shanghai = timezone(timedelta(hours=8))
    with pytest.raises(ValueError, match="必须是 UTC"):
        make(ts=datetime(2026, 11, 22, 18, 3, 11, tzinfo=shanghai))


# ------------------------------------------------------------ 7. raw 规则


def test_ok_line_must_not_carry_raw() -> None:
    with pytest.raises(ValueError, match="raw 必须为 None"):
        make(raw="2026-11-22 10:03:11,482 ERROR ...")


def test_failed_line_must_carry_raw() -> None:
    with pytest.raises(ValueError, match="必须保留 raw"):
        make(parse_status=ParseStatus.FAILED, message="")
    # 带上 raw 就能过：解析失败的行不丢弃
    record = make(parse_status=ParseStatus.FAILED, message="", raw="??? 0xdeadbeef")
    assert record.raw == "??? 0xdeadbeef"


# ------------------------------------------------------- 8. id 与组件一致性


def test_id_component_mismatch_rejected() -> None:
    with pytest.raises(ValueError, match="前缀与 component 不一致"):
        make(id="dn1:00123")


def test_malformed_id_rejected() -> None:
    with pytest.raises(ValueError, match="id 必须形如"):
        make(id="l_000001")


# --------------------------------------------------------- 9. schema 漂移


def test_from_json_line_rejects_extra_field() -> None:
    data = json.loads(EXPECTED_JSON)
    data["raw_ref"] = "anchors/dn2.jsonl#123"
    with pytest.raises(ValueError, match="字段不匹配"):
        LogRecord.from_json_line(json.dumps(data))


# ------------------------------------------------------------ 10. 排序键


def test_sort_key_orders_by_ts_then_id() -> None:
    # component 必须与 id 前缀一致，所以换 id 时要一起换
    early = make(id="dn1:00099", component="dn1", ts=TS - timedelta(seconds=1))
    a = make(id="dn1:00002", component="dn1")
    b = make(id="dn1:00010", component="dn1")

    ordered = sorted([b, a, early], key=LogRecord.sort_key)

    assert ordered == [early, a, b]
