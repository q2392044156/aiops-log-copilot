"""`collect/canonical.py` 的契约测试。

覆盖 P-A §7 的 V9（旧数据两种语法 + 带前缀的续行）以及 §6.2 的三条判据：
时间戳识别、续行归并、不认识的行不丢弃。

fixture 是**真实旧数据的切片**（`tests/fixtures/logs/*.log`），不是编出来的：
旧语法的两个坑（字段之间两个空格、续行也带 `[component] ` 前缀）只有真数据
才长得出来，自己编的样例会把这两个坑一起编没了。
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from logcopilot.collect.canonical import (
    CanonicalResult,
    assert_within_window,
    canonicalize,
    normalize_component,
)
from logcopilot.domain.case import CanonicalProfile, Window
from logcopilot.domain.log import LogLevel, ParseStatus

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "logs"

#: 旧数据是 2026-11-11；新采的 log4j 样例是 2026-11-22。
SINCE_LAB = datetime(2026, 11, 11, 8, 0, tzinfo=UTC)
SINCE_HDFS = datetime(2026, 11, 22, 10, 0, tzinfo=UTC)


def load(name: str) -> list[str]:
    return FIXTURES.joinpath(name).read_text(encoding="utf-8").splitlines()


def run(
    name: str,
    *,
    profile: CanonicalProfile = CanonicalProfile.LAB,
    component: str = "dn1",
    source: str = "file:dn1",
    since: datetime = SINCE_LAB,
    redact: object | None = None,
) -> CanonicalResult:
    return canonicalize(
        enumerate(load(name), start=1),
        profile=profile,
        component=component,
        source=source,
        since=since,
        redact=redact,  # type: ignore[arg-type]
    )


def run_lines(
    lines: Iterable[str],
    *,
    profile: CanonicalProfile = CanonicalProfile.LAB,
    component: str = "dn1",
    since: datetime = SINCE_LAB,
    redact: object | None = None,
) -> CanonicalResult:
    return canonicalize(
        enumerate(lines, start=1),
        profile=profile,
        component=component,
        source="file:dn1",
        since=since,
        redact=redact,  # type: ignore[arg-type]
    )


# ---------------------------------------------------------------- 旧语法 A


def test_syntax_a_record_count() -> None:
    """9 行原始文本 → 5 条记录：4 条续行被并进它们前面那条。"""
    result = run("lab_syntax_a.log")
    assert result.raw_line_count == 9
    assert len(result.records) == 5


def test_syntax_a_ids_follow_physical_line_numbers() -> None:
    """id 的行号是**物理行号**，不是"第几条记录"。

    这是 V3（续采不重不漏）的前提：续采时重复读到的行必须映射回同一个 id。
    第 3 条吸收了第 4–7 行，所以下一条是 `00008` 而不是 `00004`。
    """
    result = run("lab_syntax_a.log")
    assert [r.id for r in result.records] == [
        "dn1:00001",
        "dn1:00002",
        "dn1:00003",
        "dn1:00008",
        "dn2:00009",
    ]


def test_syntax_a_continuation_merges_into_previous() -> None:
    result = run("lab_syntax_a.log")
    merged = result.records[2]
    assert merged.parse_status is ParseStatus.PARTIAL
    assert "\n" in merged.message
    assert merged.message.startswith("Scheduling blk_1073741944_1120 replica")
    assert merged.message.endswith("getVolume()       = <:PATH:>")
    # raw 保留 1 条主行 + 4 条续行，用 \n 拼回去
    assert merged.raw is not None
    assert len(merged.raw.split("\n")) == 5


def test_syntax_a_continuation_inherits_timestamp() -> None:
    """续行没有自己的时间戳，必须继承主行的时间（否则时间轴会空一片）。"""
    merged = run("lab_syntax_a.log").records[2]
    assert merged.ts == datetime(2026, 11, 11, 8, 15, 26, tzinfo=UTC)


def test_syntax_a_ok_records_carry_no_raw() -> None:
    """ok 行的原文在 runtime 日志里，CasePack 里不再存一份（体积翻倍）。"""
    result = run("lab_syntax_a.log")
    assert result.records[0].raw is None
    assert result.records[1].raw is None


def test_syntax_a_component_comes_from_each_line_prefix() -> None:
    """旧数据一个文件里 dn1 / dn2 交替出现，component 必须逐行取。"""
    records = run("lab_syntax_a.log").records
    assert records[0].component == "dn1"
    assert records[-1].component == "dn2"
    assert records[-1].id.startswith("dn2:")


def test_syntax_a_logger_keeps_only_class_name() -> None:
    """`DataNode:750 - ...` → logger=DataNode；行号不进字段（LogRecord 没有它）。"""
    first = run("lab_syntax_a.log").records[0]
    assert first.logger == "DataNode"
    assert first.level is LogLevel.INFO
    assert first.message.startswith("Receiving BP-")


# ---------------------------------------------------------------- 旧语法 B


def test_syntax_b_record_count() -> None:
    """8 行 → 3 条：STARTUP_MSG 之后的 5 行全是续行。"""
    result = run("lab_syntax_b.log")
    assert result.raw_line_count == 8
    assert len(result.records) == 3


def test_syntax_b_comma_milliseconds() -> None:
    first = run("lab_syntax_b.log").records[0]
    assert first.ts == datetime(2026, 11, 11, 17, 39, 6, 965000, tzinfo=UTC)


def test_syntax_b_trailing_space_is_stripped() -> None:
    """真实数据里 `STARTUP_MSG: ` 行尾有一个空格，message 不能带着它。"""
    first = run("lab_syntax_b.log").records[0]
    lines = first.message.split("\n")
    assert lines[0] == "STARTUP_MSG:"
    # 1 条主行 + 5 条续行（`/****`、Starting DataNode、host、args、version）
    assert len(lines) == 6
    assert lines[1].startswith("/****")


def test_syntax_b_full_qualified_logger() -> None:
    assert run("lab_syntax_b.log").records[0].logger == "datanode.DataNode"


def test_syntax_b_error_line_is_still_ok() -> None:
    """续行只影响它前面那条；ERROR 行自己没有续行，保持 ok。"""
    records = run("lab_syntax_b.log").records
    assert records[1].level is LogLevel.ERROR
    assert records[1].parse_status is ParseStatus.OK
    assert records[1].raw is None


# ---------------------------------------------------------------- 新采 log4j


def test_hdfs_log4j_record_count() -> None:
    result = run(
        "hdfs_log4j.log",
        profile=CanonicalProfile.HDFS_LOG4J,
        component="dn1",
        source="docker:dn1",
        since=SINCE_HDFS,
    )
    assert result.raw_line_count == 8
    assert len(result.records) == 5


def test_hdfs_log4j_stack_trace_merges_into_error() -> None:
    """异常栈的两行（`java.io.IOException:` 与 `\\tat ...`）都是续行。"""
    records = run(
        "hdfs_log4j.log",
        profile=CanonicalProfile.HDFS_LOG4J,
        component="dn1",
        source="docker:dn1",
        since=SINCE_HDFS,
    ).records
    error = records[3]
    assert error.level is LogLevel.ERROR
    assert error.parse_status is ParseStatus.PARTIAL
    assert "java.io.IOException: Volume failure" in error.message
    assert "\tat org.apache.hadoop" in error.message
    assert error.logger == "org.apache.hadoop.hdfs.server.datanode.DataNode"


def test_hdfs_log4j_does_not_strip_component_prefix() -> None:
    """hdfs_log4j profile 下不做前缀剥离：日志行首就是时间戳。"""
    first = run(
        "hdfs_log4j.log",
        profile=CanonicalProfile.HDFS_LOG4J,
        component="nn",
        source="docker:nn",
        since=SINCE_HDFS,
    ).records[0]
    assert first.component == "nn"
    assert first.logger == "util.ExitUtil"


# ---------------------------------------------------------------- 级别与兜底


def test_warning_alias_maps_to_warn() -> None:
    result = run_lines(["2026-11-11 08:15:05 WARNING util.ExitUtil: x"])
    assert result.records[0].level is LogLevel.WARN


def test_unknown_level_inherits_previous_level() -> None:
    """`SEVERE` 不在闭集里：时间戳与正文是真的，级别退回上一行并标 partial。"""
    result = run_lines(
        [
            "2026-11-11 08:15:05 ERROR util.ExitUtil: first",
            "2026-11-11 08:15:06 SEVERE util.ExitUtil: second",
        ]
    )
    assert result.records[1].level is LogLevel.ERROR
    assert result.records[1].parse_status is ParseStatus.PARTIAL
    assert result.records[1].raw is not None


def test_empty_message_downgrades_to_partial() -> None:
    """正文为空时不能算 ok —— LogRecord 不允许"空 message 且无 raw"。"""
    result = run_lines(["2026-11-11 08:15:05 INFO  util.ExitUtil: "])
    record = result.records[0]
    assert record.parse_status is ParseStatus.PARTIAL
    assert record.raw is not None


def test_unknown_component_prefix_yields_failed() -> None:
    """前缀认不出组件时不猜，整行 failed，component 用声明的那个兜底。"""
    result = run_lines(["[weird-box] 2026-11-11 08:15:05 INFO  Foo:500 - x"], component="dn1")
    record = result.records[0]
    assert record.parse_status is ParseStatus.FAILED
    assert record.component == "dn1"
    assert record.raw is not None


def test_orphan_continuation_at_start_uses_since() -> None:
    """文件第一行就是续行（孤儿）：用窗口起点兜底，不丢。"""
    result = run_lines(["  orphan tail without head"])
    record = result.records[0]
    assert record.parse_status is ParseStatus.FAILED
    assert record.ts == SINCE_LAB
    assert record.message == "  orphan tail without head"


def test_line_without_prefix_is_treated_as_continuation() -> None:
    """lab profile 下没有 `[comp] ` 前缀的行，归并到上一条，而不是造新记录。"""
    result = run_lines(
        [
            "2026-11-11 08:15:05 INFO  DataNode:750 - head",
            "no prefix tail line",
        ]
    )
    assert len(result.records) == 1
    assert result.records[0].parse_status is ParseStatus.PARTIAL
    assert result.records[0].message.endswith("no prefix tail line")


# ---------------------------------------------------------------- 归一与脱敏


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("aiops-lab-dn2", "dn2"),
        ("aiops-lab-namenode", "nn"),
        ("hdfs-datanode3", "dn3"),
        ("datanode1", "dn1"),
        ("datanode9", "dn9"),
        ("nn", "nn"),
        ("UnRecoverable", None),
        ("", None),
    ],
)
def test_normalize_component(raw: str, expected: str | None) -> None:
    assert normalize_component(raw) == expected


class _CountingRedactor:
    def __init__(self, needle: str = "10.0.0.1") -> None:
        self.needle = needle
        self.calls = 0

    def apply(self, text: str) -> tuple[str, dict[str, int]]:
        self.calls += 1
        return text.replace(self.needle, "<:IP:>"), {"ip": text.count(self.needle)}


def test_redactor_is_injected_into_message() -> None:
    redactor = _CountingRedactor()
    result = run_lines(
        ["2026-11-11 08:15:05 INFO  DataNode:750 - connect 10.0.0.1 failed"],
        redact=redactor,
    )
    assert "10.0.0.1" not in result.records[0].message
    assert result.records[0].message.endswith("<:IP:> failed")
    assert result.redacted == {"ip": 1}
    assert redactor.calls == 1


def test_redactor_also_scrubs_raw() -> None:
    """raw 是同样会离开这台机器的文本，不能因为"原文"就跳过脱敏。"""
    redactor = _CountingRedactor()
    result = run_lines(["  orphan 10.0.0.1 tail"], redact=redactor)
    record = result.records[0]
    assert record.raw is not None
    assert "10.0.0.1" not in record.raw
    assert result.redacted["ip"] >= 1


def test_without_redactor_counts_are_empty() -> None:
    assert run("lab_syntax_a.log").redacted == {}


def test_every_fixture_satisfies_raw_line_accounting() -> None:
    """`raw_lines >= lines` 是 manifest.counters 的对账等式之一。"""
    for name, since in (("lab_syntax_a.log", SINCE_LAB), ("lab_syntax_b.log", SINCE_LAB)):
        result = run(name, since=since)
        assert result.raw_line_count >= len(result.records)


def test_empty_input() -> None:
    result = run_lines([])
    assert result.records == ()
    assert result.raw_line_count == 0


def test_crlf_line_endings() -> None:
    result = run_lines(["2026-11-11 08:15:05 INFO  util.ExitUtil: x\r\n"])
    assert result.records[0].message == "x"


# ---------------------------------------------------------------- 窗口


def test_assert_within_window_passes() -> None:
    result = run("lab_syntax_a.log")
    window = Window(
        since=datetime(2026, 11, 11, 8, 10, tzinfo=UTC),
        until=datetime(2026, 11, 11, 8, 20, tzinfo=UTC),
    )
    assert_within_window(result.records, window)


def test_assert_within_window_rejects_out_of_range() -> None:
    result = run("lab_syntax_a.log")
    window = Window(
        since=datetime(2026, 11, 11, 12, 0, tzinfo=UTC),
        until=datetime(2026, 11, 11, 12, 5, tzinfo=UTC),
    )
    with pytest.raises(ValueError, match="窗口"):
        assert_within_window(result.records, window)


def test_since_must_be_utc() -> None:
    with pytest.raises(ValueError, match="时区|UTC"):
        canonicalize(
            enumerate(["2026-11-11 08:15:05 INFO  DataNode:750 - x"], start=1),
            profile=CanonicalProfile.LAB,
            component="dn1",
            source="file:dn1",
            since=datetime(2026, 11, 11, 8, 0),  # naive
        )


def test_ts_is_always_utc() -> None:
    for record in run("lab_syntax_a.log").records:
        assert record.ts.utcoffset() == timedelta(0)
