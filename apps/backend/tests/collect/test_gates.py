"""收口闸门（V3 / V4 / V7）的行为约定。

这些测试的重点不是"能跑通"，而是**能不能失败**：每条闸门都要有一个
"故意弄坏一个字节 / 一个时刻"的用例，证明它真的会红。
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import replace as replace_record
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from logcopilot.collect.gates import (
    ClusterStatus,
    _count_from_report,
    check_raw_parity,
    check_restored,
    compare_canonical,
    compare_packs,
)
from logcopilot.domain.case import (
    Adapter,
    CanonicalProfile,
    CaseManifest,
    Checkpoint,
    Counters,
    FaultInjection,
    Hashes,
    Labels,
    Origin,
    OriginKind,
    RedactPolicy,
    SourceRef,
    Window,
)
from logcopilot.domain.log import LogLevel, LogRecord, ParseStatus
from logcopilot.store.casepack import (
    create_pack,
    finalize,
    open_log_writer,
    pack_paths,
    write_labels,
)

T0 = datetime(2026, 11, 22, 10, 0, 0, tzinfo=UTC)
RAW_SHA = "sha256:" + "e" * 64


def make_record(
    line_no: int,
    *,
    level: LogLevel = LogLevel.INFO,
    component: str = "dn1",
    message: str | None = None,
    offset_seconds: int = 0,
) -> LogRecord:
    return LogRecord(
        id=f"{component}:{line_no:05d}",
        ts=T0 + timedelta(seconds=offset_seconds),
        level=level,
        component=component,
        logger="DataNode",
        message=message if message is not None else f"message {line_no}",
        source=f"file:{component}",
        raw=None,
        parse_status=ParseStatus.OK,
    )


def build_pack(
    root: Path,
    case_id: str,
    records: list[LogRecord],
    *,
    labels: Labels | None = None,
    raw_line_count: int | None = None,
) -> Path:
    paths = pack_paths(root, case_id)
    create_pack(paths, labels or Labels(case_id=case_id))
    with open_log_writer(paths) as writer:
        for record in records:
            writer.append(record)

    by_level: dict[str, int] = {}
    by_component: dict[str, int] = {}
    for record in records:
        by_level[record.level.value] = by_level.get(record.level.value, 0) + 1
        by_component[record.component] = by_component.get(record.component, 0) + 1

    manifest = CaseManifest(
        case_id=case_id,
        created_at=T0,
        canonical_profile=CanonicalProfile.HDFS_LOG4J,
        window=Window(since=T0, until=T0 + timedelta(minutes=10)),
        origin=Origin(kind=OriginKind.COLLECTED, src=None),
        sources=(SourceRef(adapter=Adapter.FILE, ref="dn1", component="dn1"),),
        counters=Counters(
            raw_lines=raw_line_count if raw_line_count is not None else len(records),
            lines=len(records),
            by_level=by_level,
            by_component=by_component,
        ),
        hashes=Hashes(input_fingerprint=RAW_SHA, canonical_sha256="sha256:" + "0" * 64),
        checkpoint=Checkpoint(adapter_state={}, last_ts=T0),
        redact_policy=RedactPolicy(version="redact/1.0.0"),
    )
    finalize(
        paths,
        manifest,
        raw_line_count=raw_line_count if raw_line_count is not None else len(records),
        raw_sha256=RAW_SHA,
    )
    return paths.dir


# ---------------------------------------------------------------------------
# V3：两个 pack 等价
# ---------------------------------------------------------------------------


def test_identical_packs_have_no_problems(tmp_path: Path) -> None:
    records = [make_record(1), make_record(2, level=LogLevel.WARN)]
    left = build_pack(tmp_path, "gate_v3_oneshot", records)
    right = build_pack(tmp_path, "gate_v3_resume", records)

    assert compare_packs(left, right) == []


def test_changed_message_is_caught_by_content(tmp_path: Path) -> None:
    left = build_pack(tmp_path, "gate_v3_oneshot", [make_record(1), make_record(2)])
    right = build_pack(
        tmp_path,
        "gate_v3_resume",
        [make_record(1), make_record(2, message="改过一个字")],
    )

    problems = compare_packs(left, right)

    assert any("记录内容不同" in problem for problem in problems)


def test_missing_record_is_caught_by_the_record_count(tmp_path: Path) -> None:
    left = build_pack(tmp_path, "gate_v3_oneshot", [make_record(1), make_record(2)])
    right = build_pack(tmp_path, "gate_v3_resume", [make_record(1)])

    problems = compare_packs(left, right)

    assert any("记录数不同" in problem for problem in problems)
    assert any("只在左" in problem for problem in problems)


def test_same_content_with_permuted_ids_is_not_a_problem(tmp_path: Path) -> None:
    """V3 的真实形态：内容一致，但崩点之后 id 在"同一毫秒的平局"里换了位置。

    `id` 里的行号是按**读的先后**发的，一次采完与"崩了再续"的读法不同，所以
    平局内部的 id 会换位、`canonical_sha256` 会跟着变。这不是"重了或漏了"，
    闸门只该挡重与漏，所以这里必须不报。
    """
    first = make_record(1)
    second = make_record(2)
    left = build_pack(tmp_path, "gate_v3_oneshot", [first, second])
    # 同样的两条内容，交换 id（等价于"崩点让另一条流先拿到号"）
    right = build_pack(
        tmp_path,
        "gate_v3_resume",
        [replace_record(first, id="dn1:00002"), replace_record(second, id="dn1:00001")],
    )

    assert compare_packs(left, right) == []


def test_counter_drift_is_reported(tmp_path: Path) -> None:
    records = [make_record(1), make_record(2, level=LogLevel.ERROR)]
    left = build_pack(tmp_path, "gate_v3_oneshot", records)
    # 同样的记录、不同的原始行数（模拟"补采时多读了一行被丢弃"）
    right = build_pack(tmp_path, "gate_v3_resume", records, raw_line_count=3)

    problems = compare_packs(left, right)

    assert any("counters 不同" in problem for problem in problems)


# ---------------------------------------------------------------------------
# V3 的变换层：逐行比 canonical
# ---------------------------------------------------------------------------


def test_compare_canonical_reports_length_mismatch() -> None:
    assert compare_canonical(["a"], ["a", "b"]) == ["行数不同：左 1，右 2"]


def test_compare_canonical_points_at_the_first_difference() -> None:
    problems = compare_canonical(['{"id":"dn1"}', '{"id":"dn2"}'], ['{"id":"dn1"}', '{"id":"x"}'])

    assert len(problems) == 1
    assert problems[0].startswith("第 1 行不同")


# ---------------------------------------------------------------------------
# V7 离线：fault_injection 自洽
# ---------------------------------------------------------------------------


def _injection(**overrides: object) -> FaultInjection:
    fields: dict[str, object] = {
        "script": "dn_volume_fill",
        "started": T0,
        "ended": T0 + timedelta(minutes=5),
        "restored": T0 + timedelta(minutes=5, seconds=1),
        "restored_ok": True,
    }
    fields.update(overrides)
    return FaultInjection(**fields)  # type: ignore[arg-type]


def test_restored_injection_passes(tmp_path: Path) -> None:
    labels = Labels(case_id="inc_dnvol_01", fault_injection=_injection())
    build_pack(tmp_path, "inc_dnvol_01", [make_record(1)], labels=labels)

    assert check_restored(tmp_path) == []


def test_missing_restored_ok_fails(tmp_path: Path) -> None:
    labels = Labels(case_id="inc_dnvol_01", fault_injection=_injection(restored_ok=None))
    build_pack(tmp_path, "inc_dnvol_01", [make_record(1)], labels=labels)

    problems = check_restored(tmp_path)

    assert any("restored_ok" in problem for problem in problems)


def test_restore_failed_out_loudly_also_fails_the_gate(tmp_path: Path) -> None:
    """`restored_ok=False` 是采集器现场验出的"没恢复"——比缺字段更该红。"""
    labels = Labels(case_id="inc_dnvol_01", fault_injection=_injection(restored_ok=False))
    build_pack(tmp_path, "inc_dnvol_01", [make_record(1)], labels=labels)

    problems = check_restored(tmp_path)

    assert any("restored_ok=False" in problem for problem in problems)


def test_no_fault_pack_at_all_is_not_a_pass(tmp_path: Path) -> None:
    build_pack(tmp_path, "base_healthy_01", [make_record(1)])

    problems = check_restored(tmp_path)

    assert any("无从验证" in problem for problem in problems)


# ---------------------------------------------------------------------------
# V7 现场：ClusterStatus 的判定
# ---------------------------------------------------------------------------


def test_cluster_status_accepts_a_restored_cluster() -> None:
    status = ClusterStatus(
        running=("alcp-nn", "alcp-dn1", "alcp-dn2", "alcp-dn3"),
        safemode_off=True,
        live_datanodes=3,
        stray_files=(),
        network_members=("alcp-nn", "alcp-dn1", "alcp-dn2", "alcp-dn3"),
    )

    assert status.check() == []


def test_cluster_status_reports_every_deviation() -> None:
    status = ClusterStatus(
        running=("alcp-nn",),
        safemode_off=False,
        live_datanodes=2,
        stray_files=("alcp-dn1:lab.fill",),
        network_members=("alcp-nn",),
    )

    problems = status.check()

    assert any("容器没在跑" in problem for problem in problems)
    assert any("安全模式" in problem for problem in problems)
    assert any("Live datanodes = 2" in problem for problem in problems)
    assert any("残留" in problem for problem in problems)
    assert any("网络" in problem for problem in problems)


def test_cluster_status_flags_unreplicated_blocks() -> None:
    """上一例的伤没补回来时，闸门必须红 —— 否则基线建在别人的故障上。"""
    status = ClusterStatus(
        running=("alcp-nn", "alcp-dn1", "alcp-dn2", "alcp-dn3"),
        safemode_off=True,
        live_datanodes=3,
        stray_files=(),
        network_members=("alcp-nn", "alcp-dn1", "alcp-dn2", "alcp-dn3"),
        under_replicated_blocks=4,
        missing_blocks=1,
    )

    problems = status.check()

    assert any("Under-replicated blocks = 4" in problem for problem in problems)
    assert any("Missing blocks = 1" in problem for problem in problems)


@pytest.mark.parametrize(
    ("report", "prefix", "expected"),
    [
        # 真实输出是制表符缩进的；不 strip 就永远读不到数（闸门假绿）
        ("Replicated Blocks:\n\tUnder replicated blocks: 7\n", "Under replicated blocks", 7),
        # `Missing blocks (with replication factor 1)` 是另一个键，不能被前缀命中
        (
            "\tMissing blocks: 2\n\tMissing blocks (with replication factor 1): 0\n",
            "Missing blocks",
            2,
        ),
        ("\tMissing blocks (with replication factor 1): 5\n", "Missing blocks", 0),
        ("什么都没有", "Under replicated blocks", 0),
    ],
)
def test_count_from_report_reads_indented_keys(report: str, prefix: str, expected: int) -> None:
    assert _count_from_report(report, prefix) == expected


# ---------------------------------------------------------------------------
# V4：file 与 docker 的取数层等价
# ---------------------------------------------------------------------------


class FakeContainer:
    """一个内存里的容器：只实现 gates 用到的那三条命令。"""

    def __init__(self, container: str, files: dict[str, bytes], log_dir: str) -> None:
        self.container = container
        self.files = files
        self.log_dir = log_dir
        self.calls: list[list[str]] = []

    def __call__(self, argv: Sequence[str]) -> bytes:
        self.calls.append(list(argv))
        if argv[0] != "exec":
            raise AssertionError(f"未预期的命令：{argv}")
        _, _container, *rest = argv
        if rest[:2] == ["stat", "-c"]:
            path = rest[-1]
            return str(len(self._read(path))).encode()
        if rest[:2] == ["ls", "-1"]:
            return "\n".join(sorted(self.files)).encode()
        if rest[0] == "sh":
            script = rest[-1]
            # 形如：tail -c +101 /opt/hadoop/logs/x.log | head -c 200
            # 路径可能带引号也可能不带（shlex.quote 对纯安全字符不加引号），
            # 所以用正则解析，而不是 split("'")。
            matched = re.search(r"tail -c \+(\d+) (\S+) \| head -c (\d+)", script)
            if matched is None:
                raise AssertionError(f"未预期的脚本：{script}")
            start = int(matched.group(1))
            name = matched.group(2).strip("'\"").rsplit("/", 1)[-1]
            length = int(matched.group(3))
            data = self._read_path(name)
            return data[start - 1 : start - 1 + length]
        raise AssertionError(f"未预期的命令：{argv}")

    def _read(self, path: str) -> bytes:
        return self._read_path(path.rsplit("/", 1)[-1])

    def _read_path(self, name: str) -> bytes:
        if name not in self.files:
            raise AssertionError(f"容器里没有 {name}")
        return self.files[name]


def _write_host_log(root: Path, component: str, name: str, data: bytes) -> Path:
    target = root / component / name
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(data)
    return target


def test_parity_passes_when_both_paths_read_the_same_bytes(tmp_path: Path) -> None:
    data = b"2026-11-22T10:00:00.000+00:00 INFO  DataNode:1 - hello\n" * 20
    name = "hadoop-hdfs-datanode-dn1.log"
    _write_host_log(tmp_path, "dn1", name, data)
    container = FakeContainer("alcp-dn1", {name: data}, "/opt/hadoop/logs")

    parities = check_raw_parity(tmp_path, components=("dn1",), runner=container, tail_bytes=256)

    assert len(parities) == 1
    assert parities[0].ok, parities[0].describe()


def test_parity_fails_when_container_view_differs(tmp_path: Path) -> None:
    data = b"line one\nline two\n"
    name = "hadoop-hdfs-datanode-dn1.log"
    _write_host_log(tmp_path, "dn1", name, data)
    container = FakeContainer("alcp-dn1", {name: b"line one\nline TWO\n"}, "/opt/hadoop/logs")

    parities = check_raw_parity(tmp_path, components=("dn1",), runner=container, tail_bytes=256)

    assert not parities[0].ok
    assert "不一致" in parities[0].describe()


def test_parity_reports_missing_main_log(tmp_path: Path) -> None:
    container = FakeContainer("alcp-dn1", {}, "/opt/hadoop/logs")

    parities = check_raw_parity(tmp_path, components=("dn1",), runner=container)

    assert not parities[0].ok
    assert "找不到主日志流" in parities[0].describe()


@pytest.mark.parametrize("component", ["dn1", "nn"])
def test_write_labels_round_trip_is_used_by_check_restored(tmp_path: Path, component: str) -> None:
    """`check_restored` 读的是 labels.json，所以补一次 write_labels 的往返。"""
    case_id = f"inc_{component}_01"
    build_pack(
        tmp_path, case_id, [make_record(1, component=component)], labels=Labels(case_id=case_id)
    )
    paths = pack_paths(tmp_path, case_id)
    write_labels(paths, Labels(case_id=case_id, fault_injection=_injection()))

    assert check_restored(tmp_path) == []
