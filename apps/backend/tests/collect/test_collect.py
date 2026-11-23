"""`collect/collect.py` 的契约测试（第 10 步）。

这一批测试盯的是**编排**，不是解析：解析的正确性在 `test_canonical.py`，
打包的正确性在 `test_casepack.py`。这里只回答五个问题：

1. 一次采集能不能从"一堆文件"走到"一个完成的 pack"？
2. 中途崩了，checkpoint 有没有留下来？接着采，结果与一次采到底是否等价？
3. 注入模式是不是真的"先 flush、再注入、再采一轮"？
4. docker 与 file 两条路径，编排层产出的 `canonical_sha256` 是否相同（V4）？
5. 空目录、坏行、越界时间戳这些边角，是报错还是静默放过？
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from logcopilot.collect.adapters.base import (
    AdapterState,
    StreamReader,
    StreamSpec,
    decode_line,
)
from logcopilot.collect.adapters.file import FileAdapter, hdfs_streams
from logcopilot.collect.collect import (
    CHECKPOINT_FILE,
    CollectError,
    CollectMode,
    Collector,
    RawHasher,
    SourceSpec,
)
from logcopilot.collect.redact import PatternRedactor
from logcopilot.domain.case import (
    REDACT_RULES,
    Adapter,
    CanonicalProfile,
    Expect,
    FaultInjection,
    Labels,
    OriginKind,
    Window,
)
from logcopilot.store.casepack import (
    CasePackExists,
    is_complete,
    pack_paths,
    read_anchors,
    read_labels,
    read_logs,
    read_manifest,
)

# ---------------------------------------------------------------- 固定量

SINCE = datetime(2026, 11, 22, 10, 0, tzinfo=UTC)
WINDOW = Window(since=SINCE, until=SINCE + timedelta(minutes=10))

LINE_1 = "2026-11-22 10:00:01,100 INFO  DataNode:100 - starting up"
LINE_2 = "2026-11-22 10:00:02,200 WARN  DataNode:120 - slow disk detected"
LINE_3 = "2026-11-22 10:00:03,300 ERROR DataNode:130 - Volume failure on /data/1"
LINE_OLD = "2026-11-21 03:00:00,000 INFO  DataNode:1 - 昨天的旧行"
LINE_BAD_LEVEL = "2026-11-22 10:00:05,000 SEVERE  DataNode:9 - 认不出的级别"
LINE_IP = "2026-11-22 10:00:06,000 INFO  DataNode:140 - connected to 10.1.2.3:9866"

LATER_1 = "2026-11-22 10:05:00,000 ERROR DataNode:200 - Volume failure again"


# ---------------------------------------------------------------- 工具


def write_stream(root: Path, component: str, name: str, lines: Sequence[str]) -> Path:
    """在 `<root>/<component>/` 下造一份日志文件（LF 行尾），返回路径。"""
    directory = root / component
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / name
    path.write_text("\n".join(lines) + "\n", encoding="utf-8", newline="\n")
    return path


def labels_for(case_id: str) -> Labels:
    return Labels(case_id=case_id, expect=Expect(min_error_lines=1))


def source_for(root: Path, component: str, *, ref: str = "fixture") -> SourceSpec:
    """`ref` 用常量而不是路径：它会进 `input_fingerprint`，两次采集必须相同。"""
    return SourceSpec(
        component=component,
        adapter=FileAdapter(root, hdfs_streams((component,))),
        ref=ref,
    )


def make_collector(
    root: Path,
    *,
    case_id: str = "inc_dnvol_01",
    components: Sequence[str] = ("dn1",),
    profile: CanonicalProfile = CanonicalProfile.HDFS_LOG4J,
    redactor: Any = None,
    window: Window = WINDOW,
    sources: Sequence[SourceSpec] | None = None,
) -> Collector:
    return Collector(
        root,
        case_id=case_id,
        profile=profile,
        window=window,
        labels=labels_for(case_id),
        sources=list(sources)
        if sources is not None
        else [source_for(root, component) for component in components],
        redactor=redactor,
    )


# ---------------------------------------------------------------- 假 adapter


class FakeStream:
    """内存里的一条"文件"，可以追加 —— 用来模拟注入后新写出来的日志。"""

    __slots__ = ("data", "name", "order")

    def __init__(self, name: str, order: int = 0) -> None:
        self.name = name
        self.order = order
        self.data = b""

    def append(self, lines: Sequence[str]) -> None:
        self.data += "".join(f"{line}\n" for line in lines).encode("utf-8")


class FakeReader:
    """按字节偏移读 `FakeStream`，行为与 FileReader 对齐（半行不消费）。"""

    __slots__ = ("_offset", "_spec", "_stream")

    def __init__(self, spec: StreamSpec, stream: FakeStream) -> None:
        self._spec = spec
        self._stream = stream
        self._offset = 0

    @property
    def spec(self) -> StreamSpec:
        return self._spec

    def read(self, state: AdapterState | None = None) -> Iterator[tuple[int, str]]:
        start = state.offset if state is not None else 0
        self._offset = start
        for raw in self._stream.data[start:].splitlines(keepends=True):
            if not raw.endswith(b"\n"):
                # 半行：这一轮不消费，offset 停在行边界上。
                return
            self._offset += len(raw)
            yield (0, decode_line(raw))

    def state(self) -> AdapterState:
        # 只报 offset：line_no 由 ComponentReader 发号（base.py 的职责）。
        return AdapterState(offset=self._offset, line_no=0)


class FakeAdapter:
    """一个组件、若干条内存流的 adapter，语义等价于 FileAdapter。"""

    adapter = Adapter.FILE

    def __init__(self, component: str, streams: Sequence[FakeStream]) -> None:
        self._component = component
        self._streams = {stream.name: stream for stream in streams}

    def streams(self) -> Sequence[StreamSpec]:
        return tuple(
            StreamSpec(component=self._component, name=stream.name, order=stream.order)
            for stream in self._streams.values()
        )

    def open(self, spec: StreamSpec) -> StreamReader:
        return FakeReader(spec, self._streams[spec.name])


class BoomAdapter:
    """第一次 open 就炸 —— 模拟"采到一半容器挂了"。"""

    adapter = Adapter.FILE

    def __init__(self, inner: FileAdapter) -> None:
        self._inner = inner
        self.opened = 0

    def streams(self) -> Sequence[StreamSpec]:
        return self._inner.streams()

    def open(self, spec: StreamSpec) -> StreamReader:
        self.opened += 1
        raise RuntimeError("容器挂了：模拟取数中断")


class RecordingHook:
    """记录"注入前一刻"的现场，然后往文件里追加故障段日志。"""

    name = "fake-inject"

    def __init__(self, target: Path, extra: Sequence[str]) -> None:
        self._target = target
        self._extra = extra
        self.seen_lines_when_flushed = -1
        self.calls = 0

    def __call__(self, flush: Any) -> FaultInjection:
        self.calls += 1
        flush()
        # 注入脚本执行前，baseline 段必须已经在盘上。
        self.seen_lines_when_flushed = sum(
            1 for line in self._target.read_text(encoding="utf-8").splitlines() if line.strip()
        )
        with self._target.open("a", encoding="utf-8", newline="\n") as handle:
            for line in self._extra:
                handle.write(line + "\n")
        started = datetime(2026, 11, 22, 10, 5, tzinfo=UTC)
        return FaultInjection(
            script="inject/disk_full.sh",
            started=started,
            ended=started + timedelta(seconds=30),
            restored=started + timedelta(seconds=60),
            restored_ok=True,
        )


# ---------------------------------------------------------------- 正常路径


def test_baseline_produces_complete_pack(tmp_path: Path) -> None:
    write_stream(tmp_path, "dn1", "hadoop-hdfs-datanode-dn1.log", [LINE_1, LINE_2, LINE_3])
    report = make_collector(tmp_path).collect()

    paths = pack_paths(tmp_path, "inc_dnvol_01")
    assert is_complete(paths)
    assert report.records == 3
    assert report.raw_line_count == 3
    assert report.parse_failed == 0
    assert report.mode is CollectMode.BASELINE
    assert len(read_logs(paths)) == 3


def test_manifest_counters_match_the_records(tmp_path: Path) -> None:
    write_stream(tmp_path, "dn1", "hadoop-hdfs-datanode-dn1.log", [LINE_1, LINE_2, LINE_3])
    make_collector(tmp_path).collect()

    manifest = read_manifest(pack_paths(tmp_path, "inc_dnvol_01"))
    assert manifest.counters.lines == 3
    assert manifest.counters.raw_lines == 3
    assert manifest.counters.by_level == {"INFO": 1, "WARN": 1, "ERROR": 1}
    assert manifest.counters.by_component == {"dn1": 3}
    assert manifest.counters.parse_failed == 0


def test_records_are_sorted_by_timestamp(tmp_path: Path) -> None:
    """两个组件的采集顺序是 dn1 → nn，但落盘必须按时间排。"""
    write_stream(tmp_path, "dn1", "hadoop-hdfs-datanode-dn1.log", [LINE_3])
    write_stream(
        tmp_path,
        "nn",
        "hadoop-hdfs-namenode-nn.log",
        ["2026-11-22 10:00:00,500 INFO  NameNode:1 - 更早的一行"],
    )
    make_collector(tmp_path, components=("dn1", "nn")).collect()

    records = read_logs(pack_paths(tmp_path, "inc_dnvol_01"))
    assert [record.component for record in records] == ["nn", "dn1"]
    assert records == sorted(records, key=lambda record: record.sort_key())


def test_ids_are_prefixed_by_component_and_start_at_one(tmp_path: Path) -> None:
    write_stream(tmp_path, "dn1", "hadoop-hdfs-datanode-dn1.log", [LINE_1, LINE_2])
    write_stream(tmp_path, "nn", "hadoop-hdfs-namenode-nn.log", [LINE_3])
    make_collector(tmp_path, components=("dn1", "nn")).collect()

    ids = [record.id for record in read_logs(pack_paths(tmp_path, "inc_dnvol_01"))]
    assert set(ids) == {"dn1:00001", "dn1:00002", "nn:00001"}


def test_line_numbers_do_not_leak_between_components(tmp_path: Path) -> None:
    """`line_no` 是组件内编号：dn1 采了 2 行，nn 也必须从 1 开始。"""
    write_stream(tmp_path, "dn1", "hadoop-hdfs-datanode-dn1.log", [LINE_1, LINE_2])
    write_stream(tmp_path, "nn", "hadoop-hdfs-namenode-nn.log", [LINE_3])
    make_collector(tmp_path, components=("dn1", "nn")).collect()

    manifest = read_manifest(pack_paths(tmp_path, "inc_dnvol_01"))
    assert manifest.checkpoint.adapter_state["dn1:main"].line_no == 2
    assert manifest.checkpoint.adapter_state["nn:main"].line_no == 1


def test_origin_is_collected_and_profile_recorded(tmp_path: Path) -> None:
    write_stream(tmp_path, "dn1", "hadoop-hdfs-datanode-dn1.log", [LINE_1])
    make_collector(tmp_path).collect()

    manifest = read_manifest(pack_paths(tmp_path, "inc_dnvol_01"))
    assert manifest.origin.kind is OriginKind.COLLECTED
    assert manifest.canonical_profile is CanonicalProfile.HDFS_LOG4J
    assert manifest.sources[0].adapter is Adapter.FILE


def test_empty_component_produces_an_empty_but_valid_pack(tmp_path: Path) -> None:
    """容器还没起、日志目录是空的：这也是一次合法的采集（0 行）。"""
    (tmp_path / "dn1").mkdir()
    report = make_collector(tmp_path).collect()

    paths = pack_paths(tmp_path, "inc_dnvol_01")
    assert is_complete(paths)
    assert report.records == 0
    assert read_manifest(paths).counters.lines == 0


def test_parse_failed_is_counted_not_dropped(tmp_path: Path) -> None:
    write_stream(tmp_path, "dn1", "hadoop-hdfs-datanode-dn1.log", [LINE_1, LINE_BAD_LEVEL])
    report = make_collector(tmp_path).collect()

    records = read_logs(pack_paths(tmp_path, "inc_dnvol_01"))
    assert report.parse_failed == 1
    assert any(record.parse_status.value != "ok" for record in records)


# ---------------------------------------------------------------- 脱敏


def test_redaction_counts_land_in_manifest(tmp_path: Path) -> None:
    write_stream(tmp_path, "dn1", "hadoop-hdfs-datanode-dn1.log", [LINE_IP])
    make_collector(tmp_path, redactor=PatternRedactor()).collect()

    manifest = read_manifest(pack_paths(tmp_path, "inc_dnvol_01"))
    assert manifest.counters.redacted.get("ip") == 1
    assert manifest.redact_policy.version == PatternRedactor().version
    assert "10.1.2.3" not in read_logs(pack_paths(tmp_path, "inc_dnvol_01"))[0].message


def test_redact_policy_records_none_when_no_redactor(tmp_path: Path) -> None:
    write_stream(tmp_path, "dn1", "hadoop-hdfs-datanode-dn1.log", [LINE_1])
    make_collector(tmp_path, redactor=None).collect()

    manifest = read_manifest(pack_paths(tmp_path, "inc_dnvol_01"))
    assert manifest.redact_policy.version == "none"
    assert manifest.redact_policy.rules == REDACT_RULES
    assert manifest.counters.redacted == {}


# ---------------------------------------------------------------- 窗口


def test_timestamp_outside_window_raises(tmp_path: Path) -> None:
    write_stream(tmp_path, "dn1", "hadoop-hdfs-datanode-dn1.log", [LINE_1, LINE_OLD])
    with pytest.raises(ValueError, match="落在窗口"):
        make_collector(tmp_path).collect()


def test_window_check_can_be_turned_off(tmp_path: Path) -> None:
    """真实采集时，日志文件开头总有窗口之前的老行 —— 得能关掉这条检查。"""
    write_stream(tmp_path, "dn1", "hadoop-hdfs-datanode-dn1.log", [LINE_1, LINE_OLD])
    report = make_collector(tmp_path).collect(check_window=False)
    assert report.records == 2


# ---------------------------------------------------------------- 中断与续采


def test_finalize_removes_the_checkpoint_sidecar(tmp_path: Path) -> None:
    write_stream(tmp_path, "dn1", "hadoop-hdfs-datanode-dn1.log", [LINE_1])
    make_collector(tmp_path).collect()

    paths = pack_paths(tmp_path, "inc_dnvol_01")
    assert not (paths.dir / CHECKPOINT_FILE).exists()
    # checkpoint 已经并进 manifest，信息没丢。
    assert paths.manifest.is_file()
    assert read_manifest(paths).checkpoint.adapter_state["dn1:main"].offset > 0


def test_interrupted_collect_leaves_a_checkpoint(tmp_path: Path) -> None:
    write_stream(tmp_path, "dn1", "hadoop-hdfs-datanode-dn1.log", [LINE_1, LINE_2])
    write_stream(tmp_path, "dn2", "hadoop-hdfs-datanode-dn2.log", [LINE_3])

    broken = make_collector(
        tmp_path,
        components=("dn1", "dn2"),
        sources=[
            source_for(tmp_path, "dn1"),
            SourceSpec(
                component="dn2",
                adapter=BoomAdapter(FileAdapter(tmp_path, hdfs_streams(("dn2",)))),
                ref="fixture",
            ),
        ],
    )
    with pytest.raises(RuntimeError, match="容器挂了"):
        broken.collect()

    paths = pack_paths(tmp_path, "inc_dnvol_01")
    assert not paths.manifest.exists()
    payload = json.loads((paths.dir / CHECKPOINT_FILE).read_text(encoding="utf-8"))
    assert payload["schema"] == "collect-checkpoint/v1"
    assert payload["raw_line_count"] == 2
    assert payload["raw_sha256"].startswith("sha256:")
    assert payload["adapter_state"]["dn1:main"]["line_no"] == 2


def test_attach_resumes_and_matches_one_shot(tmp_path: Path) -> None:
    """核心不变式：中断 + attach 采完 ≡ 一次采到底。"""
    # 一次采到底
    one_shot = tmp_path / "one-shot"
    write_stream(one_shot, "dn1", "hadoop-hdfs-datanode-dn1.log", [LINE_1, LINE_2])
    write_stream(one_shot, "dn2", "hadoop-hdfs-datanode-dn2.log", [LINE_3])
    make_collector(one_shot, components=("dn1", "dn2")).collect()
    expected = read_manifest(pack_paths(one_shot, "inc_dnvol_01"))

    # 采到第二个组件时炸掉，再用 attach 接着采
    resumed = tmp_path / "resumed"
    write_stream(resumed, "dn1", "hadoop-hdfs-datanode-dn1.log", [LINE_1, LINE_2])
    write_stream(resumed, "dn2", "hadoop-hdfs-datanode-dn2.log", [LINE_3])
    broken = make_collector(
        resumed,
        components=("dn1", "dn2"),
        sources=[
            source_for(resumed, "dn1"),
            SourceSpec(
                component="dn2",
                adapter=BoomAdapter(FileAdapter(resumed, hdfs_streams(("dn2",)))),
                ref="fixture",
            ),
        ],
    )
    with pytest.raises(RuntimeError):
        broken.collect()

    report = make_collector(resumed, components=("dn1", "dn2")).collect(mode=CollectMode.ATTACH)
    paths = pack_paths(resumed, "inc_dnvol_01")
    actual = read_manifest(paths)

    # `report.records` 是**本次**写入的条数（attach 这轮只补了 dn2 那 1 条），
    # pack 里最终有多少条要看 logs.jsonl。
    assert report.records == 1
    assert report.raw_line_count == 1
    assert [record.id for record in read_logs(paths)] == [
        record.id for record in read_logs(pack_paths(one_shot, "inc_dnvol_01"))
    ]
    assert actual.hashes.canonical_sha256 == expected.hashes.canonical_sha256
    assert actual.hashes.input_fingerprint == expected.hashes.input_fingerprint
    assert actual.counters == expected.counters
    assert is_complete(paths)


def test_first_component_crash_still_leaves_a_checkpoint(tmp_path: Path) -> None:
    """第一个组件就读崩，一个组件都没跑完 —— 也得能 attach。

    否则"容器还没起好"这种最常见的失败，会把一个可续采的现场判成不可续采。
    """
    write_stream(tmp_path, "dn1", "hadoop-hdfs-datanode-dn1.log", [LINE_1, LINE_2])
    broken = make_collector(
        tmp_path,
        sources=[
            SourceSpec(
                component="dn1",
                adapter=BoomAdapter(FileAdapter(tmp_path, hdfs_streams(("dn1",)))),
                ref="fixture",
            )
        ],
    )
    with pytest.raises(RuntimeError, match="容器挂了"):
        broken.collect()

    paths = pack_paths(tmp_path, "inc_dnvol_01")
    payload = json.loads((paths.dir / CHECKPOINT_FILE).read_text(encoding="utf-8"))
    assert payload["raw_line_count"] == 0
    assert payload["adapter_state"] == {}

    report = make_collector(tmp_path).collect(mode=CollectMode.ATTACH)
    assert report.records == 2
    assert len(read_logs(paths)) == 2


def test_interrupted_component_leaves_no_partial_records(tmp_path: Path) -> None:
    """硬不变式：checkpoint 指向哪，logs.jsonl 里就只能有到哪的记录。"""
    write_stream(tmp_path, "dn1", "hadoop-hdfs-datanode-dn1.log", [LINE_1, LINE_2, LINE_3])
    broken = make_collector(
        tmp_path,
        sources=[
            SourceSpec(
                component="dn1",
                adapter=BoomAdapter(FileAdapter(tmp_path, hdfs_streams(("dn1",)))),
                ref="fixture",
            )
        ],
    )
    with pytest.raises(RuntimeError):
        broken.collect()

    paths = pack_paths(tmp_path, "inc_dnvol_01")
    assert read_logs(paths) == []


def test_attach_without_checkpoint_raises(tmp_path: Path) -> None:
    write_stream(tmp_path, "dn1", "hadoop-hdfs-datanode-dn1.log", [LINE_1])
    with pytest.raises(CollectError, match="没有 checkpoint.json"):
        make_collector(tmp_path).collect(mode=CollectMode.ATTACH)


def test_attach_refuses_a_finished_pack(tmp_path: Path) -> None:
    write_stream(tmp_path, "dn1", "hadoop-hdfs-datanode-dn1.log", [LINE_1])
    make_collector(tmp_path).collect()

    paths = pack_paths(tmp_path, "inc_dnvol_01")
    # 完成的 pack 没有 sidecar，attach 会先撞上"没有 checkpoint"，
    # 这里手工补一个，验证的是"已完成就不许再 attach"这条判断。
    (paths.dir / CHECKPOINT_FILE).write_text("{}", encoding="utf-8")
    with pytest.raises(CollectError, match="已经是完成的 pack"):
        make_collector(tmp_path).collect(mode=CollectMode.ATTACH)


def test_baseline_refuses_to_overwrite_a_finished_pack(tmp_path: Path) -> None:
    write_stream(tmp_path, "dn1", "hadoop-hdfs-datanode-dn1.log", [LINE_1])
    make_collector(tmp_path).collect()
    with pytest.raises(CasePackExists):
        make_collector(tmp_path).collect()


# ---------------------------------------------------------------- 注入模式


def test_inject_flushes_baseline_before_running_the_hook(tmp_path: Path) -> None:
    target = write_stream(tmp_path, "dn1", "hadoop-hdfs-datanode-dn1.log", [LINE_1, LINE_2])
    hook = RecordingHook(target, [LINE_3])

    make_collector(tmp_path).collect(mode=CollectMode.INJECT, hook=hook)

    assert hook.calls == 1
    assert hook.seen_lines_when_flushed == 2  # flush 之后、注入之前，baseline 已落盘


def test_inject_second_pass_picks_up_the_fault_segment(tmp_path: Path) -> None:
    target = write_stream(tmp_path, "dn1", "hadoop-hdfs-datanode-dn1.log", [LINE_1])
    hook = RecordingHook(target, [LATER_1])

    report = make_collector(tmp_path).collect(mode=CollectMode.INJECT, hook=hook)
    records = read_logs(pack_paths(tmp_path, "inc_dnvol_01"))

    assert report.records == 2
    assert [record.id for record in records] == ["dn1:00001", "dn1:00002"]
    assert records[-1].level.value == "ERROR"


def test_inject_records_the_injection_into_labels(tmp_path: Path) -> None:
    target = write_stream(tmp_path, "dn1", "hadoop-hdfs-datanode-dn1.log", [LINE_1])
    hook = RecordingHook(target, [LATER_1])
    make_collector(tmp_path).collect(mode=CollectMode.INJECT, hook=hook)

    labels = read_labels(pack_paths(tmp_path, "inc_dnvol_01"))
    assert labels.fault_injection is not None
    assert labels.fault_injection.script == "inject/disk_full.sh"
    assert labels.fault_injection.restored_ok is True


def test_inject_requires_a_hook(tmp_path: Path) -> None:
    write_stream(tmp_path, "dn1", "hadoop-hdfs-datanode-dn1.log", [LINE_1])
    with pytest.raises(CollectError, match="inject 模式必须给 hook"):
        make_collector(tmp_path).collect(mode=CollectMode.INJECT)


def test_non_inject_mode_rejects_a_hook(tmp_path: Path) -> None:
    write_stream(tmp_path, "dn1", "hadoop-hdfs-datanode-dn1.log", [LINE_1])
    hook = RecordingHook(tmp_path / "nope.log", [])
    with pytest.raises(CollectError, match="不该有 hook"):
        make_collector(tmp_path).collect(hook=hook)


def test_hook_must_return_a_fault_injection(tmp_path: Path) -> None:
    write_stream(tmp_path, "dn1", "hadoop-hdfs-datanode-dn1.log", [LINE_1])

    class BadHook:
        name = "bad"

        def __call__(self, flush: Any) -> Any:
            flush()
            return "not-a-record"

    with pytest.raises(CollectError, match="必须返回 FaultInjection"):
        make_collector(tmp_path).collect(mode=CollectMode.INJECT, hook=BadHook())


# ---------------------------------------------------------------- 构造期校验


def test_labels_case_id_must_match(tmp_path: Path) -> None:
    with pytest.raises(CollectError, match="labels.case_id"):
        Collector(
            tmp_path,
            case_id="inc_a",
            profile=CanonicalProfile.HDFS_LOG4J,
            window=WINDOW,
            labels=labels_for("inc_b"),
            sources=[source_for(tmp_path, "dn1")],
        )


def test_duplicate_component_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(CollectError, match="同一个组件声明了两次"):
        make_collector(tmp_path, components=("dn1", "dn1"))


def test_no_sources_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(CollectError, match="至少要有一个来源"):
        Collector(
            tmp_path,
            case_id="inc_a",
            profile=CanonicalProfile.HDFS_LOG4J,
            window=WINDOW,
            labels=labels_for("inc_a"),
            sources=[],
        )


# ---------------------------------------------------------------- 两条路径等价（V4）


def test_canonical_sha_is_adapter_independent(tmp_path: Path) -> None:
    """file 与内存 adapter 读同一批文本 → 同一个 canonical_sha256。"""
    write_stream(tmp_path / "file", "dn1", "hadoop-hdfs-datanode-dn1.log", [LINE_1, LINE_2, LINE_3])
    make_collector(tmp_path / "file").collect()

    stream = FakeStream("main")
    stream.append([LINE_1, LINE_2, LINE_3])
    Collector(
        tmp_path / "fake",
        case_id="inc_dnvol_01",
        profile=CanonicalProfile.HDFS_LOG4J,
        window=WINDOW,
        labels=labels_for("inc_dnvol_01"),
        sources=[SourceSpec(component="dn1", adapter=FakeAdapter("dn1", [stream]), ref="fixture")],
    ).collect()

    left = read_manifest(pack_paths(tmp_path / "file", "inc_dnvol_01"))
    right = read_manifest(pack_paths(tmp_path / "fake", "inc_dnvol_01"))
    assert left.hashes.canonical_sha256 == right.hashes.canonical_sha256


# ---------------------------------------------------------------- 哈希链


def test_raw_hasher_matches_a_manual_chain() -> None:
    import hashlib

    hasher = RawHasher()
    for text in ("a", "b"):
        hasher.update(text)

    chain = hashlib.sha256(b"aiops-log-copilot/raw-lines/v1").digest()
    for text in ("a", "b"):
        chain = hashlib.sha256(chain + b"\n" + text.encode("utf-8")).digest()
    assert hasher.hexdigest == "sha256:" + chain.hex()


def test_raw_hasher_chain_is_resumable() -> None:
    """这是"中断 + 续采 ≡ 一次采到底"的数学基础。"""
    one_shot = RawHasher()
    for text in ("a", "b", "c"):
        one_shot.update(text)

    first = RawHasher()
    for text in ("a", "b"):
        first.update(text)
    resumed = RawHasher(first.hexdigest)
    resumed.update("c")

    assert resumed.hexdigest == one_shot.hexdigest


def test_raw_hasher_rejects_a_broken_chain() -> None:
    with pytest.raises(ValueError, match="链值长度"):
        RawHasher("sha256:abc")


# ---------------------------------------------------------------- anchors（原文指针）
#
# `anchors/index.json` 是 P-A §5 承诺的东西：pack 里**不复制原文**，只存
# "这条记录的原话在哪个文件的哪几个字节"。所以这些测试的重点不是"文件里有
# 几条"，而是"按指针回去读，读到的确实是那行原话"。


def test_anchor_pointers_cover_only_evidence_lines(tmp_path: Path) -> None:
    stream = write_stream(tmp_path, "dn1", "hadoop-hdfs-datanode-dn1.log", [LINE_1, LINE_2, LINE_3])
    make_collector(tmp_path).collect()

    anchors = read_anchors(pack_paths(tmp_path, "inc_dnvol_01"))
    # LINE_1 是 INFO：它是样本的主体，但没人会为一条正常日志回看原文。
    assert [anchor.id for anchor in anchors] == ["dn1:00002", "dn1:00003"]

    data = stream.read_bytes()
    for anchor, line in zip(anchors, (LINE_2, LINE_3), strict=True):
        assert anchor.file == str(stream)
        raw = (line + "\n").encode()
        assert anchor.length == len(raw)
        assert data[anchor.offset : anchor.offset + anchor.length] == raw
        assert anchor.sha256 == f"sha256:{hashlib.sha256(raw).hexdigest()}"


def test_a_pack_with_only_info_lines_has_no_anchors(tmp_path: Path) -> None:
    write_stream(tmp_path, "dn1", "hadoop-hdfs-datanode-dn1.log", [LINE_1])
    make_collector(tmp_path).collect()

    assert read_anchors(pack_paths(tmp_path, "inc_dnvol_01")) == []


def test_attach_keeps_the_anchors_of_the_first_pass(tmp_path: Path) -> None:
    """中断重采后，第一轮已经证明过的证据行不能"忘掉"。

    指针只在 finalize 写一次 `index.json`，而中断时 finalize 还没跑 —— 所以
    指针必须活在 checkpoint.json 里，否则 attach 会把它们丢掉。丢掉是**静默**的：
    `entries: []` 也是合法 pack，没有任何报错。
    """
    write_stream(tmp_path, "dn1", "hadoop-hdfs-datanode-dn1.log", [LINE_2])
    write_stream(tmp_path, "dn2", "hadoop-hdfs-datanode-dn2.log", [LINE_3])

    broken = make_collector(
        tmp_path,
        components=("dn1", "dn2"),
        sources=[
            source_for(tmp_path, "dn1"),
            SourceSpec(
                component="dn2",
                adapter=BoomAdapter(FileAdapter(tmp_path, hdfs_streams(("dn2",)))),
                ref="fixture",
            ),
        ],
    )
    with pytest.raises(RuntimeError, match="容器挂了"):
        broken.collect()
    paths = pack_paths(tmp_path, "inc_dnvol_01")
    assert not paths.anchors_index.exists()  # finalize 还没跑

    make_collector(tmp_path, components=("dn1", "dn2")).collect(mode=CollectMode.ATTACH)
    # dn1 的 WARN 来自第一轮（当时只存进了 checkpoint），dn2 的 ERROR 来自第二轮。
    assert [anchor.id for anchor in read_anchors(paths)] == ["dn1:00001", "dn2:00001"]
