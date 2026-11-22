"""跟随模式（`follow_until`）与指标接线的测试。

第 15 步之前，`collect()` 只 drain 一轮：把"当下已有的日志"读完就完事。
真实采集要的是"盯着日志看 5 分钟"，而且这 5 分钟里指标要按 5s/30s 一直采。
这两件事都必须能在测试里被钉住，否则第一次真采就会掉进两个坑：

1. 只采到了采集器启动那一刻的存量日志，5 分钟的窗口里后面长出来的全丢；
2. `metrics.jsonl` 是 0 字节，事后没人能证明"那会儿 DN 真的死了"。

这里用**真的 `FileAdapter` + 真的文件**，让 `sleeper` 在"睡"的时候往文件里
追加一行 —— 这是最接近真实 tail 的做法，又完全确定（不依赖真实时间）。
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from logcopilot.collect.adapters.file import FileAdapter, FileStream
from logcopilot.collect.collect import CollectError, CollectMode, Collector, SourceSpec
from logcopilot.collect.metrics import MetricPoint, MetricsSession
from logcopilot.domain.case import (
    Adapter,
    CanonicalProfile,
    FaultInjection,
    Labels,
    Phase,
    PhaseWindow,
    RootCause,
    Window,
)
from logcopilot.domain.log import ParseStatus

T0 = datetime(2026, 11, 22, 10, 0, 0, tzinfo=UTC)
LINE = "[aiops-lab-dn1] 2026-11-22 10:00:00 INFO  FsDatasetImpl:425 - 第 {n} 条\n"


class FakeClock:
    """可以手动推进的时钟：跟随模式的每一轮都必须有确定的"现在"。"""

    def __init__(self, start: datetime) -> None:
        self.now = start

    def __call__(self) -> datetime:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += timedelta(seconds=seconds)


class StubSampler:
    """假的 `docker stats` 采样器：只数被调了几次。"""

    def __init__(self) -> None:
        self.calls = 0

    def sample(self, now: datetime) -> list[MetricPoint]:
        self.calls += 1
        return [MetricPoint(ts=now, kind="docker_stats", component="dn1", values={"cpu_pct": 1.0})]


def _log(root: Path, n: int) -> Path:
    directory = root / "dn1"
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / "hadoop-hdfs-datanode-dn1.log"
    path.write_text("".join(LINE.format(n=i) for i in range(n)), encoding="utf-8", newline="\n")
    return path


def _collector(
    root: Path,
    clock: FakeClock,
    *,
    until: datetime,
    metrics: MetricsSession | None = None,
    start_at_end: bool = False,
) -> Collector:
    # 日志目录是 pack 目录的**兄弟**（`<data>/dn1/...` 与 `<data>/cases/<case>/`）：
    # 采集器只读日志目录，pack 写在自己的根下，两者不能混在一起。
    data_root = root.parent
    adapter = FileAdapter(
        data_root, (FileStream("dn1", "main", 0, "dn1/hadoop-hdfs-datanode-*.log"),)
    )
    return Collector(
        root,
        case_id="base_healthy_01",
        profile=CanonicalProfile.LAB,
        window=Window(since=T0, until=until),
        labels=Labels(case_id="base_healthy_01"),
        sources=(SourceSpec(component="dn1", adapter=adapter, ref="test", path=str(root)),),
        clock=clock,
        metrics=metrics,
        start_at_end=start_at_end,
    )


def _sleep_advancing(clock: FakeClock, path: Path) -> Any:
    """返回一个"sleeper"：推进假时钟，同时往日志里追加一行。"""

    def sleep(seconds: float) -> None:
        clock.advance(seconds)
        _append(path, 2)

    return sleep


def _append(path: Path, n: int) -> None:
    with path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(LINE.format(n=n))


def _records(root: Path) -> list[dict[str, Any]]:
    text = (root / "base_healthy_01" / "logs.jsonl").read_text(encoding="utf-8")
    return [json.loads(line) for line in text.splitlines() if line.strip()]


# ------------------------------------------------------------------ 跟随模式


def test_follow_keeps_draining_until_deadline(tmp_path: Path) -> None:
    path = _log(tmp_path / "logs", 2)
    clock = FakeClock(T0)
    until = T0 + timedelta(seconds=6)
    appended = 0

    def sleep(seconds: float) -> None:
        # "睡"的时候日志还在长 —— 这正是跟随模式要捕捉的东西。
        nonlocal appended
        appended += 1
        _append(path, appended + 2)
        clock.advance(seconds)

    collector = _collector(tmp_path / "logs" / "cases", clock, until=until)
    report = collector.collect(follow_until=until, poll_interval=2.0, sleeper=sleep)

    # 第 0 秒读到 2 条；之后三次 sleep 各追加一条并再读一轮 → 5 条。
    assert report.records == 5
    assert report.raw_line_count == 5
    assert clock.now >= until


def test_follow_stops_when_deadline_already_passed(tmp_path: Path) -> None:
    _log(tmp_path / "logs", 2)
    clock = FakeClock(T0 + timedelta(seconds=10))
    sleeps: list[float] = []

    collector = _collector(tmp_path / "logs" / "cases", clock, until=T0 + timedelta(seconds=10))
    report = collector.collect(
        follow_until=T0 + timedelta(seconds=10), poll_interval=2.0, sleeper=sleeps.append
    )

    assert report.records == 2  # 时间已到，仍读一轮把存量收干净
    assert sleeps == []  # 但不再 sleep


def test_follow_rejects_deadline_beyond_window(tmp_path: Path) -> None:
    _log(tmp_path / "logs", 1)
    clock = FakeClock(T0)
    collector = _collector(tmp_path / "logs" / "cases", clock, until=T0 + timedelta(seconds=5))

    with pytest.raises(CollectError, match="超出窗口上界"):
        collector.collect(follow_until=T0 + timedelta(seconds=6))


def test_follow_rejects_non_positive_interval(tmp_path: Path) -> None:
    _log(tmp_path / "logs", 1)
    clock = FakeClock(T0)
    collector = _collector(tmp_path / "logs" / "cases", clock, until=T0 + timedelta(seconds=5))

    with pytest.raises(CollectError, match="poll_interval"):
        collector.collect(follow_until=T0 + timedelta(seconds=5), poll_interval=0)


def test_follow_advances_checkpoint_each_round(tmp_path: Path) -> None:
    """每轮都落 checkpoint：中途 Ctrl-C 只丢最后一轮。"""
    path = _log(tmp_path / "logs", 1)
    root = tmp_path / "logs" / "cases"
    clock = FakeClock(T0)
    until = T0 + timedelta(seconds=4)
    seen: list[int] = []

    def sleep(seconds: float) -> None:
        checkpoint = root / "base_healthy_01" / "checkpoint.json"
        if checkpoint.is_file():
            seen.append(json.loads(checkpoint.read_text(encoding="utf-8"))["raw_line_count"])
        _append(path, len(seen) + 1)
        clock.advance(seconds)

    collector = _collector(root, clock, until=until)
    collector.collect(follow_until=until, poll_interval=2.0, sleeper=sleep)

    assert seen == [1, 2]  # 每次 sleep 之前盘上的进度各是 1 条、2 条


# ------------------------------------------------------------------ 指标接线


def test_metrics_are_written_while_following(tmp_path: Path) -> None:
    _log(tmp_path / "logs", 1)
    root = tmp_path / "logs" / "cases"
    clock = FakeClock(T0)
    until = T0 + timedelta(seconds=4)
    sampler = StubSampler()
    session = MetricsSession({"docker_stats": sampler}, intervals={"docker_stats": timedelta(0)})

    collector = _collector(root, clock, until=until, metrics=session)
    collector.collect(follow_until=until, poll_interval=2.0, sleeper=clock.advance)

    text = (root / "base_healthy_01" / "metrics.jsonl").read_text(encoding="utf-8")
    rows = [json.loads(line) for line in text.splitlines() if line.strip()]
    assert rows, "跟随模式必须写出指标，否则故障段的指标是空白"
    assert {row["kind"] for row in rows} == {"docker_stats"}
    assert sampler.calls == len(rows)


def test_metrics_file_stays_empty_without_session(tmp_path: Path) -> None:
    _log(tmp_path / "logs", 1)
    root = tmp_path / "logs" / "cases"
    clock = FakeClock(T0)

    collector = _collector(root, clock, until=T0 + timedelta(seconds=2))
    collector.collect(
        follow_until=T0 + timedelta(seconds=2), poll_interval=2.0, sleeper=clock.advance
    )

    assert (root / "base_healthy_01" / "metrics.jsonl").stat().st_size == 0


def test_metrics_do_not_change_the_hashes(tmp_path: Path) -> None:
    """指标不进任何哈希：同样的日志，采两次 canonical_sha256 必须一样。"""
    root = tmp_path / "logs" / "cases"
    _log(tmp_path / "logs", 3)

    def run(with_metrics: bool) -> str:
        clock = FakeClock(T0)
        session = None
        if with_metrics:
            session = MetricsSession(
                {"docker_stats": StubSampler()},
                intervals={"docker_stats": timedelta(0)},
            )
        collector = _collector(root, clock, until=T0 + timedelta(seconds=2), metrics=session)
        report = collector.collect(
            follow_until=T0 + timedelta(seconds=2), poll_interval=2.0, sleeper=clock.advance
        )
        return report.manifest.hashes.canonical_sha256

    # 第二次要覆盖，所以先删目录（`create_pack` 拒绝覆盖，这是对的）。
    first = run(with_metrics=False)
    import shutil

    shutil.rmtree(root / "base_healthy_01")
    second = run(with_metrics=True)

    assert first == second


# --------------------------------------------------------------- 注入 + 跟随


class _TickingHook:
    """假 hook：故障段里反复调 `flush`，模拟 `CommandInjection` 的行为。"""

    name = "inject/fake"

    def __init__(self, clock: FakeClock, ticks: int) -> None:
        self._clock = clock
        self._ticks = ticks
        self.progress_calls = 0

    def __call__(self, flush: Any) -> Any:
        from logcopilot.domain.case import FaultInjection

        started = self._clock()
        for _ in range(self._ticks):
            self._clock.advance(1)
            flush()
            self.progress_calls += 1
        ended = self._clock()
        return FaultInjection(
            script=self.name, started=started, ended=ended, restored=ended, restored_ok=True
        )


def test_inject_keeps_collecting_metrics_during_the_fault(tmp_path: Path) -> None:
    path = _log(tmp_path / "logs", 1)
    root = tmp_path / "logs" / "cases"
    clock = FakeClock(T0)
    session = MetricsSession(
        {"docker_stats": StubSampler()}, intervals={"docker_stats": timedelta(0)}
    )
    hook = _TickingHook(clock, ticks=5)

    collector = _collector(root, clock, until=T0 + timedelta(minutes=1), metrics=session)
    report = collector.collect(
        mode=CollectMode.INJECT,
            hook=hook,
        follow_until=T0 + timedelta(minutes=1),
        poll_interval=2.0,
        sleeper=_sleep_advancing(clock, path),
    )

    assert hook.progress_calls == 5
    text = (root / "base_healthy_01" / "metrics.jsonl").read_text(encoding="utf-8")
    rows = [json.loads(line) for line in text.splitlines() if line.strip()]
    assert len(rows) > 5  # 注入前 1 拍 + 故障段 5 拍 + 恢复段若干
    assert report.injection is not None
    assert report.injection.restored_ok is True
    labels = json.loads((root / "base_healthy_01" / "labels.json").read_text(encoding="utf-8"))
    assert labels["fault_injection"]["script"] == "inject/fake"


def test_inject_hook_must_return_fault_injection(tmp_path: Path) -> None:
    _log(tmp_path / "logs", 1)
    clock = FakeClock(T0)

    class _BadHook:
        name = "inject/bad"

        def __call__(self, flush: Any) -> Any:
            return "not a FaultInjection"

    collector = _collector(tmp_path / "logs" / "cases", clock, until=T0 + timedelta(seconds=1))
    with pytest.raises(CollectError, match="必须返回 FaultInjection"):
        collector.collect(mode=CollectMode.INJECT, hook=_BadHook())


def test_parse_status_closed_set_is_used_by_records(tmp_path: Path) -> None:
    """顺手确认这一批记录真的走了 canonicalize（而不是被伪造出来）。"""
    _log(tmp_path / "logs", 2)
    root = tmp_path / "logs" / "cases"
    clock = FakeClock(T0)
    collector = _collector(root, clock, until=T0 + timedelta(seconds=2))
    collector.collect(
        follow_until=T0 + timedelta(seconds=2), poll_interval=2.0, sleeper=clock.advance
    )

    rows = _records(root)
    assert len(rows) == 2
    assert {row["parse_status"] for row in rows} == {ParseStatus.OK.value}
    assert [row["id"] for row in rows] == ["dn1:00001", "dn1:00002"]


# --------------------------------------------------------------- 从末尾开始采


def test_start_at_end_ignores_existing_lines(tmp_path: Path) -> None:
    """基线采集从 EOF 起读：已经跑过的启动日志不能算进这 5 分钟。"""
    path = _log(tmp_path / "logs", 3)  # 3 条都发生在采集开始之前
    root = tmp_path / "logs" / "cases"
    clock = FakeClock(T0)
    until = T0 + timedelta(seconds=2)

    def sleep(seconds: float) -> None:
        _append(path, 99)
        clock.advance(seconds)

    collector = _collector(root, clock, until=until, start_at_end=True)
    report = collector.collect(follow_until=until, poll_interval=2.0, sleeper=sleep)

    assert report.records == 1
    assert [row["id"] for row in _records(root)] == ["dn1:00004"]


def test_start_at_end_keeps_physical_line_numbers(tmp_path: Path) -> None:
    """编号接在文件已有行数之后：`dn1:00004` 而不是重新从 1 开始。"""
    _log(tmp_path / "logs", 3)
    adapter = FileAdapter(
        tmp_path / "logs", (FileStream("dn1", "main", 0, "dn1/hadoop-hdfs-datanode-*.log"),)
    )

    states = adapter.prime()

    (state,) = states.values()
    assert state.line_no == 3
    assert (
        state.offset == (tmp_path / "logs" / "dn1" / "hadoop-hdfs-datanode-dn1.log").stat().st_size
    )
    assert state.tail_line_sha1 is not None


def test_start_at_end_rejects_adapter_without_prime(tmp_path: Path) -> None:
    """不支持 prime 的适配器必须**显式报错**，而不是悄悄从头读一遍。"""

    class _NoPrime:
        adapter = Adapter.FILE

        def streams(self) -> tuple[()]:
            return ()

        def open(self, spec: Any) -> Any:
            raise NotImplementedError

    clock = FakeClock(T0)
    collector = Collector(
        tmp_path / "cases",
        case_id="base_healthy_01",
        profile=CanonicalProfile.LAB,
        window=Window(since=T0, until=T0 + timedelta(seconds=1)),
        labels=Labels(case_id="base_healthy_01"),
        sources=(SourceSpec(component="dn1", adapter=_NoPrime(), ref="test"),),
        clock=clock,
        start_at_end=True,
    )

    with pytest.raises(CollectError, match="不支持 start_at_end"):
        collector.collect(follow_until=T0 + timedelta(seconds=1), sleeper=clock.advance)


# ------------------------------------------------------ 窗口上界的硬边界


def test_records_written_after_the_window_are_dropped(tmp_path: Path) -> None:
    """收尾那一轮读到的、时间戳越过 `until` 的记录必须被丢掉。

    真实故障来自 20 分钟的那次采集：`_follow` 在 `now >= until` 前还会 drain
    一轮，那一轮读到了 `until` 之后写出来的几行，于是
    `checkpoint.last_ts > window.until`，manifest 直接把整次采集判成
    "采到了窗口外的数据"。这里把那个现场压成 4 秒。
    """
    path = _log(tmp_path / "logs", 2)
    clock = FakeClock(T0)
    until = T0 + timedelta(seconds=4)

    def sleep(seconds: float) -> None:
        clock.advance(seconds)
        # 睡醒时已经越过窗口上界，日志里又长出一条 10:00:09 的行
        with path.open("a", encoding="utf-8", newline="\n") as handle:
            handle.write("[aiops-lab-dn1] 2026-11-22 10:00:09 INFO  FsDatasetImpl:425 - 窗口外\n")

    collector = _collector(tmp_path / "logs" / "cases", clock, until=until)
    report = collector.collect(follow_until=until, poll_interval=4.0, sleeper=sleep)

    # 窗口内 2 条留下，窗口外 1 条丢账。
    assert report.records == 2
    assert report.dropped_out_of_window == 1
    # 关键：manifest 不再被"晚了几秒"顶掉，且 last_ts 卡在窗口内。
    last_ts = report.manifest.checkpoint.last_ts
    assert last_ts is not None and last_ts <= until
    assert all(row["message"] != "窗口外" for row in _records(tmp_path / "logs" / "cases"))


def test_inject_starts_only_at_the_fault_phase_boundary(tmp_path: Path) -> None:
    """注入不能落在 baseline 里 —— 必须等 fault 段的起点。

    这条是实采踩出来的：第一版 hook 在采集一开始就被调用，于是"写满数据盘"
    发生在 baseline 段，5 分钟 baseline 里全程是坏盘、恢复段也全程是坏盘，
    快照里根本没有"由好变坏"的转折点。
    """
    path = _log(tmp_path / "logs", 1)
    clock = FakeClock(T0)
    fault_since = T0 + timedelta(seconds=6)
    until = T0 + timedelta(seconds=12)
    adapter = FileAdapter(
        tmp_path / "logs", (FileStream("dn1", "main", 0, "dn1/hadoop-hdfs-datanode-*.log"),)
    )
    collector = Collector(
        tmp_path / "cases",
        case_id="inc_dnvol_01",
        profile=CanonicalProfile.LAB,
        window=Window(
            since=T0,
            until=until,
            phases=(
                PhaseWindow(Phase.BASELINE, T0, fault_since),
                PhaseWindow(Phase.FAULT, fault_since, until),
            ),
        ),
        labels=Labels(case_id="inc_dnvol_01", root_cause=RootCause.DN_VOLUME),
        sources=(SourceSpec(component="dn1", adapter=adapter, ref="test"),),
        clock=clock,
    )

    seen: list[datetime] = []

    class Hook:
        """最小 `InjectionHook`：只记下"我被调用的那一刻"。"""

        name = "test"

        def __call__(self, flush: Any) -> FaultInjection:
            seen.append(clock())
            now = clock()
            return FaultInjection(script="test", started=now, ended=now, restored=now)

    collector.collect(
        mode=CollectMode.INJECT,
        hook=Hook(),
        follow_until=until,
        poll_interval=1.0,
        sleeper=_sleep_advancing(clock, path),
    )
    assert seen, "hook 必须被调用"
    assert seen[0] >= fault_since, f"注入发生在 {seen[0]}，早于 fault 段起点 {fault_since}"
