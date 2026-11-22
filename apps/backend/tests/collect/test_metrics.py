"""指标采样（顺序 5b）的契约测试。

重点不是"解析对不对"，而是三条约束：
1. 采不到要留证据（error 行），不能静默丢；
2. 单位不能混（kB 与 KiB 差 2.4%）；
3. 采样节奏由 `MetricsSession` 决定，测试里喂假时间，不 sleep。
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import pytest

from logcopilot.collect.metrics import (
    DEFAULT_INTERVALS,
    DfSampler,
    DockerStatsSampler,
    HdfsReportSampler,
    MetricPoint,
    MetricsSession,
    MetricsWriter,
    parse_size,
)

T0 = datetime(2026, 11, 22, 12, 0, 0, tzinfo=UTC)
CONTAINERS = {"nn": "alcp-nn", "dn1": "alcp-dn1", "dn2": "alcp-dn2", "dn3": "alcp-dn3"}


class FakeDocker:
    """按 argv 前缀回放预置输出，并记录每次调用。"""

    def __init__(self, replies: dict[str, tuple[int, str]]) -> None:
        self.replies = replies
        self.calls: list[tuple[str, ...]] = []

    def __call__(self, argv):  # type: ignore[no-untyped-def]
        self.calls.append(tuple(argv))
        joined = " ".join(argv)
        for key, reply in self.replies.items():
            if key in joined:
                return reply
        return 1, f"unexpected command: {joined}"


# --------------------------------------------------------------------------- 单位


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("0B", 0.0),
        ("1.2kB", 1200.0),
        ("210.5MiB", 210.5 * 2.0**20),
        ("7.657GiB", 7.657 * 2.0**30),
        ("1.00 TB", 1e12),
    ],
)
def test_parse_size_handles_decimal_and_binary_units(text: str, expected: float) -> None:
    assert parse_size(text) == pytest.approx(expected)


def test_parse_size_returns_none_for_garbage() -> None:
    assert parse_size("--") is None
    assert parse_size("12 parsecs") is None


# ------------------------------------------------------------------------ MetricPoint


def test_metric_point_serializes_utc_millis() -> None:
    point = MetricPoint(ts=T0, kind="df", component="dn1", values={"size_bytes": 134217728})
    payload = json.loads(point.to_json_line())
    assert payload["ts"] == "2026-11-22T12:00:00.000+00:00"
    assert payload["kind"] == "df"
    assert payload["values"] == {"size_bytes": 134217728}
    assert "error" not in payload


def test_metric_point_rejects_naive_ts() -> None:
    with pytest.raises(ValueError, match="时区"):
        MetricPoint(ts=datetime(2026, 11, 22, 12, 0, 0), kind="df", component="dn1")


@pytest.mark.parametrize("kind", ["", "cpu", "DOCKER_STATS"])
def test_metric_point_rejects_unknown_kind(kind: str) -> None:
    with pytest.raises(ValueError, match="kind"):
        MetricPoint(ts=T0, kind=kind, component="dn1")


def test_metric_point_rejects_unknown_component() -> None:
    with pytest.raises(ValueError, match="component"):
        MetricPoint(ts=T0, kind="df", component="namenode")


def test_metric_point_refuses_error_and_values_together() -> None:
    with pytest.raises(ValueError, match="不能同时存在"):
        MetricPoint(ts=T0, kind="df", component="dn1", values={"a": 1}, error="炸了")


# ------------------------------------------------------------------- docker stats


def test_docker_stats_parses_every_container() -> None:
    lines = [
        json.dumps(
            {
                "Name": "alcp-nn",
                "CPUPerc": "0.35%",
                "MemUsage": "512.5MiB / 7.657GiB",
                "MemPerc": "6.54%",
                "NetIO": "1.2kB / 3.4kB",
                "BlockIO": "0B / 8.19MB",
                "PIDs": "42",
            }
        ),
        json.dumps({"Name": "alcp-dn1", "CPUPerc": "1.00%", "MemUsage": "0B / 0B"}),
    ]
    sampler = DockerStatsSampler(FakeDocker({"docker stats": (0, "\n".join(lines))}), CONTAINERS)
    points = {point.component: point for point in sampler.sample(T0)}

    assert points["nn"].values["cpu_pct"] == pytest.approx(0.35)
    assert points["nn"].values["mem_used_bytes"] == pytest.approx(512.5 * 2.0**20)
    assert points["nn"].values["mem_pct"] == pytest.approx(6.54)
    assert points["nn"].values["pids"] == 42
    # 输出里没有的容器必须留 error 行，而不是消失
    assert points["dn2"].error
    assert points["dn3"].error


def test_docker_stats_command_failure_marks_every_container() -> None:
    runner = FakeDocker({"docker stats": (1, "Cannot connect to the Docker daemon")})
    points = DockerStatsSampler(runner, CONTAINERS).sample(T0)
    assert len(points) == 4
    assert all(point.error for point in points)


# ------------------------------------------------------------------- dfsadmin -report

REPORT = """\
Configured Capacity: 134217728 (128 MB)
DFS Remaining: 100000000 (95.37 MB)
DFS Used: 34217728 (32.63 MB)
Under replicated blocks: 3
Missing blocks: 0
-------------------------------------------------
Live datanodes (2):

Name: 172.20.0.4:9866 (dn1)
"""


def test_hdfs_report_parses_counts_and_safemode() -> None:
    runner = FakeDocker(
        {
            "dfsadmin -report": (0, REPORT),
            "dfsadmin -safemode get": (0, "Safe mode is OFF\n"),
        }
    )
    points = HdfsReportSampler(runner, namenode="alcp-nn").sample(T0)
    by_kind = {point.kind: point for point in points}
    assert by_kind["hdfs_report"].values["capacity_bytes"] == 134217728
    assert by_kind["hdfs_report"].values["under_replicated_blocks"] == 3
    assert by_kind["hdfs_report"].values["live_datanodes"] == 2
    assert "dead_datanodes" not in by_kind["hdfs_report"].values  # 输出里没有就不瞎填
    assert by_kind["safemode"].values["state"] == "OFF"


def test_hdfs_report_failure_is_recorded_not_raised() -> None:
    runner = FakeDocker({"dfsadmin -report": (1, "Connection refused")})
    points = HdfsReportSampler(runner).sample(T0)
    assert len(points) == 1
    assert "Connection refused" in points[0].error


# ------------------------------------------------------------------------- df


def test_df_parses_data_disk() -> None:
    out = (
        "Filesystem     1B-blocks      Used Available Use% Mounted on\n"
        "tmpfs          134217728  34217728 100000000  26% /tmp/hadoop-root/dfs/data\n"
    )
    runner = FakeDocker({"df -B1": (0, out)})
    points = DfSampler(runner, {"dn1": "alcp-dn1"}).sample(T0)
    assert points[0].values["size_bytes"] == 134217728
    assert points[0].values["avail_bytes"] == 100000000
    assert points[0].values["use_pct"] == pytest.approx(25.49)


def test_df_failure_is_recorded() -> None:
    runner = FakeDocker({"df -B1": (1, "no such container")})
    points = DfSampler(runner, {"dn1": "alcp-dn1"}).sample(T0)
    assert points[0].error
    assert points[0].values == {}


# ---------------------------------------------------------------------- writer


def test_writer_appends_lines_with_lf(tmp_path: Path) -> None:
    path = tmp_path / "metrics.jsonl"
    with MetricsWriter(path) as writer:
        writer.append(
            [
                MetricPoint(ts=T0, kind="df", component="dn1", values={"a": 1}),
                MetricPoint(ts=T0, kind="df", component="dn2", error="停了"),
            ]
        )
        writer.flush()
    raw = path.read_bytes()
    assert raw.count(b"\n") == 2
    assert b"\r\n" not in raw
    assert json.loads(raw.decode("utf-8").splitlines()[1])["error"] == "停了"


# --------------------------------------------------------------------- session


class _StubSampler:
    def __init__(self) -> None:
        self.calls = 0

    def sample(self, now: datetime) -> list[MetricPoint]:
        self.calls += 1
        return [MetricPoint(ts=now, kind="df", component="dn1", values={"n": self.calls})]


def test_session_respects_intervals() -> None:
    stats, report = _StubSampler(), _StubSampler()
    session = MetricsSession({"docker_stats": stats, "hdfs_report": report})

    session.tick(T0)  # 首次：都到期
    assert stats.calls == 1
    assert report.calls == 1

    session.tick(T0 + timedelta(seconds=5))  # 5 秒：只有 docker_stats
    assert stats.calls == 2
    assert report.calls == 1

    session.tick(T0 + timedelta(seconds=30))  # 30 秒：两个都到期
    assert stats.calls == 3
    assert report.calls == 2


def test_session_rejects_unknown_kind() -> None:
    with pytest.raises(ValueError, match="不在"):
        MetricsSession({"cpu": _StubSampler()})


def test_session_rejects_sampler_without_sample() -> None:
    with pytest.raises(ValueError, match="sample"):
        MetricsSession({"df": cast(Any, object())})


def test_default_intervals_match_plan() -> None:
    assert DEFAULT_INTERVALS["docker_stats"] == timedelta(seconds=5)
    assert DEFAULT_INTERVALS["df"] == timedelta(seconds=30)
