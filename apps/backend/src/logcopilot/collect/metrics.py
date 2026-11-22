# 顺序 5b · 指标采样：日志之外的第二种证据（docker stats / dfsadmin -report / df -h）
"""指标快照：`metrics.jsonl` 的一行。

日志证明"报错了"，指标证明"错到什么程度、还在不在"。两类故障只能靠指标定论：

- `DN_VOLUME`：日志只说 `DiskOutOfSpaceException`，只有 `df` 能说明数据盘还剩多少字节；
- `DN_HEARTBEAT`：日志里没有"DN 死了"这句话，只有 `dfsadmin -report` 的
  `Live datanodes (2)` 能证明。

三条设计约束（都不是随手定的）：

1. **指标不进任何哈希**。`input_fingerprint` / `canonical_sha256` 只覆盖日志：
   指标是环境快照，CPU 抖一下、内存差 1MB 都会变，进哈希就等于"这个 pack 永远不可复现"。
2. **一行一个 `(ts, kind, component)`**，不是一个时间点一行。查询时按组件过滤、
   按时间画曲线都只需要读一列，不用在内存里再拆。
3. **采不到也要记一行**。容器被 `docker stop` / `pause` 时 `docker stats` 会失败，
   而这恰好是 `DN_HEARTBEAT` 最需要证据的时刻 —— 静默丢样本等于把最关键的证据扔掉。
   失败行带 `error` 字段，`values` 为空。
"""

from __future__ import annotations

import json
import os
import re
import subprocess
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Protocol

from ..domain.log import COMPONENTS, format_ts, require_utc

__all__ = [
    "DEFAULT_INTERVALS",
    "METRIC_KINDS",
    "DockerStatsSampler",
    "DfSampler",
    "HdfsReportSampler",
    "MetricPoint",
    "MetricsSession",
    "MetricsWriter",
    "docker_runner",
    "parse_size",
]

#: `kind` 的闭集。加新采样器时必须同时加到这里，否则 `MetricPoint` 直接拒收。
METRIC_KINDS: tuple[str, ...] = ("docker_stats", "hdfs_report", "safemode", "df")

#: 采样周期。5 秒的容器指标与 30 秒的 HDFS 报告，是 P-A §6.1 定下的。
DEFAULT_INTERVALS: Mapping[str, timedelta] = {
    "docker_stats": timedelta(seconds=5),
    "hdfs_report": timedelta(seconds=30),
    "safemode": timedelta(seconds=30),
    "df": timedelta(seconds=30),
}

#: 跑一条命令 → `(退出码, stdout)`。测试用假工厂顶掉，不碰真 docker。
Runner = Callable[[Sequence[str]], "tuple[int, str]"]


class Sampler(Protocol):
    """一个采样器：给一个时刻，吐出一批指标行。"""

    def sample(self, now: datetime) -> list[MetricPoint]: ...


def docker_runner(argv: Sequence[str], *, timeout: float = 60.0) -> tuple[int, str]:
    """默认 runner：真跑子进程。

    退出码非零**不抛异常** —— 采样器要把它变成一条带 `error` 的指标行，
    而不是让整次采集因为"某台容器挂了"而中断。
    """
    try:
        proc = subprocess.run(  # noqa: S603 - 命令全部由本模块拼出，不经过 shell
            list(argv),
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:  # 连 docker 都起不来
        return 127, f"{type(exc).__name__}: {exc}"
    return proc.returncode, proc.stdout if proc.returncode == 0 else (proc.stderr or proc.stdout)


_SIZE_UNITS = {
    "B": 1.0,
    "KB": 1e3,
    "MB": 1e6,
    "GB": 1e9,
    "TB": 1e12,
    "KIB": 2.0**10,
    "MIB": 2.0**20,
    "GIB": 2.0**30,
    "TIB": 2.0**40,
}
_SIZE_RE = re.compile(r"([0-9]+(?:\.[0-9]+)?)\s*([A-Za-z]+)")


def parse_size(text: str) -> float | None:
    """`"210.5MiB"` → 字节数；`"0B"` → `0.0`；`"1.2kB"` → `1200.0`。

    单位在 HDFS 与 `docker stats` 里混用十进（kB）与二进制（KiB），
    这两种在数值上差 2.4%，不会有人注意到，但画容量曲线时会差出一条线。
    """
    match = _SIZE_RE.search(text.strip())
    if match is None:
        return None
    number, unit = float(match.group(1)), match.group(2).upper()
    scale = _SIZE_UNITS.get(unit)
    return None if scale is None else number * scale


def _split_pair(text: str) -> tuple[float | None, float | None]:
    """`"1.2kB / 3.4kB"` → `(1200.0, 3400.0)`。"""
    left, _, right = text.partition("/")
    return parse_size(left), parse_size(right)


def _pct(text: str) -> float | None:
    match = re.search(r"([0-9]+(?:\.[0-9]+)?)", text)
    return None if match is None else float(match.group(1))


@dataclass(frozen=True, slots=True)
class MetricPoint:
    """一个时间点的一个指标样本。"""

    ts: datetime
    kind: str
    component: str
    #: 值允许 `None`：`docker stats` 的某一列解析不出来时留 `None`，
    #: 比瞎填 0 诚实（0% CPU 与"没读到"是两回事）。
    values: Mapping[str, float | int | str | None] = field(default_factory=dict)
    error: str = ""

    def __post_init__(self) -> None:
        require_utc(self.ts, "MetricPoint.ts")
        if self.kind not in METRIC_KINDS:
            raise ValueError(f"kind 必须是 {METRIC_KINDS} 之一，收到 {self.kind!r}")
        if self.component not in COMPONENTS:
            raise ValueError(f"component 必须是 {COMPONENTS} 之一，收到 {self.component!r}")
        if self.error and self.values:
            # 采到值又报错，等于说不清这一行到底可不可信；宁可只留错误。
            raise ValueError("error 与 values 不能同时存在（要么采到了，要么没采到）")

    def to_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "ts": format_ts(self.ts),
            "kind": self.kind,
            "component": self.component,
            "values": dict(self.values),
        }
        if self.error:
            payload["error"] = self.error
        return payload

    def to_json_line(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, separators=(",", ":")) + "\n"


class MetricsWriter:
    """往 `metrics.jsonl` 追加（先 flush 再 fsync，与 `LogWriter` 同一套语义）。"""

    def __init__(self, path: Path) -> None:
        self._path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        self._handle = path.open("w", encoding="utf-8", newline="\n")

    def append(self, points: Iterable[MetricPoint]) -> int:
        count = 0
        for point in points:
            self._handle.write(point.to_json_line())
            count += 1
        return count

    def flush(self) -> None:
        self._handle.flush()
        os.fsync(self._handle.fileno())

    def close(self) -> None:
        self._handle.close()

    def __enter__(self) -> MetricsWriter:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


class DockerStatsSampler:
    """`docker stats --no-stream --format '{{json .}}'` —— 一次调用覆盖全部容器。"""

    def __init__(self, runner: Runner, containers: Mapping[str, str]) -> None:
        # component → 容器名。反过来存一份，报告里只出现容器名。
        self._runner = runner
        self._by_container = {name: comp for comp, name in containers.items()}

    def sample(self, now: datetime) -> list[MetricPoint]:
        code, out = self._runner(["docker", "stats", "--no-stream", "--format", "{{json .}}"])
        if code != 0:
            # 整条命令失败（docker 没起、没有权限）：每个容器各记一条错误，
            # 不然"四台全黑"和"只黑了 docker"看起来一样。
            return [
                MetricPoint(
                    ts=now,
                    kind="docker_stats",
                    component=comp,
                    error=f"docker stats 失败: {out.strip()[:200]}",
                )
                for comp in self._by_container.values()
            ]
        points: list[MetricPoint] = []
        seen: set[str] = set()
        for line in out.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            name = str(payload.get("Name", ""))
            component = self._by_container.get(name)
            if component is None:
                continue
            seen.add(component)
            mem_used, mem_limit = _split_pair(str(payload.get("MemUsage", "")))
            net_in, net_out = _split_pair(str(payload.get("NetIO", "")))
            blk_in, blk_out = _split_pair(str(payload.get("BlockIO", "")))
            points.append(
                MetricPoint(
                    ts=now,
                    kind="docker_stats",
                    component=component,
                    values={
                        "cpu_pct": _pct(str(payload.get("CPUPerc", ""))),
                        "mem_used_bytes": mem_used,
                        "mem_limit_bytes": mem_limit,
                        "mem_pct": _pct(str(payload.get("MemPerc", ""))),
                        "net_in_bytes": net_in,
                        "net_out_bytes": net_out,
                        "block_in_bytes": blk_in,
                        "block_out_bytes": blk_out,
                        "pids": int(payload.get("PIDs", 0))
                        if str(payload.get("PIDs", "")).isdigit()
                        else None,
                    },
                )
            )
        # 容器停掉之后 `docker stats` 里就没有它了 —— 这本身是证据，补一条错误行。
        for comp in self._by_container.values():
            if comp not in seen:
                points.append(
                    MetricPoint(
                        ts=now,
                        kind="docker_stats",
                        component=comp,
                        error="docker stats 输出里没有这个容器（已停止或被删除）",
                    )
                )
        return points


_REPORT_FIELDS: tuple[tuple[str, str], ...] = (
    ("capacity_bytes", r"Configured Capacity:\s*(\d+)"),
    ("dfs_used_bytes", r"^DFS Used:\s*(\d+)"),
    ("dfs_remaining_bytes", r"DFS Remaining:\s*(\d+)"),
    ("under_replicated_blocks", r"Under replicated blocks:\s*(\d+)"),
    ("missing_blocks", r"^Missing blocks:\s*(\d+)"),
    ("live_datanodes", r"Live datanodes \((\d+)\)"),
    ("dead_datanodes", r"Dead datanodes \((\d+)\)"),
)


class HdfsReportSampler:
    """`hdfs dfsadmin -report` —— DN 存活数、副本不足块数，只能从这里读。"""

    def __init__(self, runner: Runner, *, namenode: str = "alcp-nn") -> None:
        self._runner = runner
        self._namenode = namenode

    def sample(self, now: datetime) -> list[MetricPoint]:
        code, out = self._runner(["docker", "exec", self._namenode, "hdfs", "dfsadmin", "-report"])
        if code != 0:
            return [
                MetricPoint(
                    ts=now,
                    kind="hdfs_report",
                    component="nn",
                    error=f"dfsadmin -report 失败: {out.strip()[:200]}",
                )
            ]
        values: dict[str, float | int | str | None] = {}
        for key, pattern in _REPORT_FIELDS:
            match = re.search(pattern, out, re.MULTILINE)
            if match is not None:
                values[key] = int(match.group(1))
        # 安全模式单列一个 kind：它 30 秒才变一次，和容量/存活数不是一回事。
        safe_code, safe_out = self._runner(
            ["docker", "exec", self._namenode, "hdfs", "dfsadmin", "-safemode", "get"]
        )
        safemode = ""
        if safe_code == 0:
            lowered = safe_out.lower()
            safemode = (
                "ON"
                if "safe mode is on" in lowered
                else "OFF"
                if "safe mode is off" in lowered
                else ""
            )
        points = []
        if values:
            points.append(MetricPoint(ts=now, kind="hdfs_report", component="nn", values=values))
        else:
            points.append(
                MetricPoint(
                    ts=now,
                    kind="hdfs_report",
                    component="nn",
                    error="report 输出里没解析出任何字段",
                )
            )
        if safemode:
            points.append(
                MetricPoint(ts=now, kind="safemode", component="nn", values={"state": safemode})
            )
        return points


class DfSampler:
    """逐台 DataNode 的 `df -B1 <数据盘>` —— `DN_VOLUME` 的唯一硬证据。"""

    def __init__(
        self,
        runner: Runner,
        containers: Mapping[str, str],
        *,
        path: str = "/tmp/hadoop-root/dfs/data",
    ) -> None:
        self._runner = runner
        self._containers = dict(containers)
        self._path = path

    def sample(self, now: datetime) -> list[MetricPoint]:
        points: list[MetricPoint] = []
        for component, container in self._containers.items():
            code, out = self._runner(["docker", "exec", container, "df", "-B1", self._path])
            if code != 0:
                points.append(
                    MetricPoint(
                        ts=now,
                        kind="df",
                        component=component,
                        error=f"df 失败（数据盘可能已经不可用）: {out.strip()[:200]}",
                    )
                )
                continue
            rows = [row.split() for row in out.strip().splitlines() if row.strip()]
            if len(rows) < 2 or len(rows[1]) < 4:
                points.append(
                    MetricPoint(ts=now, kind="df", component=component, error="df 输出行数不对")
                )
                continue
            size, used, avail = (int(rows[1][i]) for i in (1, 2, 3))
            points.append(
                MetricPoint(
                    ts=now,
                    kind="df",
                    component=component,
                    values={
                        "size_bytes": size,
                        "used_bytes": used,
                        "avail_bytes": avail,
                        "use_pct": round(used * 100.0 / size, 2) if size else None,
                    },
                )
            )
        return points


class MetricsSession:
    """按周期决定"这一拍该采哪些"。

    只做判定不发线程：`tick(now)` 返回本次到期的采样点，调用方（采集循环）
    自己决定什么时候 sleep、什么时候 flush。这样测试里可以喂任意时间，
    不用等 30 秒。
    """

    def __init__(
        self,
        samplers: Mapping[str, Sampler],
        *,
        intervals: Mapping[str, timedelta] = DEFAULT_INTERVALS,
    ) -> None:
        for kind, sampler in samplers.items():
            if kind not in METRIC_KINDS:
                raise ValueError(f"采样器 {kind!r} 不在 {METRIC_KINDS} 里")
            if not callable(getattr(sampler, "sample", None)):
                raise ValueError(f"采样器 {type(sampler).__name__} 没有 sample() 方法")
        self._samplers = dict(samplers)
        self._intervals = {kind: intervals[kind] for kind in samplers}
        self._last: dict[str, datetime] = {}

    def due(self, now: datetime, kind: str) -> bool:
        last = self._last.get(kind)
        return last is None or now - last >= self._intervals[kind]

    def tick(self, now: datetime) -> list[MetricPoint]:
        points: list[MetricPoint] = []
        for kind, sampler in self._samplers.items():
            if not self.due(now, kind):
                continue
            self._last[kind] = now
            points.extend(sampler.sample(now))
        return points
