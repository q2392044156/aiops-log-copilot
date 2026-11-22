#!/usr/bin/env python
# 顺序 5b 的 CLI：给一次"现在的集群长什么样"的快照。
"""打印/写入一次指标快照（`metrics.jsonl` 的一批行）。

用途有两个：

1. **健康快照**：注入前后各跑一次，`dfsadmin -report` 的 Live datanodes 从 3 变 2
   再回到 3，就是 V7「注入可逆」的证据；
2. **手工检查**：采集跑完之后想确认"那一刻指标到底记了什么"，不用去翻 jsonl。

`--out` 不写就只打印（不落盘），写了就追加进那个文件。

    uv run python scripts/snapshot_metrics.py
    uv run python scripts/snapshot_metrics.py --out runtime/metrics-probe.jsonl
"""

from __future__ import annotations

import argparse
import sys
from datetime import UTC, datetime
from pathlib import Path

_BACKEND = Path(__file__).resolve().parent.parent / "apps" / "backend" / "src"
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

from logcopilot.collect.metrics import (  # noqa: E402
    DfSampler,
    DockerStatsSampler,
    HdfsReportSampler,
    MetricPoint,
    MetricsWriter,
    docker_runner,
)

CONTAINERS = {"nn": "alcp-nn", "dn1": "alcp-dn1", "dn2": "alcp-dn2", "dn3": "alcp-dn3"}


def collect_points(now: datetime) -> list[MetricPoint]:
    points: list[MetricPoint] = []
    points += DockerStatsSampler(docker_runner, CONTAINERS).sample(now)
    points += HdfsReportSampler(docker_runner).sample(now)
    points += DfSampler(docker_runner, {k: v for k, v in CONTAINERS.items() if k != "nn"}).sample(
        now
    )
    return points


def _summary(points: list[MetricPoint]) -> list[str]:
    lines: list[str] = []
    for point in points:
        label = f"{point.kind:<13} {point.component:<4}"
        if point.error:
            lines.append(f"{label} ✗ {point.error[:110]}")
            continue
        pairs = "  ".join(f"{key}={value}" for key, value in point.values.items())
        lines.append(f"{label} {pairs}")
    return lines


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="采一次 HDFS 集群指标快照")
    parser.add_argument("--out", type=Path, default=None, help="追加写入这个 jsonl（不写就只打印）")
    args = parser.parse_args(argv)

    now = datetime.now(UTC)
    points = collect_points(now)
    for line in _summary(points):
        print(line)

    errors = [point for point in points if point.error]
    print(f"\n共 {len(points)} 条，其中错误 {len(errors)} 条")

    if args.out is not None:
        with MetricsWriter(args.out) as writer:
            writer.append(points)
            writer.flush()
        print(f"已追加到 {args.out}")

    # 指标全错说明 docker 或集群根本没起，让调用方（make / 脚本）能感知到。
    return 1 if len(errors) == len(points) else 0


if __name__ == "__main__":
    raise SystemExit(main())
