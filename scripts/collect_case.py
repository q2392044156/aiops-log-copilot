# 顺序 5d · 采集用例的入口：把 collect.py 的零件接成"一条命令采完一个病例"
"""采一个病例，落成 `data/cases/<case_id>/` 下的一个完整 CasePack。

这是 P-A 从"能读日志"走到"能采出可用语料"的那一步，做三件事：

1. **跟随**（`follow_until`）：窗口内每 2 秒 drain 一次，而不是读完一轮就收工。
   不跟随的话，5 分钟窗口里后面长出来的日志全部丢失。
2. **采样指标**（`MetricsSession`）：`docker stats` 5 秒一次，
   `dfsadmin -report` + `df` 30 秒一次，写进 `metrics.jsonl`。
3. **从末尾起采**（`start_at_end=True`）：起采前先 `prime()` 把每条流的起点挪到
   "现在"，否则集群启动时的 22 KB classpath（STARTUP_MSG）会被算进这 5 分钟。

用法（正常态样本）：

    python scripts/collect_case.py --case base_healthy_01 --minutes 5

注入样本（基线 5 分钟 → 注入 10 分钟 → 恢复 5 分钟 = 20 分钟）：

    python scripts/collect_case.py --case inc_dnvol_01 --inject dn_volume \\
        --root-cause DN_VOLUME --minutes 5 --fault-minutes 10 --recovery-minutes 5

`--dry-run` 只打印注入剧本、不碰集群（仍会采日志，用来验证管线本身）。
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "apps" / "backend" / "src"))

from logcopilot.collect.adapters.file import FileAdapter, hdfs_streams  # noqa: E402
from logcopilot.collect.collect import (  # noqa: E402
    CollectMode,
    Collector,
    CollectReport,
    SourceSpec,
)
from logcopilot.collect.inject import INJECTIONS, CommandInjection  # noqa: E402
from logcopilot.collect.metrics import (  # noqa: E402
    DfSampler,
    DockerStatsSampler,
    HdfsReportSampler,
    MetricsSession,
    docker_runner,
)
from logcopilot.collect.redact import PatternRedactor  # noqa: E402
from logcopilot.domain.case import (  # noqa: E402
    CanonicalProfile,
    Labels,
    Phase,
    PhaseWindow,
    RootCause,
    Window,
)

COMPONENTS = ("nn", "dn1", "dn2", "dn3")
CONTAINERS = {component: f"alcp-{component}" for component in COMPONENTS}
LOGS_ROOT = REPO_ROOT / "runtime" / "hdfs-logs"
CASES_ROOT = REPO_ROOT / "data" / "cases"
WORKLOAD_PATH = "/lab/bin/workload.sh"
STOP_PATH = "/opt/hadoop/logs/.workload-stop"


def _sources() -> tuple[SourceSpec, ...]:
    """每个"日志目录已经出现"的组件一条来源。

    缺目录的组件直接跳过：容器可能还没起来，而这不该让整次采集失败。
    """
    found = []
    for component in COMPONENTS:
        adapter = FileAdapter(LOGS_ROOT, hdfs_streams((component,)))
        if not adapter.streams():
            print(f"[warn] {component} 没有可采的日志文件，跳过（容器起来了吗？）")
            continue
        found.append(SourceSpec(component=component, adapter=adapter, ref=str(component)))
    return tuple(found)


def _metrics_session() -> MetricsSession:
    """5 秒一次容器指标，30 秒一次 HDFS 集群指标。"""
    return MetricsSession(
        {
            "docker_stats": DockerStatsSampler(docker_runner, CONTAINERS),
            "hdfs_report": HdfsReportSampler(docker_runner, namenode=CONTAINERS["nn"]),
            "df": DfSampler(docker_runner, {c: CONTAINERS[c] for c in ("dn1", "dn2", "dn3")}),
        }
    )


def _workload(action: str) -> None:
    """后台起停读写负载。

    空载集群几乎不写日志 —— 没有负载的"正常样本"会是一段死水，
    P-B 的模板挖掘和 P-C 的上下文都无从谈起。

    停机是**协作式**的：先建哨兵文件让循环自己退出（当前那次 `hdfs dfs -put`
    会写完），等 15 秒还没退才 `pkill`。直接 kill 会掐断写管线，DN 立刻打
    `DataXceiver error processing WRITE_BLOCK operation` 的 ERROR —— 实测这样
    会在故障窗口外凭空多出 3 条 ERROR，把 V6 的门槛顶成噪声。
    """
    if action == "start":
        argv = ["docker", "exec", "-d", CONTAINERS["nn"], WORKLOAD_PATH]
        code, out = docker_runner(argv)
        if code != 0:
            print(f"[warn] 负载启动失败（{code}）：{out.strip()[:200]}")
        return

    docker_runner(["docker", "exec", CONTAINERS["nn"], "touch", STOP_PATH])
    for _ in range(15):
        probe = ["docker", "exec", CONTAINERS["nn"], "pgrep", "-f", WORKLOAD_PATH]
        code, _out = docker_runner(probe)
        if code != 0:  # pgrep 找不到进程 = 已经干净退出
            return
        time.sleep(1)
    print("[warn] 负载 15 秒内没退出，强制结束（这一轮可能有截断写入）")
    docker_runner(["docker", "exec", CONTAINERS["nn"], "pkill", "-f", WORKLOAD_PATH])


def _overrides(items: list[str]) -> dict[str, str]:
    """把 `--config-override log_level=DEBUG` 这类写法解析成字典。

    严格拒绝没有等号、键为空、重复键的写法：这些错误在生成的 manifest 里
    会表现成"少了一条覆盖"或"后写的静默盖掉前面的"，而 manifest 是事后解释
    数据差异的唯一依据 —— 不能靠猜。
    """
    parsed: dict[str, str] = {}
    for item in items:
        key, sep, value = item.partition("=")
        key = key.strip()
        if not sep or not key or not value.strip():
            raise SystemExit(f"[error] --config-override 要吃 KEY=VALUE，收到 {item!r}")
        if key in parsed:
            raise SystemExit(f"[error] --config-override 的键重复了: {key}")
        parsed[key] = value.strip()
    return parsed


def _describe(report: CollectReport) -> str:
    rows = [f"  case_id        {report.case_id}"]
    # counts/hashes 的真相在 manifest 里：report 只报"这一轮"的数字，
    # 全量统计（含续采累加）必须看 manifest，别在这里重新算一遍。
    counters = report.manifest.counters
    rows.append(f"  记录数         {report.records}")
    rows.append(f"  原始行数       {report.raw_line_count}")
    # counters.parse_failed 只数 failed（真的一个字都认不出来）。两者分开报：
    # DEBUG 档位下 partial 一千多条是常态（Java 栈被正确并进上一条），若混报成
    # "解析失败"，会让人误以为解析器坏了。
    rows.append(f"  解析异常       {report.parse_failed} 条（partial+failed）")
    if counters.parse_failed:
        rows.append(f"  其中硬失败     {counters.parse_failed} 条（failed，原文见 raw）")
    rows.append(f"  level 分布     {dict(counters.by_level)}")
    rows.append(f"  component 分布 {dict(counters.by_component)}")
    rows.append(f"  脱敏计数       {dict(counters.redacted)}")
    rows.append(f"  canonical      {report.manifest.hashes.canonical_sha256}")
    rows.append(f"  fingerprint    {report.manifest.hashes.input_fingerprint}")
    # 窗口外被丢掉的记录：>0 说明收尾那一轮读到了窗口上界之后的几秒。
    # 不是错误（窗口是硬边界），但要让人看见"丢了几个"，而不是猜。
    if report.dropped_out_of_window:
        rows.append(
            f"  窗口外丢弃     {report.dropped_out_of_window} 条"
            "（收尾那一轮读到了 until 之后写出的日志，已按硬边界丢弃）"
        )
    return "\n".join(rows)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="采一个病例并写成 CasePack v1")
    parser.add_argument("--case", required=True, help="病例 id，例如 base_healthy_01")
    parser.add_argument("--minutes", type=float, default=5.0, help="基线时长（分钟）")
    parser.add_argument("--inject", choices=sorted(INJECTIONS), default=None)
    # 同一个变体换个目标容器，就是矩阵里的另一例（`inc_hb_02` 停 dn2、
    # `inc_hb_03` pause dn1……）。不写就用构造器的默认目标。
    parser.add_argument(
        "--inject-target",
        default=None,
        help="注入目标容器（如 alcp-dn1）；不写用该变体的默认目标",
    )
    parser.add_argument("--root-cause", default=None, help="根因码（正常例留空）")
    parser.add_argument("--fault-minutes", type=float, default=10.0)
    parser.add_argument("--recovery-minutes", type=float, default=5.0)
    parser.add_argument("--root", default=str(CASES_ROOT), help="case 根目录")
    # 1 秒一轮：跟随越快，指标节拍越接近 5 秒（每轮 drain 只读增量，代价与新增行数成正比）。
    # 2 秒一轮时实测 docker_stats 平均间隔约 6.7 秒 —— 比 P-A §6.1 要求的 5 秒稀。
    parser.add_argument("--poll-interval", type=float, default=1.0)
    # 这一例的现场与同族不一样时（典型：O4 的 DEBUG 对照例把 log4j 调到 DEBUG），
    # 必须把差异写进 manifest —— 否则同一族里"行数差二十倍"没法解释，
    # 也没法复现。键值随便写，`config_overrides` 不进任何哈希。
    parser.add_argument(
        "--config-override",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="写进 manifest.config_overrides，可重复（如 --config-override log_level=DEBUG）",
    )
    parser.add_argument("--dry-run", action="store_true", help="只打印注入剧本，不碰集群")
    parser.add_argument("--no-workload", action="store_true", help="不跑读写负载")
    args = parser.parse_args(argv)

    # `uv run --directory apps/backend` 会把子进程的 cwd 变成 apps/backend，
    # 所以相对路径一律按**仓库根**解析 —— 否则 pack 会悄悄落到 apps/backend/data 下。
    root = Path(args.root)
    if not root.is_absolute():
        root = REPO_ROOT / root

    sources = _sources()
    if not sources:
        print("[error] 一个来源都没找到。先 `cd infra/hdfs && docker compose up -d`。")
        return 2

    now = datetime.now(UTC).replace(microsecond=0)
    baseline = timedelta(minutes=args.minutes)
    since = now
    if args.inject is None:
        baseline_end = now + baseline
        until = baseline_end
        phases = (PhaseWindow(Phase.BASELINE, since, until),)
    else:
        baseline_end = now + baseline
        fault_end = baseline_end + timedelta(minutes=args.fault_minutes)
        until = fault_end + timedelta(minutes=args.recovery_minutes)
        phases = (
            PhaseWindow(Phase.BASELINE, since, baseline_end),
            PhaseWindow(Phase.FAULT, baseline_end, fault_end),
            PhaseWindow(Phase.RECOVERY, fault_end, until),
        )

    # 正常例 `root_cause=None`；`RootCause(...)` 会拒绝闭集外的字符串（大写拼错也别放过）。
    labels = Labels(
        case_id=args.case,
        root_cause=RootCause(args.root_cause) if args.root_cause is not None else None,
        reviewer="collect_case.py",
    )

    collector = Collector(
        root,
        case_id=args.case,
        profile=CanonicalProfile.HDFS_LOG4J,
        # 三段窗口必须显式声明：`Collector` 拿 `window.phases[fault].since` 当
        # 注入的触发点。少了它，`window.phase(FAULT)` 返回 None，注入会退化成
        # "采满整个窗口之后再动手" —— 故障段整段落在窗口之外（2026-11-22 实采
        # 踩到：`fault_injection.started` 比 `window.until` 还晚 1 秒）。
        window=Window(since=since, until=until, phases=phases),
        labels=labels,
        sources=sources,
        redactor=PatternRedactor(),
        metrics=_metrics_session(),
        start_at_end=True,
        config_overrides=_overrides(args.config_override),
    )

    mode, hook = CollectMode.BASELINE, None
    if args.inject is not None:
        extra = {"container": args.inject_target} if args.inject_target else {}
        plan = INJECTIONS[args.inject](duration=timedelta(minutes=args.fault_minutes), **extra)
        hook = CommandInjection(plan, runner=docker_runner, dry_run=args.dry_run)
        mode = CollectMode.INJECT
        print(f"[plan] {plan.describe()}")

    if not args.no_workload:
        _workload("start")
    try:
        print(f"[collect] {args.case}: {since.isoformat()} → {until.isoformat()}（跟随中）")
        report = collector.collect(
            mode=mode, hook=hook, follow_until=until, poll_interval=args.poll_interval
        )
    finally:
        if not args.no_workload:
            _workload("stop")

    print("[done]")
    print(_describe(report))
    case_dir = root / args.case
    metrics_bytes = (case_dir / "metrics.jsonl").stat().st_size
    print(f"  指标文件       {metrics_bytes} 字节")
    print(f"  产物目录       {case_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
