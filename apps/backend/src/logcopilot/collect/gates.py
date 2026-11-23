# 顺序 9 · 验收闸门：把 V3 / V4 / V7 从"演示过"变成"能失败的命令"
"""P-A 收口的三条闸门（P-A §7 的 V3 / V4 / V7）。

为什么单独一个模块
------------------
V1（pack-lint）校验的是"一个 pack 自己自洽"，V2（verify-collect）校验的是
"与冻结基准没漂移"。剩下的三条问的是**跨对象**的问题：

- **V3 续采不重不漏**：同一段日志，"一次采完"与"中断后 attach 补完"必须
  得到相同的结果。单看任何一个 pack 都发现不了重号或漏行。
- **V4 双路径等价**：file（读 bind mount 出来的宿主机目录）与 docker
  （`docker exec` 读容器里的同一份文件）必须产出逐字节相同的 canonical
  投影。单看一个 pack 也发现不了——只有把两条路径并排比才看得出来。
- **V7 注入可逆**：故障注入之后集群必须回到注入前的样子。这条要读
  `labels.fault_injection` 的四个时刻，再去看现场。

三条闸门的共同点：**它们必须能失败**。一条永远绿的命令比没有命令更糟，
因为它会让人以为"这一项已经守住了"。所以这里的每个函数都返回
`list[str]` / 带 `error` 的结构，把"哪里不一致"说清楚，而不是只回一个 bool。

关于 V4 的诚实边界
------------------
`check_raw_parity` 证明的是**取数层**等价：同一段字节区间，两条路径读出来
一模一样。canonical 投影那一层只要 canonicalize 是同一个函数、输入相同，
输出就相同——这一点由 `test_file_and_docker_readers_are_byte_identical`
和本模块的 `compare_canonical` 一起兜住。换句话说，V4 不是"两条路径碰巧
一致"，而是"取数等价 + 变换共享同一个实现"。
"""

from __future__ import annotations

import hashlib
import shlex
import subprocess
from collections import Counter
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Final

from logcopilot.collect.adapters.base import StreamSpec
from logcopilot.collect.adapters.docker import DockerAdapter, SubprocessRunner
from logcopilot.collect.adapters.file import FileAdapter, hdfs_streams
from logcopilot.domain.case import CaseManifest, Phase
from logcopilot.domain.log import LogRecord
from logcopilot.store.casepack import (
    CasePackPaths,
    pack_paths,
    read_labels,
    read_logs,
    read_manifest,
)

__all__ = [
    "CONTAINERS",
    "DEFAULT_TAIL_BYTES",
    "VOLUME_NETWORK",
    "ClusterStatus",
    "RawParity",
    "check_cluster",
    "check_raw_parity",
    "check_restored",
    "compare_canonical",
    "compare_packs",
]

# 容器名与 compose 里的 service 同名（见 infra/hdfs/compose.yaml）。写死而不是
# 从环境变量读，是因为这三条闸门本身就是"对着这一套环境"的断言：容器名变了，
# 闸门应该红，而不是悄悄跟着变。
CONTAINERS: Final[Mapping[str, str]] = {
    "nn": "alcp-nn",
    "dn1": "alcp-dn1",
    "dn2": "alcp-dn2",
    "dn3": "alcp-dn3",
}

# docker compose 生成的网络名 = <project>_<network>。注入脚本里的断网变体
# 用的也是这个名字，两处必须一致，否则 "断开/接回" 会作用到不同的网络。
VOLUME_NETWORK: Final[str] = "aiops-log-copilot_hdfs"

# 一次比对 64KB：一天的日志也就几 MB，64KB 足够覆盖几十条真实记录；再大
# 只是在给 `docker exec` 加传输量，对"是否一致"这个判断没有增益。
DEFAULT_TAIL_BYTES: Final[int] = 64 * 1024

# 与 `adapters/docker.py` 的 Runner 同一形状：argv 进、stdout 字节出。
# 抽成 Protocol 式的 Callable 是为了让测试能塞一个内存容器进来。
Runner = Callable[[Sequence[str]], bytes]


# ---------------------------------------------------------------------------
# V4：取数层等价（file vs docker）
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class RawParity:
    """一条流在 file / docker 两条路径上的读取结果对照。"""

    component: str
    container: str
    stream: str
    size: int
    compared_bytes: int
    file_sha256: str
    docker_sha256: str
    error: str = ""

    @property
    def ok(self) -> bool:
        """一致 = 没出错、比了非零字节、两边哈希相同。"""
        return (
            not self.error and self.compared_bytes > 0 and (self.file_sha256 == self.docker_sha256)
        )

    def describe(self) -> str:
        if self.error:
            return f"{self.component}:{self.stream} 无法比对：{self.error}"
        if not self.ok:
            return (
                f"{self.component}:{self.stream} 不一致："
                f"file={self.file_sha256[:12]} docker={self.docker_sha256[:12]}"
                f"（比了 {self.compared_bytes} 字节）"
            )
        return (
            f"{self.component}:{self.stream} 一致（{self.compared_bytes} 字节，"
            f"{self.file_sha256[:12]}）"
        )


def _pick(specs: Sequence[StreamSpec], name: str) -> StreamSpec | None:
    for spec in specs:
        if spec.name == name:
            return spec
    return None


def _docker_size(runner: Runner, container: str, path: str) -> int:
    out = runner(["exec", container, "stat", "-c", "%s", path])
    return int(out.decode("utf-8", "replace").split()[0])


def check_raw_parity(
    mount_root: Path | str,
    *,
    components: Sequence[str] = ("nn", "dn1", "dn2", "dn3"),
    containers: Mapping[str, str] | None = None,
    tail_bytes: int = DEFAULT_TAIL_BYTES,
    runner: Runner | None = None,
) -> tuple[RawParity, ...]:
    """V4 的取数层断言：同一段字节区间，file 与 docker 读出来必须相同。

    `mount_root` 是宿主机上 bind mount 出来的日志目录（`runtime/hdfs-logs`），
    容器里对应 `/opt/hadoop/logs`。**两端看的是同一份文件**，这正是 V4 在
    物理上成立的前提（P-A §2 把日志挂出来就是为了这个）。

    取法上刻意选"固定区间"而不是"读尾巴"：先取容器里文件的大小，再定
    `[size - tail_bytes, size)` 这一段，两条路径各读这一段。如果改成
    "file 读完再 docker 读尾巴"，中间新写进来几行就会让两次读的区间错位，
    闸门会开始报假红（比漏报更糟）。
    """
    root = Path(mount_root)
    names = containers if containers is not None else CONTAINERS
    cmd = runner if runner is not None else SubprocessRunner()
    results: list[RawParity] = []

    for component in components:
        container = names.get(component)
        if container is None:
            results.append(
                RawParity(component, "", "main", 0, 0, "", "", f"没有组件 {component} 的容器名")
            )
            continue

        file_adapter = FileAdapter(root, hdfs_streams((component,)))
        docker_adapter = DockerAdapter(container, component, runner=cmd)
        try:
            file_spec = _pick(file_adapter.streams(), "main")
            docker_spec = _pick(docker_adapter.streams(), "main")
        except Exception as exc:  # 适配器自己的错误（AmbiguousStream / 命令失败）
            results.append(RawParity(component, container, "main", 0, 0, "", "", str(exc)))
            continue

        if file_spec is None or docker_spec is None or file_spec.path is None:
            results.append(RawParity(component, container, "main", 0, 0, "", "", "找不到主日志流"))
            continue

        host_path = Path(file_spec.path)
        container_path = docker_spec.path or ""
        try:
            size = _docker_size(cmd, container, container_path)
            start = max(0, size - tail_bytes)
            length = size - start

            # 文件侧：只读固定区间（不用 read() 全文，避免把 60MB 读进内存）
            with host_path.open("rb") as handle:
                handle.seek(start)
                file_bytes = handle.read(length)

            # 容器侧：tail -c +N 是 1 基偏移，配 head -c 卡死长度
            script = f"tail -c +{start + 1} {shlex.quote(container_path)} | head -c {length}"
            docker_bytes = cmd(["exec", container, "sh", "-c", script])
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
            results.append(RawParity(component, container, "main", 0, 0, "", "", error))
            continue

        file_sha = hashlib.sha256(file_bytes).hexdigest()
        docker_sha = hashlib.sha256(docker_bytes).hexdigest()
        error = ""
        if len(file_bytes) != length:
            error = f"宿主机只读到 {len(file_bytes)}/{length} 字节（文件被截断？）"
        elif len(docker_bytes) != length:
            error = f"容器里只读到 {len(docker_bytes)}/{length} 字节"
        results.append(
            RawParity(
                component=component,
                container=container,
                stream="main",
                size=size,
                compared_bytes=length,
                file_sha256=file_sha,
                docker_sha256=docker_sha,
                error=error,
            )
        )
    return tuple(results)


def compare_canonical(left: Iterable[str], right: Iterable[str]) -> list[str]:
    """逐字节比两份 canonical 行序列（V4 的变换层那一半）。"""
    left_list = list(left)
    right_list = list(right)
    if len(left_list) != len(right_list):
        return [f"行数不同：左 {len(left_list)}，右 {len(right_list)}"]
    problems = [
        f"第 {i} 行不同：{a[:60]!r} != {b[:60]!r}"
        for i, (a, b) in enumerate(zip(left_list, right_list, strict=True))
        if a != b
    ]
    return problems


# ---------------------------------------------------------------------------
# V3：续采不重不漏
# ---------------------------------------------------------------------------


def _as_paths(value: CasePackPaths | Path | str) -> CasePackPaths:
    """把"pack 目录"的三种写法（paths / Path / str）统一成 `CasePackPaths`。"""
    if isinstance(value, CasePackPaths):
        return value
    path = Path(value)
    return pack_paths(path.parent, path.name)


def compare_packs(
    left: CasePackPaths | Path | str,
    right: CasePackPaths | Path | str,
) -> list[str]:
    """V3 的断言：一次采完 vs 中断后补完，两个 pack 必须等价。

    比三样东西：

    1. **id 不重号**（两侧各自查）：重号意味着续采把同一段读了两遍，这是 V3
       真正要挡的错；
    2. **记录内容的多重集合相同**：`(ts, level, component, logger, message, raw,
       parse_status)` 逐条对账，多一条少一条都要报；
    3. **counters**：行数、level 分布、component 分布、parse 失败数。

    **不比 id 序列与 canonical_sha256**，这是这一轮现场演示打出来的结论：
    `id` 里的行号是"全局自增计数"，按**读的先后**发号，而一次采完与
    "崩了再续"的读法不同 —— 崩点之后继续读的那条流会拿到比别的流更大的号，
    于是同一个时间戳（同一毫秒里几条流各写一行）的平局排序会变。内容一模一样，
    只是 id 在平局内部换了位置，`canonical_sha256` 会跟着变。

    要让这两个也稳定，得让 id 由"流 + 流内偏移"派生（而不是全局自增），
    那是一次 schema 变更（id 格式变了，132 个已迁移 pack 都要重迁）。记在
    P-A 的 O11 里，等真有需要再动。

    也**不比 `input_fingerprint`**：它包含 `window.since/until` 与原始行数，
    两次采集的窗口边界本来就差几秒，比它等于要求"两次采集的起止时刻完全相同"，
    那不是 V3 要保证的事。
    """
    left_paths = _as_paths(left)
    right_paths = _as_paths(right)

    problems: list[str] = []
    left_logs = read_logs(left_paths)
    right_logs = read_logs(right_paths)

    left_ids = [record.id for record in left_logs]
    right_ids = [record.id for record in right_logs]
    if len(set(left_ids)) != len(left_ids):
        problems.append("左侧自身有重复 id（同一段被读了两次）")
    if len(set(right_ids)) != len(right_ids):
        problems.append("右侧自身有重复 id（同一段被读了两次）")
    if len(left_logs) != len(right_logs):
        problems.append(f"记录数不同：{len(left_logs)} != {len(right_logs)}")

    only_left = Counter(_content_key(record) for record in left_logs)
    only_right = Counter(_content_key(record) for record in right_logs)
    extra_left = only_left - only_right
    extra_right = only_right - only_left
    if extra_left or extra_right:
        sample_left = [key[1:4] for key, _ in list(extra_left.items())[:3]]
        sample_right = [key[1:4] for key, _ in list(extra_right.items())[:3]]
        problems.append(
            f"记录内容不同：只在左 {sum(extra_left.values())} 条，只在右 "
            f"{sum(extra_right.values())} 条；样例 左{sample_left} 右{sample_right}"
        )

    left_manifest = read_manifest(left_paths)
    right_manifest = read_manifest(right_paths)
    if left_manifest.counters != right_manifest.counters:
        problems.append(
            f"counters 不同：{left_manifest.counters.to_dict()} != "
            f"{right_manifest.counters.to_dict()}"
        )
    return problems


def _content_key(record: LogRecord) -> tuple[object, ...]:
    """一条记录"内容"的指纹：**故意不含 id**（理由见 `compare_packs`）。"""
    return (
        record.ts,
        record.level.value,
        record.component,
        record.logger,
        record.message,
        record.raw,
        record.parse_status.value,
        record.template_id,
    )


# ---------------------------------------------------------------------------
# V7：注入可逆
# ---------------------------------------------------------------------------


def check_restored(root: Path | str = Path("data/cases")) -> list[str]:
    """V7 的离线那一半：读每个故障 pack 的 `fault_injection`，看它是否声称复原过。

    只看 pack 能证明的是"记账诚实"：注入记录里的 `restored_ok` 是采集器在
    现场执行完 restore 之后**实测**写下的（见 `collect/inject.py` 的三条规则），
    不是人补的。现场是否真的恢复，由 `check_cluster` 那一半负责，两者一起
    才算 V7 过。

    时刻的先后顺序（started <= ended <= restored）与 `script` 非空都由
    `FaultInjection` 自己在构造时挡住，所以这里**不再重复校验** —— 重复写
    一遍只会给人"这里管了"的错觉，实际永远不会触发。这里只查 schema 管不
    到的那一件事：`restored_ok` 到底有没有被真的验过（None = 没验）。
    """
    root_path = Path(root)
    problems: list[str] = []
    checked = 0
    for case_dir in sorted(root_path.iterdir() if root_path.is_dir() else []):
        if not case_dir.is_dir():
            continue
        paths = pack_paths(root_path, case_dir.name)
        if not (paths.manifest).exists():
            continue  # 没 finalize 的残包不算（pack-lint 会单独报）
        labels = read_labels(paths)
        injection = labels.fault_injection
        if injection is None:
            continue
        checked += 1
        if injection.restored_ok is not True:
            problems.append(
                f"{case_dir.name}: restored_ok={injection.restored_ok!r}（注入没验证过就收工）"
            )
    if checked == 0:
        problems.append(f"{root_path} 下没有任何带 fault_injection 的 pack，V7 无从验证")
    return problems


@dataclass(frozen=True, slots=True)
class ClusterStatus:
    """V7 的现场那一半：集群是否回到"注入前"的样子。"""

    running: tuple[str, ...]
    safemode_off: bool
    live_datanodes: int
    stray_files: tuple[str, ...]
    network_members: tuple[str, ...]
    #: 副本不足的块数。注入（尤其是 `dn_disk_broken`／`dn_volume`）会让 DN 暂时掉线，
    #: 恢复后留下 Under-replicated 块；不补回来，下一例的基线就带着上一例的伤。
    under_replicated_blocks: int = 0
    #: 缺副本的块数（`dfsadmin -report` 里的 Missing blocks）。
    missing_blocks: int = 0

    def check(self, *, expect_datanodes: int = 3) -> list[str]:
        problems: list[str] = []
        missing = [name for name in CONTAINERS.values() if name not in self.running]
        if missing:
            problems.append(f"容器没在跑：{missing}")
        if not self.safemode_off:
            problems.append("安全模式没有退出（注入后必须 leave）")
        if self.live_datanodes != expect_datanodes:
            problems.append(f"Live datanodes = {self.live_datanodes}，期望 {expect_datanodes}")
        if self.stray_files:
            problems.append(f"注入残留文件：{list(self.stray_files)}")
        missing_net = [name for name in CONTAINERS.values() if name not in self.network_members]
        if missing_net:
            problems.append(f"网络 {VOLUME_NETWORK} 里缺容器：{missing_net}")
        if self.under_replicated_blocks:
            problems.append(
                f"Under-replicated blocks = {self.under_replicated_blocks}"
                "（上一例的伤还没补回来，基线会被污染；用 --repair 恢复）"
            )
        if self.missing_blocks:
            problems.append(f"Missing blocks = {self.missing_blocks}（数据已经丢了，需要重采）")
        return problems


def _count_from_report(report: str, prefix: str) -> int:
    """从 `dfsadmin -report` 里取 `Prefix: N` 这种计数（取不到就当 0）。

    两个坑都在真实输出里踩过：

      - 这些行是**制表符缩进**的（`\\tUnder replicated blocks: 0`），
        所以必须先 `strip()` 再比 —— 否则永远匹配不上、永远返回 0，
        闸门看起来"通过"，其实压根没读到数；
      - `Missing blocks` 后面还跟着 `Missing blocks (with replication factor 1)`，
        前缀匹配会误命中它，所以要求 `prefix` 之后紧跟冒号。
    """
    for line in report.splitlines():
        text = line.strip()
        if not text.startswith(prefix):
            continue
        rest = text[len(prefix) :]
        if not rest.startswith(":"):
            continue  # `Missing blocks (with replication factor 1): 0` 不是同一个键
        digits = "".join(ch if ch.isdigit() else " " for ch in rest).split()
        return int(digits[0]) if digits else 0
    return 0


def check_cluster(
    *,
    containers: Mapping[str, str] | None = None,
    network: str = VOLUME_NETWORK,
    expect_datanodes: int = 3,
) -> ClusterStatus:
    """现场体检：容器活着、安全模式已退、DN 都回来了、没有注入残留。"""
    names = containers if containers is not None else CONTAINERS

    def run(argv: Sequence[str]) -> str:
        completed = subprocess.run(argv, capture_output=True, check=False, text=True)
        return completed.stdout.strip()

    running = tuple(
        line.strip()
        for line in run(["docker", "ps", "--format", "{{.Names}}"]).splitlines()
        if line.strip()
    )

    namenode = names["nn"]
    safemode = run(["docker", "exec", namenode, "hdfs", "dfsadmin", "-safemode", "get"])
    report = run(["docker", "exec", namenode, "hdfs", "dfsadmin", "-report"])
    live = 0
    for line in report.splitlines():
        if line.startswith("Live datanodes"):
            digits = "".join(ch if ch.isdigit() else " " for ch in line).split()
            live = int(digits[0]) if digits else 0
            break

    # 注入脚本留下的填充文件：dn1 在 tmpfs 上，dn2/dn3 在命名卷上
    stray: list[str] = []
    dn1_fill = run(
        ["docker", "exec", names["dn1"], "sh", "-c", "ls /tmp/hadoop-root/dfs/data/lab.fill"]
    )
    if dn1_fill:
        stray.append(f"{names['dn1']}:lab.fill")
    for key in ("dn2", "dn3"):
        volume = f"aiops-log-copilot_{key}-data"
        found = run(
            [
                "docker",
                "run",
                "--rm",
                "--user",
                "0:0",
                "-v",
                f"{volume}:/d:ro",
                "apache/hadoop:3.3.6",
                "sh",
                "-c",
                "ls /d/lab.fill",
            ]
        )
        if "/lab.fill" in found:
            stray.append(f"{volume}:lab.fill")

    members = tuple(
        name
        for name in run(
            [
                "docker",
                "network",
                "inspect",
                network,
                "--format",
                "{{range .Containers}}{{.Name}} {{end}}",
            ]
        ).split()
        if name
    )

    return ClusterStatus(
        running=running,
        safemode_off="OFF" in safemode.upper(),
        live_datanodes=live,
        stray_files=tuple(stray),
        network_members=members,
        under_replicated_blocks=_count_from_report(report, "Under replicated blocks"),
        missing_blocks=_count_from_report(report, "Missing blocks"),
    )


# ---------------------------------------------------------------------------
# 小工具：给 CLI 用
# ---------------------------------------------------------------------------


def repair_cluster(*, containers: Mapping[str, str] | None = None) -> str:
    """把注入留下的坏块从 HDFS 里清掉（`fsck -delete`），返回一行摘要。

    为什么需要它：`dn_disk_broken` / `dn_volume` 这类注入会让 DN 掉线，
    恢复后集群里留着 `Under-replicated` 甚至 `Missing` 的块。这些块不会自己消失
    （副本数就这样定住了），下一例的基线就建在"上一例的伤"上面 ——
    指标里的 `under_replicated_blocks` 会一直非 0，V7 也就永远只红不绿。

    `fsck -delete` 只删**损坏/缺失**的块记录，健康块一个字节都不动；
    代价是那些文件的数据真的没了 —— 对实验集群这是可接受的，
    对生产集群这就是一次数据丢失，所以这个函数只在 `--repair` 时被调用。
    """
    names = containers if containers is not None else CONTAINERS
    completed = subprocess.run(
        ["docker", "exec", names["nn"], "hdfs", "fsck", "/", "-delete"],
        capture_output=True,
        check=False,
        text=True,
    )
    tail = [line for line in completed.stdout.splitlines() if line.strip()][-3:]
    return " / ".join(tail) if tail else completed.stdout.strip()


def phase_window(manifest: CaseManifest, phase: Phase) -> tuple[str, str] | None:
    """取某个阶段的窗口（CLI 打摘要用；缺这个阶段就返回 None）。"""
    window = manifest.window.phase(phase)
    if window is None:
        return None
    return (window.since.isoformat(), window.until.isoformat())


def summarize_manifest(manifest: CaseManifest) -> str:
    """一行摘要，CLI 报告用。"""
    counters = manifest.counters
    return (
        f"{manifest.case_id}: {counters.lines} 条"
        f"（raw {counters.raw_lines}，partial/failed {counters.parse_failed}），"
        f"level={dict(counters.by_level)}"
    )
