# 顺序 5 · 采集编排：把 0→1→2→3→4 串成一次采集，产出第一个完整的 pack
"""`collect/collect.py` —— 采集编排器（P-A §4 的三种模式）。

前面四步各自只管一件事：

    0  取数      adapters/*      把字节变成 `(行号, 原文)`
    1  翻译      canonical.py    原文 → `LogRecord`（解析 + 续行归并 + 脱敏）
    2  质检      domain/log.py   `LogRecord` 自己校验字段
    3  元信息    domain/case.py  manifest / labels 契约
    4  打包      store/casepack  建包 / 追加 / finalize / 两个哈希

这一步是**唯一知道"一次采集从头到尾怎么走"的地方**，也是唯一会同时 import
`adapters` 与 `store` 的地方（`collect/` 仍然不碰 `rag/agents/tools`，P-A §9）。

## 三个必须在这里回答的问题

**① checkpoint 写在哪？**
P-A §5 只允许 checkpoint 待在 `manifest.json` 里，而 manifest 是 finalize
**最后**才写的（写了才算完成）。可采一例要 20 分钟，中途 Ctrl-C 是常态 ——
如果 checkpoint 跟着 manifest 一起推迟，中断就等于什么都没记住，V3
（续采不重不漏）永远测不了。
所以采集期把它落在 **`checkpoint.json`**（sidecar），finalize 成功后并入
manifest 再删掉。这是对 P-A §5 目录树的一处增补，要回填文档。

**② `raw_sha256` 怎么算，才既能续采、又跟 adapter 无关？**
不能去读文件字节：docker 路径拿字节要额外命令，而且同一份内容在两条路径上
的 inode/mtime 本来就不同。也不能等最后一次性算：中断后拿不到前半段。
解法是**行式哈希链**（见 `RawHasher`）：每消费一行就把链推进一步，链值可
序列化进 checkpoint.json，续采时从链值接着推。两条路径吐出的原文逐字相同，
所以链值也相同 —— `input_fingerprint` 因此与 adapter 无关。

**③ 三种模式差在哪？**
- `baseline`：只采，不注入。用于 `base_healthy_01` 这种正常态样本。
- `inject`：采一轮 → **flush** → 跑注入钩子（记录 started）→ 再采一轮 →
  钩子收尾时记 ended/restored → finalize。注入脚本本身不在这里执行，
  而是通过 `InjectionHook` 注入进来：这一步还不知道 docker/compose 长什么样。
- `attach`：接着一个**已有**的未完成 pack 采（从 checkpoint.json 恢复），
  不新建目录。用于"采集器崩了，人工重启接上"。

## 一处刻意不做的事

`inject` 模式这里只做"两轮"，不做"按时间窗轮询"。原因：轮询需要 sleep，
而 sleep 会让单测变成慢测；窗口调度属于第 11 步 `scripts/ask.py` / 端到端
那一段的事，届时由 `--follow --duration` 调 `drain()` 循环即可，编排逻辑
不用改。
"""

from __future__ import annotations

import hashlib
import json
import os
import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Protocol

from logcopilot.collect.adapters.base import ComponentReader, RawLine, SourceAdapter
from logcopilot.collect.canonical import (
    CanonicalResult,
    Redactor,
    assert_within_window,
    canonicalize,
)
from logcopilot.collect.metrics import MetricsSession, MetricsWriter
from logcopilot.domain.case import (
    REDACT_RULES,
    AdapterState,
    CanonicalProfile,
    CaseManifest,
    Checkpoint,
    Counters,
    FaultInjection,
    Hashes,
    Labels,
    Origin,
    OriginKind,
    Phase,
    RedactPolicy,
    SourceRef,
    Window,
)
from logcopilot.domain.log import COMPONENTS, ParseStatus
from logcopilot.store.casepack import (
    CasePackPaths,
    LogWriter,
    create_pack,
    finalize,
    open_log_writer,
    pack_paths,
    write_labels,
)

__all__ = [
    "CHECKPOINT_FILE",
    "CollectError",
    "CollectMode",
    "CollectReport",
    "Collector",
    "InjectionHook",
    "RawHasher",
    "SourceSpec",
]

#: 采集期的 checkpoint sidecar（finalize 成功后删除，见模块头注释 ①）。
CHECKPOINT_FILE = "checkpoint.json"

#: 行式哈希链的种子。换它 = 换指纹算法，历史 `input_fingerprint` 全部失效，
#: 所以它带了版本号。
RAW_SEED = b"aiops-log-copilot/raw-lines/v1"

#: `finalize` 会覆盖这两个占位值，但 `CaseManifest` 要求它们是合法的
#: sha256 形状，所以不能写 None。
_PLACEHOLDER_HASH = "sha256:" + "0" * 64


class CollectError(RuntimeError):
    """编排层面的错误：模式用错、没有可续采的 checkpoint、窗口越界等。

    与 `CasePackError`（包本身坏了）和 `CheckpointMismatch`（源变了）区分开：
    这个错误是"调用姿势不对"，重试同样的命令不会变好。
    """


class CollectMode(StrEnum):
    """P-A §4 的三种采集模式。"""

    BASELINE = "baseline"
    INJECT = "inject"
    ATTACH = "attach"


class RawHasher:
    """`input_fingerprint` 里那个 `raw_sha256`：一条**可续采**的行式哈希链。

    每行推进一次：`h_i = sha256(h_{i-1} || 0x0A || line_i)`。

    为什么不用"把所有原文拼起来再 sha256"：
      - 采一例 20 分钟，中途断了就拿不到前半段的原文了；
      - `hashlib` 的中间态不可序列化，只能从 0 重算；
      - 而行式链的"中间态"就是那 32 字节的链值本身，写进 checkpoint.json
        就能从第二次采集接着推 —— 于是"采集两次"和"一次采到底"得到的
        `raw_sha256` 必须相等（有测试钉住这条不变式）。

    为什么不用"对源文件字节取 sha256"：docker 路径要多发一条命令，而且
    bind mount 与容器内看到的 inode/mtime 不同 —— 那属于"怎么搬"，不属于
    "是什么"，不该进指纹。
    """

    __slots__ = ("_chain",)

    def __init__(self, chain: str | None = None) -> None:
        if chain is None:
            self._chain = hashlib.sha256(RAW_SEED).digest()
        else:
            # 续采：直接从上次的链值接着推。**不要**再混一次种子，
            # 否则"两次采"与"一次采"的链值必然不同，V3 的等价性就丢了。
            digest = chain.removeprefix("sha256:")
            if len(digest) != 64:
                raise ValueError(f"raw_sha256 链值长度不对: {chain!r}")
            self._chain = bytes.fromhex(digest)

    def update(self, text: str) -> None:
        self._chain = hashlib.sha256(self._chain + b"\n" + text.encode("utf-8")).digest()

    def update_many(self, lines: Iterable[str]) -> None:
        for line in lines:
            self.update(line)

    @property
    def hexdigest(self) -> str:
        return "sha256:" + self._chain.hex()


@dataclass(frozen=True, slots=True)
class SourceSpec:
    """一个组件配一个 adapter，外加"人能不能看懂"的来源标识。

    `SourceRef` 是**契约**（要进 manifest、要进指纹），但 adapter 自己不知道
    容器名或宿主机路径该填什么，所以由调用方在这里给出。

    注意 `sources` 的顺序会进 `input_fingerprint`：同一个 case 两次采集必须
    用同一个顺序，否则明明没换数据，指纹也会变。
    """

    component: str
    adapter: SourceAdapter
    ref: str
    path: str | None = None
    image_digest: str | None = None

    def __post_init__(self) -> None:
        if self.component not in COMPONENTS:
            raise ValueError(f"component 非法: {self.component!r}，闭集={sorted(COMPONENTS)}")
        if not self.ref:
            raise ValueError("source.ref 不能为空 —— 它要进 manifest，人得看得懂这是哪台机器")

    def source_ref(self) -> SourceRef:
        return SourceRef(
            adapter=self.adapter.adapter,
            ref=self.ref,
            component=self.component,
            image_digest=self.image_digest,
            path=self.path,
        )

    @property
    def source(self) -> str:
        """写进 `LogRecord.source` 的字符串（`docker:dn1`）。它不进 canonical 投影。"""
        return f"{self.adapter.adapter.value}:{self.component}"


class InjectionHook(Protocol):
    """故障注入钩子。

    编排器只负责"什么时候调它"与"把结果记进 labels"，不负责怎么注入 ——
    那属于 `collect/inject/*.sh`（第 12 步），而且要能在 Windows 上被测试
    替身顶掉。

    `flush` 由编排器传给钩子：脚本执行前**必须**先 flush，否则崩溃时
    "注入之前采到的行"可能还留在缓冲里，事后无法证明注入前系统是好的。
    """

    # 只读属性而不是可变字段：`CommandInjection` 用 `@property name` 实现，
    # pyright 会认为"可变字段"与"只读属性"不兼容（同 StreamReader.spec 的坑）。
    @property
    def name(self) -> str: ...

    def __call__(self, flush: Callable[[], None]) -> FaultInjection: ...


@dataclass(slots=True)
class _Accumulator:
    """一次采集过程中不断累加的东西。

    做成可变对象是为了让 `_drain()` 与 `collect()` 不用互相 nonlocal。
    """

    hasher: RawHasher
    states: dict[str, AdapterState] = field(default_factory=dict)
    redacted: dict[str, int] = field(default_factory=dict)
    raw_lines: int = 0
    records: int = 0
    parse_failed: int = 0
    last_ts: datetime | None = None
    # 读到了、但时间戳落在窗口上界之后的记录数。行本身已经被消费（offset 前进、
    # 进 raw 哈希链），所以它们不会在续采时再出现 —— 丢的是"记录"，不是"字节"。
    dropped: int = 0

    def absorb(
        self, lines: Sequence[RawLine], result: CanonicalResult, *, dropped: int = 0
    ) -> None:
        self.raw_lines += len(lines)
        self.dropped += dropped
        self.records += len(result.records)
        self.parse_failed += sum(
            1 for record in result.records if record.parse_status is not ParseStatus.OK
        )
        for name, count in result.redacted.items():
            self.redacted[name] = self.redacted.get(name, 0) + count
        for _line_no, text in lines:
            self.hasher.update(text)
        for record in result.records:
            if self.last_ts is None or record.ts > self.last_ts:
                self.last_ts = record.ts


@dataclass(frozen=True, slots=True)
class CollectReport:
    """采集结果。给 CLI 打印用，也给测试断言用（避免测试去读私有属性）。

    `records` / `parse_failed` 是**本次这一轮**的数字：attach 只补一小段时，
    它们说的是"这轮写了多少"，而 `manifest.counters.lines` 说的是**整个
    pack** 有多少。两者在 baseline/inject 下相等，在 attach 下不等 —— 这是
    刻意区分的，别拿 report 去代表 pack 的全量。
    """

    case_id: str
    mode: CollectMode
    manifest: CaseManifest
    raw_line_count: int
    records: int
    parse_failed: int
    injection: FaultInjection | None = None
    # 跟随收尾时撞到窗口上界、被丢掉的记录数（见 `_drain` 的窗口过滤）。
    # 一般是 0 或个位数；大于 0 说明最后一轮读到了窗口外的几秒。
    dropped_out_of_window: int = 0

    @property
    def dir(self) -> str:
        return f"data/cases/{self.case_id}"


class Collector:
    """把一次采集从头走到尾。

    用法（正常态样本）：

        Collector(root, case_id="base_healthy_01", profile=HDFS_LOG4J,
                  window=Window(since, until), labels=labels, sources=[...]).collect()

    注入样本则传 `mode=CollectMode.INJECT, hook=...`。
    """

    __slots__ = (
        "_case_id",
        "_clock",
        "_created_at",
        "_labels",
        "_metrics",
        "_profile",
        "_redactor",
        "_root",
        "_sources",
        "_start_at_end",
        "_window",
    )

    def __init__(
        self,
        root: Path | str,
        *,
        case_id: str,
        profile: CanonicalProfile,
        window: Window,
        labels: Labels,
        sources: Sequence[SourceSpec],
        redactor: Redactor | None = None,
        created_at: datetime | None = None,
        clock: Callable[[], datetime] | None = None,
        metrics: MetricsSession | None = None,
        start_at_end: bool = False,
    ) -> None:
        if labels.case_id != case_id:
            raise CollectError(f"labels.case_id={labels.case_id!r} 与 case_id={case_id!r} 不一致")
        if not sources:
            raise CollectError("至少要有一个来源，否则采出来的是空 pack")
        components = [spec.component for spec in sources]
        if len(set(components)) != len(components):
            raise CollectError(f"同一个组件声明了两次: {components}")

        self._root = Path(root)
        self._case_id = case_id
        self._profile = profile
        self._window = window
        self._labels = labels
        self._sources = tuple(sources)
        self._redactor = redactor
        self._created_at = created_at
        self._clock = clock if clock is not None else (lambda: datetime.now(UTC))
        self._metrics = metrics
        self._start_at_end = start_at_end

    # ------------------------------------------------------------ 对外

    def collect(
        self,
        *,
        mode: CollectMode = CollectMode.BASELINE,
        hook: InjectionHook | None = None,
        check_window: bool = True,
        follow_until: datetime | None = None,
        poll_interval: float = 2.0,
        sleeper: Callable[[float], None] | None = None,
        resume_from: Mapping[str, AdapterState] | None = None,
    ) -> CollectReport:
        """跑一次采集并 finalize。

        `mode=ATTACH` 时要求目录里已经有 `checkpoint.json`（也就是上一次
        采集中断过）；否则报错，而不是悄悄从头再来一遍。

        `follow_until` 给了就进入**跟随模式**：反复 drain 直到这个时刻，
        每次之间 sleep `poll_interval` 秒。不给就只 drain 一轮（单测默认）。
        真实采集必须给 —— 5 分钟的正常态样本不是"读一次现有日志"，而是
        "盯着日志看 5 分钟"。

        `resume_from` 是"外部指定的起点"（key 形如 `dn1:main`）。它存在的
        唯一理由是**可重复性**：V3 要证明"一次采完"与"中断后补完"等价，
        那就必须让两次采集从**同一批偏移**出发；各自 `prime()` 的话，两次
        prime 之间写入的行会让两侧天然错位，闸门会报假红。给了它就不再做
        `start_at_end`，也不读磁盘上的 checkpoint（attach 模式另说）。
        """
        paths = pack_paths(self._root, self._case_id)

        if mode is CollectMode.INJECT and hook is None:
            raise CollectError("inject 模式必须给 hook —— 否则采出来的是个普通样本")
        if mode is not CollectMode.INJECT and hook is not None:
            raise CollectError(f"{mode.value} 模式不该有 hook")
        if follow_until is not None and follow_until > self._window.until:
            raise CollectError(
                f"follow_until={follow_until.isoformat()} 超出窗口上界 "
                f"{self._window.until.isoformat()} —— 多出来的行会被质检拒收"
            )
        if poll_interval <= 0:
            raise CollectError("poll_interval 必须大于 0")
        if resume_from is not None and mode is CollectMode.ATTACH:
            raise CollectError(
                "attach 模式不接受 resume_from —— 续采的起点只能来自 checkpoint.json，"
                "否则等于用别人的偏移读这个 pack"
            )

        resuming = mode is CollectMode.ATTACH
        if resuming:
            acc, created_at = self._load(paths)
        else:
            create_pack(paths, self._labels)
            acc = _Accumulator(hasher=RawHasher())
            created_at = self._created_at or self._clock()
            # **先落一个空 checkpoint**：如果第一个组件就读崩了（容器没起、
            # 权限不对），一个组件都没跑完，也就一次都没保存过 —— 那时
            # attach 会以为"这不是一次中断的采集"，把一个可续采的现场
            # 判成不可续采。空 checkpoint 表示"这次采集开过头，一行没成"。
            self._save_checkpoint(paths, acc)
            if resume_from is not None:
                # 外部指定的起点（V3 场景）：直接种进 acc 并落盘，跳过一次 prime。
                acc.states.update(resume_from)
                self._save_checkpoint(paths, acc)
            elif self._start_at_end:
                # "从现在开始采"：把每条流推到文件末尾再落一次 checkpoint。
                # 不这么做的话，第一次 drain 会把集群启动时的 22KB 启动日志
                # 一口吞进 5 分钟窗口，采出来的是"启动分布"而不是运行状态。
                for spec in self._sources:
                    prime = getattr(spec.adapter, "prime", None)
                    if prime is None:
                        raise CollectError(
                            f"{spec.source} 的适配器不支持 start_at_end —— "
                            "要么换一个支持 prime() 的适配器，要么关掉这个选项"
                        )
                    acc.states.update(prime())
                self._save_checkpoint(paths, acc)

        injection: FaultInjection | None = None
        raw_before = acc.raw_lines
        writer = open_log_writer(paths)
        # 指标写者与 logs 写者同生共死：一次采集要么两条腿都有，要么都没有。
        metrics_writer = MetricsWriter(paths.metrics) if self._metrics is not None else None

        def progress() -> None:
            """采集进度的安全点：把已采的行与已采的指标都落盘。

            注意两个写者都要 flush：`MetricsWriter.append` 只写缓冲，
            不 flush 的话整个故障段的指标都躺在内存里 —— 采集器一崩，
            "指标说 DN 死了"这条证据就跟着没了（实采时踩到过：
            20 分钟跑完 `metrics.jsonl` 还是 0 字节）。

            `inject` 模式的自定义 hook 在故障段里每秒调它一次 —— 所以故障
            持续 10 分钟时，指标不是"注入前后各一个点"，而是 600 秒的连续
            曲线。这正是"日志说 DN 死了、指标说 dead_datanodes 从 3 掉到 2"
            能对上的前提。
            """
            writer.flush()
            self._sample_metrics(metrics_writer)

        # 注入段的起点 = 窗口里声明的 fault 段起点。没有声明就退化成"立刻注入"。
        fault = self._window.phase(Phase.FAULT) if mode is CollectMode.INJECT else None
        baseline_until = fault.since if fault is not None else follow_until

        try:
            if follow_until is None or baseline_until is None:
                self._drain_all(paths, writer, acc, check_window=check_window)
                self._sample_metrics(metrics_writer)
            else:
                # 先把 baseline 段采满，再动手注入。少了这一轮，注入会落在
                # baseline 里 —— 事后谁也没法证明"注入之前系统是好的"，
                # 而且故障窗口里根本没有"由好变坏"的转折点。
                self._follow(
                    paths,
                    writer,
                    acc,
                    metrics_writer,
                    until=baseline_until,
                    poll_interval=poll_interval,
                    sleeper=sleeper,
                    check_window=check_window,
                )

            if mode is CollectMode.INJECT:
                # 先 flush 再注入：注入前那一刻的日志必须已经在盘上，
                # 否则事后无法证明"注入前系统是好的"。
                assert hook is not None  # 上面已经挡过，这里给类型检查看
                injection = hook(progress)
                if not isinstance(injection, FaultInjection):
                    raise CollectError(
                        f"hook {hook.name!r} 必须返回 FaultInjection，"
                        f"拿到 {type(injection).__name__}"
                    )
                write_labels(paths, replace(self._labels, fault_injection=injection))
                # 第二轮：注入之后新写出来的日志（故障段/恢复段）。
                if follow_until is None:
                    self._drain_all(paths, writer, acc, check_window=check_window)
                    self._sample_metrics(metrics_writer)
                else:
                    self._follow(
                        paths,
                        writer,
                        acc,
                        metrics_writer,
                        until=follow_until,
                        poll_interval=poll_interval,
                        sleeper=sleeper,
                        check_window=check_window,
                    )
        finally:
            writer.close()
            if metrics_writer is not None:
                metrics_writer.close()

        manifest = self._build_manifest(acc, created_at, injection)
        final = finalize(
            paths,
            manifest,
            raw_line_count=acc.raw_lines,
            raw_sha256=acc.hasher.hexdigest,
            redacted=acc.redacted,
        )
        # finalize 成功 = checkpoint 已经并进 manifest，sidecar 可以退休了。
        paths.checkpoint_file.unlink(missing_ok=True)

        return CollectReport(
            case_id=self._case_id,
            mode=mode,
            manifest=final,
            raw_line_count=acc.raw_lines - raw_before,
            records=acc.records,
            parse_failed=acc.parse_failed,
            injection=injection,
            dropped_out_of_window=acc.dropped,
        )

    # ------------------------------------------------------------ 内部：采集

    def _drain_all(
        self,
        paths: CasePackPaths,
        writer: LogWriter,
        acc: _Accumulator,
        *,
        check_window: bool,
        until: datetime | None = None,
    ) -> None:
        for spec in self._sources:
            self._drain(paths, writer, acc, spec, check_window=check_window, until=until)

    def _drain(
        self,
        paths: CasePackPaths,
        writer: LogWriter,
        acc: _Accumulator,
        spec: SourceSpec,
        *,
        check_window: bool,
        until: datetime | None = None,
    ) -> None:
        """采一个组件：读流 → 解析 → 写盘 → 存 checkpoint。

        每个组件采完就落一次 checkpoint：崩了最多丢最后一个组件的那一段，
        而不是整次采集。

        ## 失败时为什么要把 logs.jsonl 截回去

        一个组件的记录是"读完 → 解析完 → 一次性写盘"的，所以正常失败
        （读的时候容器挂了）发生在写盘之前，`logs.jsonl` 里没有这个组件的
        任何一条 —— 一致。但写盘写到一半失败（磁盘满、进程被杀）就可能留下
        半个组件，而 `checkpoint.json` 还停留在组件开始前的位置：续采会把
        这几条重采一遍，`finalize` 报 id 重复。

        所以进来时量一下 `logs.jsonl` 的字节数，出事就截回那个位置 ——
        "checkpoint 指向哪，盘上就只能有到哪的记录"是一条硬不变式。

        ## 为什么窗口上界是硬边界

        跟随收尾那一轮读的是"此刻已写出来的行"，而这些行的时间戳可能已经
        越过 `until`（日志写出来总是比时钟滞后一点，收尾那一刻又多读了几秒）。
        不拦的话 `checkpoint.last_ts > window.until`，manifest 直接把整次
        采集判成"采到了窗口外的数据"—— 20 分钟白采。

        拦法是**丢记录、不丢字节**：`ts > until` 的记录不写盘、不进 counters，
        但它们确实被消费了（offset 已前进、已进 raw 哈希链），所以续采不会
        再读到它们，`dropped` 计数把它们记在账上。窗口上界不右移 ——
        "采 5 分钟"就得是 5 分钟，否则每个 pack 的窗口长度都不一样。
        """
        # 只把**本组件**的流状态交给 ComponentReader：line_no 是"组件内连续
        # 编号"，若把别的组件的状态也传进去，编号会从别人的最大值接着走。
        prefix = f"{spec.component}:"
        own = {key: value for key, value in acc.states.items() if key.startswith(prefix)}

        writer.flush()
        start_size = paths.logs.stat().st_size
        try:
            reader = ComponentReader(spec.adapter, spec.component, own)
            lines = list(reader)
            result = canonicalize(
                lines,
                profile=self._profile,
                component=spec.component,
                source=spec.source,
                since=self._window.since,
                redact=self._redactor,
            )
            dropped = 0
            if until is not None and result.records:
                kept = tuple(record for record in result.records if record.ts <= until)
                dropped = len(result.records) - len(kept)
                if dropped:
                    # 只换掉 records，raw_line_count/redacted 保持原样：
                    # 那几行确实被读进来、也被脱敏统计过了，只是不该成为记录。
                    result = CanonicalResult(
                        records=kept,
                        raw_line_count=result.raw_line_count,
                        redacted=result.redacted,
                    )
            if check_window and result.records:
                assert_within_window(result.records, self._window)

            writer.extend(result.records)
            acc.absorb(lines, result, dropped=dropped)
            acc.states.update(reader.checkpoint())
            self._save_checkpoint(paths, acc)
        except BaseException:
            writer.flush()
            _truncate(paths.logs, start_size)
            # 已完成组件与这个组件开始前的状态仍然有效，照样落盘。
            self._save_checkpoint(paths, acc)
            raise

    # ------------------------------------------------------------ 内部：跟随

    def _follow(
        self,
        paths: CasePackPaths,
        writer: LogWriter,
        acc: _Accumulator,
        metrics_writer: MetricsWriter | None,
        *,
        until: datetime,
        poll_interval: float,
        sleeper: Callable[[float], None] | None,
        check_window: bool,
    ) -> None:
        """反复 drain 直到 `until`。

        为什么不能"读一次就完"：`FileReader` 每次只拿**当下已有**的行。
        5 分钟的正常态样本要的是"这 5 分钟里又长了什么"，所以必须每隔
        `poll_interval` 再读一次。每轮都 drain 全部来源，每轮都存 checkpoint
        与指标 —— 中途 Ctrl-C 也只丢最后一轮。

        时间从 `self._clock()` 取而不是 `time.monotonic()`：一是保持与
        `created_at`/`last_ts` 同一时间源，二是测试可以喂假时钟。
        """
        sleep = sleeper if sleeper is not None else time.sleep
        if self._clock() >= until:
            # 时间已经到了就没有"跟随"可言，但仍读一轮：调用方可能只想
            # 把现有日志收干净（例如 attach 之后补最后一段）。
            self._drain_all(paths, writer, acc, check_window=check_window, until=until)
            self._sample_metrics(metrics_writer)
            return
        rounds = 0
        while True:
            self._drain_all(paths, writer, acc, check_window=check_window, until=until)
            self._sample_metrics(metrics_writer)
            rounds += 1
            now = self._clock()
            if now >= until:
                break
            sleep(min(poll_interval, (until - now).total_seconds()))

    def _sample_metrics(self, metrics_writer: MetricsWriter | None) -> None:
        """采这一拍到期的指标点。

        没有配 `metrics`（单测里一般不配）就什么都不做 —— `metrics.jsonl`
        保持 0 字节，与迁移来的 pack 一样，不会伪造出一个"看起来有指标"
        的样本。
        """
        if metrics_writer is None or self._metrics is None:
            return
        points = self._metrics.tick(self._clock())
        if points:
            # `MetricsWriter.append` 吃的是可迭代 —— 一拍可能同时出好几条
            # （一次 hdfs_report 会带 report + safemode 两个点）。
            metrics_writer.append(points)
            # 每拍都落盘：采集器随时可能被杀掉（Ctrl-C、OOM、容器重启），
            # 缓冲里的指标是"已经采到但没人看得见"的假数据。
            metrics_writer.flush()

    # ------------------------------------------------------------ 内部：状态

    def _save_checkpoint(self, paths: CasePackPaths, acc: _Accumulator) -> None:
        payload = {
            "schema": "collect-checkpoint/v1",
            "case_id": self._case_id,
            "raw_line_count": acc.raw_lines,
            "raw_sha256": acc.hasher.hexdigest,
            "redacted": dict(sorted(acc.redacted.items())),
            "last_ts": acc.last_ts,
            "adapter_state": {key: state.to_dict() for key, state in sorted(acc.states.items())},
        }
        text = (
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=False, default=_isoformat)
            + "\n"
        )
        _atomic_write_text(paths.checkpoint_file, text)

    def _load(self, paths: CasePackPaths) -> tuple[_Accumulator, datetime]:
        if not paths.checkpoint_file.is_file():
            raise CollectError(
                f"{paths.dir} 里没有 {CHECKPOINT_FILE} —— attach 只能接一次中断过的采集，"
                "从头采请用 baseline 模式"
            )
        if paths.manifest.is_file():
            raise CollectError(f"{paths.dir} 已经是完成的 pack，不能再 attach")

        data = json.loads(paths.checkpoint_file.read_text(encoding="utf-8"))
        acc = _Accumulator(
            hasher=RawHasher(str(data["raw_sha256"])),
            states={
                key: AdapterState.from_dict(value) for key, value in data["adapter_state"].items()
            },
            redacted={str(key): int(value) for key, value in data.get("redacted", {}).items()},
            raw_lines=int(data["raw_line_count"]),
            last_ts=_parse_ts(data.get("last_ts")),
        )
        created_at = self._created_at or self._clock()
        return acc, created_at

    # ------------------------------------------------------------ 内部：manifest

    def _build_manifest(
        self,
        acc: _Accumulator,
        created_at: datetime,
        injection: FaultInjection | None,
    ) -> CaseManifest:
        """组装 finalize 要吃的 manifest。

        `counters` / `hashes` 这里是占位值：真实值只能由 finalize 按盘上
        记录算出来（它读的才是最终排序后的那一份）。占位值永远不会落盘 ——
        manifest 是 finalize 最后一步写的。

        `injection` 只用于确认"注入这一轮确实发生过"，它本身不进 manifest：
        注入记录属于 labels（P-A §5 把两者分开），manifest 只描述数据。
        """
        del injection

        return CaseManifest(
            case_id=self._case_id,
            created_at=created_at,
            canonical_profile=self._profile,
            window=self._window,
            origin=Origin(kind=OriginKind.COLLECTED),
            sources=tuple(spec.source_ref() for spec in self._sources),
            counters=Counters(raw_lines=0, lines=0, by_level={}, by_component={}),
            hashes=Hashes(input_fingerprint=_PLACEHOLDER_HASH, canonical_sha256=_PLACEHOLDER_HASH),
            checkpoint=Checkpoint(
                adapter_state={key: state for key, state in sorted(acc.states.items())},
                last_ts=acc.last_ts,
            ),
            redact_policy=RedactPolicy(
                version=getattr(self._redactor, "version", "none"),
                rules=tuple(getattr(self._redactor, "rules", REDACT_RULES)),
            ),
        )


# ---------------------------------------------------------------- 小工具


def _isoformat(value: object) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(f"checkpoint.json 里出现了不可序列化的类型: {type(value).__name__}")


def _parse_ts(value: object) -> datetime | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise CollectError(f"checkpoint.last_ts 类型不对: {type(value).__name__}")
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise CollectError("checkpoint.last_ts 必须带时区 —— 无时区的时间戳是 A-R2 事故的入口")
    return parsed


def _atomic_write_text(path: Path, text: str) -> None:
    """先写 .tmp 再 `os.replace`：中途断电不会留下半个 checkpoint。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(text, encoding="utf-8", newline="\n")
    os.replace(tmp, path)


def _truncate(path: Path, size: int) -> None:
    """把文件截回 `size` 字节（幂等：已经更短就补不回来，直接不动）。"""
    if not path.exists() or path.stat().st_size <= size:
        return
    with path.open("r+b") as handle:
        handle.truncate(size)
