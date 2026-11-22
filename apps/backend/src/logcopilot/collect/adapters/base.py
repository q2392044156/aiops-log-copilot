# 顺序 0a · 取数（adapter）协议：把字节变成一行行原文，喂给节点 1 的翻译
"""`collect/adapters/base.py` —— 取数段的协议，不是任何具体适配器。

## 为什么先有协议、后有实现

P-A 要两条**完全等价**的取数路径：`file`（直接读挂载出来的日志文件）和
`docker`（`docker logs` / `docker exec cat`）。V4 的红线是"两条路径产出的
`canonical_sha256` 必须相等" —— 这句话翻译成代码就是：**流水线后半段
（canonicalize / package）一行都不许知道日志是从哪来的**。

所以本文件只定义三件事，`file.py`（第 8 步）和 `docker.py`（第 9 步）
各自实现它们：

  1. `StreamSpec`   —— 一条物理流长什么样（哪个组件、哪份文件、读的先后）
  2. `StreamReader` —— 怎么从一条流里读、读完进度怎么交代
  3. `SourceAdapter`—— 怎么列出一条流、怎么打开它

本文件真正值钱的部分是 `ComponentReader`：它把"一个组件的多条流"编成一条
**连续编号**的行流。没有它，`id = {component}:{line_no}` 会在
`[主日志, GC 日志, hdfs-audit]` 三条流之间撞号，`finalize` 直接抛
`CasePackCorrupt`。

## 两条不变式（第 8、9 步实现适配器时必须守住）

- **不重不漏**：`AdapterState.offset` 之内的字节不再产出；恢复时先校验
  `tail_line_sha1`，对不上就抛 `CheckpointMismatch`，绝不"猜着接着读"。
- **编号与 adapter 无关**：同一个组件、同一批文件，`file` 与 `docker` 两条
  路径必须产出**同一套** `line_no`。做到这点的方法是：流的先后顺序由
  `StreamSpec.order` 定死（不看时间、不看文件名排序），编号从 1 起按
  读入顺序累加。
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, replace
from typing import Protocol, runtime_checkable

from logcopilot.domain.case import Adapter, AdapterState
from logcopilot.domain.log import COMPONENTS

__all__ = [
    "CheckpointMismatch",
    "ComponentReader",
    "LineAllocator",
    "RawLine",
    "SourceAdapter",
    "StreamReader",
    "StreamSpec",
    "line_sha1",
]

#: canonicalize() 吃的就是它：`(源文件物理行号, 原文)`。
#: 行号是 **1 起的连续整数**，由 `LineAllocator` 分配 —— 注意它不等于
#: 文件里的第几行（一个组件有多份文件时，行号跨文件连续）。
RawLine = tuple[int, str]


class CheckpointMismatch(RuntimeError):
    """续采时发现"源已经不是你上次读的那个文件了"。

    两种触发方式，都会让"接着 offset 往下读"变成读错位置：
      - `inode` 变了（日志轮转、容器重建、bind mount 换了文件）
      - `offset` 之前的最后一行 sha1 对不上（文件被截断或改写过）

    这时**不能**自作聪明从 offset 继续，也不能静默从头再采一遍 ——
    前者产出乱码行，后者产出重复行。正确做法是把选择权交给 `collect.py`：
    要么开新 pack，要么显式 `--restart`。
    """


def line_sha1(text: str) -> str:
    """一行原文的 sha1，用于 `AdapterState.tail_line_sha1`。

    只算**一行文本本身**（不含行尾换行符），这样同一份日志在 Windows
    （CRLF）与 Linux（LF）上算出来的值一致 —— 否则 file 与 docker 两条
    路径的 checkpoint 永远对不上。
    """
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class StreamSpec:
    """一条物理日志流的身份。

    - `component`：归属组件，闭集 `nn / dn1 / dn2 / dn3`（P-A §5）
    - `name`：这条流的角色，如 `main` / `gc` / `audit` / `stdout`。
      它进 checkpoint 的键（`dn1:audit`），所以**不许含冒号**。
    - `order`：同组件内多条流的读取先后。必须由配置写死，不能靠目录遍历
      顺序、更不能靠文件时间 —— 那两样在 file 与 docker 两条路径下必然不同。
    - `path`：文件的物理位置。`file` 适配器用它读盘，`docker` 适配器用它
      组 `docker exec cat <path>` 命令。留 `None` 表示这条流没有对应文件
      （例如纯 `docker logs` 的 stdout）。
    """

    component: str
    name: str
    order: int = 0
    path: str | None = None

    def __post_init__(self) -> None:
        if self.component not in COMPONENTS:
            raise ValueError(
                f"stream.component 非法: {self.component!r}，闭集={sorted(COMPONENTS)}"
            )
        if not isinstance(self.name, str) or not self.name.strip():
            raise ValueError("stream.name 必须是非空字符串")
        if ":" in self.name:
            raise ValueError(
                f"stream.name 不能含冒号: {self.name!r} —— 它要和 component 拼成 "
                "checkpoint 的键（如 dn1:audit）"
            )
        if not isinstance(self.order, int) or isinstance(self.order, bool) or self.order < 0:
            raise ValueError(f"stream.order 必须是非负整数，拿到 {self.order!r}")
        if self.path is not None and (not isinstance(self.path, str) or not self.path):
            raise ValueError(f"stream.path 必须是非空字符串或 None，拿到 {self.path!r}")

    @property
    def key(self) -> str:
        """checkpoint 里这条流的键，形如 `dn1:audit`。"""
        return f"{self.component}:{self.name}"

    @property
    def sort_key(self) -> tuple[int, str]:
        """组件内流的读取顺序。同 `order` 时按 `name` 字典序兜底，
        保证"同样的输入永远得到同样的顺序"，否则 V4 会随实现细节漂移。"""
        return (self.order, self.name)


@runtime_checkable
class StreamReader(Protocol):
    """一条流的一次读取会话。

    生命周期：`adapter.open(spec)` → `read(state)` 逐行产出 → `state()`
    取走进度。`read` 是**生成器**，边读边改内部进度，所以中途停下也能拿到
    一个可用的 checkpoint（续采就靠这个）。

    `spec` 声明成只读 property，而不是普通属性：真实实现里它是只读属性，
    若这里声明成可变属性，类型检查器会认为"可写"是套件的硬要求而拒绝实现类。
    """

    @property
    def spec(self) -> StreamSpec: ...

    def read(self, state: AdapterState | None = None) -> Iterator[RawLine]:
        """从 `state` 指示的位置往后读，逐行产出 `(line_no 占位, 原文)`。

        这里的行号是**流内的物理行号**（从 1 起），只用于报错定位；
        进 `logs.jsonl` 的 `id` 由 `ComponentReader` 重新编号。
        `state` 为 None 表示从头读。
        """
        ...

    def state(self) -> AdapterState:
        """当前进度。已消费到的 offset、行号、inode、最后一行 sha1。"""
        ...


@runtime_checkable
class SourceAdapter(Protocol):
    """一种取数方式（`file` / `docker` / 将来的 `wsl`）。

    `adapter` 与 manifest 的 `source[].adapter` 同值，别自己编字符串。
    """

    adapter: Adapter

    def streams(self) -> Sequence[StreamSpec]:
        """列出这次要采的所有流。顺序无所谓，`ComponentReader` 会按
        `StreamSpec.sort_key` 自己排 —— 但返回的集合必须稳定。"""
        ...

    def open(self, spec: StreamSpec) -> StreamReader:
        """打开一条流。打开时**不读**任何字节（生成器在 `read` 里才动）。"""
        ...


class LineAllocator:
    """给一个组件的所有流分配连续 `line_no`。

    起点是"checkpoint 里所有流的 `line_no` 最大值 + 1"，不是某一条流自己的
    `line_no + 1` —— 因为续采可能发生在"主日志已经读到 500 行、GC 日志读到
    520 行"的状态，这时再从主日志读，编号必须从 521 起，否则会与 GC 已占用的
    501–520 撞号。
    """

    __slots__ = ("_next",)

    def __init__(self, checkpoint: Mapping[str, AdapterState] | None = None) -> None:
        highest = 0
        for state in (checkpoint or {}).values():
            highest = max(highest, state.line_no)
        self._next = highest + 1

    @property
    def next_line_no(self) -> int:
        return self._next

    def take(self) -> int:
        value = self._next
        self._next += 1
        return value


class ComponentReader:
    """把一个组件的多条物理流，编成一条**连续编号**的行流。

    这是 `id` 唯一性的守门人。用法::

        reader = ComponentReader(adapter, "dn1", checkpoint.adapter_state)
        for line_no, text in reader:
            ...              # line_no 从 1 起，跨文件连续
        new_state = reader.checkpoint()   # 每条流各自的 {offset, line_no, ...}

    为什么可以"按流顺序一条读完再读下一条"，而不用按时间交错：因为
    `store/casepack.finalize` 会按 `(ts, id)` 重排。采集中间保持物理顺序
    （可续采、可解释），时间顺序在落盘前一次性整理。
    """

    __slots__ = (
        "_active_last",
        "_active_reader",
        "_adapter",
        "_allocator",
        "_component",
        "_specs",
        "_states",
    )

    def __init__(
        self,
        adapter: SourceAdapter,
        component: str,
        checkpoint: Mapping[str, AdapterState] | None = None,
    ) -> None:
        if component not in COMPONENTS:
            raise ValueError(f"component 非法: {component!r}，闭集={sorted(COMPONENTS)}")
        self._adapter = adapter
        self._component = component
        self._specs: tuple[StreamSpec, ...] = tuple(
            sorted(
                (spec for spec in adapter.streams() if spec.component == component),
                key=lambda spec: spec.sort_key,
            )
        )
        self._states: dict[str, AdapterState] = dict(checkpoint or {})
        self._allocator = LineAllocator(self._states)
        #: 正在读的那条流。采集可能被 Ctrl-C 或时间窗结束打断，
        #: 这时 checkpoint() 必须能说出"读到这条流的第几号了"，
        #: 否则续采只能退回到上一条流的末尾，把已读的行重采一遍。
        self._active_reader: StreamReader | None = None
        self._active_last: int = 0

    @property
    def specs(self) -> tuple[StreamSpec, ...]:
        """本组件这次要读的流，已按读取顺序排好。"""
        return self._specs

    def __iter__(self) -> Iterator[RawLine]:
        for spec in self._specs:
            reader = self._adapter.open(spec)
            previous = self._states.get(spec.key)
            self._active_reader = reader
            self._active_last = previous.line_no if previous is not None else 0
            try:
                for _physical_line_no, text in reader.read(previous):
                    self._active_last = self._allocator.take()
                    yield self._active_last, text
            except GeneratorExit:
                # 采集被中途叫停（break / close() / Ctrl-C）：
                # 先把已读到的位置记下来，再把异常照原样抛出。
                self._record(reader)
                raise
            else:
                self._record(reader)
            finally:
                self._active_reader = None

    def _record(self, reader: StreamReader) -> None:
        """把一条流的进度写进快照。

        **改写**适配器报上来的行号是这里的关键：适配器只管自己这条流的物理
        行号（读到第几行），它没有、也不该有"全局编号"的概念 —— 否则
        file 与 docker 两条路径各算一套，V4 必挂。
        一条流这次一行都没读（上次就早已读完）时，`_active_last` 保留原值，
        这样 `max(line_no)` 不会被抹回 0、续采才不会撞号。
        """
        self._states[reader.spec.key] = replace(reader.state(), line_no=self._active_last)

    def checkpoint(self) -> dict[str, AdapterState]:
        """当前进度快照，直接塞进 `Checkpoint.adapter_state`。

        正在读的流也会出现在里面（用适配器的实时进度），所以采集中断时
        快照仍然是"读到哪了"的真实答案，而不是"上一条流读完在哪"。

        注意只含**本组件**的键；多组件采集时由 `collect.py` 合并各
        `ComponentReader` 的返回值。
        """
        states = dict(self._states)
        if self._active_reader is not None:
            states[self._active_reader.spec.key] = replace(
                self._active_reader.state(), line_no=self._active_last
            )
        return states
