# 顺序 0b · 取数（adapter）实现：file —— 读 bind mount 出来的日志文件
"""`collect/adapters/file.py` —— 从磁盘读日志，是 `SourceAdapter` 的第一个真实实现。

## 它为什么必须存在

P-A §2 让容器的 `/opt/hadoop/logs` 通过 bind mount 落到
`runtime/hdfs-logs/<container>/`。这条挂载带来一个非常值钱的性质：
**同一份日志既是"容器里的文件"，也是"宿主机上的文件"**。于是 `file` 与
`docker`（第 9 步，`docker exec cat`）读的是同一批字节 —— 这就是 V4
"两条路径的 `canonical_sha256` 必须相等"的物理基础。本文件负责把这条
路径做对；做对了，V4 才有意义。

## 本文件守住的三条边界（都是追日志时天天遇到的）

1. **半行不消费**：文件末尾没有换行符的那一行，可能是 `tail -f` 正在写的
   半行。消费它 → 产出被截断的日志、offset 推进到行中间、下次续采从半个
   单词开始。所以：没看到 `\\n`，就当这一行不存在，offset 停在它前面。

2. **轮转/截断抛 `CheckpointMismatch`，不猜**：`inode` 变了（rename 轮转、
   容器重建）或文件比 `offset` 还短（truncate），继续按 offset 读只会读到
   错位的内容。这时必须停下来让人决定（开新 pack 还是 `--restart`），
   而不是产出乱码行。

3. **文件还没出现不算错**：容器刚起时日志文件可能还不存在。
   `streams()` 直接跳过它（不列进本次要读的流），而不是抛异常 ——
   否则"采集器比 HDFS 先起来"这种正常时序会变成失败。

## offset 的精确含义

`AdapterState.offset` = **最后一条已完整消费的行的下一个字节**，也就是
"下一行的第一个字节"。它永远落在行边界上，于是 `seek(offset)` 之后
`readline()` 读到的第一行就是完整的下一行。
"""

from __future__ import annotations

import hashlib
import os
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path

from logcopilot.collect.adapters.base import (
    AmbiguousStream,
    CheckpointMismatch,
    StreamSpec,
    decode_line,
    last_line_sha1,
    line_sha1,
)
from logcopilot.domain.case import Adapter, AdapterState

__all__ = [
    "AmbiguousStream",
    "FileAdapter",
    "FileReader",
    "FileStream",
    "hdfs_streams",
]


@dataclass(frozen=True, slots=True)
class FileStream:
    """一条流的**声明**（还没解析成具体文件）。

    `pattern` 是相对 `root` 的 glob：用一个 `*` 把 hostname 这类随环境变化的
    部分挡住（容器自己的 hostname 是随即启动变的），这样同一份配置在
    开发机和 CI 上都能用。

    只用 `*.log` 这种窄 pattern，不要写 `*.log*` —— 后者会命中轮转出来的
    `.log.1`，直接触发 `AmbiguousStream`。
    """

    component: str
    name: str
    order: int
    pattern: str

    def __post_init__(self) -> None:
        # 复用 StreamSpec 的校验规则：错误信息只有一份，避免两处各写一套
        StreamSpec(component=self.component, name=self.name, order=self.order)
        if not isinstance(self.pattern, str) or not self.pattern.strip():
            raise ValueError("stream.pattern 必须是非空 glob 字符串")

    def matches(self, root: Path) -> tuple[Path, ...]:
        """在 `root` 下解析这条流，返回排序后的匹配结果（0 个/1 个/多个）。"""
        return tuple(sorted(root.glob(self.pattern)))


def hdfs_streams(components: Sequence[str] = ("nn", "dn1", "dn2", "dn3")) -> tuple[FileStream, ...]:
    """`apache/hadoop:3.3.6` 容器里那几份日志的默认声明（P-A §1）。

    文件名来自镜像的默认 `HADOOP_LOG_DIR=/opt/hadoop/logs`：

    - `hadoop-hdfs-namenode-<host>.log` / `...-datanode-<host>.log`：主日志
    - `hdfs-audit.log`：审计日志（P-A §1 决定**要采**，它记录了谁在改集群）
    - `*.out`：stdout/stderr，Hadoop 自己的日志走文件，这里通常几乎为空
    - `workload.out`（只在 nn 上）：`hdfs dfs` 客户端日志。它用的是
      `conf/client-log4j.properties`（pattern 与守护进程一致），所以能被
      同一个 `hdfs_log4j` 档位解析 —— 这是客户端 ERROR 唯一能进日志的地方

    **GC 日志（`gc.log`）故意不在这张表里**（O8，见 P-A §10）。原因不是没用，而是
    "现在收进来会污染别的记录"：Java 8 的 GC 行长这样

        2026-11-22T11:46:27.166+0000: 0.123: [GC (Allocation Failure) ...]

    它既不符合 hdfs_log4j 档位（`%d{ISO8601}` 是空格分隔 + 逗号毫秒，没有 `T` 也没有时区后缀），
    也不符合 lab 档位。而续行判据是"这一行没有时间戳就归上一组"，于是 GC 的每一行都会被当成
    主日志最后一条记录的续行拼进去 —— 实测 10 行 GC 输出被并进了最后一条 DataNode 日志。
    要收它，得先加第三个档位 `jvm_gc`；在那之前，`gc.log` 只留在盘上给人看。

    `order` 就是 `ComponentReader` 的读入顺序：主日志 → 审计 → stdout。
    顺序必须写死，不能靠文件名排序（`hadoop-...` 与 `hdfs-audit.log` 的相对
    次序在不同 region 下不一样），更不能靠 mtime。
    """
    layout: list[FileStream] = []
    for component in components:
        if component == "nn":
            main = "hadoop-hdfs-namenode-*.log"
            audit = "hdfs-audit.log"
        else:
            main = "hadoop-hdfs-datanode-*.log"
            audit = None
        layout.append(FileStream(component, "main", 0, f"{component}/{main}"))
        if audit is not None:
            layout.append(FileStream(component, "audit", 1, f"{component}/{audit}"))
        # stdout 的 pattern 必须写全名：`*.out` 会把下面那条 `workload.out` 也
        # 匹配进来（同一个 component 两条流命中同一个文件 → AmbiguousStream）。
        layout.append(FileStream(component, "stdout", 2, f"{component}/{component}.out"))
        if component == "nn":
            # 客户端日志：workload 用 `hdfs dfs` 打出来的 ERROR/WARN 全在这里。
            # 服务端只在"盘坏到 DN 起不来"时才报 ERROR，四族里另外三族的
            # ERROR 只能靠客户端日志补齐（P-A §1 的第四类来源）。
            layout.append(FileStream(component, "workload", 3, f"{component}/workload.out"))
    return tuple(layout)


class FileReader:
    """一条文件流的一次读取会话（实现 `base.StreamReader`）。

    设计上它**不知道** `line_no` 会进 `id`：它报的是"这条文件读到第几行"，
    `ComponentReader` 会把它改写成全局编号。两个编号的职责必须分开，
    否则 file 与 docker 两条路径各算一套，V4 必挂。
    """

    __slots__ = ("_inode", "_line_no", "_offset", "_path", "_spec", "_tail")

    def __init__(self, spec: StreamSpec) -> None:
        if spec.path is None:
            raise ValueError(f"file 适配器要求 spec.path 非空，{spec.key} 没给路径")
        self._spec = spec
        self._path = Path(spec.path)
        self._offset = 0
        self._line_no = 0
        self._inode: int | None = None
        self._tail: str | None = None

    @property
    def spec(self) -> StreamSpec:
        return self._spec

    def read(self, state: AdapterState | None = None) -> Iterator[tuple[int, str]]:
        """从 `state` 指向的行边界往后读，只产出**完整**的行。"""
        if not self._path.exists():
            raise CheckpointMismatch(
                f"{self._spec.key}: 文件不存在 {self._path}。"
                "若它是被删掉的，说明这个源已经不可续采，请开新 pack。"
            )

        stat = self._path.stat()
        self._inode = stat.st_ino
        size = stat.st_size

        if state is not None:
            self._offset = state.offset
            self._line_no = state.line_no

            # ① inode 变了 = 文件被替换（轮转 rename / 容器重建）
            if state.inode is not None and state.inode != self._inode:
                raise CheckpointMismatch(
                    f"{self._spec.key}: inode 变了 {state.inode} → {self._inode}，"
                    f"文件已不是上次读的那一个（{self._path}）"
                )
            # ② 文件比记录的 offset 还短 = 被 truncate 过
            if self._offset > size:
                raise CheckpointMismatch(
                    f"{self._spec.key}: offset={self._offset} 超过文件大小 {size}"
                    f"（{self._path}）—— 文件被截断过，偏移已失效"
                )
            # ③ offset 之前的最后一行必须和记录的一致
            if state.tail_line_sha1 is not None and self._offset > 0:
                actual = self._tail_before(self._offset)
                if actual != state.tail_line_sha1:
                    raise CheckpointMismatch(
                        f"{self._spec.key}: tail 对不上"
                        f"（记录 {state.tail_line_sha1[:8]}，实际 {actual[:8]}）"
                    )
            self._tail = state.tail_line_sha1

        with self._path.open("rb") as handle:
            handle.seek(self._offset)
            while True:
                raw = handle.readline()
                if not raw:
                    return  # 到文件尾
                if not raw.endswith(b"\n"):
                    # 半行：可能是正在写的最后一行，不消费、不推进 offset
                    return
                text = decode_line(raw)
                self._offset += len(raw)
                self._line_no += 1
                self._tail = line_sha1(text)
                yield self._line_no, text

    def state(self) -> AdapterState:
        return AdapterState(
            offset=self._offset,
            line_no=self._line_no,
            inode=self._inode,
            tail_line_sha1=self._tail,
        )

    def _tail_before(self, offset: int, *, window: int = 64 * 1024) -> str:
        """取 `offset` 之前那一行的 sha1，用于续采校验。

        只往回读一个窗口（默认 64KB）——Hadoop 日志单行不会超过这个量级，
        真超过了说明这一行本身有问题，宁可校验失败也不要读整个文件。
        """
        start = max(0, offset - window)
        with self._path.open("rb") as handle:
            handle.seek(start)
            chunk = handle.read(offset - start)
        if chunk.endswith(b"\n"):
            chunk = chunk[:-1]
        if not chunk:
            return line_sha1("")
        cut = chunk.rfind(b"\n")
        return line_sha1(decode_line(chunk[cut + 1 :] + b"\n"))


class FileAdapter:
    """把一组 `FileStream` 声明变成可读的流（实现 `base.SourceAdapter`）。

    用法::

        adapter = FileAdapter(Path("runtime/hdfs-logs"), hdfs_streams())
        for line_no, text in ComponentReader(adapter, "dn1"):
            ...

    `root` 一定要传**绝对路径**：相对路径会随进程 cwd 变化，而采集器可能
    被 make / cron / 容器从不同目录拉起来。
    """

    adapter = Adapter.FILE

    __slots__ = ("_root", "_streams")

    def __init__(self, root: Path, streams: Sequence[FileStream] | None = None) -> None:
        self._root = Path(root)
        self._streams = tuple(streams) if streams is not None else hdfs_streams()

    @property
    def root(self) -> Path:
        return self._root

    def streams(self) -> tuple[StreamSpec, ...]:
        """解析 glob，产出本次真正要读的流。

        匹配不到文件 = 这条流这次不存在（容器还没写日志），**跳过，不报错**。
        匹配到多个 = 配置错了，抛 `AmbiguousStream` 让人去改 pattern。
        """
        specs: list[StreamSpec] = []
        for stream in self._streams:
            matches: tuple[Path, ...] = stream.matches(self._root)
            if len(matches) == 0:
                continue
            if len(matches) > 1:
                names = ", ".join(path.name for path in matches)
                raise AmbiguousStream(
                    f"{stream.component}:{stream.name} 的 pattern "
                    f"{stream.pattern!r} 匹配到 {len(matches)} 个文件（{names}）——"
                    "请把 pattern 收窄，或为归档文件单独声明一条流"
                )
            specs.append(
                StreamSpec(
                    component=stream.component,
                    name=stream.name,
                    order=stream.order,
                    path=str(matches[0].resolve()),
                )
            )
        return tuple(specs)

    def open(self, spec: StreamSpec) -> FileReader:
        return FileReader(spec)

    def prime(self) -> dict[str, AdapterState]:
        """所有流都跳到文件末尾（"从现在开始采"）。

        `line_no` 填**文件里已有的物理行数**：它不是"这个组件已经采了多少条"，
        而是"这个组件的编号发到哪儿了"。两者在"从头采"时相同；在"从末尾采"
        时用物理行数才不会与后面续采的编号撞车。

        `tail_line_sha1` 填最后一行的 sha1：续采时 `FileReader` 会用它校验
        "offset 之前那一行到底是不是我以为的那一行"。
        """
        states: dict[str, AdapterState] = {}
        for spec in self.streams():
            assert spec.path is not None  # `streams()` 一定会填
            path = Path(spec.path)
            data = path.read_bytes()
            # 只认**完整行**：末尾半行不算消费过，offset 停在最后一个 `\n` 之后。
            # 否则续采时 `_tail_before` 会拿到半行，sha1 必然对不上。
            end = data.rfind(b"\n") + 1
            states[spec.key] = AdapterState(
                offset=end,
                line_no=data[:end].count(b"\n"),
                inode=file_inode(path),
                tail_line_sha1=last_line_sha1(data[:end]),
            )
        return states


def file_inode(path: Path) -> int:
    """给测试与诊断用的 inode 读取（Windows 上 `st_ino` 也可靠）。"""
    return os.stat(path).st_ino


def sha256_file_head(path: Path, size: int) -> str:
    """读文件前 `size` 字节的 sha256 —— `input_fingerprint` 的原料之一。

    放在这里是因为它只依赖"文件字节"，不依赖解析；第 10 步的
    `collect/collect.py` 会在 finalize 时用它算原始日志的指纹。
    """
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        remaining = size
        while remaining > 0:
            chunk = handle.read(min(1 << 20, remaining))
            if not chunk:
                break
            digest.update(chunk)
            remaining -= len(chunk)
    return digest.hexdigest()
