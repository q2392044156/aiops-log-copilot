# 顺序 0c · 取数（adapter）实现：docker —— 和 file 读出逐字节相同的内容
"""`collect/adapters/docker.py` —— 第二种取数方式，走 `docker exec`。

## 为什么要有第二条路径

P-A 的红线 V4：同一个案例，用 `file` 采一遍、用 `docker` 采一遍，
`canonical_sha256` 必须相等。这条红线不是为了"多一种玩法"，而是为了证明
**我们的日志理解不依赖于日志怎么被搬出来** —— 一旦 docker 路径也能算出
同一个哈希，就说明偏移、行边界、解码这三件事没有一处依赖特定运输方式。

## 关键设计：把 docker 当作"远端文件系统"，而不是"日志流"

`docker logs` 看起来最像"取日志"，但它**给不出字节偏移**：只能
`--since <时间戳>`、只能从头 `--follow`。这会让续采变成"猜时间戳"，
V3（不重不漏）立即失守。

所以这里不用 `docker logs`，而是用三条命令把容器内文件伪装成本地文件：

| 本地文件操作 | docker 等价命令 |
|---|---|
| `os.stat()`（拿 inode / size） | `docker exec <c> stat -c '%i %s' <path>` |
| `seek(offset)` + 逐行读 | `docker exec <c> tail -c +<offset+1> <path>` |
| 读 offset 前一行做校验 | `docker exec <c> sh -c "tail -c +<start> <path> \\| head -c <len>"` |
| 列目录（解析 glob） | `docker exec <c> ls -1 <log_dir>` |

`tail -c +N` 是 **1 起**的字节位置，所以偏移 `offset` 对应 `+offset+1`。
三查（inode / 大小 / tail sha1）与 `FileReader` 一字不差，语义因此完全对齐。

## 为什么 `stdout` 流不在这里实现

P-A §6.1 写过"`docker logs --since` 取容器 stdout"。但 Hadoop 的日志是**写文件**
的，stdout 基本为空；更要紧的是 `docker logs` 无法按字节续采，接上它会
把 V3 打破。这里的做法是：第 12 步的 compose 里让 stdout 也落成文件
（`... 2>&1 | tee /opt/hadoop/logs/<component>.out`），于是它同样走
`tail -c` 这条路 —— **一个 adapter 只有一种取数语义**，比多一种"特例"更稳。
"""

from __future__ import annotations

import fnmatch
import hashlib
import shlex
import subprocess
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from pathlib import PurePosixPath

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
    "DockerAdapter",
    "DockerCommandError",
    "DockerReader",
    "DockerStream",
    "SubprocessRunner",
    "container_streams",
]

#: 容器里的默认日志目录（`apache/hadoop:3.3.6` 的 `HADOOP_LOG_DIR`）。
DEFAULT_LOG_DIR = "/opt/hadoop/logs"


class DockerCommandError(RuntimeError):
    """`docker` 命令本身失败了（容器不存在、docker 没起、权限不足）。

    与 `CheckpointMismatch` 区分开：那个是"源变了，数据不可信"，这个是
    "工具链坏了，重试可能有用"。两者的处理方式完全不同，混成一个异常
    会让 collect.py 在 docker 没起的时候误报"日志轮转了"。
    """


#: 执行一条 docker 命令并返回 stdout 字节。抽成可注入的，测试才能不依赖 Docker。
Runner = Callable[[Sequence[str]], bytes]


class SubprocessRunner:
    """真的去跑 `docker` 的 runner。超时是必须的 —— 容器卡死时不能把采集挂住。"""

    __slots__ = ("binary", "timeout")

    def __init__(self, binary: str = "docker", timeout: float = 60.0) -> None:
        self.binary = binary
        self.timeout = timeout

    def __call__(self, argv: Sequence[str]) -> bytes:
        try:
            completed = subprocess.run(  # noqa: S603 - argv 由本模块构造，无 shell
                [self.binary, *argv],
                capture_output=True,
                timeout=self.timeout,
                check=False,
            )
        except FileNotFoundError as exc:  # docker 没装
            raise DockerCommandError(
                f"找不到 docker 可执行文件 {self.binary!r} —— 请确认 Docker 已安装并在 PATH 里"
            ) from exc
        except subprocess.TimeoutExpired as exc:
            raise DockerCommandError(f"docker 命令超时（{self.timeout}s）: {list(argv)}") from exc
        if completed.returncode != 0:
            detail = completed.stderr.decode("utf-8", "replace").strip()
            raise DockerCommandError(
                f"docker 命令失败（exit {completed.returncode}）: {list(argv)}\n{detail}"
            )
        return completed.stdout


@dataclass(frozen=True, slots=True)
class DockerStream:
    """容器内一条流的声明。`pattern` 是相对 `log_dir` 的文件名 glob。

    与 `FileStream` 的区别只是"根目录在哪"：`FileStream.pattern` 带
    `dn1/` 这样的子目录，因为宿主机上每个容器一个目录；而 docker 适配器
    **一个实例只服务一个容器**，所以 pattern 就是裸文件名。
    """

    name: str
    order: int
    pattern: str

    def __post_init__(self) -> None:
        # 复用 StreamSpec 的校验（name 非空、不含冒号、order 非负），保证两个
        # 适配器对"什么算合法的流"没有分歧
        StreamSpec(component="dn1", name=self.name, order=self.order)
        if not isinstance(self.pattern, str) or not self.pattern.strip():
            raise ValueError("stream.pattern 必须是非空 glob 字符串")
        if "/" in self.pattern:
            raise ValueError(
                f"docker 流的 pattern 只能是文件名 glob（不含 /），拿到 {self.pattern!r}"
            )


def container_streams(component: str, *, container: str | None = None) -> tuple[DockerStream, ...]:
    """一个容器里那几条日志的默认声明（**必须**与 `hdfs_streams` 逐条对应）。

    这条"逐条对应"不是洁癖，是 V4 的前提：两条路径要产出逐字节相同的
    canonical 投影，就必须读同一批流、同一种顺序。所以这里刻意照抄
    `file.hdfs_streams` 的清单（含 `workload.out`、不含 `gc.log`），两边
    任何一边改了，`scripts/verify_gates.py --v4` 会当场红。

    `nn` 多一条 `hdfs-audit.log` 和一条 `workload.out`；DN 没有审计日志。
    `stdout` 由 compose 里的 `tee` 落成 `<容器名>.out`（见模块头注释），
    所以 pattern 要带上容器名——`*.out` 会把 `workload.out` 一起吞掉，
    同一个组件两条流命中同一个文件会直接 `AmbiguousStream`。
    """
    prefix = container or component
    streams = [DockerStream("main", 0, "hadoop-hdfs-*-*.log")]
    if component == "nn":
        streams.append(DockerStream("audit", 1, "hdfs-audit.log"))
    streams.append(DockerStream("stdout", 2, f"{prefix}.out"))
    if component == "nn":
        # 与 file 侧同一条流：`hdfs dfs` 客户端的 ERROR/WARN（四族里三族的
        # ERROR 只能在这里出现，见 P-A §1）
        streams.append(DockerStream("workload", 3, "workload.out"))
    return tuple(streams)


class DockerReader:
    """一条容器内文件的读取会话（实现 `base.StreamReader`）。

    与 `FileReader` 的**唯一**区别是数据怎么来：每条命令都经过 `runner`。
    行号、偏移、半行、三查的逻辑全部照抄 —— 抄不是偷懒，是要求：两边逻辑
    必须逐字对应，否则 V4 会在某个边界上悄悄分叉。
    """

    __slots__ = (
        "_container",
        "_inode",
        "_line_no",
        "_offset",
        "_path",
        "_runner",
        "_spans",
        "_spec",
        "_tail",
    )

    def __init__(self, spec: StreamSpec, container: str, runner: Runner) -> None:
        if spec.path is None:
            raise ValueError(f"docker 适配器要求 spec.path 非空，{spec.key} 没给路径")
        self._spec = spec
        self._container = container
        self._runner = runner
        self._path = spec.path
        self._offset = 0
        self._line_no = 0
        self._inode: int | None = None
        self._tail: str | None = None
        self._spans: dict[int, tuple[int, int, str]] = {}

    @property
    def spec(self) -> StreamSpec:
        return self._spec

    def read(self, state: AdapterState | None = None) -> Iterator[tuple[int, str]]:
        inode, size = self._stat()
        self._inode = inode

        if state is not None:
            self._offset = state.offset
            self._line_no = state.line_no
            if state.inode is not None and state.inode != inode:
                raise CheckpointMismatch(
                    f"{self._spec.key}: 容器 {self._container} 里 inode 变了 "
                    f"{state.inode} → {inode}（{self._path}），文件已被替换"
                )
            if self._offset > size:
                raise CheckpointMismatch(
                    f"{self._spec.key}: offset={self._offset} 超过容器内文件大小 {size}"
                    f"（{self._path}）—— 被截断过，偏移已失效"
                )
            if state.tail_line_sha1 is not None and self._offset > 0:
                actual = self._tail_before(self._offset)
                if actual != state.tail_line_sha1:
                    raise CheckpointMismatch(
                        f"{self._spec.key}: tail 对不上"
                        f"（记录 {state.tail_line_sha1[:8]}，实际 {actual[:8]}）"
                    )
            self._tail = state.tail_line_sha1

        payload = self._read_from(self._offset)
        pos = 0
        while True:
            newline = payload.find(b"\n", pos)
            if newline < 0:
                return  # 半行：不消费，offset 停在它前面
            raw = payload[pos : newline + 1]
            pos = newline + 1
            text = decode_line(raw)
            start = self._offset
            self._offset += len(raw)
            self._line_no += 1
            self._tail = line_sha1(text)
            self._spans[self._line_no] = (start, len(raw), hashlib.sha256(raw).hexdigest())
            yield self._line_no, text

    def spans(self) -> dict[int, tuple[int, int, str]]:
        """这次读到的每一行在**容器内文件**里的字节区间（同 file 侧口径）。

        `file` 与 `docker` 两条路径的偏移必须逐字节可比（V4），所以这里的
        `offset` 指的是容器里那份文件的偏移；宿主机上的 bind mount 是同一份
        字节，两边算出来的数字应当相同。
        """
        return dict(self._spans)

    def state(self) -> AdapterState:
        return AdapterState(
            offset=self._offset,
            line_no=self._line_no,
            inode=self._inode,
            tail_line_sha1=self._tail,
        )

    # ---------------------------------------------------------- 三条命令

    def _stat(self) -> tuple[int, int]:
        """`stat -c '%i %s'` —— 容器里的 inode 与文件大小。

        失败统一翻译成 `CheckpointMismatch`：文件被删和容器已经挂掉，从容器
        外面**无法区分**（两者都是 `docker exec` 返回非 0）。既然无法区分，
        就不能挑一个"看起来更友好"的异常 —— 那样一次容器崩溃会被记成
        "日志轮转"，进而悄悄开出一个空 pack。原始 docker 错误会拼进消息里，
        排查时一眼能看出是哪种。
        """
        try:
            out = self._runner(["exec", self._container, "stat", "-c", "%i %s", self._path])
        except DockerCommandError as exc:
            raise CheckpointMismatch(
                f"{self._spec.key}: 无法读取容器 {self._container} 内的文件 "
                f"{self._path}（文件被删或容器已停止）—— {exc}"
            ) from exc
        fields = out.decode("ascii", "replace").split()
        if len(fields) != 2:
            raise CheckpointMismatch(
                f"{self._spec.key}: 读不到容器内文件信息（stat 输出 {out!r}），文件可能已被删除"
            )
        try:
            return int(fields[0]), int(fields[1])
        except ValueError as exc:
            raise CheckpointMismatch(
                f"{self._spec.key}: stat 输出无法解析 {out!r}（文件不存在？）"
            ) from exc

    def _read_from(self, offset: int) -> bytes:
        """`tail -c +N`：从第 N 个字节（1 起）一直读到文件尾。"""
        return self._runner(["exec", self._container, "tail", "-c", f"+{offset + 1}", self._path])

    def _tail_before(self, offset: int, *, window: int = 64 * 1024) -> str:
        """offset 之前那一行的 sha1。用一条 shell 管道取一个窗口，别拉整个文件。"""
        start = max(1, offset - window + 1)
        length = offset - start + 1
        script = f"tail -c +{start} {shlex.quote(self._path)} | head -c {length}"
        chunk = self._runner(["exec", self._container, "sh", "-c", script])
        if chunk.endswith(b"\n"):
            chunk = chunk[:-1]
        if not chunk:
            return line_sha1("")
        cut = chunk.rfind(b"\n")
        return line_sha1(decode_line(chunk[cut + 1 :] + b"\n"))


class DockerAdapter:
    """一个容器的取数适配器（实现 `base.SourceAdapter`）。

    注意它的粒度是**一个容器**：`docker exec` 天然以容器为单位，而
    `ComponentReader` 的粒度是**一个组件**。第 10 步的 `collect.py` 会把
    "每个组件一个 adapter" 拼起来，所以这里不处理多容器。
    """

    adapter = Adapter.DOCKER

    __slots__ = ("_component", "_container", "_log_dir", "_runner", "_streams")

    def __init__(
        self,
        container: str,
        component: str,
        *,
        log_dir: str = DEFAULT_LOG_DIR,
        streams: Sequence[DockerStream] | None = None,
        runner: Runner | None = None,
    ) -> None:
        self._container = container
        self._component = component
        self._log_dir = log_dir.rstrip("/")
        self._streams = (
            tuple(streams)
            if streams is not None
            else container_streams(component, container=self._container)
        )
        self._runner: Runner = runner if runner is not None else SubprocessRunner()

    @property
    def container(self) -> str:
        return self._container

    def streams(self) -> tuple[StreamSpec, ...]:
        """列一次目录，把 pattern 解析成具体文件。

        与 file 侧规则一致：0 个 = 跳过（文件还没生成）、多个 = `AmbiguousStream`
        显式失败、正好 1 个 = 正常。
        """
        names = self._list()
        specs: list[StreamSpec] = []
        for stream in self._streams:
            matches = sorted(name for name in names if fnmatch.fnmatchcase(name, stream.pattern))
            if not matches:
                continue
            if len(matches) > 1:
                raise AmbiguousStream(
                    f"{self._component}:{stream.name} 的 pattern {stream.pattern!r} "
                    f"在容器 {self._container} 里匹配到 {len(matches)} 个文件"
                    f"（{', '.join(matches)}）—— 请收窄 pattern"
                )
            specs.append(
                StreamSpec(
                    component=self._component,
                    name=stream.name,
                    order=stream.order,
                    path=str(PurePosixPath(self._log_dir) / matches[0]),
                )
            )
        return tuple(specs)

    def open(self, spec: StreamSpec) -> DockerReader:
        return DockerReader(spec, self._container, self._runner)

    def prime(self) -> dict[str, AdapterState]:
        """所有流跳到文件末尾（"从现在开始采"），口径与 file 侧一致。

        做法：stat 拿 inode/size，	ail -c <size> 把整份文件读回来，再在
        本地按**同一套规则**算 offset / line_no / tail —— 只认完整行。
        读整个文件看着浪费，但 prime 一次采集只做一次，日志上限 60MB；
        换成"只读尾巴"要多加两条与 file 侧不同的规则，两条路径迟早分叉。
        """
        states: dict[str, AdapterState] = {}
        for spec in self.streams():
            assert spec.path is not None
            path = spec.path
            meta = self._runner(["exec", self._container, "stat", "-c", "%i %s", path])
            fields = meta.decode("utf-8", "replace").split()
            if len(fields) < 2:
                raise CheckpointMismatch(f"{self._container}:{path} 的 stat 输出看不懂: {meta!r}")
            inode, size = int(fields[0]), int(fields[1])
            data = b""
            if size:
                data = self._runner(["exec", self._container, "tail", "-c", str(size), path])
            end = data.rfind(b"\n") + 1
            states[spec.key] = AdapterState(
                offset=end,
                line_no=data[:end].count(b"\n"),
                inode=inode,
                tail_line_sha1=last_line_sha1(data[:end]),
            )
        return states

    def _list(self) -> tuple[str, ...]:
        out = self._runner(["exec", self._container, "ls", "-1", self._log_dir])
        return tuple(
            line.strip() for line in out.decode("utf-8", "replace").splitlines() if line.strip()
        )
