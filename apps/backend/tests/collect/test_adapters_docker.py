"""`collect/adapters/docker.py` 的契约测试。

两条主线：

1. **与 file 侧语义对齐**：三查、半行、offset 落在行边界 —— 每条规则都有
   一个对应的 FileReader 测试，名字刻意取得对称。
2. **双路径对拍**（V4 的种子）：同一批字节，一份写成真实文件、一份塞进假
   容器，两个适配器读出来的 `(行号, 原文)` 与 checkpoint **必须完全一致**。
   这条测试是这一步真正的验收 —— 它一红，说明两条路径已经开始分叉。

`docker` 命令用 `FakeContainer` 回放：只实现本适配器实际用到的那四条
（ls / stat / tail / sh），不依赖 Docker 是否在运行。真容器联调放到第 12 步
（infra）之后。
"""

from __future__ import annotations

import shlex
from collections.abc import Sequence
from pathlib import Path

import pytest

from logcopilot.collect.adapters.base import (
    AmbiguousStream,
    CheckpointMismatch,
    ComponentReader,
    StreamSpec,
    line_sha1,
)
from logcopilot.collect.adapters.docker import (
    DockerAdapter,
    DockerCommandError,
    DockerReader,
    DockerStream,
    container_streams,
)
from logcopilot.collect.adapters.file import FileAdapter, FileStream
from logcopilot.domain.case import Adapter, AdapterState

LOG_DIR = "/opt/hadoop/logs"


class FakeContainer:
    """一个内存里的容器：只认本适配器用到的那几条命令。

    命令解析全部用 `shlex.split`，所以 `shlex.quote` 包出来的带空格路径
    在测试里也走同一条解析路径 —— 不会出现"测试里能过、真容器里被空格
    打散"的情况。
    """

    def __init__(self, container: str = "dn1") -> None:
        self.container = container
        self.files: dict[str, bytes] = {}
        self.inodes: dict[str, int] = {}
        self.calls: list[list[str]] = []

    # 造数据 ------------------------------------------------------------

    def put(self, name: str, data: str | bytes, *, inode: int = 1) -> str:
        path = f"{LOG_DIR}/{name}"
        self.files[path] = data.encode("utf-8") if isinstance(data, str) else data
        self.inodes[path] = inode
        return path

    def append(self, name: str, data: str) -> None:
        self.files[f"{LOG_DIR}/{name}"] += data.encode("utf-8")

    # 执行 --------------------------------------------------------------

    def __call__(self, argv: Sequence[str]) -> bytes:
        self.calls.append(list(argv))
        assert list(argv[:2]) == ["exec", self.container], argv
        rest = list(argv[2:])
        command, args = rest[0], rest[1:]
        match command:
            case "ls":
                return self._ls(args[-1])
            case "stat":
                return self._stat(args[-1])
            case "tail":
                return self._tail(args[1], args[-1])
            case "sh":
                return self._sh(args[1])
            case _:
                raise AssertionError(f"FakeContainer 不认识这条命令: {command}")

    def _ls(self, directory: str) -> bytes:
        names = sorted(
            path.rsplit("/", 1)[1] for path in self.files if path.rsplit("/", 1)[0] == directory
        )
        return ("\n".join(names) + "\n").encode()

    def _stat(self, path: str) -> bytes:
        if path not in self.files:
            raise DockerCommandError(f"stat {path}: No such file or directory")
        return f"{self.inodes[path]} {len(self.files[path])}".encode()

    def _tail(self, position: str, path: str) -> bytes:
        if path not in self.files:
            raise DockerCommandError(f"tail {path}: No such file or directory")
        offset = int(position.lstrip("+")) - 1
        return self.files[path][offset:]

    def _sh(self, script: str) -> bytes:
        tokens = shlex.split(script)
        # tail -c +N '<path>' | head -c L
        assert tokens[0] == "tail" and tokens[1] == "-c", tokens
        start = int(tokens[2].lstrip("+")) - 1
        path = tokens[3]
        assert tokens[4] == "|" and tokens[5] == "head" and tokens[6] == "-c", tokens
        length = int(tokens[7])
        return self.files[path][start : start + length]


def adapter_for(
    container: FakeContainer, component: str = "dn1", **kwargs: object
) -> DockerAdapter:
    return DockerAdapter(component, component, runner=container, **kwargs)  # type: ignore[arg-type]


def spec_for(path: str, *, component: str = "dn1", name: str = "main") -> StreamSpec:
    return StreamSpec(component=component, name=name, order=0, path=path)


# ------------------------------------------------------------------ 声明


def test_docker_stream_rejects_slash_in_pattern() -> None:
    """容器内 pattern 只能是文件名，目录由 log_dir 决定。"""
    with pytest.raises(ValueError, match="不含 /"):
        DockerStream("main", 0, "sub/x.log")


def test_docker_stream_rejects_empty_pattern() -> None:
    with pytest.raises(ValueError, match="pattern"):
        DockerStream("main", 0, " ")


def test_docker_stream_rejects_bad_name_and_order() -> None:
    with pytest.raises(ValueError, match="冒号"):
        DockerStream("dn1:main", 0, "*.log")
    with pytest.raises(ValueError, match="order"):
        DockerStream("main", -1, "*.log")


def test_container_streams_match_the_file_side_layout() -> None:
    nn = {stream.name: stream.order for stream in container_streams("nn")}
    dn = {stream.name: stream.order for stream in container_streams("dn1")}
    assert nn == {"main": 0, "audit": 1, "gc": 2, "stdout": 3}
    assert dn == {"main": 0, "gc": 2, "stdout": 3}


def test_adapter_declares_docker_kind() -> None:
    assert adapter_for(FakeContainer()).adapter is Adapter.DOCKER


# ------------------------------------------------------------------ 解析成具体文件


def test_streams_lists_files_and_builds_posix_paths() -> None:
    container = FakeContainer()
    container.put("hadoop-hdfs-datanode-nn.log", "x\n")
    container.put("gc.log", "g\n")
    container.put("dn1.out", "o\n")
    specs = adapter_for(container).streams()
    assert [spec.name for spec in specs] == ["main", "gc", "stdout"]
    assert [spec.path for spec in specs] == [
        f"{LOG_DIR}/hadoop-hdfs-datanode-nn.log",
        f"{LOG_DIR}/gc.log",
        f"{LOG_DIR}/dn1.out",
    ]


def test_streams_skips_missing_files() -> None:
    container = FakeContainer()
    container.put("hadoop-hdfs-datanode-nn.log", "x\n")
    assert [spec.name for spec in adapter_for(container).streams()] == ["main"]


def test_streams_raises_on_ambiguous_pattern() -> None:
    container = FakeContainer()
    container.put("a.out", "a\n")
    container.put("b.out", "b\n")
    with pytest.raises(AmbiguousStream, match="匹配到 2 个文件"):
        adapter_for(container).streams()


def test_missing_container_surfaces_as_docker_error() -> None:
    """容器不存在时 `docker exec` 直接失败 —— 布局阶段就该炸，而不是读成空。"""

    def broken(_argv: Sequence[str]) -> bytes:
        raise DockerCommandError("Error: No such container: dn1")

    adapter = DockerAdapter("dn1", "dn1", runner=broken)
    with pytest.raises(DockerCommandError, match="No such container"):
        adapter.streams()


def test_nn_adapter_expects_audit_log() -> None:
    container = FakeContainer("nn")
    container.put("hadoop-hdfs-namenode-nn.log", "x\n")
    container.put("hdfs-audit.log", "a\n")
    specs = adapter_for(container, "nn").streams()
    assert [spec.name for spec in specs] == ["main", "audit"]


# ------------------------------------------------------------------ 读


def test_empty_file_yields_nothing() -> None:
    container = FakeContainer()
    path = container.put("main.log", "")
    reader = DockerReader(spec_for(path), "dn1", container)
    assert list(reader.read()) == []
    state = reader.state()
    assert state.offset == 0
    assert state.line_no == 0
    assert state.inode == 1


def test_reads_complete_lines_and_reports_state() -> None:
    container = FakeContainer()
    path = container.put("main.log", "l1\nl2\nl3\n")
    reader = DockerReader(spec_for(path), "dn1", container)
    assert list(reader.read()) == [(1, "l1"), (2, "l2"), (3, "l3")]
    state = reader.state()
    assert state.offset == len(b"l1\nl2\nl3\n")
    assert state.tail_line_sha1 == line_sha1("l3")


def test_crlf_is_stripped() -> None:
    container = FakeContainer()
    path = container.put("main.log", "l1\r\nl2\r\n")
    assert [text for _, text in DockerReader(spec_for(path), "dn1", container).read()] == [
        "l1",
        "l2",
    ]


def test_invalid_utf8_is_replaced() -> None:
    container = FakeContainer()
    path = container.put("main.log", b"ok\n\xb0\xa1\nend\n")
    lines = list(DockerReader(spec_for(path), "dn1", container).read())
    assert [text for _, text in lines] == ["ok", "\ufffd\ufffd", "end"]


def test_incomplete_tail_line_is_not_consumed() -> None:
    container = FakeContainer()
    path = container.put("main.log", "l1\nl2\nhalf")
    reader = DockerReader(spec_for(path), "dn1", container)
    assert list(reader.read()) == [(1, "l1"), (2, "l2")]
    assert reader.state().offset == len(b"l1\nl2\n")


def test_offset_stays_on_line_boundary() -> None:
    container = FakeContainer()
    path = container.put("main.log", "a\nbb\nccc\n")
    reader = DockerReader(spec_for(path), "dn1", container)
    iterator = reader.read()
    next(iterator)
    assert reader.state().offset == 2  # 正好在 "bb\n" 之前
    next(iterator)
    assert reader.state().offset == 5  # 正好在 "ccc\n" 之前


def test_resume_continues_after_offset() -> None:
    container = FakeContainer()
    path = container.put("main.log", "l1\nl2\n")
    first = DockerReader(spec_for(path), "dn1", container)
    list(first.read())

    container.append("main.log", "l3\n")
    second = DockerReader(spec_for(path), "dn1", container)
    assert list(second.read(first.state())) == [(3, "l3")]


def test_resume_rejects_wrong_tail() -> None:
    container = FakeContainer()
    path = container.put("main.log", "l1\nl2\n")
    state = AdapterState(offset=3, line_no=1, inode=None, tail_line_sha1=line_sha1("WRONG"))
    with pytest.raises(CheckpointMismatch, match="tail 对不上"):
        list(DockerReader(spec_for(path), "dn1", container).read(state))


def test_resume_rejects_changed_inode() -> None:
    container = FakeContainer()
    path = container.put("main.log", "l1\n")
    state = AdapterState(offset=3, line_no=1, inode=42, tail_line_sha1=line_sha1("l1"))
    with pytest.raises(CheckpointMismatch, match="inode 变了"):
        list(DockerReader(spec_for(path), "dn1", container).read(state))


def test_resume_rejects_truncated_file() -> None:
    container = FakeContainer()
    path = container.put("main.log", "l1\n")
    state = AdapterState(offset=9999, line_no=1)
    with pytest.raises(CheckpointMismatch, match="超过容器内文件大小"):
        list(DockerReader(spec_for(path), "dn1", container).read(state))


def test_missing_file_is_reported_as_checkpoint_mismatch() -> None:
    """文件被删和容器挂掉在容器外无法区分，统一翻译成 CheckpointMismatch 并带上原因。"""
    container = FakeContainer()
    with pytest.raises(CheckpointMismatch, match="无法读取容器"):
        list(DockerReader(spec_for(f"{LOG_DIR}/gone.log"), "dn1", container).read())


def test_spec_without_path_is_rejected() -> None:
    with pytest.raises(ValueError, match="spec.path 非空"):
        DockerReader(StreamSpec("dn1", "main", 0), "dn1", FakeContainer())


def test_tail_check_uses_one_pipeline_call() -> None:
    """续采校验不该把整个文件拉回来：只发一条 `sh -c "tail | head"`。"""
    container = FakeContainer()
    path = container.put("main.log", "l1\nl2\n")
    first = DockerReader(spec_for(path), "dn1", container)
    list(first.read())
    container.calls.clear()

    list(DockerReader(spec_for(path), "dn1", container).read(first.state()))
    # 三条命令：stat（三查要的 inode/size）→ sh（tail 校验）→ tail（真正读剩余字节）
    assert [call[2] for call in container.calls] == ["stat", "sh", "tail"]
    assert container.calls[1][2] == "sh"


# ------------------------------------------------------------------ 与 ComponentReader 配合


def test_component_reader_numbers_across_two_streams() -> None:
    container = FakeContainer()
    container.put("hadoop-hdfs-datanode-a.log", "m1\nm2\n")
    container.put("gc.log", "g1\n")
    adapter = adapter_for(
        container,
        streams=(
            DockerStream("main", 0, "hadoop-hdfs-*.log"),
            DockerStream("gc", 2, "gc.log"),
        ),
    )
    assert [no for no, _ in ComponentReader(adapter, "dn1")] == [1, 2, 3]


def test_component_reader_resume_keeps_numbering() -> None:
    container = FakeContainer()
    container.put("hadoop-hdfs-datanode-a.log", "m1\nm2\n")
    container.put("gc.log", "g1\n")
    adapter = adapter_for(
        container,
        streams=(
            DockerStream("main", 0, "hadoop-hdfs-*.log"),
            DockerStream("gc", 2, "gc.log"),
        ),
    )
    first = ComponentReader(adapter, "dn1")
    list(first)
    checkpoint = first.checkpoint()

    container.append("hadoop-hdfs-datanode-a.log", "m3\n")
    assert list(ComponentReader(adapter, "dn1", checkpoint)) == [(4, "m3")]


# ------------------------------------------------------------------ 双路径对拍（V4 的种子）


def test_file_and_docker_readers_are_byte_identical(tmp_path: Path) -> None:
    """同一批字节，file 与 docker 读出来必须一模一样。

    这是 V4（两条路径 canonical_sha256 相等）在适配器层面的铺垫：
    文本、行号、offset、tail sha1 四项全对齐，后面 canonicalize + 打包
    才有机会算出同一个哈希。
    """
    payload = "l1 INFO a\nl2 WARN b\nl3 ERROR c\n半行被截断"
    path = tmp_path / "dn1" / "main.log"
    path.parent.mkdir(parents=True)
    path.write_bytes(payload.encode("utf-8"))

    container = FakeContainer()
    container.put("main.log", payload)

    file_adapter = FileAdapter(tmp_path, [FileStream("dn1", "main", 0, "dn1/main.log")])
    docker_adapter = adapter_for(container, streams=(DockerStream("main", 0, "main.log"),))

    from_file = ComponentReader(file_adapter, "dn1")
    from_docker = ComponentReader(docker_adapter, "dn1")
    file_lines = list(from_file)
    docker_lines = list(from_docker)

    assert file_lines == docker_lines
    assert [text for _, text in file_lines] == ["l1 INFO a", "l2 WARN b", "l3 ERROR c"]

    file_state = from_file.checkpoint()["dn1:main"]
    docker_state = from_docker.checkpoint()["dn1:main"]
    assert file_state.offset == docker_state.offset
    assert file_state.line_no == docker_state.line_no
    assert file_state.tail_line_sha1 == docker_state.tail_line_sha1
    # inode 允许不同（一个是宿主机的，一个是容器里的），它不是内容的一部分
    assert file_state.inode != docker_state.inode or file_state.inode is not None


def test_file_and_docker_agree_on_half_line(tmp_path: Path) -> None:
    """半行在两条路径上必须同时"看不见"——否则一条路径少一行，V4 必红。"""
    payload = "a\nb\nc-trailing"
    path = tmp_path / "main.log"
    path.write_bytes(payload.encode())
    container = FakeContainer()
    container.put("main.log", payload)

    from_file = list(
        ComponentReader(FileAdapter(tmp_path, [FileStream("dn1", "main", 0, "main.log")]), "dn1")
    )
    from_docker = list(
        ComponentReader(
            adapter_for(container, streams=(DockerStream("main", 0, "main.log"),)), "dn1"
        )
    )
    assert from_file == from_docker == [(1, "a"), (2, "b")]
