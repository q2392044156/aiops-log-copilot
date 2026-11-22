"""`collect/adapters/file.py` 的契约测试。

全部在 `tmp_path` 里造真文件 —— 这个适配器的价值就是"对着真实文件把三种
边界做对"，用假数据源测它等于什么也没测。

测的是四组行为：
  1. 布局：`FileStream` 的声明怎么解析成具体文件（缺文件/多文件）
  2. 读：完整行、CRLF、非 UTF-8 字节、空文件
  3. 半行：没有换行符的最后一行绝不消费，补上换行符之后才消费
  4. 续采：offset / inode / tail_line_sha1 三种失效方式各自抛 `CheckpointMismatch`
"""

from __future__ import annotations

from pathlib import Path

import pytest

from logcopilot.collect.adapters.base import (
    CheckpointMismatch,
    ComponentReader,
    StreamSpec,
    line_sha1,
)
from logcopilot.collect.adapters.file import (
    AmbiguousStream,
    FileAdapter,
    FileReader,
    FileStream,
    hdfs_streams,
    sha256_file_head,
)
from logcopilot.domain.case import Adapter, AdapterState


def write(path: Path, text: str | bytes) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(text, str):
        path.write_bytes(text.encode("utf-8"))
    else:
        path.write_bytes(text)
    return path


def spec_for(path: Path, *, component: str = "dn1", name: str = "main") -> StreamSpec:
    return StreamSpec(component=component, name=name, order=0, path=str(path))


# ------------------------------------------------------------------ 布局声明


def test_file_stream_rejects_unknown_component() -> None:
    with pytest.raises(ValueError, match="component 非法"):
        FileStream("dn9", "main", 0, "dn9/x.log")


def test_file_stream_rejects_empty_pattern() -> None:
    with pytest.raises(ValueError, match="pattern"):
        FileStream("dn1", "main", 0, "  ")


def test_file_stream_matches_sorted(tmp_path: Path) -> None:
    write(tmp_path / "a/x.log", "x\n")
    write(tmp_path / "a/y.log", "y\n")
    stream = FileStream("dn1", "main", 0, "a/*.log")
    assert [path.name for path in stream.matches(tmp_path)] == ["x.log", "y.log"]


def test_hdfs_streams_cover_the_three_live_sources() -> None:
    """P-A §1 的清单减去 GC：GC 要等 `jvm_gc` 档位（O8），现在收进来会污染主日志末条记录。"""
    layout = hdfs_streams()
    by_component: dict[str, list[str]] = {}
    for stream in layout:
        by_component.setdefault(stream.component, []).append(f"{stream.order}:{stream.name}")
    assert by_component["nn"] == ["0:main", "1:audit", "2:stdout"]
    assert by_component["dn1"] == ["0:main", "2:stdout"]


def test_hdfs_streams_do_not_collect_gc_log() -> None:
    """显式钉住"不采 GC"这件事，否则它会被悄悄加回来、把 GC 行拼进上一条日志。"""
    assert not [stream for stream in hdfs_streams() if stream.name == "gc"]


# ------------------------------------------------------------------ 解析成具体文件


def test_adapter_skips_missing_files(tmp_path: Path) -> None:
    """容器还没写日志 = 这条流这次不存在，跳过，不报错。"""
    write(tmp_path / "dn1/hadoop-hdfs-datanode-a.log", "x\n")
    adapter = FileAdapter(tmp_path, hdfs_streams(("dn1",)))
    specs = adapter.streams()
    assert [spec.name for spec in specs] == ["main"]
    assert Path(specs[0].path or "").is_absolute()


def test_adapter_raises_on_ambiguous_pattern(tmp_path: Path) -> None:
    """两个文件都匹配 = 配置错，随便挑一个会让两代日志混进同一个 id 空间。"""
    write(tmp_path / "dn1/x.out", "a\n")
    write(tmp_path / "dn1/y.out", "b\n")
    adapter = FileAdapter(tmp_path, [FileStream("dn1", "stdout", 3, "dn1/*.out")])
    with pytest.raises(AmbiguousStream, match="匹配到 2 个文件"):
        adapter.streams()


def test_adapter_declares_file_kind() -> None:
    assert FileAdapter(Path(".")).adapter is Adapter.FILE


# ------------------------------------------------------------------ 读


def test_empty_file_yields_nothing(tmp_path: Path) -> None:
    path = write(tmp_path / "empty.log", "")
    reader = FileReader(spec_for(path))
    assert list(reader.read()) == []
    state = reader.state()
    assert state.offset == 0
    assert state.line_no == 0
    assert state.inode is not None
    assert state.tail_line_sha1 is None


def test_reads_complete_lines_and_reports_state(tmp_path: Path) -> None:
    path = write(tmp_path / "a.log", "l1\nl2\nl3\n")
    reader = FileReader(spec_for(path))
    lines = list(reader.read())
    assert lines == [(1, "l1"), (2, "l2"), (3, "l3")]
    state = reader.state()
    assert state.offset == path.stat().st_size
    assert state.line_no == 3
    assert state.tail_line_sha1 == line_sha1("l3")


def test_crlf_is_stripped(tmp_path: Path) -> None:
    path = write(tmp_path / "a.log", "l1\r\nl2\r\n")
    assert [text for _, text in FileReader(spec_for(path)).read()] == ["l1", "l2"]


def test_invalid_utf8_does_not_crash(tmp_path: Path) -> None:
    """日志里混 GBK 中文、二进制块名是常态，宁可留替换字符也不能让采集崩掉。"""
    path = write(tmp_path / "a.log", b"ok\n\xb0\xa1\xb0\xa1\nend\n")
    lines = list(FileReader(spec_for(path)).read())
    assert len(lines) == 3
    assert lines[1][1] == "\ufffd\ufffd\ufffd\ufffd"
    assert lines[2][1] == "end"


def test_chinese_and_multibyte_survive(tmp_path: Path) -> None:
    path = write(tmp_path / "a.log", "开始\n中间\n结束\n")
    assert [text for _, text in FileReader(spec_for(path)).read()] == ["开始", "中间", "结束"]


# ------------------------------------------------------------------ 半行


def test_incomplete_tail_line_is_not_consumed(tmp_path: Path) -> None:
    """`tail -f` 正在写的半行：不消费。否则下次续采从半个单词开始。"""
    path = write(tmp_path / "a.log", "l1\nl2\nl3-partial")
    reader = FileReader(spec_for(path))
    assert list(reader.read()) == [(1, "l1"), (2, "l2")]
    state = reader.state()
    assert state.offset == len(b"l1\nl2\n")  # 停在半行之前
    assert state.tail_line_sha1 == line_sha1("l2")


def test_partial_line_is_consumed_once_completed(tmp_path: Path) -> None:
    """半行补全（写入换行符）之后，续采要能把它读出来。"""
    path = write(tmp_path / "a.log", "l1\nl2-step")
    reader = FileReader(spec_for(path))
    list(reader.read())
    state = reader.state()

    write(tmp_path / "a.log", "l1\nl2-step-done\n")
    resumed = FileReader(spec_for(path))
    # 这次读的是文件里的第 2 行（物理行号），进 id 之前由 ComponentReader 改写
    assert list(resumed.read(state)) == [(2, "l2-step-done")]


def test_empty_last_line_is_a_complete_line(tmp_path: Path) -> None:
    """文件以 `\\n` 结尾时，末尾没有"半行"；不许凭空产出一条空记录之外的错。"""
    path = write(tmp_path / "a.log", "l1\n\n")
    assert list(FileReader(spec_for(path)).read()) == [(1, "l1"), (2, "")]


# ------------------------------------------------------------------ 续采


def test_resume_continues_after_offset(tmp_path: Path) -> None:
    path = write(tmp_path / "a.log", "l1\nl2\nl3\n")
    first = FileReader(spec_for(path))
    list(first.read())

    write(tmp_path / "a.log", "l1\nl2\nl3\nl4\n")
    second = FileReader(spec_for(path))
    assert list(second.read(first.state())) == [(4, "l4")]


def test_resume_rejects_wrong_tail(tmp_path: Path) -> None:
    path = write(tmp_path / "a.log", "l1\nl2\n")
    state = AdapterState(
        offset=len(b"l1\n"), line_no=1, inode=None, tail_line_sha1=line_sha1("WRONG")
    )
    with pytest.raises(CheckpointMismatch, match="tail 对不上"):
        list(FileReader(spec_for(path)).read(state))


def test_resume_rejects_changed_inode(tmp_path: Path) -> None:
    path = write(tmp_path / "a.log", "l1\nl2\n")
    state = AdapterState(offset=len(b"l1\n"), line_no=1, inode=1, tail_line_sha1=line_sha1("l1"))
    with pytest.raises(CheckpointMismatch, match="inode 变了"):
        list(FileReader(spec_for(path)).read(state))


def test_resume_rejects_truncated_file(tmp_path: Path) -> None:
    path = write(tmp_path / "a.log", "l1\n")
    state = AdapterState(offset=9999, line_no=1, inode=None, tail_line_sha1=None)
    with pytest.raises(CheckpointMismatch, match="超过文件大小"):
        list(FileReader(spec_for(path)).read(state))


def test_missing_file_is_a_checkpoint_mismatch(tmp_path: Path) -> None:
    """文件在 streams() 之后被删：不能静默产出 0 行，那会让"漏采"看起来像"没日志"。"""
    missing = tmp_path / "gone.log"
    with pytest.raises(CheckpointMismatch, match="文件不存在"):
        list(FileReader(spec_for(missing)).read())


def test_spec_without_path_is_rejected() -> None:
    with pytest.raises(ValueError, match="spec.path 非空"):
        FileReader(StreamSpec("dn1", "main", 0))


def test_state_of_fresh_reader_is_empty(tmp_path: Path) -> None:
    state = FileReader(spec_for(tmp_path / "a.log")).state()
    assert state.offset == 0
    assert state.line_no == 0
    assert state.inode is None


# ------------------------------------------------------------------ 与 ComponentReader 配合


def test_component_reader_numbers_across_two_files(tmp_path: Path) -> None:
    """主日志 2 行 + GC 1 行：编号 1..3 连续，跨文件不撞号。"""
    write(tmp_path / "dn1/main.log", "m1\nm2\n")
    write(tmp_path / "dn1/gc.log", "g1\n")
    adapter = FileAdapter(
        tmp_path,
        [FileStream("dn1", "main", 0, "dn1/main.log"), FileStream("dn1", "gc", 1, "dn1/gc.log")],
    )
    assert [no for no, _ in ComponentReader(adapter, "dn1")] == [1, 2, 3]


def test_component_reader_resume_keeps_numbering(tmp_path: Path) -> None:
    write(tmp_path / "dn1/main.log", "m1\nm2\n")
    write(tmp_path / "dn1/gc.log", "g1\n")
    adapter = FileAdapter(
        tmp_path,
        [FileStream("dn1", "main", 0, "dn1/main.log"), FileStream("dn1", "gc", 1, "dn1/gc.log")],
    )
    first = ComponentReader(adapter, "dn1")
    list(first)
    checkpoint = first.checkpoint()
    assert checkpoint["dn1:main"].line_no == 2
    assert checkpoint["dn1:gc"].line_no == 3

    write(tmp_path / "dn1/main.log", "m1\nm2\nm3\n")
    resumed = list(ComponentReader(adapter, "dn1", checkpoint))
    assert resumed == [(4, "m3")]


def test_half_line_does_not_break_numbering(tmp_path: Path) -> None:
    """一条流末尾有半行时，编号不能跳到下一段——下一次采集要接着 3。"""
    write(tmp_path / "dn1/main.log", "m1\nm2\nhalf")
    adapter = FileAdapter(tmp_path, [FileStream("dn1", "main", 0, "dn1/main.log")])
    reader = ComponentReader(adapter, "dn1")
    assert list(reader) == [(1, "m1"), (2, "m2")]
    assert reader.checkpoint()["dn1:main"].line_no == 2


# ------------------------------------------------------------------ 指纹原料


def test_sha256_file_head_is_stable_and_sized(tmp_path: Path) -> None:
    path = write(tmp_path / "a.log", "abcdef")
    assert sha256_file_head(path, 3) == sha256_file_head(path, 3)
    assert sha256_file_head(path, 3) != sha256_file_head(path, 6)
    assert len(sha256_file_head(path, 6)) == 64
