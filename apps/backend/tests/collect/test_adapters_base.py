"""`collect/adapters/base.py` 的契约测试。

这里测的全是"与具体适配器无关"的部分：流怎么排、号怎么发、checkpoint
怎么交代。`file` / `docker` 两个适配器（第 8、9 步）都复用这套规则，
所以**这套规则一旦错了，两条路径会各错一套**，V4 永远对不齐。

用一个内存里的 `FakeAdapter` 顶替真实文件：本步要验证的是协议，
碰盘只会把"协议错"和"IO 错"混在一起。
"""

from __future__ import annotations

from collections.abc import Generator, Iterator, Sequence
from typing import cast

import pytest

from logcopilot.collect.adapters.base import (
    CheckpointMismatch,
    ComponentReader,
    LineAllocator,
    StreamSpec,
    line_sha1,
)
from logcopilot.domain.case import Adapter, AdapterState


class FakeReader:
    """内存里的流：`lines` 就是文件内容，`offset` 用"已读行数"代替字节偏移。

    真实适配器的 offset 是字节数；对本步要验证的编号规则来说，
    只要"offset 单调且能定位"就够了。
    """

    def __init__(
        self,
        spec: StreamSpec,
        lines: Sequence[str],
        *,
        inode: int = 1,
        fail_at_resume: Exception | None = None,
    ) -> None:
        self.spec = spec
        self._lines = list(lines)
        self._inode = inode
        self._index = 0
        self._tail: str | None = None
        self._fail = fail_at_resume
        self.resumed_from: AdapterState | None = None

    def read(self, state: AdapterState | None = None) -> Iterator[tuple[int, str]]:
        if state is not None:
            self.resumed_from = state
            if self._fail is not None:
                raise self._fail
            if state.inode is not None and state.inode != self._inode:
                raise CheckpointMismatch(f"inode 变了：{state.inode} → {self._inode}")
            self._index = state.offset
            self._tail = state.tail_line_sha1
        while self._index < len(self._lines):
            text = self._lines[self._index]
            self._index += 1
            self._tail = line_sha1(text)
            yield self._index, text

    def state(self) -> AdapterState:
        return AdapterState(
            offset=self._index,
            line_no=self._index,
            inode=self._inode,
            tail_line_sha1=self._tail,
        )


class FakeAdapter:
    """一个组件的若干条流。`open()` 每次都新建 reader（真实适配器也一样）。"""

    adapter = Adapter.FILE

    def __init__(
        self,
        specs: Sequence[StreamSpec],
        data: dict[str, list[str]],
        *,
        fail_at_resume: Exception | None = None,
    ) -> None:
        self._specs = list(specs)
        self._data = data
        self._fail = fail_at_resume
        self.opened: list[str] = []
        self.readers: dict[str, FakeReader] = {}

    def streams(self) -> Sequence[StreamSpec]:
        return list(self._specs)

    def open(self, spec: StreamSpec) -> FakeReader:
        self.opened.append(spec.key)
        reader = FakeReader(spec, self._data[spec.key], fail_at_resume=self._fail)
        self.readers[spec.key] = reader
        return reader


def spec(name: str, order: int = 0, component: str = "dn1") -> StreamSpec:
    return StreamSpec(component=component, name=name, order=order, path=f"/logs/{name}.log")


# ---------------------------------------------------------------- StreamSpec


def test_stream_key_is_component_colon_name() -> None:
    assert spec("audit").key == "dn1:audit"


def test_stream_sort_key_is_order_then_name() -> None:
    assert spec("audit", order=2).sort_key == (2, "audit")
    assert spec("main", order=2).sort_key == (2, "main")
    # 同 order 时 main 排在 audit 前 —— 字典序，与目录遍历无关
    assert spec("audit", order=2).sort_key < spec("main", order=2).sort_key


def test_stream_rejects_unknown_component() -> None:
    with pytest.raises(ValueError, match="component 非法"):
        StreamSpec(component="dn9", name="main")


def test_stream_name_cannot_contain_colon() -> None:
    """冒号是 component 与 name 的分隔符，混进去会让 checkpoint 的键歧义。"""
    with pytest.raises(ValueError, match="不能含冒号"):
        StreamSpec(component="dn1", name="dn1:main")


def test_stream_rejects_empty_name_and_negative_order() -> None:
    with pytest.raises(ValueError, match="name"):
        StreamSpec(component="dn1", name="  ")
    with pytest.raises(ValueError, match="order"):
        StreamSpec(component="dn1", name="main", order=-1)


# ---------------------------------------------------------------- 编号


def test_numbering_starts_at_one() -> None:
    adapter = FakeAdapter([spec("main")], {"dn1:main": ["a", "b"]})
    lines = list(ComponentReader(adapter, "dn1"))
    assert lines == [(1, "a"), (2, "b")]


def test_numbering_continues_across_streams_of_one_component() -> None:
    """主日志 2 行 + GC 2 行：编号 1..4，跨文件连续。"""
    adapter = FakeAdapter(
        [spec("main", order=0), spec("gc", order=1)],
        {"dn1:main": ["a", "b"], "dn1:gc": ["g1", "g2"]},
    )
    lines = list(ComponentReader(adapter, "dn1"))
    assert [no for no, _ in lines] == [1, 2, 3, 4]
    assert [text for _, text in lines] == ["a", "b", "g1", "g2"]


def test_ids_never_collide_across_three_streams() -> None:
    adapter = FakeAdapter(
        [spec("main"), spec("gc"), spec("audit")],
        {"dn1:main": ["a"], "dn1:gc": ["g"], "dn1:audit": ["u1", "u2"]},
    )
    numbers = [no for no, _ in ComponentReader(adapter, "dn1")]
    assert numbers == [1, 2, 3, 4]
    assert len(set(numbers)) == len(numbers)


def test_streams_are_read_in_declared_order_not_input_order() -> None:
    """输入顺序打乱，读出来的顺序必须仍然由 `(order, name)` 决定。"""
    adapter = FakeAdapter(
        [spec("audit", order=2), spec("main", order=0), spec("gc", order=1)],
        {"dn1:main": ["m"], "dn1:gc": ["g"], "dn1:audit": ["u"]},
    )
    assert [text for _, text in ComponentReader(adapter, "dn1")] == ["m", "g", "u"]


def test_other_components_are_ignored() -> None:
    adapter = FakeAdapter(
        [spec("main"), spec("main", component="dn2")],
        {"dn1:main": ["a"], "dn2:main": ["b"]},
    )
    assert [text for _, text in ComponentReader(adapter, "dn1")] == ["a"]


def test_component_must_be_in_closed_set() -> None:
    with pytest.raises(ValueError, match="component 非法"):
        ComponentReader(FakeAdapter([], {}), "dn9")


# ---------------------------------------------------------------- 续采


def test_resume_starts_after_global_max_not_stream_local() -> None:
    """checkpoint 里 GC 已经读到 4 号，主日志再从 2 号追加读，必须从 5 起。

    这条是最容易写错的地方：如果按"这条流自己的 line_no + 1"续，编号会从 3
    开始，正好撞上 GC 已占用的 3、4。
    """
    adapter = FakeAdapter(
        [spec("main", order=0), spec("gc", order=1)],
        {"dn1:main": ["a", "b", "c", "d"], "dn1:gc": ["g1", "g2"]},
    )
    checkpoint = {
        "dn1:main": AdapterState(offset=2, line_no=2, inode=1, tail_line_sha1=line_sha1("b")),
        "dn1:gc": AdapterState(offset=2, line_no=4, inode=1, tail_line_sha1=line_sha1("g2")),
    }
    lines = list(ComponentReader(adapter, "dn1", checkpoint))
    assert lines == [(5, "c"), (6, "d")]
    assert adapter.readers["dn1:gc"].resumed_from is not None


def test_finished_stream_keeps_its_line_no() -> None:
    """已读完的流这次一行不产出，它的 line_no 不能被抹成 0。"""
    adapter = FakeAdapter(
        [spec("main", order=0), spec("gc", order=1)],
        {"dn1:main": ["a", "b"], "dn1:gc": ["g1", "g2"]},
    )
    checkpoint = {"dn1:gc": AdapterState(offset=2, line_no=6, inode=1)}
    reader = ComponentReader(adapter, "dn1", checkpoint)
    assert [no for no, _ in reader] == [7, 8]
    assert reader.checkpoint()["dn1:gc"].line_no == 6


def test_checkpoint_reports_each_stream_with_global_numbers() -> None:
    adapter = FakeAdapter(
        [spec("main", order=0), spec("gc", order=1)],
        {"dn1:main": ["a", "b"], "dn1:gc": ["g1"]},
    )
    reader = ComponentReader(adapter, "dn1")
    list(reader)
    state = reader.checkpoint()
    assert set(state) == {"dn1:main", "dn1:gc"}
    assert state["dn1:main"].line_no == 2  # 主日志贡献了 1、2
    assert state["dn1:gc"].line_no == 3  # GC 贡献了 3
    assert state["dn1:main"].offset == 2


def test_checkpoint_tail_sha1_is_last_line_of_that_stream() -> None:
    adapter = FakeAdapter([spec("main")], {"dn1:main": ["a", "b"]})
    reader = ComponentReader(adapter, "dn1")
    list(reader)
    assert reader.checkpoint()["dn1:main"].tail_line_sha1 == line_sha1("b")


def test_partial_consumption_leaves_usable_checkpoint() -> None:
    """读到一半就停（采集中断），进度必须落在"真正读到的那一行"。"""
    adapter = FakeAdapter(
        [spec("main", order=0), spec("gc", order=1)],
        {"dn1:main": ["a", "b"], "dn1:gc": ["g"]},
    )
    reader = ComponentReader(adapter, "dn1")
    iterator = iter(reader)
    assert next(iterator) == (1, "a")
    state = reader.checkpoint()["dn1:main"]
    assert (state.offset, state.line_no) == (1, 1)

    assert next(iterator) == (2, "b")
    state = reader.checkpoint()["dn1:main"]
    assert (state.offset, state.line_no) == (2, 2)
    assert state.tail_line_sha1 == line_sha1("b")
    # GC 这条流还没开始，不该凭空出现在快照里
    assert "dn1:gc" not in reader.checkpoint()


def test_checkpoint_mid_stream_points_at_the_in_flight_stream() -> None:
    """中断时快照必须指向正在读的那条流，而不是上一条流读完的位置。

    否则续采会从主日志开头再来一遍，产出重复行（V3 直接红）。
    """
    adapter = FakeAdapter(
        [spec("main", order=0), spec("audit", order=1)],
        {"dn1:main": ["a", "b", "c"], "dn1:audit": ["u"]},
    )
    reader = ComponentReader(adapter, "dn1")
    # `__iter__` 是生成器函数，转成 Generator 只是为了能显式 close() 模拟 Ctrl-C
    iterator = cast("Generator[tuple[int, str], None, None]", iter(reader))
    next(iterator)
    assert reader.checkpoint()["dn1:main"].offset == 1
    next(iterator)
    assert reader.checkpoint()["dn1:main"].offset == 2
    # 中途 close，模拟 Ctrl-C
    iterator.close()
    assert reader.checkpoint()["dn1:main"].offset == 2


def test_inode_change_is_reported_as_checkpoint_mismatch() -> None:
    adapter = FakeAdapter([spec("main")], {"dn1:main": ["a", "b", "c"]})
    checkpoint = {"dn1:main": AdapterState(offset=2, line_no=2, inode=99)}
    with pytest.raises(CheckpointMismatch, match="inode"):
        list(ComponentReader(adapter, "dn1", checkpoint))


def test_reader_errors_are_not_swallowed() -> None:
    adapter = FakeAdapter(
        [spec("main")], {"dn1:main": ["a"]}, fail_at_resume=CheckpointMismatch("tail 对不上")
    )
    with pytest.raises(CheckpointMismatch, match="tail"):
        list(ComponentReader(adapter, "dn1", {"dn1:main": AdapterState(offset=1, line_no=1)}))


def test_iteration_is_lazy() -> None:
    """构造 ComponentReader 不该碰盘：只有开始迭代才 open。"""
    adapter = FakeAdapter([spec("main")], {"dn1:main": ["a"]})
    reader = ComponentReader(adapter, "dn1")
    assert adapter.opened == []
    list(reader)
    assert adapter.opened == ["dn1:main"]


def test_empty_stream_yields_nothing() -> None:
    adapter = FakeAdapter([spec("main")], {"dn1:main": []})
    reader = ComponentReader(adapter, "dn1")
    assert list(reader) == []
    assert reader.checkpoint()["dn1:main"].line_no == 0


# ---------------------------------------------------------------- 工具


def test_line_sha1_is_stable_hex() -> None:
    assert line_sha1("a") == line_sha1("a")
    assert line_sha1("a") != line_sha1("b")
    assert len(line_sha1("a")) == 40


def test_line_sha1_covers_exactly_the_line_without_terminator() -> None:
    """契约：调用方传进来的必须是**已去掉行尾换行**的一行。

    换行符算进哈希的话，同一份日志经 Windows（CRLF）与 Linux（LF）读出来
    会得到两个值，file 与 docker 的 checkpoint 就永远对不上。
    """
    assert line_sha1("a") != line_sha1("a\n")
    assert line_sha1("a") != line_sha1("a\r\n")


def test_allocator_starts_at_one_and_follows_max() -> None:
    assert LineAllocator().next_line_no == 1
    allocator = LineAllocator(
        {"dn1:main": AdapterState(offset=5, line_no=5), "dn1:gc": AdapterState(offset=2, line_no=2)}
    )
    assert allocator.next_line_no == 6
    assert allocator.take() == 6
    assert allocator.take() == 7
