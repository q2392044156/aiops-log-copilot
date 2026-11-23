"""`store/casestore.py` 的契约：`ports.CaseStore` 的磁盘实现。

这一层几乎全是"转发"，但有三件事只在这里成立，必须钉住：

  1. `list_cases()` 只列**完成的** pack（采集中途崩掉的半成品目录不能出现）；
  2. `read_anchors()` 对没有 `index.json` 的 pack 返回空序列，不抛异常
     （迁移来的 132 例本来就没有指针）；
  3. 非法 `case_id` 在第一层就被挡住（它同时是目录名，`../` 不能穿出去）。
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from logcopilot.domain.case import (
    Adapter,
    AnchorRef,
    CanonicalProfile,
    CaseManifest,
    Checkpoint,
    Counters,
    Hashes,
    Labels,
    Origin,
    OriginKind,
    RedactPolicy,
    SourceRef,
    Window,
)
from logcopilot.domain.log import LogLevel, LogRecord
from logcopilot.store.casepack import (
    CasePackError,
    CasePackIncomplete,
    create_pack,
    finalize,
    open_log_writer,
    pack_paths,
)
from logcopilot.store.casestore import DiskCaseStore

T0 = datetime(2026, 11, 22, 10, 3, 0, tzinfo=UTC)
RAW_SHA = "sha256:" + "e" * 64


def make_record(line_no: int, *, level: LogLevel = LogLevel.INFO) -> LogRecord:
    return LogRecord(
        id=LogRecord.build_id("dn1", line_no),
        ts=T0 + timedelta(seconds=line_no),
        level=level,
        component="dn1",
        logger="DataNode",
        message=f"第 {line_no} 行",
        source="file:dn1",
        raw=None,
        parse_status=__import__("logcopilot.domain.log", fromlist=["ParseStatus"]).ParseStatus.OK,
    )


def build_pack(
    root: Path,
    case_id: str,
    *,
    records: int = 2,
    anchors: tuple[AnchorRef, ...] = (),
    complete: bool = True,
) -> Path:
    paths = pack_paths(root, case_id)
    create_pack(paths, Labels(case_id=case_id))
    rows = [make_record(i) for i in range(1, records + 1)]
    with open_log_writer(paths) as writer:
        for row in rows:
            writer.append(row)
    if not complete:
        # 采集中途崩掉的样子：有 logs.jsonl、没有 manifest.json
        return paths.dir

    manifest = CaseManifest(
        case_id=case_id,
        created_at=T0,
        canonical_profile=CanonicalProfile.LAB,
        window=Window(since=T0, until=T0 + timedelta(minutes=10)),
        origin=Origin(
            kind=OriginKind.MIGRATED, src="ops/lab", incident_sha256="sha256:" + "a" * 64
        ),
        sources=(SourceRef(adapter=Adapter.FILE, ref="ops/lab/x.log", component="dn1"),),
        counters=Counters(
            raw_lines=len(rows),
            lines=len(rows),
            by_level={"INFO": len(rows)},
            by_component={"dn1": len(rows)},
        ),
        hashes=Hashes(input_fingerprint=RAW_SHA, canonical_sha256="sha256:" + "0" * 64),
        checkpoint=Checkpoint(adapter_state={}, last_ts=T0),
        redact_policy=RedactPolicy(version="redact/1.0.0"),
    )
    finalize(
        paths,
        manifest,
        raw_line_count=len(rows),
        raw_sha256=RAW_SHA,
        anchors=anchors,
    )
    return paths.dir


# --------------------------------------------------------------------------- list / exists


def test_list_cases_returns_only_complete_packs(tmp_path: Path) -> None:
    build_pack(tmp_path, "done_case")
    build_pack(tmp_path, "half_case", complete=False)

    assert DiskCaseStore(tmp_path).list_cases() == ("done_case",)


def test_list_cases_is_sorted_and_empty_when_root_missing(tmp_path: Path) -> None:
    build_pack(tmp_path, "b_case")
    build_pack(tmp_path, "a_case")
    store = DiskCaseStore(tmp_path)
    assert store.list_cases() == ("a_case", "b_case")

    assert DiskCaseStore(tmp_path / "nope").list_cases() == ()


def test_exists_tells_complete_from_half_written(tmp_path: Path) -> None:
    build_pack(tmp_path, "done_case")
    build_pack(tmp_path, "half_case", complete=False)
    store = DiskCaseStore(tmp_path)

    assert store.exists("done_case") is True
    assert store.exists("half_case") is False


def test_unknown_case_raises_instead_of_returning_none(tmp_path: Path) -> None:
    store = DiskCaseStore(tmp_path)
    build_pack(tmp_path, "half_case", complete=False)

    with pytest.raises(CasePackIncomplete):
        store.read_manifest("half_case")
    with pytest.raises(CasePackError):
        store.read_manifest("never_existed")


# --------------------------------------------------------------------------- 读


def test_read_manifest_and_labels_round_trip(tmp_path: Path) -> None:
    build_pack(tmp_path, "done_case")
    store = DiskCaseStore(tmp_path)

    manifest = store.read_manifest("done_case")
    labels = store.read_labels("done_case")

    assert manifest.case_id == "done_case"
    assert manifest.counters.lines == 2
    assert labels.case_id == "done_case"


def test_iter_logs_is_lazy_and_in_file_order(tmp_path: Path) -> None:
    build_pack(tmp_path, "done_case", records=3)
    store = DiskCaseStore(tmp_path)

    logs = store.iter_logs("done_case")

    assert hasattr(logs, "__next__")  # 返回的是迭代器，不是一次读好的 list
    assert [record.id for record in logs] == ["dn1:00001", "dn1:00002", "dn1:00003"]


def test_read_anchors_returns_empty_when_index_is_absent(tmp_path: Path) -> None:
    """迁移来的 132 例没有 anchors/index.json —— 这是设计，不是缺陷。"""
    build_pack(tmp_path, "migrated_case")
    store = DiskCaseStore(tmp_path)

    assert list(store.read_anchors("migrated_case")) == []


def test_read_anchors_returns_the_pointers(tmp_path: Path) -> None:
    anchor = AnchorRef(
        id="dn1:00002",
        file="runtime/hdfs-logs/dn1/x.log",
        offset=120,
        length=64,
        sha256="sha256:" + "b" * 64,
    )
    build_pack(tmp_path, "collected_case", anchors=(anchor,))
    store = DiskCaseStore(tmp_path)

    anchors = store.read_anchors("collected_case")

    assert len(anchors) == 1
    assert anchors[0].id == "dn1:00002"
    assert anchors[0].to_dict()["offset"] == 120


# --------------------------------------------------------------------------- 安全


@pytest.mark.parametrize("bad", ["../etc", "Cases", "a", "has space"])
def test_illegal_case_id_is_rejected_at_the_pack_path_layer(tmp_path: Path, bad: str) -> None:
    """`case_id` 同时是目录名：非法值必须在 `pack_paths` 就被挡住。"""
    store = DiskCaseStore(tmp_path)
    with pytest.raises(ValueError):
        store.read_manifest(bad)


def test_store_does_not_cache_between_instances(tmp_path: Path) -> None:
    """同一个 root 的第二个实例必须能看见刚采完的 pack。"""
    first = DiskCaseStore(tmp_path)
    assert first.list_cases() == ()

    build_pack(tmp_path, "late_case")

    assert DiskCaseStore(tmp_path).list_cases() == ("late_case",)
    assert first.list_cases() == ("late_case",)


def test_write_anchors_is_not_exposed(tmp_path: Path) -> None:
    """写能力不进跨阶段契约：`CaseStore` 只有读方法。"""
    store = DiskCaseStore(tmp_path)
    assert not hasattr(store, "write_anchors")
    assert not hasattr(store, "create_pack")
