"""`store/query.py` 的契约：检索是**确定性**的，原文指针要么指得住、要么说清为什么指不住。

`ask` 是 P-A 第一次"人机交界"，所以它的错误分两类，测试也要分开盯：

- **筛错**（该出的没出、不该出的出了）：靠关键词/级别/组件/时间四组条件钉住。
- **说错**（把"读不回来"当成"没有"）：靠 anchors 的三条失败路径钉住 —— 文件没了、
  偏移越界、内容被改。这三条都不能让整次查询失败，但必须在报告里说出来。
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from logcopilot.domain.case import (
    Adapter,
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
from logcopilot.domain.log import LogLevel, LogRecord, ParseStatus
from logcopilot.store.casepack import (
    AnchorRef,
    create_pack,
    finalize,
    open_log_writer,
    pack_paths,
)
from logcopilot.store.query import (
    Hit,
    Query,
    QueryError,
    ask,
    build_report,
    dump_json,
    matches,
    resolve_anchor,
    search,
)

T0 = datetime(2026, 11, 22, 10, 3, 0, tzinfo=UTC)
FAKE_SHA = "sha256:" + "e" * 64

RAW_LINE = "2026-11-22 10:03:02,000 ERROR DataNode:130 - Volume failure on /data/1"


def make_record(
    line_no: int,
    *,
    level: LogLevel = LogLevel.INFO,
    component: str = "dn1",
    logger: str | None = "DataNode",
    message: str | None = None,
    offset_seconds: int = 0,
) -> LogRecord:
    return LogRecord(
        id=LogRecord.build_id(component, line_no),
        ts=T0 + timedelta(seconds=offset_seconds),
        level=level,
        component=component,
        logger=logger,
        message=message if message is not None else f"第 {line_no} 行",
        source=f"file:{component}",
        raw=None,
        parse_status=ParseStatus.OK,
    )


def build_pack(
    root: Path,
    case_id: str,
    records: list[LogRecord],
    *,
    anchors: list[AnchorRef] | None = None,
    origin_kind: OriginKind = OriginKind.MIGRATED,
) -> Path:
    """走真实的 create → append → finalize 路径建 pack。"""
    paths = pack_paths(root, case_id)
    create_pack(paths, Labels(case_id=case_id))
    with open_log_writer(paths) as writer:
        for record in records:
            writer.append(record)

    origin = (
        Origin(kind=origin_kind, src="ops/lab", incident_sha256="sha256:" + "a" * 64)
        if origin_kind is OriginKind.MIGRATED
        else Origin(kind=origin_kind, src=None)
    )
    manifest = CaseManifest(
        case_id=case_id,
        created_at=T0,
        canonical_profile=CanonicalProfile.LAB,
        window=Window(since=T0, until=T0 + timedelta(minutes=10)),
        origin=origin,
        sources=(SourceRef(adapter=Adapter.FILE, ref="ops/lab/x.log", component="dn1"),),
        counters=Counters(
            raw_lines=len(records),
            lines=len(records),
            by_level={"INFO": len(records)},
            by_component={"dn1": len(records)},
        ),
        hashes=Hashes(input_fingerprint=FAKE_SHA, canonical_sha256="sha256:" + "0" * 64),
        checkpoint=Checkpoint(adapter_state={}, last_ts=T0),
        redact_policy=RedactPolicy(version="redact/1.0.0"),
    )
    finalize(
        paths,
        manifest,
        raw_line_count=manifest.counters.raw_lines,
        raw_sha256=FAKE_SHA,
        anchors=anchors or (),
    )
    return paths.dir


MIXED = [
    make_record(1, message="starting up"),
    make_record(
        2,
        level=LogLevel.WARN,
        logger="DFSUtil",
        message="Unexpected value for data transfer bytes=44",
        offset_seconds=1,
    ),
    make_record(
        3,
        level=LogLevel.ERROR,
        logger="FsDatasetImpl",
        message="Volume failure on /data/1",
        offset_seconds=2,
    ),
    make_record(
        4, component="nn", logger="FSNamesystem", message="BLOCK* NameSystem", offset_seconds=3
    ),
]


# --------------------------------------------------------------------------- 筛选


def test_keyword_hits_the_message(tmp_path: Path) -> None:
    build_pack(tmp_path, "inc_dnvol_01", MIXED)
    hits = search(pack_paths(tmp_path, "inc_dnvol_01"), Query(keywords=("Volume",)))
    assert [hit.record.id for hit in hits] == ["dn1:00003"]


def test_keyword_is_case_insensitive_and_searches_the_logger(tmp_path: Path) -> None:
    build_pack(tmp_path, "inc_dnvol_01", MIXED)
    hits = search(pack_paths(tmp_path, "inc_dnvol_01"), Query(keywords=("fsdatasetimpl",)))
    assert [hit.record.id for hit in hits] == ["dn1:00003"]


def test_multiple_keywords_default_to_or(tmp_path: Path) -> None:
    build_pack(tmp_path, "inc_dnvol_01", MIXED)
    hits = search(pack_paths(tmp_path, "inc_dnvol_01"), Query(keywords=("volume", "BLOCK")))
    assert [hit.record.id for hit in hits] == ["dn1:00003", "nn:00004"]


def test_require_all_needs_every_keyword() -> None:
    query = Query.parse(keywords=["volume", "failure"], require_all=True)
    assert matches(MIXED[2], query)
    # 只含其中一个 → AND 下不成立
    assert not matches(make_record(9, level=LogLevel.ERROR, message="Volume is fine"), query)


def test_level_and_component_filters(tmp_path: Path) -> None:
    build_pack(tmp_path, "inc_dnvol_01", MIXED)
    paths = pack_paths(tmp_path, "inc_dnvol_01")
    assert [hit.record.id for hit in search(paths, Query(levels=frozenset({LogLevel.ERROR})))] == [
        "dn1:00003"
    ]
    assert [hit.record.id for hit in search(paths, Query(components=frozenset({"nn"})))] == [
        "nn:00004"
    ]


def test_time_window_is_inclusive(tmp_path: Path) -> None:
    build_pack(tmp_path, "inc_dnvol_01", MIXED)
    paths = pack_paths(tmp_path, "inc_dnvol_01")
    hits = search(
        paths,
        Query(since=T0 + timedelta(seconds=1), until=T0 + timedelta(seconds=2)),
    )
    assert [hit.record.id for hit in hits] == ["dn1:00002", "dn1:00003"]


def test_limit_truncates_and_the_report_says_so(tmp_path: Path) -> None:
    build_pack(tmp_path, "inc_dnvol_01", MIXED)
    report = build_report(pack_paths(tmp_path, "inc_dnvol_01"), Query(limit=2))
    assert len(report.hits) == 2
    assert report.truncated
    assert "可能还有更多" in report.render()


def test_zero_hits_is_a_valid_answer(tmp_path: Path) -> None:
    build_pack(tmp_path, "inc_dnvol_01", MIXED)
    report = build_report(pack_paths(tmp_path, "inc_dnvol_01"), Query(keywords=("不存在的东西",)))
    assert report.hits == []
    assert not report.truncated
    assert "没有命中" in report.render()


def test_regex_search(tmp_path: Path) -> None:
    build_pack(tmp_path, "inc_dnvol_01", MIXED)
    query = Query.parse(regex=r"bytes=\d+")
    hits = search(pack_paths(tmp_path, "inc_dnvol_01"), query)
    assert [hit.record.id for hit in hits] == ["dn1:00002"]


# --------------------------------------------------------------------------- 参数校验


def test_query_rejects_naive_timestamps() -> None:
    with pytest.raises(QueryError, match="必须带时区"):
        Query(since=datetime(2026, 11, 22, 10, 3))


def test_parse_attaches_utc_to_a_naive_time() -> None:
    # 命令行里人不会打时区，按 UTC 解释（pack 的时间轴就是 UTC）。
    assert Query.parse(since="2026-11-23T03:10").since == datetime(2026, 11, 23, 3, 10, tzinfo=UTC)


def test_bad_level_names_list_the_closed_set() -> None:
    with pytest.raises(QueryError, match="未知的日志级别"):
        Query.parse(levels=["NOPE"])


def test_limit_must_be_positive() -> None:
    with pytest.raises(QueryError, match="limit"):
        Query.parse(limit=0)


def test_bad_regex_is_reported_not_raised_raw() -> None:
    with pytest.raises(QueryError, match="正则写错了"):
        Query.parse(regex="[unclosed")


def test_ask_needs_a_finished_pack(tmp_path: Path) -> None:
    with pytest.raises(QueryError, match="不是一个完成的 pack"):
        ask(tmp_path, "nope", Query())


# --------------------------------------------------------------------------- 原文指针


def write_original(tmp_path: Path, text: str = RAW_LINE + "\n") -> tuple[Path, AnchorRef]:
    """造一份"原始日志" + 一条指到它第 1 行的 anchor。"""
    path = tmp_path / "raw" / "dn1.log"
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = text.encode("utf-8")
    path.write_bytes(raw)
    anchor = AnchorRef(
        id="dn1:00003",
        file=str(path),
        offset=0,
        length=len(raw),
        sha256=f"sha256:{hashlib.sha256(raw).hexdigest()}",
    )
    return path, anchor


def test_anchor_gives_back_the_original_line(
    tmp_path: Path,
) -> None:
    _, anchor = write_original(tmp_path)
    build_pack(tmp_path, "inc_dnvol_01", MIXED, anchors=[anchor])
    report = build_report(
        pack_paths(tmp_path, "inc_dnvol_01"), Query(levels=frozenset({LogLevel.ERROR}))
    )
    hit = report.hits[0]
    assert hit.excerpt == RAW_LINE
    assert hit.dangling is None
    assert "原文│ " + RAW_LINE in report.render()


def test_a_migrated_pack_says_it_has_no_pointer(tmp_path: Path) -> None:
    build_pack(tmp_path, "inc_dnvol_01", MIXED)
    report = build_report(
        pack_paths(tmp_path, "inc_dnvol_01"), Query(levels=frozenset({LogLevel.ERROR}))
    )
    assert report.hits[0].excerpt is None
    assert "无指针" in report.render()


def test_anchor_to_a_missing_file_is_reported_not_hidden(tmp_path: Path) -> None:
    path, anchor = write_original(tmp_path)
    path.unlink()
    excerpt, reason = resolve_anchor(anchor)
    assert excerpt is None
    assert reason is not None and "不在了" in reason


def test_anchor_detects_a_rewritten_line(tmp_path: Path) -> None:
    path, anchor = write_original(tmp_path)
    # 长度必须与原文一致，否则先命中的是"偏移越界"，测不到 sha256 那一条。
    path.write_bytes((RAW_LINE.upper() + "\n").encode("utf-8"))
    excerpt, reason = resolve_anchor(anchor)
    assert excerpt is None
    assert reason is not None and "sha256" in reason


def test_anchor_past_the_end_of_file_is_reported(tmp_path: Path) -> None:
    _, anchor = write_original(tmp_path)
    beyond = AnchorRef(
        id=anchor.id, file=anchor.file, offset=10_000, length=anchor.length, sha256=anchor.sha256
    )
    excerpt, reason = resolve_anchor(beyond)
    assert excerpt is None
    assert reason is not None and "偏移越界" in reason


# --------------------------------------------------------------------------- 报告与 JSON


def test_json_output_is_stable_and_complete(tmp_path: Path) -> None:
    build_pack(tmp_path, "inc_dnvol_01", MIXED)
    report = build_report(pack_paths(tmp_path, "inc_dnvol_01"), Query(keywords=("Volume",)))
    payload = json.loads(dump_json(report))
    assert payload["case_id"] == "inc_dnvol_01"
    assert payload["truncated"] is False
    assert payload["hits"][0]["id"] == "dn1:00003"
    assert set(payload["hits"][0]) == {
        "id",
        "ts",
        "level",
        "component",
        "logger",
        "message",
        "parse_status",
        "excerpt",
        "dangling",
        "anchor",
    }


def test_report_shows_the_query_it_ran(tmp_path: Path) -> None:
    build_pack(tmp_path, "inc_dnvol_01", MIXED)
    report = build_report(
        pack_paths(tmp_path, "inc_dnvol_01"),
        Query.parse(keywords=["volume"], levels=["ERROR"], components=["dn1"]),
    )
    text = report.render()
    assert "关键词 volume" in text
    assert "级别 ERROR" in text
    assert "组件 dn1" in text


def test_hit_without_original_is_distinguishable() -> None:
    hit = Hit(record=MIXED[2])
    assert not hit.has_original
