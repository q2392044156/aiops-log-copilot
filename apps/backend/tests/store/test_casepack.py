"""`store/casepack.py` 的契约测试：建包 → 追加 → finalize → 读回 → 校验。

这一层和上一层的区别：`domain/case.py` 验的是"字段合不合法"，这里验的是
"落到盘上的字节对不对"。所以大部分用例都在**故意把盘上的东西弄坏**，
再看 `audit()` 能不能抓到 —— 一个只会说 PASS 的校验器和没有校验器一样危险。
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from logcopilot.domain.case import (
    Adapter,
    AdapterState,
    CanonicalProfile,
    CaseManifest,
    Checkpoint,
    Counters,
    Expect,
    Hashes,
    Labels,
    Origin,
    OriginKind,
    Phase,
    PhaseWindow,
    RedactPolicy,
    RootCause,
    Severity,
    SourceRef,
    Window,
)
from logcopilot.domain.log import LogLevel, LogRecord, ParseStatus
from logcopilot.store.casepack import (
    AnchorRef,
    CasePackCorrupt,
    CasePackError,
    CasePackExists,
    CasePackIncomplete,
    audit,
    audit_levels,
    build_input_fingerprint,
    canonical_sha256,
    create_pack,
    finalize,
    is_complete,
    iter_logs,
    open_log_writer,
    pack_paths,
    read_anchors,
    read_labels,
    read_logs,
    read_manifest,
)

T0 = datetime(2026, 11, 22, 10, 3, 0, tzinfo=UTC)
RAW_SHA = "sha256:" + "e" * 64


# ---------------------------------------------------------------- 工厂


def make_record(
    line_no: int,
    ts: datetime,
    *,
    level: LogLevel = LogLevel.INFO,
    component: str = "dn1",
    message: str = "Volume Failure",
    logger: str | None = "DataNode",
    parse_status: ParseStatus = ParseStatus.OK,
    raw: str | None = None,
) -> LogRecord:
    return LogRecord(
        id=LogRecord.build_id(component, line_no),
        ts=ts,
        level=level,
        component=component,
        logger=logger,
        message=message,
        source=f"docker:{component}",
        raw=raw,
        parse_status=parse_status,
        template_id=None,
    )


def make_manifest(
    case_id: str = "inc_dnvol_01",
    *,
    pipeline_version: str = "collect/0.2.0",
    canonical_profile: CanonicalProfile = CanonicalProfile.HDFS_LOG4J,
    window: Window | None = None,
) -> CaseManifest:
    return CaseManifest(
        case_id=case_id,
        created_at=T0 + timedelta(minutes=20),
        canonical_profile=canonical_profile,
        window=window
        or Window(
            since=T0,
            until=T0 + timedelta(minutes=20),
            phases=(
                PhaseWindow(phase=Phase.BASELINE, since=T0, until=T0 + timedelta(minutes=5)),
                PhaseWindow(
                    phase=Phase.FAULT,
                    since=T0 + timedelta(minutes=5),
                    until=T0 + timedelta(minutes=15),
                ),
            ),
        ),
        origin=Origin(kind=OriginKind.COLLECTED),
        sources=(
            SourceRef(
                adapter=Adapter.DOCKER,
                ref="datanode1",
                component="dn1",
                image_digest="sha256:" + "c" * 64,
                path="/opt/hadoop/logs/hadoop-hdfs-datanode-datanode1.log",
            ),
        ),
        counters=Counters(raw_lines=0, lines=0, by_level={}, by_component={}),
        hashes=Hashes(input_fingerprint=RAW_SHA, canonical_sha256=RAW_SHA),
        checkpoint=Checkpoint(
            adapter_state={"dn1": AdapterState(offset=0, line_no=0)},
            last_ts=None,
        ),
        redact_policy=RedactPolicy(version="v1"),
        pipeline_version=pipeline_version,
    )


def build_pack(
    root: Path,
    *,
    records: Sequence[LogRecord] | None = None,
    labels: Labels | None = None,
    manifest: CaseManifest | None = None,
    raw_line_count: int | None = None,
    anchors: Sequence[AnchorRef] = (),
) -> CaseManifest:
    """建一个完整合法的 pack，返回写进去的 manifest。"""
    records = list(records) if records is not None else [make_record(1, T0)]
    manifest = manifest or make_manifest()
    paths = pack_paths(root, manifest.case_id)
    create_pack(paths, labels or Labels(case_id=manifest.case_id))
    with open_log_writer(paths) as writer:
        writer.extend(records)
    return finalize(
        paths,
        manifest,
        raw_line_count=raw_line_count if raw_line_count is not None else len(records) + 1,
        raw_sha256=RAW_SHA,
        anchors=anchors,
    )


# ---------------------------------------------------------------- 建包


def test_create_pack_makes_incomplete_dir(tmp_path: Path) -> None:
    paths = pack_paths(tmp_path, "inc_dnvol_01")
    create_pack(paths, Labels(case_id="inc_dnvol_01"))

    assert paths.dir.is_dir()
    assert paths.logs.is_file() and paths.logs.stat().st_size == 0
    assert paths.labels.is_file()
    assert paths.anchors_dir.is_dir()
    # manifest 还没写 —— 所以这个 pack 现在是"不存在"的
    assert not is_complete(paths)
    with pytest.raises(CasePackIncomplete):
        read_manifest(paths)


def test_create_pack_refuses_to_overwrite_completed_pack(tmp_path: Path) -> None:
    build_pack(tmp_path)
    paths = pack_paths(tmp_path, "inc_dnvol_01")
    with pytest.raises(CasePackExists):
        create_pack(paths, Labels(case_id="inc_dnvol_01"))


def test_create_pack_rejects_case_id_mismatch(tmp_path: Path) -> None:
    with pytest.raises(CasePackError, match="不一致"):
        create_pack(pack_paths(tmp_path, "inc_dnvol_01"), Labels(case_id="inc_dnvol_02"))


# ---------------------------------------------------------------- finalize


def test_finalize_writes_manifest_last_and_stats(tmp_path: Path) -> None:
    records = [
        make_record(1, T0, level=LogLevel.INFO),
        make_record(2, T0 + timedelta(seconds=1), level=LogLevel.ERROR),
        make_record(3, T0 + timedelta(seconds=2), level=LogLevel.WARN),
    ]
    manifest = build_pack(tmp_path, records=records, raw_line_count=10)

    paths = pack_paths(tmp_path, "inc_dnvol_01")
    assert is_complete(paths)
    assert manifest.counters.lines == 3
    assert manifest.counters.raw_lines == 10
    assert manifest.counters.by_level == {"INFO": 1, "ERROR": 1, "WARN": 1}
    assert manifest.counters.by_component == {"dn1": 3}
    assert manifest.counters.parse_failed == 0
    assert manifest.hashes.canonical_sha256 == canonical_sha256(records)


def test_finalize_sorts_by_ts_then_id(tmp_path: Path) -> None:
    # 故意乱序追加，模拟"多源并行、按到达序落盘"
    records = [
        make_record(3, T0 + timedelta(seconds=3)),
        make_record(1, T0 + timedelta(seconds=1)),
        make_record(2, T0 + timedelta(seconds=1)),  # 与上一条同 ts
    ]
    build_pack(tmp_path, records=records)

    read_back = read_logs(pack_paths(tmp_path, "inc_dnvol_01"))
    # 先按 ts 升序：两条 10:03:01 排在 10:03:03 前面；
    # 同 ts 再按 id 升序：dn1:00001 在 dn1:00002 前面。
    assert [record.id for record in read_back] == ["dn1:00001", "dn1:00002", "dn1:00003"]


def test_finalize_same_ts_orders_by_id(tmp_path: Path) -> None:
    records = [
        make_record(12, T0),
        make_record(9, T0),
        make_record(30, T0),
    ]
    build_pack(tmp_path, records=records)
    ids = [record.id for record in read_logs(pack_paths(tmp_path, "inc_dnvol_01"))]
    assert ids == ["dn1:00009", "dn1:00012", "dn1:00030"]


def test_finalize_counts_parse_failed(tmp_path: Path) -> None:
    records = [
        make_record(1, T0),
        make_record(
            2,
            T0 + timedelta(seconds=1),
            parse_status=ParseStatus.FAILED,
            raw="2026-11-22 10:03:01,000 ERROR org.apache...",
            logger=None,
        ),
    ]
    manifest = build_pack(tmp_path, records=records)
    assert manifest.counters.parse_failed == 1


def test_finalize_rejects_duplicate_id(tmp_path: Path) -> None:
    paths = pack_paths(tmp_path, "inc_dnvol_01")
    create_pack(paths, Labels(case_id="inc_dnvol_01"))
    record = make_record(1, T0)
    with open_log_writer(paths) as writer:
        writer.append(record)
        writer.append(record)  # 同一条写两次 = 续采时 line_no 算错的典型症状
    with pytest.raises(CasePackCorrupt, match="id 重复"):
        finalize(paths, make_manifest(), raw_line_count=2, raw_sha256=RAW_SHA)


def test_finalize_leaves_no_tmp_files(tmp_path: Path) -> None:
    build_pack(tmp_path)
    leftovers = [path.name for path in pack_paths(tmp_path, "inc_dnvol_01").dir.rglob("*.tmp")]
    assert leftovers == []


def test_finalize_requires_raw_line_count_not_smaller_than_lines(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="raw_lines"):
        build_pack(tmp_path, records=[make_record(1, T0), make_record(2, T0)], raw_line_count=1)


# ---------------------------------------------------------------- 两个哈希


def test_input_fingerprint_is_stable_for_same_source(tmp_path: Path) -> None:
    first = build_pack(tmp_path / "a")
    second = build_pack(tmp_path / "b")
    assert first.hashes.input_fingerprint == second.hashes.input_fingerprint


def test_input_fingerprint_ignores_pipeline_version(tmp_path: Path) -> None:
    """§6.4 排除项：换个版本号不该让"同一份数据"看起来变了。"""
    old = build_pack(tmp_path / "a", manifest=make_manifest(pipeline_version="collect/0.1.0"))
    new = build_pack(tmp_path / "b", manifest=make_manifest(pipeline_version="collect/9.9.9"))
    assert old.hashes.input_fingerprint == new.hashes.input_fingerprint
    assert old.hashes.canonical_sha256 == new.hashes.canonical_sha256


def test_input_fingerprint_ignores_phases(tmp_path: Path) -> None:
    """phases 是标注，不是数据：补标一次 baseline 不该改变输入指纹。"""
    window = Window(since=T0, until=T0 + timedelta(minutes=20))
    with_phases = make_manifest(
        window=Window(
            since=T0,
            until=T0 + timedelta(minutes=20),
            phases=(PhaseWindow(phase=Phase.FAULT, since=T0, until=T0 + timedelta(minutes=1)),),
        )
    )
    assert build_input_fingerprint(
        case_id=with_phases.case_id,
        sources=with_phases.sources,
        window=with_phases.window,
        raw_line_count=9,
        raw_sha256=RAW_SHA,
    ) == build_input_fingerprint(
        case_id=with_phases.case_id,
        sources=with_phases.sources,
        window=window,
        raw_line_count=9,
        raw_sha256=RAW_SHA,
    )


def test_input_fingerprint_changes_with_window(tmp_path: Path) -> None:
    first = build_pack(tmp_path / "a")
    shifted = make_manifest(
        window=Window(since=T0 + timedelta(minutes=1), until=T0 + timedelta(minutes=21))
    )
    second = build_pack(tmp_path / "b", manifest=shifted)
    assert first.hashes.input_fingerprint != second.hashes.input_fingerprint


def test_canonical_sha256_changes_when_message_changes(tmp_path: Path) -> None:
    """§6.4 不变式 2：源没变、处理结果变了 → input 相等，canonical 不等。"""
    first = build_pack(tmp_path / "a", records=[make_record(1, T0, message="Volume Failure")])
    second = build_pack(tmp_path / "b", records=[make_record(1, T0, message="Volume Failure!")])
    assert first.hashes.input_fingerprint == second.hashes.input_fingerprint
    assert first.hashes.canonical_sha256 != second.hashes.canonical_sha256


def test_canonical_sha256_ignores_source_and_raw(tmp_path: Path) -> None:
    """V4 的前提：`source` 不进投影，换 adapter 哈希不变。"""
    docker = make_record(1, T0)
    local = LogRecord(
        id=docker.id,
        ts=docker.ts,
        level=docker.level,
        component=docker.component,
        logger=docker.logger,
        message=docker.message,
        source="file:/var/log/hadoop/hadoop-hdfs-datanode.log",
        raw=None,
        parse_status=ParseStatus.OK,
        template_id=None,
    )
    assert canonical_sha256([docker]) == canonical_sha256([local])
    assert canonical_sha256([docker]) != canonical_sha256([local, local])


def test_build_input_fingerprint_rejects_bad_raw_sha(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="sha256"):
        build_input_fingerprint(
            case_id="inc_dnvol_01",
            sources=make_manifest().sources,
            window=make_manifest().window,
            raw_line_count=1,
            raw_sha256="deadbeef",
        )


# ---------------------------------------------------------------- anchors


def test_anchors_round_trip(tmp_path: Path) -> None:
    anchor = AnchorRef(
        id="dn1:00042",
        file="runtime/hdfs-logs/datanode1/x.log",
        offset=10,
        length=42,
        sha256=RAW_SHA,
    )
    build_pack(tmp_path, anchors=[anchor])
    assert read_anchors(pack_paths(tmp_path, "inc_dnvol_01")) == [anchor]


def test_anchors_empty_is_valid(tmp_path: Path) -> None:
    build_pack(tmp_path)
    assert read_anchors(pack_paths(tmp_path, "inc_dnvol_01")) == []


def test_anchor_ref_rejects_bad_geometry() -> None:
    with pytest.raises(ValueError, match="length"):
        AnchorRef(id="dn1:00001", file="x.log", offset=0, length=0, sha256=RAW_SHA)
    with pytest.raises(ValueError, match="offset"):
        AnchorRef(id="dn1:00001", file="x.log", offset=-1, length=1, sha256=RAW_SHA)


# ---------------------------------------------------------------- audit


def test_audit_passes_on_good_pack(tmp_path: Path) -> None:
    build_pack(tmp_path, records=[make_record(1, T0), make_record(2, T0 + timedelta(seconds=1))])
    assert audit(pack_paths(tmp_path, "inc_dnvol_01")) == []


def test_audit_detects_missing_file(tmp_path: Path) -> None:
    build_pack(tmp_path)
    paths = pack_paths(tmp_path, "inc_dnvol_01")
    paths.labels.unlink()
    problems = audit(paths)
    assert any("labels.json" in problem for problem in problems)


def test_audit_detects_counters_mismatch(tmp_path: Path) -> None:
    manifest = build_pack(tmp_path)  # 一条 INFO
    paths = pack_paths(tmp_path, "inc_dnvol_01")
    tampered = manifest.to_dict()
    # 把 INFO 记成 ERROR：总和仍是 1，所以 Counters 自己的对账等式过得去，
    # 只有拿盘上真实记录数一遍才抓得到 —— 这正是 audit 存在的理由。
    tampered["counters"]["by_level"] = {"ERROR": 1}
    paths.manifest.write_text(json.dumps(tampered, ensure_ascii=False), encoding="utf-8")
    problems = audit(paths)
    assert any(problem.startswith("counters: by_level") for problem in problems)


def test_audit_detects_unreadable_counters(tmp_path: Path) -> None:
    manifest = build_pack(tmp_path, records=[make_record(1, T0), make_record(2, T0)])
    paths = pack_paths(tmp_path, "inc_dnvol_01")
    tampered = manifest.to_dict()
    tampered["counters"]["lines"] = 99  # 与 by_level 之和对不上 → 读 manifest 就拒
    paths.manifest.write_text(json.dumps(tampered, ensure_ascii=False), encoding="utf-8")
    problems = audit(paths)
    assert any(problem.startswith("read:") for problem in problems)


def test_audit_detects_canonical_hash_mismatch(tmp_path: Path) -> None:
    build_pack(tmp_path, records=[make_record(1, T0, message="before")])
    paths = pack_paths(tmp_path, "inc_dnvol_01")
    record = make_record(1, T0, message="after")
    paths.logs.write_text(record.to_json_line() + "\n", encoding="utf-8")
    problems = audit(paths)
    assert any(problem.startswith("hashes:") for problem in problems)


def test_audit_detects_unsorted_logs(tmp_path: Path) -> None:
    build_pack(
        tmp_path,
        records=[make_record(1, T0), make_record(2, T0 + timedelta(seconds=1))],
    )
    paths = pack_paths(tmp_path, "inc_dnvol_01")
    lines = paths.logs.read_text(encoding="utf-8").splitlines()
    paths.logs.write_text("\n".join(reversed(lines)) + "\n", encoding="utf-8")
    assert any(problem.startswith("sort:") for problem in audit(paths))


def test_audit_detects_corrupt_log_line(tmp_path: Path) -> None:
    build_pack(tmp_path)
    paths = pack_paths(tmp_path, "inc_dnvol_01")
    paths.logs.write_text('{"id":"dn1:00001"}\n', encoding="utf-8")
    problems = audit(paths)
    assert any(problem.startswith("read:") for problem in problems)


def test_iter_logs_reports_line_number_on_corruption(tmp_path: Path) -> None:
    build_pack(tmp_path)
    paths = pack_paths(tmp_path, "inc_dnvol_01")
    good = make_record(1, T0).to_json_line()
    paths.logs.write_text(good + "\nnot json\n", encoding="utf-8")
    with pytest.raises(CasePackCorrupt, match="logs.jsonl:2"):
        list(iter_logs(paths))


def test_audit_detects_case_id_mismatch_between_labels_and_manifest(tmp_path: Path) -> None:
    build_pack(tmp_path)
    paths = pack_paths(tmp_path, "inc_dnvol_01")
    paths.labels.write_text(Labels(case_id="inc_dnvol_02").to_json(), encoding="utf-8")
    assert any(problem.startswith("labels:") for problem in audit(paths))


# ---------------------------------------------------------------- audit_levels（V5 / V6）


def test_audit_levels_normal_case_requires_zero_error(tmp_path: Path) -> None:
    records = [
        make_record(1, T0, level=LogLevel.INFO),
        make_record(2, T0 + timedelta(seconds=1), level=LogLevel.ERROR),
    ]
    manifest = build_pack(tmp_path, records=records)
    paths = pack_paths(tmp_path, "inc_dnvol_01")
    labels = Labels(case_id="inc_dnvol_01")  # 正常例：root_cause=None
    problems = audit_levels(paths, manifest, labels)
    assert any("正常例" in problem for problem in problems)


def test_audit_levels_fault_case_passes_when_expect_met(tmp_path: Path) -> None:
    records = [
        make_record(1, T0, level=LogLevel.ERROR, component="dn1"),
        make_record(2, T0 + timedelta(seconds=1), level=LogLevel.WARN, component="dn1"),
    ]
    labels = Labels(
        case_id="inc_dnvol_01",
        root_cause=RootCause.DN_VOLUME,
        severity=Severity.ERROR,
        expect=Expect(min_error_lines=1, min_warn_lines=1, error_components=("dn1",)),
    )
    manifest = build_pack(tmp_path, records=records, labels=labels)
    paths = pack_paths(tmp_path, "inc_dnvol_01")
    assert audit_levels(paths, manifest, read_labels(paths)) == []


def test_audit_levels_fault_case_without_expect_is_an_error(tmp_path: Path) -> None:
    records = [make_record(1, T0, level=LogLevel.ERROR)]
    labels = Labels(case_id="inc_dnvol_01", root_cause=RootCause.DN_VOLUME)
    manifest = build_pack(tmp_path, records=records, labels=labels)
    paths = pack_paths(tmp_path, "inc_dnvol_01")
    problems = audit_levels(paths, manifest, read_labels(paths))
    assert any("expect" in problem for problem in problems)


def test_audit_levels_tolerates_cascade_noise_from_other_components(tmp_path: Path) -> None:
    """故障会级联：expect 之外的组件跟着报错，不算失败。"""
    records = [
        make_record(1, T0, level=LogLevel.ERROR, component="dn1"),
        make_record(2, T0 + timedelta(seconds=1), level=LogLevel.ERROR, component="dn2"),
    ]
    labels = Labels(
        case_id="inc_dnvol_01",
        root_cause=RootCause.DN_VOLUME,
        expect=Expect(min_error_lines=1, error_components=("dn1",)),
    )
    manifest = build_pack(tmp_path, records=records, labels=labels)
    paths = pack_paths(tmp_path, "inc_dnvol_01")
    assert audit_levels(paths, manifest, read_labels(paths)) == []


def test_audit_levels_rejects_all_zero_thresholds(tmp_path: Path) -> None:
    """门槛全是 0 = 什么都没要求，等于没标注。"""
    records = [make_record(1, T0, level=LogLevel.WARN, component="dn1")]
    labels = Labels(
        case_id="inc_dnvol_01",
        root_cause=RootCause.DN_VOLUME,
        expect=Expect(error_components=("dn1",)),
    )
    manifest = build_pack(tmp_path, records=records, labels=labels)
    paths = pack_paths(tmp_path, "inc_dnvol_01")
    problems = audit_levels(paths, manifest, read_labels(paths))
    assert any("门槛全是 0" in problem for problem in problems)


def test_audit_levels_accepts_warn_only_evidence(tmp_path: Path) -> None:
    """旧语料里心跳/网络/安全模式三族只有 WARN —— 它们也要能过 V6。"""
    records = [
        make_record(1, T0, level=LogLevel.WARN, component="dn1"),
        make_record(2, T0 + timedelta(seconds=1), level=LogLevel.WARN, component="dn1"),
    ]
    labels = Labels(
        case_id="inc_dnvol_01",
        root_cause=RootCause.DN_VOLUME,
        expect=Expect(min_warn_lines=2, error_components=("dn1",)),
    )
    manifest = build_pack(tmp_path, records=records, labels=labels)
    paths = pack_paths(tmp_path, "inc_dnvol_01")
    assert audit_levels(paths, manifest, read_labels(paths)) == []


def test_audit_levels_flags_expected_component_without_evidence(tmp_path: Path) -> None:
    records = [make_record(1, T0, level=LogLevel.WARN, component="dn1")]
    labels = Labels(
        case_id="inc_dnvol_01",
        root_cause=RootCause.DN_VOLUME,
        expect=Expect(min_warn_lines=1, error_components=("dn2",)),
    )
    manifest = build_pack(tmp_path, records=records, labels=labels)
    paths = pack_paths(tmp_path, "inc_dnvol_01")
    problems = audit_levels(paths, manifest, read_labels(paths))
    assert any("dn2" in problem for problem in problems)


def test_audit_levels_flags_empty_error_components(tmp_path: Path) -> None:
    records = [make_record(1, T0, level=LogLevel.WARN, component="dn1")]
    labels = Labels(
        case_id="inc_dnvol_01",
        root_cause=RootCause.DN_VOLUME,
        expect=Expect(min_warn_lines=1),
    )
    manifest = build_pack(tmp_path, records=records, labels=labels)
    paths = pack_paths(tmp_path, "inc_dnvol_01")
    problems = audit_levels(paths, manifest, read_labels(paths))
    assert any("error_components 为空" in problem for problem in problems)
