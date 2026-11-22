"""标注工具的行为约定：只动点名的那几个字段，机器写的原样保留。"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from logcopilot.collect.labels import LabelError, annotate, describe, measure
from logcopilot.domain.case import (
    Adapter,
    CanonicalProfile,
    CaseManifest,
    Checkpoint,
    Counters,
    Expect,
    FaultInjection,
    Hashes,
    Labels,
    Origin,
    OriginKind,
    RedactPolicy,
    RootCause,
    SourceRef,
    Window,
)
from logcopilot.domain.log import LogLevel, LogRecord, ParseStatus
from logcopilot.store.casepack import (
    create_pack,
    finalize,
    open_log_writer,
    pack_paths,
    read_labels,
    write_labels,
)

T0 = datetime(2026, 11, 22, 10, 0, 0, tzinfo=UTC)
RAW_SHA = "sha256:" + "e" * 64


def make_record(
    line_no: int,
    *,
    level: LogLevel = LogLevel.INFO,
    component: str = "dn1",
    offset_seconds: int = 0,
) -> LogRecord:
    return LogRecord(
        id=f"{component}:{line_no:05d}",
        ts=T0 + timedelta(seconds=offset_seconds),
        level=level,
        component=component,
        logger="DataNode",
        message=f"message {line_no}",
        source=f"file:{component}",
        raw=None,
        parse_status=ParseStatus.OK,
    )


def build_pack(
    root: Path,
    case_id: str,
    records: list[LogRecord],
    *,
    labels: Labels | None = None,
) -> Path:
    """建一个完整 pack。`labels` 可预置（例如已经带 `fault_injection`）。"""
    paths = pack_paths(root, case_id)
    create_pack(paths, labels or Labels(case_id=case_id))
    with open_log_writer(paths) as writer:
        for record in records:
            writer.append(record)

    by_level: dict[str, int] = {}
    by_component: dict[str, int] = {}
    for record in records:
        by_level[record.level.value] = by_level.get(record.level.value, 0) + 1
        by_component[record.component] = by_component.get(record.component, 0) + 1

    manifest = CaseManifest(
        case_id=case_id,
        created_at=T0,
        canonical_profile=CanonicalProfile.HDFS_LOG4J,
        window=Window(since=T0, until=T0 + timedelta(minutes=10)),
        origin=Origin(kind=OriginKind.COLLECTED, src=None),
        sources=(SourceRef(adapter=Adapter.FILE, ref="dn1", component="dn1"),),
        counters=Counters(
            raw_lines=len(records),
            lines=len(records),
            by_level=by_level,
            by_component=by_component,
        ),
        hashes=Hashes(input_fingerprint=RAW_SHA, canonical_sha256="sha256:" + "0" * 64),
        checkpoint=Checkpoint(adapter_state={}, last_ts=T0),
        redact_policy=RedactPolicy(version="redact/1.0.0"),
    )
    finalize(paths, manifest, raw_line_count=len(records), raw_sha256=RAW_SHA)
    return paths.dir


def test_measure_counts_levels_and_error_components(tmp_path: Path) -> None:
    build_pack(
        tmp_path,
        "inc_dnvol_01",
        [
            make_record(1),
            make_record(2, level=LogLevel.WARN),
            make_record(3, level=LogLevel.ERROR),
            make_record(4, level=LogLevel.ERROR, component="dn2"),
        ],
    )

    measured = measure(pack_paths(tmp_path, "inc_dnvol_01"))

    assert measured.lines == 4
    assert measured.by_level == {"INFO": 1, "WARN": 1, "ERROR": 2}
    assert measured.error_components == ("dn1", "dn2")
    assert measured.error_lines == 2
    assert measured.warn_lines == 1


def test_suggest_expect_uses_measured_numbers(tmp_path: Path) -> None:
    """门槛来自实测：这次采到 2 条 ERROR，下次至少也得有 2 条。"""
    build_pack(
        tmp_path,
        "inc_dnvol_01",
        [make_record(1), make_record(2, level=LogLevel.ERROR), make_record(3, level=LogLevel.WARN)],
    )

    expect = measure(pack_paths(tmp_path, "inc_dnvol_01")).suggest_expect()

    assert expect.min_error_lines == 1
    assert expect.min_warn_lines == 1
    assert expect.error_components == ("dn1",)


def test_annotate_fills_expect_from_measurements(tmp_path: Path) -> None:
    build_pack(tmp_path, "inc_dnvol_01", [make_record(1, level=LogLevel.ERROR)])
    paths = pack_paths(tmp_path, "inc_dnvol_01")

    updated = annotate(
        paths, from_measurements=True, severity="critical", reviewer="lead", notes="写满 tmpfs"
    )

    assert updated.expect is not None
    assert updated.expect.min_error_lines == 1
    assert updated.expect.error_components == ("dn1",)
    assert read_labels(paths).severity == "critical"
    assert read_labels(paths).notes == "写满 tmpfs"


def test_annotate_keeps_the_machine_written_fault_injection(tmp_path: Path) -> None:
    """最容易被整份覆盖抹掉的字段：`fault_injection` 没了，V7 就失去证据。"""
    injection = FaultInjection(
        script="dn_volume_fill",
        started=T0,
        ended=T0 + timedelta(minutes=10),
        restored=T0 + timedelta(minutes=10, seconds=3),
        restored_ok=True,
    )
    build_pack(
        tmp_path,
        "inc_dnvol_01",
        [make_record(1, level=LogLevel.ERROR)],
        labels=Labels(
            case_id="inc_dnvol_01", root_cause=RootCause.DN_VOLUME, fault_injection=injection
        ),
    )
    paths = pack_paths(tmp_path, "inc_dnvol_01")

    annotate(paths, from_measurements=True, reviewer="lead")

    after = read_labels(paths)
    assert after.fault_injection == injection
    assert after.root_cause == "DN_VOLUME"


def test_annotate_refuses_to_overwrite_expect_without_force(tmp_path: Path) -> None:
    build_pack(
        tmp_path,
        "inc_dnvol_01",
        [make_record(1, level=LogLevel.ERROR)],
        labels=Labels(case_id="inc_dnvol_01", expect=Expect(min_error_lines=5, min_warn_lines=0)),
    )
    paths = pack_paths(tmp_path, "inc_dnvol_01")

    with pytest.raises(LabelError, match="已存在"):
        annotate(paths, from_measurements=True)

    assert read_labels(paths).expect == Expect(min_error_lines=5, min_warn_lines=0)


def test_annotate_force_overwrites_expect(tmp_path: Path) -> None:
    build_pack(
        tmp_path,
        "inc_dnvol_01",
        [make_record(1, level=LogLevel.ERROR)],
        labels=Labels(case_id="inc_dnvol_01", expect=Expect(min_error_lines=5, min_warn_lines=0)),
    )
    paths = pack_paths(tmp_path, "inc_dnvol_01")

    annotate(paths, from_measurements=True, force=True)

    assert read_labels(paths).expect == Expect(
        min_error_lines=1, min_warn_lines=0, error_components=("dn1",)
    )


def test_annotate_rejects_unknown_root_cause(tmp_path: Path) -> None:
    build_pack(tmp_path, "inc_dnvol_01", [make_record(1)])
    paths = pack_paths(tmp_path, "inc_dnvol_01")

    with pytest.raises(ValueError, match="not a valid RootCause"):
        annotate(paths, root_cause="DISK_FULL")


def test_annotate_leaves_no_temp_file(tmp_path: Path) -> None:
    build_pack(tmp_path, "inc_dnvol_01", [make_record(1, level=LogLevel.ERROR)])
    paths = pack_paths(tmp_path, "inc_dnvol_01")

    annotate(paths, from_measurements=True, reviewer="lead")

    assert not paths.labels.with_suffix(".json.tmp").exists()


def test_annotate_rejects_a_missing_pack(tmp_path: Path) -> None:
    with pytest.raises(LabelError, match="labels.json"):
        annotate(pack_paths(tmp_path, "nope"), from_measurements=True)


def test_describe_lists_the_missing_fields(tmp_path: Path) -> None:
    build_pack(tmp_path, "inc_dnvol_01", [make_record(1, level=LogLevel.ERROR)])

    info = describe(pack_paths(tmp_path, "inc_dnvol_01"))

    assert info["missing"] == ["root_cause", "severity", "expect", "reviewer"]
    assert info["fault_injection"] is False
    assert info["error_components"] == ("dn1",)


def test_write_labels_is_atomic_and_keeps_lf(tmp_path: Path) -> None:
    """`write_labels` 走的是同一个原子写：不能留 `.tmp`，也不能有 CR。"""
    build_pack(tmp_path, "inc_dnvol_01", [make_record(1)])
    paths = pack_paths(tmp_path, "inc_dnvol_01")

    write_labels(paths, Labels(case_id="inc_dnvol_01", reviewer="lead"))

    text = paths.labels.read_text(encoding="utf-8")
    assert "\r" not in text
    assert '"reviewer": "lead"' in text
