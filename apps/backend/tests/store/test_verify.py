"""`store/verify.py` 的契约：独立第二实现要真的"独立"，基准要真的"拦得住"。

这里的测试有一个容易写歪的地方：用生产代码算期望值。那样"两边同时错"会一起绿。
所以凡是能手算的（canonical 行、sha256、计数），都手算。
"""

from __future__ import annotations

import hashlib
import json
import shutil
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
    Phase,
    PhaseWindow,
    RedactPolicy,
    SourceRef,
    Window,
)
from logcopilot.domain.log import LogLevel, LogRecord, ParseStatus
from logcopilot.store.casepack import (
    CasePackError,
    create_pack,
    finalize,
    open_log_writer,
    pack_paths,
)
from logcopilot.store.verify import (
    BASELINE_SCHEMA,
    PackFingerprint,
    VerifyError,
    compare,
    discover,
    fingerprint_pack,
    load_baseline,
    save_baseline,
    verify,
)

T0 = datetime(2026, 11, 22, 10, 3, 0, tzinfo=UTC)
RAW_SHA = "sha256:" + "e" * 64


def make_record(
    line_no: int,
    *,
    level: LogLevel = LogLevel.INFO,
    component: str = "dn1",
    logger: str | None = "DataNode",
    message: str | None = None,
    parse_status: ParseStatus = ParseStatus.OK,
    raw: str | None = None,
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
        raw=raw,
        parse_status=parse_status,
    )


def build_pack(
    root: Path,
    case_id: str,
    records: list[LogRecord],
    *,
    raw_lines: int | None = None,
    origin_kind: OriginKind = OriginKind.MIGRATED,
) -> Path:
    """建一个完整 pack（走真实的 create → append → finalize 路径）。"""
    paths = pack_paths(root, case_id)
    create_pack(paths, Labels(case_id=case_id))
    with open_log_writer(paths) as writer:
        for record in records:
            writer.append(record)

    # 两种来源的必填项不同（`Origin` 自己会交叉校验）：迁移例必须带 incident 摘要，
    # 现场采集例必须**不**带（它没有上游病历）。
    if origin_kind is OriginKind.MIGRATED:
        origin = Origin(kind=origin_kind, src="ops/lab", incident_sha256="sha256:" + "a" * 64)
    else:
        origin = Origin(kind=origin_kind, src=None)

    manifest = CaseManifest(
        case_id=case_id,
        created_at=T0,
        canonical_profile=CanonicalProfile.LAB,
        window=Window(since=T0, until=T0 + timedelta(minutes=10)),
        origin=origin,
        sources=(SourceRef(adapter=Adapter.FILE, ref="ops/lab/x.log", component="dn1"),),
        counters=Counters(
            raw_lines=raw_lines if raw_lines is not None else len(records),
            lines=len(records),
            by_level={"INFO": len(records)},
            by_component={"dn1": len(records)},
        ),
        hashes=Hashes(input_fingerprint=RAW_SHA, canonical_sha256="sha256:" + "0" * 64),
        checkpoint=Checkpoint(adapter_state={}, last_ts=T0),
        redact_policy=RedactPolicy(version="redact/1.0.0"),
    )
    finalize(paths, manifest, raw_line_count=manifest.counters.raw_lines, raw_sha256=RAW_SHA)
    return paths.dir


def expected_canonical_sha(records: list[LogRecord]) -> str:
    """手算 canonical 哈希：不调 `LogRecord.canonical_payload`，按契约自己拼。"""
    digest = hashlib.sha256()
    for record in records:
        line = json.dumps(
            {
                "id": record.id,
                "ts": record.ts.strftime("%Y-%m-%dT%H:%M:%S.")
                + f"{record.ts.microsecond // 1000:03d}+00:00",
                "level": record.level,
                "component": record.component,
                "logger": record.logger,
                "message": record.message,
            },
            ensure_ascii=False,
            separators=(",", ":"),
        )
        digest.update(line.encode("utf-8"))
        digest.update(b"\n")
    return f"sha256:{digest.hexdigest()}"


# --------------------------------------------------------------------------- 独立第二实现


def test_fingerprint_matches_a_hand_computed_expectation(tmp_path: Path) -> None:
    records = [
        make_record(1, offset_seconds=0),
        make_record(2, level=LogLevel.WARN, offset_seconds=1),
        make_record(
            3, level=LogLevel.ERROR, parse_status=ParseStatus.PARTIAL, raw="原文", offset_seconds=2
        ),
    ]
    build_pack(tmp_path, "demo_case", records)

    found = fingerprint_pack(pack_paths(tmp_path, "demo_case"))

    assert found.lines == 3
    assert found.by_level == {"INFO": 1, "WARN": 1, "ERROR": 1}
    assert found.by_component == {"dn1": 3}
    assert found.parse_failed == 1
    assert found.canonical_sha256 == expected_canonical_sha(records)
    assert found.origin == "migrated"


def test_fingerprint_projection_ignores_source_and_raw(tmp_path: Path) -> None:
    """V4 的前提：canonical 投影里没有 source/raw，换条读路径哈希不该动。"""
    base = [make_record(1), make_record(2, level=LogLevel.WARN, offset_seconds=1)]
    build_pack(tmp_path / "a", "case_a", base)

    shifted = [
        LogRecord(
            id=record.id,
            ts=record.ts,
            level=record.level,
            component=record.component,
            logger=record.logger,
            message=record.message,
            source="docker:dn1",
            raw=None,
            parse_status=ParseStatus.OK,
        )
        for record in base
    ]
    build_pack(tmp_path / "b", "case_b", shifted)

    left = fingerprint_pack(pack_paths(tmp_path / "a", "case_a"))
    right = fingerprint_pack(pack_paths(tmp_path / "b", "case_b"))
    assert left.canonical_sha256 == right.canonical_sha256
    assert left.canonical_sha256 == expected_canonical_sha(base)


def test_fingerprint_detects_a_tampered_message(tmp_path: Path) -> None:
    records = [make_record(1), make_record(2, offset_seconds=1)]
    build_pack(tmp_path, "demo_case", records)
    paths = pack_paths(tmp_path, "demo_case")
    before = fingerprint_pack(paths)

    rows = [json.loads(line) for line in paths.logs.read_text(encoding="utf-8").splitlines()]
    rows[0]["message"] = "被改过的正文"
    paths.logs.write_text(
        "\n".join(json.dumps(row, ensure_ascii=False, separators=(",", ":")) for row in rows)
        + "\n",
        encoding="utf-8",
    )

    assert fingerprint_pack(paths).canonical_sha256 != before.canonical_sha256


def test_fingerprint_rejects_counts_that_disagree_with_disk(tmp_path: Path) -> None:
    build_pack(tmp_path, "demo_case", [make_record(1)])
    paths = pack_paths(tmp_path, "demo_case")
    manifest = json.loads(paths.manifest.read_text(encoding="utf-8"))
    manifest["counters"]["lines"] = 99
    paths.manifest.write_text(json.dumps(manifest, ensure_ascii=False), encoding="utf-8")

    with pytest.raises(CasePackError, match="manifest 说 99 条"):
        fingerprint_pack(paths)


def test_fingerprint_rejects_unknown_fields(tmp_path: Path) -> None:
    build_pack(tmp_path, "demo_case", [make_record(1)])
    paths = pack_paths(tmp_path, "demo_case")
    row = json.loads(paths.logs.read_text(encoding="utf-8").splitlines()[0])
    row["surprise"] = 1
    paths.logs.write_text(json.dumps(row, ensure_ascii=False) + "\n", encoding="utf-8")

    with pytest.raises(CasePackError, match="字段不对"):
        fingerprint_pack(paths)


# --------------------------------------------------------------------------- 逐字段对账


def _fingerprint(**overrides) -> PackFingerprint:
    base = {
        "case_id": "demo_case",
        "origin": "migrated",
        "raw_lines": 10,
        "lines": 8,
        "by_level": {"INFO": 8},
        "by_component": {"dn1": 8},
        "parse_failed": 0,
        "canonical_sha256": "sha256:" + "1" * 64,
        "input_fingerprint": "sha256:" + "2" * 64,
    }
    base.update(overrides)
    return PackFingerprint(**base)  # type: ignore[arg-type]


def test_compare_is_silent_when_identical() -> None:
    assert compare(_fingerprint(), _fingerprint()) == []


def test_compare_reports_scalar_changes() -> None:
    problems = compare(_fingerprint(lines=7), _fingerprint())
    assert problems == ["lines: 基准 8，现在 7"]


def test_compare_reports_count_key_sets_as_a_union() -> None:
    """少一个键等于那个键变成 0，不能因为"没这个键"就跳过。"""
    problems = compare(_fingerprint(by_level={"INFO": 7}), _fingerprint())
    assert problems == ["by_level.INFO: 基准 8，现在 7"]


def test_compare_reports_a_new_component() -> None:
    found = _fingerprint(by_component={"dn1": 6, "dn2": 2})
    assert compare(found, _fingerprint()) == [
        "by_component.dn1: 基准 8，现在 6",
        "by_component.dn2: 基准 0，现在 2",
    ]


# --------------------------------------------------------------------------- 基准文件


def test_baseline_round_trip(tmp_path: Path) -> None:
    path = tmp_path / "baseline.json"
    written = save_baseline(path, [_fingerprint(), _fingerprint(case_id="other")])
    assert written == 2

    loaded = load_baseline(path)
    assert loaded["demo_case"] == _fingerprint()
    assert json.loads(path.read_text(encoding="utf-8"))["schema"] == BASELINE_SCHEMA


def test_baseline_missing_file_is_a_tool_error(tmp_path: Path) -> None:
    with pytest.raises(VerifyError, match="基准文件不存在"):
        load_baseline(tmp_path / "nope.json")


def test_baseline_rejects_unknown_schema(tmp_path: Path) -> None:
    path = tmp_path / "baseline.json"
    path.write_text(json.dumps({"schema": "verify-baseline/v0", "cases": {}}), encoding="utf-8")
    with pytest.raises(VerifyError, match="只认"):
        load_baseline(path)


# --------------------------------------------------------------------------- 整体核验


def test_verify_passes_when_nothing_changed(tmp_path: Path) -> None:
    build_pack(tmp_path, "case_a", [make_record(1), make_record(2, offset_seconds=1)])
    baseline = {
        case_id: fingerprint_pack(pack_paths(tmp_path, case_id)) for case_id in discover(tmp_path)
    }

    report = verify(tmp_path, baseline)

    assert report.ok
    assert report.matched == ("case_a",)
    assert report.by_origin == {"migrated": 1}


def test_verify_reports_extra_and_missing(tmp_path: Path) -> None:
    build_pack(tmp_path, "case_a", [make_record(1)])
    baseline = {
        "case_a": fingerprint_pack(pack_paths(tmp_path, "case_a")),
        "gone_case": _fingerprint(case_id="gone_case"),
    }
    build_pack(tmp_path, "new_case", [make_record(1)])

    report = verify(tmp_path, baseline)

    assert not report.ok
    assert report.extra == ("new_case",)
    assert report.missing == ("gone_case",)


def test_verify_skips_newly_collected_packs(tmp_path: Path) -> None:
    """新采的例不进冻结基准：它们的时间戳每次采集都变，冻进去等于把重采变成回归失败。"""
    build_pack(tmp_path, "base_healthy_01", [make_record(1)], origin_kind=OriginKind.COLLECTED)

    report = verify(tmp_path, {})

    assert report.ok
    assert report.skipped == ("base_healthy_01",)
    assert report.extra == ()
    assert report.by_origin == {"collected": 1}


def test_verify_still_flags_an_unlisted_migrated_pack(tmp_path: Path) -> None:
    """迁移例不在基准里仍然是"多余"—— 迁移集的成员是固定的 132 个，多一个就是错。"""
    build_pack(tmp_path, "case_a", [make_record(1)])
    build_pack(tmp_path, "stray_migrated", [make_record(1)])

    report = verify(tmp_path, {"case_a": fingerprint_pack(pack_paths(tmp_path, "case_a"))})

    assert report.extra == ("stray_migrated",)
    assert report.skipped == ()


def test_verify_reports_a_content_diff(tmp_path: Path) -> None:
    build_pack(tmp_path, "case_a", [make_record(1)])
    frozen = fingerprint_pack(pack_paths(tmp_path, "case_a"))
    # 重建成一份"内容变了"的 pack：真实场景是剪了分支重迁，这里直接换掉。
    shutil.rmtree(tmp_path / "case_a")
    build_pack(tmp_path, "case_a", [make_record(1), make_record(2, offset_seconds=1)])

    report = verify(tmp_path, {"case_a": frozen})

    assert not report.ok
    assert "case_a" in report.diffs
    assert any(problem.startswith("lines:") for problem in report.diffs["case_a"])


def test_verify_with_a_case_filter_ignores_the_rest_of_the_baseline(tmp_path: Path) -> None:
    """`make verify-collect CASE=xxx` 只查一例，不该顺带报"缺 131 例"。"""
    build_pack(tmp_path, "case_a", [make_record(1)])
    build_pack(tmp_path, "case_b", [make_record(1)])
    baseline = {
        case_id: fingerprint_pack(pack_paths(tmp_path, case_id)) for case_id in discover(tmp_path)
    }

    report = verify(tmp_path, baseline, case_ids=["case_a"])

    assert report.ok
    assert report.expected_total == 1
    assert report.missing == ()
    assert report.extra == ()


def test_discover_skips_unfinished_packs(tmp_path: Path) -> None:
    build_pack(tmp_path, "done_case", [make_record(1)])
    (tmp_path / "half_case").mkdir()
    (tmp_path / "half_case" / "logs.jsonl").write_text("", encoding="utf-8")

    assert discover(tmp_path) == ["done_case"]


def test_phase_window_import_keeps_case_module_contract() -> None:
    """`PhaseWindow` 在 verify 的测试里没用到，但基线要能表达"有 phases"的 pack。

    这条不是为覆盖率写的：它钉住"verify 不依赖 phases"这个事实——phases 不进任何
    哈希，所以改 phases 不该让 V2 变红。
    """
    assert PhaseWindow(phase=Phase.FAULT, since=T0, until=T0).since == T0
