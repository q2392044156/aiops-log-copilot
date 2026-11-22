"""`collect/migrate.py` 的契约测试。

这里刻意**不碰真实旧项目**：测试要能在任何机器上跑，所以用 `tmp_path`
搭一个"迷你旧实验台"（registry + incident.json + evidence/logs/*.log），
真实 132 例的迁移由 `scripts/migrate_cases.py` 在本地跑一次即可。

重点验四件事：
  1. 时区按**来源**平移（docker 不动、wsl −8h），不是按案例猜；
  2. 窗口 = 日志真正覆盖的区间，`fault` 段被裁剪进窗口里；
  3. 旧数据缺的东西（baseline / 注入记录 / expect）在包里**显式为空**，
     而不是被默认值伪装成"有"；
  4. 一例失败不影响整批。
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import pytest

from logcopilot.collect.migrate import (
    LegacyCase,
    MigrationError,
    discover_legacy_cases,
    load_legacy_case,
    migrate_all,
    migrate_case,
)
from logcopilot.domain.case import CanonicalProfile, OriginKind, Phase, Window
from logcopilot.store.casepack import audit, audit_levels, pack_paths, read_labels, read_manifest


def _phase(window: Window, name: str):
    """取某一段的起止；没标这一段时返回 None。"""
    return window.phase(Phase(name))


# ---------------------------------------------------------------- 搭旧实验台

LOG_DOCKER = """\
[aiops-lab-dn1] 2026-11-11 08:15:05 INFO  DataNode:750 - Receiving block blk_1
[aiops-lab-dn1] 2026-11-11 08:15:09 WARN  FsDatasetImpl:1312 - Slow write
[aiops-lab-dn1] 2026-11-11 08:15:20 ERROR DataNode:1201 - Disk volume failure
"""

# WSL 组：本地时间，比 UTC 快 8 小时。同样的时刻在 UTC 里是 04:52。
LOG_WSL = """\
[aiops-lab-dn2] 2026-10-28 12:52:12 INFO  DataNode:750 - Starting
[aiops-lab-dn2] 2026-10-28 12:52:30 WARN  DataNode:900 - Heartbeat late
[aiops-lab-dn2] 2026-10-28 12:53:01 WARN  DataNode:900 - Heartbeat late again
"""


def _incident(
    case_id: str,
    *,
    root_cause: str | None = "DN_VOLUME",
    fault_start: str | None = "2026-11-11T08:15:10Z",
    recovered: str | None = None,
    log_sha256: str | None = None,
) -> dict[str, object]:
    refs = (
        [{"content_sha256": log_sha256, "redaction_status": "approved_redacted"}]
        if log_sha256 is not None
        else []
    )
    return {
        "incident_id": case_id,
        "fault_family": "dn_volume",
        "labels": {"root_cause_code": root_cause, "review_status": "reviewed"},
        "timestamps": {
            "fault_start": fault_start,
            "recovered": recovered,
            "timezone_source": "Asia/Shanghai (lab-ntp)",
        },
        "diagnosis_snapshot": {"log_refs": refs},
    }


def _write_case(
    old_root: Path,
    case_id: str,
    *,
    group: str,
    log_text: str,
    incident: dict[str, object] | None = None,
) -> Path:
    case_dir = old_root / group / case_id
    (case_dir / "evidence/logs").mkdir(parents=True)
    (case_dir / "incident.json").write_text(
        json.dumps(incident if incident is not None else _incident(case_id), ensure_ascii=False),
        encoding="utf-8",
    )
    (case_dir / f"evidence/logs/{case_id}.log").write_text(log_text, encoding="utf-8")
    return case_dir


def _write_registry(old_root: Path, docker_ids: list[str], wsl_ids: list[str]) -> None:
    for rel, ids in (
        ("runtime/unified4/docker_source.sqlite3", docker_ids),
        ("runtime/unified4/wsl_source.sqlite3", wsl_ids),
    ):
        path = old_root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            # 同一个 fixture 里可能改写 registry（比如造"registry 里有、目录里没有"），
            # 重写前先清干净，别让 CREATE TABLE 撞上上一次的表。
            path.unlink()
        # 显式 close：Windows 上没关掉的连接会锁住文件，下一次 unlink 就报 WinError 32。
        conn = sqlite3.connect(path)
        try:
            with conn:
                conn.execute("CREATE TABLE incident_registry (incident_id TEXT PRIMARY KEY)")
                conn.executemany(
                    "INSERT INTO incident_registry (incident_id) VALUES (?)",
                    [(case_id,) for case_id in ids],
                )
        finally:
            conn.close()


@pytest.fixture
def old_root(tmp_path: Path) -> Path:
    """两个病例：docker 组一例（覆盖 fault），wsl 组一例（日志到故障前）。"""
    root = tmp_path / "old"
    _write_case(root, "inc_dock_01", group="ops/lab/real_batch_cases", log_text=LOG_DOCKER)
    _write_case(
        root,
        "inc_wsl_01",
        group="ops/lab/wsl_cases",
        log_text=LOG_WSL,
        incident=_incident(
            "inc_wsl_01",
            root_cause="DN_HEARTBEAT",
            fault_start="2026-10-28T04:53:30Z",  # 比日志末行还晚 → 日志不含故障期
        ),
    )
    _write_registry(root, ["inc_dock_01"], ["inc_wsl_01"])
    return root


# ---------------------------------------------------------------- 读旧病例


def test_load_legacy_case_reads_incident_and_log(old_root: Path) -> None:
    case = load_legacy_case(old_root / "ops/lab/real_batch_cases/inc_dock_01", source="docker")

    assert case.case_id == "inc_dock_01"
    assert case.group == "real_batch_cases"
    assert case.root_cause == "DN_VOLUME"
    assert case.fault_start == datetime(2026, 11, 11, 8, 15, 10, tzinfo=UTC)
    assert case.incident_sha256.startswith("sha256:")
    assert len(case.incident_sha256) == len("sha256:") + 64
    assert case.log_sha256 == "sha256:" + hashlib.sha256(LOG_DOCKER.encode("utf-8")).hexdigest()


def test_load_legacy_case_normalizes_crlf_before_hashing(tmp_path: Path) -> None:
    """旧项目的 content_sha256 是按 LF 归一后的文本算的，我们要同口径。"""
    case_dir = tmp_path / "case"
    (case_dir / "evidence/logs").mkdir(parents=True)
    (case_dir / "incident.json").write_text(json.dumps(_incident("c1")), encoding="utf-8")
    (case_dir / "evidence/logs/c1.log").write_bytes(LOG_DOCKER.replace("\n", "\r\n").encode())

    case = load_legacy_case(case_dir, source="docker")

    assert "\r" not in case.log_text
    assert case.log_sha256 == "sha256:" + hashlib.sha256(LOG_DOCKER.encode("utf-8")).hexdigest()


def test_load_legacy_case_rejects_missing_log(tmp_path: Path) -> None:
    case_dir = tmp_path / "case"
    (case_dir / "evidence/logs").mkdir(parents=True)
    (case_dir / "incident.json").write_text(json.dumps(_incident("c1")), encoding="utf-8")

    with pytest.raises(MigrationError, match="没有 .log"):
        load_legacy_case(case_dir, source="docker")


def test_load_legacy_case_rejects_multiple_logs(tmp_path: Path) -> None:
    case_dir = tmp_path / "case"
    (case_dir / "evidence/logs").mkdir(parents=True)
    (case_dir / "incident.json").write_text(json.dumps(_incident("c1")), encoding="utf-8")
    (case_dir / "evidence/logs/a.log").write_text(LOG_DOCKER, encoding="utf-8")
    (case_dir / "evidence/logs/b.log").write_text(LOG_DOCKER, encoding="utf-8")

    with pytest.raises(MigrationError, match="预期 1 份"):
        load_legacy_case(case_dir, source="docker")


# ---------------------------------------------------------------- 找 132 例


def test_discover_uses_registry_and_marks_source(old_root: Path) -> None:
    cases = discover_legacy_cases(old_root)

    assert [case.case_id for case in cases] == ["inc_dock_01", "inc_wsl_01"]
    assert {case.case_id: case.source for case in cases} == {
        "inc_dock_01": "docker",
        "inc_wsl_01": "wsl",
    }


def test_source_ref_is_relative_to_the_old_project_root(old_root: Path) -> None:
    """`source.ref` 会进 input_fingerprint，所以不能带本机绝对路径。

    同一份旧数据放在 `D:\\lab` 或 `E:\\project\\...`，指纹必须一样 —— 指纹回答的是
    "数据变了没有"，不是"它躺在哪个盘"。
    """
    cases = discover_legacy_cases(old_root)

    case = next(c for c in cases if c.case_id == "inc_dock_01")
    assert case.rel_log_path == (
        "ops/lab/real_batch_cases/inc_dock_01/evidence/logs/inc_dock_01.log"
    )
    assert str(old_root).replace("\\", "/") not in case.rel_log_path


def test_discover_ignores_dirs_outside_registry(old_root: Path) -> None:
    """旧目录里有 197 个子目录，只有 registry 里的 132 个算数。"""
    _write_case(old_root, "inc_extra_99", group="ops/lab/real_batch_cases", log_text=LOG_DOCKER)

    cases = discover_legacy_cases(old_root)

    assert "inc_extra_99" not in {case.case_id for case in cases}


def test_discover_reports_missing_directories(old_root: Path) -> None:
    _write_registry(old_root, ["inc_dock_01", "inc_gone"], [])

    with pytest.raises(MigrationError, match="找不到目录"):
        discover_legacy_cases(old_root)


# ---------------------------------------------------------------- 时区与窗口


def test_docker_case_keeps_utc_timestamps(old_root: Path, tmp_path: Path) -> None:
    case = load_legacy_case(old_root / "ops/lab/real_batch_cases/inc_dock_01", source="docker")
    result = migrate_case(case, root=tmp_path / "out")

    manifest = read_manifest(pack_paths(tmp_path / "out", case.case_id))
    assert manifest.window.since == datetime(2026, 11, 11, 8, 15, 5, tzinfo=UTC)
    assert manifest.window.until == datetime(2026, 11, 11, 8, 15, 20, tzinfo=UTC)
    assert result.shifted.total_seconds() == 0


def test_wsl_case_is_shifted_eight_hours_to_utc(old_root: Path, tmp_path: Path) -> None:
    """WSL 组写的是本地 +08，减 8 小时才是 UTC。"""
    case = load_legacy_case(old_root / "ops/lab/wsl_cases/inc_wsl_01", source="wsl")
    result = migrate_case(case, root=tmp_path / "out")

    manifest = read_manifest(pack_paths(tmp_path / "out", case.case_id))
    assert manifest.window.since == datetime(2026, 10, 28, 4, 52, 12, tzinfo=UTC)
    assert manifest.window.until == datetime(2026, 10, 28, 4, 53, 1, tzinfo=UTC)
    assert result.shifted.total_seconds() == -8 * 3600


def test_timezone_shift_is_recorded_in_config_overrides(old_root: Path, tmp_path: Path) -> None:
    case = load_legacy_case(old_root / "ops/lab/wsl_cases/inc_wsl_01", source="wsl")
    migrate_case(case, root=tmp_path / "out")

    manifest = read_manifest(pack_paths(tmp_path / "out", case.case_id))
    assert manifest.config_overrides["legacy_tz_offset_hours"] == "-8"
    assert manifest.config_overrides["legacy_source"] == "wsl"


def test_fault_phase_is_clamped_into_the_log_window(old_root: Path, tmp_path: Path) -> None:
    """fault_start 落在日志中间时，fault 段从它开始、到日志末行结束。"""
    case = load_legacy_case(old_root / "ops/lab/real_batch_cases/inc_dock_01", source="docker")
    result = migrate_case(case, root=tmp_path / "out")

    manifest = read_manifest(pack_paths(tmp_path / "out", case.case_id))
    assert result.phases == ("fault",)
    fault = _phase(manifest.window, "fault")
    assert fault is not None
    assert fault.since == datetime(2026, 11, 11, 8, 15, 10, tzinfo=UTC)
    assert fault.until == manifest.window.until


def test_case_ending_before_fault_has_no_phase_and_says_so(old_root: Path, tmp_path: Path) -> None:
    """22 例的真实现状：日志在 fault_start 之前就结束了，故障信号为空。"""
    case = load_legacy_case(old_root / "ops/lab/wsl_cases/inc_wsl_01", source="wsl")
    result = migrate_case(case, root=tmp_path / "out")

    paths = pack_paths(tmp_path / "out", case.case_id)
    manifest = read_manifest(paths)
    labels = read_labels(paths)

    assert result.covers_fault is False
    assert manifest.window.phases == ()
    assert labels.notes is not None
    assert "不含任何故障期日志" in labels.notes


def test_phase_is_empty_when_recovered_precedes_fault_start(tmp_path: Path) -> None:
    """旧数据里 recovered 早于 fault_start 的脏值不能把整份 manifest 拒掉。"""
    root = tmp_path / "old"
    _write_case(
        root,
        "inc_bad_recovery",
        group="ops/lab/real_batch_cases",
        log_text=LOG_DOCKER,
        incident=_incident(
            "inc_bad_recovery",
            fault_start="2026-11-11T08:15:10Z",
            recovered="2026-11-11T08:15:01Z",
        ),
    )
    case = load_legacy_case(root / "ops/lab/real_batch_cases/inc_bad_recovery", source="docker")
    migrate_case(case, root=tmp_path / "out")

    fault = _phase(read_manifest(pack_paths(tmp_path / "out", case.case_id)).window, "fault")
    assert fault is not None
    assert fault.until == datetime(2026, 11, 11, 8, 15, 20, tzinfo=UTC)


# ---------------------------------------------------------------- labels / origin


def test_labels_map_root_cause_and_leave_expect_empty(old_root: Path, tmp_path: Path) -> None:
    case = load_legacy_case(old_root / "ops/lab/real_batch_cases/inc_dock_01", source="docker")
    migrate_case(case, root=tmp_path / "out")

    labels = read_labels(pack_paths(tmp_path / "out", case.case_id))
    assert str(labels.root_cause) == "DN_VOLUME"
    assert labels.expect is None  # 不由数据反推
    assert labels.severity is None  # 旧项目没有这个字段
    assert labels.reviewer == "migrated"
    assert labels.notes is not None
    assert "无 fault_injection 记录" in labels.notes
    assert "metrics.jsonl 留空" in labels.notes


def test_origin_is_migrated_with_incident_hash(old_root: Path, tmp_path: Path) -> None:
    case = load_legacy_case(old_root / "ops/lab/real_batch_cases/inc_dock_01", source="docker")
    migrate_case(case, root=tmp_path / "out")

    manifest = read_manifest(pack_paths(tmp_path / "out", case.case_id))
    assert manifest.origin.kind is OriginKind.MIGRATED
    assert manifest.origin.incident_sha256 == case.incident_sha256
    assert manifest.origin.src == "real_batch_cases/inc_dock_01"
    assert manifest.canonical_profile is CanonicalProfile.LAB


def test_one_source_ref_per_component_present(tmp_path: Path) -> None:
    """旧日志一个文件里可能有 4 台机器交替打日志，每台都要有出处。"""
    root = tmp_path / "old"
    mixed = (
        "[aiops-lab-nn] 2026-11-11 08:15:05 INFO  FSNamesystem:750 - enter safe mode\n"
        "[aiops-lab-dn1] 2026-11-11 08:15:06 WARN  DataNode:750 - heartbeat\n"
        "[aiops-lab-dn2] 2026-11-11 08:15:07 WARN  DataNode:750 - heartbeat\n"
    )
    _write_case(root, "inc_mixed", group="ops/lab/real_batch_cases", log_text=mixed)
    case = load_legacy_case(root / "ops/lab/real_batch_cases/inc_mixed", source="docker")
    migrate_case(case, root=tmp_path / "out")

    manifest = read_manifest(pack_paths(tmp_path / "out", case.case_id))
    assert sorted(source.component for source in manifest.sources) == ["dn1", "dn2", "nn"]


def test_unknown_root_cause_is_rejected(tmp_path: Path) -> None:
    root = tmp_path / "old"
    _write_case(
        root,
        "inc_weird",
        group="ops/lab/real_batch_cases",
        log_text=LOG_DOCKER,
        incident=_incident("inc_weird", root_cause="JOURNAL_QUORUM"),
    )
    case = load_legacy_case(root / "ops/lab/real_batch_cases/inc_weird", source="docker")

    with pytest.raises(MigrationError, match="不在新闭集"):
        migrate_case(case, root=tmp_path / "out")


def test_empty_log_is_rejected(tmp_path: Path) -> None:
    root = tmp_path / "old"
    _write_case(root, "inc_empty", group="ops/lab/real_batch_cases", log_text="")
    case = load_legacy_case(root / "ops/lab/real_batch_cases/inc_empty", source="docker")

    with pytest.raises(MigrationError, match="日志是空的"):
        migrate_case(case, root=tmp_path / "out")


# ---------------------------------------------------------------- 产物合法性


def test_migrated_pack_passes_audit_and_levels(old_root: Path, tmp_path: Path) -> None:
    case = load_legacy_case(old_root / "ops/lab/real_batch_cases/inc_dock_01", source="docker")
    migrate_case(case, root=tmp_path / "out")

    paths = pack_paths(tmp_path / "out", case.case_id)
    manifest = read_manifest(paths)
    labels = read_labels(paths)

    assert audit(paths) == []
    # 迁移例不适用 V5/V6：它没有 expect，也不该因为"没有 ERROR"被判失败。
    assert audit_levels(paths, manifest, labels) == []


def test_migrated_pack_has_real_hashes(old_root: Path, tmp_path: Path) -> None:
    case = load_legacy_case(old_root / "ops/lab/real_batch_cases/inc_dock_01", source="docker")
    migrate_case(case, root=tmp_path / "out")

    manifest = read_manifest(pack_paths(tmp_path / "out", case.case_id))
    assert manifest.hashes.input_fingerprint.startswith("sha256:")
    assert len(manifest.hashes.input_fingerprint) == len("sha256:") + 64
    assert len(manifest.hashes.canonical_sha256) == len("sha256:") + 64
    assert not manifest.hashes.canonical_sha256.startswith("sha256:0000")


def test_upstream_hash_mismatch_is_reported_not_raised(tmp_path: Path) -> None:
    """旧项目自己记的 content_sha256 对不上时，包照建，问题记进 result。"""
    root = tmp_path / "old"
    _write_case(
        root,
        "inc_hash",
        group="ops/lab/real_batch_cases",
        log_text=LOG_DOCKER,
        incident=_incident("inc_hash", log_sha256="f" * 64),
    )
    case = load_legacy_case(root / "ops/lab/real_batch_cases/inc_hash", source="docker")

    result = migrate_case(case, root=tmp_path / "out")

    assert result.hash_matches_upstream is False
    assert any("content_sha256" in problem for problem in result.problems)


def test_upstream_hash_match_is_silent(tmp_path: Path) -> None:
    root = tmp_path / "old"
    digest = hashlib.sha256(LOG_DOCKER.encode("utf-8")).hexdigest()
    _write_case(
        root,
        "inc_hash_ok",
        group="ops/lab/real_batch_cases",
        log_text=LOG_DOCKER,
        incident=_incident("inc_hash_ok", log_sha256=digest),
    )
    case = load_legacy_case(root / "ops/lab/real_batch_cases/inc_hash_ok", source="docker")

    result = migrate_case(case, root=tmp_path / "out")

    assert result.hash_matches_upstream is True
    assert result.problems == ()


# ---------------------------------------------------------------- 重跑与批量


def test_existing_pack_is_not_overwritten(old_root: Path, tmp_path: Path) -> None:
    case = load_legacy_case(old_root / "ops/lab/real_batch_cases/inc_dock_01", source="docker")
    migrate_case(case, root=tmp_path / "out")

    with pytest.raises(MigrationError, match="--force"):
        migrate_case(case, root=tmp_path / "out")


def test_force_rewrites_existing_pack(old_root: Path, tmp_path: Path) -> None:
    case = load_legacy_case(old_root / "ops/lab/real_batch_cases/inc_dock_01", source="docker")
    first = migrate_case(case, root=tmp_path / "out")
    second = migrate_case(case, root=tmp_path / "out", force=True)

    assert first.records == second.records
    assert audit(pack_paths(tmp_path / "out", case.case_id)) == []


def test_migrate_all_keeps_going_after_a_failure(old_root: Path, tmp_path: Path) -> None:
    good = load_legacy_case(old_root / "ops/lab/real_batch_cases/inc_dock_01", source="docker")
    bad = LegacyCase(
        case_id="inc_broken",
        source="docker",
        group="real_batch_cases",
        rel_log_path="x.log",
        root_cause=None,
        fault_start=None,
        recovered=None,
        incident_sha256="sha256:" + "0" * 64,
        log_sha256="sha256:" + "0" * 64,
        upstream_log_sha256=None,
        log_text="",
    )

    report = migrate_all([bad, good], root=tmp_path / "out")

    assert report.ok == 1
    assert len(report.failures) == 1
    assert report.failures[0][0] == "inc_broken"
