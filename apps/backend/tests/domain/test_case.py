"""`domain/case.py` 的契约测试。

用法：`make test`。这些测试都在验"坏数据进不来"，不是在验"好数据能跑"——
好数据能跑是次要的，坏数据悄悄进 pack 才是要命的。
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import pytest

from logcopilot.domain.case import (
    SCHEMA,
    Adapter,
    AdapterState,
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
    Phase,
    PhaseWindow,
    RedactPolicy,
    RootCause,
    Severity,
    SourceRef,
    Window,
)

T0 = datetime(2026, 11, 22, 10, 3, 0, tzinfo=UTC)
T1 = datetime(2026, 11, 22, 10, 20, 0, tzinfo=UTC)
HASH_A = "sha256:" + "a" * 64
HASH_B = "sha256:" + "b" * 64


def make_manifest(**overrides: Any) -> CaseManifest:
    """造一个最小但完全合法的 manifest。测试只覆盖需要变的那一两个字段。"""
    defaults: dict[str, Any] = {
        "case_id": "inc_dnvol_01",
        "created_at": T1,
        "canonical_profile": CanonicalProfile.HDFS_LOG4J,
        "window": Window(
            since=T0,
            until=T1,
            phases=(
                PhaseWindow(phase=Phase.BASELINE, since=T0, until=T0 + timedelta(minutes=5)),
                PhaseWindow(
                    phase=Phase.FAULT,
                    since=T0 + timedelta(minutes=5),
                    until=T0 + timedelta(minutes=15),
                ),
            ),
        ),
        "origin": Origin(kind=OriginKind.COLLECTED),
        "sources": (
            SourceRef(
                adapter=Adapter.DOCKER,
                ref="datanode1",
                component="dn1",
                image_digest="sha256:" + "c" * 64,
                path="/opt/hadoop/logs/hadoop-hdfs-datanode-datanode1.log",
            ),
        ),
        "counters": Counters(
            raw_lines=10,
            lines=9,
            by_level={"INFO": 7, "ERROR": 2},
            by_component={"dn1": 9},
            parse_failed=1,
            redacted={"ip": 3},
        ),
        "hashes": Hashes(input_fingerprint=HASH_A, canonical_sha256=HASH_B),
        "checkpoint": Checkpoint(
            adapter_state={"dn1": AdapterState(offset=8399, line_no=112, inode=1234)},
            last_ts=T1 - timedelta(minutes=1),
        ),
        "redact_policy": RedactPolicy(version="v1"),
    }
    defaults.update(overrides)
    return CaseManifest(**defaults)


# ---------------------------------------------------------------- manifest


def test_manifest_round_trip_through_json() -> None:
    manifest = make_manifest()
    assert CaseManifest.from_json(manifest.to_json()) == manifest


def test_manifest_json_is_indented_and_ends_with_newline() -> None:
    text = make_manifest().to_json()
    assert text.endswith("\n")
    assert '\n  "case_id"' in text


def test_manifest_to_dict_key_order_matches_spec() -> None:
    assert list(make_manifest().to_dict()) == [
        "schema",
        "case_id",
        "created_at",
        "pipeline_version",
        "canonical_profile",
        "redact_policy",
        "tz",
        "config_overrides",
        "source",
        "window",
        "origin",
        "counters",
        "hashes",
        "checkpoint",
    ]


def test_manifest_rejects_unknown_schema() -> None:
    with pytest.raises(ValueError, match="schema"):
        make_manifest(schema="casepack/v2")


def test_manifest_rejects_bad_case_id() -> None:
    with pytest.raises(ValueError, match="case_id"):
        make_manifest(case_id="Inc-DNVOL")


def test_manifest_rejects_naive_created_at() -> None:
    with pytest.raises(ValueError, match="必须带时区"):
        make_manifest(created_at=datetime(2026, 11, 22, 10, 20))


def test_manifest_rejects_non_utc_tz() -> None:
    with pytest.raises(ValueError, match="tz"):
        make_manifest(tz="Asia/Shanghai")


def test_manifest_rejects_empty_source() -> None:
    with pytest.raises(ValueError, match="source 不能为空"):
        make_manifest(sources=())


def test_manifest_rejects_unknown_field_when_reading() -> None:
    data = make_manifest().to_dict()
    data["debug"] = True
    with pytest.raises(ValueError, match="未知字段"):
        CaseManifest.from_dict(data)


def test_manifest_rejects_missing_field_when_reading() -> None:
    data = make_manifest().to_dict()
    del data["hashes"]
    with pytest.raises(ValueError, match="缺少必需字段"):
        CaseManifest.from_dict(data)


def test_manifest_tolerates_missing_optional_fields() -> None:
    """老 pack 里没有 pipeline_version / tz 时按当前默认值补齐，不因此拒读。"""
    data = make_manifest().to_dict()
    for key in ("pipeline_version", "tz", "config_overrides"):
        del data[key]
    assert CaseManifest.from_dict(data).tz == "UTC"


def test_manifest_rejects_last_ts_after_window() -> None:
    with pytest.raises(ValueError, match="窗口外"):
        make_manifest(checkpoint=Checkpoint(last_ts=T1 + timedelta(seconds=1)))


def test_manifest_rejects_config_override_with_empty_value() -> None:
    with pytest.raises(ValueError, match="config_overrides"):
        make_manifest(config_overrides={"dfs.replication": ""})


# ---------------------------------------------------------------- counters


def test_counters_reconcile_by_level() -> None:
    with pytest.raises(ValueError, match="对账失败"):
        Counters(raw_lines=10, lines=9, by_level={"INFO": 8}, by_component={"dn1": 9})


def test_counters_reconcile_by_component() -> None:
    with pytest.raises(ValueError, match="对账失败"):
        Counters(raw_lines=10, lines=9, by_level={"INFO": 9}, by_component={"dn1": 8})


def test_counters_lines_cannot_exceed_raw_lines() -> None:
    with pytest.raises(ValueError, match="不能大于 raw_lines"):
        Counters(raw_lines=5, lines=6, by_level={"INFO": 6}, by_component={"dn1": 6})


def test_counters_reject_unknown_component_key() -> None:
    with pytest.raises(ValueError, match="by_component"):
        Counters(raw_lines=1, lines=1, by_level={"INFO": 1}, by_component={"dn9": 1})


def test_counters_reject_unknown_level_key() -> None:
    with pytest.raises(ValueError, match="by_level"):
        Counters(raw_lines=1, lines=1, by_level={"WARNING": 1}, by_component={"dn1": 1})


def test_counters_reject_unknown_redact_rule() -> None:
    with pytest.raises(ValueError, match="redacted"):
        Counters(
            raw_lines=1,
            lines=1,
            by_level={"INFO": 1},
            by_component={"dn1": 1},
            redacted={"password": 1},
        )


# ---------------------------------------------------------------- hashes


def test_hashes_require_sha256_prefix() -> None:
    with pytest.raises(ValueError, match="sha256"):
        Hashes(input_fingerprint="a" * 64, canonical_sha256=HASH_B)


def test_hashes_reject_uppercase_hex() -> None:
    with pytest.raises(ValueError, match="sha256"):
        Hashes(input_fingerprint="sha256:" + "A" * 64, canonical_sha256=HASH_B)


# ---------------------------------------------------------------- window


def test_window_rejects_inverted_range() -> None:
    with pytest.raises(ValueError, match="窗口倒挂"):
        Window(since=T1, until=T0)


def test_window_rejects_phase_outside_window() -> None:
    with pytest.raises(ValueError, match="越出采集窗口"):
        Window(
            since=T0,
            until=T1,
            phases=(PhaseWindow(phase=Phase.FAULT, since=T0, until=T1 + timedelta(minutes=1)),),
        )


def test_window_rejects_duplicate_phase() -> None:
    with pytest.raises(ValueError, match="出现了两次"):
        Window(
            since=T0,
            until=T1,
            phases=(
                PhaseWindow(phase=Phase.FAULT, since=T0, until=T0 + timedelta(minutes=1)),
                PhaseWindow(phase=Phase.FAULT, since=T0, until=T0 + timedelta(minutes=2)),
            ),
        )


def test_window_empty_phases_serialize_as_null() -> None:
    """可空是设计：旧 132 例没有 baseline 标注，不该让整条读路径炸掉。"""
    window = Window(since=T0, until=T1)
    assert window.to_dict()["phases"] is None
    assert Window.from_dict(window.to_dict()) == window


def test_window_phases_round_trip() -> None:
    window = make_manifest().window
    assert Window.from_dict(window.to_dict()) == window
    assert window.phase(Phase.BASELINE) is not None
    assert window.phase(Phase.RECOVERY) is None


# ---------------------------------------------------------------- origin / checkpoint


def test_origin_collected_must_not_carry_incident_sha256() -> None:
    with pytest.raises(ValueError, match="incident_sha256"):
        Origin(kind=OriginKind.COLLECTED, incident_sha256=HASH_A)


def test_origin_migrated_requires_incident_sha256() -> None:
    with pytest.raises(ValueError, match="incident_sha256"):
        Origin(kind=OriginKind.MIGRATED, src="ops/lab/cases/x/incident.json")


def test_origin_migrated_round_trip() -> None:
    origin = Origin(
        kind=OriginKind.MIGRATED,
        src="ops/lab/cases/x/incident.json",
        incident_sha256=HASH_A,
    )
    assert Origin.from_dict(origin.to_dict()) == origin


def test_checkpoint_adapter_state_round_trip() -> None:
    checkpoint = Checkpoint(
        adapter_state={"dn1": AdapterState(offset=10, line_no=2, tail_line_sha1="d" * 40)}
    )
    assert Checkpoint.from_dict(checkpoint.to_dict()) == checkpoint


def test_adapter_state_rejects_bad_sha1() -> None:
    with pytest.raises(ValueError, match="sha1"):
        AdapterState(offset=0, line_no=0, tail_line_sha1="not-a-sha1")


# ---------------------------------------------------------------- source / redact


def test_source_ref_maps_container_adapter_and_validates_component() -> None:
    # 传字符串是运行时支持的行为（构造时收进枚举），类型标注写的是收进来的
    # 形态，所以这里显式 cast：测试在越界使用"更宽松的入口"。
    source = SourceRef(adapter=cast(Any, "docker"), ref="namenode", component="nn")
    assert source.adapter is Adapter.DOCKER
    with pytest.raises(ValueError, match="component"):
        SourceRef(adapter=Adapter.DOCKER, ref="x", component="namenode")


def test_source_ref_rejects_unknown_adapter() -> None:
    with pytest.raises(ValueError, match="adapter"):
        SourceRef(adapter=cast(Any, "http"), ref="x", component="nn")


def test_redact_policy_rejects_unknown_rule() -> None:
    with pytest.raises(ValueError, match="rules"):
        RedactPolicy(version="v1", rules=("ip", "password"))


# ---------------------------------------------------------------- labels


def test_labels_minimal_round_trip() -> None:
    """正常例（base_healthy_01）没有根因、没有注入记录，也必须能写能读。"""
    labels = Labels(case_id="base_healthy_01")
    assert Labels.from_json(labels.to_json()) == labels
    assert json.loads(labels.to_json())["root_cause"] is None


def test_labels_full_round_trip() -> None:
    labels = Labels(
        case_id="inc_dnvol_01",
        root_cause=RootCause.DN_VOLUME,
        severity=Severity.ERROR,
        fault_injection=FaultInjection(
            script="inject/dn_volume_full.sh",
            started=T0,
            ended=T0 + timedelta(minutes=10),
            restored=T0 + timedelta(minutes=10, seconds=5),
            restored_ok=True,
        ),
        expect=Expect(min_error_lines=1, min_warn_lines=5, error_components=("dn1",)),
        notes="dd 写满 tmpfs 128m",
    )
    assert Labels.from_json(labels.to_json()) == labels


def test_labels_reject_inverted_fault_injection() -> None:
    with pytest.raises(ValueError, match="时间倒挂"):
        FaultInjection(script="x.sh", started=T1, ended=T0)


def test_labels_restored_ok_requires_restored_timestamp() -> None:
    with pytest.raises(ValueError, match="restored_ok"):
        FaultInjection(script="x.sh", started=T0, ended=T1, restored_ok=True)


def test_labels_reject_unknown_root_cause() -> None:
    with pytest.raises(ValueError, match="root_cause"):
        Labels(case_id="inc_x_01", root_cause=cast(Any, "DISK_FULL"))


def test_labels_reject_bad_phase_map_key() -> None:
    with pytest.raises(ValueError, match="phase_map"):
        Labels(case_id="inc_x_01", phase_map={"warmup": "x"})


def test_labels_expect_rejects_unknown_component() -> None:
    with pytest.raises(ValueError, match="error_components"):
        Expect(error_components=("nn2",))


def test_labels_rejects_unknown_field_when_reading() -> None:
    data = Labels(case_id="inc_x_01").to_dict()
    data["confidence"] = "confirmed"
    with pytest.raises(ValueError, match="未知字段"):
        Labels.from_dict(data)


def test_schema_constant_is_v1() -> None:
    assert SCHEMA == "casepack/v1"
