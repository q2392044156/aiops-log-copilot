"""`collect/redact.py` 的契约测试（P-A V8「脱敏有账」）。

两类断言必须同时成立，缺一条这条流水线就不可信：

  1. **该脱的脱干净** —— 四类规则各有正例。
  2. **脱过的不能变** —— 幂等：同一段文本跑两遍，文本逐字节相同、计数为零。
     旧 132 例的日志早就脱过一遍，满屏 `<:IP:>`；如果重复脱敏会改动它们，
     `canonical_sha256` 就跟着变，V4 与两条哈希不变式当场失效。
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from logcopilot.collect.canonical import canonicalize
from logcopilot.collect.redact import (
    DEFAULT_ORDER,
    PLACEHOLDERS,
    REDACT_VERSION,
    PatternRedactor,
)
from logcopilot.domain.case import REDACT_RULES, CanonicalProfile, RedactPolicy

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "logs"


@pytest.fixture
def redactor() -> PatternRedactor:
    return PatternRedactor()


# ---------------------------------------------------------------- ip


def test_ip_is_redacted(redactor: PatternRedactor) -> None:
    text, counts = redactor.apply("connect to 10.0.0.1:9866 failed")
    assert text == "connect to <:IP:>:9866 failed"
    assert counts == {"ip": 1}


def test_ip_multiple_hits_counted(redactor: PatternRedactor) -> None:
    _, counts = redactor.apply("from 10.0.0.1 to 10.0.0.2 via 192.168.1.100")
    assert counts == {"ip": 3}


def test_three_octet_version_is_not_ip(redactor: PatternRedactor) -> None:
    """`3.3.6` 是 Hadoop 版本号，三段，不该被当成 IP。"""
    text, counts = redactor.apply("version = 3.3.6")
    assert text == "version = 3.3.6"
    assert counts == {}


def test_ip_must_not_be_glued_to_other_chars(redactor: PatternRedactor) -> None:
    """`1.2.3.4.5` 与 `x1.2.3.4x` 都不是一个独立的 IP。"""
    text, counts = redactor.apply("1.2.3.4.5 and x10.0.0.1x")
    assert text == "1.2.3.4.5 and x10.0.0.1x"
    assert counts == {}


# ---------------------------------------------------------------- host


@pytest.mark.parametrize(
    "host",
    ["localhost", "WIN-20260727MVX", "hdfs-datanode2", "hdfs-namenode", "foo.corp.local"],
)
def test_host_forms_are_redacted(redactor: PatternRedactor, host: str) -> None:
    text, counts = redactor.apply(f"reported by {host} now")
    assert text == "reported by <:HOST:> now"
    assert counts == {"host": 1}


@pytest.mark.parametrize(
    "safe",
    [
        "org.apache.hadoop.hdfs.server.datanode.DataNode",
        "DataNode",
        "datanode2",
        "DataNode.java",
        "WIN-ABC",  # 太短，不像机器名
        "aiops-lab-dn1",  # 旧数据的结构标签，归一化成 dn1 时已经处理过
    ],
)
def test_class_names_are_not_hosts(redactor: PatternRedactor, safe: str) -> None:
    """脱掉类名等于把日志内容删了 —— P-C 再也认不出是哪一层在报错。"""
    text, counts = redactor.apply(f"at {safe} line 1")
    assert text == f"at {safe} line 1"
    assert counts == {}


# ---------------------------------------------------------------- path


def test_absolute_path_is_redacted(redactor: PatternRedactor) -> None:
    text, counts = redactor.apply("volume failure on /data/1")
    assert text == "volume failure on <:PATH:>"
    assert counts == {"path": 1}


def test_nested_path_is_redacted(redactor: PatternRedactor) -> None:
    text, _ = redactor.apply("log at /usr/local/hadoop/logs/hadoop-datanode.log")
    assert text == "log at <:PATH:>"


def test_windows_path_is_redacted(redactor: PatternRedactor) -> None:
    text, counts = redactor.apply(r"saved to C:\Users\Administrator\a.txt")
    assert text == "saved to <:PATH:>"
    assert counts == {"path": 1}


def test_uri_path_is_redacted(redactor: PatternRedactor) -> None:
    """`file:/data/1` 里的 `/data/1` 也要脱（零宽断言给 file: 开了口子）。"""
    text, _ = redactor.apply("URI file:/var/log/hadoop/hdfs.log")
    assert text == "URI file:<:PATH:>"


@pytest.mark.parametrize(
    "safe",
    [
        "either and/or both",
        "/************************************************************",
        "src: /<:IP:> dest: /<:IP:>",
        "blk_1073741944_1120/current/finalized/subdir0",
        "hdfs://nn:8020/foo",
    ],
)
def test_relative_and_decorative_slashes_are_untouched(
    redactor: PatternRedactor, safe: str
) -> None:
    text, counts = redactor.apply(safe)
    assert text == safe
    assert counts == {}


# ---------------------------------------------------------------- token


@pytest.mark.parametrize(
    "secret",
    [
        "sk-abcdefghijklmnopqrstu",
        "ghp_abcdefghijklmnopqrstuvwxyz",
        "AKIAIOSFODNN7EXAMPLE",
        "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.abcdefghij",
    ],
)
def test_token_forms_are_redacted(redactor: PatternRedactor, secret: str) -> None:
    text, counts = redactor.apply(f"credential {secret} used")
    assert secret not in text
    assert "<:TOKEN:>" in text
    assert counts == {"token": 1}


def test_key_value_secret_keeps_the_key_name(redactor: PatternRedactor) -> None:
    """`password=hunter2` → `password=<:TOKEN:>`：键名留下，值换掉。"""
    text, counts = redactor.apply("password=hunter2")
    assert text == "password=<:TOKEN:>"
    assert counts == {"token": 1}


def test_two_token_shapes_share_one_counter(redactor: PatternRedactor) -> None:
    _, counts = redactor.apply("password=hunter2 and sk-abcdefghijklmnopqrstu")
    assert counts == {"token": 2}
    assert set(counts) <= set(REDACT_RULES)


@pytest.mark.parametrize(
    "safe",
    ["blk_1073741944_1120", "6faafd61-4bb9-466d-a697-d572030217c7", "invalid token here"],
)
def test_identifiers_are_not_tokens(redactor: PatternRedactor, safe: str) -> None:
    """块 ID 与 UUID 是诊断的关键线索，脱掉就查不出是哪块副本出问题。"""
    text, counts = redactor.apply(safe)
    assert text == safe
    assert counts == {}


# ---------------------------------------------------------------- 幂等


@pytest.mark.parametrize(
    "text",
    [
        "connect to 10.0.0.1 on /data/1 as password=hunter2",
        "already <:IP:> and <:PATH:> and <:BLOCK:> here",
        "host WIN-20260727MVX and sk-abcdefghijklmnopqrstu",
        "no secrets at all",
        "",
    ],
)
def test_apply_is_idempotent(redactor: PatternRedactor, text: str) -> None:
    first, counts = redactor.apply(text)
    second, again = redactor.apply(first)
    assert second == first
    assert again == {}
    if text:
        assert counts == {} or set(counts) <= set(REDACT_RULES)


def test_preexisting_placeholders_survive_untouched(redactor: PatternRedactor) -> None:
    """旧数据的 `<:IP:>` / `<:PATH:>` / `<:BLOCK:>` 一个字节都不能变。"""
    text = "<:IP:> <:PATH:> <:BLOCK:> <:HOST:> <:SOMETHING_NEW:>"
    out, counts = redactor.apply(text)
    assert out == text
    assert counts == {}


def test_real_fixture_needs_no_redaction(redactor: PatternRedactor) -> None:
    """旧 132 例实测：已经脱过了，再跑一遍应当毫无命中（V8 的已知情形）。"""
    lines = FIXTURES.joinpath("lab_syntax_a.log").read_text(encoding="utf-8").splitlines()
    result = canonicalize(
        enumerate(lines, start=1),
        profile=CanonicalProfile.LAB,
        component="dn1",
        source="file:dn1",
        since=datetime(2026, 11, 11, 8, 0, tzinfo=UTC),
        redact=redactor,
    )
    assert result.redacted == {}


# ---------------------------------------------------------------- 配置


def test_rules_subset_can_be_selected() -> None:
    partial = PatternRedactor(("ip",))
    assert partial.rules == ("ip",)
    text, counts = partial.apply("10.0.0.1 on /data/1")
    assert text == "<:IP:> on /data/1"
    assert counts == {"ip": 1}


def test_unknown_rule_is_rejected() -> None:
    with pytest.raises(ValueError, match="未知脱敏规则"):
        PatternRedactor(("ip", "password"))


def test_empty_rules_are_rejected() -> None:
    with pytest.raises(ValueError, match="不能为空"):
        PatternRedactor(())


def test_every_rule_has_a_placeholder() -> None:
    assert set(PLACEHOLDERS) == set(REDACT_RULES)
    assert DEFAULT_ORDER == ("ip", "token", "host", "path")


def test_version_feeds_manifest_redact_policy() -> None:
    """`redact_policy` 的两个字段都来自本模块，且必须过 manifest 的校验。"""
    redactor = PatternRedactor()
    policy = RedactPolicy(version=redactor.version, rules=redactor.rules)
    assert policy.version == REDACT_VERSION
    assert policy.rules == DEFAULT_ORDER


def test_all_four_rules_fire_in_one_pass(redactor: PatternRedactor) -> None:
    text, counts = redactor.apply("host WIN-20260727MVX ip 10.0.0.1 path /data/1 token=abc123")
    assert counts == {"host": 1, "ip": 1, "path": 1, "token": 1}
    assert all(placeholder in text for placeholder in PLACEHOLDERS.values())
