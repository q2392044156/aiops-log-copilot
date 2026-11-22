# 顺序 3 · 元信息与标注：manifest.json + labels.json（节点 6 的 finalize 负责写入）
"""
CasePack v1 的"身份证"与"标注" —— `manifest.json` 与 `labels.json`。

一个 CasePack 目录里有四个文件，本文件负责其中两个的**结构与取值规则**：

```
data/cases/<case_id>/
├─ manifest.json   ← CaseManifest（本文件）
├─ logs.jsonl      ← LogRecord（domain/log.py，已实现）
├─ metrics.jsonl   ← 第 8 步迁移/采集时再定
├─ labels.json     ← Labels（本文件）
└─ anchors/        ← 指针，第 4 步 store/casepack.py
```

规格来源 `P-A_COLLECT_PLAN.md`：
  §5      manifest 字段表（第 209-235 行）+ labels 字段表（第 237-252 行）
  §6.4    两个哈希的定义与**排除项** —— pipeline_version / redact_policy /
          canonical_profile 一律不进哈希，否则"代码重构"会被误判成"数据变了"
  §10     O3 已拍板：`tz` 固定 UTC，canonical 层不再猜日志时区

为什么校验全部塞在 `__post_init__`、而不是留给读写 pack 的时候顺手做：
  一个 CasePack 一旦落盘并被 P-B 建进索引，再发现 manifest 少了
  `canonical_profile` 就晚了——那时要重采一遍。这里每一条都是"写盘之前
  最后一道能拦住坏数据的闸门"，`store/casepack.py`（第 4 步）只负责搬字节，
  不再重复判断内容对不对。
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any, Final

# 复用 log.py 的三个小工具：闭集校验、时间格式化、UTC 校验。
# 不各写一份，是因为"什么算合法时间戳"只能有一个定义，两份迟早会漂。
from logcopilot.domain.log import COMPONENTS, LogLevel, coerce_enum, format_ts, require_utc

#: 目录结构的版本号，写进 `manifest.schema`。读到不认识的值**直接拒读**：
#: 让 P-B 拿着"猜"的解析器去读 v2 的 pack，比读不出来危险得多。
SCHEMA: Final[str] = "casepack/v1"

#: 采集器代码版本。**不进任何一个哈希**（P-A §6.4）。它变化的唯一用途，
#: 是当 `canonical_sha256` 变了、而 `input_fingerprint` 没变时，
#: 让人能看出"是代码改了"而不是"数据改了"。
PIPELINE_VERSION: Final[str] = "collect/0.2.0"

#: 脱敏规则闭集，四条都由第 7 步的 `collect/redact.py` 实现。
REDACT_RULES: Final[tuple[str, ...]] = ("host", "ip", "path", "token")

_SHA256_RE: Final[re.Pattern[str]] = re.compile(r"^sha256:[0-9a-f]{64}$")
_SHA1_RE: Final[re.Pattern[str]] = re.compile(r"^[0-9a-f]{40}$")
#: case_id 会变成目录名，也会出现在 URL 里（P-C 的 `/case/<id>`）：
#: 只允许小写字母开头的 3-64 位字母数字下划线，杜绝路径穿越与大小写歧义。
_CASE_ID_RE: Final[re.Pattern[str]] = re.compile(r"^[a-z][a-z0-9_]{2,63}$")


# ---------------------------------------------------------------- 小工具


def _reject_extra(data: Any, allowed: frozenset[str], where: str) -> dict[str, Any]:
    """拒绝未知字段。多出来的键说明有人改过 schema 而没升版本号。"""
    if not isinstance(data, dict):
        raise ValueError(f"{where} 必须是对象，拿到 {type(data).__name__}")
    extra = set(data) - allowed
    if extra:
        raise ValueError(f"{where} 出现未知字段 {sorted(extra)} —— schema 已漂移，拒读")
    return data


def _require_keys(data: dict[str, Any], names: tuple[str, ...], where: str) -> None:
    missing = [name for name in names if name not in data]
    if missing:
        raise ValueError(f"{where} 缺少必需字段 {missing}")


def _as_int(value: Any, where: str, *, minimum: int = 0) -> int:
    # bool 是 int 的子类，不排除的话 `True` 会被当成计数 1 蒙混过关。
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{where} 必须是整数，拿到 {value!r}")
    if value < minimum:
        raise ValueError(f"{where} 必须 >= {minimum}，拿到 {value}")
    return value


def _as_text(value: Any, where: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{where} 必须是非空字符串，拿到 {value!r}")
    return value


def _as_sha256(value: Any, where: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.match(value) is None:
        raise ValueError(f"{where} 必须形如 'sha256:<64 位小写十六进制>'，拿到 {value!r}")
    return value


def _as_sha1(value: Any, where: str) -> str:
    if not isinstance(value, str) or _SHA1_RE.match(value) is None:
        raise ValueError(f"{where} 必须是 40 位小写十六进制 sha1，拿到 {value!r}")
    return value


def _as_ts(value: Any, where: str) -> datetime:
    if isinstance(value, datetime):
        return require_utc(value, where)
    if not isinstance(value, str):
        raise ValueError(f"{where} 必须是 datetime 或 ISO8601 字符串，拿到 {value!r}")
    return require_utc(datetime.fromisoformat(value), where)


def _as_opt_ts(value: Any, where: str) -> datetime | None:
    return None if value is None else _as_ts(value, where)


def _ts_or_none(value: datetime | None) -> str | None:
    return None if value is None else format_ts(value)


def _case_id(value: Any, where: str = "case_id") -> str:
    if not isinstance(value, str) or _CASE_ID_RE.match(value) is None:
        raise ValueError(
            f"{where} 必须匹配 {_CASE_ID_RE.pattern}（小写字母开头、3-64 位），拿到 {value!r}"
        )
    return value


# ---------------------------------------------------------------- 闭集


class Phase(StrEnum):
    """窗口三段（P-A §3）。迁移例只有 fault，baseline 由 §6.2 的规则反推。"""

    BASELINE = "baseline"
    FAULT = "fault"
    RECOVERY = "recovery"


class Adapter(StrEnum):
    """A1 的取数方式。`wsl` 本期只有骨架（P-A §6.1）。"""

    DOCKER = "docker"
    FILE = "file"
    WSL = "wsl"


class CanonicalProfile(StrEnum):
    """canonical 层用哪套解析规则（P-A §6.2）。

    这两套不是"两种风格"，是两批数据的真实形态：新采的是 log4j 原生，
    旧 132 例行首多一个 `[component] ` 前缀且 level/logger 写法不同。
    用一套正则硬吃两批，旧数据会整批落进 failed，V2 直接作废。
    """

    HDFS_LOG4J = "hdfs_log4j"
    LAB = "lab"


class OriginKind(StrEnum):
    COLLECTED = "collected"
    MIGRATED = "migrated"


class RootCause(StrEnum):
    """根因闭集 = P-A 本期要采的 4 类（M1.5）。

    旧项目有 11 个根因码，但 132 例只覆盖这 4 个（卷 28 / 心跳 37 /
    安全模式 46 / 网络 21）。闭集收窄到一个"当前真的会产生数据"的范围，
    剩下的码等 P-C 的判定图要全量根因表时再扩——扩枚举比收枚举容易。
    """

    DN_VOLUME = "DN_VOLUME"
    DN_HEARTBEAT = "DN_HEARTBEAT"
    NN_SAFE_MODE = "NN_SAFE_MODE"
    NETWORK_CONNECTIVITY = "NETWORK_CONNECTIVITY"


class Severity(StrEnum):
    """故障严重度。P-A 的样例只出现过 `error`，另外三档是为 P-C 的排序留位。"""

    INFO = "info"
    WARN = "warn"
    ERROR = "error"
    CRITICAL = "critical"


# ---------------------------------------------------------------- manifest 组成


@dataclass(frozen=True, slots=True)
class RedactPolicy:
    """用了哪套脱敏规则。写进 manifest 以便"为什么哈希变了"有据可查。

    注意它**不进 input_fingerprint**（P-A §6.4）：脱敏策略变了不该让
    "原始输入"看起来变了。
    """

    version: str
    rules: tuple[str, ...] = REDACT_RULES

    def __post_init__(self) -> None:
        _as_text(self.version, "redact_policy.version")
        object.__setattr__(self, "rules", tuple(self.rules))
        if not self.rules:
            raise ValueError("redact_policy.rules 不能为空")
        unknown = [rule for rule in self.rules if rule not in REDACT_RULES]
        if unknown:
            raise ValueError(
                f"redact_policy.rules 出现未知规则 {unknown}，闭集={list(REDACT_RULES)}"
            )
        if len(set(self.rules)) != len(self.rules):
            raise ValueError(f"redact_policy.rules 有重复: {list(self.rules)}")

    def to_dict(self) -> dict[str, Any]:
        return {"version": self.version, "rules": list(self.rules)}

    @classmethod
    def from_dict(cls, data: Any) -> RedactPolicy:
        _reject_extra(data, frozenset({"version", "rules"}), "redact_policy")
        _require_keys(data, ("version",), "redact_policy")
        rules = data.get("rules", REDACT_RULES)
        if not isinstance(rules, (list, tuple)):
            raise ValueError(f"redact_policy.rules 必须是数组，拿到 {type(rules).__name__}")
        return cls(version=_as_text(data["version"], "redact_policy.version"), rules=tuple(rules))


@dataclass(frozen=True, slots=True)
class SourceRef:
    """一个采集源的出处。manifest.source[] 的一项。

    同一 component 可以有多条（dn1 的主日志 + hdfs-audit + GC + stdout），
    所以这里**不校验 component 唯一**。
    """

    adapter: Adapter
    ref: str
    component: str
    image_digest: str | None = None
    path: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "adapter", coerce_enum(Adapter, self.adapter, "source.adapter"))
        _as_text(self.ref, "source.ref")
        if self.component not in COMPONENTS:
            raise ValueError(
                f"source.component 非法: {self.component!r}，闭集={sorted(COMPONENTS)}"
            )
        if self.image_digest is not None:
            _as_sha256(self.image_digest, "source.image_digest")
        if self.path is not None:
            _as_text(self.path, "source.path")

    def to_dict(self) -> dict[str, Any]:
        return {
            "adapter": str(self.adapter),
            "ref": self.ref,
            "component": self.component,
            "image_digest": self.image_digest,
            "path": self.path,
        }

    @classmethod
    def from_dict(cls, data: Any) -> SourceRef:
        allowed = frozenset({"adapter", "ref", "component", "image_digest", "path"})
        _reject_extra(data, allowed, "source[]")
        _require_keys(data, ("adapter", "ref", "component"), "source[]")
        return cls(
            adapter=data["adapter"],
            ref=data["ref"],
            component=data["component"],
            image_digest=data.get("image_digest"),
            path=data.get("path"),
        )


@dataclass(frozen=True, slots=True)
class PhaseWindow:
    """某一阶段的起止时刻。"""

    phase: Phase
    since: datetime
    until: datetime

    def __post_init__(self) -> None:
        object.__setattr__(self, "phase", coerce_enum(Phase, self.phase, "phases.phase"))
        # 校验的同时把值写回：从 json 读进来的是字符串，不写回的话字段里
        # 会同时存在 "datetime" 和 "str" 两种形态，比较时会在别处炸。
        object.__setattr__(self, "since", _as_ts(self.since, f"{self.phase}.since"))
        object.__setattr__(self, "until", _as_ts(self.until, f"{self.phase}.until"))
        if self.until < self.since:
            raise ValueError(
                f"{self.phase} 段的窗口倒挂: since={format_ts(self.since)} > "
                f"until={format_ts(self.until)}"
            )

    def to_pair(self) -> list[str]:
        return [format_ts(self.since), format_ts(self.until)]


@dataclass(frozen=True, slots=True)
class Window:
    """采集窗口 + 三段标注（P-A §5）。

    `phases` 允许为空：旧 132 例没有 baseline 标注，`fault_injection` 也没有，
    只能用 §6.2 的规则从 `fault_start` 反推。空不等于"数据坏"，只是"没标注"。
    """

    since: datetime
    until: datetime
    phases: tuple[PhaseWindow, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "since", _as_ts(self.since, "window.since"))
        object.__setattr__(self, "until", _as_ts(self.until, "window.until"))
        if self.until < self.since:
            raise ValueError(
                f"窗口倒挂: since={format_ts(self.since)} > until={format_ts(self.until)}"
            )

        object.__setattr__(self, "phases", tuple(self.phases))
        names = [window.phase for window in self.phases]
        if len(set(names)) != len(names):
            raise ValueError(f"window.phases 同一个阶段出现了两次: {[str(n) for n in names]}")
        for window in self.phases:
            if window.since < self.since or window.until > self.until:
                raise ValueError(
                    f"{window.phase} 段越出采集窗口: "
                    f"[{format_ts(window.since)}, {format_ts(window.until)}] ⊄ "
                    f"[{format_ts(self.since)}, {format_ts(self.until)}]"
                )

    def phase(self, name: Phase) -> PhaseWindow | None:
        for window in self.phases:
            if window.phase is name:
                return window
        return None

    def to_dict(self) -> dict[str, Any]:
        return {
            "since": format_ts(self.since),
            "until": format_ts(self.until),
            "phases": (
                None
                if not self.phases
                else {str(window.phase): window.to_pair() for window in self.phases}
            ),
        }

    @classmethod
    def from_dict(cls, data: Any) -> Window:
        _reject_extra(data, frozenset({"since", "until", "phases"}), "window")
        _require_keys(data, ("since", "until"), "window")
        raw_phases = data.get("phases")
        phases: list[PhaseWindow] = []
        if raw_phases is not None:
            if not isinstance(raw_phases, dict):
                raise ValueError(
                    f"window.phases 必须是对象或 null，拿到 {type(raw_phases).__name__}"
                )
            for name, pair in raw_phases.items():
                if not isinstance(pair, (list, tuple)) or len(pair) != 2:
                    raise ValueError(f"window.phases.{name} 必须是 [since, until] 两元素数组")
                phases.append(PhaseWindow(phase=name, since=pair[0], until=pair[1]))
        return cls(since=data["since"], until=data["until"], phases=tuple(phases))


@dataclass(frozen=True, slots=True)
class Counters:
    """采集计数，也是 pack-lint 对账的依据（P-A §6.3 / V1）。

    三条等式必须成立，否则说明"统计口径"和"实际写出的行"对不上：
      ``lines == sum(by_level.values())``      每一行都有 level
      ``lines == sum(by_component.values())``  每一行都有 component
      ``parse_failed <= lines <= raw_lines``   续行归并只能让行数变少
    """

    raw_lines: int
    lines: int
    by_level: dict[str, int]
    by_component: dict[str, int]
    parse_failed: int = 0
    redacted: dict[str, int] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _as_int(self.raw_lines, "counters.raw_lines")
        _as_int(self.lines, "counters.lines")
        _as_int(self.parse_failed, "counters.parse_failed")

        if self.lines > self.raw_lines:
            raise ValueError(
                f"counters.lines({self.lines}) 不能大于 raw_lines({self.raw_lines}) "
                "—— 续行归并只会让记录数变少，反过来一定是漏记了 raw_lines"
            )
        if self.parse_failed > self.lines:
            raise ValueError(
                f"counters.parse_failed({self.parse_failed}) 不能大于 lines({self.lines})"
            )

        object.__setattr__(self, "by_level", dict(self.by_level))
        object.__setattr__(self, "by_component", dict(self.by_component))
        object.__setattr__(self, "redacted", dict(self.redacted))

        for level, count in self.by_level.items():
            coerce_enum(LogLevel, level, f"counters.by_level.{level}")
            _as_int(count, f"counters.by_level.{level}")
        if sum(self.by_level.values()) != self.lines:
            raise ValueError(
                f"counters 对账失败: sum(by_level)={sum(self.by_level.values())} "
                f"!= lines={self.lines}"
            )

        for component, count in self.by_component.items():
            if component not in COMPONENTS:
                raise ValueError(
                    f"counters.by_component 键非法: {component!r}，闭集={sorted(COMPONENTS)}"
                )
            _as_int(count, f"counters.by_component.{component}")
        if sum(self.by_component.values()) != self.lines:
            raise ValueError(
                f"counters 对账失败: sum(by_component)={sum(self.by_component.values())} "
                f"!= lines={self.lines}"
            )

        for rule, count in self.redacted.items():
            if rule not in REDACT_RULES:
                raise ValueError(f"counters.redacted 键非法: {rule!r}，闭集={list(REDACT_RULES)}")
            _as_int(count, f"counters.redacted.{rule}")

    def to_dict(self) -> dict[str, Any]:
        return {
            "raw_lines": self.raw_lines,
            "lines": self.lines,
            "by_level": dict(self.by_level),
            "by_component": dict(self.by_component),
            "parse_failed": self.parse_failed,
            "redacted": dict(self.redacted),
        }

    @classmethod
    def from_dict(cls, data: Any) -> Counters:
        allowed = frozenset(
            {"raw_lines", "lines", "by_level", "by_component", "parse_failed", "redacted"}
        )
        _reject_extra(data, allowed, "counters")
        _require_keys(data, ("raw_lines", "lines", "by_level", "by_component"), "counters")
        return cls(
            raw_lines=data["raw_lines"],
            lines=data["lines"],
            by_level=data["by_level"],
            by_component=data["by_component"],
            parse_failed=data.get("parse_failed", 0),
            redacted=data.get("redacted", {}),
        )


@dataclass(frozen=True, slots=True)
class Hashes:
    """两个哈希，含义不同，**不要混用**（P-A §6.4）：

    ``input_fingerprint``  脱敏前的原始字节指纹。只有"源变了"它才变；
                           代码重构、换 adapter、改脱敏策略都不该动它。
    ``canonical_sha256``   归一+脱敏后的 `logs.jsonl` 投影（log.py 的 6 字段）。
                           代码或策略一变它就变 —— 这正是它能当 V4 判据的原因。

    这里只存值，不负责计算（第 4 步 store/casepack.py 算），也不包含
    pipeline_version / redact_policy / canonical_profile 任何元数据。
    """

    input_fingerprint: str
    canonical_sha256: str

    def __post_init__(self) -> None:
        _as_sha256(self.input_fingerprint, "hashes.input_fingerprint")
        _as_sha256(self.canonical_sha256, "hashes.canonical_sha256")

    def to_dict(self) -> dict[str, Any]:
        return {
            "input_fingerprint": self.input_fingerprint,
            "canonical_sha256": self.canonical_sha256,
        }

    @classmethod
    def from_dict(cls, data: Any) -> Hashes:
        _reject_extra(data, frozenset({"input_fingerprint", "canonical_sha256"}), "hashes")
        _require_keys(data, ("input_fingerprint", "canonical_sha256"), "hashes")
        return cls(
            input_fingerprint=data["input_fingerprint"],
            canonical_sha256=data["canonical_sha256"],
        )


@dataclass(frozen=True, slots=True)
class AdapterState:
    """一个源的读取进度（P-A §6.1）。续采靠它做到"不重不漏"。

    ``offset``    已消费到的字节偏移，恢复时从 offset+1 读
    ``line_no``   这条流**最后贡献的全局编号** —— 也就是 `id` 里的那个号。
                  注意它由 `collect/adapters/base.ComponentReader` 统一发号，
                  适配器上报的物理行号会被改写掉：一个组件有多份文件
                  （主日志 / GC / audit）时，编号必须跨文件连续，否则撞号
    ``inode``     文件被轮转/重建就变，变了就必须从头采（单靠 offset 会错位）
    ``tail_line_sha1``  最后一行原文的 sha1，恢复前先 tail 校验，防"文件被改写过"
    """

    offset: int
    line_no: int
    inode: int | None = None
    tail_line_sha1: str | None = None

    def __post_init__(self) -> None:
        _as_int(self.offset, "checkpoint.offset")
        _as_int(self.line_no, "checkpoint.line_no")
        if self.inode is not None:
            _as_int(self.inode, "checkpoint.inode")
        if self.tail_line_sha1 is not None:
            _as_sha1(self.tail_line_sha1, "checkpoint.tail_line_sha1")

    def to_dict(self) -> dict[str, Any]:
        return {
            "inode": self.inode,
            "offset": self.offset,
            "line_no": self.line_no,
            "tail_line_sha1": self.tail_line_sha1,
        }

    @classmethod
    def from_dict(cls, data: Any) -> AdapterState:
        allowed = frozenset({"inode", "offset", "line_no", "tail_line_sha1"})
        _reject_extra(data, allowed, "checkpoint.adapter_state[]")
        _require_keys(data, ("offset", "line_no"), "checkpoint.adapter_state[]")
        return cls(
            offset=data["offset"],
            line_no=data["line_no"],
            inode=data.get("inode"),
            tail_line_sha1=data.get("tail_line_sha1"),
        )


@dataclass(frozen=True, slots=True)
class Checkpoint:
    """所有源的进度快照。键是源的标识（如 `dn1` / `dn1:audit`）。

    允许为空：一个新 pack 还没跑过采集，或者迁移例本来就没有 checkpoint。
    """

    adapter_state: dict[str, AdapterState] = field(default_factory=dict)
    last_ts: datetime | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "adapter_state", dict(self.adapter_state))
        for key, state in self.adapter_state.items():
            _as_text(key, "checkpoint.adapter_state 的键")
            if not isinstance(state, AdapterState):
                raise ValueError(
                    f"checkpoint.adapter_state[{key!r}] 必须是 AdapterState，"
                    f"拿到 {type(state).__name__}"
                )
        if self.last_ts is not None:
            object.__setattr__(self, "last_ts", _as_ts(self.last_ts, "checkpoint.last_ts"))

    def to_dict(self) -> dict[str, Any]:
        return {
            "adapter_state": {key: state.to_dict() for key, state in self.adapter_state.items()},
            "last_ts": _ts_or_none(self.last_ts),
        }

    @classmethod
    def from_dict(cls, data: Any) -> Checkpoint:
        _reject_extra(data, frozenset({"adapter_state", "last_ts"}), "checkpoint")
        raw_state = data.get("adapter_state", {})
        if not isinstance(raw_state, dict):
            raise ValueError("checkpoint.adapter_state 必须是对象")
        return cls(
            adapter_state={key: AdapterState.from_dict(value) for key, value in raw_state.items()},
            last_ts=_as_opt_ts(data.get("last_ts"), "checkpoint.last_ts"),
        )


@dataclass(frozen=True, slots=True)
class Origin:
    """这份 pack 从哪来：新采的，还是从旧 132 例迁过来的（M1.4）。

    迁移例必须带 `incident_sha256`：P-B 之后要拿它跟旧实验台的
    `incident.json` 对账，证明"迁过来的是同一份病例"。
    """

    kind: OriginKind
    src: str | None = None
    incident_sha256: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "kind", coerce_enum(OriginKind, self.kind, "origin.kind"))
        if self.kind is OriginKind.COLLECTED:
            if self.incident_sha256 is not None:
                raise ValueError("origin.kind=collected 时不应该有 incident_sha256（那是迁移例的）")
            if self.src is not None:
                _as_text(self.src, "origin.src")
            return
        _as_text(self.src, "origin.src")
        if self.incident_sha256 is None:
            raise ValueError("origin.kind=migrated 时必须带 incident_sha256")
        _as_sha256(self.incident_sha256, "origin.incident_sha256")

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": str(self.kind),
            "src": self.src,
            "incident_sha256": self.incident_sha256,
        }

    @classmethod
    def from_dict(cls, data: Any) -> Origin:
        _reject_extra(data, frozenset({"kind", "src", "incident_sha256"}), "origin")
        _require_keys(data, ("kind",), "origin")
        return cls(
            kind=data["kind"],
            src=data.get("src"),
            incident_sha256=data.get("incident_sha256"),
        )


@dataclass(frozen=True, slots=True)
class CaseManifest:
    """`manifest.json` 的全部内容。**最后写**，写了才算这个 pack 完成（P-A §5）。"""

    case_id: str
    created_at: datetime
    canonical_profile: CanonicalProfile
    window: Window
    origin: Origin
    sources: tuple[SourceRef, ...]
    counters: Counters
    hashes: Hashes
    checkpoint: Checkpoint
    redact_policy: RedactPolicy
    schema: str = SCHEMA
    pipeline_version: str = PIPELINE_VERSION
    tz: str = "UTC"
    config_overrides: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        # schema 放在最前面校验：不认识的版本，后面所有字段的解释都可能是错的。
        if self.schema != SCHEMA:
            raise ValueError(
                f"manifest.schema={self.schema!r} 与当前版本 {SCHEMA!r} 不匹配 —— "
                "拒读。升级读取端而不是猜着读"
            )
        _case_id(self.case_id)
        object.__setattr__(self, "created_at", _as_ts(self.created_at, "created_at"))
        object.__setattr__(
            self,
            "canonical_profile",
            coerce_enum(CanonicalProfile, self.canonical_profile, "canonical_profile"),
        )
        _as_text(self.pipeline_version, "pipeline_version")

        # O3 已拍板 tz=UTC：留一个字段是为了让"以后要不要支持本地时区"这件事
        # 在 manifest 里有痕迹，而不是散在代码里。
        if self.tz != "UTC":
            raise ValueError(f"tz 目前只支持 'UTC'（O3 已拍板），拿到 {self.tz!r}")

        object.__setattr__(self, "sources", tuple(self.sources))
        if not self.sources:
            raise ValueError("source 不能为空 —— 没有出处的 pack 无法复现，也无法解释哈希")
        for source in self.sources:
            if not isinstance(source, SourceRef):
                raise ValueError(f"source[] 必须是 SourceRef，拿到 {type(source).__name__}")

        object.__setattr__(self, "config_overrides", dict(self.config_overrides))
        for key, value in self.config_overrides.items():
            _as_text(key, "config_overrides 的键")
            _as_text(value, "config_overrides 的值")

        if self.checkpoint.last_ts is not None and self.checkpoint.last_ts > self.window.until:
            raise ValueError(
                f"checkpoint.last_ts({format_ts(self.checkpoint.last_ts)}) 晚于 "
                f"window.until({format_ts(self.window.until)}) —— 采到了窗口外的数据"
            )

    # ---------- 序列化 ----------

    def to_dict(self) -> dict[str, Any]:
        """字段顺序照 P-A §5 的表格，方便和文档逐行对照。"""
        return {
            "schema": self.schema,
            "case_id": self.case_id,
            "created_at": format_ts(self.created_at),
            "pipeline_version": self.pipeline_version,
            "canonical_profile": str(self.canonical_profile),
            "redact_policy": self.redact_policy.to_dict(),
            "tz": self.tz,
            "config_overrides": dict(self.config_overrides),
            "source": [source.to_dict() for source in self.sources],
            "window": self.window.to_dict(),
            "origin": self.origin.to_dict(),
            "counters": self.counters.to_dict(),
            "hashes": self.hashes.to_dict(),
            "checkpoint": self.checkpoint.to_dict(),
        }

    def to_json(self) -> str:
        """写盘用。缩进 2 是为了有人 `git diff` 时能一眼看出改了哪个字段。"""
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2) + "\n"

    @classmethod
    def from_dict(cls, data: Any) -> CaseManifest:
        allowed = frozenset(
            {
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
            }
        )
        _reject_extra(data, allowed, "manifest")
        _require_keys(
            data,
            (
                "schema",
                "case_id",
                "created_at",
                "canonical_profile",
                "source",
                "window",
                "origin",
                "counters",
                "hashes",
                "checkpoint",
                "redact_policy",
            ),
            "manifest",
        )
        sources = data["source"]
        if not isinstance(sources, (list, tuple)):
            raise ValueError("manifest.source 必须是数组")
        overrides = data.get("config_overrides", {})
        if not isinstance(overrides, dict):
            raise ValueError("manifest.config_overrides 必须是对象")
        return cls(
            schema=data["schema"],
            case_id=data["case_id"],
            created_at=_as_ts(data["created_at"], "created_at"),
            pipeline_version=data.get("pipeline_version", PIPELINE_VERSION),
            canonical_profile=data["canonical_profile"],
            redact_policy=RedactPolicy.from_dict(data["redact_policy"]),
            tz=data.get("tz", "UTC"),
            config_overrides=overrides,
            sources=tuple(SourceRef.from_dict(item) for item in sources),
            window=Window.from_dict(data["window"]),
            origin=Origin.from_dict(data["origin"]),
            counters=Counters.from_dict(data["counters"]),
            hashes=Hashes.from_dict(data["hashes"]),
            checkpoint=Checkpoint.from_dict(data["checkpoint"]),
        )

    @classmethod
    def from_json(cls, text: str) -> CaseManifest:
        try:
            data = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError(f"manifest.json 不是合法 json: {exc}") from None
        return cls.from_dict(data)


# ---------------------------------------------------------------- labels


@dataclass(frozen=True, slots=True)
class FaultInjection:
    """一次故障注入的记录，由 `collect/labels.py` 自动写（第 9 步）。

    这是 V7「注入可逆」的唯一证据链：`started`/`ended` 圈住故障窗口，
    `restored` + `restored_ok` 证明环境回到过干净状态。缺一项，那次采集
    就只能当"看了个热闹"，不能进评测集。
    """

    script: str
    started: datetime
    ended: datetime | None = None
    restored: datetime | None = None
    restored_ok: bool | None = None

    def __post_init__(self) -> None:
        _as_text(self.script, "fault_injection.script")
        object.__setattr__(self, "started", _as_ts(self.started, "fault_injection.started"))
        if self.ended is not None:
            object.__setattr__(self, "ended", _as_ts(self.ended, "fault_injection.ended"))
            if self.ended < self.started:
                raise ValueError(
                    f"fault_injection 时间倒挂: ended({format_ts(self.ended)}) < "
                    f"started({format_ts(self.started)})"
                )
        if self.restored is not None:
            object.__setattr__(self, "restored", _as_ts(self.restored, "fault_injection.restored"))
            boundary = self.ended if self.ended is not None else self.started
            if self.restored < boundary:
                raise ValueError(
                    f"fault_injection 时间倒挂: restored({format_ts(self.restored)}) 早于 "
                    f"故障结束({format_ts(boundary)}) —— 恢复动作不可能发生在故障结束之前"
                )
        if self.restored_ok is not None and not isinstance(self.restored_ok, bool):
            raise ValueError(
                f"fault_injection.restored_ok 必须是 bool 或 null，拿到 {self.restored_ok!r}"
            )
        if self.restored_ok is True and self.restored is None:
            raise ValueError(
                "fault_injection.restored_ok=True 但没记 restored 时刻 —— "
                "声称恢复成功却没有时间戳，V7 无法复核"
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "script": self.script,
            "started": format_ts(self.started),
            "ended": _ts_or_none(self.ended),
            "restored": _ts_or_none(self.restored),
            "restored_ok": self.restored_ok,
        }

    @classmethod
    def from_dict(cls, data: Any) -> FaultInjection:
        allowed = frozenset({"script", "started", "ended", "restored", "restored_ok"})
        _reject_extra(data, allowed, "fault_injection")
        _require_keys(data, ("script", "started"), "fault_injection")
        return cls(
            script=data["script"],
            started=_as_ts(data["started"], "fault_injection.started"),
            ended=_as_opt_ts(data.get("ended"), "fault_injection.ended"),
            restored=_as_opt_ts(data.get("restored"), "fault_injection.restored"),
            restored_ok=data.get("restored_ok"),
        )


@dataclass(frozen=True, slots=True)
class Expect:
    """人先填好的期望值，V5/V6 拿它当门禁（P-A §5 / §7）。

    这是"采集是否采到了故障"的唯一判据：没有它，一个 3 行的 safe_mode 例子
    和一个 239 行的 network 例子没法用同一套标准验收。

    ## 门槛可以是 0，但不能全是 0

    `min_error_lines` / `min_warn_lines` 是**实测下限**，允许为 0 —— HDFS 的
    多数故障只有 WARN（心跳、网络、安全模式三族在旧语料里 ERROR 皆 0）。
    但两者之和必须 ≥ 1：一条"ERROR>=0 且 WARN>=0"的 expect 什么都没要求，
    等于没标注，故障没被采到也照样过。

    `error_components` 的名字里虽然写着 error，语义早已放宽成**"哪个组件会
    出现非 INFO（WARN 或 ERROR）记录"**。字段名沿用是为了不动 schema ——
    它进 labels 契约，改名会让已迁的 132 个 pack 一起要动。
    """

    min_error_lines: int = 0
    min_warn_lines: int = 0
    error_components: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _as_int(self.min_error_lines, "expect.min_error_lines")
        _as_int(self.min_warn_lines, "expect.min_warn_lines")
        object.__setattr__(self, "error_components", tuple(self.error_components))
        for component in self.error_components:
            if component not in COMPONENTS:
                raise ValueError(
                    f"expect.error_components 非法: {component!r}，闭集={sorted(COMPONENTS)}"
                )

    def to_dict(self) -> dict[str, Any]:
        return {
            "min_error_lines": self.min_error_lines,
            "min_warn_lines": self.min_warn_lines,
            "error_components": list(self.error_components),
        }

    @classmethod
    def from_dict(cls, data: Any) -> Expect:
        allowed = frozenset({"min_error_lines", "min_warn_lines", "error_components"})
        _reject_extra(data, allowed, "expect")
        return cls(
            min_error_lines=data.get("min_error_lines", 0),
            min_warn_lines=data.get("min_warn_lines", 0),
            error_components=tuple(data.get("error_components", ())),
        )


@dataclass(frozen=True, slots=True)
class Labels:
    """`labels.json` —— 一个病例的标注。

    谁写哪几个字段（P-A §5 定死，别再混）：
      `fault_injection`  采集器自动写，人不碰
      `root_cause` / `severity` / `expect` / `reviewer` / `notes`  人写
      `phase_map`        本期恒 None，阶段窗口的唯一真相在 manifest.window.phases

    `root_cause` 允许为 None：`base_healthy_01` 是正常例，本来就没有根因。
    """

    case_id: str
    root_cause: RootCause | None = None
    severity: Severity | None = None
    phase_map: dict[str, str] | None = None
    fault_injection: FaultInjection | None = None
    expect: Expect | None = None
    reviewer: str | None = None
    notes: str | None = None

    def __post_init__(self) -> None:
        _case_id(self.case_id)
        if self.root_cause is not None:
            object.__setattr__(
                self, "root_cause", coerce_enum(RootCause, self.root_cause, "root_cause")
            )
        if self.severity is not None:
            object.__setattr__(self, "severity", coerce_enum(Severity, self.severity, "severity"))
        if self.phase_map is not None:
            object.__setattr__(self, "phase_map", dict(self.phase_map))
            phases = {str(phase) for phase in Phase}
            for name, target in self.phase_map.items():
                if name not in phases:
                    raise ValueError(f"phase_map 键非法: {name!r}，闭集={sorted(phases)}")
                _as_text(target, f"phase_map.{name}")
        if self.reviewer is not None:
            _as_text(self.reviewer, "reviewer")
        if self.notes is not None:
            _as_text(self.notes, "notes")

    def to_dict(self) -> dict[str, Any]:
        return {
            "case_id": self.case_id,
            "root_cause": None if self.root_cause is None else str(self.root_cause),
            "severity": None if self.severity is None else str(self.severity),
            "phase_map": None if self.phase_map is None else dict(self.phase_map),
            "fault_injection": (
                None if self.fault_injection is None else self.fault_injection.to_dict()
            ),
            "expect": None if self.expect is None else self.expect.to_dict(),
            "reviewer": self.reviewer,
            "notes": self.notes,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2) + "\n"

    @classmethod
    def from_dict(cls, data: Any) -> Labels:
        allowed = frozenset(
            {
                "case_id",
                "root_cause",
                "severity",
                "phase_map",
                "fault_injection",
                "expect",
                "reviewer",
                "notes",
            }
        )
        _reject_extra(data, allowed, "labels")
        _require_keys(data, ("case_id",), "labels")
        raw_injection = data.get("fault_injection")
        raw_expect = data.get("expect")
        return cls(
            case_id=data["case_id"],
            root_cause=data.get("root_cause"),
            severity=data.get("severity"),
            phase_map=data.get("phase_map"),
            fault_injection=(
                None if raw_injection is None else FaultInjection.from_dict(raw_injection)
            ),
            expect=None if raw_expect is None else Expect.from_dict(raw_expect),
            reviewer=data.get("reviewer"),
            notes=data.get("notes"),
        )

    @classmethod
    def from_json(cls, text: str) -> Labels:
        try:
            data = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError(f"labels.json 不是合法 json: {exc}") from None
        return cls.from_dict(data)
