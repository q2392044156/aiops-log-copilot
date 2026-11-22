"""CasePack v1 的日志行类型 —— `logs.jsonl` 的一行。
用于表示日志行的结构，以及校验字段的取值范围。

规格来源 `P-A_COLLECT_PLAN.md`：
  §5     字段表，以及 id / ts / raw / component 的取值规则
  §6.3   排序键 (ts, id)；canonical_sha256 只覆盖 6 个字段的投影
  A-R2   时间戳必须 UTC —— 防"某处把 UTC 当本地时区"的差 8 小时事故

三个设计取舍（后面每个阶段都会被继承，值得记牢）：
  1. `frozen`：下游 P-B/P-C 全部只读。哈希是按字段投影算的，只要有一条路径
     能改字段，哈希就会在没有提示的地方发生变化。
  2. `slots`：finalize 归并排序时内存里可能同时有几万条，省下 __dict__。
  3. 校验全部放在 `__post_init__`：错的记录在这里就炸，而不是等到 pack-lint
     事后扫描才发现（那时已经写了几千行 jsonl）。
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from typing import Any, Final, TypeVar

# 常量定义
#: component 闭集。容器名（namenode / datanode1：`aiops-lab-dn1` 之类）由
#: canonical 层统一映射到这四个短名，domain 层只认短名 —— 这样 counters 的
#: by_component 才能跨数据源对账（P-A §5）。
COMPONENTS: Final[frozenset[str]] = frozenset({"nn", "dn1", "dn2", "dn3"})

#: logs.jsonl 的字段顺序。序列化按此顺序写出；顺序固定才能让
#: canonical_sha256 跨进程、跨机器一致。
FIELDS: Final[tuple[str, ...]] = (
    "id",
    "ts",
    "level",
    "component",
    "logger",
    "message",
    "source",
    "raw",
    "parse_status",
    "template_id",
)

#: 进 canonical_sha256 的 6 个字段（P-A §6.3 的投影）。必须排除：
#:   source        —— file 与 docker 两条路径下字符串不同，V4 要求两者哈希相等
#:   raw           —— 原文副本，不该影响"内容等价"
#:   parse_status  —— 解析质量，不是日志内容
#:   template_id   —— P-B 才回填
CANONICAL_FIELDS: Final[tuple[str, ...]] = (
    "id",
    "ts",
    "level",
    "component",
    "logger",
    "message",
)

#: `dn2:00123` —— 组件短名 + 源文件绝对物理行号（5 位起）。
#: 行号必须来自源文件物理行号（由 offset 反算），不能用"读入顺序"：
#: 否则续采后重号，V3「续采不重不漏」永远过不了。
_ID_RE: Final[re.Pattern[str]] = re.compile(r"^(nn|dn[1-3]):(\d{5,})$")


class LogLevel(StrEnum):
    """日志级别闭集。注意 Hadoop 打的是 ``WARN`` 而不是 ``WARNING``。"""

    TRACE = "TRACE"
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARN = "WARN"
    ERROR = "ERROR"
    FATAL = "FATAL"


class ParseStatus(StrEnum):
    """一行日志的解析质量。取小写，因为它出现在 jsonl 里（P-A §5 示例）。

    ok      —— 完整解析出 ts / level / logger / message
    partial —— 续行，或时间戳识别成功但 body 不完整；ts/component/source 继承上一行
    failed  —— 完全认不出来；不丢弃，message 可为空，raw 必留
    """

    OK = "ok"
    PARTIAL = "partial"
    FAILED = "failed"


_E = TypeVar("_E", bound=StrEnum)


def coerce_enum(enum_cls: type[_E], value: str | _E, field_name: str) -> _E:
    """把字符串收进枚举闭集；非法值抛出带闭集的错误信息。

    `LogRecord(level="WARNING")` 会在构造时失败，而不是等到写进 CasePack
    之后在 P-B 建索引时才炸。

    公开（不带下划线）是因为 `domain/case.py` 也要用同一套规则 —— 闭集校验
    只能有一份实现，两份迟早会不一致。
    """
    if isinstance(value, enum_cls):
        return value
    try:
        return enum_cls(value)
    except ValueError:
        allowed = [member.value for member in enum_cls]
        raise ValueError(f"{field_name} 非法: {value!r}，闭集={allowed}") from None


def format_ts(ts: datetime) -> str:
    """统一输出 ISO 8601 + `+00:00`（不写 `Z`，P-A §5）。

    微秒恰好是整毫秒时用毫秒精度（正常日志都是这种），否则保留微秒，
    保证 `from_json_line(to_json_line(r)) == r` 的往返不丢精度。
    """
    timespec = "milliseconds" if ts.microsecond % 1000 == 0 else "microseconds"
    return ts.isoformat(timespec=timespec)


def require_utc(value: datetime, field_name: str = "ts") -> datetime:
    """校验并原样返回一个 UTC 时间；naive 或带其它偏移都抛 ``ValueError``。

    抽成函数是因为 `manifest.created_at`、`window.since/until`、
    `labels.fault_injection.started` 全都要过同一道关。差 8 小时（A-R2）这种
    事故只要有一条路径漏了，整条时间轴就废了。
    """
    if not isinstance(value, datetime):
        raise ValueError(f"{field_name} 必须是 datetime，拿到 {type(value).__name__}")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(
            f"{field_name} 必须带时区 —— naive 时间等于把 UTC 当本地时间，"
            "整条时间轴会差 8 小时（A-R2）"
        )
    if value.utcoffset() != timedelta(0):
        raise ValueError(
            f"{field_name} 必须是 UTC，拿到 offset={value.utcoffset()}"
            " —— 带偏移的时间戳说明上游没做归一，先修 canonical 层"
        )
    return value


@dataclass(frozen=True, slots=True)
class LogRecord:
    """一行已归一、已脱敏的日志。

    构造时会校验下列不变式，违反任何一条都抛 ``ValueError``：

    ======================  ==================================================
    字段                    规则
    ======================  ==================================================
    ``level``               闭集 ``TRACE/DEBUG/INFO/WARN/ERROR/FATAL``
    ``parse_status``        闭集 ``ok/partial/failed``
    ``component``           闭集 ``nn/dn1/dn2/dn3``
    ``ts``                  必须 tz-aware，且 offset 必须为 0（UTC）
    ``id``                  形如 ``dn2:00123``，且前缀必须等于 ``component``
    ``raw``                 ``ok`` 时必须为 ``None``；非 ``ok`` 时必须非空
    ``message`` / ``raw``   至少一个非空
    ======================  ==================================================
    """

    id: str
    ts: datetime
    level: LogLevel
    component: str
    logger: str | None
    message: str
    source: str
    raw: str | None
    parse_status: ParseStatus
    template_id: str | None = None

    def __post_init__(self) -> None:
        # frozen 的 dataclass 不能直接赋值，归一化结果用 object.__setattr__ 写回。
        object.__setattr__(self, "level", coerce_enum(LogLevel, self.level, "level"))
        object.__setattr__(
            self, "parse_status", coerce_enum(ParseStatus, self.parse_status, "parse_status")
        )

        if self.component not in COMPONENTS:
            raise ValueError(f"component 非法: {self.component!r}，闭集={sorted(COMPONENTS)}")

        match = _ID_RE.match(self.id)
        if match is None:
            raise ValueError(
                f"id 必须形如 'dn2:00123'（组件短名 + 5 位以上物理行号），拿到 {self.id!r}"
            )
        if match.group(1) != self.component:
            raise ValueError(
                f"id 前缀与 component 不一致: id={self.id!r} component={self.component!r}"
                " —— 两者存了同一件事，必须在构造时就锁死"
            )

        # 时间戳是整条 P-A 里最容易错的地方：naive、本地时区、`Z` 与 `+00:00`
        # 混用，三种都会让"跨源按 ts 排序"安安静静地排错。统一交给 require_utc。
        require_utc(self.ts)

        if not self.source:
            raise ValueError("source 不能为空（如 'file:nn' / 'docker:dn2'）")
        if self.logger is not None and not self.logger:
            raise ValueError("logger 要么是 None，要么是非空字符串")
        if self.template_id is not None and not self.template_id:
            raise ValueError("template_id 要么是 None，要么是非空字符串")

        if self.parse_status is ParseStatus.OK:
            if self.raw is not None:
                raise ValueError(
                    "parse_status=ok 时 raw 必须为 None —— 原文在 anchors/ 指向的 "
                    "runtime 日志里留着，每条 ok 行再存一份会让 CasePack 体积翻倍"
                )
        elif not self.raw:
            raise ValueError(
                f"parse_status={self.parse_status} 时必须保留 raw 原文 —— 解析失败的行不丢弃"
            )

        if not self.message and not self.raw:
            raise ValueError("message 与 raw 不能同时为空")

    # ---------- 构造辅助 ----------

    @staticmethod
    def build_id(component: str, line_no: int) -> str:
        """按 P-A §5 的规则生成 id：``component:line_no``，行号左补零到 5 位。"""
        return f"{component}:{line_no:05d}"

    # ---------- 排序与哈希 ----------

    def sort_key(self) -> tuple[datetime, str]:
        """finalize 的归并排序键 ``(ts, id)``（P-A §6.3）。

        不用 source：换 adapter 后 source 字符串必然变，排序就跟着变，V4 直接挂。
        """
        return (self.ts, self.id)

    def canonical_payload(self) -> dict[str, Any]:
        """canonical_sha256 的输入：只有 6 个字段。

        第 4 步 `store/casepack.py` 会把它按行 json 化后 sha256；
        现在先把这个"哪些字段算内容"的决定固化下来。
        """
        data = self.to_dict()
        return {key: data[key] for key in CANONICAL_FIELDS}

    # ---------- 序列化 ----------

    def to_dict(self) -> dict[str, Any]:
        """按 `FIELDS` 的顺序转成可 json 化的 dict（ts 转字符串）。"""
        return {
            "id": self.id,
            "ts": format_ts(self.ts),
            "level": str(self.level),
            "component": self.component,
            "logger": self.logger,
            "message": self.message,
            "source": self.source,
            "raw": self.raw,
            "parse_status": str(self.parse_status),
            "template_id": self.template_id,
        }

    def to_json_line(self) -> str:
        """一行 jsonl。紧凑分隔符 + 不转义中文，保证逐字节可复现。"""
        return json.dumps(self.to_dict(), ensure_ascii=False, separators=(",", ":"))

    @classmethod
    def from_json_line(cls, line: str) -> LogRecord:
        """读回一行 jsonl。

        字段必须**正好**是那 10 个：少一个说明 pack 残缺，多一个说明 schema
        已漂移 —— 两种情况都当场拒绝，不让坏数据流到 P-B。
        """
        data = json.loads(line)
        if not isinstance(data, dict):
            raise ValueError(f"jsonl 每行必须是对象，拿到 {type(data).__name__}")

        missing = set(FIELDS) - set(data)
        extra = set(data) - set(FIELDS)
        if missing or extra:
            raise ValueError(f"字段不匹配：缺 {sorted(missing)}，多 {sorted(extra)}")

        data["ts"] = datetime.fromisoformat(data["ts"])
        return cls(**data)
