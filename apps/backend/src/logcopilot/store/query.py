# 顺序 8b · 查询：把「关键词」翻成一次检索，把命中翻成一份模板报告（`make ask`）
"""P-A §8.2 的 `scripts/ask.py`：CasePack → 关键词检索 → 模板报告。

**为什么 P-A 需要一个"查"的入口。**
采了半天数据，如果验证方式只有"打开 logs.jsonl 用眼睛看"，那这套采集就没法
被人使用，只能被作者使用。`ask` 是这条流水线的第一次"人机交界"：人打几个词，
它告诉你"这个案例里有没有这回事、在哪、原文长什么样"。

**为什么不接大模型。**
P-A 是采集阶段，`domain/` 与 `store/` 里没有一个字依赖 LLM（铁律 L4 的延伸：
`collect/` 与 `store/` 不 import 任何推理侧代码）。这里做的是**确定性检索**：
同样的输入永远同样的输出，可以进测试、可以进 CI。真正的"自然语言问答"是 P-C
的事，它会把这里当作一个工具（`search_logs`）来调。

**为什么不自己维护索引。**
一个 pack 最大也就两万条记录、几 MB，全量扫一遍是毫秒级。为它建倒排索引属于
过度设计，而且会引入"索引与 pack 不同步"这一整类 bug —— 采样本就要求"盘上
那份是唯一真相"，查询必须直接读它。
"""

from __future__ import annotations

import hashlib
import json
import re
from collections import Counter
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from logcopilot.domain.case import CaseManifest
from logcopilot.domain.log import LogLevel, LogRecord
from logcopilot.store.casepack import (
    AnchorRef,
    CasePackPaths,
    pack_paths,
    read_anchors,
    read_logs,
    read_manifest,
)

__all__ = [
    "Hit",
    "Query",
    "QueryError",
    "build_report",
    "resolve_anchor",
    "search",
]


class QueryError(ValueError):
    """查询参数本身不合法（时间格式、级别名、正则）。"""


# ---------------------------------------------------------------- 查询条件


@dataclass(frozen=True, slots=True)
class Query:
    """一次检索的全部条件。

    全部条件之间是 **AND**：`--level ERROR --grep Volume` 的意思是"既要是 ERROR，
    又要在 message/logger/raw 里出现 Volume"。多个关键词之间默认 **OR**（人打
    "Volume lock" 通常想找"提到其中任意一个"的行），`require_all=True` 改成 AND。
    """

    keywords: tuple[str, ...] = ()
    levels: frozenset[LogLevel] = frozenset()
    components: frozenset[str] = frozenset()
    since: datetime | None = None
    until: datetime | None = None
    pattern: re.Pattern[str] | None = None
    require_all: bool = False
    limit: int = 20

    def __post_init__(self) -> None:
        if self.limit <= 0:
            raise QueryError(f"limit 必须大于 0，拿到 {self.limit}")
        # 级别允许传字符串（`Query(levels={"ERROR"})` 是测试和脚本最自然的写法），
        # 在这里统一归一成枚举 —— 否则 `record.level not in {"ERROR"}` 永远为真，
        # 查询会安静地返回 0 条。这类"筛少了"比报错难查得多。
        if self.levels:
            normalized: set[LogLevel] = set()
            for item in self.levels:
                try:
                    normalized.add(LogLevel(item) if isinstance(item, str) else item)
                except ValueError:
                    closed = "、".join(level.value for level in LogLevel)
                    raise QueryError(f"未知的日志级别 {item!r}，可选：{closed}") from None
            object.__setattr__(self, "levels", frozenset(normalized))
        for moment, name in ((self.since, "since"), (self.until, "until")):
            if moment is not None and moment.tzinfo is None:
                raise QueryError(f"{name} 必须带时区（pack 里的时间一律 UTC）")
        if self.since is not None and self.until is not None and self.since > self.until:
            raise QueryError(f"since {self.since.isoformat()} 晚于 until {self.until.isoformat()}")

    @classmethod
    def parse(
        cls,
        *,
        keywords: Iterable[str] = (),
        levels: Iterable[str] = (),
        components: Iterable[str] = (),
        since: str | None = None,
        until: str | None = None,
        regex: str | None = None,
        require_all: bool = False,
        limit: int = 20,
    ) -> Query:
        """把命令行字符串翻成 `Query`，坏输入给**能看懂**的报错。

        时间不带时区时**按 UTC 处理**而不是报错：pack 里的时间全是 UTC 且都带
        `+00:00`，人打 `--since 2026-11-23T03:10` 时脑子里想的就是 pack 的那条
        时间轴。反过来（按本地时区解释）会让同一句话在不同机器上查出不同结果。
        """
        wanted_levels: set[LogLevel] = set()
        for raw in _split(levels):
            try:
                wanted_levels.add(LogLevel(raw.strip().upper()))
            except ValueError:
                closed = "、".join(level.value for level in LogLevel)
                raise QueryError(f"未知的日志级别 {raw!r}，可选：{closed}") from None

        wanted_components = {item.strip() for item in _split(components) if item.strip()}

        compiled: re.Pattern[str] | None = None
        if regex is not None:
            try:
                compiled = re.compile(regex, re.IGNORECASE)
            except re.error as exc:
                raise QueryError(f"正则写错了：{exc}") from None

        return cls(
            keywords=tuple(item for item in _split(keywords) if item),
            levels=frozenset(wanted_levels),
            components=frozenset(wanted_components),
            since=parse_moment(since),
            until=parse_moment(until),
            pattern=compiled,
            require_all=require_all,
            limit=limit,
        )

    @property
    def is_empty(self) -> bool:
        """什么都没筛 —— 那就等于"把这个 pack 前 N 条给我看"。"""
        return not (
            self.keywords
            or self.levels
            or self.components
            or self.since
            or self.until
            or self.pattern
        )


def _split(items: Iterable[str]) -> list[str]:
    """`--level ERROR,WARN` 与 `--level ERROR --level WARN` 都要能用。"""
    out: list[str] = []
    for item in items:
        out.extend(part for part in item.replace("，", ",").split(",") if part)
    return out


def parse_moment(text: str | None) -> datetime | None:
    """`2026-11-23T03:10` / `2026-11-23 03:10:00` / 带 `Z` 或 `+00:00` 都吃。"""
    if text is None or not text.strip():
        return None
    raw = text.strip().replace("Z", "+00:00")
    try:
        moment = datetime.fromisoformat(raw)
    except ValueError:
        raise QueryError(
            f"时间写错了：{text!r}，要 ISO 8601（如 2026-11-23T03:10 或 2026-11-23T03:10:00+00:00）"
        ) from None
    return moment.replace(tzinfo=UTC) if moment.tzinfo is None else moment


# ---------------------------------------------------------------- 匹配


def matches(record: LogRecord, query: Query) -> bool:
    """一条记录要不要留下。

    关键词只在 `message` / `logger` / `raw` 里找，**不在 `id` 里找**：`id` 是
    `dn1:00042` 这种坐标，搜 `dn1` 会命中整个组件的每一行，等于没筛。
    """
    if query.levels and record.level not in query.levels:
        return False
    if query.components and record.component not in query.components:
        return False
    if query.since is not None and record.ts < query.since:
        return False
    if query.until is not None and record.ts > query.until:
        return False
    if query.pattern is not None:
        if not any(query.pattern.search(part) for part in _searchable(record)):
            return False
    elif query.keywords:
        haystacks = [part.lower() for part in _searchable(record)]
        hits = [word.lower() in hay for word in query.keywords for hay in haystacks]
        if query.require_all:
            # 每个关键词都得**自己**找到至少一处，而不是"总共找到了几处"。
            found = {
                word.lower() for word in query.keywords for hay in haystacks if word.lower() in hay
            }
            if len(found) != len({word.lower() for word in query.keywords}):
                return False
        elif not any(hits):
            return False
    return True


def _searchable(record: LogRecord) -> tuple[str, ...]:
    return (record.message, record.logger or "", record.raw or "")


# ---------------------------------------------------------------- 命中与原文


@dataclass(frozen=True, slots=True)
class Hit:
    """一条命中。`excerpt` 是**按 anchors 指针回读出来的原文**（可能没有）。"""

    record: LogRecord
    anchor: AnchorRef | None = None
    excerpt: str | None = None
    #: 指针存在但读不回来时的原因（文件被删、偏移越界、哈希对不上）。
    dangling: str | None = None

    @property
    def has_original(self) -> bool:
        return self.excerpt is not None


def resolve_anchor(anchor: AnchorRef | None) -> tuple[str | None, str | None]:
    """按指针回读原始字节，返回 `(原文, 读不回来的原因)`。

    三条失败路径都**返回原因而不是抛异常**：一个 bundle 里 71 条指针中有一条
    悬空（日志被轮转删了），不该让整次查询失败 —— 那会把"能不能查"和"数据完不
    完整"两件事绑在一起。悬空是**要显示给人看的信息**，不是错误。
    """
    if anchor is None:
        return None, None
    path = Path(anchor.file)
    if not path.is_file():
        return None, f"原始文件不在了（{anchor.file}）"
    try:
        with path.open("rb") as handle:
            handle.seek(anchor.offset)
            raw = handle.read(anchor.length)
    except OSError as exc:  # 权限、被占用、路径非法
        return None, f"读不到原始文件：{exc}"
    if len(raw) != anchor.length:
        return None, f"偏移越界（要 {anchor.length} 字节，只读到 {len(raw)}）"
    digest = f"sha256:{hashlib.sha256(raw).hexdigest()}"
    if digest != anchor.sha256:
        return None, "原文已被改写（sha256 对不上）"
    return raw.decode("utf-8", errors="replace").rstrip("\n"), None


def search(paths: CasePackPaths, query: Query, *, with_anchor: bool = True) -> list[Hit]:
    """扫一个 pack，返回前 `query.limit` 条命中（按盘上的 `(ts, id)` 顺序）。

    `logs.jsonl` 已经由 `finalize` 排好序，所以"前 N 条"就是"时间上最早的 N 条"，
    不需要在查询里再排一次 —— 也不该排：排序是写入时的契约，查询只消费它。
    """
    anchors: dict[str, AnchorRef] = {}
    # 没有 anchors/index.json 的 pack（迁移样本）不该让查询失败：它们只是没有
    # 原文指针，检索本身照样能用。`with_anchor` 只在"明确说不想读原文"时关。
    if with_anchor and paths.anchors_index.is_file():
        anchors = {anchor.id: anchor for anchor in read_anchors(paths)}

    hits: list[Hit] = []
    for record in read_logs(paths):
        if not matches(record, query):
            continue
        anchor = anchors.get(record.id)
        excerpt, dangling = resolve_anchor(anchor)
        hits.append(Hit(record=record, anchor=anchor, excerpt=excerpt, dangling=dangling))
        if len(hits) >= query.limit:
            break
    return hits


# ---------------------------------------------------------------- 模板报告


@dataclass(frozen=True, slots=True)
class Report:
    """一份可以直接打印的报告（`render()` 出来的就是 CLI 的输出）。"""

    case_id: str
    manifest: CaseManifest
    query: Query
    hits: Sequence[Hit]
    total_records: int
    by_level: dict[str, int] = field(default_factory=dict)
    by_component: dict[str, int] = field(default_factory=dict)

    @property
    def truncated(self) -> bool:
        """命中数达到了 limit —— 报告里必须说出来，否则人会以为"就这么多"。"""
        return len(self.hits) >= self.query.limit

    def render(self, *, show_raw: bool = True) -> str:
        lines: list[str] = []
        window = self.manifest.window
        lines.append(
            f"案例 {self.case_id}　窗口 {window.since.isoformat()} → {window.until.isoformat()}"
        )
        lines.append(
            f"  pack：{self.total_records} 条记录"
            f"（INFO {self.manifest.counters.by_level.get('INFO', 0)}"
            f" / WARN {self.manifest.counters.by_level.get('WARN', 0)}"
            f" / ERROR {self.manifest.counters.by_level.get('ERROR', 0)}）"
            f"　来源 {len(self.manifest.sources)} 个"
        )
        if self.query.is_empty:
            lines.append("  查询条件：空（列出最早的若干条）")
        else:
            parts = []
            if self.query.keywords:
                joiner = " 且 " if self.query.require_all else " 或 "
                parts.append("关键词 " + joiner.join(self.query.keywords))
            if self.query.pattern is not None:
                parts.append(f"正则 /{self.query.pattern.pattern}/")
            if self.query.levels:
                parts.append("级别 " + "/".join(sorted(level.value for level in self.query.levels)))
            if self.query.components:
                parts.append("组件 " + "/".join(sorted(self.query.components)))
            if self.query.since is not None:
                parts.append(f"起 {self.query.since.isoformat()}")
            if self.query.until is not None:
                parts.append(f"止 {self.query.until.isoformat()}")
            lines.append("  查询条件：" + "；".join(parts))

        if not self.hits:
            lines.append("")
            lines.append("  没有命中。")
            return "\n".join(lines) + "\n"

        lines.append("")
        for number, hit in enumerate(self.hits, start=1):
            record = hit.record
            lines.append(
                f"[{number:>3}] {record.id}  {record.ts.isoformat()}  "
                f"{record.level.value:<5} {record.component:<3} {record.logger or '-'}"
            )
            for chunk in record.message.splitlines():
                lines.append(f"      {chunk}")
            if show_raw:
                if hit.excerpt is not None:
                    for chunk in hit.excerpt.splitlines():
                        lines.append(f"      原文│ {chunk}")
                elif hit.dangling is not None:
                    lines.append(f"      原文│ 不可用：{hit.dangling}")
                else:
                    lines.append("      原文│ 无指针（迁移样本，或采集时还没这项功能）")

        lines.append("")
        summary = f"命中 {len(self.hits)} 条"
        if self.truncated:
            summary += f"（已达到 --limit {self.query.limit}，可能还有更多）"
        if self.by_level:
            summary += "　按级别：" + " ".join(
                f"{name}={count}" for name, count in sorted(self.by_level.items())
            )
        if self.by_component:
            summary += "　按组件：" + " ".join(
                f"{name}={count}" for name, count in sorted(self.by_component.items())
            )
        lines.append(summary)
        return "\n".join(lines) + "\n"


def build_report(paths: CasePackPaths, query: Query, *, with_anchor: bool = True) -> Report:
    manifest = read_manifest(paths)
    hits = search(paths, query, with_anchor=with_anchor)
    return Report(
        case_id=manifest.case_id,
        manifest=manifest,
        query=query,
        hits=hits,
        total_records=manifest.counters.lines,
        by_level=dict(Counter(hit.record.level.value for hit in hits)),
        by_component=dict(Counter(hit.record.component for hit in hits)),
    )


def dump_json(report: Report) -> str:
    """`--json`：给脚本消费用。字段是**稳定**的，改它等于改一个接口。"""
    payload = {
        "case_id": report.case_id,
        "window": {
            "since": report.manifest.window.since.isoformat(),
            "until": report.manifest.window.until.isoformat(),
        },
        "total_records": report.total_records,
        "truncated": report.truncated,
        "hits": [
            {
                "id": hit.record.id,
                "ts": hit.record.ts.isoformat(),
                "level": hit.record.level.value,
                "component": hit.record.component,
                "logger": hit.record.logger,
                "message": hit.record.message,
                "parse_status": hit.record.parse_status.value,
                "excerpt": hit.excerpt,
                "dangling": hit.dangling,
                "anchor": hit.anchor.to_dict() if hit.anchor is not None else None,
            }
            for hit in report.hits
        ],
    }
    return json.dumps(payload, ensure_ascii=False, indent=2) + "\n"


def ask(root: Path, case_id: str, query: Query, *, with_anchor: bool = True) -> Report:
    """`ask` 的库入口（`scripts/ask.py` 只是它的外壳）。"""
    paths = pack_paths(root, case_id)
    if not paths.manifest.is_file():
        raise QueryError(
            f"{paths.dir} 不是一个完成的 pack（没有 manifest.json）—— 采集没跑完、或 case_id 写错了"
        )
    return build_report(paths, query, with_anchor=with_anchor)
