# 顺序 6 · 迁移（旁路）：把旧实验台的 132 例翻译成 CasePack v1
"""migrate —— M1.4：旧 132 例 → 新格式。

它和 `collect/collect.py` 是并行的两条入口，不是流水线的一段：
`collect` 从容器/文件现场采，`migrate` 从旧实验台的 `incident.json + evidence/`
翻译。两条路径最终都落成同一个 `store/casepack.py` 的产物，所以 P-B/P-C
读它们时不需要知道数据是哪来的。

三条必须记住的规则（都来自这次真跑 132 例的结论）：

  1. **时区必须显式统一到 UTC**。旧数据是两批不同时钟的混合：docker 组
     （`real_batch_cases` / `network_cases`）的日志就是 UTC，WSL 组
     （`wsl_cases` / `recaptured_cases/wsl_volume`，共 51 例）写的是本机
     时间（+08）。整批 51 例减去 8 小时之后与 `fault_start` 落在同一小时，
     所以偏移按"来源"定，不按案例定 —— 按案自适配会把一个数据缺陷变成静默修正。
  2. **写进 manifest 的 `config_overrides` 要留痕**：用了多少偏移、来自哪批
     数据。以后有人问"这条日志的时间为什么和旧项目对不上"，答案在包里。
  3. **没有故障信号的案例照样迁，但要把"空"写清楚**：132 例里有 22 例的日志
     在 `fault_start` 之前就结束了，另外三族（心跳/网络/安全模式）在日志里
     没有 ERROR 行。这两件事都不是"数据坏"，不能靠猜补上 ——
     `phases` 留空/只留 fault，`labels.expect` 留 `None`，`labels.notes`
     写明原因，`origin.kind=migrated` 让下游知道这批是回归语料不是质量基准。

刻意不做的事：

  * **不重新脱敏后再声称"已脱敏"**。旧数据已经 `approved_redacted`，但仍会
    过一遍本项目的 `PatternRedactor`（它幂等，占位符 `<:IP:>` 不会被二次改写），
    因为脱敏后的文本才是这个项目里所有 pack 的统一形态；命中的次数照实记进
    `counters.redacted`。
  * **不产出 `expect`**。拿数据反推期望值会让 V6 变成自证。
  * **不迁 `metrics.jsonl`**：`metrics` 的行 schema 还没定稿（P-A §5 只给了
    文件名），现在塞 CSV 进去会让 P-B 读到两种格式。留空并在 notes 里注明。
"""

from __future__ import annotations

import hashlib
import json
import re
import shutil
import sqlite3
from collections import Counter
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Final

from logcopilot.collect.canonical import canonicalize, normalize_component
from logcopilot.collect.redact import REDACT_VERSION, PatternRedactor
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
    RootCause,
    SourceRef,
    Window,
)
from logcopilot.domain.log import LogRecord, ParseStatus
from logcopilot.store.casepack import (
    audit,
    create_pack,
    finalize,
    open_log_writer,
    pack_paths,
)

__all__ = [
    "LEGACY_UTC_OFFSET",
    "LegacyCase",
    "MigrationError",
    "MigrationReport",
    "MigrationResult",
    "discover_legacy_cases",
    "load_legacy_case",
    "migrate_all",
    "migrate_case",
]

#: 迁移器自身的版本。**不进任何一个哈希**（P-A §6.4 的排除项），
#: 只写进 manifest 的 config_overrides，用来解释"同一份旧数据为什么迁出了两份不同的包"。
MIGRATION_VERSION: Final[str] = "migrate/1.0.0"

#: 旧实验台两批数据的时钟偏移。见模块头第 1 条。
LEGACY_UTC_OFFSET: Final[dict[str, timedelta]] = {
    "docker": timedelta(0),
    "wsl": timedelta(hours=-8),
}

#: 旧案例目录（相对旧项目根）。registry 只给 incident_id，不给路径，
#: 所以这里要显式列出来 —— 旧项目里还有 `ops/lab/cases` 等多套重复副本，
#: rglob 会把同一个病例找到十几遍。
LEGACY_GROUPS: Final[tuple[str, ...]] = (
    "ops/lab/real_batch_cases",
    "ops/lab/wsl_cases",
    "ops/lab/recaptured_cases/wsl_volume",
    "ops/lab/network_cases",
)

#: 旧项目的两份 source DB。132 例的**唯一**权威来自它们的 `incident_registry`，
#: 不是目录名（目录里有 197 个子目录，比 132 多出 59 个不在评测集里的病例）。
LEGACY_DBS: Final[tuple[str, ...]] = (
    "runtime/unified4/docker_source.sqlite3",
    "runtime/unified4/wsl_source.sqlite3",
)

#: 窗口之外允许多少漂移。旧数据的时间戳只到秒、`fault_start` 带微秒，
#: 所以"日志首行比 fault_start 晚 0.9 秒"是正常的量化误差，不是数据问题。
WINDOW_TOLERANCE: Final[timedelta] = timedelta(minutes=1)

#: `Hashes` 的两个字段在 finalize 之前必须是合法 sha256 形态，所以占位值
#: 只能长成这样（`finalize` 会用真实值 replace 掉，占位值永远不落盘）。
_PLACEHOLDER_SHA: Final[str] = "sha256:" + "0" * 64

#: 行首时间戳。与 `collect/canonical.py` 的 `_TS_HEAD` 同形，但这里是"预扫
#: 一遍拿窗口起点"用的：canonicalize 需要一个 since 给孤儿行兜底。
_TS_AT_START: Final[re.Pattern[str]] = re.compile(
    r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:[.,]\d{1,9})?"
)

#: 旧数据的行首组件前缀（`[aiops-lab-dn1] `）。只在算"兜底 component"时用。
_PREFIX: Final[re.Pattern[str]] = re.compile(r"^\[(?P<name>[^\]]+)\]")


class MigrationError(RuntimeError):
    """迁移一例失败。**不吞**：一批 132 例里少了一例必须让它显形。"""


# ---------------------------------------------------------------- 旧案例


@dataclass(frozen=True, slots=True)
class LegacyCase:
    """旧项目里一个病例，已经读成内存里的形态（不再碰文件系统）。"""

    case_id: str
    source: str  # "docker" / "wsl"，决定时钟偏移
    group: str  # 目录名，只用于报告与 origin.src
    rel_log_path: str  # 相对旧项目根的日志路径，写进 source.ref
    root_cause: str | None
    fault_start: datetime | None
    recovered: datetime | None
    incident_sha256: str  # `sha256:...`，incident.json 的原始字节
    log_sha256: str  # `sha256:...`，**统一成 LF 之后**的日志文本
    upstream_log_sha256: str | None  # 旧项目自己记的 content_sha256（裸 hex）
    log_text: str


def _parse_ts(value: object) -> datetime | None:
    """旧项目的时刻一律是 `...Z` 的 UTC；返回 tz-aware 的 datetime。"""
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    stamp = datetime.fromisoformat(text)
    if stamp.tzinfo is None:
        # 没带时区的旧值就当 UTC —— 这条假设写进迁移报告，不静默。
        return stamp.replace(tzinfo=UTC)
    return stamp.astimezone(UTC)


def _lf(text: str) -> str:
    """把换行统一成 LF。

    旧项目的 `content_sha256` 就是按 LF 归一后的文本算的（132/132 吻合），
    我们沿用同一口径，才能拿它当"迁移没搬错"的校验。
    """
    return text.replace("\r\n", "\n").replace("\r", "\n")


def _sha256_text(text: str) -> str:
    return "sha256:" + hashlib.sha256(text.encode("utf-8")).hexdigest()


def load_legacy_case(case_dir: Path, *, source: str, old_root: Path | None = None) -> LegacyCase:
    """读一个旧病例目录：`incident.json` + 唯一那份 `.log`。

    给了 `old_root` 就把日志路径记成**相对旧项目根**的形式：`source.ref` 会进
    `input_fingerprint`，若写成绝对路径，同一份数据换台机器（旧项目放在
    `D:\\lab` 而不是 `E:\\project\\...`）算出来的指纹就完全不同 —— 指纹要回答的
    是"数据变了没有"，不是"它躺哪个盘"。
    """
    incident_path = case_dir / "incident.json"
    if not incident_path.is_file():
        raise MigrationError(f"{case_dir} 下没有 incident.json")

    incident_bytes = incident_path.read_bytes()
    data = json.loads(incident_bytes.decode("utf-8"))

    logs = sorted((case_dir / "evidence/logs").glob("*.log"))
    if not logs:
        raise MigrationError(f"{case_dir.name} 的 evidence/logs 下没有 .log")
    if len(logs) > 1:
        # 132 例里每例都只有一份日志。多份说明布局变了，宁可停下来看。
        raise MigrationError(f"{case_dir.name} 有 {len(logs)} 份日志，预期 1 份")

    log_text = logs[0].read_bytes().decode("utf-8", errors="replace")
    normalized = _lf(log_text)

    refs = (data.get("diagnosis_snapshot") or {}).get("log_refs") or []
    upstream = refs[0].get("content_sha256") if refs else None
    stamps = data.get("timestamps") or {}

    rel_log_path = logs[0]
    if old_root is not None:
        # 只在这里转相对；测试里直接调 load_legacy_case 时可省掉 old_root。
        rel_log_path = rel_log_path.relative_to(old_root)

    return LegacyCase(
        case_id=str(data["incident_id"]),
        source=source,
        group=case_dir.parent.name,
        rel_log_path=rel_log_path.as_posix(),
        root_cause=(data.get("labels") or {}).get("root_cause_code"),
        fault_start=_parse_ts(stamps.get("fault_start")),
        recovered=_parse_ts(stamps.get("recovered")),
        incident_sha256="sha256:" + hashlib.sha256(incident_bytes).hexdigest(),
        log_sha256=_sha256_text(normalized),
        upstream_log_sha256=upstream,
        log_text=normalized,
    )


def discover_legacy_cases(
    old_root: Path, *, groups: Sequence[str] = LEGACY_GROUPS
) -> list[LegacyCase]:
    """按 registry 找出 132 例并逐个读进来。

    顺序照 registry 的 `incident_id` 排，保证同一份数据每次迁出来的批次顺序一致。
    """
    wanted = _registry_ids(old_root)

    located: dict[str, tuple[Path, str]] = {}
    for group in groups:
        root = old_root / group
        if not root.is_dir():
            continue
        for case_dir in sorted(p for p in root.iterdir() if p.is_dir()):
            if case_dir.name in wanted:
                located[case_dir.name] = (case_dir, group)

    missing = sorted(set(wanted) - set(located))
    if missing:
        raise MigrationError(f"registry 里有 {len(missing)} 例找不到目录，前几个: {missing[:5]}")

    return [
        load_legacy_case(located[case_id][0], source=wanted[case_id], old_root=old_root)
        for case_id in sorted(located)
    ]


def _registry_ids(old_root: Path) -> dict[str, str]:
    """`case_id -> "docker"/"wsl"`。registry 是 132 例的唯一权威。"""
    wanted: dict[str, str] = {}
    for rel in LEGACY_DBS:
        path = old_root / rel
        if not path.is_file():
            raise MigrationError(f"找不到 source DB: {path}")
        source = "wsl" if "wsl" in path.name else "docker"
        # 显式 close：`with sqlite3.connect(...)` 只提交事务、不关连接，
        # Windows 上会一直持着文件句柄，后面想删/改这个库就报 WinError 32。
        conn = sqlite3.connect(path)
        try:
            rows = list(conn.execute("SELECT incident_id FROM incident_registry"))
        finally:
            conn.close()
        for (incident_id,) in rows:
            wanted[str(incident_id)] = source
    return wanted


# ---------------------------------------------------------------- 单例迁移


@dataclass(frozen=True, slots=True)
class MigrationResult:
    """迁完一例之后的账。CLI 用它打表，测试用它断言。"""

    case_id: str
    raw_lines: int
    records: int
    parse_failed: int
    by_level: dict[str, int]
    by_component: dict[str, int]
    phases: tuple[str, ...]
    covers_fault: bool
    shifted: timedelta
    hash_matches_upstream: bool
    problems: tuple[str, ...] = ()


def _first_stamp(lines: Sequence[str]) -> datetime | None:
    """扫一遍拿到第一个**带时间戳的行**，用来给 canonicalize 定 since。"""
    for line in lines:
        body = line
        prefix = _PREFIX.match(line)
        if prefix is not None:
            body = line[prefix.end() :]
        if _TS_AT_START.match(body):
            stamp = _parse_ts(_TS_AT_START.match(body).group(0))  # type: ignore[union-attr]
            if stamp is not None:
                return stamp
    return None


def _dominant_component(lines: Sequence[str], fallback: str = "nn") -> str:
    """猜一个兜底 component：给"没有 `[comp] ` 前缀的首行"用。

    旧数据里绝大多数行都带前缀，所以这里取出现次数最多的那个组件即可；
    真的一行前缀都没有时用 `nn`（NN 主日志是最常见的那条流）。
    """
    counts: Counter[str] = Counter()
    for line in lines:
        prefix = _PREFIX.match(line)
        if prefix is None:
            continue
        name = normalize_component(prefix.group("name"))
        if name is not None:
            counts[name] += 1
    return counts.most_common(1)[0][0] if counts else fallback


def _fault_phase(
    *, fault_start: datetime | None, recovered: datetime | None, since: datetime, until: datetime
) -> PhaseWindow | None:
    """从 `fault_start` / `recovered` 反推故障段（P-A §6.2 末尾那条规则）。

    旧数据没有 baseline 标注、也没有注入记录，所以能写的只有 fault 一段，
    而且必须**裁剪到日志真正覆盖的区间**里 —— 否则 `Window` 会以"阶段越出窗口"
    拒绝整份 manifest，那是比"少标一段"更糟的结果。
    """
    if fault_start is None or until <= fault_start:
        return None
    start = max(fault_start, since)
    end = until if recovered is None else min(recovered, until)
    if end < start:
        # recovered 早于 fault_start（旧数据里确有这种脏值）→ 就当整段都是故障期。
        end = until
    return PhaseWindow(phase=Phase.FAULT, since=start, until=end)


def migrate_case(
    case: LegacyCase,
    *,
    root: Path,
    created_at: datetime | None = None,
    force: bool = False,
    redactor: PatternRedactor | None = None,
) -> MigrationResult:
    """把一个 `LegacyCase` 落成一个完整的 pack 目录。

    `force=True` 会**删掉**目标目录重来 —— 只给"迁移器改了写法、要重跑一遍"用，
    CLI 必须显式传 `--force` 才会走到这里。
    """
    shift = LEGACY_UTC_OFFSET.get(case.source)
    if shift is None:
        raise MigrationError(f"未知的旧数据来源 {case.source!r}，无法确定时钟偏移")

    if case.root_cause is not None and case.root_cause not in {str(c) for c in RootCause}:
        raise MigrationError(
            f"{case.case_id} 的根因码 {case.root_cause!r} 不在新闭集 "
            f"{[str(c) for c in RootCause]} 里 —— 先决定是扩闭集还是剔掉这一例"
        )
    # 上面校验过闭集，这里只是把它变成枚举（Labels 的字段是 RootCause | None）。
    root_cause = RootCause(case.root_cause) if case.root_cause is not None else None

    lines = case.log_text.splitlines()
    if not lines:
        raise MigrationError(f"{case.case_id} 的日志是空的")

    # 旧日志的时间是本地时间（WSL 组）或 UTC（docker 组）。canonicalize 只认
    # 文本里的字面值（一律先当 UTC），所以时钟平移必须在**解析之后**做：
    # 挪文本会破坏 raw 的可信度（raw 必须逐字等于原始行）。
    raw_first = _first_stamp(lines)
    since = raw_first
    if since is None:
        since = case.fault_start or datetime(1970, 1, 1, tzinfo=UTC)

    fallback = _dominant_component(lines)
    canonical = canonicalize(
        ((number, line) for number, line in enumerate(lines, start=1)),
        profile=CanonicalProfile.LAB,
        component=fallback,
        # source 不进 canonical 投影，但会写进每一行，方便 P-C 的引用回溯。
        source=f"migrated:{case.group}",
        since=since,
        redact=redactor if redactor is not None else PatternRedactor(),
    )
    records: tuple[LogRecord, ...] = tuple(
        replace(record, ts=record.ts + shift) for record in canonical.records
    )
    if not records:
        raise MigrationError(f"{case.case_id} 的 {len(lines)} 行日志一条都没解析出来")

    # 窗口 = 日志真正覆盖的区间。不是"故障前 5 分钟到故障后 5 分钟"——
    # 旧数据给不出那个范围（baseline 中位 11 秒，fault 中位 46 秒）。
    stamps = [record.ts for record in records]
    window_since, window_until = min(stamps), max(stamps)
    phase = _fault_phase(
        fault_start=case.fault_start,
        recovered=case.recovered,
        since=window_since,
        until=window_until,
    )
    phases: tuple[PhaseWindow, ...] = (phase,) if phase is not None else ()

    notes = _notes(case, shift=shift, records=records, covers_fault=phase is not None)

    components = sorted({record.component for record in records})
    labels = Labels(
        case_id=case.case_id,
        root_cause=root_cause,
        severity=None,  # 旧项目没有严重度字段，不猜
        expect=None,  # 不由数据反推（否则 V6 自证）
        reviewer="migrated",
        notes=notes,
    )

    paths = pack_paths(root, case.case_id)
    if paths.dir.exists():
        if not force:
            raise MigrationError(f"{paths.dir} 已存在；要重迁请显式 --force（会删掉整份目录重来）")
        shutil.rmtree(paths.dir)

    create_pack(paths, labels)

    manifest = CaseManifest(
        case_id=case.case_id,
        created_at=created_at or datetime.now(UTC),
        canonical_profile=CanonicalProfile.LAB,
        window=Window(since=window_since, until=window_until, phases=phases),
        origin=Origin(
            kind=OriginKind.MIGRATED,
            src=f"{case.group}/{case.case_id}",
            incident_sha256=case.incident_sha256,
        ),
        sources=tuple(
            SourceRef(
                adapter=Adapter.FILE,
                ref="legacy:" + case.rel_log_path,
                component=component,
            )
            for component in components
        ),
        counters=Counters(raw_lines=0, lines=0, by_level={}, by_component={}),
        hashes=Hashes(
            input_fingerprint=_PLACEHOLDER_SHA,
            canonical_sha256=_PLACEHOLDER_SHA,
        ),
        checkpoint=Checkpoint(adapter_state={}, last_ts=window_until),
        redact_policy=RedactPolicy(version=REDACT_VERSION),
        config_overrides={
            "migration_version": MIGRATION_VERSION,
            "legacy_source": case.source,
            "legacy_tz_offset_hours": str(int(shift.total_seconds() // 3600)),
            "legacy_log_ref": case.rel_log_path.rsplit("/", 1)[-1],
        },
    )

    writer = open_log_writer(paths)
    try:
        writer.extend(records)
    finally:
        writer.close()

    final = finalize(
        paths,
        manifest,
        raw_line_count=canonical.raw_line_count,
        raw_sha256=case.log_sha256,
        redacted=canonical.redacted,
    )

    problems = audit(paths)
    matches = (
        case.upstream_log_sha256 is None or case.log_sha256 == "sha256:" + case.upstream_log_sha256
    )
    if not matches:
        problems.append(
            f"hash: 旧项目记的 content_sha256 与本地重算不一致"
            f"（旧 {case.upstream_log_sha256}，本地 {case.log_sha256}）"
        )

    return MigrationResult(
        case_id=case.case_id,
        raw_lines=final.counters.raw_lines,
        records=final.counters.lines,
        parse_failed=final.counters.parse_failed,
        by_level=dict(final.counters.by_level),
        by_component=dict(final.counters.by_component),
        phases=tuple(str(window.phase) for window in phases),
        covers_fault=phase is not None,
        shifted=shift,
        hash_matches_upstream=matches,
        problems=tuple(problems),
    )


def _notes(
    case: LegacyCase, *, shift: timedelta, records: Sequence[LogRecord], covers_fault: bool
) -> str:
    """把"这份数据缺什么"写进 labels.notes。

    迁移例和现场采的例最大的区别不在内容，在**缺什么**：没有 baseline 段、
    没有注入记录、没有期望值。这些必须写在包里面，而不是写在迁移脚本的
    stdout 里 —— 半年后看这个 pack 的人不会去翻控制台。
    """
    hours = int(shift.total_seconds() // 3600)
    parts = [
        f"迁移自旧实验台 {case.group}/{case.case_id}",
        f"原始 {len(records)} 条记录 / 时钟偏移 {hours:+d}h（已统一到 UTC）",
        "无 baseline 段（旧数据未标注）",
        "无 fault_injection 记录（旧数据未保存注入脚本）",
        "无 expect（不由数据反推）",
        "metrics.jsonl 留空（旧 CSV 未迁，行 schema 未定稿）",
    ]
    if not covers_fault:
        parts.insert(
            3,
            "日志在 fault_start 之前结束：本例**不含任何故障期日志**，只能当采集/解析回归语料",
        )
    failed = sum(1 for record in records if record.parse_status is ParseStatus.FAILED)
    if failed:
        parts.append(f"含 {failed} 条 failed 行（原文留在 raw 里，未丢弃）")
    return "；".join(parts)


# ---------------------------------------------------------------- 批量


@dataclass(slots=True)
class MigrationReport:
    """一批迁移的汇总。CLI 直接把它打成表。"""

    results: list[MigrationResult] = field(default_factory=list)
    failures: list[tuple[str, str]] = field(default_factory=list)

    @property
    def ok(self) -> int:
        return len(self.results)

    @property
    def records(self) -> int:
        return sum(result.records for result in self.results)

    @property
    def raw_lines(self) -> int:
        return sum(result.raw_lines for result in self.results)

    def by_root_cause(self) -> dict[str, int]:
        # 根因在 labels 里，这里只按"有无故障段"粗分，够回答"迁了多少"。
        kinds = Counter("fault" if result.covers_fault else "no_fault" for result in self.results)
        return dict(kinds)


def migrate_all(
    cases: Iterable[LegacyCase],
    *,
    root: Path,
    force: bool = False,
    created_at: datetime | None = None,
    on_result: Callable[[MigrationResult], None] | None = None,
) -> MigrationReport:
    """逐例迁移，**一例失败不影响其余**，失败原因收进 `report.failures`。"""
    report = MigrationReport()
    for case in cases:
        try:
            result = migrate_case(case, root=root, created_at=created_at, force=force)
        except MigrationError as error:
            report.failures.append((case.case_id, str(error)))
            continue
        report.results.append(result)
        if on_result is not None:
            on_result(result)
    return report
