# 顺序 8 · 一致性核验（V2）：把盘上的 pack 与「独立第二实现」和「冻结基准」对账
"""V2 的实现：`make verify-collect` 要让 132 例的产出"和上次一模一样"。

**为什么不能拿自己当参照。**
`pack` 是 `migrate.py` / `collect.py` 用 `log.py` + `canonical.py` + `casepack.py`
建出来的。如果核验时又调一遍 `read_logs()`（它内部就是 `LogRecord.from_json_line`）
再调 `canonical_sha256()`（它内部就是 `LogRecord.canonical_payload`），那么"核验
通过"只证明"代码还能跑"，不证明"数据没变"——两边同时错，一样绿。

所以这里做两件互不依赖的事：

1. **独立第二实现**（`fingerprint_pack`）：只吃 `logs.jsonl` 的**文本**，用
   `json.loads` 取字段、用 f-string 按 §5 的列顺序重新拼 canonical 行、用
   `hashlib` 重算 sha256。全程不 import `domain.log`，也不调 `store.casepack`
   的任何计算函数——它和生产者只共享"契约"（10 个字段、6 个投影、`\\n` 分隔），
   不共享代码。
2. **冻结基准**（`baseline.json`）：上一次核对通过时每个 pack 的指纹被写进
   版本库。今天再算一遍，逐字段比。**差异白名单为空**——P-A §7 的 V2 门槛就是
   这么写的：任何一例的任何一个数字变了，都必须是显式改了代码并重新冻结基准。

于是 V2 拦得住两类事：解析器/脱敏/排序被改动（canonical 变），以及原始语料被
改动或迁移口径变了（counters / input_fingerprint 变）。它拦不住"两边一起从头
就错"——那要靠 V9（真实语法 fixture）和 V4（两条读路径对拍）。
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Final

from .casepack import CasePackError, CasePackPaths, pack_paths

#: 基准文件的格式版本。基准文件一旦结构变化（不是数值变化）就升它，
#: 免得读到旧格式时把"字段缺失"误判成"数据变了"。
BASELINE_SCHEMA: Final[str] = "verify-baseline/v1"

#: `logs.jsonl` 一行的字段集合。独立实现也硬编码它，而不是往 domain 里取常量：
#: 两边都从契约写死，"少一个字段"才会在任一侧暴露出来。
_LOG_FIELDS: Final[frozenset[str]] = frozenset(
    {
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
    }
)

#: canonical 投影的列顺序（P-A §6.3）。顺序就是哈希的一部分，不能随手调。
_CANONICAL_ORDER: Final[tuple[str, ...]] = ("id", "ts", "level", "component", "logger", "message")


class VerifyError(RuntimeError):
    """基准文件本身不可用（不是 pack 有问题，是核对工具没法工作）。"""


@dataclass(frozen=True, slots=True)
class PackFingerprint:
    """一个 pack 的"可对账指纹"：全部由 `logs.jsonl` 与 manifest 的**声明值**算出。

    `raw_lines` / `input_fingerprint` 无法从 `logs.jsonl` 反推（它们覆盖的是脱敏前
    的原始字节），所以这两项只能取 manifest 的声明值——基准把它们钉住，等于钉住
    "原始输入是哪一份"，而不是相信生产者算得对。
    """

    case_id: str
    origin: str
    raw_lines: int
    lines: int
    by_level: dict[str, int]
    by_component: dict[str, int]
    parse_failed: int
    canonical_sha256: str
    input_fingerprint: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "origin": self.origin,
            "raw_lines": self.raw_lines,
            "lines": self.lines,
            "by_level": dict(sorted(self.by_level.items())),
            "by_component": dict(sorted(self.by_component.items())),
            "parse_failed": self.parse_failed,
            "canonical_sha256": self.canonical_sha256,
            "input_fingerprint": self.input_fingerprint,
        }

    @classmethod
    def from_dict(cls, case_id: str, data: Mapping[str, Any]) -> PackFingerprint:
        try:
            return cls(
                case_id=case_id,
                origin=str(data["origin"]),
                raw_lines=int(data["raw_lines"]),
                lines=int(data["lines"]),
                by_level={str(k): int(v) for k, v in data["by_level"].items()},
                by_component={str(k): int(v) for k, v in data["by_component"].items()},
                parse_failed=int(data["parse_failed"]),
                canonical_sha256=str(data["canonical_sha256"]),
                input_fingerprint=str(data["input_fingerprint"]),
            )
        except (KeyError, TypeError, ValueError, AttributeError) as exc:
            raise VerifyError(f"基准文件里 {case_id} 的字段不可用：{exc}") from exc


@dataclass(frozen=True, slots=True)
class VerifyReport:
    """一次核验的汇总。`diffs` 只装真有差异的 case（一致的不占空间）。"""

    expected_total: int
    matched: tuple[str, ...] = ()
    missing: tuple[str, ...] = ()
    extra: tuple[str, ...] = ()
    diffs: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    by_origin: Mapping[str, int] = field(default_factory=dict)

    @property
    def failures(self) -> int:
        return len(self.missing) + len(self.extra) + len(self.diffs)

    @property
    def ok(self) -> bool:
        return self.failures == 0 and len(self.matched) == self.expected_total


def _canonical_line(row: Mapping[str, Any]) -> str:
    """按 §6.3 的投影重拼一行 canonical JSON。

    刻意不复用 `store.casepack.canonical_line()`：那正是被核对的一方。
    这里只认契约——6 个字段、这个顺序、无空格、非 ASCII 不转义。
    """
    projected = {name: row[name] for name in _CANONICAL_ORDER}
    return json.dumps(projected, ensure_ascii=False, separators=(",", ":"))


def fingerprint_pack(paths: CasePackPaths) -> PackFingerprint:
    """只读 `logs.jsonl` 文本 + manifest 的声明值，算出一个可对账的指纹。"""
    if not paths.manifest.is_file():
        raise CasePackError(f"{paths.dir} 缺 manifest.json —— 采集未完成")

    try:
        manifest = json.loads(paths.manifest.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CasePackError(f"{paths.manifest} 读不动：{exc}") from exc

    by_level: dict[str, int] = {}
    by_component: dict[str, int] = {}
    parse_failed = 0
    lines = 0
    digest = hashlib.sha256()

    try:
        raw = paths.logs.read_bytes()
    except OSError as exc:
        raise CasePackError(f"{paths.logs} 读不动：{exc}") from exc

    for lineno, chunk in enumerate(raw.split(b"\n"), start=1):
        if not chunk:
            # 文件末尾必然多出一个空串（也容忍中间的空行），不是记录。
            continue
        try:
            row = json.loads(chunk.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise CasePackError(f"{paths.logs.name}:{lineno} 不是合法 JSON：{exc}") from exc
        if not isinstance(row, dict):
            raise CasePackError(f"{paths.logs.name}:{lineno} 不是 JSON 对象")
        missing = _LOG_FIELDS - row.keys()
        extra = row.keys() - _LOG_FIELDS
        if missing or extra:
            raise CasePackError(
                f"{paths.logs.name}:{lineno} 字段不对（缺 {sorted(missing)}，多 {sorted(extra)}）"
            )

        lines += 1
        level = str(row["level"])
        component = str(row["component"])
        by_level[level] = by_level.get(level, 0) + 1
        by_component[component] = by_component.get(component, 0) + 1
        if str(row["parse_status"]) != "ok":
            parse_failed += 1
        digest.update(_canonical_line(row).encode("utf-8"))
        digest.update(b"\n")

    counters = manifest.get("counters") or {}
    hashes = manifest.get("hashes") or {}
    origin = manifest.get("origin") or {}

    if int(counters.get("lines", -1)) != lines:
        raise CasePackError(
            f"{paths.case_id}: manifest 说 {counters.get('lines')} 条，盘上实际 {lines} 条"
        )

    return PackFingerprint(
        case_id=paths.case_id,
        origin=str(origin.get("kind", "unknown")),
        raw_lines=int(counters.get("raw_lines", -1)),
        lines=lines,
        by_level=by_level,
        by_component=by_component,
        parse_failed=parse_failed,
        canonical_sha256=f"sha256:{digest.hexdigest()}",
        input_fingerprint=str(hashes.get("input_fingerprint", "")),
    )


def _diff_counts(label: str, expected: Mapping[str, int], found: Mapping[str, int]) -> list[str]:
    """按"键的并集"比两个计数表，给出人一眼能读懂的差异行。"""
    problems: list[str] = []
    for key in sorted(set(expected) | set(found)):
        want = expected.get(key, 0)
        got = found.get(key, 0)
        if want != got:
            problems.append(f"{label}.{key}: 基准 {want}，现在 {got}")
    return problems


def compare(found: PackFingerprint, expected: PackFingerprint) -> list[str]:
    """逐字段对比，返回差异描述；空列表 = 一致。"""
    problems: list[str] = []
    for name in (
        "origin",
        "raw_lines",
        "lines",
        "parse_failed",
        "canonical_sha256",
        "input_fingerprint",
    ):
        want = getattr(expected, name)
        got = getattr(found, name)
        if want != got:
            problems.append(f"{name}: 基准 {want}，现在 {got}")
    problems.extend(_diff_counts("by_level", expected.by_level, found.by_level))
    problems.extend(_diff_counts("by_component", expected.by_component, found.by_component))
    return problems


def load_baseline(path: Path) -> dict[str, PackFingerprint]:
    """读冻结基准。结构不对就抛 `VerifyError`（这是工具坏了，不是数据变了）。"""
    if not path.is_file():
        raise VerifyError(f"基准文件不存在：{path}（首次可加 --update-baseline 生成）")
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise VerifyError(f"基准文件读不动：{exc}") from exc

    if data.get("schema") != BASELINE_SCHEMA:
        raise VerifyError(
            f"基准文件 schema 是 {data.get('schema')!r}，本脚本只认 {BASELINE_SCHEMA!r}"
        )
    cases = data.get("cases")
    if not isinstance(cases, dict):
        raise VerifyError("基准文件缺 cases 段")
    return {
        str(case_id): PackFingerprint.from_dict(str(case_id), item)
        for case_id, item in cases.items()
    }


def save_baseline(path: Path, fingerprints: Iterable[PackFingerprint]) -> int:
    """把当前指纹冻结成基准（原子写）。返回写入了多少例。"""
    ordered = sorted(fingerprints, key=lambda item: item.case_id)
    payload = {
        "schema": BASELINE_SCHEMA,
        "cases": {item.case_id: item.to_dict() for item in ordered},
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    tmp.replace(path)
    return len(ordered)


def discover(root: Path) -> list[str]:
    """root 下所有已完成 pack 的 case_id（缺 manifest 的目录视为未完成，跳过）。"""
    if not root.is_dir():
        return []
    return sorted(
        child.name
        for child in root.iterdir()
        if child.is_dir() and (child / "manifest.json").is_file()
    )


def verify(
    root: Path, baseline: Mapping[str, PackFingerprint], *, case_ids: Iterable[str] | None = None
) -> VerifyReport:
    """把 `root` 下（或指定的）pack 与基准逐一对账。

    带 `case_ids` 时只核对这几例，并且**不再**把基准里其余的例算成"缺失"——
    只查一例却报"缺 131 例"是噪声，会让人以为出了大事。
    """
    filtered = case_ids is not None
    wanted = sorted(case_ids) if case_ids is not None else discover(root)
    matched: list[str] = []
    missing: list[str] = []
    extra: list[str] = []
    diffs: dict[str, tuple[str, ...]] = {}
    by_origin: dict[str, int] = {}

    for case_id in wanted:
        paths = pack_paths(root, case_id)
        if not paths.manifest.is_file():
            missing.append(case_id)
            continue
        expected = baseline.get(case_id)
        try:
            found = fingerprint_pack(paths)
        except CasePackError as exc:
            diffs[case_id] = (str(exc),)
            continue
        by_origin[found.origin] = by_origin.get(found.origin, 0) + 1
        if expected is None:
            extra.append(case_id)
            continue
        problems = compare(found, expected)
        if problems:
            diffs[case_id] = tuple(problems)
        else:
            matched.append(case_id)

    # 基准里有、盘上没有的，单独报：删包和改包是两种错误。
    if not filtered:
        for case_id in sorted(set(baseline) - set(wanted)):
            missing.append(case_id)

    return VerifyReport(
        expected_total=len(wanted) if filtered else len(baseline),
        matched=tuple(matched),
        missing=tuple(sorted(missing)),
        extra=tuple(sorted(extra)),
        diffs=diffs,
        by_origin=dict(sorted(by_origin.items())),
    )
