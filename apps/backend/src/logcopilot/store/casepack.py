# 顺序 4–8 · 打包：建包 → 流式写 → finalize（排序/哈希/manifest）→ 读回
"""
CasePack v1 的磁盘读写：建目录、流式追加、`finalize`、两个哈希、原子写。

这是 P-A 里**唯一**知道 CasePack 目录长什么样的模块。`domain/case.py` 只管
"字段合不合法"，这里管"字节怎么落到盘上"。P-B / P-C / P-E 读 pack 也只经过
这里 —— 三处各写一遍读逻辑，就是三份会各自漂移的解析器。

规格来源 `P-A_COLLECT_PLAN.md`：
  §5    目录形状与四个文件
  §6.3  写入方式、排序键、canonical 投影、原子性、pack-lint 校验项
  §6.4  两个哈希各覆盖什么、哪些字段必须排除

三个必须记住的不变式：

1. **manifest 最后写**。采集途中崩了，盘上就只有半截 `logs.jsonl` 而没有
   `manifest.json` —— 于是它天然是一个"未完成的 pack"，读的人一看就知道
   不能当样本用，不需要额外状态位。
2. **`canonical_sha256` 只覆盖 6 个字段**（`id/ts/level/component/logger/message`），
   排除 `source`/`raw`/`parse_status`/`template_id`。这是 V4「file 与 docker
   两条读路径哈希相等」能成立的前提：`source` 里必然带着 adapter 名字。
3. **`input_fingerprint` 不含任何元数据**。`pipeline_version`、
   `redact_policy.version`、`canonical_profile` 都不进（§6.4 排除项）——
   否则"换了个版本号"会被误判成"数据变了"，两条不变式同时废。
"""

from __future__ import annotations

import hashlib
import json
import os
from collections import Counter
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Final

from logcopilot.domain.case import (
    SCHEMA,
    CaseManifest,
    Counters,
    Hashes,
    Labels,
    SourceRef,
    Window,
)
from logcopilot.domain.log import LogLevel, LogRecord, ParseStatus, format_ts

MANIFEST_FILE: Final[str] = "manifest.json"
LOGS_FILE: Final[str] = "logs.jsonl"
METRICS_FILE: Final[str] = "metrics.jsonl"
LABELS_FILE: Final[str] = "labels.json"
ANCHORS_DIR: Final[str] = "anchors"
ANCHORS_INDEX: Final[str] = "index.json"

#: 采集期的 checkpoint sidecar。名字与格式由 `collect/collect.py` 定义，
#: 但路径归这里管 —— "pack 目录里有哪些文件"只能有一个定义处。
CHECKPOINT_FILE: Final[str] = "checkpoint.json"

#: anchors/index.json 自己的版本号。它与 manifest.schema 分开升版本：
#: 指针格式（加个字段）比 pack 结构（加个文件）变得更频繁。
ANCHORS_SCHEMA: Final[str] = "anchors/v1"

#: 原子写的临时后缀。写完之后 `os.replace` 覆盖目标文件 —— 同盘内 replace
#: 是原子的，所以读的人要么看到旧文件、要么看到完整的新文件，不会看到半截。
_TMP_SUFFIX: Final[str] = ".tmp"


# ---------------------------------------------------------------- 异常


class CasePackError(RuntimeError):
    """CasePack 相关错误的基类。

    用自己的一层异常而不是裸 OSError / ValueError，是为了让调用方（
    `pack_lint`、第 9 步的 `collect.py`）能一句 `except CasePackError`
    接住"这个 pack 有问题"，而不用把文件系统错误和数据错误混在一起。
    """


class CasePackIncomplete(CasePackError):
    """目录存在但 manifest 还没写 —— 采集没跑完，或中途崩了。"""


class CasePackExists(CasePackError):
    """已经有一个完成的 pack 在同一个 case_id 上，拒绝悄悄覆盖。"""


class CasePackCorrupt(CasePackError):
    """盘上的内容读不出来：jsonl 某行坏了、字段对不上、哈希不符。"""


# ---------------------------------------------------------------- 路径


@dataclass(frozen=True, slots=True)
class CasePackPaths:
    """一个 CasePack 的地址簿。

    单独做成类型，是为了让"pack 在哪"这件事只有一个地方定义 —— 目录形状
    以后加文件（比如 P-B 的 `vectors.npy`），改这里一处就够。
    """

    root: Path
    case_id: str

    @property
    def dir(self) -> Path:
        return self.root / self.case_id

    @property
    def manifest(self) -> Path:
        return self.dir / MANIFEST_FILE

    @property
    def logs(self) -> Path:
        return self.dir / LOGS_FILE

    @property
    def metrics(self) -> Path:
        return self.dir / METRICS_FILE

    @property
    def labels(self) -> Path:
        return self.dir / LABELS_FILE

    @property
    def anchors_dir(self) -> Path:
        return self.dir / ANCHORS_DIR

    @property
    def anchors_index(self) -> Path:
        return self.anchors_dir / ANCHORS_INDEX

    @property
    def checkpoint_file(self) -> Path:
        """采集期的 checkpoint sidecar（P-A §5 目录树之外的一处增补）。

        为什么不在 §5 的四个文件里：`manifest.json` 是 finalize **最后**写
        的（写了才算完成），而采一例要 20 分钟、中途 Ctrl-C 是常态。把
        checkpoint 也压到最后的 manifest 里，等于中断就什么都不记得。

        生命周期：`collect/collect.py` 每采完一个组件重写它一次；
        finalize 成功后 checkpoints 已并进 manifest，这个文件被删除。
        """
        return self.dir / CHECKPOINT_FILE

    @property
    def is_finalized(self) -> bool:
        return self.manifest.is_file()


def pack_paths(root: Path | str, case_id: str) -> CasePackPaths:
    """从 `data/cases` 这样的根目录 + case_id 推出全部路径。"""
    return CasePackPaths(root=Path(root), case_id=case_id)


# ---------------------------------------------------------------- 建包与追加


def create_pack(paths: CasePackPaths, labels: Labels) -> None:
    """建一个未完成的 pack：目录 + 空 logs/metrics + labels + anchors/。

    **不写 manifest** —— manifest 由 `finalize` 最后写。所以在 finalize 之前，
    这个目录在协议上等于不存在（`read_manifest` 会抛 `CasePackIncomplete`）。

    已经完成的 pack 会被拒绝覆盖：重采一个 case 应该换 case_id（`inc_dnvol_02`），
    而不是把上一份证据抹掉。
    """
    if labels.case_id != paths.case_id:
        raise CasePackError(f"labels.case_id={labels.case_id!r} 与目录名 {paths.case_id!r} 不一致")
    if paths.manifest.exists():
        raise CasePackExists(f"{paths.dir} 已经是一个完成的 pack，拒绝覆盖")

    paths.dir.mkdir(parents=True, exist_ok=True)
    paths.anchors_dir.mkdir(exist_ok=True)
    # 空文件先落地：后续所有写入都是 append，不需要再关心"文件在不在"。
    paths.logs.touch(exist_ok=True)
    paths.metrics.touch(exist_ok=True)
    _atomic_write_text(paths.labels, labels.to_json())


class LogWriter:
    """采集期的 `logs.jsonl` 追加器（P-A §6.3：按到达序流式追加）。

    为什么不先攒在内存里再一次性写：采一例是 20 分钟，中途 Ctrl-C 或容器崩了
    很常见。流式追加意味着**已经采到的行不会丢**，重启后 `--resume` 接着采，
    这也是 V3（续采不重不漏）能测的前提。

    行尾统一 `\\n`：Windows 上默认的 `\\r\\n` 会让 `canonical_sha256` 与 Linux
    算出来不一样，V4 直接假失败。
    """

    def __init__(self, path: Path) -> None:
        self._path = path
        self._fh = path.open("a", encoding="utf-8", newline="\n")

    def append(self, record: LogRecord) -> None:
        self._fh.write(record.to_json_line())
        self._fh.write("\n")

    def extend(self, records: Iterable[LogRecord]) -> None:
        for record in records:
            self.append(record)

    def flush(self) -> None:
        """落盘但不关文件。注入脚本执行前会调一次 —— 崩溃时也要有迹可循。"""
        self._fh.flush()
        os.fsync(self._fh.fileno())

    def close(self) -> None:
        if not self._fh.closed:
            self._fh.close()

    def __enter__(self) -> LogWriter:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()


def open_log_writer(paths: CasePackPaths) -> LogWriter:
    """打开追加器。要求 pack 目录已经存在（即 `create_pack` 跑过）。"""
    if not paths.dir.is_dir():
        raise CasePackError(f"{paths.dir} 不存在 —— 先调 create_pack")
    return LogWriter(paths.logs)


def write_labels(paths: CasePackPaths, labels: Labels) -> None:
    """重写 `labels.json`（原子写）。

    `create_pack` 会把 labels 写一次，但注入模式需要**再写一次**：注入脚本
    执行完之后才知道 `fault_injection{started, ended, restored}` 是什么。
    标注是被事实补充的，不是一开始就全知道的。
    """
    if labels.case_id != paths.case_id:
        raise CasePackError(f"labels.case_id={labels.case_id!r} 与目录名 {paths.case_id!r} 不一致")
    if not paths.dir.is_dir():
        raise CasePackError(f"{paths.dir} 不存在 —— 先调 create_pack")
    _atomic_write_text(paths.labels, labels.to_json())


# ---------------------------------------------------------------- 读


def is_complete(paths: CasePackPaths) -> bool:
    """manifest 在不在，就是这个 pack 完不完成。没有额外的状态文件。"""
    return paths.manifest.is_file()


def read_manifest(paths: CasePackPaths) -> CaseManifest:
    if not paths.manifest.is_file():
        raise CasePackIncomplete(
            f"{paths.dir} 没有 manifest.json —— 采集没跑完（或没跑 finalize），不能当样本用"
        )
    try:
        return CaseManifest.from_json(paths.manifest.read_text(encoding="utf-8"))
    except ValueError as exc:
        raise CasePackCorrupt(f"{paths.manifest} 读不出来: {exc}") from None


def read_labels(paths: CasePackPaths) -> Labels:
    if not paths.labels.is_file():
        raise CasePackIncomplete(f"{paths.dir} 没有 labels.json")
    try:
        return Labels.from_json(paths.labels.read_text(encoding="utf-8"))
    except ValueError as exc:
        raise CasePackCorrupt(f"{paths.labels} 读不出来: {exc}") from None


def iter_logs(paths: CasePackPaths) -> Iterator[LogRecord]:
    """逐行读 `logs.jsonl`。坏行当场报错，不跳过 —— 静默跳过会让 V2 的
    "行数相等"变成一句空话。"""
    if not paths.logs.is_file():
        raise CasePackIncomplete(f"{paths.dir} 没有 logs.jsonl")
    with paths.logs.open("r", encoding="utf-8") as fh:
        for line_no, line in enumerate(fh, start=1):
            text = line.strip()
            if not text:
                continue
            try:
                yield LogRecord.from_json_line(text)
            except ValueError as exc:
                raise CasePackCorrupt(f"{LOGS_FILE}:{line_no} 不是合法 LogRecord: {exc}") from None


def read_logs(paths: CasePackPaths) -> list[LogRecord]:
    return list(iter_logs(paths))


# ---------------------------------------------------------------- 哈希


def canonical_line(record: LogRecord) -> str:
    """canonical 投影的一行（含行尾换行），`canonical_sha256` 的输入单位。

    字段顺序由 `LogRecord.canonical_payload()` 固定，分隔符紧凑、中文不转义：
    **同样的记录在任何机器上都必须得到同一串字节**，否则 V2 的 132/132 会
    变成"看起来像但是不相等"。
    """
    return json.dumps(record.canonical_payload(), ensure_ascii=False, separators=(",", ":")) + "\n"


def canonical_sha256(records: Iterable[LogRecord]) -> str:
    """§6.3 的投影哈希。**只吃 6 个字段**，与 `source`/`raw`/`parse_status` 无关。

    `pack_lint` 会用同一个函数重算一遍与 manifest 里存的值比对 —— 所以这个
    函数是"哈希怎么算"的唯一定义处。
    """
    digest = hashlib.sha256()
    for record in records:
        digest.update(canonical_line(record).encode("utf-8"))
    return f"sha256:{digest.hexdigest()}"


def build_input_fingerprint(
    *,
    case_id: str,
    sources: Sequence[SourceRef],
    window: Window,
    raw_line_count: int,
    raw_sha256: str,
) -> str:
    """§6.4 的 `input_fingerprint`：只由"源"决定。

    覆盖 `case_id / source_refs / window / raw_line_count / raw_sha256`。
    换日志、改窗口 → 变；改代码、改脱敏策略、换 adapter → **不变**。

    两个刻意的取舍：

    * `window.phases` 不进指纹，只进 `since/until`。phases 是**标注**，
      补标一次 baseline 不该让"这是同一份数据"这件事失效。
    * 传进来的是 `raw_sha256` 而不是原文：脱敏后的 pack 里没有原文
      （§6.2 硬约束②：不改源文件、不复制原文），所以只能由取数的一方
      （第 8 步的 adapter）在读完原始字节时算好交进来。
    """
    if not raw_sha256.startswith("sha256:"):
        raise ValueError(f"raw_sha256 必须以 'sha256:' 开头，拿到 {raw_sha256!r}")

    parts: list[str] = [SCHEMA, f"case_id:{case_id}"]
    for source in sources:
        # 逐字段拼，不用 repr(source)：`SourceRef` 以后加字段时，指纹不该
        # 因为"多了一个不影响数据的字段"而变。
        parts.append(
            f"source:{source.adapter}|{source.ref}|{source.component}"
            f"|{source.image_digest or '-'}|{source.path or '-'}"
        )
    parts.append(f"window:{format_ts(window.since)}|{format_ts(window.until)}")
    parts.append(f"raw_lines:{raw_line_count}")
    parts.append(f"raw_sha256:{raw_sha256}")

    text = "\n".join(parts) + "\n"
    return f"sha256:{hashlib.sha256(text.encode('utf-8')).hexdigest()}"


# ---------------------------------------------------------------- anchors


@dataclass(frozen=True, slots=True)
class AnchorRef:
    """一条"原文在哪"的指针（P-A §5：**不复制原文**）。

    采到的原文留在 `runtime/hdfs-logs/<container>/` 下（bind mount 出来的），
    pack 里只存 `(file, offset, length, sha256)`。这样 pack 能被人 clone 走，
    体积也不随日志膨胀；要复核某行时按指针回去读，并用 sha256 证明没被改过。
    """

    file: str
    offset: int
    length: int
    sha256: str

    def __post_init__(self) -> None:
        if not self.file:
            raise ValueError("anchor.file 不能为空")
        if self.offset < 0:
            raise ValueError(f"anchor.offset 必须 >= 0，拿到 {self.offset}")
        if self.length <= 0:
            raise ValueError(f"anchor.length 必须 > 0，拿到 {self.length}")
        if not self.sha256.startswith("sha256:") or len(self.sha256) != 71:
            raise ValueError(f"anchor.sha256 格式不对: {self.sha256!r}")

    def to_dict(self) -> dict[str, object]:
        return {
            "file": self.file,
            "offset": self.offset,
            "length": self.length,
            "sha256": self.sha256,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, object]) -> AnchorRef:
        try:
            return cls(
                file=str(data["file"]),
                offset=int(data["offset"]),  # type: ignore[arg-type]
                length=int(data["length"]),  # type: ignore[arg-type]
                sha256=str(data["sha256"]),
            )
        except KeyError as exc:
            raise ValueError(f"anchors 条目缺字段 {exc}") from None


def write_anchors(paths: CasePackPaths, anchors: Sequence[AnchorRef]) -> None:
    """写 `anchors/index.json`。空列表也要写 —— "没有原文指针"和"文件不存在"
    是两件事，前者是合法的（比如单元测试造的 pack）。"""
    payload = {
        "schema": ANCHORS_SCHEMA,
        "entries": [anchor.to_dict() for anchor in anchors],
    }
    paths.anchors_dir.mkdir(parents=True, exist_ok=True)
    _atomic_write_text(
        paths.anchors_index,
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
    )


def read_anchors(paths: CasePackPaths) -> list[AnchorRef]:
    if not paths.anchors_index.is_file():
        raise CasePackIncomplete(f"{paths.dir} 没有 {ANCHORS_DIR}/{ANCHORS_INDEX}")
    try:
        payload = json.loads(paths.anchors_index.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise CasePackCorrupt(f"anchors/{ANCHORS_INDEX} 不是合法 json: {exc}") from None
    if not isinstance(payload, dict) or payload.get("schema") != ANCHORS_SCHEMA:
        raise CasePackCorrupt(f"anchors/{ANCHORS_INDEX} 的 schema 不是 {ANCHORS_SCHEMA}")
    entries = payload.get("entries")
    if not isinstance(entries, list):
        raise CasePackCorrupt(f"anchors/{ANCHORS_INDEX} 的 entries 必须是数组")
    return [AnchorRef.from_dict(entry) for entry in entries]


# ---------------------------------------------------------------- finalize


def finalize(
    paths: CasePackPaths,
    manifest: CaseManifest,
    *,
    raw_line_count: int,
    raw_sha256: str,
    redacted: Mapping[str, int] | None = None,
    anchors: Sequence[AnchorRef] = (),
) -> CaseManifest:
    """收尾：排序 → 重算哈希 → 写 anchors → 最后写 manifest。

    三个参数是"只有取数一方才知道"的信息，所以必须从外面传进来：

    ``raw_line_count``  脱敏前读到的原始行数（续行归并会把它变小）
    ``raw_sha256``      脱敏前原始字节的 sha256
    ``redacted``        四类脱敏规则各命中多少次

    调用前 `manifest` 里的 `counters` / `hashes` 是占位值：真实值由这里按
    盘上的记录算出来，再用 `dataclasses.replace` 覆盖。占位值永远不会落盘 ——
    manifest 是这个函数最后才写的。
    """
    if manifest.case_id != paths.case_id:
        raise CasePackError(
            f"manifest.case_id={manifest.case_id!r} 与目录名 {paths.case_id!r} 不一致"
        )

    records = read_logs(paths)
    records.sort(key=LogRecord.sort_key)
    # 排序键是 (ts, id)，二者理论上唯一；id 撞车说明续采时 line_no 算错了
    # （P-A §5 的 line_no 是源文件绝对物理行号，就是为了防这个）。
    seen: set[str] = set()
    for record in records:
        if record.id in seen:
            raise CasePackCorrupt(f"logs.jsonl 里 id 重复: {record.id}")
        seen.add(record.id)

    _write_logs_atomic(paths, records)

    counters = _count_records(records, raw_line_count=raw_line_count, redacted=redacted)
    hashes = Hashes(
        input_fingerprint=build_input_fingerprint(
            case_id=manifest.case_id,
            sources=manifest.sources,
            window=manifest.window,
            raw_line_count=raw_line_count,
            raw_sha256=raw_sha256,
        ),
        canonical_sha256=canonical_sha256(records),
    )

    write_anchors(paths, anchors)
    final = replace(manifest, counters=counters, hashes=hashes)
    _write_manifest_atomic(paths, final)
    return final


def _count_records(
    records: Sequence[LogRecord],
    *,
    raw_line_count: int,
    redacted: Mapping[str, int] | None,
) -> Counters:
    """按盘上真实记录重算 counters。

    不信任调用方传进来的 by_level/by_component —— 那些是"我以为我写了什么"，
    这里数的是"实际写出了什么"。两边的差就是 bug 所在。
    """
    by_level = Counter(str(record.level) for record in records)
    by_component = Counter(record.component for record in records)
    parse_failed = sum(1 for record in records if record.parse_status is ParseStatus.FAILED)
    return Counters(
        raw_lines=raw_line_count,
        lines=len(records),
        by_level=dict(by_level),
        by_component=dict(by_component),
        parse_failed=parse_failed,
        redacted=dict(redacted or {}),
    )


# ---------------------------------------------------------------- 原子写


def _write_logs_atomic(paths: CasePackPaths, records: Sequence[LogRecord]) -> None:
    """把排序后的记录整体重写回 `logs.jsonl`。

    写临时文件再 `os.replace`，而不是原地截断重写：排序阶段如果崩了，
    原来的追加顺序还完整保留着，下次跑 finalize 能重来。
    """
    tmp = paths.logs.with_name(paths.logs.name + _TMP_SUFFIX)
    with tmp.open("w", encoding="utf-8", newline="\n") as fh:
        for record in records:
            fh.write(record.to_json_line())
            fh.write("\n")
    os.replace(tmp, paths.logs)


def _write_manifest_atomic(paths: CasePackPaths, manifest: CaseManifest) -> None:
    _atomic_write_text(paths.manifest, manifest.to_json())


def _atomic_write_text(path: Path, text: str) -> None:
    tmp = path.with_name(path.name + _TMP_SUFFIX)
    with tmp.open("w", encoding="utf-8", newline="\n") as fh:
        fh.write(text)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)


# ---------------------------------------------------------------- 校验（V1 / V5 / V6）


def audit(paths: CasePackPaths) -> list[str]:
    """V1 的全套检查。返回问题列表，**空列表 = 通过**。

    为什么返回字符串而不是抛异常：pack-lint 要一次把一个 pack 的所有问题
    列出来，抛异常只能报第一个。人在修数据时最不想做的就是"改一条、跑一次"。

    这里能重算的只有 `canonical_sha256`：`input_fingerprint` 覆盖的是**脱敏前
    的原始字节**，而原文按设计不复制进 pack（§6.2），所以它只能靠取数一方
    在采集时算对。这不是漏检，是这两个哈希职责不同的必然结果。
    """
    problems: list[str] = []

    for path in (paths.manifest, paths.logs, paths.labels, paths.anchors_index):
        if not path.is_file():
            problems.append(f"files: 缺 {path.relative_to(paths.dir)}")
    if problems:
        # 文件都不齐，后面的检查会在半路抛 OSError，没有意义。
        return problems

    try:
        manifest = read_manifest(paths)
        labels = read_labels(paths)
        records = read_logs(paths)
    except CasePackError as exc:
        return [f"read: {exc}"]

    if labels.case_id != manifest.case_id:
        problems.append(
            f"labels: case_id={labels.case_id!r} 与 manifest 的 {manifest.case_id!r} 不一致"
        )

    seen: set[str] = set()
    duplicated: set[str] = set()
    previous_ts = None
    for record in records:
        if record.id in seen:
            duplicated.add(record.id)
        seen.add(record.id)
        if previous_ts is not None and record.ts < previous_ts:
            problems.append(
                f"sort: {record.id} 的 ts={format_ts(record.ts)} 早于上一条 "
                f"{format_ts(previous_ts)} —— finalize 没排序或排序键不对"
            )
            break
        previous_ts = record.ts
    if duplicated:
        problems.append(f"id: 重复 {sorted(duplicated)[:5]}（共 {len(duplicated)} 个）")

    counters = manifest.counters
    if counters.lines != len(records):
        problems.append(f"counters: lines={counters.lines}，盘上实际 {len(records)} 行")
    if counters.raw_lines < counters.lines:
        problems.append(f"counters: raw_lines={counters.raw_lines} 小于 lines={counters.lines}")
    by_level = Counter(str(record.level) for record in records)
    if dict(counters.by_level) != dict(by_level):
        problems.append(f"counters: by_level 与盘上不符，盘上={dict(by_level)}")
    by_component = Counter(record.component for record in records)
    if dict(counters.by_component) != dict(by_component):
        problems.append(f"counters: by_component 与盘上不符，盘上={dict(by_component)}")
    parse_failed = sum(1 for record in records if record.parse_status is ParseStatus.FAILED)
    if counters.parse_failed != parse_failed:
        problems.append(f"counters: parse_failed={counters.parse_failed}，盘上实际 {parse_failed}")

    recomputed = canonical_sha256(records)
    if recomputed != manifest.hashes.canonical_sha256:
        problems.append(
            f"hashes: canonical_sha256 重算为 {recomputed}，"
            f"manifest 里是 {manifest.hashes.canonical_sha256}"
        )

    return problems


def audit_levels(paths: CasePackPaths, manifest: CaseManifest, labels: Labels) -> list[str]:
    """V5 / V6：正常例零 ERROR、故障例有预期证据。

    这两条是"采到了没有"的判据 —— 一个 pack 结构再合法，如果故障例里一条
    ERROR 都没有，那它只能证明环境能跑，不能进评测集。
    """
    problems: list[str] = []
    error_count = manifest.counters.by_level.get(str(LogLevel.ERROR), 0)

    if labels.root_cause is None:
        # V5：正常例。这条不需要人填 expect 就该成立。
        if error_count:
            problems.append(f"levels: 正常例（root_cause=null）却有 {error_count} 条 ERROR")
        return problems

    if labels.expect is None:
        problems.append("levels: 故障例必须填 labels.expect，否则无法判定采到没采到")
        return problems

    expect = labels.expect
    if error_count < expect.min_error_lines:
        problems.append(
            f"levels: ERROR 只有 {error_count} 条，expect 要求 >= {expect.min_error_lines}"
        )
    warn_count = manifest.counters.by_level.get(str(LogLevel.WARN), 0)
    if warn_count < expect.min_warn_lines:
        problems.append(
            f"levels: WARN 只有 {warn_count} 条，expect 要求 >= {expect.min_warn_lines}"
        )

    actual = {record.component for record in read_logs(paths) if record.level is LogLevel.ERROR}
    expected = set(expect.error_components)
    if expected and not expected <= actual:
        problems.append(f"levels: expect 里说会报 ERROR 的 {sorted(expected - actual)} 没有出现")
    unexpected = actual - expected
    if expected and unexpected:
        problems.append(f"levels: 出现了 expect 之外的 ERROR 组件 {sorted(unexpected)}")

    return problems
