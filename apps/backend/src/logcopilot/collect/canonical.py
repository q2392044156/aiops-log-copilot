# 顺序 1 · 翻译的第一步与第二步：解析 + 续行归并（第三步脱敏在 redact.py）
"""canonicalize —— A2 的前两步：解析 + 续行归并。

规格来源 `P-A_COLLECT_PLAN.md`：

  §6.2  :269-298  两步不可换序 —— 先解析，再归并续行；脱敏是第三步
                  （第 6 步 `collect/redact.py`，本模块只留注入口）
  §5    :186-197  id / ts / level / component / logger / raw 的取值规则
  A-R8            旧数据有两种语法，且续行也带 `[component] ` 前缀

三个必须记牢的点：

  1. **续行判据在"剥掉前缀之后的 body"上做**，不是在原始行上做。
     旧数据每一行（包括续行）都以 `[aiops-lab-dn1] ` 开头，
     拿原始行首列判时间戳，会把每一条续行都认成新日志。
  2. **解析与脱敏的边界**：本模块产出的是"已归一但未脱敏"的文本。
     没装 redactor 时产出会带真实 host/IP，只允许在测试和 dry-run 里用。
  3. **failed 行不丢弃**。一行都认不出来时仍产出 LogRecord，
     `raw` 留原文（P-A §6.2 第二步）。丢弃 = 静默少数据，比留下坏行更危险。
  4. **客户端失败行要单独成条**（O12）。FsShell 的 stderr 是纯文本
     （`put: Cannot create file ... Name node is in safe mode.`），没有时间戳，
     先前的实现把它当续行并进上一条记录的 `raw` —— 于是客户端侧故障在 level
     分布里结构性不可见，`NN_SAFE_MODE` 这一族就是这么被"造不出证据"的。
     现在它单独成条、级别 WARN、时间戳继承上一行（`parse_status=partial`）。
     只对 `hdfs_log4j` 生效，`lab` 保持原样（理由见 `_client_failure` 的注释）。
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Final, Protocol

from logcopilot.domain.case import CanonicalProfile, Window
from logcopilot.domain.log import (
    LogLevel,
    LogRecord,
    ParseStatus,
    coerce_enum,
    require_utc,
)

__all__ = [
    "CanonicalResult",
    "Redactor",
    "assert_within_window",
    "canonicalize",
    "normalize_component",
]

# ---------------------------------------------------------------- 组件归一

#: 旧实验台的容器名 → 短名。域名（域名）不在这张表里的，再走 `_GENERIC` 的规则。
_ALIASES: Final[dict[str, str]] = {
    "nn": "nn",
    "namenode": "nn",
    "namenode1": "nn",
    "nn1": "nn",
    "dn1": "dn1",
    "datanode1": "dn1",
    "dn2": "dn2",
    "datanode2": "dn2",
    "dn3": "dn3",
    "datanode3": "dn3",
}

#: 去掉这些前缀再查表。旧数据是 `aiops-lab-dn1`，容器名可能是 `hdfs-datanode2`。
_PREFIXES: Final[tuple[str, ...]] = ("aiops-lab-", "aiops-", "hdfs-")

#: `datanode7` 这种没登记过的编号，也要能落到 dn<N>（P-A §2 是 3 个 DN，
#: 但写死只认 1–3 会让换拓扑时整批数据 failed）。
_GENERIC_DN: Final[re.Pattern[str]] = re.compile(r"^datanode(\d)$")


def normalize_component(name: str) -> str | None:
    """把容器名 / 日志前缀里的组件名归一到 `nn` / `dn<N>`。

    认不出来时返回 `None` —— 调用方据此把该行判为 failed，
    而不是猜一个 component 出来：猜错会让 `counters.by_component` 全错。
    """
    key = name.strip().lower()
    for prefix in _PREFIXES:
        if key.startswith(prefix):
            key = key[len(prefix) :]
            break
    if key in _ALIASES:
        return _ALIASES[key]
    match = _GENERIC_DN.match(key)
    if match:
        return f"dn{match.group(1)}"
    return None


# ---------------------------------------------------------------- 行首剥壳

#: 旧数据每行的前缀：`[aiops-lab-dn1] `。只吃一个空格 —— 续行
#: `[aiops-lab-dn1]   getNumBytes()` 有两个空格，第二个属于 body 的缩进，
#: 后面要靠它判"这行不是新日志"。
_LAB_PREFIX: Final[re.Pattern[str]] = re.compile(r"^\[(?P<name>[^\]]+)\]\s?")

#: 时间戳表头。三种写法都要吃：`2026-11-11 08:15:05`、
#: `2026-10-29 01:12:46,453`、`2026-11-22T10:03:11.482`。
_TS_HEAD: Final[re.Pattern[str]] = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:[.,]\d{1,9})?)"
)

#: 表头之后的部分：级别 + logger + 可选行号 + 分隔符 + 正文。
#: 覆盖这几种真实写法：
#:   `INFO  DataNode:750 - Receiving ...`      旧语法 A（两个空格、带行号）
#:   `INFO datanode.DataNode: STARTUP_MSG:`    旧语法 B（全限定类别名）
#:   `ERROR org.apache.hadoop...DataNode: ...` Hadoop 原生 log4j（`%c` 全限定）
_REST: Final[re.Pattern[str]] = re.compile(
    r"^\s+(?P<level>[A-Za-z]+)"
    r"\s+(?P<logger>[A-Za-z0-9_.$]+)"
    r"(?::(?P<lineno>\d+))?"
    r"\s*(?:[-:]\s*)?"
    r"(?P<message>.*)$"
)

#: 级别别名。Hadoop 各版本混用 `WARN` / `WARNING`，都收进闭集。
_LEVEL_ALIASES: Final[dict[str, str]] = {"WARNING": "WARN", "ERR": "ERROR"}

#: FsShell 的命令名闭集（用于识别客户端失败行，见 `_CLIENT_FAILURE`）。
#: 只收"真会出现在 stderr 里的命令"，不是把 `hdfs` 的 200 个子命令抄一遍：
#: 漏一个的代价是那行退回续行（旧的错误行为），多一个假名的代价是把一行
#: 普通正文错认成失败 —— 后者更贵，所以闭集宁窄勿宽。
_FS_SHELL_COMMANDS: Final[frozenset[str]] = frozenset(
    {
        "appendToFile", "cat", "checksum", "chgrp", "chmod", "chown", "copyFromLocal",
        "copyToLocal", "count", "cp", "createSnapshot", "deleteSnapshot", "dfs", "du",
        "dus", "expunge", "find", "fsck", "get", "getfacl", "getmerge", "hdfs", "head",
        "help", "ls", "mkdir", "moveFromLocal", "moveToLocal", "mv", "put", "renameSnapshot",
        "rm", "rmdir", "setfacl", "setrep", "stat", "tail", "test", "touchz",
    }
)  # fmt: skip

#: 客户端失败行（O12）。形如：
#:
#:   `put: Cannot create file/workload/w_1.txt._COPYING_. Name node is in safe mode.`
#:   `cat: '/workload/w_1.txt': No such file or directory`
#:
#: 它们是 FsShell 往 stderr 打的**纯文本**，没有时间戳、没有级别、没有 logger。
#: 修复前这类行被当成"续行"并进上一条 log4j 记录的 `raw`，于是：
#:   ① 失败既不算一条记录、也不进 `by_level` —— 客户端侧故障在 level 上结构性不可见；
#:   ② 每例留下 200–400 条 `parse_failed`（严格说它们不算"解析失败"，是没被认领）。
#: 现在单独成条：级别 WARN（它确实是客户端侧的失败），时间戳继承上一行
#: （这一行本身没有时间），`parse_status=partial` 明示"时间戳是继承来的"。
_CLIENT_FAILURE: Final[re.Pattern[str]] = re.compile(
    r"^(?P<command>[A-Za-z][A-Za-z0-9]*):\s+(?P<message>\S.*)$"
)


class Redactor(Protocol):
    """脱敏器的注入口（第 6 步 `collect/redact.py` 实现）。

    放在这里而不是直接 import redact，是为了让 canonical 可在没装
    脱敏规则时被单测：`apply` 返回"脱敏后的文本 + 各规则命中计数"，
    计数直接进 `manifest.counters.redacted`（V8 要求脱敏有账）。
    """

    def apply(self, text: str) -> tuple[str, dict[str, int]]: ...


# ---------------------------------------------------------------- 结果


@dataclass(frozen=True, slots=True)
class CanonicalResult:
    """一次 canonicalize 的产出。

    `raw_line_count` 是**读进来的物理行数**（含续行），`len(records)` 是
    归并后的条数 —— 两者不等是常态，manifest 的
    `counters.raw_lines >= counters.lines` 就是这么来的。
    """

    records: tuple[LogRecord, ...]
    raw_line_count: int
    redacted: dict[str, int] = field(default_factory=dict)


# ---------------------------------------------------------------- 内部：一组


@dataclass(slots=True)
class _Group:
    """一条"逻辑日志"在归并完成前的中间态。

    续行会往这里追加，所以必须是可变的；`LogRecord` 是 frozen 的，
    不能边解析边拼。
    """

    line_no: int
    component: str
    ts: datetime
    level: LogLevel
    logger: str | None
    message: str
    status: ParseStatus
    raw_lines: list[str] = field(default_factory=list)

    def add_continuation(self, body: str, original: str) -> None:
        """吸收一条续行。

        续行继承 ts / component / level / logger（P-A §6.2），正文用 `\\n`
        拼接 —— 保留换行是为了让 P-C 看到异常栈时还能认出它是多行的。
        failed 的组保持 failed：已经认不出来的东西，再多一行也还是认不出来。
        """
        self.message = f"{self.message}\n{body}" if self.message else body
        self.raw_lines.append(original)
        if self.status is ParseStatus.OK:
            self.status = ParseStatus.PARTIAL


# ---------------------------------------------------------------- 解析单行


def _parse_ts(text: str) -> datetime:
    """解析表头时间戳。

    一律按 UTC 处理，不做时区猜测：`manifest.tz` 已由契约固定为 "UTC"
    （P-A §5），猜时区就是 A-R2 那个差 8 小时事故的入口。
    """
    return datetime.fromisoformat(text.replace(",", ".")).replace(tzinfo=UTC)


@dataclass(frozen=True, slots=True)
class _Head:
    ts: datetime
    level: LogLevel | None
    logger: str | None
    message: str
    complete: bool


def _parse_head(body: str) -> _Head | None:
    """解析一条"以时间戳开头"的 body；不以时间戳开头返回 `None`。

    `complete=False` 表示时间戳认出来了、但级别/正文没认全 —— 这种行按
    P-A §6.2 叫 partial（不能丢，它带着时间信息）。
    """
    head = _TS_HEAD.match(body)
    if head is None:
        return None

    ts = _parse_ts(head.group("ts"))
    rest = body[head.end() :]

    match = _REST.match(rest)
    if match is None:
        return _Head(ts=ts, level=None, logger=None, message=rest.strip(), complete=False)

    raw_level = match.group("level").upper()
    raw_level = _LEVEL_ALIASES.get(raw_level, raw_level)
    try:
        level = coerce_enum(LogLevel, raw_level, "level")
    except ValueError:
        # 级别不在闭集（比如 `SEVERE`）。不当成解析失败：时间戳和正文都是真的，
        # 只是这一格认不出来，交给调用方继承上一行的级别。
        return _Head(ts=ts, level=None, logger=None, message=rest.strip(), complete=False)

    message = match.group("message").rstrip()
    logger = match.group("logger") or None
    # 正文为空（`INFO DataNode:750 - `）：LogRecord 不允许"空 message 且无 raw"，
    # 所以这里降级为 partial，让原文留在 raw 里。
    complete = bool(message)
    return _Head(ts=ts, level=level, logger=logger, message=message, complete=complete)


def _client_failure(body: str, *, profile: CanonicalProfile) -> tuple[str, str] | None:
    """认一条 FsShell 客户端失败行；认不出返回 `None`。

    **只对 `hdfs_log4j` 生效**，理由有二：

      1. `lab` 档位跑的是 132 个已迁移、已冻结的 pack —— 给它们多造记录会改
         `canonical_sha256`，V2 的 `tests/fixtures/verify/baseline.json` 全部作废，
         而迁移是为了"证明解析器吃得下真实形态"，不是为了让旧数据更好看。
      2. 旧实验台的客户端日志本来就没被采集（只采了服务端四个文件），
         所以 `lab` 里根本不存在这类行 —— 开个用不上的分支只会留下没被测过的代码。
    """
    if profile is not CanonicalProfile.HDFS_LOG4J:
        return None
    match = _CLIENT_FAILURE.match(body)
    if match is None:
        return None
    command = match.group("command")
    if command not in _FS_SHELL_COMMANDS:
        return None
    return command, body.strip()


# ---------------------------------------------------------------- 主流程


def canonicalize(
    raw_lines: Iterable[tuple[int, str]],
    *,
    profile: CanonicalProfile,
    component: str,
    source: str,
    since: datetime,
    redact: Redactor | None = None,
) -> CanonicalResult:
    """把原始文本行变成 `LogRecord` 序列。

    参数
    ----
    raw_lines
        可迭代的 `(line_no, text)`。`line_no` 是**该 component 的流内连续序号**
        （1 起），由 adapter 负责给：一个 component 若有多条日志流（主日志、
        GC、audit），必须合并成一条连续编号的流，否则 `id` 会撞车
        （`finalize` 会报 id 重复）。第 8 步的 file adapter 要落实这一条。
    profile
        `hdfs_log4j` —— 新采的 Hadoop 原生日志，行首就是时间戳；
        `lab`        —— 旧 132 例，行首多一个 `[component] ` 前缀。
    component
        profile=hdfs_log4j 时整份文件属于哪个组件；profile=lab 时只在
        前缀认不出来时兜底使用（lab 的组件是逐行给的，一个文件里 dn1 与
        dn2 交替出现）。
    source
        写进每行的 `source` 字段（`file:dn1` / `docker:dn1`）。它**不进**
        canonical 投影，所以 file 与 docker 两条路径的哈希仍然相等（V4）。
    since
        窗口起点。只用来给"文件第一行就是续行"这种孤儿行兜底时间戳。
    redact
        脱敏器，`None` 表示不脱敏（仅测试/dry-run 可用）。
    """
    require_utc(since, "since")

    groups: list[_Group] = []
    raw_line_count = 0

    # 上一行的"继承源"。续行与 partial 行都从这里取 ts/level/logger/component。
    prev_ts = since
    prev_level = LogLevel.INFO
    prev_logger: str | None = None
    prev_component: str | None = None

    for line_no, text in raw_lines:
        raw_line_count += 1
        line = text.rstrip("\r\n")

        # ---- 第 0 步：剥掉 `[component] ` 前缀（只在 lab profile 下做）
        comp: str | None
        if profile is CanonicalProfile.LAB:
            prefix = _LAB_PREFIX.match(line)
            if prefix is None:
                # 没有前缀：要么是上游把前缀吃掉了，要么是续行的尾巴。
                # 一律当续行处理 —— 断言它是新日志会造出错误的 component。
                # 组件名沿用上一行；文件第一行就没前缀时用 adaptper 声明的那个。
                comp = prev_component if prev_component is not None else component
                body = line
            else:
                comp = normalize_component(prefix.group("name"))
                body = line[prefix.end() :]
        else:
            comp = component
            body = line

        if not body.strip():
            # 纯空行（或只含空格的行）不是记录，是排版。
            # 但 raw_line_count 已经数过它了 —— 文件里确实存在这一行，
            # 计数少一行会让 counters 与 `wc -l` 对不上。
            # 真跑 132 例时就是这里炸的：某个 case 的首行是空行，
            # 它被当成"孤儿续行"造出一条 raw="" 的 failed 记录，
            # 而 LogRecord 要求 failed 必须带原文。
            continue

        if comp is None:
            groups.append(
                _Group(
                    line_no=line_no,
                    component=component,
                    ts=prev_ts,
                    level=prev_level,
                    logger=None,
                    message=line,
                    status=ParseStatus.FAILED,
                    raw_lines=[line],
                )
            )
            continue

        head = _parse_head(body)

        if head is None:
            failure = _client_failure(body, profile=profile)
            if failure is not None:
                # ---- 客户端失败行（O12）：单独成条、级别 WARN
                # 时间戳继承上一行：这一行自己没时间。`partial` 就是在说这件事，
                # 别把它当 ok —— P-C 若按 ts 排序，要知道这一条的 ts 是近似的。
                _command, message = failure
                groups.append(
                    _Group(
                        line_no=line_no,
                        component=comp,
                        ts=prev_ts,
                        level=LogLevel.WARN,
                        logger="FsShell",
                        message=message,
                        status=ParseStatus.PARTIAL,
                        raw_lines=[line],
                    )
                )
                prev_component = comp
                continue

            # ---- 续行：追加到上一组
            if groups:
                groups[-1].add_continuation(body, line)
            else:
                # 文件第一行就是续行 —— 孤儿。用 since 兜底时间戳，
                # 级别用 INFO。这不是"猜"，是"留个位置让 pack-lint 报出来"。
                groups.append(
                    _Group(
                        line_no=line_no,
                        component=comp,
                        ts=since,
                        level=LogLevel.INFO,
                        logger=None,
                        message=body,
                        status=ParseStatus.FAILED,
                        raw_lines=[line],
                    )
                )
            prev_component = comp
            continue

        # ---- 新日志
        level = head.level if head.level is not None else prev_level
        logger = head.logger if head.logger is not None else prev_logger
        groups.append(
            _Group(
                line_no=line_no,
                component=comp,
                ts=head.ts,
                level=level,
                logger=logger,
                message=head.message,
                status=ParseStatus.OK if head.complete else ParseStatus.PARTIAL,
                # 主行也进 raw_lines：万一后面跟了续行，这条记录就降级为
                # partial，那时 raw 必须包含它的第一行，否则原文只剩尾巴。
                # 状态仍是 ok 时，收尾处会把 raw 丢掉（不写进 jsonl）。
                raw_lines=[line],
            )
        )
        prev_ts, prev_level, prev_logger, prev_component = head.ts, level, logger, comp

    # ---- 收尾：脱敏 + 定型
    counts: dict[str, int] = {}

    def _redact(text: str) -> str:
        if redact is None or not text:
            return text
        cleaned, hits = redact.apply(text)
        for rule, n in hits.items():
            if n:
                counts[rule] = counts.get(rule, 0) + n
        return cleaned

    records: list[LogRecord] = []
    for group in groups:
        raw = "\n".join(group.raw_lines) if group.status is not ParseStatus.OK else None
        records.append(
            LogRecord(
                id=LogRecord.build_id(group.component, group.line_no),
                ts=group.ts,
                level=group.level,
                component=group.component,
                logger=group.logger,
                message=_redact(group.message),
                source=source,
                # raw 也要脱敏：它同样是"会离开这台机器"的文本，
                # 从 raw 漏出去的 IP 和从 message 漏出去的一样是事故。
                raw=_redact(raw) if raw else raw,
                parse_status=group.status,
                template_id=None,
            )
        )

    return CanonicalResult(
        records=tuple(records),
        raw_line_count=raw_line_count,
        redacted=counts,
    )


# ---------------------------------------------------------------- 窗口校验


def assert_within_window(
    records: Iterable[LogRecord],
    window: Window,
    *,
    tolerance: timedelta = timedelta(minutes=1),
) -> None:
    """校验所有时间戳都落在 `window` 内（允许 `tolerance` 的漂移）。

    P-A §6.2：解析结果比窗口早/晚超过 1 分钟 → 报错。这条能抓住两类事故：
    时区没归一（整体差 8 小时）、adapter 取错了文件（读到了别的天）。
    报错而不是过滤：静默丢行会让 V2 的 132/132 变成"看起来对"。
    """
    lower = window.since - tolerance
    upper = window.until + tolerance
    offenders = [r.id for r in records if not (lower <= r.ts <= upper)]
    if offenders:
        raise ValueError(
            f"{len(offenders)} 条日志的时间戳落在窗口 "
            f"[{window.since.isoformat()}, {window.until.isoformat()}] "
            f"±{tolerance} 之外，前几条: {offenders[:5]}"
        )
