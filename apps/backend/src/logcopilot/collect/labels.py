# 顺序 5f · 标注：把「人的判断」写回 labels.json（机器写的那部分不碰）
"""采集器只写 `fault_injection`；`root_cause` / `severity` / `expect` 是人写的。

为什么要单独一个模块：`labels.json` 有两个作者，而它们的**写入时机不同**。

- `fault_injection`：采集器在注入过程中写（`Collector` 内部），时间是"故障发生时"；
- 其余字段：人看完 pack 之后写，时间是"采集结束之后"。

如果两者共用一个"整份覆盖"的入口，后写的那个会悄悄把前一个抹掉（最典型的是
`annotate` 时 `fault_injection` 没了，V7 当场失去证据）。所以这里的规则是：
**读 → 改 → 原子写**，只动点名的字段，其它字段原样带过去。

`--from-measurements` 是给 `expect` 用的：采完先看实测到了多少 ERROR/WARN、
哪些组件报错，再把这些数字填成门槛（P-A §7 V6）。门槛**必须来自实测**，
凭想象填的结果是"要么永远不过，要么永远过"，两种都让 V6 失去意义。
"""

from __future__ import annotations

import json
from dataclasses import dataclass

from ..domain.case import Expect, Labels, RootCause, Severity
from ..domain.log import LogLevel
from ..store.casepack import CasePackPaths, read_labels, write_labels

#: 这些字段由采集器写，标注工具**永远不碰**。
MACHINE_FIELDS = frozenset({"fault_injection"})


class LabelError(RuntimeError):
    """标注失败（pack 不存在 / 覆盖已被填过的值但没给 force）。"""


@dataclass(frozen=True, slots=True)
class Measured:
    """从 `logs.jsonl` 数出来的事实。注解门槛就按它填。"""

    lines: int
    by_level: dict[str, int]
    error_components: tuple[str, ...]

    @property
    def error_lines(self) -> int:
        return self.by_level.get(LogLevel.ERROR.value, 0)

    @property
    def warn_lines(self) -> int:
        return self.by_level.get(LogLevel.WARN.value, 0)

    @property
    def noisy_components(self) -> tuple[str, ...]:
        """出现过非 INFO（WARN 或 ERROR）记录的组件。"""
        return self.error_components

    def suggest_expect(self) -> Expect:
        """按实测值给出门槛。

        门槛用实测值，不是"至少 1 条"：上次采到几条，这次至少也得有这么多，
        少了就是采漏了，不该悄悄放行。**指标全 0 的时候也不凑数** ——
        一个日志里看不出故障的故障例，就应该被 V6 挡下来，而不是被门槛
        抬过去（`inc_dnvol_01` 第一次采集就是这种情况：盘满只在指标里有痕迹）。
        """
        return Expect(
            min_error_lines=self.error_lines,
            min_warn_lines=self.warn_lines,
            error_components=self.error_components,
        )


def measure(paths: CasePackPaths) -> Measured:
    """数一遍盘上的记录：level 分布 + 非 INFO 记录出现在哪些组件。

    数的是 WARN **和** ERROR 的组件，不只是 ERROR：V6 已经不要求故障必有
    ERROR（旧语料 104/132 例就没有），证据往往是 WARN。
    """
    if not paths.logs.is_file():
        raise LabelError(f"{paths.logs} 不存在 —— 这不是一个采完的 pack")
    by_level: dict[str, int] = {}
    errors: set[str] = set()
    lines = 0
    with paths.logs.open(encoding="utf-8") as handle:
        for raw in handle:
            raw = raw.strip()
            if not raw:
                continue
            row = json.loads(raw)
            level = str(row["level"])
            by_level[level] = by_level.get(level, 0) + 1
            lines += 1
            if level != LogLevel.INFO.value:
                errors.add(str(row["component"]))
    return Measured(lines=lines, by_level=by_level, error_components=tuple(sorted(errors)))


def annotate(
    paths: CasePackPaths,
    *,
    root_cause: str | RootCause | None = None,
    severity: str | Severity | None = None,
    reviewer: str | None = None,
    notes: str | None = None,
    expect: Expect | None = None,
    from_measurements: bool = False,
    force: bool = False,
) -> Labels:
    """改 `labels.json` 里点名的那几个字段，其余原样保留。

    `from_measurements=True` 时按 `logs.jsonl` 的实测值生成 `expect`。
    已填过 `expect` 又没给 `force` 时**拒绝覆盖**：人工门槛被脚本悄悄改掉，
    V6 就从"验收"退化成"自证"。
    """
    if not paths.labels.is_file():
        raise LabelError(f"{paths.labels} 不存在 —— 先 create_pack")
    current = read_labels(paths)

    # 先算出"这一轮要写的 expect"，再判能不能写：
    # 顺序反了的话，`from_measurements=True` 会在守卫之后才生成 expect，守卫形同虚设。
    if from_measurements:
        expect = measure(paths).suggest_expect()
    if expect is not None and current.expect is not None and not force:
        raise LabelError("labels.expect 已存在；要覆盖请显式 force=True（门槛被改就等于 V6 失效）")

    resolved_root_cause = current.root_cause
    if isinstance(root_cause, str):
        resolved_root_cause = RootCause(root_cause)
    elif root_cause is not None:
        resolved_root_cause = root_cause

    resolved_severity = current.severity
    if isinstance(severity, str):
        resolved_severity = Severity(severity)
    elif severity is not None:
        resolved_severity = severity

    updated = Labels(
        case_id=current.case_id,
        root_cause=resolved_root_cause,
        severity=resolved_severity,
        phase_map=current.phase_map,
        # 机器写的字段原样带过去 —— 这是本模块存在的理由。
        fault_injection=current.fault_injection,
        expect=expect if expect is not None else current.expect,
        reviewer=reviewer if reviewer is not None else current.reviewer,
        notes=notes if notes is not None else current.notes,
    )
    write_labels(paths, updated)
    return updated


def describe(paths: CasePackPaths) -> dict[str, object]:
    """给 CLI 打印用：这个 pack 现在缺什么、实测到了什么。"""
    labels = read_labels(paths)
    measured = measure(paths)
    missing = [
        name
        for name, value in (
            ("root_cause", labels.root_cause),
            ("severity", labels.severity),
            ("expect", labels.expect),
            ("reviewer", labels.reviewer),
        )
        if value is None
    ]
    return {
        "case_id": labels.case_id,
        "missing": missing,
        "fault_injection": labels.fault_injection is not None,
        "lines": measured.lines,
        "by_level": measured.by_level,
        "error_components": measured.error_components,
    }
