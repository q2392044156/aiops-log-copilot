#!/usr/bin/env python
"""把旧实验台的病例迁成 CasePack v1（M1.4 的 CLI）。

用法::

    python scripts/migrate_cases.py --old-root ../aiops-fault-diagnosis-agent
    python scripts/migrate_cases.py --dry-run          # 只读旧数据、不建包
    python scripts/migrate_cases.py --only inc_lab_...  # 迁一例看看
    python scripts/migrate_cases.py --force            # 重迁（删掉目标目录）

`--old-root` 默认找当前仓库的兄弟目录 `aiops-fault-diagnosis-agent`，
因为旧项目不在本仓库里，也不能被本仓库引用。
"""

from __future__ import annotations

import argparse
import sys
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

# 让 `python scripts/migrate_cases.py` 在没装 editable 时也能跑起来。
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "apps/backend/src"))

from logcopilot.collect.migrate import (
    LegacyCase,
    MigrationError,
    MigrationResult,
    discover_legacy_cases,
    migrate_all,
)

REPO_ROOT = Path(__file__).resolve().parents[1]


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="旧实验台病例 → CasePack v1")
    parser.add_argument(
        "--old-root",
        type=Path,
        default=REPO_ROOT.parent / "aiops-fault-diagnosis-agent",
        help="旧项目根目录（只读，不会被修改）",
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=REPO_ROOT / "data/cases",
        help="迁移产物落盘目录",
    )
    parser.add_argument("--only", action="append", default=[], help="只迁这些 case_id，可重复")
    parser.add_argument("--limit", type=int, default=0, help="最多迁多少例（0=全部）")
    parser.add_argument("--force", action="store_true", help="目标目录已存在时删掉重迁")
    parser.add_argument("--dry-run", action="store_true", help="只读旧数据并打印，不落盘")
    return parser.parse_args(argv)


def _print_row(result: MigrationResult) -> None:
    phases = ",".join(result.phases) if result.phases else "-"
    print(
        f"  {result.case_id:<44} "
        f"raw={result.raw_lines:<5} rec={result.records:<5} "
        f"failed={result.parse_failed:<3} "
        f"phases={phases:<7} "
        f"fault={'yes' if result.covers_fault else 'no ':<3} "
        f"{'OK' if not result.problems else 'PROBLEM'}"
    )
    for problem in result.problems:
        print(f"      ! {problem}")


def _summary(results: list[MigrationResult], failures: list[tuple[str, str]]) -> None:
    print()
    print("=" * 96)
    print(f"迁移完成：{len(results)} 例成功，{len(failures)} 例失败")
    if not results:
        return

    raw_total = sum(result.raw_lines for result in results)
    rec_total = sum(result.records for result in results)
    levels: Counter[str] = Counter()
    components: Counter[str] = Counter()
    for result in results:
        levels.update(result.by_level)
        components.update(result.by_component)

    print(f"  原始行数 {raw_total}  →  记录数 {rec_total}")
    print(f"  level 分布 {dict(sorted(levels.items()))}")
    print(f"  component 分布 {dict(sorted(components.items()))}")
    covered = sum(1 for result in results if result.covers_fault)
    print(f"  日志覆盖 fault_start 的 {covered} 例 / 只在故障前结束的 {len(results) - covered} 例")
    shifted = sum(1 for result in results if result.shifted.total_seconds() != 0)
    print(f"  做过时区平移的 {shifted} 例（WSL 组 +08 → UTC）")
    mismatched = [result.case_id for result in results if not result.hash_matches_upstream]
    print(f"  与旧 content_sha256 对得上的 {len(results) - len(mismatched)} 例")
    for case_id in mismatched[:5]:
        print(f"      ! {case_id} 的 hash 对不上")

    problems = [(result.case_id, problem) for result in results for problem in result.problems]
    print(f"  pack-lint 发现 {len(problems)} 个问题")
    for case_id, problem in problems[:10]:
        print(f"      ! {case_id}: {problem}")

    if failures:
        print(f"  失败 {len(failures)} 例：")
        for case_id, reason in failures[:10]:
            print(f"      ! {case_id}: {reason}")


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    old_root: Path = args.old_root.resolve()
    root: Path = args.root.resolve()

    if not old_root.is_dir():
        print(f"[migrate] 旧项目根目录不存在: {old_root}", file=sys.stderr)
        return 2

    try:
        cases = discover_legacy_cases(old_root)
    except MigrationError as error:
        print(f"[migrate] 读旧数据失败: {error}", file=sys.stderr)
        return 2

    if args.only:
        wanted = set(args.only)
        cases = [case for case in cases if case.case_id in wanted]
        missing = wanted - {case.case_id for case in cases}
        if missing:
            print(
                f"[migrate] 这些 case_id 不在 132 例里: {sorted(missing)}",
                file=sys.stderr,
            )
            return 2
    if args.limit > 0:
        cases = cases[: args.limit]

    print(f"[migrate] 旧项目 {old_root}")
    print(f"[migrate] 产物目录 {root}")
    print(f"[migrate] 待迁移 {len(cases)} 例{'（dry-run，不落盘）' if args.dry_run else ''}")
    print()

    if args.dry_run:
        # dry-run 只验证"读得出来、算得出窗口"，不建目录。
        for case in cases:
            _dry_run_one(case)
        return 0

    started = datetime.now(UTC)
    report = migrate_all(
        cases,
        root=root,
        force=args.force,
        created_at=started,
        on_result=_print_row,
    )
    _summary(report.results, report.failures)
    print(f"  用时 {(datetime.now(UTC) - started).total_seconds():.1f}s")
    return 1 if report.failures else 0


def _dry_run_one(case: LegacyCase) -> None:
    shift_hours = {"docker": 0, "wsl": -8}[case.source]
    first = case.log_text.splitlines()[0] if case.log_text else ""
    print(f"  {case.case_id:<44} {case.group:<24} {shift_hours:+d}h  {first[:60]}")


if __name__ == "__main__":
    raise SystemExit(main())
