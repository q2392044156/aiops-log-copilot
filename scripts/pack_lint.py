# 顺序 7 · 开箱检查：pack 建完之后的独立验收（不在采集流水线里）
"""pack-lint —— 校验 CasePack 目录（P-A §7 的 V1 / V5 / V6）。

一个 pack "跑出来了"和"能用"是两件事。这个脚本只回答后者：结构齐不齐、
排序对不对、计数与盘上是否一致、哈希能不能重算、故障例里到底有没有证据。

    make pack-lint                                  # data/cases 下全部
    make pack-lint CASE=inc_dnvol_01                # 只校验一个
    make pack-lint CASE=base_healthy_01 CHECK=levels # V5：正常例零 ERROR
    make pack-lint CASE=inc_dnvol_01  CHECK=levels # V6：故障例有 ERROR 证据

真正的检查逻辑在 `logcopilot.store.casepack.audit()` / `audit_levels()` 里，
这里只负责"找目录、打印、定退出码"。这样测试可以直接 import 那些函数，
不必启动子进程去解析 stdout。
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from logcopilot.domain.case import Labels
from logcopilot.store.casepack import (
    CasePackError,
    audit,
    audit_levels,
    is_complete,
    pack_paths,
    read_labels,
    read_manifest,
)

#: 默认根目录。与 `.gitignore` 里 `data/cases/*` 呼应：产物不进版本库。
DEFAULT_ROOT = "data/cases"


def _discover(root: Path) -> list[Path]:
    """找出 root 下所有**完成的** pack。

    没有 manifest.json 的目录说明采集还在进行（或者中途崩了），跳过而不是
    报错 —— 否则采一例的 20 分钟里 `make pack-lint` 会一直挂。
    """
    if not root.is_dir():
        return []
    return sorted(
        child
        for child in root.iterdir()
        if child.is_dir() and (child / "manifest.json").is_file()
    )


def _format_problems(header: str, problems: list[str]) -> str:
    lines = [f"[pack-lint] {header}"]
    if not problems:
        lines.append("  PASS")
        return "\n".join(lines)
    for problem in problems:
        lines.append(f"  FAIL  {problem}")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="pack_lint",
        description="校验 CasePack 目录（V1 结构 / V5·V6 等级证据）",
    )
    parser.add_argument(
        "--root",
        default=DEFAULT_ROOT,
        help=f"pack 的根目录（默认 {DEFAULT_ROOT}）",
    )
    parser.add_argument(
        "--case",
        action="append",
        default=[],
        metavar="CASE_ID",
        help="只校验指定 case（可重复）",
    )
    parser.add_argument(
        "--check",
        choices=("all", "levels"),
        default="all",
        help="all=结构+哈希；levels=额外按 labels.expect 判定 V5/V6",
    )
    args = parser.parse_args(argv)

    root = Path(args.root)
    if args.case:
        dirs = [root / case for case in args.case]
        for directory in dirs:
            if not directory.is_dir():
                print(f"[pack-lint] {directory} 不存在", file=sys.stderr)
                return 2
    else:
        dirs = _discover(root)
        if not dirs:
            print(
                f"[pack-lint] {root} 下没有任何完成的 pack"
                "（缺 manifest.json 的目录视为采集未完成，已跳过）",
                file=sys.stderr,
            )
            return 2

    failed = 0
    for directory in dirs:
        paths = pack_paths(root, directory.name)
        if not is_complete(paths):
            print(
                f"[pack-lint] {directory}\n  FAIL  files: 缺 manifest.json —— 采集未完成"
            )
            failed += 1
            continue

        problems = audit(paths)
        if args.check == "levels" and not problems:
            try:
                manifest = read_manifest(paths)
                labels: Labels = read_labels(paths)
            except CasePackError as exc:
                problems.append(f"levels: 读不到 manifest/labels：{exc}")
            else:
                problems.extend(audit_levels(paths, manifest, labels))

        print(_format_problems(str(directory), problems))
        if problems:
            failed += 1

    print(f"\n共 {len(dirs)} 个 pack，{failed} 个有问题")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
