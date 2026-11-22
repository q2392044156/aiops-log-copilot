# 顺序 7 · 标注 CLI：人看完 pack 之后把判断写进 labels.json
"""给一个采完的 pack 补标注（`root_cause` / `severity` / `expect` / `reviewer`）。

为什么门槛要用 `--from-measurements` 而不是手填：

V6 要回答的是"这次是否真的采到了故障"。`expect` 填得比实测松，任何 pack 都能过；
填得比实测紧，则永远不过 —— 两种都让 V6 变成摆设。所以默认按 `logs.jsonl`
实测的 ERROR/WARN 条数与报错组件生成门槛，人只负责**确认**这个门槛合理。

用法：

    python scripts/annotate_case.py --case inc_dnvol_01 --check
    python scripts/annotate_case.py --case inc_dnvol_01 --from-measurements \\
        --severity critical --reviewer lead

`labels.fault_injection` 由采集器写，本工具**永远不碰**（`annotate()` 会原样带过去）。
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "apps" / "backend" / "src"))

from logcopilot.collect.labels import LabelError, annotate, describe  # noqa: E402
from logcopilot.store.casepack import pack_paths  # noqa: E402

DEFAULT_ROOT = "data/cases"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="annotate_case",
        description="给一个 CasePack 补标注（人写的那几个字段）",
    )
    parser.add_argument("--case", required=True, help="病例 id")
    parser.add_argument("--root", default=DEFAULT_ROOT, help=f"pack 根目录（默认 {DEFAULT_ROOT}）")
    parser.add_argument(
        "--from-measurements", action="store_true", help="按实测 ERROR/WARN 生成 expect"
    )
    parser.add_argument("--root-cause", default=None, help="根因码（留空表示不改）")
    parser.add_argument("--severity", default=None, help="info / warn / error / critical")
    parser.add_argument("--reviewer", default=None)
    parser.add_argument("--notes", default=None)
    parser.add_argument("--force", action="store_true", help="覆盖已经填过的 expect")
    parser.add_argument("--check", action="store_true", help="只看现状，不写")
    args = parser.parse_args(argv)

    root = Path(args.root)
    if not root.is_absolute():
        root = REPO_ROOT / root
    paths = pack_paths(root, args.case)

    try:
        before = describe(paths)
    except LabelError as exc:
        print(f"[error] {exc}")
        return 2

    print(f"[annotate] {before['case_id']}")
    print(f"  记录数        {before['lines']}")
    print(f"  level 分布    {before['by_level']}")
    print(f"  ERROR 组件    {before['error_components']}")
    print(f"  注入记录      {'有' if before['fault_injection'] else '无'}")
    print(f"  还缺字段      {before['missing'] or '无'}")
    if args.check:
        return 0

    try:
        updated = annotate(
            paths,
            root_cause=args.root_cause,
            severity=args.severity,
            reviewer=args.reviewer,
            notes=args.notes,
            from_measurements=args.from_measurements,
            force=args.force,
        )
    except (LabelError, ValueError) as exc:
        print(f"[error] {exc}")
        return 2

    expect = updated.expect
    print("[done]")
    print(f"  root_cause    {updated.root_cause}")
    print(f"  severity      {updated.severity}")
    print(f"  reviewer      {updated.reviewer}")
    if expect is not None:
        print(
            f"  expect        ERROR>={expect.min_error_lines} "
            f"WARN>={expect.min_warn_lines} 组件={list(expect.error_components)}"
        )
    print(f"  fault_injection {'保留' if updated.fault_injection else '（本 pack 没有）'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
