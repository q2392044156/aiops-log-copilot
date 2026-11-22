# 顺序 8 · 一致性核验（V2）：`make verify-collect` 的入口，只负责找目录、打印、定退出码
"""verify-collect —— P-A §7 的 V2：132/132 逐例对账。

    make verify-collect                      # 拿 tests/fixtures/verify/baseline.json 比
    make verify-collect CASE=inc_dnvol_01    # 只核一例
    make verify-collect ARGS="--update-baseline"   # 代码/语料确实变了，重新冻结基准

**它是回归闸门，不是质量评分。** 通过的含义只有一句：这 132 个 pack 的每一个
数字都与上次冻结的一致。所以"改完解析器跑一遍通过"不等于没改坏——如果确实
改了输出，必须 `--update-baseline` 再提交基准，让 diff 出现在 PR 里，人来看。

检查逻辑在 `logcopilot.store.verify`（独立第二实现 + 冻结基准），这里只做 CLI。
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from logcopilot.store.casepack import CasePackError, pack_paths
from logcopilot.store.verify import (
    VerifyError,
    discover,
    fingerprint_pack,
    load_baseline,
    save_baseline,
    verify,
)

#: 默认根目录。与 `.gitignore` 里 `data/cases/*` 呼应：产物不进版本库。
DEFAULT_ROOT = "data/cases"

#: 基准文件放 `tests/fixtures/` 而不是 `data/`：它必须进版本库，
#: 否则换台机器就没有"上次"可比了。
DEFAULT_BASELINE = "tests/fixtures/verify/baseline.json"

#: V2 的门槛：132 例。写成参数是为了让"只迁了 10 例就想跑过"当场暴露。
DEFAULT_EXPECT = 132


def _print_report(report) -> None:
    for case_id in report.missing:
        print(f"[verify-collect] MISSING  {case_id}: 基准里有，盘上没有（被删了？）")
    for case_id in report.extra:
        print(f"[verify-collect] EXTRA    {case_id}: 盘上有，基准里没有（新增的例要重新冻结基准）")
    for case_id, problems in sorted(report.diffs.items()):
        print(f"[verify-collect] DIFF     {case_id}")
        for problem in problems:
            print(f"    {problem}")
    origins = "，".join(f"{kind} {count}" for kind, count in report.by_origin.items())
    tail = f"差异 {len(report.diffs)} 例，缺 {len(report.missing)} 例，多 {len(report.extra)} 例"
    if report.skipped:
        # 新采的例不冻结：时间戳每次采集都变，冻进基准等于把"重采一次"变成失败。
        tail += f"，不参与比对 {len(report.skipped)} 例（collected）"
    print(
        f"\n[verify-collect] {len(report.matched)}/{report.expected_total} 一致"
        f"（{origins}）；{tail}"
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="verify_collect",
        description="V2：132/132 逐例对账（行数 / level / component / 两个哈希）",
    )
    parser.add_argument("--root", default=DEFAULT_ROOT, help=f"pack 根目录（默认 {DEFAULT_ROOT}）")
    parser.add_argument(
        "--baseline",
        default=DEFAULT_BASELINE,
        help=f"冻结基准（默认 {DEFAULT_BASELINE}）",
    )
    parser.add_argument(
        "--case", action="append", default=[], metavar="CASE_ID", help="只核指定 case（可重复）"
    )
    parser.add_argument(
        "--expect",
        type=int,
        default=DEFAULT_EXPECT,
        help=f"要求至少核到多少例（默认 {DEFAULT_EXPECT}；V2 的门槛就是它）",
    )
    parser.add_argument(
        "--update-baseline",
        action="store_true",
        help="把当前指纹写成新的基准（确认变化是有意的之后再用）",
    )
    args = parser.parse_args(argv)

    root = Path(args.root)
    baseline_path = Path(args.baseline)

    if args.update_baseline:
        case_ids = args.case or discover(root)
        if not case_ids:
            print(f"[verify-collect] {root} 下没有任何完成的 pack", file=sys.stderr)
            return 2
        fingerprints = []
        for case_id in case_ids:
            paths = pack_paths(root, case_id)
            try:
                fingerprints.append(fingerprint_pack(paths))
            except CasePackError as exc:
                print(f"[verify-collect] FAIL  {case_id}: {exc}", file=sys.stderr)
                return 1
        written = save_baseline(baseline_path, fingerprints)
        print(f"[verify-collect] 已冻结 {written} 例 → {baseline_path}")
        return 0

    try:
        baseline = load_baseline(baseline_path)
    except VerifyError as exc:
        print(f"[verify-collect] {exc}", file=sys.stderr)
        return 2

    report = verify(root, baseline, case_ids=args.case or None)
    _print_report(report)

    if not report.ok:
        return 1
    if len(report.matched) < args.expect:
        print(
            f"[verify-collect] 只核到 {len(report.matched)} 例，少于要求的 {args.expect} 例",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
