# 顺序 9 · 验收闸门 CLI：V3 / V4 / V7 三条断言的可执行入口
"""交互式验收：P-A §7 的 V3 / V4 / V7。

和 `pack_lint.py` / `verify_collect.py` 的分工
----------------------------------------------
- `pack-lint`   = V1，看**一个** pack 自不自洽；
- `verify-collect` = V2，看**与冻结基准**有没有漂移；
- 本脚本        = V3 / V4 / V7，看**两个对象之间**、**两条路径之间**、
                  **注入前后**的关系。它们都需要现场（集群在跑），所以单独一个入口。

用法
----
    # V3：一次采完 vs 中断后补完
    python scripts/verify_gates.py --v3 data/cases/gate_v3_oneshot data/cases/gate_v3_resume

    # V4：file 与 docker 两条取数路径逐字节比
    python scripts/verify_gates.py --v4

    # V7：离线（读 labels.fault_injection）+ 现场（集群是否复原）
    python scripts/verify_gates.py --v7
    python scripts/verify_gates.py --v7 --live

不给任何开关时默认跑 `--v7`（不需要集群，最便宜的那条）。
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "apps" / "backend" / "src"))

from logcopilot.collect.gates import (  # noqa: E402 - 上面的 sys.path 必须先执行
    DEFAULT_TAIL_BYTES,
    check_cluster,
    check_raw_parity,
    check_restored,
    compare_packs,
)
from logcopilot.store.casepack import pack_paths  # noqa: E402


def _print_header(title: str) -> None:
    print(f"\n=== {title} ===")


def run_v3(left: str, right: str) -> int:
    _print_header("V3 · 续采不重不漏")
    left_paths = pack_paths(Path(left).parent, Path(left).name)
    right_paths = pack_paths(Path(right).parent, Path(right).name)
    for paths in (left_paths, right_paths):
        if not paths.manifest.exists():
            print(f"[V3] {paths.dir} 不是一个完成的 pack（缺 manifest.json）")
            return 2
    problems = compare_packs(left_paths, right_paths)
    if problems:
        for problem in problems:
            print(f"[V3] FAIL {problem}")
        return 1
    print(f"[V3] PASS 两侧等价：{left_paths.dir.name} == {right_paths.dir.name}")
    return 0


def run_v4(mount_root: Path, tail_bytes: int) -> int:
    _print_header("V4 · file 与 docker 双路径等价")
    try:
        parities = check_raw_parity(mount_root, tail_bytes=tail_bytes)
    except FileNotFoundError as exc:
        print(f"[V4] 目录不存在：{exc}")
        return 2
    failures = 0
    for parity in parities:
        print(f"[V4] {'PASS' if parity.ok else 'FAIL'} {parity.describe()}")
        if not parity.ok:
            failures += 1
    if failures:
        print(f"[V4] {failures}/{len(parities)} 条流不一致")
        return 1
    print(f"[V4] PASS {len(parities)} 条主流逐字节一致")
    return 0


def run_v7(root: Path, live: bool) -> int:
    _print_header("V7 · 注入可逆")
    problems = check_restored(root)
    for problem in problems:
        print(f"[V7] FAIL {problem}")
    if not live:
        if problems:
            return 1
        print("[V7] PASS 离线部分：所有故障 pack 的 fault_injection 自洽且 restored_ok")
        print("[V7] 提示：加 --live 再验一次现场（容器 / 安全模式 / DN 数 / 残留文件 / 网络）")
        return 0

    status = check_cluster()
    live_problems = status.check()
    for problem in live_problems:
        print(f"[V7] FAIL {problem}")
    if problems or live_problems:
        return 1
    print(
        "[V7] PASS 现场已复原：容器 "
        f"{len(status.running)} 个在跑、安全模式 OFF、"
        f"Live DN={status.live_datanodes}、无注入残留"
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="verify-gates",
        description="P-A §7 的 V3 / V4 / V7 闸门（需要集群在跑）",
    )
    parser.add_argument("--v3", nargs=2, metavar=("LEFT", "RIGHT"), help="比两个 pack 是否等价")
    parser.add_argument("--v4", action="store_true", help="file 与 docker 两条取数路径逐字节比")
    parser.add_argument("--v7", action="store_true", help="故障注入是否可逆（离线 + 可选现场）")
    parser.add_argument(
        "--live", action="store_true", help="V7 附加现场体检（容器 / 安全模式 / DN）"
    )
    parser.add_argument(
        "--mount-root",
        default=str(REPO_ROOT / "runtime" / "hdfs-logs"),
        help="宿主机上 bind mount 出来的日志目录（默认 runtime/hdfs-logs）",
    )
    parser.add_argument(
        "--root",
        default=str(REPO_ROOT / "data" / "cases"),
        help="pack 根目录（默认 data/cases）",
    )
    parser.add_argument(
        "--tail-bytes", type=int, default=DEFAULT_TAIL_BYTES, help="V4 每次比多少字节"
    )
    args = parser.parse_args(argv)

    codes: list[int] = []
    if args.v3:
        codes.append(run_v3(*args.v3))
    if args.v4:
        codes.append(run_v4(Path(args.mount_root), args.tail_bytes))
    if args.v7 or not codes:
        codes.append(run_v7(Path(args.root), args.live))

    worst = max(codes) if codes else 0
    print(f"\n[verify-gates] {'全部通过' if worst == 0 else '有失败项'}")
    return worst


if __name__ == "__main__":
    raise SystemExit(main())
