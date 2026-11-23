# 顺序 8b · `make ask`：命令行问答入口（P-A §8.2 的"最小闭环"）
"""对一个 CasePack 做关键词检索，打印模板报告。

用法：:

    make ask CASE=inc_dnvol_01 ARGS="--level ERROR"
    make ask CASE=inc_net_02 ARGS="-k Excluding --component nn --no-raw"
    make ask CASE=base_healthy_02 ARGS="--since 2026-11-23T03:10 --until 2026-11-23T03:11"

这个脚本只做三件事：解析参数 → 调 `store/query.py` → 打印。所有检索逻辑都在库里
（`build_report` / `search` / `resolve_anchor`），因为逻辑要能被测试直接调用，而
"脚本"的职责是**适配命令行**，不是实现业务。
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# 让脚本在"没装 editable"的情况下也能跑（`python scripts/ask.py`）。
_REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO_ROOT / "apps" / "backend" / "src"))

from logcopilot.store.query import Query, QueryError, ask, dump_json  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="ask",
        description="对一个 CasePack 做关键词检索（P-A 的验收入口）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "示例：\n"
            "  ask --case inc_dnvol_01 --level ERROR\n"
            "  ask --case inc_net_02 --keyword Excluding --component nn\n"
            "  ask --case base_healthy_02 --since 2026-11-23T03:10\n"
        ),
    )
    parser.add_argument("--root", default="data/cases", help="pack 根目录（默认 data/cases）")
    parser.add_argument("--case", required=True, help="case_id（即 data/cases 下的目录名）")
    parser.add_argument(
        "-k",
        "--keyword",
        action="append",
        default=[],
        help="关键词，可重复；默认「或」，加 --all 变成「且」",
    )
    parser.add_argument("-r", "--regex", default=None, help="正则（与 --keyword 二选一，正则优先）")
    parser.add_argument(
        "--level",
        action="append",
        default=[],
        help="日志级别，逗号分隔，如 --level ERROR,WARN",
    )
    parser.add_argument(
        "--component", action="append", default=[], help="组件，逗号分隔，如 --component dn1,nn"
    )
    parser.add_argument("--since", default=None, help="起始时间（ISO 8601；不带时区按 UTC）")
    parser.add_argument("--until", default=None, help="结束时间（同上）")
    parser.add_argument("--all", action="store_true", help="多个关键词取「且」")
    parser.add_argument("--limit", type=int, default=20, help="最多打印多少条（默认 20）")
    # 回读原文是**默认行为**：不知原文的检索结果没法验证，等于把 ask 降级成 grep。
    # 只有明确说 --no-raw 才只打印 message（大 pack 里想快速扫一遍时用）。
    parser.add_argument("--no-raw", action="store_true", help="只看 message，不回读原文")
    parser.add_argument("--json", action="store_true", help="输出 JSON（给脚本消费）")
    parser.add_argument(
        "--no-anchor", action="store_true", help="完全不读 anchors（只想快点看 message）"
    )
    args = parser.parse_args(argv)

    try:
        query = Query.parse(
            keywords=args.keyword,
            levels=args.level,
            components=args.component,
            since=args.since,
            until=args.until,
            regex=args.regex,
            require_all=args.all,
            limit=args.limit,
        )
    except QueryError as exc:
        print(f"[ask] 参数不对：{exc}", file=sys.stderr)
        return 2

    try:
        report = ask(
            _resolve_root(args.root),
            args.case,
            query,
            with_anchor=not args.no_anchor,
        )
    except QueryError as exc:
        print(f"[ask] {exc}", file=sys.stderr)
        return 2

    if args.json:
        sys.stdout.write(dump_json(report))
    else:
        sys.stdout.write(report.render(show_raw=not args.no_raw))

    # 命中 0 条不是错误：那是"这个案例里没有这回事"，正是查询该告诉人的答案。
    return 0


def _resolve_root(raw: str) -> Path:
    """相对路径一律相对**仓库根**。

    `uv run --directory apps/backend` 会把子进程的 cwd 换成 `apps/backend`，
    所以 `--root data/cases` 会在那边落成 `apps/backend/data/cases`（第 15 步
    实采时踩过：pack 建到了错误目录）。这里显式换算，调用方不用关心 cwd。
    """
    path = Path(raw)
    return path if path.is_absolute() else (_REPO_ROOT / path)


if __name__ == "__main__":
    raise SystemExit(main())
