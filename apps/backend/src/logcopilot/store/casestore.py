# 顺序 — · CaseStore 的磁盘实现（ports.py 里那份契约的落地）
"""`ports.CaseStore` 的磁盘实现。

薄壳，但有三个必须由它决定的事，所以不写在 `casepack.py` 里：

  1. **root 从哪来**：`CaseStore` 的契约只说"给 case_id"，不说"pack 在哪个盘上"。
     路径策略（默认 `data/cases`）属于基础设施，随着部署变，不该混进 pack 读写。
  2. **"完成"的定义**：`list_cases()` 只列有 `manifest.json` 的目录。
     采集中途崩掉会留下半成品目录（有 logs.jsonl、没有 manifest），
     把它列出来会让 P-B 索引到一半的 pack。
  3. **不缓存**：采集与索引是两个进程，缓存只会让"刚采完的 pack 看不见"。

依赖方向：`store` → `ports` + `domain`，不反向。
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from pathlib import Path

from logcopilot.domain.case import AnchorRef, CaseManifest, Labels
from logcopilot.domain.log import LogRecord

from . import casepack as _pack

__all__ = ["DiskCaseStore"]


class DiskCaseStore:
    """把 `<root>/<case_id>/` 这套目录当 CaseStore。

    >>> store = DiskCaseStore("data/cases")   # doctest: +SKIP
    >>> store.list_cases()[:1]                # doctest: +SKIP
    ['base_healthy_01']
    """

    def __init__(self, root: Path | str = "data/cases") -> None:
        self._root = Path(root)

    @property
    def root(self) -> Path:
        return self._root

    # ------------------------------------------------------------ 查询

    def list_cases(self) -> Sequence[str]:
        if not self._root.is_dir():
            return ()
        cases = [
            entry.name
            for entry in sorted(self._root.iterdir())
            if entry.is_dir() and (entry / _pack.MANIFEST_FILE).is_file()
        ]
        return tuple(cases)

    def exists(self, case_id: str) -> bool:
        return _pack.is_complete(self._paths(case_id))

    # ------------------------------------------------------------ 读

    def read_manifest(self, case_id: str) -> CaseManifest:
        return _pack.read_manifest(self._paths(case_id))

    def read_labels(self, case_id: str) -> Labels:
        return _pack.read_labels(self._paths(case_id))

    def iter_logs(self, case_id: str) -> Iterator[LogRecord]:
        return _pack.iter_logs(self._paths(case_id))

    def read_anchors(self, case_id: str) -> Sequence[AnchorRef]:
        paths = self._paths(case_id)
        # 没有 index.json 不是错误：迁移来的 132 例本来就没有指针
        # （它们指向旧项目的日志文件，换 clone 就悬空，所以刻意不生成）。
        if not paths.anchors_index.is_file():
            return ()
        return tuple(_pack.read_anchors(paths))

    # ------------------------------------------------------------ 内部

    def _paths(self, case_id: str) -> _pack.CasePackPaths:
        # `pack_paths` 会校验 case_id 的正则（它同时是目录名），
        # 所以 `"/etc/passwd"` 这种输入在构造第一层就被挡住。
        return _pack.pack_paths(self._root, case_id)
