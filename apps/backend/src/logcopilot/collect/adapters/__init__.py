# 顺序 0 · 取数（adapter）：包骨架。协议在 base.py（0a），实现是 file.py（0b）
"""P-A 的取数段（A1）。

本包只放"从哪儿把日志读出来"的东西，**不含**任何解析逻辑：
读出来的永远是 `(行号, 原文)`，解析交给 `collect/canonical.py`。

依赖方向（P-A §9 的硬规则）：
    collect/adapters  →  domain/
    collect/adapters  ✗→  collect/canonical  ✗→  store/
适配器不认识 `LogRecord`，也不认识 `CasePack` —— 这是 V4
（file 与 docker 两条路径哈希相等）能成立的前提。

实现列表：
    base.py  协议（StreamSpec / StreamReader / SourceAdapter / ComponentReader）
    file.py  从 bind mount 出来的文件读（本步）
"""

from logcopilot.collect.adapters.base import (
    CheckpointMismatch,
    ComponentReader,
    LineAllocator,
    RawLine,
    SourceAdapter,
    StreamReader,
    StreamSpec,
    line_sha1,
)
from logcopilot.collect.adapters.file import (
    AmbiguousStream,
    FileAdapter,
    FileReader,
    FileStream,
    hdfs_streams,
)

__all__ = [
    "AmbiguousStream",
    "CheckpointMismatch",
    "ComponentReader",
    "FileAdapter",
    "FileReader",
    "FileStream",
    "LineAllocator",
    "RawLine",
    "SourceAdapter",
    "StreamReader",
    "StreamSpec",
    "hdfs_streams",
    "line_sha1",
]
