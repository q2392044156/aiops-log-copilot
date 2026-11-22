# 顺序 3/6 · 取数（acquire）—— 包的骨架，协议在 base.py，实现在 file/docker
"""P-A 的取数段（A1）。

本包只放"从哪儿把日志读出来"的东西，**不含**任何解析逻辑：
读出来的永远是 `(行号, 原文)`，解析交给 `collect/canonical.py`。

依赖方向（P-A §9 的硬规则）：
    collect/adapters  →  domain/
    collect/adapters  ✗→  collect/canonical  ✗→  store/
适配器不认识 `LogRecord`，也不认识 `CasePack` —— 这是 V4
（file 与 docker 两条路径哈希相等）能成立的前提。
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

__all__ = [
    "CheckpointMismatch",
    "ComponentReader",
    "LineAllocator",
    "RawLine",
    "SourceAdapter",
    "StreamReader",
    "StreamSpec",
    "line_sha1",
]
