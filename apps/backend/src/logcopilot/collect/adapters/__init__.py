# 顺序 0 · 取数（adapter）：包骨架。协议 0a，file 实现 0b，docker 实现 0c
"""P-A 的取数段（A1）。

本包只放"从哪儿把日志读出来"的东西，**不含**任何解析逻辑：
读出来的永远是 `(行号, 原文)`，解析交给 `collect/canonical.py`。

依赖方向（P-A §9 的硬规则）：
    collect/adapters  →  domain/
    collect/adapters  ✗→  collect/canonical  ✗→  store/
适配器不认识 `LogRecord`，也不认识 `CasePack` —— 这是 V4
（file 与 docker 两条路径哈希相等）能成立的前提。

实现列表（三者平级、可互换，这是 V4 的结构性保证）：
    base.py    顺序 0a · 协议（StreamSpec / StreamReader / SourceAdapter / ComponentReader）
    file.py    顺序 0b · 读宿主机上的 bind mount 文件
    docker.py  顺序 0c · 读容器内的文件（docker exec stat / tail -c / ls）
"""

from logcopilot.collect.adapters.base import (
    AmbiguousStream,
    CheckpointMismatch,
    ComponentReader,
    LineAllocator,
    RawLine,
    SourceAdapter,
    StreamReader,
    StreamSpec,
    decode_line,
    line_sha1,
)
from logcopilot.collect.adapters.docker import (
    DockerAdapter,
    DockerCommandError,
    DockerReader,
    DockerStream,
    SubprocessRunner,
    container_streams,
)
from logcopilot.collect.adapters.file import (
    FileAdapter,
    FileReader,
    FileStream,
    hdfs_streams,
)

__all__ = [
    "AmbiguousStream",
    "CheckpointMismatch",
    "ComponentReader",
    "DockerAdapter",
    "DockerCommandError",
    "DockerReader",
    "DockerStream",
    "FileAdapter",
    "FileReader",
    "FileStream",
    "LineAllocator",
    "RawLine",
    "SourceAdapter",
    "StreamReader",
    "StreamSpec",
    "SubprocessRunner",
    "container_streams",
    "decode_line",
    "hdfs_streams",
    "line_sha1",
]
