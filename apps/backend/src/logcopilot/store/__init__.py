"""包骨架。

P-A 只有两个包：`domain/`（纯类型与校验，零 IO）和 `store/`（CasePack 的读写）。
采集侧的 `collect/` 从第 6 步才开始有内容，那时再建。
"""

from __future__ import annotations
