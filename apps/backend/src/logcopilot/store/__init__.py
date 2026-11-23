"""包骨架。

P-A 只有两个包：`domain/`（纯类型与校验，零 IO）和 `store/`（CasePack 的读写）。

`store/` 里四个模块的分工：

  - `casepack.py`   CasePack 的读写 + finalize + 两个哈希（P-A 的主体）
  - `verify.py`     V2 闸门：独立第二实现重算指纹，和冻结基准对账
  - `query.py`      `ask` 的检索与报告（确定性，不接 LLM）
  - `casestore.py`  `ports.CaseStore` 的磁盘实现（给 P-B/P-C/P-E 用的读接口）
"""

from __future__ import annotations
