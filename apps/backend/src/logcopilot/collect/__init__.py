"""``collect/`` —— 采集侧。目前只有 canonicalize（A2），adapter（A1）从第 8 步开始。

分层约定（总计划 §5 硬规则）：`collect/` 可以 import `domain/` 和 `store/`，
但**不得** import `rag/`、`agents/`、`tools/` —— 采集是离线的，跟在线诊断链路无关。
"""

from __future__ import annotations
