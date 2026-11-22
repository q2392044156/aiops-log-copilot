"""``collect/`` —— 采集侧：取数、翻译、编排、指标、注入。

分层约定（总计划 §5 硬规则）：`collect/` 可以 import `domain/` 和 `store/`，
但**不得** import `rag/`、`agents/`、`tools/` —— 采集是离线的，跟在线诊断链路无关。

按流水线顺序（见各文件第一行的 `# 顺序 N` 标注）：

| 顺序 | 模块 | 作用 |
|---|---|---|
| 0a/0b/0c | `adapters/base.py`、`adapters/file.py`、`adapters/docker.py` | 把字节变成一行行原文 |
| 1 | `canonical.py`、`redact.py` | 解析 + 续行归并 + 脱敏 |
| 5 | `collect.py` | 编排：多源并行、checkpoint、finalize |
| 5b | `metrics.py` | 指标快照（`docker stats` / `dfsadmin -report` / `df`） |
| 5c | `inject.py` | 故障注入的剧本与复原 |
| 6 | `migrate.py` | 旧 132 例迁移（旁路，不进采集链路） |
"""

from __future__ import annotations
