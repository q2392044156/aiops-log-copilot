# 顺序 — · 跨阶段接口（ports）：只写契约，不写实现
"""ports —— 跨阶段接口。

总计划 v1.0 §5 把"谁实现、谁调用"钉在四个名字上：

  `LlmClient` / `Retriever` / `CaseStore` / `Clock`

P-A 只落 **`CaseStore`** 与 **`Clock`**：

  - `CaseStore`：P-A 写 pack，P-B/P-C/P-E 读 pack —— 唯一一个从 P-A 就开始被
    别人依赖的接口，所以它必须先有契约再谈实现（实现在 `store/casestore.py`）。
  - `Clock`：采集与注入都要"现在几点"。做成接口不是为了好看，是为了测试里
    能把 20 分钟的采集压成 0 秒（`tests/collect/test_collect_follow.py` 的
    `FakeClock` 就是它的假实现）。

`LlmClient` / `Retriever` 属于 P-C（诊断），等那一步再往这里加。ports 是共享
文件，后阶段改它是预期行为，不是越界。

**依赖方向：`ports` → `domain`，仅此一条。** 不 import `collect` / `store` /
`rag` / `agents` —— 一旦 ports 反过来依赖实现，分层就只活在文档里了。
这也是 `AnchorRef` 必须住在 `domain/case.py` 而不是 `store/casepack.py` 的原因：
`CaseStore.read_anchors()` 的返回类型属于契约的一部分。
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from datetime import datetime
from typing import Protocol

from logcopilot.domain.case import AnchorRef, CaseManifest, Labels
from logcopilot.domain.log import LogRecord

__all__ = ["CaseStore", "Clock"]


class Clock(Protocol):
    """可注入的时钟。返回的时间戳必须是带时区的（实现方保证是 UTC）。"""

    def now(self) -> datetime: ...


class CaseStore(Protocol):
    """读 CasePack 的跨阶段接口。

    为什么是"读"而不是"读写"：**写只有 P-A 一家**（`collect/` + `store/`），
    把写方法也放进跨阶段契约，等于允许 P-B/P-C 往别人的 pack 里塞东西 ——
    那正是"同一份产物被两个阶段改"的开始。

    `case_id` 找不到时抛 `store.casepack.CasePackError` 系列异常
    （`CasePackIncomplete` / `CasePackCorrupt`），**不要返回 None**：
    "这个 pack 不存在"与"存在但只写了一半"是两种事故，调用方必须能分开处理。
    """

    def list_cases(self) -> Sequence[str]:
        """列出所有**已完成**的 case_id（有 `manifest.json` 的才算）。"""
        ...

    def exists(self, case_id: str) -> bool:
        """这个 case 是否已经是一个完成的 pack。"""
        ...

    def read_manifest(self, case_id: str) -> CaseManifest:
        """读 manifest.json。字段不合法或 schema 不认识 → 抛 `CasePackCorrupt`。"""
        ...

    def read_labels(self, case_id: str) -> Labels:
        """读 labels.json（人写的部分与机器写的 `fault_injection` 都在里面）。"""
        ...

    def iter_logs(self, case_id: str) -> Iterator[LogRecord]:
        """流式读 logs.jsonl。大 pack（两万条）不要用 `list()` 一次吃进内存。"""
        ...

    def read_anchors(self, case_id: str) -> Sequence[AnchorRef]:
        """读 `anchors/index.json` 的原文指针；没有就返回空序列（迁移例就是空的）。"""
        ...
