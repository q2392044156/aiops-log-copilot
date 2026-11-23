# 顺序 0d · 取数（adapter）：WSL 骨架（本期不实现，构造即失败）
"""WSL 适配器 —— **骨架，本期不实现**（P-A §6.1 的"`wsl` 留骨架"）。

为什么留文件而不是留一句注释：

  - 旧 132 例里有 **51 例来自 WSL 实验台**（`wsl_cases` 30 + `wsl_volume` 21），
    它们已经通过 `collect/migrate.py` 迁进 CasePack，所以 P-A 不需要再"采一次 WSL"；
  - 但 `domain.case.Adapter` 里已经有 `WSL` 这个取值（迁移来的 132 例
    `SourceRef.adapter` 里也真的写着 `wsl`），所以"读取器"这一侧必须有个明确的落点，
    否则下一个人会以为它是漏写的；
  - 骨架要能**立刻失败**：构造就抛 `NotImplementedError`，而不是读出一半才发现不对。

真要做的时候，难点在下面三条（写在这里，省得重新踩）：

  1. `\\\\wsl$\\<distro>\\...` 的 UNC 路径在 Python 里能做 `open()`，但 **`os.stat().st_ino`
     跨 9p 文件系统不可靠**，checkpoint 的"inode 变了 = 文件被轮转"判据会误报；
  2. WSL 里 Hadoop 的时钟、网络栈与容器不同，"心跳超时"这类时间敏感的故障测不准
     （P-A §2.1 已经把"WSL 里装原生 Hadoop"排除在方案外，理由同一条）；
  3. 因此正确的做法不是在 Windows 侧读 UNC 路径，而是**在 WSL 里跑一次 `file` 适配器**
     （`uv run --project apps/backend python ...`），把 pack 写回共享目录 ——
     那时候 `Adapter.WSL` 只是个来源标签，不需要新适配器。
"""

from __future__ import annotations

from typing import Final, NoReturn

from logcopilot.domain.case import Adapter

__all__ = ["WSL_REASON", "WslAdapter"]

WSL_REASON: Final[str] = (
    "WSL 适配器本期不实现：51 例 WSL 数据已由 collect/migrate.py 迁入，"
    "真需要时应在 WSL 内直接跑 file 适配器（见 collect/adapters/wsl.py 的说明）"
)


class WslAdapter:
    """占位实现：任何构造尝试都直接报错。

    保留这个类是为了让"`wsl` 还没做"成为一个**可被代码发现的事实**：
    `Adapter.WSL` 在枚举里合法，但没有任何一条路径能真的用它取数。
    """

    adapter = Adapter.WSL

    def __init__(self, *_args: object, **_kwargs: object) -> NoReturn:
        raise NotImplementedError(WSL_REASON)
