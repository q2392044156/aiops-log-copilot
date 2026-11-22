# 顺序 5c · 注入编排：把"造一次故障"包成可复现、可回滚的一段
"""故障注入编排（P-A §4 的 inject 模式的实现）。

分工：

- 本模块**只描述命令**（`InjectionPlan`：apply 建故障、restore 复原、verify 验复原），
  并负责在正确时刻调用 `InjectionHook` 约定的 `flush`；
- `infra/hdfs/inject/*.sh` 是同一批命令的**人工版**（有人在终端里复现时用它们），
  不是采集链路的依赖 —— 采集链路不调 bash，免得 Windows 上没 bash 就跑不动。

三条硬规则：

1. **注入前必须 flush**。`flush()` 由采集器传进来，注入前调用；否则"注入之前系统是好的"
   这句话在盘上找不到证据。
2. **restore 一定要跑**：apply 抛异常时也跑（`try/finally`）。留下一个没修复的集群，
   下一次采集就是在污染现场上采的。
3. **`restored_ok` 必须是被验证出来的**，不是"我调用了 restore 所以它好了"。
   `verify` 命令的退出码才是答案；没有 verify 的注入，`restored_ok` 记 `False` 并写进 notes。

`dd` 写满数据盘那一步是**预期失败**的（`expect_ok=False`）：`No space left on device`
正是我们要的故障，不能当成注入失败。
"""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from ..domain.case import FaultInjection
from .metrics import Runner, docker_runner

__all__ = [
    "DEFAULT_DN_DATA_DIR",
    "DEFAULT_NETWORK",
    "INJECTIONS",
    "Command",
    "CommandInjection",
    "InjectionError",
    "InjectionPlan",
    "dn_heartbeat_pause",
    "dn_heartbeat_stop",
    "dn_volume_fill",
    "network_disconnect",
    "nn_safemode_enter",
]

#: compose 里 `name: aiops-log-copilot` + 网络 `hdfs` → 实际网络名。
DEFAULT_NETWORK = "aiops-log-copilot_hdfs"

#: DataNode 数据盘路径（P-A §2：dn1 是 128MB tmpfs）。
DEFAULT_DN_DATA_DIR = "/tmp/hadoop-root/dfs/data"

#: 填盘文件名。带 `lab.` 前缀是为了 `ls` 一眼能认出"这是注入留下的"。
FILL_NAME = "lab.fill"


class InjectionError(RuntimeError):
    """注入或复原过程中某条命令没有按预期结束。"""


@dataclass(frozen=True, slots=True)
class Command:
    """一条要执行的命令（argv 形式，不经过 shell）。"""

    argv: tuple[str, ...]
    #: `False` 表示"这条命令失败是预期的"（`dd` 写满盘就是这种）。
    expect_ok: bool = True
    #: 复原判据：不是"退出码 0 就算好了"。`docker inspect` 对 paused 的容器
    #: 也返回 0，只有 stdout 里出现 `running` 才算真的恢复了。
    expected_stdout: str | None = None
    note: str = ""

    def render(self) -> str:
        return " ".join(self.argv)


@dataclass(frozen=True, slots=True)
class InjectionPlan:
    """一次故障注入的完整剧本。"""

    name: str
    family: str
    target: str
    apply: tuple[Command, ...]
    restore: tuple[Command, ...]
    duration: timedelta
    #: 复原后跑一次；退出码 0 == `restored_ok=True`。
    verify: Command | None = None
    note: str = ""

    def __post_init__(self) -> None:
        if self.duration <= timedelta(0):
            raise ValueError("duration 必须大于 0 —— 故障段为空的样本证明不了任何事")
        if not self.apply:
            raise ValueError("apply 不能为空")
        if not self.restore:
            raise ValueError("restore 不能为空 —— 每个注入都必须能撤回")

    def describe(self) -> list[str]:
        """给人看的执行计划（`--dry-run` 就打印它）。"""
        lines = [f"[inject] {self.name} → {self.target}，持续 {self.duration}"]
        lines += [f"  apply   {cmd.render()}" for cmd in self.apply]
        lines += [f"  restore {cmd.render()}" for cmd in self.restore]
        if self.verify is not None:
            lines.append(f"  verify  {self.verify.render()}")
        return lines


def _dd(container: str, *, count_mb: int, data_dir: str) -> Command:
    return Command(
        (
            "docker",
            "exec",
            container,
            "dd",
            "if=/dev/zero",
            f"of={data_dir}/{FILL_NAME}",
            "bs=1M",
            f"count={count_mb}",
            "status=none",
        ),
        # 128MB 的 tmpfs 放 200MB：dd 必然在中途收到 ENOSPC —— 这就是故障本身。
        expect_ok=False,
        note="写满数据盘（预期 ENOSPC）",
    )


def _df(container: str, *, data_dir: str) -> Command:
    return Command(("docker", "exec", container, "df", "-B1", data_dir), note="复原判据：df 能读")


def dn_volume_fill(
    *,
    container: str = "alcp-dn1",
    duration: timedelta = timedelta(minutes=10),
    count_mb: int = 200,
    data_dir: str = DEFAULT_DN_DATA_DIR,
) -> InjectionPlan:
    """`DN_VOLUME`：把 dn1 的 tmpfs 数据盘写满。"""
    return InjectionPlan(
        name="inject/dn_volume_fill",
        family="dn_volume",
        target=container,
        apply=(
            _dd(container, count_mb=count_mb, data_dir=data_dir),
            Command(
                ("docker", "exec", container, "df", "-B1", data_dir),
                expect_ok=False,
                note="只做记录",
            ),
        ),
        restore=(Command(("docker", "exec", container, "rm", "-f", f"{data_dir}/{FILL_NAME}")),),
        verify=_df(container, data_dir=data_dir),
        duration=duration,
        note="数据盘写满 → DiskOutOfSpaceException + volume failure",
    )


def dn_heartbeat_pause(
    *,
    container: str = "alcp-dn2",
    duration: timedelta = timedelta(minutes=10),
) -> InjectionPlan:
    """`DN_HEARTBEAT`：`docker pause`（SIGSTOP）。

    进程还在、日志不写、心跳不打 —— 这是最干净的"这台 DN 卡死了"。
    """
    return InjectionPlan(
        name="inject/dn_heartbeat_pause",
        family="dn_heartbeat",
        target=container,
        apply=(Command(("docker", "pause", container)),),
        restore=(Command(("docker", "unpause", container)),),
        verify=Command(
            ("docker", "inspect", "-f", "{{.State.Status}}", container), expected_stdout="running"
        ),
        duration=duration,
        note="SIGSTOP：进程在但心跳停",
    )


def dn_heartbeat_stop(
    *,
    container: str = "alcp-dn3",
    duration: timedelta = timedelta(minutes=10),
) -> InjectionPlan:
    """`DN_HEARTBEAT`：`docker stop` —— 进程没了，重启会重新注册。"""
    return InjectionPlan(
        name="inject/dn_heartbeat_stop",
        family="dn_heartbeat",
        target=container,
        apply=(Command(("docker", "stop", container)),),
        restore=(Command(("docker", "start", container)),),
        verify=Command(
            ("docker", "inspect", "-f", "{{.State.Status}}", container), expected_stdout="running"
        ),
        duration=duration,
        note="进程被杀：与 pause 的区别是重启会重新注册块",
    )


def nn_safemode_enter(
    *,
    container: str = "alcp-nn",
    duration: timedelta = timedelta(minutes=10),
) -> InjectionPlan:
    """`NN_SAFE_MODE`：让 NameNode 进安全模式（只能读，不能写）。"""
    return InjectionPlan(
        name="inject/nn_safemode_enter",
        family="nn_safemode",
        target=container,
        apply=(Command(("docker", "exec", container, "hdfs", "dfsadmin", "-safemode", "enter")),),
        restore=(Command(("docker", "exec", container, "hdfs", "dfsadmin", "-safemode", "leave")),),
        verify=Command(
            ("docker", "exec", container, "hdfs", "dfsadmin", "-safemode", "get"),
            expected_stdout="OFF",
        ),
        duration=duration,
        note="安全模式：写操作全被拒",
    )


def network_disconnect(
    *,
    container: str = "alcp-dn2",
    network: str = DEFAULT_NETWORK,
    duration: timedelta = timedelta(minutes=10),
) -> InjectionPlan:
    """`NETWORK_CONNECTIVITY`：把这个容器从 HDFS 网络上摘下来。

    用 `docker network disconnect` 而不是容器内 `iptables`：后者要 `NET_ADMIN`
    能力，而且要镜像里有 iptables；断网这个动作在宿主机侧就能做完，不需要任何特权。
    代价是断的是整台机器的网络（不只是 DN↔NN），但作为"网络不通"的样本仍然成立，
    已经在 `note` 里写明。
    """
    return InjectionPlan(
        name="inject/network_disconnect",
        family="network",
        target=container,
        apply=(Command(("docker", "network", "disconnect", network, container)),),
        restore=(Command(("docker", "network", "connect", network, container)),),
        verify=Command(
            (
                "docker",
                "network",
                "inspect",
                network,
                "--format",
                "{{range .Containers}}{{.Name}} {{end}}",
            ),
            expected_stdout=container,
        ),
        duration=duration,
        note="整机断网（不是单条链路）：网络不通的样本成立，但粒度比分链路粗",
    )


#: 家族 → 构造器。`--dry-run` 与采集编排都从这里取剧本。
INJECTIONS: Mapping[str, Callable[..., InjectionPlan]] = {
    "dn_volume": dn_volume_fill,
    "dn_heartbeat_pause": dn_heartbeat_pause,
    "dn_heartbeat_stop": dn_heartbeat_stop,
    "nn_safemode": nn_safemode_enter,
    "network": network_disconnect,
}


def _utc_now() -> datetime:
    return datetime.now(UTC)


class CommandInjection:
    """把 `InjectionPlan` 变成采集器能调用的 `InjectionHook`。"""

    def __init__(
        self,
        plan: InjectionPlan,
        *,
        runner: Runner = docker_runner,
        clock: Callable[[], datetime] = _utc_now,
        sleeper: Callable[[float], None] = time.sleep,
        dry_run: bool = False,
    ) -> None:
        self.plan = plan
        self._runner = runner
        self._clock = clock
        self._sleeper = sleeper
        self._dry_run = dry_run

    @property
    def name(self) -> str:
        return self.plan.name

    def __call__(self, flush: Callable[[], None]) -> FaultInjection:
        plan = self.plan
        if self._dry_run:
            for line in plan.describe():
                print(line)
            started = self._clock()
            return FaultInjection(
                script=plan.name, started=started, ended=started, restored=started
            )

        # ① 先把"注入之前采到的行"落盘：这是"注入前系统是好的"的唯一证据。
        flush()
        started = self._clock()
        failure: Exception | None = None
        try:
            self._run_all(plan.apply, phase="apply")
            # ② 故障段。真实的 10 分钟就是在这里睡的，不是采完再补时间戳。
            self._sleeper(plan.duration.total_seconds())
        except Exception as exc:  # noqa: BLE001 - 要保证 restore 一定被执行
            failure = exc
        ended = self._clock()
        # ③ 无论如何都要复原。
        restored_ok = False
        try:
            self._run_all(plan.restore, phase="restore")
            restored_ok = self._verify()
        except Exception as exc:  # noqa: BLE001 - 复原失败也要留下记录
            restored_ok = False
            failure = failure or exc
        restored = self._clock()
        if failure is not None:
            raise InjectionError(f"{plan.name} 执行失败：{failure}") from failure
        return FaultInjection(
            script=plan.name,
            started=started,
            ended=ended,
            restored=restored,
            restored_ok=restored_ok,
        )

    def _run_all(self, commands: Sequence[Command], *, phase: str) -> None:
        for command in commands:
            code, out = self._runner(command.argv)
            if command.expect_ok and code != 0:
                raise InjectionError(
                    f"{phase} 阶段 `{command.render()}` 退出码 {code}：{out.strip()[:300]}"
                )

    def _verify(self) -> bool:
        verify = self.plan.verify
        if verify is None:
            # 没有 verify 命令就没有证据，宁可记 False —— 见模块头第 3 条。
            return False
        code, out = self._runner(verify.argv)
        if code != 0:
            return False
        if verify.expected_stdout is None:
            return True
        return verify.expected_stdout in out
