# 顺序 5c · 注入编排：把"造一次故障"包成可复现、可回滚的一段
"""故障注入编排（P-A §4 的 inject 模式的实现）。

分工：

- 本模块**只描述命令**（`InjectionPlan`：apply 建故障、restore 复原、verify 验复原），
  并负责在正确时刻调用 `InjectionHook` 约定的 `flush`；
- `infra/hdfs/inject/*.sh` 是同一批命令的**人工版**（有人在终端里复现时用它们），
  不是采集链路的依赖 —— 采集链路不调 bash，免得 Windows 上没 bash 就跑不动。

三条硬规则：

1. **注入前必须 flush**。`flush()` 由采集器传进来，注入前调用；否则"注入之前系统是好的"
   这句话在盘上找不到证据。故障段里**每秒也调一次** `flush()` —— 采集器传进来的这个
   回调同时会采一拍指标，于是故障期间的指标是连续曲线，不是前后两个点。
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
    count_mb: int = 4096,
    data_dir: str = DEFAULT_DN_DATA_DIR,
) -> InjectionPlan:
    """`DN_VOLUME`：把 dn1 的 tmpfs 数据盘写满。

    `count_mb` 必须**大于盘的容量**（默认 1024 vs 512）：`dd` 会一直写到 ENOSPC，
    这才叫写满。原来写 200，盘升到 512 之后它只占 40%，故障根本没发生 ——
    白白采了 20 分钟"正常态"，还因为 `expect_ok=False` 把失败吞掉了。
    """
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
def dn_disk_broken(
    *,
    container: str = "alcp-dn2",
    volume: str = "aiops-log-copilot_dn2-data",
    duration: timedelta = timedelta(minutes=5),
    image: str = "apache/hadoop:3.3.6",
) -> InjectionPlan:
    """`DN_VOLUME` 的重症变体：把 `current/` 换成一个普通文件，DN 起不来。

    为什么需要它：**服务端日志里的 ERROR 只在 DN 致命失败时出现**。写满盘
    （`dn_volume_fill`）只会让 NN 反复说"副本放不下"，DN 自己不喊 —— 四族里
    三族因此永远拿不到 ERROR 样例。这个变体制造的是真实的启动失败：

        ERROR org.apache.hadoop.hdfs.server.datanode.DataNode:
            Exception in secureMain: java.io.IOException:
            File /tmp/hadoop-root/dfs/data/current exists but is not a directory

    dn1 用 tmpfs、没有命名卷，所以只能拿 dn2/dn3 做（卷在重建容器后仍然在，
    坏掉的 `current` 才留得住）。复原是"删掉那个文件、重启容器"，DN 会重新
    格式化该卷 —— 卷里的块丢失，集群先欠副本、再由另外两台补齐，这是可接受的
    实验代价（`replication=3` 保证不会真丢数据）。
    """
    return InjectionPlan(
        name="inject/dn_disk_broken",
        family="dn_volume",
        target=container,
        apply=(
            Command(("docker", "stop", container), note="先停容器，卷才空得出来"),
            Command(
                (
                    "docker",
                    "run",
                    "--rm",
                    "--user",
                    "0:0",
                    "--entrypoint",
                    "sh",
                    "-v",
                    f"{volume}:/d",
                    image,
                    "-c",
                    "rm -rf /d/current && echo broken > /d/current",
                ),
                note="把 current/ 换成普通文件：DN 启动时必然抛 IOException",
            ),
            Command(("docker", "start", container), note="启动后它会立刻退出，这才是故障"),
        ),
        restore=(
            Command(("docker", "stop", container), expect_ok=False, note="已经退出时这里是空操作"),
            Command(
                (
                    "docker",
                    "run",
                    "--rm",
                    "--user",
                    "0:0",
                    "--entrypoint",
                    "sh",
                    "-v",
                    f"{volume}:/d",
                    image,
                    "-c",
                    "rm -f /d/current",
                ),
            ),
            Command(("docker", "start", container)),
        ),
        verify=Command(
            ("docker", "inspect", "-f", "{{.State.Running}}", container), expected_stdout="true"
        ),
        duration=duration,
        note="数据目录不可用 → DN 启动失败 → 服务端 ERROR + NN 副本不足",
    )


INJECTIONS: Mapping[str, Callable[..., InjectionPlan]] = {
    "dn_volume": dn_volume_fill,
    "dn_disk_broken": dn_disk_broken,
    "dn_heartbeat_pause": dn_heartbeat_pause,
    "dn_heartbeat_stop": dn_heartbeat_stop,
    "nn_safemode": nn_safemode_enter,
    "network": network_disconnect,
}


def _utc_now() -> datetime:
    return datetime.now(UTC)


class CommandInjection:
    """把 `InjectionPlan` 变成采集器能调用的 `InjectionHook`。"""

    # 故障段里每几秒 flush 一次。见 `__call__` 里的注释。
    _FLUSH_EVERY = 5

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

        # ①.5 先做一次"复原"，把上一次没恢复干净的注入清掉。
        # 实采踩到过：一次采集被 Ctrl-C 打断，`lab.fill` 留在 dn1 上，
        # 下一次采集的 baseline 一起跳就是"盘 100%"，整段基线作废。
        # 这一步是**幂等**的 best-effort：删不存在的文件、唤醒没暂停的容器
        # 都返回非零，但都不该阻断采集。
        for command in plan.restore:
            self._runner(command.argv)

        started = self._clock()
        failure: Exception | None = None
        try:
            self._run_all(plan.apply, phase="apply")
            # ② 故障段。真实时长在这里等，不是"采完再补时间戳"。
            #    等待期间每秒调一次 flush：采集编排传给这里的 flush 同时
            #    会采一拍指标，于是 10 分钟的故障段里指标是一条连续曲线，
            #    而不是"注入前后各一个点"。日志说 DN 死了、指标说
            #    dead_datanodes 从 3 掉到 2，两边的时刻要对得上才行。
            #
            #    计时必须按**截止时刻**算，不能按"睡够多少次"算：`flush()`
            #    自身要花时间（drain + docker stats 每拍 3–5 秒），按次数算
            #    会让 5 分钟的故障段实际跑成 10 分钟，`ended` 越过窗口上界，
            #    回收段里落的其实还是故障态日志（2026-11-22 实采踩到）。
            deadline = started + plan.duration
            while True:
                remaining = (deadline - self._clock()).total_seconds()
                if remaining <= 0:
                    break
                self._sleeper(min(1.0, remaining))
                flush()
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
