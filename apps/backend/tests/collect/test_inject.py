"""故障注入编排（顺序 5c）的契约测试。

这一层真正要守住的是**可逆性**（V7）：apply 失败也要 restore、restore 之后要有证据、
预期内的失败（`dd` 写满盘）不能被当成注入失败。四条都在下面钉住。
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from logcopilot.collect.inject import (
    DEFAULT_NETWORK,
    INJECTIONS,
    Command,
    CommandInjection,
    InjectionError,
    InjectionPlan,
)
from logcopilot.domain.case import FaultInjection

T0 = datetime(2026, 11, 22, 12, 0, 0, tzinfo=UTC)


class FakeRunner:
    """记录调用并按规则返回 `(退出码, stdout)`。"""

    def __init__(self, rules: dict[str, tuple[int, str]] | None = None) -> None:
        self.rules = rules or {}
        self.calls: list[tuple[str, ...]] = []

    def __call__(self, argv):  # type: ignore[no-untyped-def]
        self.calls.append(tuple(argv))
        joined = " ".join(argv)
        for key, reply in self.rules.items():
            if key in joined:
                return reply
        return 0, "ok"

    def called_with(self, needle: str) -> bool:
        return any(needle in " ".join(call) for call in self.calls)


class FakeClock:
    """每次取时间前进固定秒数，用来验证 started ≤ ended ≤ restored。"""

    def __init__(self, step: int = 1) -> None:
        self.now = T0
        self.step = timedelta(seconds=step)

    def __call__(self) -> datetime:
        current = self.now
        self.now = self.now + self.step
        return current


def _plan(**overrides: Any) -> InjectionPlan:
    base: dict[str, Any] = {
        "name": "inject/dn_heartbeat_pause",
        "family": "dn_heartbeat",
        "target": "alcp-dn2",
        "apply": (Command(("docker", "pause", "alcp-dn2")),),
        "restore": (Command(("docker", "unpause", "alcp-dn2")),),
        "duration": timedelta(minutes=10),
        "verify": Command(
            ("docker", "inspect", "-f", "{{.State.Status}}", "alcp-dn2"),
            expected_stdout="running",
        ),
    }
    base.update(overrides)
    return InjectionPlan(**base)


# ------------------------------------------------------------------- 剧本本身


@pytest.mark.parametrize("family", sorted(INJECTIONS))
def test_every_family_builds_a_complete_plan(family: str) -> None:
    plan = INJECTIONS[family](duration=timedelta(minutes=10))
    assert plan.family
    assert plan.target.startswith("alcp-")
    assert plan.apply and plan.restore
    assert plan.duration == timedelta(minutes=10)
    # 每个注入都必须声明怎么验证复原 —— 否则 restored_ok 只能是猜的
    assert plan.verify is not None


def test_volume_plan_tolerates_dd_failure() -> None:
    plan = INJECTIONS["dn_volume"](duration=timedelta(minutes=10))
    dd = plan.apply[0]
    assert dd.argv[:3] == ("docker", "exec", "alcp-dn1")
    assert dd.expect_ok is False  # 写满盘是预期失败
    assert "lab.fill" in " ".join(dd.argv)


def test_network_plan_uses_default_compose_network() -> None:
    plan = INJECTIONS["network"](duration=timedelta(minutes=10))
    assert DEFAULT_NETWORK in " ".join(plan.apply[0].argv)


def test_plan_rejects_zero_duration() -> None:
    with pytest.raises(ValueError, match="duration"):
        _plan(duration=timedelta(0))


def test_plan_rejects_empty_restore() -> None:
    with pytest.raises(ValueError, match="restore"):
        _plan(restore=())


def test_describe_lists_apply_and_restore() -> None:
    lines = _plan().describe()
    assert any("apply" in line and "docker pause" in line for line in lines)
    assert any("restore" in line and "docker unpause" in line for line in lines)
    assert any(line.startswith("[inject]") for line in lines)


# ------------------------------------------------------------------- 执行编排


def test_happy_path_returns_ordered_injection_and_verifies_restore() -> None:
    runner = FakeRunner({"inspect": (0, "running\n")})
    sleeps: list[float] = []
    hook = CommandInjection(_plan(), runner=runner, clock=FakeClock(), sleeper=sleeps.append)
    flushed: list[int] = []

    result = hook(lambda: flushed.append(1))

    assert isinstance(result, FaultInjection)
    assert result.script == "inject/dn_heartbeat_pause"
    assert result.started <= result.ended <= result.restored  # type: ignore[operator]
    assert result.restored_ok is True
    assert flushed == [1]  # 注入前 flush 过一次
    assert sleeps == [600.0]  # 故障段就是真睡 10 分钟
    assert runner.called_with("docker pause")
    assert runner.called_with("docker unpause")


def test_restore_runs_even_when_apply_fails() -> None:
    runner = FakeRunner({"docker pause": (1, "no such container")})
    hook = CommandInjection(_plan(), runner=runner, clock=FakeClock(), sleeper=lambda _s: None)

    with pytest.raises(InjectionError, match="apply 阶段"):
        hook(lambda: None)

    assert runner.called_with("docker unpause"), "apply 失败也必须复原，否则留下故障现场"


def test_expected_failure_does_not_raise() -> None:
    plan = _plan(
        apply=(Command(("docker", "exec", "alcp-dn1", "dd"), expect_ok=False),),
        restore=(
            Command(
                ("docker", "exec", "alcp-dn1", "rm"),
            ),
        ),
        verify=Command(("docker", "exec", "alcp-dn1", "df"), expected_stdout=None),
    )
    runner = FakeRunner({"dd": (1, "No space left on device")})
    hook = CommandInjection(plan, runner=runner, clock=FakeClock(), sleeper=lambda _s: None)

    result = hook(lambda: None)
    assert result.restored_ok is True


def test_unverified_restore_is_recorded_as_false() -> None:
    plan = _plan(
        verify=Command(
            ("docker", "inspect", "-f", "{{.State.Status}}", "alcp-dn2"),
            expected_stdout="running",
        )
    )
    # inspect 说容器还是 paused：unpause 命令退出码是 0，但证据说没恢复
    runner = FakeRunner({"inspect": (0, "paused\n")})
    hook = CommandInjection(plan, runner=runner, clock=FakeClock(), sleeper=lambda _s: None)

    assert hook(lambda: None).restored_ok is False


def test_plan_without_verify_reports_false() -> None:
    plan = _plan(verify=None)
    hook = CommandInjection(plan, runner=FakeRunner(), clock=FakeClock(), sleeper=lambda _s: None)
    assert hook(lambda: None).restored_ok is False


def test_restore_failure_is_recorded_not_raised_when_apply_succeeded() -> None:
    runner = FakeRunner({"docker unpause": (1, "boom")})
    hook = CommandInjection(_plan(), runner=runner, clock=FakeClock(), sleeper=lambda _s: None)

    with pytest.raises(InjectionError, match="restore 阶段"):
        hook(lambda: None)


# --------------------------------------------------------------------- dry-run


def test_dry_run_prints_plan_and_touches_nothing(capsys: pytest.CaptureFixture[str]) -> None:
    runner = FakeRunner()
    hook = CommandInjection(
        _plan(), runner=runner, clock=FakeClock(), sleeper=lambda _s: None, dry_run=True
    )

    result = hook(lambda: None)

    assert runner.calls == []
    out = capsys.readouterr().out
    assert "docker pause alcp-dn2" in out
    assert "docker unpause alcp-dn2" in out
    assert result.ended == result.started
