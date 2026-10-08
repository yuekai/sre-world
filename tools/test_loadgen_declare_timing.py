"""Regression tests for declaration timing in the Helm loadgen.

Andre caught that the OracleAgent could apply the golden fix and declare during
warmup, causing the graded soak to start against a cold pool even though a real
diagnostic agent would normally declare after warmup. These tests keep that
boundary honest without needing Docker, Harbor, or live HTTP requests.

The episode clock starts at the episode-start signal. A declaration (or the
agent window elapsing) freezes the agent; the graded soak then starts on the
first cycle boundary at or after the freeze, never inside warmup, and always
runs its full configured duration. There is no declaration deadline and no
verifier-driven undeclared finalization.
"""

from __future__ import annotations

import asyncio
import sys
import types
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
LOADGEN_COMMON = ROOT / "loadgen-common"
if str(LOADGEN_COMMON) not in sys.path:
    sys.path.insert(0, str(LOADGEN_COMMON))

if "aiohttp" not in sys.modules:
    class _ClientTimeout:
        def __init__(self, *, total: float):
            self.total = total

    class _ClientSession:
        closed = False

        def __init__(self, *args, **kwargs):
            pass

        async def close(self) -> None:
            self.closed = True

    sys.modules["aiohttp"] = types.SimpleNamespace(
        ClientError=Exception,
        ClientSession=_ClientSession,
        ClientTimeout=_ClientTimeout,
    )

from loadgen.runner import LoadGen, agent_window_s, next_cycle_boundary_s  # noqa: E402
from loadgen.schedule import Profile  # noqa: E402


def _profile() -> Profile:
    # warmup 0.05 + three 0.1s cycles: schedule end 0.35, agent window 0.25
    # (one cycle short of the schedule end), cycle boundaries at 0.15/0.25/0.35.
    return Profile(
        name="unit",
        seed=1,
        warmup_s=0.05,
        warmup_rps=200.0,
        cycles=[(0.05, 200.0, 0.05, 200.0)] * 3,
        soak_cycles=1,
        declare_deadline_s=0.35,
    )


def _record_fires(monkeypatch) -> list[tuple[str, float, float]]:
    fired: list[tuple[str, float, float]] = []

    def fake_fire(self: LoadGen, phase: str, sched_s: float) -> None:
        assert self._t0 is not None
        fired.append((phase, sched_s, asyncio.get_running_loop().time() - self._t0))

    monkeypatch.setattr(LoadGen, "_fire", fake_fire)
    return fired


async def _start(lg: LoadGen) -> asyncio.Task:
    task = asyncio.create_task(lg.run())
    await asyncio.sleep(0)
    assert lg.signal_episode_start() is True
    return task


def test_profile_agent_window_is_one_cycle_short_of_schedule_end():
    profile = _profile()
    assert agent_window_s(profile) == pytest.approx(0.25)
    assert next_cycle_boundary_s(profile, 0.01) == profile.warmup_s
    assert next_cycle_boundary_s(profile, 0.075) == pytest.approx(0.15)


def test_declare_during_warmup_delays_soak_until_warmup_floor(tmp_path, monkeypatch):
    fired = _record_fires(monkeypatch)

    async def scenario() -> dict:
        lg = LoadGen(_profile(), tmp_path / "loadgen.jsonl")
        task = await _start(lg)
        await asyncio.sleep(0.012)
        lg.declare()
        summary = await asyncio.wait_for(task, timeout=2.0)

        assert lg.declare_ts_s is not None
        assert lg.declare_ts_s < lg.profile.warmup_s
        assert lg.freeze_reason == "declared"
        assert lg.soak_start_s == lg.profile.warmup_s
        assert lg.completion_reason == "declared_soak_complete"
        return summary

    summary = asyncio.run(scenario())
    assert summary["completion_reason"] == "declared_soak_complete"

    phases = [phase for phase, _sched, _sent in fired]
    pre_soak = [phase for phase in phases if not phase.startswith("soak")]
    assert pre_soak and all(phase == "warmup" for phase in pre_soak)
    first_soak = next(i for i, phase in enumerate(phases) if phase.startswith("soak"))
    assert all(phase.startswith("soak") for phase in phases[first_soak:])
    # The warmup clock stays honest: no soak arrival fires before warmup ends.
    assert all(
        sent >= 0.05 - 0.005 for phase, _sched, sent in fired if phase.startswith("soak")
    )


def test_declare_after_warmup_starts_soak_on_next_cycle_boundary(tmp_path, monkeypatch):
    fired = _record_fires(monkeypatch)

    async def scenario() -> LoadGen:
        lg = LoadGen(_profile(), tmp_path / "loadgen.jsonl")
        task = await _start(lg)
        await asyncio.sleep(0.075)
        lg.declare()
        await asyncio.wait_for(task, timeout=2.0)
        return lg

    lg = asyncio.run(scenario())
    assert lg.declare_ts_s is not None
    assert lg.declare_ts_s > lg.profile.warmup_s
    assert lg.freeze_ts_s is not None and lg.freeze_ts_s >= lg.declare_ts_s
    assert lg.soak_start_s == next_cycle_boundary_s(lg.profile, lg.freeze_ts_s)
    assert lg.soak_start_s >= lg.declare_ts_s
    assert lg.completion_reason == "declared_soak_complete"

    phases = [phase for phase, _sched, _sent in fired]
    assert any(phase.startswith("c1.") for phase in phases)
    first_soak = next(i for i, phase in enumerate(phases) if phase.startswith("soak"))
    assert all(phase.startswith("soak") for phase in phases[first_soak:])
    # Pre-soak load stays continuous up to, and never past, the soak boundary.
    assert all(
        sched < lg.soak_start_s for phase, sched, _ in fired if not phase.startswith("soak")
    )
    # Soak arrivals are rebased to the soak-start instant.
    assert all(
        sched >= lg.soak_start_s for phase, sched, _ in fired if phase.startswith("soak")
    )


def test_undeclared_episode_freezes_at_agent_window_and_still_soaks(tmp_path, monkeypatch):
    fired = _record_fires(monkeypatch)

    async def scenario() -> tuple[LoadGen, float]:
        lg = LoadGen(_profile(), tmp_path / "nop.jsonl")
        task = await _start(lg)
        started = asyncio.get_running_loop().time()
        await asyncio.wait_for(task, timeout=3.0)
        return lg, asyncio.get_running_loop().time() - started

    lg, elapsed = asyncio.run(scenario())
    window = agent_window_s(lg.profile)
    assert lg.declare_ts_s is None
    assert lg.freeze_reason == "window_elapsed"
    assert lg.freeze_ts_s is not None and lg.freeze_ts_s >= window - 0.01
    assert lg.soak_start_s == next_cycle_boundary_s(lg.profile, lg.freeze_ts_s)
    assert lg.completion_reason == "window_elapsed_soak_complete"
    assert any(phase.startswith("soak") for phase, _sched, _sent in fired)
    # The soak always runs its full configured duration.
    assert elapsed >= lg.soak_start_s + lg.profile.soak_duration_s() - 0.02


def test_begin_declaration_is_accepted_until_declared_or_finished(tmp_path, monkeypatch):
    _record_fires(monkeypatch)

    async def scenario() -> LoadGen:
        lg = LoadGen(_profile(), tmp_path / "loadgen.jsonl")
        task = await _start(lg)
        await asyncio.sleep(0.075)
        assert lg.begin_declaration() is True
        assert not task.done()
        lg.declare()
        assert lg.begin_declaration() is False
        await asyncio.wait_for(task, timeout=2.0)
        assert lg.begin_declaration() is False
        return lg

    lg = asyncio.run(scenario())
    assert lg.declare_ts_s is not None


def test_late_report_is_never_rejected_by_a_deadline(tmp_path, monkeypatch):
    """Declaring is an early-finish signal only: after the window froze the
    agent, a report is still recorded rather than refused for lateness."""
    _record_fires(monkeypatch)

    async def scenario() -> LoadGen:
        lg = LoadGen(_profile(), tmp_path / "late.jsonl")
        task = await _start(lg)
        await asyncio.sleep(agent_window_s(lg.profile) + 0.03)
        assert lg.freeze_reason == "window_elapsed"
        assert lg.begin_declaration() is True
        lg.declare()
        await asyncio.wait_for(task, timeout=2.0)
        return lg

    lg = asyncio.run(scenario())
    assert lg.declare_ts_s is not None
    assert lg.declare_ts_s > agent_window_s(lg.profile)
    assert lg.freeze_reason == "window_elapsed"


def test_hard_stop_ends_episode_without_claiming_completion(tmp_path, monkeypatch):
    _record_fires(monkeypatch)

    async def scenario() -> dict:
        lg = LoadGen(_profile(), tmp_path / "hard-stop.jsonl")
        task = await _start(lg)
        await asyncio.sleep(0.02)
        lg.stop()
        summary = await asyncio.wait_for(task, timeout=0.5)
        assert lg.completion_reason is None
        assert lg.declare_ts_s is None
        assert lg.begin_declaration() is False
        return summary

    summary = asyncio.run(scenario())
    assert summary["completion_reason"] is None
