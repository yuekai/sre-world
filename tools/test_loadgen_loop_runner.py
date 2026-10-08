"""Runner-level tests for loop mode (Profile.loop=True).

Mirrors tools/test_loadgen_declare_timing.py: no Docker/Harbor/live HTTP — the
real schedule generators drive a real LoadGen event loop with `_fire`
monkeypatched to record instead of sending.

What must hold:
- A declare mid-loop ends the repeating pre-soak window and fires the
  independent soak stream rebased to the soak-start instant.
- A never-declaring (nop) episode is frozen when its agent window elapses (one
  cycle short of declare_deadline_s, the loop's schedule end) and then runs the
  same full soak a declaring episode gets — loop mode removes the
  hand-enumerated schedule length, not the episode bound.
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


def _loop_profile() -> Profile:
    # ONE configured cycle: any c2+ label in the fired stream proves the loop
    # extended past the configured list. warmup 0.05 + 0.1s cycles, deadline
    # 0.35 => c1 (0.05-0.15), c2 (0.15-0.25), c3 (0.25-0.35).
    return Profile(
        name="unit_loop",
        seed=1,
        warmup_s=0.05,
        warmup_rps=200.0,
        cycles=[(0.05, 200.0, 0.05, 200.0)],
        soak_cycles=1,
        declare_deadline_s=0.35,
        loop=True,
    )


def _run_with_recording(profile: Profile, tmp_path, monkeypatch, declare_at_s: float | None):
    fired: list[tuple[str, float, float]] = []

    def fake_fire(self: LoadGen, phase: str, sched_s: float) -> None:
        assert self._t0 is not None
        fired.append((phase, sched_s, asyncio.get_running_loop().time() - self._t0))

    monkeypatch.setattr(LoadGen, "_fire", fake_fire)

    lg = LoadGen(profile, tmp_path / "loadgen.jsonl")

    async def scenario() -> None:
        task = asyncio.create_task(lg.run())
        await asyncio.sleep(0)
        assert lg.signal_episode_start() is True
        if declare_at_s is not None:
            await asyncio.sleep(declare_at_s)
            lg.declare()
        await asyncio.wait_for(task, timeout=5.0)

    asyncio.run(scenario())
    return lg, fired


def test_declare_mid_loop_switches_to_independent_soak(tmp_path, monkeypatch):
    lg, fired = _run_with_recording(_loop_profile(), tmp_path, monkeypatch, declare_at_s=0.22)

    assert lg.declare_ts_s is not None
    # Declared after the warmup floor: the soak starts on the next cycle boundary.
    assert lg.soak_start_s == next_cycle_boundary_s(lg.profile, lg.freeze_ts_s)
    assert lg.soak_start_s >= lg.declare_ts_s
    assert lg.completion_reason == "declared_soak_complete"

    phases = [phase for phase, _sched, _sent in fired]
    # The loop extended past the single configured cycle before the declare...
    assert any(p.startswith("c2.") for p in phases)
    # ...then the soak stream took over: nothing pre-soak fires after soak starts.
    first_soak = phases.index("soak.peak")
    assert all(p.startswith("soak") for p in phases[first_soak:])
    # Soak arrivals are rebased to the soak-start instant.
    soak_scheds = [sched for phase, sched, _sent in fired if phase.startswith("soak")]
    assert soak_scheds
    assert min(soak_scheds) >= lg.soak_start_s


def test_nop_episode_freezes_at_agent_window_then_soaks(tmp_path, monkeypatch):
    profile = _loop_profile()
    lg, fired = _run_with_recording(profile, tmp_path, monkeypatch, declare_at_s=None)

    window = agent_window_s(profile)
    assert window == pytest.approx(profile.declare_deadline_s - 0.1)  # one cycle short
    assert lg.declare_ts_s is None
    assert lg.freeze_reason == "window_elapsed"
    assert lg.freeze_ts_s is not None and lg.freeze_ts_s >= window - 0.01
    assert lg.soak_start_s == next_cycle_boundary_s(profile, lg.freeze_ts_s)
    assert lg.completion_reason == "window_elapsed_soak_complete"
    phases = [phase for phase, _sched, _sent in fired]
    # The loop kept repeating the single configured cycle up to the freeze...
    assert any(p.startswith("c2.") for p in phases)
    # ...and the full soak still ran after it.
    first_soak = phases.index("soak.peak")
    assert all(p.startswith("soak") for p in phases[first_soak:])
    assert all(
        sched < lg.soak_start_s for p, sched, _s in fired if not p.startswith("soak")
    )
