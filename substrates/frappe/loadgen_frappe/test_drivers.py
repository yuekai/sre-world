"""Unit tests for the Frappe drivers and load profiles (D16 Phase 2).

Run (BOTH substrates/slack-spine/ AND substrates/frappe/ must be on the path so the Frappe
drivers can import from the Slack scheduling core):

    PYTHONPATH=substrates/frappe:substrates/slack-spine:loadgen-common uv run \\
        --with pytest --with pytest-asyncio --with aiohttp \\
        python -m pytest substrates/frappe/loadgen_frappe/test_drivers.py -q

Deterministic; no network (all HTTP calls are mocked at the ClientSession seam).
"""

from __future__ import annotations

import asyncio
import collections
import json
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from loadgen.runner import DRIVERS, DriverResult
import loadgen_frappe.drivers as drivers_module
from loadgen_frappe.drivers import (
    DeskWorkDriver,
    DeskWriteDriver,
    RQEnqueueDriver,
    SessionPool,
    _SessionExpired,
)
from loadgen_frappe.schedule import PROFILES


# --------------------------------------------------------------------------- #
# Protocol shape
# --------------------------------------------------------------------------- #
def test_all_drivers_expose_name_op_target():
    for d in (DeskWorkDriver(SessionPool(size=1)),
              DeskWriteDriver(SessionPool(size=1)),
              RQEnqueueDriver(SessionPool(size=1))):
        assert isinstance(d.name, str) and d.name
        assert d.op in ("GET", "POST", "PUT", "DELETE"), d.op
        assert d.target.startswith("/")


def test_sessionpool_rejects_zero_size():
    with pytest.raises(ValueError):
        SessionPool(size=0)


def test_sessionpool_sid_for_round_robins():
    pool = SessionPool(size=4)
    # Manually stash sids to isolate the round-robin logic from network.
    pool._sids = ["sid-a", "sid-b", "sid-c", "sid-d"]
    assert pool.sid_for(0) == (0, "sid-a")
    assert pool.sid_for(1) == (1, "sid-b")
    assert pool.sid_for(4) == (0, "sid-a")
    assert pool.sid_for(5) == (1, "sid-b")


@pytest.mark.asyncio
async def test_sessionpool_provision_bounds_concurrency(monkeypatch):
    monkeypatch.setattr(drivers_module, "SESSION_PROVISION_CONCURRENCY", 2)
    pool = SessionPool(size=8)
    active = 0
    high_watermark = 0

    async def login(_session, slot):
        nonlocal active, high_watermark
        active += 1
        high_watermark = max(high_watermark, active)
        await asyncio.sleep(0)
        pool._sids[slot] = f"sid-{slot}"
        active -= 1

    monkeypatch.setattr(pool, "_login", login)
    await pool.provision(MagicMock())

    assert high_watermark == 2
    assert all(pool._sids)


@pytest.mark.asyncio
async def test_sessionpool_provision_fails_after_serial_retry(monkeypatch):
    monkeypatch.setattr(drivers_module, "SESSION_PROVISION_CONCURRENCY", 2)
    pool = SessionPool(size=3)

    async def login(_session, _slot):
        raise ConnectionError("database rejected login")

    monkeypatch.setattr(pool, "_login", login)
    with pytest.raises(RuntimeError, match="3/3 logins failed after serial retry"):
        await pool.provision(MagicMock())


@pytest.mark.asyncio
async def test_sessionpool_rejects_zero_provision_concurrency(monkeypatch):
    monkeypatch.setattr(drivers_module, "SESSION_PROVISION_CONCURRENCY", 0)
    with pytest.raises(ValueError, match="must be positive"):
        await SessionPool(size=1).provision(MagicMock())


# --------------------------------------------------------------------------- #
# Profile shape
# --------------------------------------------------------------------------- #
def test_frappe_dev_profile_is_registered():
    assert "frappe_dev" in PROFILES
    p = PROFILES["frappe_dev"]
    # Mirrors Slack `dev`: 30 s warmup + 2 × (20 s peak + 40 s trough) = 150 s
    # of configured schedule ⇒ declare_deadline_s = 150 (must equal schedule_end_s).
    assert p.declare_deadline_s == 150.0
    assert p.schedule_end_s() == 150.0
    assert p.drivers == ["desk_work", "desk_write_readback", "rq_enqueue"]
    assert p.soak_cycles == 1


def test_frappe_db_profile_excludes_independent_queue_surface():
    assert "frappe_db" in PROFILES
    p = PROFILES["frappe_db"]
    assert p.declare_deadline_s == 150.0
    assert p.schedule_end_s() == 150.0
    assert p.drivers == ["desk_work", "desk_write_readback"]
    assert p.soak_cycles == 1


def test_frappe_read_profile_is_read_only():
    assert "frappe_read" in PROFILES
    assert PROFILES["frappe_read"].drivers == ["desk_work"]


# --------------------------------------------------------------------------- #
# DeskWorkDriver — mocked-session request flow
# --------------------------------------------------------------------------- #
class _MockResponse:
    """Minimal aiohttp Response stand-in for driver tests."""
    def __init__(self, status: int, body: str = "") -> None:
        self.status = status
        self._body = body
    async def text(self) -> str:
        return self._body
    async def __aenter__(self):
        return self
    async def __aexit__(self, *_a):
        return False


def _mock_session(response: _MockResponse) -> MagicMock:
    """Build a mock ClientSession whose .request() returns ``response`` as an
    async context manager. Matches the aiohttp API surface our _do_request uses.
    """
    s = MagicMock()
    s.request = MagicMock(return_value=response)
    return s


def _loop_time_factory():
    """Monotonic-ish clock stub: returns 0.0, 0.001, 0.002, ..."""
    t = [0.0]
    def loop_time() -> float:
        t[0] += 0.001
        return t[0]
    return loop_time


@pytest.mark.asyncio
async def test_desk_work_driver_ok_on_200():
    pool = SessionPool(size=2)
    pool._sids = ["sid-a", "sid-b"]
    driver = DeskWorkDriver(pool)
    session = _mock_session(_MockResponse(200, '{"message":"Administrator"}'))
    result = await driver.request(
        session, seq=0, x="hello", loop_time=_loop_time_factory()
    )
    assert isinstance(result, DriverResult)
    assert result.ok is True
    assert result.status == 200
    assert result.correct is True   # 200 → correct=True per driver semantics
    assert result.timeout is False


@pytest.mark.asyncio
async def test_desk_work_driver_non_200_marks_not_ok():
    pool = SessionPool(size=2)
    pool._sids = ["sid-a", "sid-b"]
    driver = DeskWorkDriver(pool)
    session = _mock_session(_MockResponse(500, "internal error"))
    result = await driver.request(
        session, seq=1, x="hello", loop_time=_loop_time_factory()
    )
    assert result.ok is False
    assert result.status == 500
    assert result.correct is None


@pytest.mark.asyncio
async def test_desk_work_driver_401_raises_session_expired_after_reauth_fails():
    pool = SessionPool(size=2)
    pool._sids = ["sid-a", "sid-b"]
    # refresh_slot returns None ⇒ re-auth failed ⇒ _SessionExpired raised.
    pool.refresh_slot = AsyncMock(return_value=None)
    driver = DeskWorkDriver(pool)
    session = _mock_session(_MockResponse(401, "unauthorized"))
    with pytest.raises(_SessionExpired):
        await driver.request(
            session, seq=0, x="hello", loop_time=_loop_time_factory()
        )


# --------------------------------------------------------------------------- #
# RQEnqueueDriver — correctness = Prepared Report name present in body.
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_rq_enqueue_driver_correct_when_prepared_report_name_present():
    pool = SessionPool(size=1)
    pool._sids = ["sid-a"]
    driver = RQEnqueueDriver(pool)
    session = _mock_session(_MockResponse(200, '{"message":{"name":"abc-123"}}'))
    result = await driver.request(
        session, seq=0, x="ignored", loop_time=_loop_time_factory()
    )
    assert result.ok is True
    assert result.correct is True
    _, url = session.request.call_args.args[:2]
    assert url.endswith(".prepared_report.make_prepared_report")
    assert session.request.call_args.kwargs["params"] == {
        "report_name": "Database Storage Usage By Tables",
        "filters": "{}",
    }


@pytest.mark.asyncio
async def test_rq_enqueue_driver_not_correct_when_prepared_report_name_missing():
    pool = SessionPool(size=1)
    pool._sids = ["sid-a"]
    driver = RQEnqueueDriver(pool)
    session = _mock_session(_MockResponse(200, '{"message":{}}'))
    result = await driver.request(
        session, seq=0, x="ignored", loop_time=_loop_time_factory()
    )
    assert result.ok is True
    assert result.correct is False


# --------------------------------------------------------------------------- #
# DeskWriteDriver — write path issues TWO requests (POST + GET readback).
# We stub _do_request itself to sequence the two responses; simpler than a
# per-call MagicMock and it exercises the driver's flow (name-parse + readback
# correctness compare) rather than the aiohttp seam.
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_desk_write_driver_correct_when_readback_matches(monkeypatch):
    pool = SessionPool(size=1)
    pool._sids = ["sid-a"]
    driver = DeskWriteDriver(pool)
    call_state = {"n": 0}
    # Predict the description the driver will POST (has the seq + first 8 hex
    # chars of md5(x)) so the readback body echoes it back.
    import hashlib
    seq = 0; x = "readback-round-trip"
    desc = f"sre-world-loadgen-{seq}-{hashlib.md5(x.encode()).hexdigest()[:8]}"

    async def fake_do_request(method, url, session, sid, params=None, data=None, json_body=None):
        call_state["n"] += 1
        if call_state["n"] == 1:
            # POST /api/resource/ToDo → 200 with name
            assert method == "POST"
            return 200, json.dumps({"data": {"name": "TODO-0001", "description": desc}})
        # readback GET
        assert method == "GET"
        return 200, json.dumps({"data": {"description": desc}})

    monkeypatch.setattr("loadgen_frappe.drivers._do_request", fake_do_request)
    result = await driver.request(
        _mock_session(_MockResponse(200)), seq=seq, x=x, loop_time=_loop_time_factory()
    )
    assert result.ok is True
    assert result.correct is True


@pytest.mark.asyncio
async def test_desk_write_driver_incorrect_when_readback_desc_differs(monkeypatch):
    pool = SessionPool(size=1)
    pool._sids = ["sid-a"]
    driver = DeskWriteDriver(pool)
    call_state = {"n": 0}

    async def fake_do_request(method, url, session, sid, params=None, data=None, json_body=None):
        call_state["n"] += 1
        if call_state["n"] == 1:
            return 200, json.dumps({"data": {"name": "TODO-0002", "description": "as-posted"}})
        return 200, json.dumps({"data": {"description": "TAMPERED"}})

    monkeypatch.setattr("loadgen_frappe.drivers._do_request", fake_do_request)
    result = await driver.request(
        _mock_session(_MockResponse(200)), seq=1, x="does-not-matter",
        loop_time=_loop_time_factory()
    )
    assert result.ok is True
    assert result.correct is False


# --------------------------------------------------------------------------- #
# Registry integration — verify our monkey-patch pattern from the sidecar works.
# --------------------------------------------------------------------------- #
def test_registering_frappe_drivers_into_slack_registry():
    """Simulates what loadgen_sidecar._register_frappe_drivers does."""
    pool = SessionPool(size=1)
    pool._sids = ["sid-x"]
    DRIVERS[DeskWorkDriver.name] = DeskWorkDriver(pool)
    DRIVERS[DeskWriteDriver.name] = DeskWriteDriver(pool)
    DRIVERS[RQEnqueueDriver.name] = RQEnqueueDriver(pool)
    for name in ("desk_work", "desk_write_readback", "rq_enqueue"):
        assert name in DRIVERS
        assert DRIVERS[name].name == name


# --------------------------------------------------------------------------- #
# PreparedReportLedger + RQCompleteDriver (v19)
# --------------------------------------------------------------------------- #
from loadgen_frappe.drivers import (  # noqa: E402  (v19 additions)
    PreparedReportLedger,
    RQCompleteDriver,
    RQ_COMPLETE_BUDGET_S,
)


def test_ledger_fifo_eligibility_and_claim():
    led = PreparedReportLedger()
    led.append("r1", 0.0)
    led.append("r2", 1.0)
    # not aged yet
    assert led.pop_eligible(now=RQ_COMPLETE_BUDGET_S - 0.1) is None
    assert len(led) == 2
    # oldest ages in first; claim is FIFO and removes exactly one entry
    assert led.pop_eligible(now=RQ_COMPLETE_BUDGET_S + 0.0) == ("r1", 0.0)
    assert len(led) == 1
    assert led.pop_eligible(now=RQ_COMPLETE_BUDGET_S + 0.5) is None
    assert led.pop_eligible(now=RQ_COMPLETE_BUDGET_S + 1.0) == ("r2", 1.0)
    assert len(led) == 0
    # empty ledger is a None, never an exception
    assert led.pop_eligible(now=1e9) is None


def test_ledger_capacity_fails_loudly_instead_of_evicting_accepted_work():
    led = PreparedReportLedger(maxlen=8)
    for i in range(8):
        led.append(f"r{i}", float(i))
    assert len(led) == 8
    with pytest.raises(RuntimeError, match="refusing to evict accepted work"):
        led.append("r8", 8.0)
    # Overflow did not discard the oldest accepted identity.
    assert led.pop_eligible(now=1e9) == ("r0", 0.0)


@pytest.mark.asyncio
async def test_rq_enqueue_appends_to_shared_ledger():
    pool = SessionPool(size=1)
    pool._sids = ["sid-x"]
    led = PreparedReportLedger()
    driver = RQEnqueueDriver(pool, led)
    body = json.dumps({"message": {"name": "PR-0001"}})
    res = await driver.request(
        _mock_session(_MockResponse(200, body)),
        seq=0, x="x", loop_time=_loop_time_factory(),
    )
    assert res.ok and res.correct
    assert len(led) == 1
    assert led.pop_eligible(now=1e9) == ("PR-0001", pytest.approx(0.001, abs=0.01))


@pytest.mark.asyncio
async def test_rq_complete_correct_on_completed_status():
    pool = SessionPool(size=1)
    pool._sids = ["sid-x"]
    led = PreparedReportLedger()
    led.append("PR-0001", 0.0)
    driver = RQCompleteDriver(pool, led)
    body = json.dumps({"message": {"status": "Completed"}})
    # loop_time starts near 0.001 — make the entry eligible by backdating
    led._q[0] = ("PR-0001", -RQ_COMPLETE_BUDGET_S)
    res = await driver.request(
        _mock_session(_MockResponse(200, body)),
        seq=0, x="x", loop_time=_loop_time_factory(),
    )
    assert res.ok is True
    assert res.correct is True
    assert len(led) == 0  # claimed exactly once


@pytest.mark.asyncio
async def test_rq_complete_incorrect_on_nonterminal_or_failed_status():
    for st in ("Queued", "Started", "Error", "Failed"):
        pool = SessionPool(size=1)
        pool._sids = ["sid-x"]
        led = PreparedReportLedger()
        led.append("PR-0002", -RQ_COMPLETE_BUDGET_S)
        driver = RQCompleteDriver(pool, led)
        body = json.dumps({"message": {"status": st}})
        res = await driver.request(
            _mock_session(_MockResponse(200, body)),
            seq=0, x="x", loop_time=_loop_time_factory(),
        )
        assert res.ok is True
        assert res.correct is False, st


@pytest.mark.asyncio
async def test_slow_pre_soak_poll_is_retained_then_recovered_before_fresh_soak(
    monkeypatch,
):
    pool = SessionPool(size=1)
    pool._sids = ["sid-x"]
    led = PreparedReportLedger()
    led.append("PRE-1", -RQ_COMPLETE_BUDGET_S)
    led.append("PRE-2", -RQ_COMPLETE_BUDGET_S)
    driver = RQCompleteDriver(pool, led)
    responses = collections.deque(
        [
            (200, json.dumps({"message": {"status": "Queued"}})),
            (200, json.dumps({"message": {"status": "Completed"}})),
            (200, json.dumps({"message": {"status": "Completed"}})),
        ]
    )

    async def fake_do_request(_method, url, *_args, **_kwargs):
        if url == drivers_module.GET_COUNT_URL:
            return 200, json.dumps({"message": 0})
        return responses.popleft()

    monkeypatch.setattr(drivers_module, "_do_request", fake_do_request)
    clock = _loop_time_factory()
    pre_result = await driver.request(
        MagicMock(), seq=0, x="x", loop_time=clock
    )
    assert pre_result.correct is False
    assert led.pre_soak_pending == 2

    receipt = await driver.recover_pre_soak(
        MagicMock(), loop_time=clock, stall_s=1.0, max_s=1.0, backoff_s=0.001
    )
    assert receipt["pass"] is True
    assert receipt["accepted"] == receipt["verified_completed"] == 2
    assert receipt["remaining"] == 0

    # Post-transition enqueue/completion uses only the fresh soak partition.
    led.append("SOAK-1", -RQ_COMPLETE_BUDGET_S)
    assert led.pop_eligible(now=1.0) == ("SOAK-1", -RQ_COMPLETE_BUDGET_S)


@pytest.mark.asyncio
async def test_unrepaired_pre_soak_backlog_returns_graded_failure(monkeypatch):
    pool = SessionPool(size=1)
    pool._sids = ["sid-x"]
    led = PreparedReportLedger()
    led.append("PRE-STUCK", -RQ_COMPLETE_BUDGET_S)
    driver = RQCompleteDriver(pool, led)

    async def still_queued(_method, url, *_args, **_kwargs):
        if url == drivers_module.GET_COUNT_URL:
            return 200, json.dumps({"message": 7})
        return 200, json.dumps({"message": {"status": "Queued"}})

    monkeypatch.setattr(drivers_module, "_do_request", still_queued)
    receipt = await driver.recover_pre_soak(
        MagicMock(),
        loop_time=_loop_time_factory(),
        stall_s=0.004,
        max_s=1.0,
        backoff_s=0.001,
    )
    assert receipt["pass"] is False
    assert receipt["accepted"] == receipt["remaining"] == 1
    assert receipt["verified_completed"] == 0
    assert led.pre_soak_pending == 1
    assert led._phase == "soak"
    from verifier.providers.redis_state import _validate_backlog_recovery

    _validate_backlog_recovery(receipt, where="stalled worker")
    led.append("SOAK-1", 0.0)
    assert led.pop_eligible(now=RQ_COMPLETE_BUDGET_S + 1.0) == ("SOAK-1", 0.0)


@pytest.mark.asyncio
async def test_failed_accepted_report_returns_graded_failure(monkeypatch):
    pool = SessionPool(size=1)
    pool._sids = ["sid-x"]
    led = PreparedReportLedger()
    led.append("PRE-FAILED", -RQ_COMPLETE_BUDGET_S)
    driver = RQCompleteDriver(pool, led)

    async def failed_report(_method, url, *_args, **_kwargs):
        if url == drivers_module.GET_COUNT_URL:
            return 200, json.dumps({"message": 0})
        return 200, json.dumps({"message": {"status": "Failed"}})

    monkeypatch.setattr(drivers_module, "_do_request", failed_report)
    receipt = await driver.recover_pre_soak(
        MagicMock(),
        loop_time=_loop_time_factory(),
        stall_s=90.0,
        max_s=900.0,
        backoff_s=0.001,
    )

    assert receipt["pass"] is False
    assert receipt["accepted"] == receipt["remaining"] == 1
    assert receipt["verified_completed"] == 0
    assert receipt["poll_attempts"] == 1
    assert led.pre_soak_pending == 1
    assert led._phase == "soak"
    from verifier.providers.redis_state import _validate_backlog_recovery

    _validate_backlog_recovery(receipt, where="terminal failed report")


class _LifoLongQueue:
    """Virtual-time model of Frappe's starved long queue and one RQ worker.

    Newer reports sit ahead of the accepted ledger reports (at_front_when_
    starved); the worker completes one job per ``service_s`` of virtual time.
    Every HTTP request advances the virtual clock by ``request_s``.
    """

    def __init__(self, buried: list[str], ahead: int, service_s: float = 1.0):
        self.now = 0.0
        self.request_s = 0.05
        self.service_s = service_s
        self.queue = [f"NEWER-{i}" for i in range(ahead)] + list(buried)
        self.completed: set[str] = set()
        self.site_completed = 100
        self.worker_alive = True
        self._busy_until = service_s

    def clock(self) -> float:
        return self.now

    def _advance(self) -> None:
        self.now += self.request_s
        while self.worker_alive and self.queue and self.now >= self._busy_until:
            self.completed.add(self.queue.pop(0))
            self.site_completed += 1
            self._busy_until += self.service_s

    async def do_request(self, _method, url, *_args, params=None, **_kwargs):
        self._advance()
        filters = json.loads(params["filters"])
        if url == drivers_module.GET_COUNT_URL:
            if filters["status"] == "Completed":
                return 200, json.dumps({"message": self.site_completed})
            return 200, json.dumps({"message": len(self.queue)})
        status = "Completed" if filters["name"] in self.completed else "Queued"
        return 200, json.dumps({"message": {"status": status}})


def _buried_recovery(monkeypatch, sim: _LifoLongQueue, names: list[str]):
    pool = SessionPool(size=1)
    pool._sids = ["sid-x"]
    led = PreparedReportLedger()
    for name in names:
        led.append(name, -RQ_COMPLETE_BUDGET_S)
    monkeypatch.setattr(drivers_module, "_do_request", sim.do_request)
    return led, RQCompleteDriver(pool, led)


@pytest.mark.asyncio
async def test_recovery_waits_for_buried_reports_while_worker_drains(monkeypatch):
    # Two accepted reports behind 300 newer jobs at 1 job/s: the fixed 90s
    # window failed this healthy, draining queue.
    names = ["PRE-OLD-1", "PRE-OLD-2"]
    sim = _LifoLongQueue(names, ahead=300)
    led, driver = _buried_recovery(monkeypatch, sim, names)

    receipt = await driver.recover_pre_soak(
        MagicMock(), loop_time=sim.clock, stall_s=90, max_s=900, backoff_s=1e-6
    )

    assert receipt["pass"] is True
    assert receipt["verified_completed"] == receipt["accepted"] == 2
    assert 300 < receipt["duration_s"] < 310
    assert sim.site_completed - 100 >= 302
    assert led.pre_soak_pending == 0


@pytest.mark.asyncio
async def test_recovery_grades_failure_when_worker_stops(monkeypatch):
    names = ["PRE-OLD-1"]
    sim = _LifoLongQueue(names, ahead=300)
    sim.worker_alive = False
    led, driver = _buried_recovery(monkeypatch, sim, names)

    receipt = await driver.recover_pre_soak(
        MagicMock(), loop_time=sim.clock, stall_s=90, max_s=900, backoff_s=1e-6
    )
    assert receipt["pass"] is False
    assert receipt["remaining"] == 1
    assert 90 <= sim.now < 92
    assert led.pre_soak_pending == 1


@pytest.mark.asyncio
async def test_recovery_grades_stranded_report_after_worker_goes_idle(monkeypatch):
    # The worker drains everything it holds, but the accepted report has no
    # job: progress stops, so the stall window fails it.
    sim = _LifoLongQueue([], ahead=50)
    led, driver = _buried_recovery(monkeypatch, sim, ["PRE-STRANDED"])

    receipt = await driver.recover_pre_soak(
        MagicMock(), loop_time=sim.clock, stall_s=90, max_s=900, backoff_s=1e-6
    )
    assert receipt["pass"] is False
    assert receipt["remaining"] == 1
    assert 140 <= sim.now < 142


@pytest.mark.asyncio
async def test_recovery_ceiling_bounds_endless_progress(monkeypatch):
    names = ["PRE-OLD-1"]
    sim = _LifoLongQueue(names, ahead=5000)
    led, driver = _buried_recovery(monkeypatch, sim, names)

    with pytest.raises(TimeoutError, match="exceeded ceiling.*pending=1/1"):
        await driver.recover_pre_soak(
            MagicMock(), loop_time=sim.clock, stall_s=90, max_s=900, backoff_s=1e-6
        )
    assert 900 <= sim.now < 901


@pytest.mark.asyncio
async def test_unreadable_progress_probe_cannot_extend_recovery(monkeypatch):
    names = ["PRE-OLD-1"]
    sim = _LifoLongQueue(names, ahead=300)
    led, driver = _buried_recovery(monkeypatch, sim, names)
    real = sim.do_request

    async def count_unavailable(method, url, *args, **kwargs):
        if url == drivers_module.GET_COUNT_URL:
            sim._advance()
            return 500, "{}"
        return await real(method, url, *args, **kwargs)

    monkeypatch.setattr(drivers_module, "_do_request", count_unavailable)
    receipt = await driver.recover_pre_soak(
        MagicMock(), loop_time=sim.clock, stall_s=90, max_s=900, backoff_s=1e-6
    )
    assert receipt["pass"] is False
    assert receipt["remaining"] == 1
    assert 90 <= sim.now < 92


@pytest.mark.asyncio
async def test_rq_complete_sentinel_on_empty_ledger():
    pool = SessionPool(size=1)
    pool._sids = ["sid-x"]
    driver = RQCompleteDriver(pool, PreparedReportLedger())
    session = _mock_session(_MockResponse(200, "{}"))
    res = await driver.request(
        session, seq=0, x="x", loop_time=_loop_time_factory(),
    )
    assert res.status == 0 and res.ok is False and res.correct is None
    session.request.assert_not_called()  # no HTTP without a claimed entry


def test_registering_v19_drivers_shares_one_ledger_object():
    """Mirrors _register_frappe_drivers: producer and consumer MUST share."""
    pool = SessionPool(size=1)
    pool._sids = ["sid-x"]
    led = PreparedReportLedger()
    DRIVERS[RQEnqueueDriver.name] = RQEnqueueDriver(pool, led)
    DRIVERS[RQCompleteDriver.name] = RQCompleteDriver(pool, led)
    assert DRIVERS["rq_enqueue"].ledger is DRIVERS["rq_complete"].ledger


def test_frappe_jobs_profile_shape():
    assert "frappe_jobs" in PROFILES
    p = PROFILES["frappe_jobs"]
    # 2/4 desk weighting, no independent write surface, completion consumer in
    assert p.drivers == ["desk_work", "desk_work", "rq_enqueue", "rq_complete"]
    # deliberately cold: peak 8 rps (2 report creations/s vs ~1/s worker rate)
    assert p.warmup_rps == 4
    assert [c[1] for c in p.cycles] == [8, 8]
    assert p.declare_deadline_s == 150.0
    assert p.schedule_end_s() == 150.0
