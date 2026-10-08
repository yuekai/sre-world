"""Frappe-substrate loadgen drivers (D16 Phase 2).

Plugs into the Slack substrate's ``loadgen.runner`` Driver protocol (peer of
``substrate/loadgen/runner.py`` ``WorkDriver`` etc.). Every driver exposes:

  - ``name``   — labels the driver in the per-arrival JSONL record
  - ``op``     — HTTP verb
  - ``target`` — endpoint path (constant per driver)
  - ``async request(session, seq, x, loop_time, ...)`` → ``DriverResult``

Frappe-specific concern: every Desk API call requires an authenticated session
(``sid`` cookie). Frappe does NOT support anonymous access to the API surface
we exercise. The :class:`SessionPool` pre-provisions N sids at loadgen startup
via ``POST /api/method/login {usr, pwd}``; drivers round-robin through the pool
via the arrival's monotonically increasing ``seq``. If a session expires
mid-episode the pool re-authenticates that slot lazily; the arrival that saw
the 401 is recorded with ``status: "session_expired"`` — a new terminal state
we extend the sidecar's STATUS_KEYS to include.

Endpoints exercised (all under ``LOADGEN_TARGET_BASE_URL``, e.g.
``http://svc-frappe-web:8000``):

  - ``GET /api/method/frappe.auth.get_logged_user`` — cheap session check;
    correctness = body ``.message`` equals the pool's login user.
  - ``GET /api/resource/DocType?limit_page_length=50`` — list view read; ok
    on 200.
  - ``GET /api/method/frappe.client.get_list?doctype=ToDo&limit=20`` — mixed
    filtering read; ok on 200.
  - ``POST /api/resource/ToDo {description: ...}`` + ``GET /api/resource/ToDo/<name>``
    (readback) — write path; correctness = readback returns the created row.
  - ``POST /api/method/frappe.core.doctype.prepared_report.prepared_report.make_prepared_report``
    — supported Desk API which creates a Prepared Report and enqueues its
    generation through the document's ``after_insert`` hook; correctness =
    ``.message.name`` non-empty.
"""

from __future__ import annotations

import asyncio
import collections
import hashlib
import json
import logging
import os
import random
from typing import Any

import aiohttp

logger = logging.getLogger(__name__)

# Reuse the Slack scheduler's env-derived target base + the shared exceptions.
# Both substrates use LOADGEN_TARGET_BASE_URL as the single knob (chart wires
# `TARGET` -> `LOADGEN_TARGET_BASE_URL` in both loadgen sidecars).
_TARGET_BASE = os.environ.get(
    "LOADGEN_TARGET_BASE_URL", "http://svc-frappe-web:8000"
).rstrip("/")

LOGIN_URL          = f"{_TARGET_BASE}/api/method/login"
LOGGED_USER_URL    = f"{_TARGET_BASE}/api/method/frappe.auth.get_logged_user"
DOCTYPE_LIST_URL   = f"{_TARGET_BASE}/api/resource/DocType"
GET_LIST_URL       = f"{_TARGET_BASE}/api/method/frappe.client.get_list"
GET_VALUE_URL      = f"{_TARGET_BASE}/api/method/frappe.client.get_value"
GET_COUNT_URL      = f"{_TARGET_BASE}/api/method/frappe.client.get_count"
TODO_RESOURCE_URL  = f"{_TARGET_BASE}/api/resource/ToDo"
ENQUEUE_URL        = (
    f"{_TARGET_BASE}/api/method/frappe.core.doctype.prepared_report."
    "prepared_report.make_prepared_report"
)

# Login credentials come from the loadgen container env (chart injects them
# from a Secret, mirroring how the createSite job sets ADMIN_PASSWORD).
LOGIN_USR = os.environ.get("FRAPPE_LOGIN_USER", "Administrator")
LOGIN_PWD = os.environ.get("FRAPPE_LOGIN_PWD", "admin")
SESSION_POOL_SIZE = int(os.environ.get("FRAPPE_SESSION_POOL_SIZE", "32"))
# rq_complete grades "the job actually finished": a poll arrival claims the
# oldest Prepared Report older than this budget and asks for its status. The
# budget encodes "completed WITHIN budget" via ledger eligibility age; there is
# no in-driver polling loop, so the open-loop schedule is undisturbed.
# Finalized by the v19 C2 kind measurement (run 32799442781): healthy
# completion age is sub-second (62/62 aged polls returned Completed), so 15s
# keeps >10x margin while shrinking the peak-onset sentinel artifact (polls at
# peak rate vs supply aged from trough-rate production 25s earlier measured
# 61.8% ok at peak with a 25s budget). Must stay under warmup_s (30).
RQ_COMPLETE_BUDGET_S = float(os.environ.get("RQ_COMPLETE_BUDGET_S", "15"))
# Pre-soak recovery waits for every accepted report, but only while the long
# worker is visibly draining. Frappe enqueues Prepared Reports with
# at_front_when_starved=True: once the long queue exceeds 16 jobs new reports
# jump to the FRONT, so the oldest accepted reports sit behind a backlog of
# newer ones until arrivals stop. Their own status therefore cannot measure
# progress; the site-wide Completed count can. Recovery fails when that count
# stops moving for RQ_RECOVERY_STALL_S (stranded/lost jobs or a dead worker),
# or at the RQ_RECOVERY_MAX_S ceiling. The ceiling covers a full Frappe queue
# (MAX_QUEUED_JOBS 500 + 50 per site) drained by the single long worker.
RQ_RECOVERY_STALL_S = float(
    os.environ.get(
        "RQ_RECOVERY_STALL_S", os.environ.get("RQ_RECOVERY_TIMEOUT_S", "90")
    )
)
RQ_RECOVERY_MAX_S = float(os.environ.get("RQ_RECOVERY_MAX_S", "900"))
# One sweep over the pending reports per backoff: the single long worker
# finishes at most about one job per second, so faster sweeps only load the web
# tier (0.25s produced ~80 polls/s against two buried reports).
RQ_RECOVERY_BACKOFF_S = float(os.environ.get("RQ_RECOVERY_BACKOFF_S", "1.0"))
# Provisioning every session simultaneously can self-DoS the intentionally low
# connection-cap fault before the steady load begins. Bound the setup burst and
# retry any missed slots serially; terminal authentication failures still abort.
SESSION_PROVISION_CONCURRENCY = int(
    os.environ.get("FRAPPE_SESSION_PROVISION_CONCURRENCY", "4")
)
# Random-doctype rotation: keep the DocType-list surface hot without pinning it
# to one row (Frappe's ORM caches list queries per (user, filters) key).
_DOCTYPE_FILTERS = ("Standard", "Setup", "Core", "Contacts", "Communication")


# --------------------------------------------------------------------------- #
# SessionPool
# --------------------------------------------------------------------------- #
class SessionPool:
    """Pre-provisioned pool of authenticated Frappe sessions.

    Login is a heavy round-trip (Frappe hashes the pwd and hits the DB) — doing
    it inline on every arrival would dwarf the driver's target request. We
    front-load N logins at loadgen startup and round-robin the resulting sids
    across arrivals via ``sid_for(seq)``.

    Thread-safety: not needed — every arrival runs as a fire-and-forget task on
    the single asyncio loop, and list index access is atomic. The single lock
    below is only held during a lazy re-auth on session expiry.
    """

    def __init__(self, size: int = SESSION_POOL_SIZE) -> None:
        if size <= 0:
            raise ValueError(f"SESSION_POOL_SIZE must be positive, got {size}")
        self._size = size
        # ``None`` means "not yet authenticated"; filled by ``provision()``. Slot
        # can also revert to ``None`` on session expiry, then re-provisioned
        # inline by ``refresh_slot``.
        self._sids: list[str | None] = [None] * size
        self._locks: list[asyncio.Lock] = [asyncio.Lock() for _ in range(size)]

    async def provision(self, session: aiohttp.ClientSession) -> None:
        """Authenticate every slot without creating an unbounded login burst."""
        if SESSION_PROVISION_CONCURRENCY <= 0:
            raise ValueError(
                "FRAPPE_SESSION_PROVISION_CONCURRENCY must be positive, got "
                f"{SESSION_PROVISION_CONCURRENCY}"
            )
        semaphore = asyncio.Semaphore(SESSION_PROVISION_CONCURRENCY)

        async def guarded_login(slot: int) -> None:
            async with semaphore:
                await self._login(session, slot)

        await asyncio.gather(
            *[guarded_login(slot) for slot in range(self._size)],
            return_exceptions=True,
        )

        # Tight caps can reject a bounded setup request transiently. Retry only
        # unfilled slots one at a time, then fail loudly if any remain.
        last_error: Exception | None = None
        for slot, sid in enumerate(self._sids):
            if sid is not None:
                continue
            try:
                await self._login(session, slot)
            except Exception as exc:  # noqa: BLE001 -- reported below
                last_error = exc
        unfilled = [slot for slot, sid in enumerate(self._sids) if sid is None]
        if unfilled:
            raise RuntimeError(
                f"SessionPool.provision: {len(unfilled)}/{self._size} logins "
                f"failed after serial retry: {last_error!r}"
            )

    async def _login(self, session: aiohttp.ClientSession, slot: int) -> None:
        """POST /api/method/login and stash the resulting sid at ``slot``."""
        async with session.post(
            LOGIN_URL, data={"usr": LOGIN_USR, "pwd": LOGIN_PWD}
        ) as resp:
            resp.raise_for_status()
            # Frappe sets `sid` in the response cookies. Extract it explicitly so
            # a shared aiohttp CookieJar (which would clobber sids across slots)
            # doesn't matter.
            sid = None
            for c in resp.cookies.values():
                if c.key == "sid":
                    sid = c.value
                    break
            if not sid:
                # Fall back: Frappe's session middleware always writes sid, so
                # this is a hard failure (probably a login schema error).
                text = await resp.text()
                raise RuntimeError(f"login: no sid in response: {text[:200]}")
            self._sids[slot] = sid

    def sid_for(self, seq: int) -> tuple[int, str | None]:
        """Return ``(slot, sid)`` for a given arrival. sid may be None if the
        pool has an unauthenticated slot (see refresh_slot).
        """
        slot = seq % self._size
        return slot, self._sids[slot]

    async def refresh_slot(self, session: aiohttp.ClientSession, slot: int) -> str | None:
        """Re-authenticate one slot after a 401 mid-episode. Returns the fresh sid
        or None on failure (the caller records the arrival as session_expired).
        """
        async with self._locks[slot]:
            self._sids[slot] = None
            try:
                await self._login(session, slot)
                return self._sids[slot]
            except Exception:
                return None


# --------------------------------------------------------------------------- #
# Sentinels used by the runner's outer catch to categorise arrivals.
# --------------------------------------------------------------------------- #
class _SessionExpired(Exception):
    """Raised when a driver saw HTTP 401 mid-episode and re-auth failed. The
    runner records ``status: "session_expired"`` (a new terminal state — see the
    Frappe sidecar's parse_metrics fork)."""

    def __init__(self, latency_ms: float) -> None:
        self.latency_ms = latency_ms


# --------------------------------------------------------------------------- #
# Common helpers
# --------------------------------------------------------------------------- #
async def _do_request(
    method: str,
    url: str,
    session: aiohttp.ClientSession,
    sid: str | None,
    *,
    params: dict[str, Any] | None = None,
    data: Any | None = None,
    json_body: Any | None = None,
) -> tuple[int, str]:
    """Fire one HTTP request with the Frappe sid cookie explicitly attached.

    Aiohttp's CookieJar can't distinguish sids across our pool of sessions in a
    single ClientSession, so we bypass it by passing the cookie via a per-call
    ``cookies={"sid": sid}`` override. This keeps the ClientSession shared
    (connection pooling wins) while each request carries its own sid.
    """
    cookies = {"sid": sid} if sid else {}
    async with session.request(
        method, url, cookies=cookies, params=params, data=data, json=json_body
    ) as resp:
        return resp.status, await resp.text()


# --------------------------------------------------------------------------- #
# DeskWorkDriver — read-heavy analogue of Slack's WorkDriver.
# --------------------------------------------------------------------------- #
class DeskWorkDriver:
    """Alternates between three cheap Desk API reads per arrival.

    Correctness = HTTP 200 (the endpoints have no scalar oracle like md5(x);
    Frappe's dev site returns non-deterministic list bodies). The runner's
    goodput/error-rate/latency gates work off ``ok`` + ``status``, so this
    matches the shape ``substrate/loadgen_grader_common.py`` expects.
    """

    name = "desk_work"
    op = "GET"
    target = "/api/method/*"

    def __init__(self, pool: SessionPool | None = None) -> None:
        self.pool = pool or SessionPool()

    async def request(
        self,
        session: aiohttp.ClientSession,
        *,
        seq: int,
        x: str,
        loop_time: Any,
        channel_keyspace: int = 8,
        plan: Any | None = None,
    ) -> "DriverResult":
        from loadgen.runner import DriverResult, _DriverClientError, _DriverTimeout

        slot, sid = self.pool.sid_for(seq)
        # Rotate across the three read endpoints deterministically by seq.
        which = seq % 3
        if which == 0:
            url, params = LOGGED_USER_URL, None
        elif which == 1:
            url, params = DOCTYPE_LIST_URL, {"limit_page_length": "50"}
        else:
            url, params = GET_LIST_URL, {
                "doctype": "ToDo",
                "limit_page_length": "20",
                "filters": f'[["ToDo","status","=","{random.choice(_DOCTYPE_FILTERS)}"]]',
            }

        t_send = loop_time()
        try:
            status, _body = await _do_request(
                "GET", url, session, sid, params=params
            )
            latency_ms = (loop_time() - t_send) * 1000.0
        except asyncio.TimeoutError:
            raise _DriverTimeout((loop_time() - t_send) * 1000.0) from None
        except aiohttp.ClientError as exc:
            raise _DriverClientError((loop_time() - t_send) * 1000.0, exc) from None

        if status == 401 and sid is not None:
            # Session expired mid-episode. Try one re-auth then re-raise as
            # session_expired if that also fails.
            fresh = await self.pool.refresh_slot(session, slot)
            if fresh is None:
                raise _SessionExpired(latency_ms)
            # Optionally retry with the fresh sid — for now we record the
            # arrival as session_expired to keep the driver deterministic
            # (retries confound the open-loop schedule).
            raise _SessionExpired(latency_ms)

        return DriverResult(
            status=status,
            latency_ms=latency_ms,
            ok=status == 200,
            correct=(True if status == 200 else None),
            timeout=False,
        )


# --------------------------------------------------------------------------- #
# DeskWriteDriver — write_readback analogue.
# --------------------------------------------------------------------------- #
class DeskWriteDriver:
    """POST /api/resource/ToDo + GET readback of the created record.

    Correctness = readback returned the same ``description`` we posted. This
    exercises the DB write path (INSERT on ``tabToDo``) + read path (SELECT
    by name) — the failure surface for the Phase 5 MariaDB max_connections cap
    (POST 500 during peak load).
    """

    name = "desk_write_readback"
    op = "POST"
    target = "/api/resource/ToDo"

    def __init__(self, pool: SessionPool | None = None) -> None:
        self.pool = pool or SessionPool()

    async def request(
        self,
        session: aiohttp.ClientSession,
        *,
        seq: int,
        x: str,
        loop_time: Any,
        channel_keyspace: int = 8,
        plan: Any | None = None,
    ) -> "DriverResult":
        from loadgen.runner import DriverResult, _DriverClientError, _DriverTimeout

        slot, sid = self.pool.sid_for(seq)
        desc = f"sre-world-loadgen-{seq}-{hashlib.md5(x.encode()).hexdigest()[:8]}"
        payload = {"description": desc}

        t_send = loop_time()
        try:
            status, body = await _do_request(
                "POST", TODO_RESOURCE_URL, session, sid, json_body=payload
            )
            if status == 401 and sid is not None:
                fresh = await self.pool.refresh_slot(session, slot)
                if fresh is None:
                    raise _SessionExpired((loop_time() - t_send) * 1000.0)
                raise _SessionExpired((loop_time() - t_send) * 1000.0)
            if status != 200:
                latency_ms = (loop_time() - t_send) * 1000.0
                return DriverResult(
                    status=status,
                    latency_ms=latency_ms,
                    ok=False,
                    correct=None,
                    timeout=False,
                )
            # Frappe's REST resource POST returns {"data": {"name": "...", ...}}.
            import json as _json
            resp = _json.loads(body).get("data", {})
            name = resp.get("name")
            if not name:
                latency_ms = (loop_time() - t_send) * 1000.0
                return DriverResult(
                    status=status,
                    latency_ms=latency_ms,
                    ok=False,
                    correct=False,
                    timeout=False,
                )
            # Readback
            r_status, r_body = await _do_request(
                "GET", f"{TODO_RESOURCE_URL}/{name}", session, sid
            )
            latency_ms = (loop_time() - t_send) * 1000.0
            if r_status != 200:
                return DriverResult(
                    status=r_status,
                    latency_ms=latency_ms,
                    ok=False,
                    correct=False,
                    timeout=False,
                )
            got = _json.loads(r_body).get("data", {}).get("description")
            return DriverResult(
                status=r_status,
                latency_ms=latency_ms,
                ok=True,
                correct=(got == desc),
                timeout=False,
            )
        except asyncio.TimeoutError:
            raise _DriverTimeout((loop_time() - t_send) * 1000.0) from None
        except aiohttp.ClientError as exc:
            raise _DriverClientError((loop_time() - t_send) * 1000.0, exc) from None


# --------------------------------------------------------------------------- #
# RQEnqueueDriver — background_jobs enqueue analogue.
# --------------------------------------------------------------------------- #
class RQEnqueueDriver:
    """Create a Prepared Report through Frappe's supported Desk API.

    ``frappe.utils.background_jobs.enqueue`` is an internal callable and is not
    whitelisted for HTTP, so calling it through ``/api/method`` is rejected
    with 403 even for Administrator. Inserting a Prepared Report is the stable
    public path: its ``after_insert`` hook enqueues report generation on the
    long RQ queue. Records enqueue latency ONLY — execution is measured through
    the queue-depth metrics emitted by the frappe-admin sidecar.
    """

    name = "rq_enqueue"
    op = "POST"
    target = (
        "/api/method/frappe.core.doctype.prepared_report.prepared_report."
        "make_prepared_report"
    )

    def __init__(
        self,
        pool: SessionPool | None = None,
        ledger: "PreparedReportLedger | None" = None,
    ) -> None:
        self.pool = pool or SessionPool()
        # When a PreparedReportLedger is shared with RQCompleteDriver, every
        # successfully created report is recorded for later status polls.
        self.ledger = ledger

    async def request(
        self,
        session: aiohttp.ClientSession,
        *,
        seq: int,
        x: str,
        loop_time: Any,
        channel_keyspace: int = 8,
        plan: Any | None = None,
    ) -> "DriverResult":
        from loadgen.runner import DriverResult, _DriverClientError, _DriverTimeout

        slot, sid = self.pool.sid_for(seq)
        params = {
            "report_name": "Database Storage Usage By Tables",
            "filters": "{}",
        }

        t_send = loop_time()
        try:
            status, body = await _do_request(
                "POST", ENQUEUE_URL, session, sid, params=params
            )
            latency_ms = (loop_time() - t_send) * 1000.0
        except asyncio.TimeoutError:
            raise _DriverTimeout((loop_time() - t_send) * 1000.0) from None
        except aiohttp.ClientError as exc:
            raise _DriverClientError((loop_time() - t_send) * 1000.0, exc) from None

        if status == 401 and sid is not None:
            fresh = await self.pool.refresh_slot(session, slot)
            if fresh is None:
                raise _SessionExpired(latency_ms)
            raise _SessionExpired(latency_ms)
        if status != 200:
            return DriverResult(
                status=status, latency_ms=latency_ms, ok=False, correct=None, timeout=False
            )
        import json as _json
        prepared_report = (
            _json.loads(body).get("message", {}).get("name") if body else None
        )
        if self.ledger is not None and prepared_report:
            self.ledger.append(str(prepared_report), loop_time())
        return DriverResult(
            status=status,
            latency_ms=latency_ms,
            ok=True,
            correct=bool(prepared_report),
            timeout=False,
        )


# --------------------------------------------------------------------------- #
# PreparedReportLedger + RQCompleteDriver — job-COMPLETION grading (v19).
# --------------------------------------------------------------------------- #
class PreparedReportLedger:
    """Phase-partitioned FIFO shared by the enqueue and completion drivers.

    Concurrency contract: arrivals run as concurrent tasks on one asyncio
    loop. Claims and resolutions are synchronous, so no entry can be consumed
    twice. Pre-soak nonterminal polls are requeued: accepted work remains
    available for the bounded recovery boundary. Once that backlog is fully
    verified, ``begin_soak`` switches both drivers to a fresh queue so the
    graded rq_complete slice contains only post-repair traffic.

    The ledger is bounded but never silently evicts. Overflow, duplicate report
    identities, unresolved claims, and an incomplete phase transition all fail
    loudly because losing accepted jobs would violate traffic conservation.
    """

    def __init__(self, maxlen: int = 8192) -> None:
        if maxlen <= 0:
            raise ValueError(f"PreparedReportLedger maxlen must be positive, got {maxlen}")
        self._maxlen = maxlen
        self._pre_soak: collections.deque[tuple[str, float]] = collections.deque()
        self._soak: collections.deque[tuple[str, float]] = collections.deque()
        # Kept as a compatibility alias for focused tests and diagnostics.
        self._q = self._pre_soak
        self._claimed: dict[str, tuple[tuple[str, float], str]] = {}
        self._known: set[str] = set()
        self._phase = "pre_soak"
        self._pre_soak_names: list[str] = []
        self._pre_soak_completed = 0

    def append(self, name: str, ts: float) -> None:
        if not name:
            raise ValueError("PreparedReportLedger report name must be non-empty")
        if name in self._known:
            raise RuntimeError(f"PreparedReportLedger duplicate report identity: {name}")
        if len(self._known) >= self._maxlen:
            raise RuntimeError(
                "PreparedReportLedger capacity exhausted; refusing to evict accepted "
                f"work (maxlen={self._maxlen})"
            )
        entry = (name, ts)
        self._known.add(name)
        if self._phase == "pre_soak":
            self._pre_soak.append(entry)
            self._pre_soak_names.append(name)
        else:
            self._soak.append(entry)

    def pop_eligible(
        self, now: float, budget_s: float | None = None
    ) -> tuple[str, float] | None:
        """Claim the oldest entry at least ``budget_s`` old, else None.

        Synchronous peek-then-pop: atomic on the single loop (no await).
        """
        budget = RQ_COMPLETE_BUDGET_S if budget_s is None else budget_s
        queue = self._pre_soak if self._phase == "pre_soak" else self._soak
        if queue and (now - queue[0][1]) >= budget:
            return self._claim(queue, self._phase)
        return None

    def pop_pre_soak_for_recovery(self) -> tuple[str, float] | None:
        if self._phase != "pre_soak":
            raise RuntimeError("PreparedReportLedger recovery requested after soak began")
        if not self._pre_soak:
            return None
        return self._claim(self._pre_soak, "pre_soak")

    def resolve(self, entry: tuple[str, float], *, completed: bool) -> None:
        name = entry[0]
        claimed = self._claimed.pop(name, None)
        if claimed is None or claimed[0] != entry:
            raise RuntimeError(f"PreparedReportLedger resolved an unclaimed entry: {name}")
        phase = claimed[1]
        if phase == "pre_soak" and not completed:
            self._pre_soak.append(entry)
            return
        self._known.remove(name)
        if phase == "pre_soak" and completed:
            self._pre_soak_completed += 1

    def begin_soak(self) -> None:
        if self._phase != "pre_soak":
            raise RuntimeError("PreparedReportLedger soak phase began more than once")
        if self._pre_soak or self._claimed:
            raise RuntimeError(
                "PreparedReportLedger cannot begin soak with unresolved pre-soak work: "
                f"queued={len(self._pre_soak)} claimed={len(self._claimed)}"
            )
        if self._pre_soak_completed != len(self._pre_soak_names):
            raise RuntimeError(
                "PreparedReportLedger pre-soak conservation mismatch: "
                f"accepted={len(self._pre_soak_names)} "
                f"verified={self._pre_soak_completed}"
            )
        self._phase = "soak"
        self._q = self._soak

    def begin_failed_soak(self) -> None:
        """Keep unresolved accepted work in evidence after a solver-owned stall.

        The graded soak uses a fresh queue, while the failed recovery receipt
        conserves every accepted pre-soak report as still pending.
        """
        if self._phase != "pre_soak" or self._claimed or not self._pre_soak:
            raise RuntimeError("PreparedReportLedger has no settled failed backlog")
        self._phase = "soak"
        self._q = self._soak

    @property
    def pre_soak_pending(self) -> int:
        return len(self._pre_soak) + sum(
            phase == "pre_soak" for _entry, phase in self._claimed.values()
        )

    @property
    def pre_soak_accepted(self) -> int:
        return len(self._pre_soak_names)

    @property
    def pre_soak_completed(self) -> int:
        return self._pre_soak_completed

    @property
    def pre_soak_names_sha256(self) -> str:
        canonical = "\n".join(sorted(self._pre_soak_names)).encode("utf-8")
        return hashlib.sha256(canonical).hexdigest()

    def _claim(
        self,
        queue: collections.deque[tuple[str, float]],
        phase: str,
    ) -> tuple[str, float]:
        entry = queue.popleft()
        name = entry[0]
        if name in self._claimed:
            raise RuntimeError(f"PreparedReportLedger double-claimed report: {name}")
        self._claimed[name] = (entry, phase)
        return entry

    def __len__(self) -> int:
        return len(self._pre_soak) + len(self._soak)


class RQCompleteDriver:
    """Poll a previously enqueued Prepared Report for terminal completion.

    Grades the half of the queue path rq_enqueue cannot see: whether the
    long-queue worker actually EXECUTED the job. Per arrival: claim the
    oldest ledger entry older than ``RQ_COMPLETE_BUDGET_S``, then GET
    ``frappe.client.get_value`` on its ``status`` field (tiny response;
    Administrator session; whitelisted method).

    Result semantics:
      - no eligible entry -> ``status=0, ok=False`` — a fail-loud "cannot
        prove completion" sentinel. Occurs in the first ~budget seconds
        (before any entry ages in), at PEAK ONSET when poll rate exceeds the
        supply aged from lower-rate production budget-seconds earlier (a
        structural rate artifact — C2 runs measured ~26% of peak polls at the
        15s budget; bands must be calibration-derived, never assumed clean),
        and when enqueue itself is broken, where an inflated rq_complete
        error rate is the CORRECT fault signal.
      - HTTP 200 -> ``ok=True``; ``correct = (status == "Completed")``.
        Queued/Started/Error at age >= budget count as not-completed; so
        does "Failed", which the pinned image's stalled-report cron stamps
        despite being outside the doctype's declared options.
    """

    name = "rq_complete"
    op = "GET"
    target = "/api/method/frappe.client.get_value"

    @staticmethod
    def accepted_work(name: str) -> tuple[str, str, str]:
        """Resolve task-seeded mail alongside ordinary Prepared Reports."""
        if name.startswith("email:"):
            return "Email Queue", name.removeprefix("email:"), "Sent"
        return "Prepared Report", name, "Completed"

    def __init__(
        self,
        pool: SessionPool | None = None,
        ledger: PreparedReportLedger | None = None,
    ) -> None:
        self.pool = pool or SessionPool()
        # `ledger or ...` would discard a SHARED-but-empty ledger (falsy via
        # __len__) and silently sever the producer/consumer link — identity
        # check only.
        self.ledger = ledger if ledger is not None else PreparedReportLedger()

    async def request(
        self,
        session: aiohttp.ClientSession,
        *,
        seq: int,
        x: str,
        loop_time: Any,
        channel_keyspace: int = 8,
        plan: Any | None = None,
    ) -> "DriverResult":
        from loadgen.runner import DriverResult, _DriverClientError, _DriverTimeout

        # Claim BEFORE any await — atomic under cooperative scheduling.
        entry = self.ledger.pop_eligible(loop_time())
        if entry is None:
            return DriverResult(
                status=0, latency_ms=0.0, ok=False, correct=None, timeout=False
            )
        report_name, _enqueued_ts = entry
        doctype, document_name, completed_status = self.accepted_work(report_name)

        slot, sid = self.pool.sid_for(seq)

        params = {
            "doctype": doctype,
            "fieldname": "status",
            "filters": json.dumps({"name": document_name}),
        }

        t_send = loop_time()
        try:
            status, body = await _do_request(
                "GET", GET_VALUE_URL, session, sid, params=params
            )
            latency_ms = (loop_time() - t_send) * 1000.0
        except TimeoutError:
            self.ledger.resolve(entry, completed=False)
            raise _DriverTimeout((loop_time() - t_send) * 1000.0) from None
        except aiohttp.ClientError as exc:
            self.ledger.resolve(entry, completed=False)
            raise _DriverClientError((loop_time() - t_send) * 1000.0, exc) from None

        if status == 401 and sid is not None:
            fresh = await self.pool.refresh_slot(session, slot)
            self.ledger.resolve(entry, completed=False)
            if fresh is None:
                raise _SessionExpired(latency_ms)
            raise _SessionExpired(latency_ms)
        if status != 200:
            self.ledger.resolve(entry, completed=False)
            return DriverResult(
                status=status,
                latency_ms=latency_ms,
                ok=False,
                correct=None,
                timeout=False,
            )
        try:
            payload = json.loads(body) if body else None
        except json.JSONDecodeError:
            self.ledger.resolve(entry, completed=False)
            raise
        message = payload.get("message") if isinstance(payload, dict) else None
        report_status = (message or {}).get("status")
        completed = report_status == completed_status
        self.ledger.resolve(entry, completed=completed)
        return DriverResult(
            status=status,
            latency_ms=latency_ms,
            ok=True,
            correct=completed,
            timeout=False,
        )

    async def _prepared_report_count(
        self, session: aiohttp.ClientSession, status: str, slot_seq: int
    ) -> int | None:
        """Site-wide Prepared Report count in ``status``; None if unreadable.

        A diagnostic/progress probe only: an unreadable count never completes
        or fails an accepted report, it just cannot extend the stall window.
        """
        _slot, sid = self.pool.sid_for(slot_seq)
        params = {
            "doctype": "Prepared Report",
            "filters": json.dumps({"status": status}),
        }
        try:
            http_status, body = await _do_request(
                "GET", GET_COUNT_URL, session, sid, params=params
            )
            if http_status != 200:
                return None
            payload = json.loads(body)
        except (TimeoutError, aiohttp.ClientError, json.JSONDecodeError):
            return None
        count = payload.get("message") if isinstance(payload, dict) else None
        if isinstance(count, bool) or not isinstance(count, int) or count < 0:
            return None
        return count

    async def recover_pre_soak(
        self,
        session: aiohttp.ClientSession,
        *,
        loop_time: Any,
        stall_s: float | None = None,
        max_s: float | None = None,
        backoff_s: float | None = None,
    ) -> dict[str, Any]:
        """Verify every accepted pre-soak report before opening the graded soak.

        Waits while the long worker makes progress — an accepted report reaches
        Completed or the site-wide Completed count grows — and fails loudly when
        progress stalls for ``stall_s`` or the ``max_s`` ceiling is reached.
        Accepted work is never skipped: success still requires every one.
        """
        stall = RQ_RECOVERY_STALL_S if stall_s is None else stall_s
        ceiling = RQ_RECOVERY_MAX_S if max_s is None else max_s
        backoff = RQ_RECOVERY_BACKOFF_S if backoff_s is None else backoff_s
        if stall <= 0 or ceiling <= 0 or backoff <= 0:
            raise ValueError(
                "RQ recovery stall/max/backoff must be positive, got "
                f"{stall}/{ceiling}/{backoff}"
            )
        started = loop_time()
        hard_deadline = started + ceiling
        last_progress = started
        accepted = self.ledger.pre_soak_accepted
        completed_before = self.ledger.pre_soak_completed
        pending_at_boundary = self.ledger.pre_soak_pending
        attempts = 0
        last_observed: dict[str, str] = {}
        terminal_failure: tuple[str, str] | None = None
        site_completed_start = await self._prepared_report_count(
            session, "Completed", 0
        )
        site_completed = site_completed_start
        site_queued_start = await self._prepared_report_count(session, "Queued", 1)

        def deadline() -> float:
            return min(last_progress + stall, hard_deadline)

        while self.ledger.pre_soak_pending:
            round_size = self.ledger.pre_soak_pending
            for _ in range(round_size):
                if loop_time() >= deadline():
                    break
                entry = self.ledger.pop_pre_soak_for_recovery()
                if entry is None:
                    break
                report_name = entry[0]
                slot, sid = self.pool.sid_for(attempts)
                doctype, document_name, completed_status = self.accepted_work(report_name)
                params = {
                    "doctype": doctype,
                    "fieldname": "status",
                    "filters": json.dumps({"name": document_name}),
                }
                try:
                    status, body = await _do_request(
                        "GET", GET_VALUE_URL, session, sid, params=params
                    )
                except (TimeoutError, aiohttp.ClientError) as exc:
                    self.ledger.resolve(entry, completed=False)
                    last_observed[report_name] = f"{type(exc).__name__}: {exc}"
                    attempts += 1
                    continue
                attempts += 1
                if status == 401 and sid is not None:
                    await self.pool.refresh_slot(session, slot)
                    self.ledger.resolve(entry, completed=False)
                    last_observed[report_name] = "HTTP 401"
                    continue
                if status != 200:
                    self.ledger.resolve(entry, completed=False)
                    last_observed[report_name] = f"HTTP {status}"
                    continue
                try:
                    payload = json.loads(body)
                except json.JSONDecodeError as exc:
                    self.ledger.resolve(entry, completed=False)
                    raise RuntimeError(
                        f"RQ recovery received malformed JSON for {report_name}: {exc}"
                    ) from exc
                message = payload.get("message") if isinstance(payload, dict) else None
                if not isinstance(message, dict) or not isinstance(
                    message.get("status"), str
                ):
                    self.ledger.resolve(entry, completed=False)
                    raise RuntimeError(
                        "RQ recovery could not read accepted Prepared Report "
                        f"{report_name}; data may have been lost: {payload!r}"
                    )
                report_status = message["status"]
                if report_status == completed_status:
                    self.ledger.resolve(entry, completed=True)
                    last_observed.pop(report_name, None)
                    last_progress = loop_time()
                elif report_status in {"Error", "Failed"}:
                    self.ledger.resolve(entry, completed=False)
                    terminal_failure = (report_name, report_status)
                    last_observed[report_name] = report_status
                    break
                else:
                    self.ledger.resolve(entry, completed=False)
                    last_observed[report_name] = report_status

            if terminal_failure or not self.ledger.pre_soak_pending:
                break
            # Accepted reports may be buried behind newer jobs (LIFO under
            # starvation); the worker draining those is still progress.
            observed = await self._prepared_report_count(
                session, "Completed", attempts
            )
            if observed is not None:
                if site_completed is not None and observed > site_completed:
                    last_progress = loop_time()
                if site_completed is None or observed > site_completed:
                    site_completed = observed
            remaining = deadline() - loop_time()
            if remaining <= 0:
                break
            await asyncio.sleep(min(backoff, remaining))

        if self.ledger.pre_soak_pending:
            now = loop_time()
            # The class says who owns the failure: a worker still draining at
            # the ceiling is a harness capacity problem; a stalled worker with
            # accepted work left is a system-under-test failure.
            reason = (
                "class=sut_terminal_failed: accepted report "
                f"{terminal_failure[0]} entered {terminal_failure[1]}"
                if terminal_failure
                else (
                    "class=harness_drain_ceiling: exceeded ceiling"
                    if now >= hard_deadline
                    else f"class=sut_no_worker_progress: no worker progress for {stall:g}s"
                )
            )
            site_queued = await self._prepared_report_count(
                session, "Queued", attempts
            )
            sample = sorted(last_observed.items())[:5]
            detail = (
                "RQ pre-soak recovery incomplete without losing or ignoring accepted "
                f"work ({reason}): pending={self.ledger.pre_soak_pending}/{accepted}, "
                f"elapsed_s={now - started:.1f}, stall_s={stall:g}, max_s={ceiling:g}, "
                f"site_completed={site_completed_start}->{site_completed}, "
                f"site_queued={site_queued_start}->{site_queued}, "
                f"attempts={attempts}, last_observed={sample}"
            )
            if now >= hard_deadline and not terminal_failure:
                raise TimeoutError(detail)
            logger.warning("%s; continuing to graded failure evidence", detail)
            self.ledger.begin_failed_soak()
            return {
                "schema_version": 1,
                "phase": "pre_soak_recovery",
                "pass": False,
                "accepted": accepted,
                "pending_at_boundary": pending_at_boundary,
                "completed_before_recovery": completed_before,
                "completed_during_recovery": (
                    self.ledger.pre_soak_completed - completed_before
                ),
                "verified_completed": self.ledger.pre_soak_completed,
                "remaining": self.ledger.pre_soak_pending,
                "poll_attempts": attempts,
                "duration_s": round(now - started, 6),
                "accepted_names_sha256": self.ledger.pre_soak_names_sha256,
            }

        self.ledger.begin_soak()
        # Diagnostics stay out of the receipt: its field set is a verifier
        # contract (verifier/providers/redis_state.py).
        logger.info(
            "RQ pre-soak recovery drained: accepted=%d pending_at_boundary=%d "
            "elapsed_s=%.1f site_completed=%s->%s site_queued_at_start=%s "
            "stall_s=%g max_s=%g attempts=%d",
            accepted,
            pending_at_boundary,
            loop_time() - started,
            site_completed_start,
            site_completed,
            site_queued_start,
            stall,
            ceiling,
            attempts,
        )
        completed_during = self.ledger.pre_soak_completed - completed_before
        return {
            "schema_version": 1,
            "phase": "pre_soak_recovery",
            "pass": True,
            "accepted": accepted,
            "pending_at_boundary": pending_at_boundary,
            "completed_before_recovery": completed_before,
            "completed_during_recovery": completed_during,
            "verified_completed": self.ledger.pre_soak_completed,
            "remaining": 0,
            "poll_attempts": attempts,
            "duration_s": round(loop_time() - started, 6),
            "accepted_names_sha256": self.ledger.pre_soak_names_sha256,
        }
