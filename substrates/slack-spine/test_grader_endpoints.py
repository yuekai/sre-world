"""Gated /grader endpoint tests for the loadgen sidecar (the Oddish fetch surface).

Exercises the HTTP routing in isolation with an aiohttp TestClient:
  * every /grader/* route rejects callers without the verifier capability; an
    authorized request is 503 until episode_done.json exists,
  * POST /declare_repair_complete and POST /grader/episode-start are 503
    until the episode publishes its LoadGen; the incident report (POST /report)
    is advisory and validated only for usability,
  * once episode_done exists, /grader/bundle serves a tar of ONLY the fixed
    allowlist (no traversal),
  * collector errors are exposed directly in episode_done (fail loud), never
    converted into a fabricated reward.

Run from substrate/ with PYTHONPATH=. (same as the other loadgen tests).
"""

from __future__ import annotations

import asyncio
import io
import json
import tarfile
from pathlib import Path
from types import SimpleNamespace

import pytest
from aiohttp.test_utils import TestClient, TestServer

import loadgen_grader_common as common
import loadgen_sidecar as sidecar


TOKEN = "v" * 48
AUTH = {common.GRADER_ACCESS_HEADER: TOKEN}


class _SnapshotResponse:
    def __init__(self, payload: dict) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return self._payload


class _SnapshotClient:
    def __init__(self, get) -> None:
        self.get = get

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args) -> None:
        return None


def test_grader_capability_load_fails_loudly(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    token_file = tmp_path / "token"
    monkeypatch.setattr(common, "GRADER_ACCESS_TOKEN_FILE", token_file)
    with pytest.raises(RuntimeError, match="grader access token is unavailable"):
        common.load_grader_access_token()

    token_file.write_text("short")
    with pytest.raises(RuntimeError, match="missing or too short"):
        common.load_grader_access_token()

    token_file.write_text(TOKEN + "\n")
    assert common.load_grader_access_token() == TOKEN


def test_grader_app_refuses_to_start_without_capability() -> None:
    with pytest.raises(RuntimeError, match="grader access token is required"):
        sidecar.build_grader_app({"lg": None})


async def test_service_config_snapshot_retries_transient_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempts = 0
    delays: list[float] = []

    async def get(_url: str):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise sidecar.httpx.ConnectTimeout("transient auth timeout")
        return _SnapshotResponse({"db": {"pool_size": 5}})

    async def sleep(delay_s: float) -> None:
        delays.append(delay_s)

    monkeypatch.setattr(sidecar, "SNAPSHOT_SERVICES", ["auth"])
    monkeypatch.setattr(
        sidecar.httpx,
        "AsyncClient",
        lambda **_kwargs: _SnapshotClient(get),
    )
    monkeypatch.setattr(sidecar.asyncio, "sleep", sleep)

    assert await sidecar._snapshot_service_configs() == {
        "auth": {"ok": True, "config": {"db": {"pool_size": 5}}}
    }
    assert attempts == 2
    assert delays == [0.5]


async def test_service_config_snapshot_exhaustion_remains_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempts = 0
    delays: list[float] = []

    async def get(_url: str):
        nonlocal attempts
        attempts += 1
        raise sidecar.httpx.ConnectTimeout("auth remains unreachable")

    async def sleep(delay_s: float) -> None:
        delays.append(delay_s)

    monkeypatch.setattr(sidecar, "SNAPSHOT_SERVICES", ["auth"])
    monkeypatch.setattr(
        sidecar.httpx,
        "AsyncClient",
        lambda **_kwargs: _SnapshotClient(get),
    )
    monkeypatch.setattr(sidecar.asyncio, "sleep", sleep)

    snapshot = await sidecar._snapshot_service_configs()
    assert snapshot["auth"]["ok"] is False
    assert snapshot["auth"]["error"] == "ConnectTimeout: auth remains unreachable"
    assert attempts == 3
    assert delays == [0.5, 1.0]


@pytest.fixture
def grader(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point the shared grader path constants at a temp dir.

    Both the extracted common module and the sidecar re-export need patching so
    every code path (bundle builder in common, config-snapshot writer in sidecar)
    reads the same tmp dir.
    """
    g = tmp_path / "grader"
    g.mkdir()
    for mod in (common, sidecar):
        monkeypatch.setattr(mod, "GRADER", g)
        monkeypatch.setattr(mod, "EPISODE_DONE_JSON", g / "episode_done.json")
    return g


@pytest.fixture
async def client(grader: Path) -> TestClient:
    state: dict = {"lg": None, "grader_access_token": TOKEN}
    # build_grader_app gives the routed app with NO TCP bind (no :9100 listener).
    cli = TestClient(TestServer(sidecar.build_grader_app(state)))
    await cli.start_server()
    yield cli
    await cli.close()


async def test_grader_routes_503_before_episode_done(client: TestClient) -> None:
    for path in ("/grader/episode_done", "/grader/bundle"):
        resp = await client.get(path, headers=AUTH)
        assert resp.status == 503, path


async def test_grader_routes_reject_agent_without_capability(client: TestClient) -> None:
    for path in ("/grader/episode_done", "/grader/bundle"):
        response = await client.get(path)
        assert response.status == 403, path
        assert await response.json() == {"error": "grader_access_forbidden"}


async def test_declare_503_before_lg_published(client: TestClient) -> None:
    resp = await client.post("/declare_repair_complete")
    assert resp.status == 503


async def test_episode_start_requires_capability_and_published_loadgen(
    client: TestClient,
) -> None:
    forbidden = await client.post("/grader/episode-start")
    assert forbidden.status == 403
    unavailable = await client.post("/grader/episode-start", headers=AUTH)
    assert unavailable.status == 503


async def test_episode_start_pins_t0_once(
    grader: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(common, "_agent_window_s", lambda _lg: 3600.0)

    class StubLoadGen:
        bringup_complete = True

        def __init__(self) -> None:
            self._t0: float | None = None

        def signal_episode_start(self) -> bool:
            if self._t0 is not None:
                return False
            self._t0 = 1.0
            return True

    lg = StubLoadGen()
    state = {"lg": lg, "grader_access_token": TOKEN}
    cli = TestClient(TestServer(sidecar.build_grader_app(state)))
    await cli.start_server()
    try:
        first = await cli.post("/grader/episode-start", headers=AUTH)
        assert first.status == 200
        assert await first.json() == {
            "ok": True, "pinned": True, "agent_window_s": 3600.0,
        }
        again = await cli.post("/grader/episode-start", headers=AUTH)
        assert again.status == 200
        assert (await again.json())["pinned"] is False
        assert lg._t0 == 1.0
    finally:
        await cli.close()


async def test_episode_start_refused_until_bringup_complete(grader: Path) -> None:
    lg = SimpleNamespace(bringup_complete=False)
    state = {"lg": lg, "grader_access_token": TOKEN}
    cli = TestClient(TestServer(sidecar.build_grader_app(state)))
    await cli.start_server()
    try:
        response = await cli.post("/grader/episode-start", headers=AUTH)
        assert response.status == 503
        assert "bringup" in (await response.json())["error"]
    finally:
        await cli.close()


async def test_episode_ready_is_independent_from_health(grader: Path) -> None:
    state = {"lg": None, "grader_access_token": TOKEN, "episode_ready": False}
    client = TestClient(TestServer(sidecar.build_grader_app(state)))
    await client.start_server()
    try:
        assert (await client.get("/healthz")).status == 200
        assert (await client.get("/episode-ready")).status == 503
        state["episode_ready"] = True
        response = await client.get("/episode-ready")
        assert response.status == 200
        assert await response.json() == {"ok": True, "episode_ready": True}
    finally:
        await client.close()


async def test_temporal_watcher_failure_is_visible_and_stops_loadgen() -> None:
    stopped = False

    class StubLoadGen:
        def stop(self) -> None:
            nonlocal stopped
            stopped = True

    async def run_forever() -> dict:
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    async def fail_watcher() -> None:
        raise ModuleNotFoundError("No module named 'auth_rotation'")

    run_task = asyncio.create_task(run_forever())
    watcher = asyncio.create_task(fail_watcher())
    state: dict = {}
    with pytest.raises(RuntimeError, match="ModuleNotFoundError.*auth_rotation"):
        await sidecar._await_loadgen_run(run_task, watcher, StubLoadGen(), state)
    assert stopped is True
    assert "ModuleNotFoundError" in state["episode_ready_error"]
    assert run_task.cancelled()


async def test_temporal_watcher_must_explicitly_open_gate() -> None:
    class StubLoadGen:
        def stop(self) -> None:
            pass

    async def run_forever() -> dict:
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    async def empty_watcher() -> None:
        return None

    run_task = asyncio.create_task(run_forever())
    watcher = asyncio.create_task(empty_watcher())
    state: dict = {}
    with pytest.raises(RuntimeError, match="without opening the agent gate"):
        await sidecar._await_loadgen_run(run_task, watcher, StubLoadGen(), state)
    assert run_task.cancelled()


async def test_report_rejects_empty_findings(
    grader: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(common, "REPORT_JSON", grader / "report.json")
    state: dict = {"lg": object(), "grader_access_token": TOKEN}
    cli = TestClient(TestServer(sidecar.build_grader_app(state)))
    await cli.start_server()
    try:
        resp = await cli.post("/report", json={"findings": []})
        assert resp.status == 400
        body = await resp.json()
        assert "at least one finding" in body["error"]
        assert not (grader / "report.json").exists()
    finally:
        await cli.close()


async def test_source_not_built_declaration_can_retry(
    grader: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    candidate_path = grader / "source_at_declare.candidate"
    submission_path = grader / "source_at_submission"
    monkeypatch.setattr(sidecar, "SOURCE_SNAPSHOT_ENABLED", True)
    monkeypatch.setattr(sidecar, "SOURCE_DECLARE_CANDIDATE", candidate_path)
    monkeypatch.setattr(sidecar, "SOURCE_SUBMISSION", submission_path)
    monkeypatch.setattr(sidecar, "ATTESTATION_REJECTED", grader / "attestation_rejected.json")
    monkeypatch.setattr(sidecar, "CONFIG_AT_SUBMISSION_JSON", grader / "config_at_submission.json")
    monkeypatch.setattr(common, "REPORT_JSON", grader / "report.json")

    digest = SimpleNamespace(as_dict=lambda: {"digest": "a" * 64})

    def capture(path: Path):
        path.mkdir()
        return digest

    attempts = 0

    async def attest(_digest, _phase):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise sidecar.AttestationError("edited source has not been built")
        return {}

    async def snapshot():
        return {}

    async def worker_snapshot(_path, _phase):
        return None

    monkeypatch.setattr(sidecar, "_capture_source_snapshot", capture)
    monkeypatch.setattr(sidecar, "_capture_build_evidence", attest)
    monkeypatch.setattr(sidecar, "_snapshot_service_configs", snapshot)
    monkeypatch.setattr(sidecar, "_snapshot_infra_configs", snapshot)
    monkeypatch.setattr(sidecar, "_snapshot_worker_configs", worker_snapshot)

    class StubLoadGen:
        def __init__(self) -> None:
            self._t0 = 0.0
            self.declare_ts_s = None
            self.pending = False

        def begin_declaration(self) -> bool:
            self.pending = True
            return True

        def declare(self) -> None:
            self.pending = False
            self.declare_ts_s = 1.0

    state = {"lg": StubLoadGen(), "grader_access_token": TOKEN}
    cli = TestClient(TestServer(sidecar.build_grader_app(state)))
    await cli.start_server()
    try:
        rejected = await cli.post("/declare_repair_complete")
        assert rejected.status == 409
        assert (await rejected.json())["error"] == "source_not_built"
        assert not state.get("declaration_locked", False)
        assert not candidate_path.exists()

        accepted = await cli.post("/declare_repair_complete")
        assert accepted.status == 200
        assert state["declaration_locked"] is True
        assert submission_path.is_dir()
        assert state["lg"].declare_ts_s == 1.0
    finally:
        await cli.close()


async def test_healthz_always_200(client: TestClient) -> None:
    resp = await client.get("/healthz")
    assert resp.status == 200
    assert (await resp.json())["ok"] is True


def test_intervention_probe_rejects_non_object(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sidecar, "_psql_scalar", lambda _sql: "[]")
    with pytest.raises(RuntimeError, match="probe is not an object"):
        sidecar._probe_intervention_state_dsn()


async def test_runtime_probe_validates_manifest_before_network() -> None:
    with pytest.raises(RuntimeError, match="service must be non-empty"):
        await sidecar._probe_runtime_state_http({"runtime_state": {}})
    with pytest.raises(RuntimeError, match="absolute path"):
        await sidecar._probe_runtime_state_http(
            {"runtime_state": {"service": "message", "endpoint": "relative"}}
        )


async def test_episode_done_served_after_collection(client: TestClient, grader: Path) -> None:
    payload = {"done": True}
    (grader / "episode_done.json").write_text(json.dumps(payload))

    resp = await client.get("/grader/episode_done", headers=AUTH)
    assert resp.status == 200
    assert await resp.json() == payload


async def test_episode_error_is_exposed_without_fabricating_reward(
    client: TestClient, grader: Path
) -> None:
    payload = {"done": False, "error": "evidence collection blew up"}
    (grader / "episode_done.json").write_text(json.dumps(payload))
    resp = await client.get("/grader/episode_done", headers=AUTH)
    assert resp.status == 200
    assert await resp.json() == payload


async def test_bundle_is_fixed_allowlist_only(client: TestClient, grader: Path) -> None:
    # Populate a mix of allowlisted + NON-allowlisted files.
    (grader / "loadgen.jsonl").write_text('{"ok": 1}\n')
    (grader / "temporal_events.jsonl").write_text('{"stage": "planned"}\n')
    (grader / "meta.json").write_text("{}")
    (grader / "ground-truth.yaml").write_text("thresholds: {}\n")
    (grader / "config_before").mkdir()
    (grader / "config_before" / "sut").mkdir()
    (grader / "config_before" / "sut" / "app.yaml").write_text("roles: {}\n")
    # These must NEVER appear in the bundle (no answer key, no k8s token, etc.).
    (grader / "secret_should_not_ship.txt").write_text("SECRET")
    (grader / "config_at_declare.json").write_text("{}")  # allowlisted, included
    (grader / "episode_done.json").write_text(json.dumps({"done": True}))

    resp = await client.get("/grader/bundle", headers=AUTH)
    assert resp.status == 200
    data = await resp.read()
    with tarfile.open(fileobj=io.BytesIO(data), mode="r") as tar:
        names = set(tar.getnames())

    assert "loadgen.jsonl" in names
    assert "temporal_events.jsonl" in names
    assert "config_before/sut/app.yaml" in names
    assert "config_at_declare.json" in names
    assert "ground-truth.yaml" in names
    assert "secret_should_not_ship.txt" not in names
    # Only allowlisted top-level names (files or dir roots) ever appear.
    tops = {n.split("/", 1)[0] for n in names}
    allowed = set(sidecar.BUNDLE_FILES) | set(sidecar.BUNDLE_DIRS)
    assert tops <= allowed, tops - allowed
