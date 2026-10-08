from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pytest


SCRIPT = (
    Path(__file__).resolve().parent
    / "chart/files/verifier-evidence-entrypoint.py"
)


@pytest.fixture
def entrypoint():
    spec = importlib.util.spec_from_file_location("verifier_evidence_entrypoint", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _sidecar(grader: Path, calls: list[str]) -> SimpleNamespace:
    done = grader / "episode_done.json"

    def run_episode() -> None:
        calls.append("episode")

    def write_done(payload: dict) -> None:
        assert not done.exists()
        done.write_text(json.dumps(payload), encoding="utf-8")

    async def start_server(_state: dict) -> None:
        calls.append("server")

    async def sleep_forever() -> None:
        calls.append("sleep")

    # Pinned loadgen images predate protected runtime-state phase capture; the
    # entrypoint backports it by wrapping these two hooks before main runs.
    def make_agent_boundary_hook(_lg: object, _state: dict):
        async def boundary(_reason: str) -> None:
            calls.append("boundary")

        return boundary

    async def snapshot_soak_end(_lg: object) -> None:
        calls.append("soak_end")

    return SimpleNamespace(
        GRADER=grader,
        EPISODE_DONE_JSON=done,
        SOURCE_SNAPSHOT_ENABLED=False,
        load_grader_access_token=lambda: "v" * 48,
        start_http_server=start_server,
        _sleep_forever=sleep_forever,
        _write_episode_done=write_done,
        main=run_episode,
        make_agent_boundary_hook=make_agent_boundary_hook,
        _snapshot_soak_end=snapshot_soak_end,
    )


def test_first_start_claims_episode_once(
    entrypoint, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[str] = []
    sidecar = _sidecar(tmp_path, calls)
    monkeypatch.setitem(__import__("sys").modules, "loadgen_sidecar", sidecar)
    original_boundary = sidecar.make_agent_boundary_hook
    original_soak_end = sidecar._snapshot_soak_end
    entrypoint.main()
    assert calls == ["episode"]
    assert (tmp_path / ".episode-started.json").is_file()
    assert sidecar.make_agent_boundary_hook is not original_boundary
    assert sidecar._snapshot_soak_end is not original_soak_end


def test_finalized_episode_is_served_without_rerun(
    entrypoint, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[str] = []
    sidecar = _sidecar(tmp_path, calls)
    sidecar.EPISODE_DONE_JSON.write_text('{"done":true}\n', encoding="utf-8")
    monkeypatch.setitem(__import__("sys").modules, "loadgen_sidecar", sidecar)
    entrypoint.main()
    assert calls == ["server", "sleep"]


def test_inflight_restart_freezes_error_and_does_not_rerun(
    entrypoint, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[str] = []
    sidecar = _sidecar(tmp_path, calls)
    (tmp_path / ".episode-started.json").write_text("{}\n", encoding="utf-8")
    monkeypatch.setitem(__import__("sys").modules, "loadgen_sidecar", sidecar)
    entrypoint.main()
    assert calls == ["server", "sleep"]
    receipt = json.loads(sidecar.EPISODE_DONE_JSON.read_text())
    assert receipt["done"] is False
    assert "InfrastructureEvidenceRestart" in receipt["error"]
