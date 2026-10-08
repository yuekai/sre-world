"""Supplemental lifecycle-race contracts excluded from SUT fingerprints.

An episode ends exactly once: the first accepted ``POST
/declare_repair_complete`` wins, and an episode that is already over (frozen at
its deadline and being graded) cannot be re-declared.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

from aiohttp.test_utils import TestClient, TestServer


SUBSTRATE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SUBSTRATE))

import loadgen_grader_common as common  # noqa: E402
import loadgen_sidecar as sidecar  # noqa: E402


TOKEN = "v" * 48
AUTH = {common.GRADER_ACCESS_HEADER: TOKEN}


class _StubLoadGen:
    def __init__(self, *, finished: bool) -> None:
        self._t0 = 0.0
        self.declare_ts_s = None
        self.finished = asyncio.Event()
        if finished:
            self.finished.set()

    def begin_declaration(self) -> bool:
        return not self.finished.is_set()

    def declare(self) -> None:
        raise AssertionError("a refused declaration must not declare")


async def test_declare_preserves_accepted_declaration() -> None:
    state = {
        "lg": _StubLoadGen(finished=False),
        "grader_access_token": TOKEN,
        "declaration_locked": True,
    }
    cli = TestClient(TestServer(sidecar.build_grader_app(state)))
    await cli.start_server()
    try:
        response = await cli.post("/declare_repair_complete")
        assert response.status == 409
        assert (await response.json())["error"] == "declaration_already_locked"
        assert state["declaration_locked"] is True
    finally:
        await cli.close()


async def test_declare_after_episode_complete_is_refused() -> None:
    state = {"lg": _StubLoadGen(finished=True), "grader_access_token": TOKEN}
    cli = TestClient(TestServer(sidecar.build_grader_app(state)))
    await cli.start_server()
    try:
        response = await cli.post("/declare_repair_complete")
        assert response.status == 409
        assert (await response.json())["error"] == "episode_already_complete"
        assert not state.get("declaration_locked", False)
    finally:
        await cli.close()
