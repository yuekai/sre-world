"""Docs stay navigable: links resolve, the AGENTS.md map is real, plans have a status.

AGENTS.md is the agent's entry point and points into docs/; a dangling pointer
sends an agent to a file that does not exist, so it fails here instead.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
DOCS = [
    REPO / "AGENTS.md",
    REPO / "README.md",
    REPO / "QUICKSTART.md",
    REPO / "CONTRIBUTING.md",
    REPO / "verifier" / "README.md",
    *sorted((REPO / "docs").glob("*.md")),
    *sorted((REPO / "docs" / "plans").glob("*.md")),
]
_LINK = re.compile(r"\]\(([^)\s]+)\)")
_STATUS = re.compile(r"^Status: (active|completed|abandoned)$")


def _local_targets(md: Path) -> list[str]:
    targets = []
    for raw in _LINK.findall(md.read_text(encoding="utf-8")):
        target = raw.split("#", 1)[0]
        if not target or re.match(r"^[a-z][a-z0-9+.-]*:", target):
            continue  # pure anchor, or a URL scheme (http:, mailto:, ...)
        targets.append(target)
    return targets


@pytest.mark.parametrize("md", DOCS, ids=lambda p: str(p.relative_to(REPO)))
def test_relative_links_resolve(md: Path) -> None:
    missing = [t for t in _local_targets(md) if not (md.parent / t).exists()]
    assert not missing, (
        f"{md.relative_to(REPO)} links to missing paths {missing}. Fix the link, or "
        "restore the target; docs are the agent's map (AGENTS.md)."
    )


def test_agents_map_paths_exist() -> None:
    text = (REPO / "AGENTS.md").read_text(encoding="utf-8")
    rows = [line for line in text.splitlines() if line.startswith("| `")]
    paths = [re.match(r"\| `([^`]+)`", row).group(1) for row in rows]
    assert paths, "AGENTS.md has no map table rows (| `path` | ... |)"
    missing = [
        p for p in paths
        if "<" not in p and not (REPO / p.rstrip("/")).exists()
    ]
    assert not missing, (
        f"AGENTS.md map names paths that do not exist: {missing}. Update the map "
        "row when you move or delete what it points at."
    )


def test_every_plan_has_a_status() -> None:
    plans = [
        p for p in (REPO / "docs" / "plans").glob("*.md")
        if p.name not in {"README.md", "tech-debt.md"}
    ]
    bad = [
        p.name for p in plans
        if not _STATUS.match(p.read_text(encoding="utf-8").splitlines()[0])
    ]
    assert not bad, (
        f"plans without a first-line 'Status: active|completed|abandoned': {bad}. "
        "See docs/plans/README.md."
    )
