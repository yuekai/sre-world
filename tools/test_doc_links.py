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
    *sorted((REPO / "docs" / "research").glob("*.md")),
    *sorted((REPO / "docs" / "archive").glob("*.md")),
]
_LINK = re.compile(r"\]\(([^)\s]+)\)")
_STATUS = re.compile(r"^Status: (active|completed|abandoned)$")
# Instructions an agent follows verbatim (commands, "edit X"); historical docs
# under docs/ may name retired paths on purpose.
LIVE_DOCS = [
    REPO / "AGENTS.md",
    REPO / "README.md",
    REPO / "QUICKSTART.md",
    REPO / "CONTRIBUTING.md",
    REPO / "verifier" / "README.md",
]
_REPO_PATH = re.compile(
    r"(?<![\w./<>-])((?:substrates|scenarios|tasks|verifier|tools|loadgen-common"
    r"|docs|ci_checks)/[^\s`'\")\]|,;]*)"
)
_PLACEHOLDER = re.compile(r"[<>*{}$]")
# Paths a live doc names as history, not as something to use.
_HISTORICAL_PATHS = {
    ("verifier/README.md", "tools/verifier_v2/"): "records where verifier/ moved from",
}
_DECISION = re.compile(r"^## D(\d+) ", re.MULTILINE)
_UNASSIGNED_DECISIONS = {17}  # skipped by the original; see docs/DECISIONS.md
_EVIDENCE = re.compile(r"^  Evidence: (reproduced|observed|inferred) \(.+\)\.$", re.MULTILINE)


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


@pytest.mark.parametrize("md", LIVE_DOCS, ids=lambda p: str(p.relative_to(REPO)))
def test_live_doc_repo_paths_exist(md: Path) -> None:
    rel = str(md.relative_to(REPO))
    missing = sorted(
        {
            path
            for path in (
                raw.rstrip(".:") for raw in _REPO_PATH.findall(md.read_text(encoding="utf-8"))
            )
            if not _PLACEHOLDER.search(path)
            and (rel, path) not in _HISTORICAL_PATHS
            and not (REPO / path.rstrip("/")).exists()
        }
    )
    assert not missing, (
        f"{rel} names repo paths that do not exist: {missing}. Point it at the "
        "current path (tasks/ and scenarios/ ids carry a numeric prefix, e.g. "
        "tasks/slack-spine/00-BASE-health), or, if the doc names it as history, "
        "add it to _HISTORICAL_PATHS in tools/test_doc_links.py with the reason."
    )


def test_decisions_are_numbered_in_order() -> None:
    numbers = [
        int(n)
        for n in _DECISION.findall((REPO / "docs" / "DECISIONS.md").read_text(encoding="utf-8"))
    ]
    expected = [n for n in range(1, max(numbers) + 1) if n not in _UNASSIGNED_DECISIONS]
    assert numbers == expected, (
        f"docs/DECISIONS.md entries run {numbers}; expected {expected}. Keep entries "
        "in ascending order and give a new decision the next number (AGENTS.md "
        "Rules: Decisions)."
    )


def test_docs_index_lists_every_top_level_doc() -> None:
    index = (REPO / "docs" / "README.md").read_text(encoding="utf-8")
    listed = set(re.findall(r"\]\(([A-Za-z0-9_-]+\.md)\)", index))
    present = {p.name for p in (REPO / "docs").glob("*.md")} - {"README.md"}
    assert listed == present, (
        f"docs/README.md is out of step with docs/: unlisted {sorted(present - listed)}, "
        f"listed but missing {sorted(listed - present)}. Add a live doc to the index "
        "table; move a historical one to docs/archive/ with a banner instead."
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


def test_tech_debt_entries_state_their_evidence() -> None:
    text = (REPO / "docs" / "plans" / "tech-debt.md").read_text(encoding="utf-8")
    entries = re.split(r"(?m)^- ", text)[1:]
    untagged = [
        re.match(r"\*\*(.+?)\*\*", entry).group(1) if entry.startswith("**") else entry[:40]
        for entry in entries
        if not _EVIDENCE.search(entry)
    ]
    assert entries, "docs/plans/tech-debt.md has no entries; is the list format intact?"
    assert not untagged, (
        f"tech-debt entries without an evidence tag: {untagged}. End each with "
        "'Evidence: reproduced|observed|inferred (<how>, <date>).' and use "
        "'inferred' for anything you did not check (docs/plans/tech-debt.md)."
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
