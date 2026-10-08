"""arena_parity — committed tasks/ must reproduce the Incident Arena task set.

The original abundant-ai/sre-world repository went offline in October 2026. Its
last generated output survives as the 20 Harbor tasks published in
https://github.com/abundant-ai/incident-arena (synced from Oddish on
2026-09-30, commit fba011e). This gate compares every file of every Arena task
against this repository's committed tasks/<substrate>/<id>/ using the sha256
list in the Arena's tasks/manifest.json, so a reconstruction that drifts from
the original's output fails loudly.

`generate_tasks --all --check` separately proves the committed tasks/ tree is
what the generator emits, so together the two gates show the generator
reproduces the original's output.

Known, explained differences live in ALLOWED_DIFFERENCES below; each entry
names the tasks and files it covers and why. Anything else is a FAIL.

    uv run --frozen python -m tools.arena_parity --arena ../incident-arena [--task 007] [--diff]
"""

from __future__ import annotations

import argparse
import difflib
import hashlib
import json
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
TASKS_DIR = REPO_ROOT / "tasks"
SCENARIOS_DIR = REPO_ROOT / "scenarios"

# Arena task dirs are `<NNN>--<substrate>--<scenario>-<hash8>`; one Frappe task
# was published without the prefix (`07-writes-and-queue-oom-<hash8>`).
_ARENA_NAME = re.compile(
    r"^(?:(?P<num>\d{3})--(?P<substrate>[a-z0-9-]+?)--)?(?P<scenario>.+)-(?P<hash>[0-9a-f]{8})$"
)

# Byte rewrites applied to OUR files before hashing, so re-hosted image
# references compare equal to the Arena's originals.
REGISTRY_REWRITES: list[tuple[bytes, bytes]] = []


@dataclass(frozen=True)
class Allowed:
    """A documented difference the gate tolerates."""

    tasks: frozenset[str]  # Arena task numbers ("008") or scenario ids; empty = all
    paths: frozenset[str]  # task-relative paths
    reason: str


ALLOWED_DIFFERENCES: list[Allowed] = [
    Allowed(
        tasks=frozenset({"000", "001", "002", "003", "004", "006", "007", "009", "010", "011", "012", "013"}),
        paths=frozenset(
            {
                "tests/verifier/closure.py",
                "tests/verifier/contract.py",
                "tests/verifier/providers/redis_state.py",
            }
        ),
        reason=(
            "Stamped before the verifier gained the service_identity materializer and "
            "redis_state's accepted-email check (first seen in task 005); this repo ships "
            "the superset, which is additive for these tasks."
        ),
    ),
    Allowed(
        tasks=frozenset({"008", "014", "015", "016", "017", "018", "019"}),
        paths=frozenset(
            {
                "tests/verifier/challenge.py",
                "tests/verifier/challenge_types.py",
                "tests/verifier/checks.py",
                "tests/verifier/closure.py",
                "tests/verifier/contract.py",
                "environment/chart/templates/obs.yaml",
                "environment/chart/files/verifier-evidence-entrypoint.py",
            }
        ),
        reason=(
            "Stamped from an older generator snapshot (deadline-freeze receipt normalized "
            "differently, fewer challenge profiles, older obs NetworkPolicies and evidence "
            "entrypoint); this repo reproduces the newest snapshot (tasks 007, 009-013)."
        ),
    ),
    Allowed(
        tasks=frozenset({"07-writes-and-queue-oom"}),
        paths=frozenset({"tests/verifier/challenge_types.py"}),
        reason=(
            "Task 005 was cut from a branch that predates the shared-store challenge "
            "profiles; the newest challenge_types.py is a superset."
        ),
    ),
]


@dataclass
class TaskReport:
    arena_name: str
    ours: Path | None
    missing: list[str] = field(default_factory=list)  # in Arena, not ours
    extra: list[str] = field(default_factory=list)  # ours, not in Arena
    changed: list[str] = field(default_factory=list)
    allowed: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.ours is not None and not (self.missing or self.extra or self.changed)


def resolve_task(arena_name: str) -> tuple[str, str] | None:
    """Map an Arena task dir name to (substrate, scenario id)."""
    m = _ARENA_NAME.match(arena_name)
    if not m:
        return None
    scenario = m["scenario"]
    if m["substrate"]:
        return m["substrate"], scenario
    owners = [p.parent.name for p in SCENARIOS_DIR.glob(f"*/{scenario}") if p.is_dir()]
    if len(owners) != 1:
        return None
    return owners[0], scenario


def _task_key(arena_name: str) -> set[str]:
    m = _ARENA_NAME.match(arena_name)
    return {k for k in (m["num"], m["scenario"]) if k} if m else set()


def _is_allowed(arena_name: str, rel: str) -> Allowed | None:
    keys = _task_key(arena_name)
    for a in ALLOWED_DIFFERENCES:
        if (not a.tasks or keys & a.tasks) and rel in a.paths:
            return a
    return None


def _our_bytes(path: Path) -> bytes:
    data = path.read_bytes()
    for old, new in REGISTRY_REWRITES:
        data = data.replace(old, new)
    return data


def _is_cache(rel: str) -> bool:
    return "__pycache__" in rel.split("/") or rel.endswith((".pyc", ".pyo"))


def compare_task(arena_root: Path, entry: dict) -> TaskReport:
    name = entry["task_id"]
    resolved = resolve_task(name)
    ours = TASKS_DIR / resolved[0] / resolved[1] if resolved else None
    report = TaskReport(name, ours if ours and ours.is_dir() else None)
    if report.ours is None:
        return report

    expected = {f["path"]: f["sha256"] for f in entry["files"]}
    actual = {
        p.relative_to(report.ours).as_posix(): p
        for p in report.ours.rglob("*")
        if p.is_file() and not _is_cache(p.relative_to(report.ours).as_posix())
    }
    for rel in sorted(expected.keys() | actual.keys()):
        if rel not in actual:
            bucket = report.missing
        elif rel not in expected:
            bucket = report.extra
        elif hashlib.sha256(_our_bytes(actual[rel])).hexdigest() != expected[rel]:
            bucket = report.changed
        else:
            continue
        if _is_allowed(name, rel):
            report.allowed.append(rel)
        else:
            bucket.append(rel)
    return report


def _print_diff(arena_task: Path, ours: Path, rel: str, limit: int) -> None:
    a = (arena_task / rel).read_bytes()
    b = _our_bytes(ours / rel)
    try:
        lines = list(
            difflib.unified_diff(
                a.decode().splitlines(),
                b.decode().splitlines(),
                f"arena/{rel}",
                f"ours/{rel}",
                lineterm="",
            )
        )
    except UnicodeDecodeError:
        print(f"      (binary file differs: {rel})")
        return
    for line in lines[:limit]:
        print(f"      {line}")
    if len(lines) > limit:
        print(f"      … {len(lines) - limit} more diff lines")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--arena", type=Path, required=True, help="incident-arena checkout")
    ap.add_argument("--task", action="append", default=[], help="substring filter on task name")
    ap.add_argument("--diff", action="store_true", help="print a unified diff for changed files")
    ap.add_argument("--diff-lines", type=int, default=40)
    args = ap.parse_args(argv)

    manifest_path = args.arena / "tasks" / "manifest.json"
    if not manifest_path.is_file():
        ap.error(f"{manifest_path}: not an incident-arena checkout")
    manifest = json.loads(manifest_path.read_text())
    entries = [
        e
        for e in manifest["tasks"]
        if not args.task or any(t in e["task_id"] for t in args.task)
    ]
    if not entries:
        ap.error("no Arena task matches the filter")

    failed = 0
    for entry in entries:
        r = compare_task(args.arena, entry)
        if r.ours is None:
            print(f"  ✗ {r.arena_name}: no committed task (expected tasks/<substrate>/<id>)")
            failed += 1
            continue
        rel_ours = r.ours.relative_to(REPO_ROOT)
        note = f" ({len(r.allowed)} allowed)" if r.allowed else ""
        if r.ok:
            print(f"  ✓ {r.arena_name} == {rel_ours}{note}")
            continue
        failed += 1
        print(
            f"  ✗ {r.arena_name} vs {rel_ours}: {len(r.changed)} changed, "
            f"{len(r.missing)} missing, {len(r.extra)} extra{note}"
        )
        for label, items in (("changed", r.changed), ("missing", r.missing), ("extra", r.extra)):
            for rel in items:
                print(f"    {label}: {rel}")
                if args.diff and label == "changed":
                    _print_diff(args.arena / "tasks" / r.arena_name, r.ours, rel, args.diff_lines)

    total = len(entries)
    print(f"arena_parity: {total - failed}/{total} task(s) match")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
