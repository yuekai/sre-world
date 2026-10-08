"""arena_backport — recover scenario sources from the Incident Arena tasks.

The original repository's commits after 2026-09-19 are lost, but their
generated output survives as the Incident Arena tasks (see tools/arena_parity).
This tool inverts the generator's deterministic transforms to recover the
authored scenario files those tasks were stamped from:

  instruction.md       <- instruction.md (verbatim copy)
  solve.sh             <- solution/solve.sh (verbatim copy)
  ground-truth.yaml    <- environment/chart/ground-truth.yaml, minus the
                          generator-owned header, agent_boundary suffix and the
                          health_ref-resolved threshold keys

It also reports the threshold values each task resolved from its base-health
record, so stale records can be brought in line (--health).

    uv run --frozen python -m tools.arena_backport --arena ../incident-arena [--task 007] [--write]
"""

from __future__ import annotations

import argparse
import io
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

from ruamel.yaml import YAML

from tools.arena_parity import REPO_ROOT, resolve_task

_HEADER_PREFIX = "# THRESHOLDS RESOLVED from the base-health record"
_BOUNDARY_SUFFIX = (
    "\n# Required for every newly stamped terminal-declaration task.\n"
    "agent_boundary:\n  required: true\n"
)


def _yaml() -> YAML:
    # Must match tools/generate_tasks.py::_emit_ground_truth exactly.
    y = YAML()
    y.preserve_quotes = True
    y.width = 4096
    return y


def recover_ground_truth(task_gt: str) -> tuple[str, dict[str, Any]]:
    """Return (scenario ground-truth text, resolved threshold values)."""
    if not task_gt.endswith(_BOUNDARY_SUFFIX):
        raise ValueError("task ground truth lacks the generator's agent_boundary suffix")
    body = task_gt[: -len(_BOUNDARY_SUFFIX)]
    if not body.startswith(_HEADER_PREFIX):
        return body, {}
    body = body.split("\n", 2)[2]
    y = _yaml()
    doc = y.load(body)
    inherit = list((doc.get("health_ref") or {}).get("inherit") or [])
    th = doc["thresholds"]
    resolved = {key: json.loads(json.dumps(th[key])) for key in inherit}
    for key in inherit:
        del th[key]
    buf = io.StringIO()
    y.dump(doc, buf)
    return buf.getvalue(), resolved


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--arena", type=Path, required=True)
    ap.add_argument("--task", action="append", default=[])
    ap.add_argument("--write", action="store_true", help="write recovered scenario files")
    args = ap.parse_args(argv)

    manifest = json.loads((args.arena / "tasks" / "manifest.json").read_text())
    bands: dict[tuple[str, str], dict[str, list]] = defaultdict(lambda: defaultdict(list))
    status = 0
    for entry in manifest["tasks"]:
        name = entry["task_id"]
        if args.task and not any(t in name for t in args.task):
            continue
        resolved_id = resolve_task(name)
        if resolved_id is None:
            print(f"  ✗ {name}: cannot map to a scenario")
            status = 1
            continue
        sub, sid = resolved_id
        task = args.arena / "tasks" / name
        scenario = REPO_ROOT / "scenarios" / sub / sid
        gt_text, resolved = recover_ground_truth(
            (task / "environment" / "chart" / "ground-truth.yaml").read_text()
        )
        recovered = {
            "instruction.md": (task / "instruction.md").read_text(),
            "solve.sh": (task / "solution" / "solve.sh").read_text(),
            "ground-truth.yaml": gt_text,
        }
        if (scenario / "spec.yaml").is_file():
            import yaml

            import tomllib

            recovered["spec.yaml"] = recover_spec(
                (scenario / "spec.yaml").read_text(),
                yaml.safe_load((task / "environment" / "task.values.yaml").read_text()),
                tomllib.loads((task / "task.toml").read_text()),
                _toml_defaults(sub),
                sub,
            )
        changed = [
            f for f, text in recovered.items()
            if not (scenario / f).is_file() or (scenario / f).read_text() != text
        ]
        state = "new scenario" if not scenario.is_dir() else f"{len(changed)} file(s) differ"
        print(f"  {'→' if changed else '✓'} {name} -> scenarios/{sub}/{sid}: {state} {changed or ''}")
        if resolved:
            profile = _profile(task)
            for key, val in resolved.items():
                bands[(sub, profile)][key].append((name[:3], val))
        if args.write and changed:
            scenario.mkdir(parents=True, exist_ok=True)
            for f in changed:
                (scenario / f).write_text(recovered[f])
                if f == "solve.sh":
                    (scenario / f).chmod(0o755)

    print("resolved health bands by (substrate, profile):")
    for (sub, profile), keys in sorted(bands.items()):
        for key, vals in keys.items():
            distinct = {json.dumps(v, sort_keys=True) for _, v in vals}
            flag = "" if len(distinct) == 1 else "  <-- INCONSISTENT"
            print(f"  {sub}/{profile} {key}: {sorted(distinct)} from {[t for t, _ in vals]}{flag}")
    return status


_SPEC_NOTE = (
    "# RECONSTRUCTED 2026-10: values below were brought in line with the Incident\n"
    "# Arena task generated from this scenario (tools/arena_backport.py). Comments\n"
    "# may still describe the 2026-09-19 version of the fault.\n"
)
# loadgen keys generate_tasks derives from spec metadata or adds itself
# (frappe's loadgen.enabled), so a spec never authors them.
_GENERATOR_LOADGEN_KEYS = {"profile", "scrapeServices", "workerSnapshotServices", "enabled"}


def _leaves(node: Any, prefix: tuple = ()) -> dict[tuple, Any]:
    """Flatten nested mappings to {path: leaf}; lists and scalars are leaves."""
    if isinstance(node, dict) and node:
        out: dict[tuple, Any] = {}
        for key, value in node.items():
            out.update(_leaves(value, prefix + (key,)))
        return out
    return {prefix: node}


def _plain(value: Any) -> Any:
    return json.loads(json.dumps(value))


_YAML11_BOOLISH = {"y", "n", "yes", "no", "on", "off", "true", "false"}


def _scalar(value: Any) -> Any:
    """Keep strings strings for the generator's YAML 1.1 (PyYAML) reader."""
    from ruamel.yaml.scalarstring import LiteralScalarString, SingleQuotedScalarString

    if isinstance(value, list):
        return [_scalar(v) for v in value]
    if isinstance(value, str):
        if "\n" in value:
            return LiteralScalarString(value)
        if value.lower() in _YAML11_BOOLISH:
            return SingleQuotedScalarString(value)
    return value


def _set(doc: Any, path: tuple, value: Any) -> None:
    from ruamel.yaml.comments import CommentedMap

    node = doc
    for key in path[:-1]:
        if key not in node or not isinstance(node[key], dict):
            node[key] = CommentedMap()
        node = node[key]
    node[path[-1]] = value


def _delete(doc: Any, path: tuple) -> None:
    node = doc
    for key in path[:-1]:
        node = node[key]
    del node[path[-1]]
    # prune now-empty parents
    for depth in range(len(path) - 1, 0, -1):
        parent = doc
        for key in path[: depth - 1]:
            parent = parent[key]
        if parent[path[depth - 1]] == {}:
            del parent[path[depth - 1]]
        else:
            break


def _metadata_targets(task_toml: dict[str, Any], defaults: dict[str, Any]) -> dict[str, Any]:
    """Spec task.metadata values implied by a task.toml (only non-defaults)."""
    meta, env, kwargs = task_toml["metadata"], task_toml["environment"], task_toml["environment"]["kwargs"]
    want: dict[str, Any] = {
        "causal_distance": meta["causal_distance"],
        "temporal_emergence": meta["temporal_emergence"],
        "fault_presentation": meta["fault_presentation"],
        "agent_timeout_sec": task_toml["agent"]["timeout_sec"],
        "verifier_timeout_sec": task_toml["verifier"]["timeout_sec"],
    }
    for key in ("agent_window_s", "soak_s"):
        if key in meta:
            want[key] = meta[key]
    optional = {
        "build_timeout_sec": env["build_timeout_sec"],
        "ready_timeout_sec": kwargs["ready_timeout_sec"],
        "helm_timeout": kwargs["helm_timeout"],
        "healthcheck_retries": env["healthcheck"]["retries"],
        "cpus": env["cpus"],
        "memory_mb": env["memory_mb"],
        "storage_mb": env["storage_mb"],
    }
    for key, value in optional.items():
        if value != defaults.get(key):
            want[key] = value
    command = env["healthcheck"]["command"]
    if "episode-ready" in command or "check_episode_ready" in command:
        want["episode_ready_gate"] = True
    return want


_PROFILES_PATH = ("loadgen", "profilesYaml")


def _recover_window_profile(
    want: dict[tuple, Any],
    spec_text: str,
    profile_name: str,
    task_toml: dict[str, Any],
    sub_name: str | None,
) -> None:
    """Replace the task's generated profile text with what the spec must author.

    generate_tasks derives deadline, soak and (for non-looping schedules) the
    cycle list from the agent window, so the spec keeps its authored profile
    whenever normalizing it already reproduces the task; otherwise it authors
    the task's entry minus those derived keys.
    """
    import yaml

    from tools import substrate as substrate_mod

    arena_text = want.pop(_PROFILES_PATH, None)
    if arena_text is None:
        return
    sub = next(s for s in substrate_mod.discover() if s.name == sub_name)
    spec = yaml.safe_load(spec_text)
    current = None
    for block in ((spec.get("difficulty") or {}).get("values"), spec["fault"].get("values")):
        lg = (block or {}).get("loadgen") or {}
        if "profilesYaml" in lg:
            current = lg["profilesYaml"]
    meta = task_toml["metadata"]
    window, soak = float(meta["agent_window_s"]), float(meta.get("soak_s", 0.0))
    try:
        normalized = substrate_mod.normalize_window_profiles(
            sub, current, profile_name, window, soak
        )
    except SystemExit:  # the authored profile no longer names this task's profile
        normalized = None
    if normalized == arena_text:
        if current is not None:
            want[_PROFILES_PATH] = current
        return
    doc = yaml.safe_load(arena_text)
    entry = doc["profiles"][profile_name]
    for key in substrate_mod.WINDOW_DERIVED_PROFILE_KEYS:
        entry.pop(key, None)
    base = substrate_mod.substrate_profiles(sub).get(entry.get("base"))
    if not entry.get("loop", getattr(base, "loop", False)):
        entry.pop("cycles", None)
    authored = yaml.safe_dump(doc, sort_keys=False, default_flow_style=False)
    if substrate_mod.normalize_window_profiles(sub, authored, profile_name, window, soak) != arena_text:
        raise ValueError(f"{profile_name}: cannot author a profile that normalizes to the task's")
    want[_PROFILES_PATH] = authored


def recover_spec(
    spec_text: str,
    task_values: dict[str, Any],
    task_toml: dict[str, Any] | None = None,
    defaults: dict[str, Any] | None = None,
    sub_name: str | None = None,
) -> str:
    """Patch a spec so difficulty.values (+) fault.values reproduces the task's
    "fault and workload settings" section, editing each leaf where it is
    authored. Returns the smallest-diff rendering over common indent styles."""
    from ruamel.yaml.scalarstring import LiteralScalarString

    reserved = {"agentReport", "agentSurface", "gradingHarness", "global", "images", "platformTools"}
    fault_section = {k: v for k, v in task_values.items() if k not in reserved}
    loadgen = dict(fault_section.get("loadgen") or {})
    generated = {k: loadgen.pop(k) for k in list(loadgen) if k in _GENERATOR_LOADGEN_KEYS}
    if loadgen:
        fault_section["loadgen"] = loadgen
    else:
        fault_section.pop("loadgen", None)
    want = _leaves(_plain(fault_section))
    if task_toml is not None and "agent_window_s" in task_toml["metadata"]:
        _recover_window_profile(want, spec_text, generated["profile"], task_toml, sub_name)

    best: tuple[int, str] | None = None
    for seq, offset in ((2, 0), (4, 2)):
        y = _yaml()
        y.indent(mapping=2, sequence=seq, offset=offset)
        doc = y.load(spec_text)
        difficulty = (doc.get("difficulty") or {}).get("values")
        fault = doc["fault"].setdefault("values", {})
        have_diff = _leaves(_plain(difficulty or {})) if difficulty else {}
        have_fault = _leaves(_plain(fault)) if fault else {}
        for path in set(have_diff) | set(have_fault):
            if path not in want:
                _delete(fault if path in have_fault else difficulty, path)
        for path, value in want.items():
            # fault.values wins the generator's merge, so it is the authored source
            # whenever it holds the leaf; otherwise difficulty.values does.
            current = have_fault[path] if path in have_fault else have_diff.get(path, object())
            if current == value:
                continue
            if path in have_fault:
                target = fault
            elif path in have_diff:
                target = difficulty
            else:
                # New leaves: the causal fault only ever lives under faultInit
                # (runtime tier) or in fault.values when there is no difficulty
                # block; everything else is benchmark pressure.
                is_fault = path[0] == "faultInit" or difficulty is None
                target = fault if is_fault else difficulty
            _set(target, path, _scalar(value))
        metadata = doc["task"]["metadata"]
        if "profile" in generated and metadata.get("profile") != generated["profile"]:
            metadata["profile"] = generated["profile"]
        if task_toml is not None:
            for key, value in _metadata_targets(task_toml, defaults or {}).items():
                if metadata.get(key) != value:
                    metadata[key] = value
            task = doc["task"]
            scenario_key = metadata if "scenario" in metadata else task
            if scenario_key.get("scenario") != task_toml["metadata"]["scenario"]:
                scenario_key["scenario"] = task_toml["metadata"]["scenario"]
            if task["name"] != task_toml["task"]["name"]:
                task["name"] = task_toml["task"]["name"]
            if " ".join(str(task["description"]).split()) != task_toml["task"]["description"]:
                from ruamel.yaml.scalarstring import FoldedScalarString

                task["description"] = FoldedScalarString(task_toml["task"]["description"] + "\n")
        buf = io.StringIO()
        y.dump(doc, buf)
        rendered = buf.getvalue()
        if rendered != spec_text and not rendered.startswith(_SPEC_NOTE):
            rendered = _SPEC_NOTE + rendered
        import difflib

        cost = sum(1 for _ in difflib.unified_diff(spec_text.splitlines(), rendered.splitlines(), n=0))
        if best is None or cost < best[0]:
            best = (cost, rendered)
    assert best is not None
    return best[1]


def _toml_defaults(sub_name: str) -> dict[str, Any]:
    """The task.toml values generate_tasks emits when a spec sets no override."""
    from tools import substrate as substrate_mod

    sub = next(s for s in substrate_mod.discover() if s.name == sub_name)
    sizing = sub.resources("hosted")
    return {
        "build_timeout_sec": float(sub.harbor.get("build_timeout_sec", 1200.0)),
        "ready_timeout_sec": 300,
        "helm_timeout": "600s",
        "healthcheck_retries": sub.harbor["healthcheck"]["retries"],
        "cpus": sizing["cpus"],
        "memory_mb": sizing["memory_mb"],
        "storage_mb": sizing["storage_mb"],
    }


def _profile(task: Path) -> str:
    import tomllib

    meta = tomllib.loads((task / "task.toml").read_text()).get("metadata", {})
    return str(meta.get("profile"))


if __name__ == "__main__":
    sys.exit(main())
