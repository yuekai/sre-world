# SRE-World

A benchmark that seeds faults into live systems (Slack-like, Frappe/ERPNext, Saleor)
and grades whether an agent removed the root cause. This repo is a reconstruction of
the offline `abundant-ai/sre-world`; see [README § About this mirror](README.md#about-this-mirror).

## Map

| Path | What it is | Read |
|---|---|---|
| `substrates/<name>/` | A healthy system-under-test: Helm chart, service source, loadgen, `checks/`, `substrate.yaml` manifest, `images.lock.json` | [docs/SUBSTRATE-INTERFACE.md](docs/SUBSTRATE-INTERFACE.md) |
| `scenarios/<substrate>/<id>/` | One fault, hand-authored: `spec.yaml`, `instruction.md`, `solve.sh`, `ground-truth.yaml` (+ optional `task_verifier/`, `layer/`) | [CONTRIBUTING.md §3](CONTRIBUTING.md) |
| `tasks/<substrate>/<id>/` | **Generated** Harbor tasks. Never edit by hand | [docs/DECISIONS.md D22](docs/DECISIONS.md) |
| `verifier/` | The deterministic grader stamped into every task as `tests/verifier/` (v1 `oracle*/` remains for base-health harnesses) | [verifier/README.md](verifier/README.md) |
| `loadgen-common/` | Shared load generator + grader HTTP plane baked into every loadgen image | [docs/LOADGEN-PROFILES.md](docs/LOADGEN-PROFILES.md) |
| `tools/generate_tasks.py` | Scenario → task generator (flagless, deterministic) | [CONTRIBUTING.md §4](CONTRIBUTING.md) |
| `tools/task_dev/` | Change-aware entry point for task work | [QUICKSTART.md](QUICKSTART.md) |
| `tools/arena_parity.py` | Gate: regenerated tasks must equal the Incident Arena tasks | [docs/plans/2026-10-07-arena-parity.md](docs/plans/2026-10-07-arena-parity.md) |
| `tools/arena_backport.py` | Recovers scenario sources from Arena task outputs | same plan |
| `ci_checks/` | Advisory LLM task-quality rubric | [ci_checks/README.md](ci_checks/README.md) |
| `docs/README.md` | Index of live docs; `docs/archive/` is historical and not maintained | — |
| `docs/DECISIONS.md` | Why things are the way they are (design log, ascending; D17 unassigned) | — |
| `docs/plans/` | Execution plans and the tech-debt list | [docs/plans/README.md](docs/plans/README.md) |

Tasks are often named by Incident Arena number ("task 007"); the table in
[README § Scenario catalog](README.md#scenario-catalog) maps each number to its `tasks/` path.

## Commands

```bash
uv sync --group dev                                   # once; pins Harbor and test deps
uv run python -m tools.task_dev <substrate>/<id>      # regenerate one task + scoped checks
uv run python -m tools.generate_tasks --all [--check] # regenerate / verify all committed tasks
./validate.sh smoke                                   # cluster-free structural gates (~1 min)
./validate.sh arena                                   # parity with Incident Arena (fetches the pinned checkout)
./validate.sh kind                                    # Kind cluster enforces NetworkPolicy as charts assume (Docker, ~1 min)
uv run pytest -q                                      # unit tests
git config core.hooksPath .githooks                   # once per clone: pre-commit gates
```

Running tasks on a cluster (local Kind, pinned images, Daytona/Oddish):
[QUICKSTART.md](QUICKSTART.md).

## Rules

- **Toolchain:** `uv` for Python (the repo is uv-locked), `helm` 3.x/4.x and `kind`
  on PATH for rendering and local runs. Do not introduce pip/conda/poetry.
- **Never hand-edit `tasks/`.** Change `substrates/`, `scenarios/`, `verifier/` or
  `tools/`, then regenerate; `generate_tasks --all --check` and the pre-commit hook
  enforce this.
- **Arena parity is a regression gate.** Any change that alters a generated Arena
  task must either keep `./validate.sh arena` green or add an
  `ALLOWED_DIFFERENCES` entry in `tools/arena_parity.py` with its reason.
- **Calibrated bands are measurements.** Never edit thresholds or health records to
  make a gate pass; recalibrate (CONTRIBUTING §5).
- **Plans:** write `docs/plans/YYYY-MM-DD-<slug>.md` for any change beyond one
  file, and keep its `Status:` line current. Record new debt in
  `docs/plans/tech-debt.md`.
- **Decisions:** a change to the model (grading contract, task format, substrate
  interface) gets a `D<n>` entry in `docs/DECISIONS.md`.
