Status: completed

# Separate live docs from historical ones

## Problem

`docs/` held 17 top-level docs (7.4k lines). Guidance that describes how this
repo works today sat beside runbooks for the original's infrastructure
(Blacksmith runner pool, `/calibrate`, Oddish), dated evidence snapshots and an
August backlog, with nothing telling an agent which is which. AGENTS.md linked
only some of them.

## Approach

- Live docs stay in `docs/` and are listed in a new `docs/README.md` index.
- Historical docs move to `docs/archive/` with a banner saying why, following the
  archive's existing convention; links into and out of them are rewritten.
- `tools/test_doc_links.py` requires every top-level doc to appear in the index,
  so a new doc has to be classified when it lands.

## Classification

| Doc | Kind | Why |
|---|---|---|
| DECISIONS, SUBSTRATE-INTERFACE, LOADGEN-PROFILES, AGENT-SURFACES | live | contracts and references for current code |
| AUTHORING-FRAPPE, TASK-QUALITY-RUBRIC, TASK-DESIGN-LEARNINGS, KILL-LEDGER, LESSONS | live | authoring guidance and records an author consults now |
| FACTORY, FACTORY-WORKFLOW, FACTORY-PLAYBOOK, BUILDS_ARCHITECTURE | archive | runbooks for the original's CI and runner pool, which do not run on this fork |
| CAPTURE-SURFACE-GAP, HOW-AGENTS-WIN-AND-LOSE | archive | dated analyses of a task corpus mostly retired in the reconstruction |
| DIFFICULTY-PROTOCOL, TASK-QUALITY-IMPROVEMENTS | archive | an unexecuted trial protocol and an August backlog on the original's infrastructure |

## Progress

- [x] Move, banner and relink (8 docs; links into and out of them rewritten)
- [x] Index and test; link checking now also covers `docs/archive/` and `docs/research/`
