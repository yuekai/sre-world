# Tech debt

Known gaps, newest first. Remove an entry in the commit that pays it.

Each entry ends with `Evidence: <kind> (<how>, <date>).`, where kind is
`reproduced` (you ran it and saw the failure), `observed` (seen in the code, a log
or tool output) or `inferred` (reasoned, not checked). A later session acts on
these entries, so say what you saw, not what you suspect; an untested diagnosis
is `inferred`. `tools/test_doc_links.py` checks the tag.

## From the 2026-10 reconstruction

- **Unrecovered verifier profile.** `verifier/challenge_types.py` registers
  `slack_source_lock_restart_v1`, but its module never shipped in an Arena task.
  Two tests are strict `xfail` until it is rewritten
  (`verifier/test_challenge.py`, `_UNRECOVERED_PROFILE`). Only the unpublished
  build-capable `11-BC1` would select it.
  Evidence: observed (registry entry and strict xfail in the tree, 2026-10-07).
- **Health records are partial.** Three Slack base-health records carry the gating
  bands the Arena tasks resolved, not the original's full recapture (see each
  record's header). Recapture with `tools.calibrate_base` before trusting other
  fields.
  Evidence: observed (headers of the three `substrates/slack-spine/health/` records, 2026-10-07).
- **Stale design contracts.** `verifier/contracts/{06-F3,06-F4,09-I1,13-P1}*.yaml`
  still list `correct_goodput`/`service_health` as safe-repair packs; the specs now
  grade them as outcome checks. Three scenario tests tolerate this with an explicit
  mapping that should go once the contracts are updated.
  Evidence: observed (all four contracts name the packs, 2026-10-07).
- **Host-side verifier still knows undeclared finalization.**
  `verifier/host.py` (`_request_undeclared_finalization`) and
  `substrates/slack-spine/verifier/slack_spine_verifier.py` keep the removed
  endpoint path; only local debugging uses them.
  Evidence: observed (both call sites in the tree, 2026-10-07).
- **Materializer fields keep Sep 19 names.** `agent_boundary`/`config_survivor`
  now key on the declaration but still emit `submitted`; no shipped task selects
  them.
  Evidence: inferred (noted during the 2026-10 port; not re-checked).
- **Local `:dev` runs of new image refs.** `tools/local_run.py` side-loads custom
  images only; digest-pinned stock refs added after Sep 19 (codex-tools, egress
  Envoy/CoreDNS, MinIO mirror) are not side-loaded under `imagePullPolicy: Never`.
  Pinned-image runs are the documented local path (QUICKSTART §7b); the `:dev`
  loop needs those refs mapped.
  Evidence: inferred (from reading `tools/local_run.py`; no `:dev` run attempted).
- **CI still targets the original's infrastructure.** Release, calibration and
  qualification workflows reference `abundant-ai` secrets, Blacksmith runners and
  Oddish; only `task-ci`, `arena-parity` and the unit tests run on this fork.
  Evidence: observed (10 workflows reference them, 2026-10-07).
- **Dead fault surface.** `substrates/slack-spine/checks/fault_validators.py`
  still accepts `faultInit.roleGuc`, which the chart no longer renders; a scenario
  using it would inject nothing (the `values_gate` would still catch it).
  Evidence: observed (validator accepts it; no chart template mentions it, 2026-10-07).
- **Frappe and Saleor image packages are private.** The codex-tools and Slack
  packages are public; the `frappe-*` and `saleor-*` ones still refuse anonymous
  pulls. GitHub has no API for package visibility; flip them in the UI.
  Evidence: reproduced (anonymous manifest fetch of every pinned ref, 2026-10-07).
