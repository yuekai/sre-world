# Tech debt

Known gaps, newest first. Remove an entry in the commit that pays it.

## From the 2026-10 reconstruction

- **Unrecovered verifier profile.** `verifier/challenge_types.py` registers
  `slack_source_lock_restart_v1`, but its module never shipped in an Arena task.
  Two tests are strict `xfail` until it is rewritten
  (`verifier/test_challenge.py`, `_UNRECOVERED_PROFILE`). Only the unpublished
  build-capable `11-BC1` would select it.
- **Health records are partial.** Three Slack base-health records carry the gating
  bands the Arena tasks resolved, not the original's full recapture (see each
  record's header). Recapture with `tools.calibrate_base` before trusting other
  fields.
- **Stale design contracts.** `verifier/contracts/{06-F3,06-F4,09-I1,13-P1}*.yaml`
  still list `correct_goodput`/`service_health` as safe-repair packs; the specs now
  grade them as outcome checks. Three scenario tests tolerate this with an explicit
  mapping that should go once the contracts are updated.
- **Host-side verifier still knows undeclared finalization.**
  `verifier/host.py` (`_request_undeclared_finalization`) and
  `substrates/slack-spine/verifier/slack_spine_verifier.py` keep the removed
  endpoint path; only local debugging uses them.
- **Materializer fields keep Sep 19 names.** `agent_boundary`/`config_survivor`
  now key on the declaration but still emit `submitted`; no shipped task selects
  them.
- **Slack tasks don't start on Kind.** The post-Sep-19 telemetry NetworkPolicies
  in `substrates/slack-spine/chart/templates/obs.yaml` assume k3s semantics; on Kind
  (kindnet) `postgres-exporter`'s probes and DNS replies are dropped and the install
  times out (seen twice running Arena task 007 with pinned images, 2026-10-07). Fix
  by matching the hosted network-policy implementation in the Kind environment
  (`tools/run_verifier_v2_matrix.py` SlackSpineKindHelmEnvironment) or by running
  hosted.
- **Local `:dev` runs of new image refs.** `tools/local_run.py` side-loads custom
  images only; digest-pinned stock refs added after Sep 19 (codex-tools, egress
  Envoy/CoreDNS, MinIO mirror) are not side-loaded under `imagePullPolicy: Never`.
  Pinned-image runs are the documented local path (QUICKSTART §7b); the `:dev`
  loop needs those refs mapped.
- **CI still targets the original's infrastructure.** Release, calibration and
  qualification workflows reference `abundant-ai` secrets, Blacksmith runners and
  Oddish; only `task-ci`, `arena-parity` and the unit tests run on this fork.
- **Dead fault surface.** `substrates/slack-spine/checks/fault_validators.py`
  still accepts `faultInit.roleGuc`, which the chart no longer renders; a scenario
  using it would inject nothing (the `values_gate` would still catch it).
- **Image packages are private.** GitHub offers no API to change container package
  visibility; set `sre-world/*` packages public in the UI for anonymous pulls.
