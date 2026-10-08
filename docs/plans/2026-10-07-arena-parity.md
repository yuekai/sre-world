Status: active

# Reconstruct the original to Incident Arena parity

## Goal

`abundant-ai/sre-world` went offline in October 2026. Bring this fork from its
Jul 21 snapshot to the state that generated the 20 Incident Arena tasks
(`abundant-ai/incident-arena@fba011e`), so every Arena task regenerates from this
repo and runs.

## Approach

1. Fast-forward to the original's history through 2026-09-19 from the public fork
   `mrshu/sre-world` (`28d44ce`, PR #488).
2. Build a parity gate first (`tools/arena_parity.py`: per-file sha256 against the
   Arena `manifest.json`), then close the Sep 19–28 gap diff by diff.
3. Recover authored sources from generated outputs (`tools/arena_backport.py`),
   chart edits by 3-way merge (Sep 19 source, Sep 19 task, Arena task), runtime
   code from the source layers of the original's published images.
4. Re-host every pinned image under `ghcr.io/yuekai/sre-world` by digest.
5. Run pinned-image trials locally to show the tasks run.

## Progress

- [x] Fast-forward `main` to `28d44ce` (provenance checked: no fork-owner commits,
      GitHub-signed merges, Arena answer-key SHAs in history).
- [x] Parity gate and back-port tool.
- [x] Verifier moved to `verifier/`; generator output (task.toml, test.sh,
      survivor wiring, window-derived load profiles) reconstructed.
- [x] 35 scenarios on the Sep 19 report-graded contract retired.
- [x] Loadgen, sidecar and `main` source recovered from images; Saleor 10-T1 added.
- [x] Images re-hosted (136 tags, digests verified); locks repointed.
- [x] Parity 20/20; Go sources rebuild byte-identical to the v21 `slack-go` image.
- [x] Unit tests updated to the recovered behaviour (1911 pass); `./validate.sh smoke`
      green including the new `arena` gate.
- [ ] Local oracle/nop trials per substrate on Kind with pinned images — blocked:
      Kind's NetworkPolicy enforcement drops the Slack exporter's probes and DNS
      (tech-debt.md); amd64 images do run under Rosetta inside Kind.
- [ ] Packages made public (GitHub UI only), then hosted runs.

## Decision log

- **Generator output over authored guesses.** Where a value could be either
  authored or derived, the version that lets the original's unchanged Sep 19
  spec regenerate the Arena bytes wins (e.g. 09-I1's layer fingerprint proved load
  profiles are generator-normalized from `agent_window_s`, not authored).
- **Newest snapshot is canonical.** Arena tasks were stamped at different times;
  the repo reproduces the newest, and older-snapshot drift is listed in
  `ALLOWED_DIFFERENCES` with reasons rather than special-cased.
- **Retire, don't convert, unverifiable scenarios.** Converting the 35 old-contract
  scenarios would have invented behaviour; deletion follows the original's #205.
- **Copy image source verbatim.** When images disagree the newest wins
  (frappe v29 > slack v21 > saleor v9 > frappe scratch); nothing is rewritten.
- **Pin what upstream left floating.** pnpm 11.24.0 (read from the v21 builder's
  `.modules.yaml`) and MinIO from the original's own registry mirror.
- **Declare grace per substrate** (`harbor.declare_grace_s`: 90 s, Saleor 50 s) —
  the only constant the Arena outputs require that no surviving source states.

See [tech-debt.md](tech-debt.md) for what remains unknown or unrecovered.
