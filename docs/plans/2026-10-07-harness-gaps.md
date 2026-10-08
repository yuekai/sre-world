Status: completed

# Close harness-audit gaps 2, 3, 4 and 7

A harness-engineering audit on 2026-10-07 ranked seven gaps. This plan closed the
four small ones; the others are below.

## Done

- **Arena-number map (gap 2).** People name tasks by Arena number ("task 007"),
  which appeared only in `.cache/incident-arena` directory names. README §
  Scenario catalog now maps each number to its `tasks/` path, and
  `tools/arena_parity.py` fails (printing the correct rows) when the table and the
  Arena checkout disagree. The pre-commit hook now runs that gate when README.md
  is staged and checks its exit status instead of grepping for `20/20`.
- **Dead paths in live docs (gap 3).** README and QUICKSTART named
  `tasks/slack-spine/seq-lock-leak` and `tasks/slack-spine/base-health`, which no
  longer exist. `tools/test_doc_links.py` now checks repo paths written anywhere
  in AGENTS/README/QUICKSTART/CONTRIBUTING/verifier README, not just markdown
  links; historical docs under `docs/` are exempt.
- **Evidence on debt entries (gap 4).** A tech-debt entry stated an untested
  diagnosis (kindnet dropping kubelet probes) as fact, and the next session
  started from it. Each entry now ends with `Evidence: reproduced|observed|inferred`,
  and the doc-link test enforces the tag.
- **Decision log order (gap 7).** `docs/DECISIONS.md` ran D1–5, D14–25, D13→D6
  and never had a D17. Entries are now ascending (content unchanged), the gap is
  noted, and a test keeps the numbering ordered.

## Later

The other three gaps were closed afterwards: gap 1 (local runs under `jobs/`
broke `verifier/test_grader_parity.py`) in `f503378`, gap 5 (`./validate.sh kind`,
`.github/workflows/kind-netpol.yaml`) in `4fa7200`, and gap 6 in
[2026-10-07-docs-live-vs-archive.md](2026-10-07-docs-live-vs-archive.md).
