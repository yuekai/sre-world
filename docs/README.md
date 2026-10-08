# Docs

Live docs describe how this repository works today and are kept current. Every
top-level doc here is listed below; `tools/test_doc_links.py` fails if one is
missing, so a new doc must be added to this index.

| Doc | What it is |
|---|---|
| [DECISIONS.md](DECISIONS.md) | Design log: why things are the way they are (D1–D25) |
| [SUBSTRATE-INTERFACE.md](SUBSTRATE-INTERFACE.md) | What a substrate must implement: manifest, checks, image build |
| [LOADGEN-PROFILES.md](LOADGEN-PROFILES.md) | Load profiles: loop mode, traffic noise, YAML-defined patterns |
| [AGENT-SURFACES.md](AGENT-SURFACES.md) | The `agent_surface` task variable: confined, shell-visible, build-capable |
| [AUTHORING-FRAPPE.md](AUTHORING-FRAPPE.md) | Frappe-specific authoring contracts, companion to CONTRIBUTING §3–§7 |
| [TASK-QUALITY-RUBRIC.md](TASK-QUALITY-RUBRIC.md) | Checklist for scoring a scenario draft before spending compute |
| [TASK-DESIGN-LEARNINGS.md](TASK-DESIGN-LEARNINGS.md) | Task-design lessons measured in trials, for authors on any substrate |
| [KILL-LEDGER.md](KILL-LEDGER.md) | Refuted Frappe fault candidates, so they are not retried |
| [LESSONS.md](LESSONS.md) | Durable lessons from building and running the benchmark |

Elsewhere in `docs/`:

- [plans/](plans/README.md): execution plans and the tech-debt list.
- [research/](research/): notes on source papers and task-archetype reviews.
- [archive/](archive/README.md): historical documents, kept for the record and not
  maintained. Each carries a banner saying what it described and why it was
  archived; do not follow its procedures without checking them against the
  live docs.
