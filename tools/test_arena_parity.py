"""The README Arena catalog is built from Arena task names, numbered first."""

from __future__ import annotations

from pathlib import Path

from tools.arena_parity import arena_catalog


def test_catalog_maps_arena_numbers_to_committed_tasks(tmp_path: Path) -> None:
    for name in (
        "007--slack-spine--06-F3-split-sequencer-eef5b438",
        "07-writes-and-queue-oom-f1db8f42",  # published without a number
        "000--frappe--07-deletes-and-jobs-fail-194d9279",
    ):
        (tmp_path / "tasks" / name).mkdir(parents=True)
    (tmp_path / "tasks" / "manifest.json").write_text("{}")

    assert arena_catalog(tmp_path) == [
        "| 000 | `tasks/frappe/07-deletes-and-jobs-fail` |",
        "| 007 | `tasks/slack-spine/06-F3-split-sequencer` |",
        "| — | `tasks/frappe/07-writes-and-queue-oom` |",
    ]
