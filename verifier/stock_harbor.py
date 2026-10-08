"""Render the exact stock-Harbor transport and reward path for v2 tasks.

Two transports exist: substrates that mount the grader capability as a file
(token) and substrates that reach the grader only through a root-only Unix
socket (broker). The broker form additionally records why a verification
aborted, so a zero reward from an abort is distinguishable from a graded zero.
"""

from __future__ import annotations

from pathlib import Path

_TEMPLATES = Path(__file__).resolve().parent / "templates"

# test.sh must give up before Harbor kills the verifier exec, so its evidence
# poll budget leaves this much of verifier_timeout_sec for bundle + grading.
_GRADING_MARGIN_S = 180


def render_test_sh(
    verifier_timeout_sec: float, grader_url: str, *, broker: bool = False
) -> str:
    budget = int(verifier_timeout_sec - _GRADING_MARGIN_S)
    if budget < 60:
        raise ValueError(
            f"verifier_timeout_sec={verifier_timeout_sec} leaves a poll budget of "
            f"{budget}s (<60s)"
        )
    template = (_TEMPLATES / ("test_broker.sh" if broker else "test_token.sh")).read_text()
    return template.replace("__POLL_BUDGET_S__", str(budget)).replace(
        "__GRADER_URL__", grader_url
    )
