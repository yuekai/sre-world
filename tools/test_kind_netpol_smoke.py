"""The Kind NetworkPolicy smoke stays wired to the chart's real policy."""

from __future__ import annotations

import shutil

import pytest

from tools import kind_netpol_smoke as smoke

pytestmark = pytest.mark.skipif(not shutil.which("helm"), reason="helm not on PATH")


def test_fixtures_follow_the_rendered_policy() -> None:
    policy = smoke.exporter_policy()
    objects = {obj["metadata"]["name"]: obj for obj in smoke.fixtures(policy)}

    def component(name: str) -> str:
        return objects[name]["metadata"]["labels"]["app.kubernetes.io/component"]

    selector = policy["spec"]["podSelector"]["matchLabels"]["app.kubernetes.io/component"]
    allowed = policy["spec"]["ingress"][0]["from"][0]["podSelector"]["matchLabels"]
    port = policy["spec"]["ingress"][0]["ports"][0]["port"]

    assert policy["spec"]["policyTypes"] == ["Ingress"]
    assert component("policed") == selector
    assert component("allowed") == allowed["app.kubernetes.io/component"]
    assert component("peer") not in {selector, allowed["app.kubernetes.io/component"]}
    policed = objects["policed"]["spec"]["containers"][0]
    assert policed["ports"][0]["containerPort"] == port
    assert "readinessProbe" in policed
