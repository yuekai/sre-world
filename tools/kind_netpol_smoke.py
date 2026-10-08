"""kind_netpol_smoke — the trusted Kind cluster enforces NetworkPolicy correctly.

Slack, Frappe and Saleor tasks put Ingress NetworkPolicies on telemetry and
agent-boundary pods. Under kind's default CNI (kindnet, kind v0.32) replies to
those pods are dropped, so DNS fails and Helm times out; the trusted Kind
environment therefore runs Calico (docs/plans/2026-10-07-kind-calico.md). This
smoke builds a cluster exactly as that environment does, applies the chart's own
`postgres-exporter-ingress` policy to stand-in pods, and checks the five
behaviours a bring-up depends on. It needs Docker, kind, kubectl and helm.

    uv run python -m tools.kind_netpol_smoke [--name sre-world-netpol] [--keep]
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import tempfile
from pathlib import Path

import yaml

from tools.run_verifier_v2_matrix import SlackSpineKindLauncher, fetch_calico_manifest

REPO_ROOT = Path(__file__).resolve().parent.parent
CHART = REPO_ROOT / "substrates" / "slack-spine" / "chart"
POLICY = "postgres-exporter-ingress"
AGNHOST = "registry.k8s.io/e2e-test-images/agnhost:2.53"
PEER_PORT = 8080
_FIX = (
    "The trusted Kind cluster no longer enforces NetworkPolicy the way the chart "
    "assumes; task bring-ups will time out. Check the CNI install in "
    "SlackSpineKindHelmEnvironment._install_calico and "
    "substrates/slack-spine/checks/kind_surface_config.yaml "
    "(docs/plans/2026-10-07-kind-calico.md)."
)


def exporter_policy() -> dict:
    """The chart's rendered exporter ingress policy, unchanged."""
    rendered = subprocess.run(
        ["helm", "template", "netpol-smoke", str(CHART)],
        check=True, capture_output=True, text=True,
    ).stdout
    for doc in yaml.safe_load_all(rendered):
        if doc and doc.get("kind") == "NetworkPolicy" and doc["metadata"]["name"] == POLICY:
            return doc
    raise SystemExit(f"kind_netpol_smoke: {CHART} no longer renders NetworkPolicy {POLICY}")


def _pod(name: str, component: str, port: int, probe: bool) -> dict:
    container = {
        "name": "web",
        "image": AGNHOST,
        "args": ["netexec", f"--http-port={port}"],
        "ports": [{"name": "http", "containerPort": port}],
    }
    if probe:
        container["readinessProbe"] = {
            "httpGet": {"path": "/healthz", "port": "http"},
            "periodSeconds": 2,
        }
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": name, "labels": {"app.kubernetes.io/component": component}},
        "spec": {"containers": [container]},
    }


def fixtures(policy: dict) -> list[dict]:
    """The policy plus a policed pod, an unlabelled peer and an allowed client."""
    selector = policy["spec"]["podSelector"]["matchLabels"]["app.kubernetes.io/component"]
    allowed = policy["spec"]["ingress"][0]["from"][0]["podSelector"]["matchLabels"][
        "app.kubernetes.io/component"
    ]
    port = policy["spec"]["ingress"][0]["ports"][0]["port"]
    return [
        policy,
        _pod("policed", selector, port, probe=True),
        _pod("peer", "netpol-smoke-peer", PEER_PORT, probe=False),
        _pod("allowed", allowed, PEER_PORT, probe=False),
    ]


class Cluster:
    def __init__(self, kubeconfig: Path) -> None:
        self.env = {**os.environ, "KUBECONFIG": str(kubeconfig)}

    def run(self, argv: list[str], *, stdin: str | None = None, check: bool = True,
            timeout: int = 900) -> subprocess.CompletedProcess[str]:
        return subprocess.run(argv, input=stdin, env=self.env, check=check,
                              capture_output=True, text=True, timeout=timeout)

    def ok(self, argv: list[str]) -> bool:
        return self.run(argv, check=False, timeout=120).returncode == 0

    def pod_ip(self, name: str) -> str:
        return self.run(["kubectl", "get", "pod", name, "-o", "jsonpath={.status.podIP}"]).stdout


def install_calico(cluster: Cluster, workdir: Path) -> None:
    manifest = workdir / "calico.yaml"
    manifest.write_bytes(fetch_calico_manifest())
    cluster.run(["kubectl", "apply", "-f", str(manifest)])
    cluster.run(["kubectl", "-n", "kube-system", "rollout", "status",
                 "daemonset/calico-node", "--timeout=600s"])
    cluster.run(["kubectl", "wait", "--for=condition=Ready", "node", "--all",
                 "--timeout=300s"])


def check(cluster: Cluster, policy: dict) -> list[tuple[str, bool]]:
    port = policy["spec"]["ingress"][0]["ports"][0]["port"]
    cluster.run(["kubectl", "apply", "-f", "-"],
                stdin=yaml.safe_dump_all(fixtures(policy)))
    cluster.run(["kubectl", "wait", "--for=condition=Ready", "pod/peer", "pod/allowed",
                 "--timeout=300s"])
    policed, peer = cluster.pod_ip("policed"), cluster.pod_ip("peer")

    def connect(src: str, dst: str) -> bool:
        return cluster.ok(["kubectl", "exec", src, "--", "/agnhost", "connect", dst,
                           "--timeout=5s"])

    return [
        ("kubelet readiness probe reaches the policed pod",
         cluster.ok(["kubectl", "wait", "--for=condition=Ready", "pod/policed",
                     "--timeout=120s"])),
        ("DNS replies reach the policed pod",
         cluster.ok(["kubectl", "exec", "policed", "--", "nslookup",
                     "kubernetes.default.svc.cluster.local"])),
        ("replies to the policed pod's own connections get back",
         connect("policed", f"{peer}:{PEER_PORT}")),
        ("an unlabelled pod is blocked from the policed port",
         not connect("peer", f"{policed}:{port}")),
        ("the policy's allowed peer reaches the policed port",
         connect("allowed", f"{policed}:{port}")),
    ]


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--name", default="sre-world-netpol", help="kind cluster name")
    ap.add_argument("--keep", action="store_true", help="leave the cluster up for debugging")
    args = ap.parse_args(argv)

    policy = exporter_policy()
    with tempfile.TemporaryDirectory() as tmp:
        workdir = Path(tmp)
        kubeconfig = workdir / "kubeconfig"
        cluster = Cluster(kubeconfig)
        launcher = SlackSpineKindLauncher(args.name, kubeconfig_path=str(kubeconfig))
        try:
            cluster.run(launcher.create_cmd())
            install_calico(cluster, workdir)
            results = check(cluster, policy)
        except subprocess.CalledProcessError as exc:
            print(f"kind_netpol_smoke: {' '.join(exc.cmd)} failed:\n{exc.stderr}", file=sys.stderr)
            return 1
        finally:
            if args.keep:
                print(f"kind_netpol_smoke: kept cluster {args.name} (KUBECONFIG={kubeconfig} "
                      "is removed on exit; use `kind get kubeconfig --name` to reach it)")
            else:
                subprocess.run(launcher.delete_cmd(), capture_output=True, check=False)

    for label, passed in results:
        print(f"  {'✓' if passed else '✗'} {label}")
    failed = sum(not passed for _, passed in results)
    if failed:
        print(f"kind_netpol_smoke: {failed} check(s) failed. {_FIX}", file=sys.stderr)
        return 1
    print(f"kind_netpol_smoke: {len(results)}/{len(results)} checks pass")
    return 0


if __name__ == "__main__":
    sys.exit(main())
