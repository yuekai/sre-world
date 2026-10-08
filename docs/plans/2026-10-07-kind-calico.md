Status: completed

# Kind environment on Calico

## Goal

Pinned-image oracle runs of Slack tasks (first: Arena task 007,
`slack-spine/06-F3-split-sequencer`) come up on local Kind.

## Problem

kindnet (kind v0.32, kindnetd v20260528) enforces NetworkPolicy but drops reply
packets to any pod under an Ingress policy, so DNS and pod-to-pod connections
opened by prometheus, loki, postgres-exporter, sequencer-config-broker and the
`agent-*` pods fail and Helm times out. A minimal repro showed Calico enforces the
same policies correctly (probes pass, replies pass, unlabelled ingress blocked).
kindnetd has no flag to disable its policy engine.

## Approach

- `substrates/slack-spine/checks/kind_surface_config.yaml`: `disableDefaultCNI`
  and `podSubnet: 10.42.0.0/16` (hosted k3s's pod CIDR, next to its service CIDR).
- `SlackSpineKindLauncher`: create without `--wait` (the node cannot go Ready
  before a CNI exists; kind would only burn the timeout).
- `SlackSpineKindHelmEnvironment._cluster_up`: install a pinned Calico manifest
  (URL + sha256), wait for calico-node and the node to be Ready.
- The chart and generated tasks are unchanged, so Arena parity is unaffected.

## Progress

- [x] Repro and A/B (kindnet vs Calico) on throwaway clusters
- [x] Config, launcher, environment, tests
- [x] Oracle run of task 007 on the new environment: reward 1.0, all ten
  required evidence packs pass (job `oracle-007-calico`, 2026-10-07)

- [x] Regression gate: `./validate.sh kind` (`tools/kind_netpol_smoke.py`), run in
  CI by `.github/workflows/kind-netpol.yaml`; it fails the two reply checks on
  kindnet and passes on Calico

## Decisions

- Fetch the manifest at cluster-up and verify its sha256 instead of vendoring
  10k lines; Kind runs already need network access to pull images.
- Calico over kube-router: it worked in the repro and is the documented kind
  path; kube-router would match hosted k3s more closely if the two diverge.
