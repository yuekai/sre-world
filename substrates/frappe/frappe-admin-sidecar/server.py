"""frappe-admin sidecar — per-workload admin HTTP surface for Frappe.

Deployed as a sidecar container in every Frappe workload pod (gunicorn, rq
short/long/default, scheduler, socketio, nginx). Provides the fix-application
primitive the operator-shell foothold (main pod) targets via ``restart-svc.sh``
— byte-identical wire shape to the Slack substrate's per-service ``/admin``
endpoints (D16).

Endpoints:

  * ``GET  /healthz``       — always 200 ``{"ok": true}``. Chart liveness probe.
  * ``GET  /admin/config``  — returns the current ``common_site_config.json``
                              content (Frappe's ``frappe.get_conf()`` equivalent
                              without importing Frappe).
  * ``GET  /admin/redis-config`` — returns a code-owned, bounded snapshot of
                              queue-safety Redis settings for protected grading
                              evidence.
  * ``POST /admin/redis-queue-repair`` — applies one bounded allowlisted queue
                              repair, persists it in the generated startup
                              script, and rolls only the queue pod.
  * ``POST /admin/redis-queue-restart`` — rolls only the queue pod for the
                              protected post-declaration durability challenge.
  * ``PUT  /admin/config``  — merges the JSON body's top-level keys into
                              ``common_site_config.json`` under a filelock, so
                              a concurrent ``bench set-config`` from another
                              pod cannot lose our write. Returns the merged
                              config.
  * ``POST /admin/reload``  — sends SIGKILL to PID 1 in the pod. Kubernetes
                              restarts the pod; the new gunicorn/rqworker
                              process re-reads ``common_site_config.json`` on
                              startup, so the config change takes effect.
                              REQUIRES ``shareProcessNamespace: true`` at the
                              pod-spec level (set by the chart).
  * ``GET  /metrics``       — Prometheus text exposition. Publishes RQ queue
                              depth by ``LLEN`` of Frappe's bench-qualified
                              Redis queue keys. No ``rq`` dep required.

Fail-loud semantics: 4xx on malformed input, 5xx on file I/O errors. No silent
degradation — the oracle attribution gate depends on our writes actually
landing.
"""
from __future__ import annotations

import asyncio
import fcntl
import json
import logging
import os
import re
import signal
import socket
import ssl
import sys
from pathlib import Path
from typing import Any

from aiohttp import ClientSession, ClientTimeout, web

# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
# Standard Frappe path — mounted from the shared `sites/` PVC. The chart wires
# this via `subPath: common_site_config.json` on a ReadWriteMany volume so both
# the workload containers AND our sidecar see the same file bytes.
COMMON_SITE_CONFIG = Path(
    os.environ.get("COMMON_SITE_CONFIG_PATH", "/home/frappe/frappe-bench/sites/common_site_config.json")
)
# Lockfile lives next to the config on the same PVC so the flock is
# cross-container-visible (fcntl.flock on the same underlying inode).
COMMON_SITE_CONFIG_LOCK = Path(
    os.environ.get("COMMON_SITE_CONFIG_LOCK", str(COMMON_SITE_CONFIG) + ".lock")
)

# Bind address / port.
ADMIN_PORT = int(os.environ.get("FRAPPE_ADMIN_PORT", "8000"))
ADMIN_HOST = os.environ.get("FRAPPE_ADMIN_HOST", "0.0.0.0")

# Queue-backing Redis (for /metrics RQ depth). The chart wires this to
# ``svc-redis-queue:6379``; unset -> /metrics returns no RQ gauges (still
# publishes the sidecar's own liveness gauge).
REDIS_QUEUE_HOST = os.environ.get("REDIS_QUEUE_HOST", "svc-redis-queue")
REDIS_QUEUE_PORT = int(os.environ.get("REDIS_QUEUE_PORT", "6379"))
REDIS_CACHE_HOST = os.environ.get("REDIS_CACHE_HOST", "svc-redis-cache")
REDIS_CACHE_PORT = int(os.environ.get("REDIS_CACHE_PORT", "6379"))
# Frappe v16 qualifies RQ queue names with the bench path.  The pinned image at
# /home/frappe/frappe-bench therefore uses Redis keys such as
# `rq:queue:home-frappe-frappe-bench:long`, not `rq:queue:long`.  Keep the
# prefix explicit/configurable so queue-depth telemetry observes the same keys
# as the web and worker processes.
RQ_QUEUE_PREFIX = os.environ.get(
    "RQ_QUEUE_PREFIX", "home-frappe-frappe-bench"
).strip(":")
# Frappe's default RQ queue set. Custom queues configured via `common_site_config.json`
# `workers` block are picked up from the workload's env dynamically, but Phase 1
# only reads the defaults; scenarios that need custom queues override this env.
RQ_QUEUES = [q.strip() for q in os.environ.get("RQ_QUEUES", "short,default,long").split(",") if q.strip()]
# Settings whose mutation can change queue durability, write availability, or
# the effective memory ceiling.  Keep this code-owned: task content must not be
# able to hide a policy change by selecting a narrower evidence surface.
REDIS_CONFIG_SETTINGS = (
    "appendfsync",
    "appendonly",
    "databases",
    "maxmemory",
    "maxmemory-clients",
    "maxmemory-policy",
    "maxmemory-samples",
    "min-replicas-max-lag",
    "min-replicas-to-write",
    "save",
    "stop-writes-on-bgsave-error",
    "tcp-keepalive",
    "timeout",
)
KUBE_API = (
    f"https://{os.environ.get('KUBERNETES_SERVICE_HOST', 'kubernetes.default.svc')}:"
    f"{os.environ.get('KUBERNETES_SERVICE_PORT_HTTPS', '443')}"
)
KUBE_NAMESPACE = os.environ.get("POD_NAMESPACE", "default")
REDIS_QUEUE_SCRIPTS_CONFIGMAP = os.environ.get(
    "REDIS_QUEUE_SCRIPTS_CONFIGMAP", ""
)
REDIS_QUEUE_POD = os.environ.get("REDIS_QUEUE_POD", "")
SERVICE_ACCOUNT = Path("/var/run/secrets/kubernetes.io/serviceaccount")
MIN_QUEUE_MEMORY = 16 * 1024 * 1024
MAX_QUEUE_MEMORY = 128 * 1024 * 1024
REDIS_RESTART_WRITE_PAUSE_MS = 30_000

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("frappe_admin")


# --------------------------------------------------------------------------- #
# Common-site-config helpers (filelock-wrapped read + merge-write)
# --------------------------------------------------------------------------- #
def _read_config_unlocked() -> dict[str, Any]:
    if not COMMON_SITE_CONFIG.is_file():
        return {}
    text = COMMON_SITE_CONFIG.read_text(encoding="utf-8").strip()
    if not text:
        return {}
    return json.loads(text)


def _merge_config_locked(patch: dict[str, Any]) -> dict[str, Any]:
    """Take an exclusive filelock, read+merge+write. Returns the merged config."""
    COMMON_SITE_CONFIG.parent.mkdir(parents=True, exist_ok=True)
    COMMON_SITE_CONFIG_LOCK.touch(exist_ok=True)
    with COMMON_SITE_CONFIG_LOCK.open("w") as lock_fh:
        fcntl.flock(lock_fh.fileno(), fcntl.LOCK_EX)
        try:
            current = _read_config_unlocked()
            merged = {**current, **patch}
            tmp = COMMON_SITE_CONFIG.with_suffix(".tmp")
            tmp.write_text(json.dumps(merged, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            tmp.replace(COMMON_SITE_CONFIG)
            return merged
        finally:
            fcntl.flock(lock_fh.fileno(), fcntl.LOCK_UN)


# --------------------------------------------------------------------------- #
# Reload primitive: SIGKILL PID 1. Requires shareProcessNamespace at pod spec.
# --------------------------------------------------------------------------- #
def _kill_pid1() -> None:
    """SIGKILL the pod's PID 1 (gunicorn / rqworker / bench schedule).

    Kubernetes RestartPolicy=Always causes the pod to relaunch immediately;
    the new PID 1 re-reads common_site_config.json on startup. Sidecar itself
    is a separate container in the same pod, so it dies with the pod and
    restarts — no supervisor loop needed.
    """
    try:
        os.kill(1, signal.SIGKILL)
    except ProcessLookupError:
        log.error("kill PID 1: no process (shareProcessNamespace not enabled?)")
        raise
    except PermissionError:
        log.error("kill PID 1: EPERM (rootless container? need capabilities?)")
        raise


# --------------------------------------------------------------------------- #
# RQ queue depth via raw Redis (no rq dep).
# --------------------------------------------------------------------------- #
async def _rq_queue_depth(queue: str) -> int | None:
    """Return the qualified Frappe RQ queue depth, or None on Redis error."""
    qualified = f"{RQ_QUEUE_PREFIX}:{queue}" if RQ_QUEUE_PREFIX else queue
    key = f"rq:queue:{qualified}"
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(REDIS_QUEUE_HOST, REDIS_QUEUE_PORT), timeout=2.0
        )
    except (OSError, asyncio.TimeoutError) as e:
        log.warning("rq depth: connect %s:%d failed: %s", REDIS_QUEUE_HOST, REDIS_QUEUE_PORT, e)
        return None
    try:
        # RESP2 inline: LLEN key\r\n
        writer.write(f"*2\r\n$4\r\nLLEN\r\n${len(key)}\r\n{key}\r\n".encode())
        await writer.drain()
        line = await asyncio.wait_for(reader.readline(), timeout=2.0)
        if not line.startswith(b":"):
            log.warning("rq depth: unexpected RESP: %r", line)
            return None
        return int(line[1:].strip())
    except Exception as e:
        log.warning("rq depth: %s", e)
        return None
    finally:
        writer.close()
        try: await writer.wait_closed()
        except Exception: pass


# --------------------------------------------------------------------------- #
# Protected Redis configuration evidence via RESP2 (no redis-py dependency).
# --------------------------------------------------------------------------- #
async def _read_resp(reader: asyncio.StreamReader) -> Any:
    line = await reader.readline()
    if not line.endswith(b"\r\n"):
        raise RuntimeError(f"truncated Redis response: {line!r}")
    kind, payload = line[:1], line[1:-2]
    if kind == b"+":
        return payload.decode("utf-8")
    if kind == b"-":
        raise RuntimeError(f"Redis command failed: {payload.decode('utf-8', errors='replace')}")
    if kind == b":":
        return int(payload)
    if kind == b"$":
        length = int(payload)
        if length == -1:
            return None
        data = await reader.readexactly(length + 2)
        if not data.endswith(b"\r\n"):
            raise RuntimeError("malformed Redis bulk response")
        return data[:-2].decode("utf-8")
    if kind == b"*":
        count = int(payload)
        if count == -1:
            return None
        return [await _read_resp(reader) for _ in range(count)]
    raise RuntimeError(f"unsupported Redis response type: {kind!r}")


def _resp_command(*parts: str) -> bytes:
    encoded = [part.encode("utf-8") for part in parts]
    return (
        f"*{len(encoded)}\r\n".encode()
        + b"".join(
            b"$" + str(len(part)).encode() + b"\r\n" + part + b"\r\n"
            for part in encoded
        )
    )


async def _redis_config(host: str, port: int) -> dict[str, str]:
    """Read the complete code-owned Redis configuration surface or fail."""
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection(host, port), timeout=2.0
    )
    try:
        writer.write(_resp_command("CONFIG", "GET", *REDIS_CONFIG_SETTINGS))
        await writer.drain()
        response = await asyncio.wait_for(_read_resp(reader), timeout=2.0)
        if not isinstance(response, list) or len(response) % 2:
            raise RuntimeError(f"CONFIG GET returned malformed response: {response!r}")
        settings = {
            str(response[index]): str(response[index + 1])
            for index in range(0, len(response), 2)
        }
        expected = set(REDIS_CONFIG_SETTINGS)
        if set(settings) != expected:
            raise RuntimeError(
                "CONFIG GET did not return the protected setting set: "
                f"missing={sorted(expected - set(settings))}, "
                f"extra={sorted(set(settings) - expected)}"
            )
        return dict(sorted(settings.items()))
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass


async def _redis_queue_config() -> dict[str, str]:
    return await _redis_config(REDIS_QUEUE_HOST, REDIS_QUEUE_PORT)


async def _redis_command_at(host: str, port: int, *parts: str) -> Any:
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection(host, port), timeout=3.0
    )
    try:
        writer.write(_resp_command(*parts))
        await writer.drain()
        return await asyncio.wait_for(_read_resp(reader), timeout=5.0)
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass


async def _redis_command(*parts: str) -> Any:
    return await _redis_command_at(
        REDIS_QUEUE_HOST, REDIS_QUEUE_PORT, *parts
    )


def _acl_rule_list(value: Any, *, field: str) -> list[str]:
    if not isinstance(value, str):
        raise RuntimeError(f"ACL GETUSER {field} must be a string")
    rules = value.split()
    if len(rules) != len(set(rules)):
        raise RuntimeError(f"ACL GETUSER {field} contains duplicate rules")
    return sorted(rules)


def _acl_sections(value: Any, *, fields: set[str], where: str) -> dict[str, Any]:
    if not isinstance(value, list) or len(value) % 2:
        raise RuntimeError(f"{where} returned malformed sections: {value!r}")
    keys = value[::2]
    if any(not isinstance(key, str) for key in keys) or len(keys) != len(set(keys)):
        raise RuntimeError(f"{where} returned invalid or duplicate section names")
    sections = dict(zip(keys, value[1::2]))
    if set(sections) != fields:
        raise RuntimeError(
            f"{where} section mismatch: missing={sorted(fields - set(sections))}, "
            f"extra={sorted(set(sections) - fields)}"
        )
    return sections


def _normalize_acl_selector(value: Any) -> dict[str, list[str]]:
    sections = _acl_sections(
        value,
        fields={"commands", "keys", "channels"},
        where="ACL GETUSER selector",
    )
    return {
        "command_rules": _acl_rule_list(sections["commands"], field="selector commands"),
        "key_rules": _acl_rule_list(sections["keys"], field="selector keys"),
        "channel_rules": _acl_rule_list(
            sections["channels"], field="selector channels"
        ),
    }


def _normalize_acl_user(value: Any, *, user: str) -> dict[str, Any]:
    sections = _acl_sections(
        value,
        fields={"flags", "passwords", "commands", "keys", "channels", "selectors"},
        where=f"ACL GETUSER {user}",
    )
    for field in ("flags", "passwords", "selectors"):
        if not isinstance(sections[field], list):
            raise RuntimeError(f"ACL GETUSER {field} must be a list")
    if any(not isinstance(item, str) or not item for item in sections["flags"]):
        raise RuntimeError("ACL GETUSER flags must contain non-empty strings")
    if any(not isinstance(item, str) or not item for item in sections["passwords"]):
        raise RuntimeError("ACL GETUSER passwords must contain non-empty strings")
    return {
        "user": user,
        "flags": sorted(sections["flags"]),
        "passwords": sorted(sections["passwords"]),
        "command_rules": _acl_rule_list(sections["commands"], field="commands"),
        "key_rules": _acl_rule_list(sections["keys"], field="keys"),
        "channel_rules": _acl_rule_list(sections["channels"], field="channels"),
        "selectors": sorted(
            (_normalize_acl_selector(item) for item in sections["selectors"]),
            key=lambda item: json.dumps(item, sort_keys=True),
        ),
    }


async def _redis_queue_acl_state(user: str = "default") -> dict[str, Any]:
    response = await _redis_command("ACL", "GETUSER", user)
    if response is None:
        raise RuntimeError(f"ACL GETUSER returned no such user: {user!r}")
    return _normalize_acl_user(response, user=user)


async def _redis_queue_safety_counters() -> dict[str, int]:
    return await _redis_safety_counters(REDIS_QUEUE_HOST, REDIS_QUEUE_PORT)


async def _redis_run_id(host: str, port: int) -> str:
    """The instance's run_id, which Redis regenerates on every start."""
    info = await _redis_command_at(host, port, "INFO", "server")
    if not isinstance(info, str):
        raise RuntimeError("Redis INFO server returned a non-string response")
    for line in info.splitlines():
        if line.startswith("run_id:"):
            run_id = line.removeprefix("run_id:").strip()
            if len(run_id) == 40 and all(c in "0123456789abcdef" for c in run_id):
                return run_id
            break
    raise RuntimeError("Redis INFO server has no well-formed run_id")


async def _redis_safety_counters(host: str, port: int) -> dict[str, int]:
    """Lifetime FLUSHALL/FLUSHDB call counts for one Redis instance.

    Counts reset when the instance restarts, so any change between captures —
    up or down — means its data was wiped.
    """
    info = await _redis_command_at(host, port, "INFO", "commandstats")
    if not isinstance(info, str):
        raise RuntimeError("Redis INFO commandstats returned a non-string response")
    counters = {"flushall": 0, "flushdb": 0}
    for line in info.splitlines():
        if not line.startswith("cmdstat_") or ":" not in line:
            continue
        command, fields = line.split(":", 1)
        name = command.removeprefix("cmdstat_")
        if name not in counters:
            continue
        calls = next(
            (
                field.removeprefix("calls=")
                for field in fields.split(",")
                if field.startswith("calls=")
            ),
            None,
        )
        if calls is None or not calls.isdigit():
            raise RuntimeError(f"malformed commandstats row for {name}: {line!r}")
        counters[name] = int(calls)
    return counters


def _memory_bytes(value: str) -> int:
    match = re.fullmatch(r"([1-9][0-9]*)(kb|mb|gb)", value)
    if match is None:
        raise ValueError("maxmemory must be a positive integer with kb/mb/gb suffix")
    multiplier = {"kb": 1024, "mb": 1024**2, "gb": 1024**3}[match.group(2)]
    return int(match.group(1)) * multiplier


def _rewrite_redis_start_script(
    script: str, *, operation: str, value: str | None
) -> str:
    """Apply one allowlisted semantic repair to the generated Redis argv."""
    if operation == "dequeue-acl":
        repaired = script
        for command in ("blpop", "blmove", "brpop"):
            repaired, count = re.subn(
                rf'(^\s*ARGS\+=\(")-{command}("\)\s*$)',
                rf"\g<1>+{command}\g<2>",
                repaired,
                flags=re.MULTILINE,
            )
            if count != 1:
                raise RuntimeError(
                    f"queue startup script has {count} deny entries for {command}"
                )
        return repaired

    flag = {
        "maxmemory": "--maxmemory",
        "min-replicas-to-write": "--min-replicas-to-write",
        "appendonly": "--appendonly",
        "appendfsync": "--appendfsync",
    }.get(operation)
    if flag is None or value is None:
        raise ValueError(f"unsupported queue repair operation {operation!r}")
    pattern = (
        rf'(^\s*ARGS\+=\("{re.escape(flag)}"\)\s*$\n)'
        r'(^\s*ARGS\+=\(")([^"]+)("\)\s*$)'
    )
    repaired, count = re.subn(
        pattern,
        lambda match: match.group(1) + match.group(2) + value + match.group(4),
        script,
        flags=re.MULTILINE,
    )
    if count != 1:
        raise RuntimeError(
            f"queue startup script has {count} value entries for {flag}"
        )
    return repaired


def _kube_ssl_context() -> ssl.SSLContext:
    return ssl.create_default_context(cafile=str(SERVICE_ACCOUNT / "ca.crt"))


async def _kube_request(
    method: str, path: str, *, payload: dict[str, Any] | None = None
) -> dict[str, Any]:
    token = (SERVICE_ACCOUNT / "token").read_text(encoding="utf-8").strip()
    headers = {"Authorization": f"Bearer {token}"}
    if method == "PATCH":
        headers["Content-Type"] = "application/merge-patch+json"
    async with ClientSession() as client:
        async with client.request(
            method,
            f"{KUBE_API}{path}",
            json=payload,
            headers=headers,
            ssl=_kube_ssl_context(),
            timeout=ClientTimeout(total=10.0),
        ) as response:
            body = await response.text()
            if response.status >= 300:
                raise RuntimeError(
                    f"Kubernetes {method} {path} returned {response.status}: {body[:500]}"
                )
            if not body:
                return {}
            parsed = json.loads(body)
            if not isinstance(parsed, dict):
                raise RuntimeError("Kubernetes API returned a non-object response")
            return parsed


def _resource_path(resource: str, name: str) -> str:
    if not name or not re.fullmatch(r"[a-z0-9]([-a-z0-9.]*[a-z0-9])?", name):
        raise RuntimeError(f"invalid configured Kubernetes resource name {name!r}")
    return f"/api/v1/namespaces/{KUBE_NAMESPACE}/{resource}/{name}"


async def _restart_redis_queue() -> dict[str, str]:
    pod_path = _resource_path("pods", REDIS_QUEUE_POD)
    before = await _kube_request("GET", pod_path)
    old_uid = str(before.get("metadata", {}).get("uid", ""))
    if not old_uid:
        raise RuntimeError("queue pod has no Kubernetes UID")
    paused = await _redis_command(
        "CLIENT", "PAUSE", str(REDIS_RESTART_WRITE_PAUSE_MS), "WRITE"
    )
    if paused != "OK":
        raise RuntimeError(f"Redis CLIENT PAUSE WRITE returned {paused!r}")
    saved = await _redis_command("SAVE")
    if saved != "OK":
        await _redis_command("CLIENT", "UNPAUSE")
        raise RuntimeError(f"Redis SAVE returned {saved!r}")
    try:
        await _kube_request(
            "DELETE",
            pod_path,
            payload={"kind": "DeleteOptions", "apiVersion": "v1"},
        )
    except Exception:
        await _redis_command("CLIENT", "UNPAUSE")
        raise
    deadline = asyncio.get_running_loop().time() + 120.0
    while asyncio.get_running_loop().time() < deadline:
        await asyncio.sleep(1.0)
        try:
            current = await _kube_request("GET", pod_path)
        except RuntimeError as exc:
            if "returned 404" in str(exc):
                continue
            raise
        metadata = current.get("metadata", {})
        statuses = current.get("status", {}).get("containerStatuses", [])
        redis_ready = any(
            item.get("name") == "redis" and item.get("ready") is True
            for item in statuses
            if isinstance(item, dict)
        )
        new_uid = str(metadata.get("uid", ""))
        if new_uid and new_uid != old_uid and redis_ready:
            last_probe_error: Exception | None = None
            for probe_attempt in range(1, 6):
                try:
                    await _redis_command("PING")
                    return {"old_uid": old_uid, "new_uid": new_uid}
                except (ConnectionError, OSError, asyncio.TimeoutError) as exc:
                    last_probe_error = exc
                    if probe_attempt < 5:
                        await asyncio.sleep(0.25 * (2 ** (probe_attempt - 1)))
            raise RuntimeError(
                "redis-queue pod became Ready but its trusted PING probe failed "
                f"after 5 attempts: {last_probe_error!r}"
            ) from last_probe_error
    raise TimeoutError("redis-queue did not become Ready with a new pod identity")


async def _persist_queue_repair(operation: str, value: str | None) -> None:
    configmap_path = _resource_path(
        "configmaps", REDIS_QUEUE_SCRIPTS_CONFIGMAP
    )
    current = await _kube_request("GET", configmap_path)
    data = current.get("data")
    if not isinstance(data, dict) or not isinstance(data.get("start-master.sh"), str):
        raise RuntimeError("queue scripts ConfigMap has no start-master.sh")
    repaired = _rewrite_redis_start_script(
        data["start-master.sh"], operation=operation, value=value
    )
    await _kube_request(
        "PATCH",
        configmap_path,
        payload={"data": {"start-master.sh": repaired}},
    )


async def _wait_for_queue_aof_ready() -> None:
    """Do not report persistence repaired until Redis has written its AOF base."""
    deadline = asyncio.get_running_loop().time() + 90.0
    while True:
        raw = await _redis_command("INFO", "persistence")
        if not isinstance(raw, str):
            raise RuntimeError("Redis INFO persistence returned malformed data")
        fields = dict(
            line.split(":", 1)
            for line in raw.splitlines()
            if ":" in line and not line.startswith("#")
        )
        if (
            fields.get("aof_enabled") == "1"
            and fields.get("aof_rewrite_in_progress") == "0"
            and fields.get("aof_rewrite_scheduled") == "0"
            and fields.get("aof_last_bgrewrite_status") == "ok"
        ):
            return
        if asyncio.get_running_loop().time() >= deadline:
            raise TimeoutError("Redis did not finish its initial AOF rewrite")
        await asyncio.sleep(1.0)


async def _apply_queue_repair(operation: str, value: str | None) -> None:
    if operation == "maxmemory":
        if value is None:
            raise ValueError("maxmemory repair requires a value")
        size = _memory_bytes(value)
        if not MIN_QUEUE_MEMORY <= size <= MAX_QUEUE_MEMORY:
            raise ValueError(
                "maxmemory repair must stay between 16mb and 128mb"
            )
        await _redis_command("CONFIG", "SET", "maxmemory", value)
    elif operation == "min-replicas-to-write":
        if value != "0":
            raise ValueError("min-replicas-to-write repair must restore standalone value 0")
        await _redis_command("CONFIG", "SET", operation, value)
    elif operation == "appendonly":
        if value != "yes":
            raise ValueError("appendonly repair must enable persistence")
        await _redis_command("CONFIG", "SET", "appendonly", "yes")
        await _wait_for_queue_aof_ready()
    elif operation == "appendfsync":
        if value not in {"everysec", "always"}:
            raise ValueError("appendfsync repair must use everysec or always")
        await _redis_command("CONFIG", "SET", "appendfsync", value)
    elif operation == "dequeue-acl":
        if value is not None:
            raise ValueError("dequeue-acl repair does not accept a value")
        await _redis_command(
            "ACL", "SETUSER", "default", "+blpop", "+blmove", "+brpop"
        )
    else:
        raise ValueError(f"unsupported queue repair operation {operation!r}")
    await _persist_queue_repair(operation, value)


# --------------------------------------------------------------------------- #
# Route handlers
# --------------------------------------------------------------------------- #
async def _healthz(_request: web.Request) -> web.Response:
    return web.json_response({"ok": True})


async def _get_config(_request: web.Request) -> web.Response:
    try:
        return web.json_response(_read_config_unlocked())
    except Exception as e:
        log.exception("GET /admin/config: read failed")
        return web.json_response({"ok": False, "error": f"{type(e).__name__}: {e}"}, status=500)


async def _get_redis_config(_request: web.Request) -> web.Response:
    try:
        settings = await _redis_queue_config()
        cache_settings = await _redis_config(REDIS_CACHE_HOST, REDIS_CACHE_PORT)
        acl_state = await _redis_queue_acl_state()
        safety_counters = await _redis_queue_safety_counters()
        cache_safety_counters = await _redis_safety_counters(
            REDIS_CACHE_HOST, REDIS_CACHE_PORT
        )
        cache_run_id = await _redis_run_id(REDIS_CACHE_HOST, REDIS_CACHE_PORT)
        return web.json_response(
            {
                "schema_version": 1,
                "engine": "redis",
                "service": "redis-queue",
                "settings": settings,
                "cache_settings": cache_settings,
                "acl_state": acl_state,
                "safety_counters": safety_counters,
                "cache_safety_counters": cache_safety_counters,
                "cache_run_id": cache_run_id,
            }
        )
    except Exception as e:
        log.exception("GET /admin/redis-config: capture failed")
        return web.json_response(
            {"ok": False, "error": f"{type(e).__name__}: {e}"}, status=500
        )


async def _post_redis_queue_repair(request: web.Request) -> web.Response:
    try:
        body = await request.json()
        if not isinstance(body, dict) or set(body) - {"operation", "value"}:
            raise ValueError("body must contain only operation and optional value")
        operation = body.get("operation")
        value = body.get("value")
        if not isinstance(operation, str) or (
            value is not None and not isinstance(value, str)
        ):
            raise ValueError("operation and value must be strings")
        await _apply_queue_repair(operation, value)
        return web.json_response(
            {
                "ok": True,
                "operation": operation,
                "value": value,
                "persisted": True,
            }
        )
    except ValueError as exc:
        return web.json_response({"ok": False, "error": str(exc)}, status=400)
    except Exception as exc:
        log.exception("POST /admin/redis-queue-repair failed")
        return web.json_response(
            {"ok": False, "error": f"{type(exc).__name__}: {exc}"}, status=500
        )


async def _post_redis_queue_restart(_request: web.Request) -> web.Response:
    try:
        restart = await _restart_redis_queue()
        return web.json_response({"ok": True, "restart": restart})
    except Exception as exc:
        log.exception("POST /admin/redis-queue-restart failed")
        return web.json_response(
            {"ok": False, "error": f"{type(exc).__name__}: {exc}"}, status=500
        )


async def _put_config(request: web.Request) -> web.Response:
    try:
        body = await request.json()
    except Exception as e:
        return web.json_response(
            {"ok": False, "error": f"invalid JSON body: {e}"}, status=400
        )
    if not isinstance(body, dict):
        return web.json_response(
            {"ok": False, "error": "body must be a JSON object of {key: value}"}, status=400
        )
    try:
        merged = _merge_config_locked(body)
        log.info("PUT /admin/config: merged %d key(s)", len(body))
        return web.json_response({"ok": True, "config": merged})
    except Exception as e:
        log.exception("PUT /admin/config: write failed")
        return web.json_response({"ok": False, "error": f"{type(e).__name__}: {e}"}, status=500)


async def _post_reload(_request: web.Request) -> web.Response:
    try:
        _kill_pid1()
    except Exception as e:
        return web.json_response({"ok": False, "error": f"{type(e).__name__}: {e}"}, status=500)
    # If we're still alive after SIGKILL, we shared PID ns and PID 1 was reaped;
    # kubelet will restart the pod shortly (and this sidecar with it).
    return web.json_response({"ok": True, "reloaded": True})


async def _metrics(_request: web.Request) -> web.Response:
    lines = [
        "# HELP frappe_admin_sidecar_up Sidecar liveness gauge.",
        "# TYPE frappe_admin_sidecar_up gauge",
        f'frappe_admin_sidecar_up{{host="{socket.gethostname()}"}} 1',
        "# HELP rq_queue_depth Number of pending jobs per qualified Frappe RQ queue.",
        "# TYPE rq_queue_depth gauge",
    ]
    depths = await asyncio.gather(*[_rq_queue_depth(q) for q in RQ_QUEUES])
    for name, depth in zip(RQ_QUEUES, depths):
        if depth is not None:
            lines.append(f'rq_queue_depth{{queue="{name}"}} {depth}')
    return web.Response(text="\n".join(lines) + "\n", content_type="text/plain")


def build_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/healthz",       _healthz)
    app.router.add_get("/admin/config",  _get_config)
    app.router.add_get("/admin/redis-config", _get_redis_config)
    app.router.add_post("/admin/redis-queue-repair", _post_redis_queue_repair)
    app.router.add_post("/admin/redis-queue-restart", _post_redis_queue_restart)
    app.router.add_put("/admin/config",  _put_config)
    app.router.add_post("/admin/reload", _post_reload)
    app.router.add_get("/metrics",       _metrics)
    return app


def main() -> None:
    log.info(
        "frappe-admin sidecar starting: bind=%s:%d rq_queues=%s config=%s",
        ADMIN_HOST, ADMIN_PORT, RQ_QUEUES, COMMON_SITE_CONFIG,
    )
    web.run_app(build_app(), host=ADMIN_HOST, port=ADMIN_PORT, print=None)


if __name__ == "__main__":
    main()
