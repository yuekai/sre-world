"""Task-local, accepted Frappe mail backed by an in-cluster SMTP sink.

Only 07-writes-and-queue-oom selects this seed. The messages are real Email
Queue documents and RQ jobs; no message leaves the ephemeral cluster.
"""

from __future__ import annotations

import asyncio
import copy
import io
import json
import pickle
import socket
import uuid
import zlib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.parser import BytesParser
from email.policy import SMTP
from typing import Any, Awaitable, Callable

import aiohttp

from loadgen_frappe.drivers import _TARGET_BASE, _do_request

EMAIL_COUNT = 4
SMTP_PORT = 2525
QUEUE = "rq:queue:home-frappe-frappe-bench:long"
EMAIL_METHOD = "frappe.deprecation_dumpster.send_mail"


@dataclass
class MailSink:
    server: asyncio.AbstractServer
    deliveries: list[tuple[str, str]] = field(default_factory=list)


async def start_mail_sink() -> MailSink:
    deliveries: list[tuple[str, str]] = []

    async def connection(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        async def reply(line: bytes) -> None:
            writer.write(line + b"\r\n")
            await writer.drain()

        recipients: list[str] = []
        try:
            await reply(b"220 loadgen.local ESMTP")
            while line := await asyncio.wait_for(reader.readline(), timeout=120):
                command = line.strip().upper()
                if command.startswith((b"EHLO ", b"HELO ")):
                    await reply(b"250-loadgen.local")
                    await reply(b"250 SIZE 1048576")
                elif command.startswith(b"MAIL FROM:"):
                    recipients = []
                    await reply(b"250 OK")
                elif command.startswith(b"RCPT TO:"):
                    address = line.split(b":", 1)[1].strip().strip(b"<>").decode()
                    recipients.append(address.lower())
                    await reply(b"250 OK")
                elif command == b"DATA":
                    await reply(b"354 End data with <CR><LF>.<CR><LF>")
                    lines: list[bytes] = []
                    size = 0
                    while True:
                        part = await asyncio.wait_for(reader.readline(), timeout=120)
                        if part in (b".\r\n", b".\n"):
                            break
                        if not part:
                            raise ConnectionError("SMTP client closed during DATA")
                        if part.startswith(b".."):
                            part = part[1:]
                        size += len(part)
                        if size > 1_048_576:
                            raise ValueError("SMTP seed message exceeded 1 MiB")
                        lines.append(part)
                    message = BytesParser(policy=SMTP).parsebytes(b"".join(lines))
                    marker = str(message.get("X-Seed-Email", ""))
                    deliveries.extend((recipient, marker) for recipient in recipients)
                    await reply(b"250 Accepted")
                elif command == b"NOOP":
                    await reply(b"250 OK")
                elif command == b"RSET":
                    recipients = []
                    await reply(b"250 OK")
                elif command == b"QUIT":
                    await reply(b"221 Bye")
                    break
                else:
                    await reply(b"502 Unsupported command")
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(connection, host="0.0.0.0", port=SMTP_PORT)
    return MailSink(server=server, deliveries=deliveries)


class _ReportJobUnpickler(pickle.Unpickler):
    """Decode the one Frappe function reference in an accepted report job."""

    def find_class(self, module: str, name: str) -> Any:
        if (module, name) == (
            "frappe.core.doctype.prepared_report.prepared_report",
            "generate_report",
        ):
            return module + "." + name
        raise ValueError(f"unexpected object in RQ report job: {module}.{name}")


def email_job_data(report_job_data: bytes, email_name: str) -> bytes:
    source = _ReportJobUnpickler(io.BytesIO(zlib.decompress(report_job_data))).load()
    if not isinstance(source, tuple) or len(source) != 4:
        raise ValueError("accepted report job has unexpected RQ payload shape")
    function, instance, args, options = source
    if function != "frappe.utils.background_jobs.execute_job" or not isinstance(options, dict):
        raise ValueError("accepted report job is not a Frappe background job")
    if options.get("method") != (
        "frappe.core.doctype.prepared_report.prepared_report.generate_report"
    ) or not isinstance(options.get("kwargs"), dict):
        raise ValueError("accepted report job method is not generate_report")
    payload = copy.deepcopy(options)
    payload["method"] = EMAIL_METHOD
    payload["job_name"] = EMAIL_METHOD
    payload["kwargs"] = {"email_queue_name": email_name}
    return zlib.compress(pickle.dumps((function, instance, args, payload), protocol=5))


async def seed_email_queue(
    pool: Any,
    ledger: Any,
    redis_command: Callable[..., Awaitable[Any]],
    report_template: dict[bytes, bytes],
) -> tuple[MailSink, dict[str, str]]:
    """Accept four emails and enqueue their distinct executable RQ jobs."""
    sink = await start_mail_sink()
    account = {
        "doctype": "Email Account",
        "email_account_name": "Benchmark Outbox",
        "email_id": "benchmark@example.invalid",
        "enable_outgoing": 1,
        "default_outgoing": 1,
        "no_smtp_authentication": 1,
        "smtp_server": socket.gethostbyname(socket.gethostname()),
        "smtp_port": str(SMTP_PORT),
        "use_tls": 0,
        "use_ssl_for_outgoing": 0,
    }

    async def insert(doc: dict[str, Any]) -> str:
        async with aiohttp.ClientSession() as session:
            _slot, sid = pool.sid_for(0)
            status, body = await _do_request(
                "POST",
                f"{_TARGET_BASE}/api/method/frappe.client.insert",
                session,
                sid,
                data={"doc": json.dumps(doc)},
            )
        parsed = json.loads(body) if body else {}
        name = parsed.get("message", {}).get("name") if isinstance(parsed, dict) else None
        if status != 200 or not isinstance(name, str) or not name:
            raise RuntimeError(f"Frappe rejected accepted email seed: HTTP {status} {body[:300]}")
        return name

    account_name = await insert(account)
    expected: dict[str, str] = {}
    for index in range(EMAIL_COUNT):
        recipient = f"accepted-{index:02d}@example.invalid"
        marker = f"005-accepted-{index:02d}"
        message = (
            f"From: Benchmark Outbox <benchmark@example.invalid>\r\n"
            f"To: {recipient}\r\n"
            f"Subject: Accepted background notice {index:02d}\r\n"
            f"X-Seed-Email: {marker}\r\n"
            "MIME-Version: 1.0\r\nContent-Type: text/plain; charset=utf-8\r\n"
            f"\r\nThe queued notice {marker} was accepted before the interruption.\r\n"
        )
        name = await insert(
            {
                "doctype": "Email Queue",
                "email_account": account_name,
                "sender": "benchmark@example.invalid",
                "status": "Not Sent",
                "message": message,
                "recipients": [
                    {"doctype": "Email Queue Recipient", "recipient": recipient, "status": "Not Sent"}
                ],
            }
        )
        job_id = "svc-frappe-web||" + str(uuid.uuid4())
        job = dict(report_template)
        job[b"data"] = email_job_data(report_template[b"data"], name)
        job[b"description"] = f"send accepted email {name}".encode()
        job[b"status"] = b"queued"
        now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z").encode()
        job[b"created_at"] = now
        job[b"enqueued_at"] = now
        for field in (b"started_at", b"ended_at", b"worker_name", b"last_heartbeat", b"exc_info"):
            job.pop(field, None)
        fields = [item for pair in job.items() for item in pair]
        if await redis_command("HSET", "rq:job:" + job_id, *fields) < 1:
            raise RuntimeError("could not accept seeded email RQ job")
        if await redis_command("LPUSH", QUEUE, job_id) < 1:
            raise RuntimeError("could not enqueue seeded email RQ job")
        ledger.append("email:" + name, asyncio.get_running_loop().time())
        expected[recipient] = marker
    return sink, expected
