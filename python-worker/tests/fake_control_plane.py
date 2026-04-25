"""A control plane that is real enough to test against.

It speaks the protocol over an actual loopback socket rather than over an in-memory stub.
The framing, the id correlation, and the reader thread are the parts most likely to be
wrong, and none of them are exercised by a fake that hands message objects straight to the
worker.
"""

from __future__ import annotations

import pathlib
import sys

# Discovery runs with this directory as the top level, so the package under test is not
# importable by default. Added here rather than requiring an install step, because a test
# suite that needs `pip install -e .` first is a test suite that gets skipped.
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

import socket
import threading
from typing import Any

from parliament_worker.protocol import (
    Ack,
    Heartbeat,
    LeaseEmpty,
    LeaseGranted,
    LeaseRelease,
    LeaseRenewed,
    LeaseRequest,
    Message,
    StepCompleted,
    StepFailed,
    decode,
    encode,
    with_id,
)


class FakeControlPlane:
    """Serves one worker connection from a scripted queue of grants."""

    def __init__(
        self,
        grants: list[LeaseGranted] | None = None,
        renew_ok: bool = True,
        renew_ok_after: int | None = None,
    ) -> None:
        self._grants = list(grants or [])
        self._renew_ok = renew_ok
        # Lets a test revoke a lease partway through a step, which is the only way to
        # exercise the "stop and report nothing" path.
        self._renew_ok_after = renew_ok_after
        self._heartbeats = 0
        self._lock = threading.Lock()

        self.received: list[Message] = []
        self.reports: list[Message] = []
        self.releases: list[LeaseRelease] = []

        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind(("127.0.0.1", 0))
        self._sock.listen(1)
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._serve, name="fake-control-plane", daemon=True)

    @property
    def address(self) -> tuple[str, int]:
        host, port = self._sock.getsockname()[:2]
        return host, port

    @property
    def heartbeats(self) -> int:
        with self._lock:
            return self._heartbeats

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        try:
            self._sock.close()
        except OSError:
            pass
        self._thread.join(timeout=2.0)

    def connect(self) -> socket.socket:
        client = socket.create_connection(self.address, timeout=5.0)
        return client

    def _serve(self) -> None:
        try:
            conn, _ = self._sock.accept()
        except OSError:
            return
        with conn, conn.makefile("r", encoding="utf-8", newline="\n") as reader:
            for line in reader:
                if self._stop.is_set():
                    return
                if not line.strip():
                    continue
                message = decode(line)
                with self._lock:
                    self.received.append(message)
                reply = self._reply_to(message)
                if reply is not None:
                    conn.sendall(encode(reply).encode("utf-8"))

    def _reply_to(self, message: Message) -> Message | None:
        if isinstance(message, LeaseRequest):
            with self._lock:
                grant = self._grants.pop(0) if self._grants else None
            return with_id(grant, message.id) if grant else with_id(LeaseEmpty(), message.id)

        if isinstance(message, Heartbeat):
            with self._lock:
                self._heartbeats += 1
                count = self._heartbeats
            ok = self._renew_ok
            if self._renew_ok_after is not None and count > self._renew_ok_after:
                ok = False
            return with_id(LeaseRenewed(ok=ok, expires_at=0), message.id)

        if isinstance(message, (StepCompleted, StepFailed)):
            with self._lock:
                self.reports.append(message)
            return with_id(Ack(), message.id)

        if isinstance(message, LeaseRelease):
            with self._lock:
                self.releases.append(message)
            return with_id(Ack(), message.id)

        return None


def grant(
    *,
    callable_name: str,
    payload: Any = None,
    step: str = "step-1",
    attempt: int = 1,
    workflow_id: str = "wf-1",
    timeout_ms: int = 2000,
    lease_duration_ms: int = 120,
    lease: str = "lease-token",
) -> LeaseGranted:
    from parliament_worker.protocol import StepRef

    return LeaseGranted(
        ref=StepRef(workflow_id=workflow_id, step=step, attempt=attempt),
        lease=lease,
        callable_name=callable_name,
        input=payload,
        timeout_ms=timeout_ms,
        lease_duration_ms=lease_duration_ms,
    )
