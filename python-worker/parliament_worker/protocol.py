"""Wire protocol for the Python worker.

Newline-delimited JSON, defined in ../PROTOCOL.md. The encoding lives in one module and
every message is a dataclass, so a field that only one side knows about is a type error
here rather than a `KeyError` on a socket at three in the morning.

Decoding is strict: an unknown ``t``, a missing field, or a field of the wrong type raises
``ProtocolError``. The alternative — tolerating it and filling in a default — turns a
version mismatch between the two implementations into a workflow that runs with arguments
nobody sent.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, replace
from typing import Any, ClassVar, Final, Mapping, Sequence

__all__ = [
    "MAX_LINE_BYTES",
    "ProtocolError",
    "StepRef",
    "StepError",
    "LeaseRequest",
    "LeaseGranted",
    "LeaseEmpty",
    "Heartbeat",
    "LeaseRenewed",
    "StepCompleted",
    "StepFailed",
    "LeaseRelease",
    "Ack",
    "ErrorFrame",
    "Message",
    "encode",
    "decode",
    "with_id",
]

#: A line longer than this closes the connection; see PROTOCOL.md on framing.
MAX_LINE_BYTES: Final[int] = 1024 * 1024


class ProtocolError(Exception):
    """A message could not be understood. Never a step failure, always a wiring fault."""


@dataclass(frozen=True)
class StepRef:
    """The (workflow, step, attempt) triple idempotency keys are derived from."""

    workflow_id: str
    step: str
    attempt: int

    def to_payload(self) -> dict[str, Any]:
        return {"workflow_id": self.workflow_id, "step": self.step, "attempt": self.attempt}

    @staticmethod
    def from_payload(payload: Mapping[str, Any]) -> "StepRef":
        return StepRef(
            workflow_id=_require_str(payload, "workflow_id"),
            step=_require_str(payload, "step"),
            attempt=_require_int(payload, "attempt"),
        )

    def key(self) -> str:
        return f"{self.workflow_id}/{self.step}/{self.attempt}"


@dataclass(frozen=True)
class StepError:
    """A failure as reported to the control plane.

    ``error_class`` is drawn from the same closed set the retry policy reads, so a Python
    failure is retried on the same rules as a TypeScript one. ``traceback`` is optional
    because a failure the worker synthesised — an unknown callable name — has no stack
    worth sending.
    """

    error_class: str
    message: str
    traceback: str | None = None

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"class": self.error_class, "message": self.message}
        if self.traceback is not None:
            payload["traceback"] = self.traceback
        return payload

    @staticmethod
    def from_payload(payload: Mapping[str, Any]) -> "StepError":
        tb = payload.get("traceback")
        if tb is not None and not isinstance(tb, str):
            raise ProtocolError("error.traceback must be a string")
        return StepError(
            error_class=_require_str(payload, "class"),
            message=_require_str(payload, "message"),
            traceback=tb,
        )


@dataclass(frozen=True)
class LeaseRequest:
    worker: str
    kinds: tuple[str, ...] = ("python",)
    id: int = 0

    T: ClassVar[str] = "lease.request"

    def to_payload(self) -> dict[str, Any]:
        return {"worker": self.worker, "kinds": list(self.kinds)}

    @staticmethod
    def from_payload(id: int, payload: Mapping[str, Any]) -> "LeaseRequest":
        return LeaseRequest(
            worker=_require_str(payload, "worker"),
            kinds=tuple(_require_str_list(payload, "kinds")),
            id=id,
        )


@dataclass(frozen=True)
class LeaseGranted:
    ref: StepRef
    lease: str
    callable_name: str
    input: Any
    timeout_ms: int
    lease_duration_ms: int
    id: int = 0

    T: ClassVar[str] = "lease.granted"

    def to_payload(self) -> dict[str, Any]:
        return {
            "ref": self.ref.to_payload(),
            "lease": self.lease,
            "callable": self.callable_name,
            "input": self.input,
            "timeout_ms": self.timeout_ms,
            "lease_duration_ms": self.lease_duration_ms,
        }

    @staticmethod
    def from_payload(id: int, payload: Mapping[str, Any]) -> "LeaseGranted":
        return LeaseGranted(
            ref=StepRef.from_payload(_require_mapping(payload, "ref")),
            lease=_require_str(payload, "lease"),
            callable_name=_require_str(payload, "callable"),
            input=payload.get("input"),
            timeout_ms=_require_int(payload, "timeout_ms"),
            lease_duration_ms=_require_int(payload, "lease_duration_ms"),
            id=id,
        )


@dataclass(frozen=True)
class LeaseEmpty:
    id: int = 0

    T: ClassVar[str] = "lease.empty"

    def to_payload(self) -> dict[str, Any]:
        return {}

    @staticmethod
    def from_payload(id: int, payload: Mapping[str, Any]) -> "LeaseEmpty":
        del payload
        return LeaseEmpty(id=id)


@dataclass(frozen=True)
class Heartbeat:
    worker: str
    lease: str
    ref: StepRef
    id: int = 0

    T: ClassVar[str] = "lease.heartbeat"

    def to_payload(self) -> dict[str, Any]:
        return {"worker": self.worker, "lease": self.lease, "ref": self.ref.to_payload()}

    @staticmethod
    def from_payload(id: int, payload: Mapping[str, Any]) -> "Heartbeat":
        return Heartbeat(
            worker=_require_str(payload, "worker"),
            lease=_require_str(payload, "lease"),
            ref=StepRef.from_payload(_require_mapping(payload, "ref")),
            id=id,
        )


@dataclass(frozen=True)
class LeaseRenewed:
    ok: bool
    expires_at: int
    id: int = 0

    T: ClassVar[str] = "lease.renewed"

    def to_payload(self) -> dict[str, Any]:
        return {"ok": self.ok, "expires_at": self.expires_at}

    @staticmethod
    def from_payload(id: int, payload: Mapping[str, Any]) -> "LeaseRenewed":
        ok = payload.get("ok")
        if not isinstance(ok, bool):
            raise ProtocolError("lease.renewed requires a boolean 'ok'")
        return LeaseRenewed(ok=ok, expires_at=_require_int(payload, "expires_at"), id=id)


@dataclass(frozen=True)
class StepCompleted:
    worker: str
    lease: str
    ref: StepRef
    output: Any
    id: int = 0

    T: ClassVar[str] = "step.completed"

    def to_payload(self) -> dict[str, Any]:
        return {
            "worker": self.worker,
            "lease": self.lease,
            "ref": self.ref.to_payload(),
            "output": self.output,
        }

    @staticmethod
    def from_payload(id: int, payload: Mapping[str, Any]) -> "StepCompleted":
        return StepCompleted(
            worker=_require_str(payload, "worker"),
            lease=_require_str(payload, "lease"),
            ref=StepRef.from_payload(_require_mapping(payload, "ref")),
            output=payload.get("output"),
            id=id,
        )


@dataclass(frozen=True)
class StepFailed:
    worker: str
    lease: str
    ref: StepRef
    error: StepError
    id: int = 0

    T: ClassVar[str] = "step.failed"

    def to_payload(self) -> dict[str, Any]:
        return {
            "worker": self.worker,
            "lease": self.lease,
            "ref": self.ref.to_payload(),
            "error": self.error.to_payload(),
        }

    @staticmethod
    def from_payload(id: int, payload: Mapping[str, Any]) -> "StepFailed":
        return StepFailed(
            worker=_require_str(payload, "worker"),
            lease=_require_str(payload, "lease"),
            ref=StepRef.from_payload(_require_mapping(payload, "ref")),
            error=StepError.from_payload(_require_mapping(payload, "error")),
            id=id,
        )


@dataclass(frozen=True)
class LeaseRelease:
    worker: str
    lease: str
    ref: StepRef
    id: int = 0

    T: ClassVar[str] = "lease.release"

    def to_payload(self) -> dict[str, Any]:
        return {"worker": self.worker, "lease": self.lease, "ref": self.ref.to_payload()}

    @staticmethod
    def from_payload(id: int, payload: Mapping[str, Any]) -> "LeaseRelease":
        return LeaseRelease(
            worker=_require_str(payload, "worker"),
            lease=_require_str(payload, "lease"),
            ref=StepRef.from_payload(_require_mapping(payload, "ref")),
            id=id,
        )


@dataclass(frozen=True)
class Ack:
    id: int = 0

    T: ClassVar[str] = "ack"

    def to_payload(self) -> dict[str, Any]:
        return {}

    @staticmethod
    def from_payload(id: int, payload: Mapping[str, Any]) -> "Ack":
        del payload
        return Ack(id=id)


@dataclass(frozen=True)
class ErrorFrame:
    message: str
    id: int = 0

    T: ClassVar[str] = "error"

    def to_payload(self) -> dict[str, Any]:
        return {"message": self.message}

    @staticmethod
    def from_payload(id: int, payload: Mapping[str, Any]) -> "ErrorFrame":
        return ErrorFrame(message=_require_str(payload, "message"), id=id)


Message = (
    LeaseRequest
    | LeaseGranted
    | LeaseEmpty
    | Heartbeat
    | LeaseRenewed
    | StepCompleted
    | StepFailed
    | LeaseRelease
    | Ack
    | ErrorFrame
)

_DECODERS: Final[Mapping[str, Any]] = {
    LeaseRequest.T: LeaseRequest.from_payload,
    LeaseGranted.T: LeaseGranted.from_payload,
    LeaseEmpty.T: LeaseEmpty.from_payload,
    Heartbeat.T: Heartbeat.from_payload,
    LeaseRenewed.T: LeaseRenewed.from_payload,
    StepCompleted.T: StepCompleted.from_payload,
    StepFailed.T: StepFailed.from_payload,
    LeaseRelease.T: LeaseRelease.from_payload,
    Ack.T: Ack.from_payload,
    ErrorFrame.T: ErrorFrame.from_payload,
}


def encode(message: Message) -> str:
    """Renders one message as a single line, newline included.

    ``separators`` is pinned and keys are left in declaration order: the wire form of a
    given message is then byte-identical across runs, which is what makes a captured line
    usable as a test fixture.
    """
    payload = {"t": message.T, "id": message.id, **message.to_payload()}
    line = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
    if len(line.encode("utf-8")) > MAX_LINE_BYTES:
        raise ProtocolError(f"{message.T} exceeds {MAX_LINE_BYTES} bytes")
    return line + "\n"


def decode(line: str) -> Message:
    """Parses one line into a message, or raises ``ProtocolError``."""
    stripped = line.strip()
    if not stripped:
        raise ProtocolError("empty line")
    try:
        payload = json.loads(stripped)
    except json.JSONDecodeError as exc:
        raise ProtocolError(f"not JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise ProtocolError(f"expected an object, got {type(payload).__name__}")

    kind = payload.get("t")
    if not isinstance(kind, str):
        raise ProtocolError("message has no 't'")
    decoder = _DECODERS.get(kind)
    if decoder is None:
        raise ProtocolError(f"unknown message type {kind!r}")

    message_id = payload.get("id")
    if not isinstance(message_id, int) or isinstance(message_id, bool):
        raise ProtocolError(f"{kind} has no integer 'id'")

    return decoder(message_id, payload)


def with_id(message: Message, message_id: int) -> Message:
    """Returns the message with its correlation id set.

    Messages are frozen, so the connection stamps ids by copying rather than by mutating
    a message a caller may still hold a reference to.
    """
    return replace(message, id=message_id)


def _require_str(payload: Mapping[str, Any], field: str) -> str:
    value = payload.get(field)
    if not isinstance(value, str):
        raise ProtocolError(f"field {field!r} must be a string, got {type(value).__name__}")
    return value


def _require_int(payload: Mapping[str, Any], field: str) -> int:
    value = payload.get(field)
    # bool is an int in Python and would silently pass; an attempt number of True is a bug.
    if not isinstance(value, int) or isinstance(value, bool):
        raise ProtocolError(f"field {field!r} must be an integer, got {type(value).__name__}")
    return value


def _require_mapping(payload: Mapping[str, Any], field: str) -> Mapping[str, Any]:
    value = payload.get(field)
    if not isinstance(value, dict):
        raise ProtocolError(f"field {field!r} must be an object, got {type(value).__name__}")
    return value


def _require_str_list(payload: Mapping[str, Any], field: str) -> Sequence[str]:
    value = payload.get(field)
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise ProtocolError(f"field {field!r} must be an array of strings")
    return value
