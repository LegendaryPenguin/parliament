"""The Python step worker.

Same shape as the TypeScript worker — lease, heartbeat, execute, report, release — and the
same non-negotiable property: the heartbeat is independent of the work. Here that means a
dedicated thread, because a callable is synchronous and arbitrary, so there is no point in
the execution path where a cooperative worker could be relied on to yield.

Three things are honest limitations rather than oversights, and they are documented where
they bite:

* A timed-out callable's thread cannot be killed. Python has no safe thread cancellation,
  so the worker stops *waiting* and reports a timeout while the thread may continue.
* Therefore a timeout is reported as retryable only in the sense the retry policy decides;
  whether retrying is safe depends on whether the callable is idempotent.
* Only one step runs per worker at a time. Concurrency is a matter of running more
  workers, which keeps the leasing arithmetic per-process and simple.
"""

from __future__ import annotations

import socket
import threading
import traceback
from dataclasses import dataclass
from typing import Any, Callable, Final, Literal

from .protocol import (
    Ack,
    ErrorFrame,
    Heartbeat,
    LeaseEmpty,
    LeaseGranted,
    LeaseRelease,
    LeaseRenewed,
    LeaseRequest,
    MAX_LINE_BYTES,
    Message,
    ProtocolError,
    StepCompleted,
    StepError,
    StepFailed,
    StepRef,
    decode,
    encode,
    with_id,
)
from .registry import CallableRegistry, UnknownCallableError

__all__ = ["Connection", "Outcome", "PythonWorker", "TimeoutExpired"]

#: How long to wait for a reply before treating the control plane as unreachable.
DEFAULT_REPLY_TIMEOUT_S: Final[float] = 10.0

LogFn = Callable[[str], None]


class TimeoutExpired(Exception):
    """A callable outran its budget."""


class ConnectionClosed(Exception):
    """The control plane went away mid-request."""


@dataclass
class _Waiter:
    event: threading.Event
    message: Message | None = None


class Connection:
    """One socket, framed as newline-delimited JSON, with replies matched by id.

    A reader thread demultiplexes. Without one, the heartbeat thread and the main thread
    would both read from the socket and each could consume the other's reply — the classic
    way a shared connection appears to work in testing and deadlocks under load.
    """

    def __init__(self, sock: socket.socket, log: LogFn | None = None) -> None:
        self._sock = sock
        self._reader = sock.makefile("r", encoding="utf-8", newline="\n")
        self._send_lock = threading.Lock()
        self._state_lock = threading.Lock()
        self._waiters: dict[int, _Waiter] = {}
        self._next_id = 1
        self._closed = threading.Event()
        self._log = log or (lambda _message: None)
        self._thread = threading.Thread(target=self._read_loop, name="parliament-reader", daemon=True)

    def start(self) -> None:
        self._thread.start()

    @property
    def closed(self) -> bool:
        return self._closed.is_set()

    def send(self, message: Message) -> int:
        """Sends a message, stamping it with a fresh id. Returns the id."""
        with self._state_lock:
            message_id = self._next_id
            self._next_id += 1
        stamped = with_id(message, message_id)
        line = encode(stamped)
        with self._send_lock:
            if self._closed.is_set():
                raise ConnectionClosed("connection is closed")
            self._sock.sendall(line.encode("utf-8"))
        return message_id

    def request(self, message: Message, timeout_s: float = DEFAULT_REPLY_TIMEOUT_S) -> Message:
        """Sends a message and waits for the reply carrying the same id."""
        waiter = _Waiter(event=threading.Event())
        with self._state_lock:
            message_id = self._next_id
            self._next_id += 1
            self._waiters[message_id] = waiter

        try:
            line = encode(with_id(message, message_id))
            with self._send_lock:
                if self._closed.is_set():
                    raise ConnectionClosed("connection is closed")
                self._sock.sendall(line.encode("utf-8"))

            if not waiter.event.wait(timeout_s):
                raise TimeoutExpired(f"no reply to {message.T} within {timeout_s}s")
            if waiter.message is None:
                raise ConnectionClosed(f"connection closed awaiting reply to {message.T}")
            if isinstance(waiter.message, ErrorFrame):
                raise ProtocolError(f"{message.T} rejected: {waiter.message.message}")
            return waiter.message
        finally:
            with self._state_lock:
                self._waiters.pop(message_id, None)

    def close(self) -> None:
        if self._closed.is_set():
            return
        self._closed.set()
        try:
            self._sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            # Already gone. Closing a closed socket is the normal shutdown race, not news.
            pass
        # The file object wraps a dup of the descriptor, so both have to be closed or the
        # descriptor leaks for as long as the process lives.
        try:
            self._reader.close()
        except OSError:
            pass
        self._sock.close()
        self._release_waiters()

    def _read_loop(self) -> None:
        try:
            for line in self._reader:
                if len(line.encode("utf-8")) > MAX_LINE_BYTES:
                    self._log(f"line exceeds {MAX_LINE_BYTES} bytes; closing")
                    break
                if not line.strip():
                    continue
                try:
                    message = decode(line)
                except ProtocolError as exc:
                    # Dropped rather than fatal: one malformed line from the control plane
                    # should not strand a step that is running fine.
                    self._log(f"undecodable message: {exc}")
                    continue
                self._deliver(message)
        except OSError as exc:
            self._log(f"read failed: {exc}")
        finally:
            self._closed.set()
            self._release_waiters()

    def _deliver(self, message: Message) -> None:
        with self._state_lock:
            waiter = self._waiters.get(message.id)
        if waiter is None:
            # A reply to a request that already timed out. Logged, because a steady stream
            # of these means the reply timeout is set below the control plane's latency.
            self._log(f"unmatched reply id={message.id} t={message.T}")
            return
        waiter.message = message
        waiter.event.set()

    def _release_waiters(self) -> None:
        with self._state_lock:
            waiters = list(self._waiters.values())
            self._waiters.clear()
        for waiter in waiters:
            waiter.event.set()


@dataclass(frozen=True)
class Outcome:
    """What one step attempt did, for the caller and for tests."""

    status: Literal["completed", "failed", "lease_lost"]
    ref: StepRef
    output: Any = None
    error: StepError | None = None
    heartbeats: int = 0


class PythonWorker:
    def __init__(
        self,
        connection: Connection,
        registry: CallableRegistry,
        worker_id: str,
        log: LogFn | None = None,
        reply_timeout_s: float = DEFAULT_REPLY_TIMEOUT_S,
    ) -> None:
        self._conn = connection
        self._registry = registry
        self._worker_id = worker_id
        self._log = log or (lambda _message: None)
        self._reply_timeout_s = reply_timeout_s

    @property
    def worker_id(self) -> str:
        return self._worker_id

    def run_once(self) -> Outcome | None:
        """Claims one step if there is one, runs it, and reports. ``None`` when idle."""
        reply = self._conn.request(
            LeaseRequest(worker=self._worker_id), timeout_s=self._reply_timeout_s
        )
        if isinstance(reply, LeaseEmpty):
            return None
        if not isinstance(reply, LeaseGranted):
            raise ProtocolError(f"expected a lease, got {reply.T}")
        return self._execute(reply)

    def run_forever(self, stop: threading.Event, idle_s: float = 0.05) -> None:
        while not stop.is_set():
            try:
                if self.run_once() is None:
                    stop.wait(idle_s)
            except (ConnectionClosed, OSError) as exc:
                self._log(f"connection lost: {exc}")
                return
            except ProtocolError as exc:
                # A wiring fault, not a step fault. Backing off rather than exiting keeps a
                # worker pool alive through a bad deploy of the control plane.
                self._log(f"protocol error: {exc}")
                stop.wait(idle_s)

    def _execute(self, grant: LeaseGranted) -> Outcome:
        lease_lost = threading.Event()
        finished = threading.Event()
        counter = _Counter()

        heartbeat = threading.Thread(
            target=self._heartbeat_loop,
            args=(grant, finished, lease_lost, counter),
            name=f"parliament-heartbeat-{grant.ref.step}",
            daemon=True,
        )
        heartbeat.start()

        try:
            output, error = self._invoke(grant)
        finally:
            finished.set()
            heartbeat.join(timeout=1.0)

        if lease_lost.is_set():
            # Nothing is reported and nothing is released: the lease belongs to another
            # worker now, and either message would interfere with its attempt.
            self._log(f"lease lost on {grant.ref.key()}; dropping result")
            return Outcome(
                status="lease_lost", ref=grant.ref, heartbeats=counter.value, error=error
            )

        if error is None:
            self._report(StepCompleted(
                worker=self._worker_id, lease=grant.lease, ref=grant.ref, output=output
            ))
            outcome = Outcome(
                status="completed", ref=grant.ref, output=output, heartbeats=counter.value
            )
        else:
            self._report(StepFailed(
                worker=self._worker_id, lease=grant.lease, ref=grant.ref, error=error
            ))
            outcome = Outcome(
                status="failed", ref=grant.ref, error=error, heartbeats=counter.value
            )

        self._release(grant)
        return outcome

    def _invoke(self, grant: LeaseGranted) -> tuple[Any, StepError | None]:
        """Resolves and runs the callable, converting any failure into a ``StepError``."""
        try:
            fn = self._registry.resolve(grant.callable_name)
        except UnknownCallableError as exc:
            # No traceback: the stack would show this worker's frames, not a cause. The
            # message names the registered alternatives, which is what a reader needs.
            return None, StepError(error_class="definition_error", message=str(exc))

        timeout_s = max(grant.timeout_ms, 1) / 1000.0
        try:
            return _call_with_timeout(fn, grant.input, timeout_s), None
        except TimeoutExpired as exc:
            return None, StepError(error_class="timeout", message=str(exc))
        except BaseException as exc:  # noqa: BLE001 - a step failure must not kill the worker
            return None, StepError(
                error_class="internal",
                message=f"{type(exc).__name__}: {exc}",
                traceback="".join(
                    traceback.format_exception(type(exc), exc, exc.__traceback__)
                ),
            )

    def _heartbeat_loop(
        self,
        grant: LeaseGranted,
        finished: threading.Event,
        lease_lost: threading.Event,
        counter: "_Counter",
    ) -> None:
        # A third of the lease, so two consecutive misses do not cost the lease.
        interval_s = max(grant.lease_duration_ms / 3.0, 1.0) / 1000.0
        while not finished.wait(interval_s):
            try:
                reply = self._conn.request(
                    Heartbeat(worker=self._worker_id, lease=grant.lease, ref=grant.ref),
                    timeout_s=self._reply_timeout_s,
                )
            except (TimeoutExpired, ConnectionClosed, ProtocolError, OSError) as exc:
                # One failed renewal is survivable; the lease still has time left. Only an
                # explicit refusal ends the step.
                self._log(f"heartbeat failed on {grant.ref.key()}: {exc}")
                continue
            counter.increment()
            if isinstance(reply, LeaseRenewed) and not reply.ok:
                lease_lost.set()
                return

    def _report(self, message: StepCompleted | StepFailed) -> None:
        try:
            reply = self._conn.request(message, timeout_s=self._reply_timeout_s)
        except (TimeoutExpired, ConnectionClosed, ProtocolError, OSError) as exc:
            # The event log is the authority. If the report did not land, the lease lapses
            # and the step is redelivered; raising here would lose the worker as well.
            self._log(f"report failed: {exc}")
            return
        if not isinstance(reply, Ack):
            self._log(f"unexpected reply to {message.T}: {reply.T}")

    def _release(self, grant: LeaseGranted) -> None:
        try:
            self._conn.request(
                LeaseRelease(worker=self._worker_id, lease=grant.lease, ref=grant.ref),
                timeout_s=self._reply_timeout_s,
            )
        except (TimeoutExpired, ConnectionClosed, ProtocolError, OSError) as exc:
            self._log(f"release failed: {exc}")


class _Counter:
    """A counter the heartbeat thread owns and the main thread reads."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._value = 0

    def increment(self) -> None:
        with self._lock:
            self._value += 1

    @property
    def value(self) -> int:
        with self._lock:
            return self._value


def _call_with_timeout(fn: Callable[[Any], Any], payload: Any, timeout_s: float) -> Any:
    """Runs ``fn`` on a thread and gives up waiting after ``timeout_s``.

    The thread is a daemon and is *not* killed on timeout, because Python offers no way to
    interrupt arbitrary code safely — ``PyThreadState_SetAsyncExc`` cannot interrupt a call
    blocked in C, and killing a thread mid-mutation corrupts whatever it was holding. So
    the contract is that the worker stops waiting, reports a timeout, and the abandoned
    thread may still finish. A callable that holds a resource must manage its own deadline;
    this is the outer bound, not the inner one.
    """
    box: dict[str, Any] = {}

    def target() -> None:
        try:
            box["value"] = fn(payload)
        except BaseException as exc:  # noqa: BLE001 - re-raised on the calling thread
            box["error"] = exc

    thread = threading.Thread(target=target, name="parliament-callable", daemon=True)
    thread.start()
    thread.join(timeout_s)
    if thread.is_alive():
        raise TimeoutExpired(f"callable exceeded {int(timeout_s * 1000)}ms")
    if "error" in box:
        raise box["error"]
    return box.get("value")
