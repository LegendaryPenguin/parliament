"""Worker behaviour against a fake control plane over a real socket."""

from __future__ import annotations

import pathlib
import sys
import threading
import time
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from fake_control_plane import FakeControlPlane, grant  # noqa: E402 - after the bootstrap
from parliament_worker.protocol import (  # noqa: E402
    Heartbeat,
    LeaseRelease,
    LeaseRequest,
    StepCompleted,
    StepFailed,
)
from parliament_worker.registry import CallableRegistry  # noqa: E402
from parliament_worker.worker import Connection, PythonWorker  # noqa: E402


class WorkerTestCase(unittest.TestCase):
    """Wires a worker to a fake plane and tears both down whatever the test does."""

    def build(self, plane: FakeControlPlane, registry: CallableRegistry) -> PythonWorker:
        plane.start()
        self.addCleanup(plane.stop)
        connection = Connection(plane.connect())
        connection.start()
        self.addCleanup(connection.close)
        return PythonWorker(connection, registry, worker_id="py-test", reply_timeout_s=5.0)


class TestHappyPath(WorkerTestCase):
    def test_executes_and_reports_completion(self) -> None:
        registry = CallableRegistry()
        registry.register("double", lambda payload: {"value": payload["value"] * 2})
        plane = FakeControlPlane([grant(callable_name="double", payload={"value": 21})])
        worker = self.build(plane, registry)

        outcome = worker.run_once()

        assert outcome is not None
        self.assertEqual(outcome.status, "completed")
        self.assertEqual(outcome.output, {"value": 42})

        reports = [m for m in plane.reports if isinstance(m, StepCompleted)]
        self.assertEqual(len(reports), 1)
        self.assertEqual(reports[0].output, {"value": 42})
        self.assertEqual(reports[0].ref.step, "step-1")

    def test_releases_the_lease_after_reporting(self) -> None:
        registry = CallableRegistry()
        registry.register("noop", lambda _payload: None)
        plane = FakeControlPlane([grant(callable_name="noop")])
        worker = self.build(plane, registry)

        worker.run_once()

        self.assertEqual(len(plane.releases), 1)
        # Report before release: a release that lands first makes the step eligible again
        # while this worker's own outcome is still in flight.
        kinds = [type(m) for m in plane.received]
        self.assertLess(kinds.index(StepCompleted), kinds.index(LeaseRelease))

    def test_idle_plane_yields_no_work(self) -> None:
        plane = FakeControlPlane([])
        worker = self.build(plane, CallableRegistry())

        self.assertIsNone(worker.run_once())
        self.assertEqual([type(m) for m in plane.received], [LeaseRequest])

    def test_heartbeats_continue_during_a_long_step(self) -> None:
        # The lease is 120ms, so renewal is every 40ms; the callable takes 250ms. A worker
        # that heartbeated only between steps would send none of these.
        registry = CallableRegistry()
        registry.register("slow", lambda _payload: time.sleep(0.25) or "done")
        plane = FakeControlPlane([grant(callable_name="slow", lease_duration_ms=120)])
        worker = self.build(plane, registry)

        outcome = worker.run_once()

        assert outcome is not None
        self.assertEqual(outcome.status, "completed")
        self.assertGreaterEqual(outcome.heartbeats, 2)
        self.assertGreaterEqual(len([m for m in plane.received if isinstance(m, Heartbeat)]), 2)


class TestFailurePaths(WorkerTestCase):
    def test_timing_out_callable_fails_with_a_timeout(self) -> None:
        registry = CallableRegistry()
        registry.register("hang", lambda _payload: time.sleep(5))
        plane = FakeControlPlane([grant(callable_name="hang", timeout_ms=80)])
        worker = self.build(plane, registry)

        outcome = worker.run_once()

        assert outcome is not None and outcome.error is not None
        self.assertEqual(outcome.status, "failed")
        self.assertEqual(outcome.error.error_class, "timeout")
        self.assertIn("80ms", outcome.error.message)
        self.assertEqual(len([m for m in plane.reports if isinstance(m, StepFailed)]), 1)

    def test_raising_callable_reports_its_traceback(self) -> None:
        def explode(_payload: object) -> None:
            raise ValueError("the ledger does not balance")

        registry = CallableRegistry()
        registry.register("explode", explode)
        plane = FakeControlPlane([grant(callable_name="explode")])
        worker = self.build(plane, registry)

        outcome = worker.run_once()

        assert outcome is not None and outcome.error is not None
        self.assertEqual(outcome.status, "failed")
        self.assertEqual(outcome.error.error_class, "internal")
        self.assertIn("the ledger does not balance", outcome.error.message)
        assert outcome.error.traceback is not None
        self.assertIn("ValueError", outcome.error.traceback)
        # The frame of the failing callable is the whole point of shipping the traceback.
        self.assertIn("explode", outcome.error.traceback)

    def test_unknown_callable_is_rejected_as_a_definition_error(self) -> None:
        registry = CallableRegistry()
        registry.register("known", lambda _payload: None)
        plane = FakeControlPlane([grant(callable_name="missing")])
        worker = self.build(plane, registry)

        outcome = worker.run_once()

        assert outcome is not None and outcome.error is not None
        self.assertEqual(outcome.error.error_class, "definition_error")
        self.assertIn("missing", outcome.error.message)
        # The registered alternatives are named, so a typo is diagnosable from the event.
        self.assertIn("known", outcome.error.message)
        self.assertIsNone(outcome.error.traceback)

    def test_worker_survives_a_failing_step_and_takes_the_next_one(self) -> None:
        def explode(_payload: object) -> None:
            raise RuntimeError("first step is bad")

        registry = CallableRegistry()
        registry.register("explode", explode)
        registry.register("ok", lambda _payload: "fine")
        plane = FakeControlPlane(
            [
                grant(callable_name="explode", step="bad"),
                grant(callable_name="ok", step="good"),
            ]
        )
        worker = self.build(plane, registry)

        first = worker.run_once()
        second = worker.run_once()

        assert first is not None and second is not None
        self.assertEqual(first.status, "failed")
        self.assertEqual(second.status, "completed")
        self.assertEqual(second.output, "fine")


class TestLeaseLoss(WorkerTestCase):
    def test_a_revoked_lease_reports_nothing_and_releases_nothing(self) -> None:
        # The plane refuses every renewal, so the step finishes holding a lease that is no
        # longer its own. Reporting then would overwrite the new holder's outcome.
        registry = CallableRegistry()
        registry.register("slow", lambda _payload: time.sleep(0.25) or "done")
        plane = FakeControlPlane(
            [grant(callable_name="slow", lease_duration_ms=120)], renew_ok=False
        )
        worker = self.build(plane, registry)

        outcome = worker.run_once()

        assert outcome is not None
        self.assertEqual(outcome.status, "lease_lost")
        self.assertEqual(plane.reports, [])
        self.assertEqual(plane.releases, [])


class TestConnection(WorkerTestCase):
    def test_concurrent_requests_get_their_own_replies(self) -> None:
        # Heartbeats and reports share one socket, so a reply must be matched by id rather
        # than by arrival order. Two in-flight requests is the smallest case that shows it.
        registry = CallableRegistry()
        registry.register("noop", lambda _payload: None)
        plane = FakeControlPlane([])
        plane.start()
        self.addCleanup(plane.stop)
        connection = Connection(plane.connect())
        connection.start()
        self.addCleanup(connection.close)

        replies: list[int] = []
        lock = threading.Lock()

        def ask() -> None:
            reply = connection.request(LeaseRequest(worker="py-test"), timeout_s=5.0)
            with lock:
                replies.append(reply.id)

        threads = [threading.Thread(target=ask) for _ in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=5.0)

        self.assertEqual(len(replies), 4)
        self.assertEqual(len(set(replies)), 4)


if __name__ == "__main__":
    unittest.main()
