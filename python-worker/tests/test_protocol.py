"""Round-trip every message, and check that malformed input is rejected rather than guessed."""

from __future__ import annotations

import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from parliament_worker.protocol import (  # noqa: E402 - after the path bootstrap
    Ack,
    ErrorFrame,
    Heartbeat,
    LeaseEmpty,
    LeaseGranted,
    LeaseRelease,
    LeaseRenewed,
    LeaseRequest,
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

REF = StepRef(workflow_id="wf-1", step="summarise", attempt=2)

EVERY_MESSAGE: tuple[Message, ...] = (
    LeaseRequest(worker="py-1", kinds=("python",), id=1),
    LeaseGranted(
        ref=REF,
        lease="token-abc",
        callable_name="score",
        input={"rows": [1, 2, 3]},
        timeout_ms=5000,
        lease_duration_ms=30000,
        id=2,
    ),
    LeaseEmpty(id=3),
    Heartbeat(worker="py-1", lease="token-abc", ref=REF, id=4),
    LeaseRenewed(ok=True, expires_at=1234, id=5),
    StepCompleted(worker="py-1", lease="token-abc", ref=REF, output={"score": 0.5}, id=6),
    StepFailed(
        worker="py-1",
        lease="token-abc",
        ref=REF,
        error=StepError(error_class="internal", message="boom", traceback="Traceback..."),
        id=7,
    ),
    LeaseRelease(worker="py-1", lease="token-abc", ref=REF, id=8),
    Ack(id=9),
    ErrorFrame(message="unknown worker", id=10),
)


class TestRoundTrip(unittest.TestCase):
    def test_every_message_survives_a_round_trip(self) -> None:
        for message in EVERY_MESSAGE:
            with self.subTest(t=message.T):
                self.assertEqual(decode(encode(message)), message)

    def test_every_message_is_one_line(self) -> None:
        for message in EVERY_MESSAGE:
            with self.subTest(t=message.T):
                line = encode(message)
                self.assertTrue(line.endswith("\n"))
                self.assertEqual(line.count("\n"), 1)

    def test_encoding_is_stable(self) -> None:
        # Two encodings of the same message must be byte-identical, or a captured line
        # cannot be used as a fixture and a diff of two logs is noise.
        for message in EVERY_MESSAGE:
            with self.subTest(t=message.T):
                self.assertEqual(encode(message), encode(message))

    def test_optional_traceback_is_omitted_when_absent(self) -> None:
        failed = StepFailed(
            worker="py-1",
            lease="t",
            ref=REF,
            error=StepError(error_class="definition_error", message="no such callable"),
            id=1,
        )
        self.assertNotIn("traceback", encode(failed))
        self.assertIsNone(decode(encode(failed)).error.traceback)

    def test_with_id_does_not_mutate(self) -> None:
        original = LeaseRequest(worker="py-1")
        stamped = with_id(original, 42)
        self.assertEqual(original.id, 0)
        self.assertEqual(stamped.id, 42)


class TestRejection(unittest.TestCase):
    def test_unknown_type_is_rejected(self) -> None:
        with self.assertRaises(ProtocolError) as caught:
            decode('{"t":"lease.steal","id":1}')
        self.assertIn("lease.steal", str(caught.exception))

    def test_missing_id_is_rejected(self) -> None:
        with self.assertRaises(ProtocolError):
            decode('{"t":"ack"}')

    def test_missing_field_is_rejected(self) -> None:
        with self.assertRaises(ProtocolError) as caught:
            decode('{"t":"lease.heartbeat","id":1,"worker":"py-1","lease":"t"}')
        self.assertIn("ref", str(caught.exception))

    def test_boolean_is_not_an_integer(self) -> None:
        # bool subclasses int, so an attempt of `true` would pass a naive isinstance check.
        with self.assertRaises(ProtocolError):
            decode(
                '{"t":"lease.heartbeat","id":1,"worker":"p","lease":"t",'
                '"ref":{"workflow_id":"w","step":"s","attempt":true}}'
            )

    def test_non_json_is_rejected(self) -> None:
        with self.assertRaises(ProtocolError):
            decode("not json at all")

    def test_non_object_is_rejected(self) -> None:
        with self.assertRaises(ProtocolError):
            decode("[1,2,3]")


if __name__ == "__main__":
    unittest.main()
