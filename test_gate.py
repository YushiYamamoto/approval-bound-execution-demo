from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import FrozenInstanceError
import json
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest
from unittest.mock import patch

import gate
from gate import GateRejected, OutcomeUnknown, Request, approve, execute


class ApprovalGateTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db = Path(self.temp.name) / "approvals.sqlite3"
        self.request = Request("send_message", "team@example.invalid", "架空のお知らせ")
        self.approval_id = approve(self.db, self.request, clock=lambda: 100, ttl_seconds=30)
        self.accepted = []

    def effect(self, request):
        self.accepted.append(request)
        return "demo-receipt-1"

    def state(self, approval_id=None):
        with sqlite3.connect(self.db) as connection:
            return connection.execute(
                "SELECT state FROM approvals WHERE approval_id=?",
                (approval_id or self.approval_id,),
            ).fetchone()[0]

    def assert_rejected(self, reason, request=None, *, clock=lambda: 101, approval_id=None):
        with self.assertRaises(GateRejected) as caught:
            execute(self.db, approval_id or self.approval_id,
                    request or self.request, self.effect, clock=clock)
        self.assertEqual(caught.exception.reason, reason)

    def test_success_passes_same_immutable_value_and_records_receipt(self):
        receipt = execute(self.db, self.approval_id, self.request, self.effect, clock=lambda: 101)
        self.assertEqual(receipt, "demo-receipt-1")
        self.assertIs(self.accepted[0], self.request)
        self.assertEqual(self.state(), "SUCCEEDED")
        with self.assertRaises(FrozenInstanceError):
            self.request.body = "changed"
        with sqlite3.connect(self.db) as connection:
            stored = connection.execute("SELECT receipt FROM approvals").fetchone()[0]
        self.assertEqual(stored, receipt)

    def test_unicode_and_newlines_are_bound_without_normalization(self):
        request = Request("send_message", "チーム@example.invalid", "確認\nCafé ✅")
        approval_id = approve(self.db, request, clock=lambda: 100, ttl_seconds=30)
        execute(self.db, approval_id, request, self.effect, clock=lambda: 101)
        self.assertEqual(self.accepted, [request])
        changed = Request(request.action, request.destination, "確認\nCafe\u0301 ✅")
        self.assertNotEqual(gate.request_digest(request), gate.request_digest(changed))

    def test_body_replacement_is_rejected_without_consuming_approval(self):
        self.assert_rejected("CONTENT_MISMATCH", Request(
            self.request.action, self.request.destination, "差し替えた本文"))
        self.assertEqual(self.accepted, [])
        self.assertEqual(self.state(), "APPROVED")
        execute(self.db, self.approval_id, self.request, self.effect, clock=lambda: 102)
        self.assertEqual(len(self.accepted), 1)

    def test_destination_change_is_rejected(self):
        self.assert_rejected("DESTINATION_MISMATCH", Request(
            self.request.action, "another@example.invalid", self.request.body))
        self.assertEqual(self.accepted, [])

    def test_action_change_is_rejected(self):
        self.assert_rejected("ACTION_MISMATCH", Request(
            "delete_message", self.request.destination, self.request.body))
        self.assertEqual(self.accepted, [])

    def test_expiry_boundary_is_exclusive(self):
        before_expiry = approve(self.db, self.request, clock=lambda: 100, ttl_seconds=30)
        execute(self.db, before_expiry, self.request, self.effect, clock=lambda: 129)
        self.assertEqual(len(self.accepted), 1)
        self.assert_rejected("EXPIRED", clock=lambda: 130)
        self.assertEqual(len(self.accepted), 1)

    def test_expiry_is_measured_after_real_sqlite_writer_lock_wait(self):
        begin_attempted = threading.Event()
        logical_time = [100]
        clock_reads = []
        original_connect = gate._connect

        def trusted_clock():
            clock_reads.append(logical_time[0])
            return logical_time[0]

        def traced_connect(path):
            connection = original_connect(path)
            connection.set_trace_callback(lambda sql: begin_attempted.set()
                if sql.strip() == "BEGIN IMMEDIATE" else None)
            return connection

        # A separate, real connection owns the writer lock before execute starts.
        blocker = sqlite3.connect(self.db, isolation_level=None)
        blocker.execute("BEGIN IMMEDIATE")
        with ThreadPoolExecutor(max_workers=1) as executor:
            try:
                with patch("gate._connect", side_effect=traced_connect):
                    future = executor.submit(execute, self.db, self.approval_id,
                        self.request, self.effect, clock=trusted_clock)
                    self.assertTrue(begin_attempted.wait(timeout=5))
                    self.assertFalse(future.done())
                    logical_time[0] = 132  # The approval expires at 130 while waiting.
                    blocker.commit()
                    with self.assertRaises(GateRejected) as caught:
                        future.result(timeout=5)
            finally:
                blocker.close()
        self.assertEqual(caught.exception.reason, "EXPIRED")
        self.assertEqual(clock_reads, [132])
        self.assertEqual(self.accepted, [])
        self.assertEqual(self.state(), "APPROVED")

    def test_unknown_approval_is_rejected(self):
        self.assert_rejected("NOT_APPROVED", approval_id="0" * 32)
        self.assertEqual(self.accepted, [])

    def test_success_cannot_be_started_again(self):
        execute(self.db, self.approval_id, self.request, self.effect, clock=lambda: 101)
        self.assert_rejected("ALREADY_STARTED", clock=lambda: 102)
        self.assertEqual(len(self.accepted), 1)

    def test_two_real_connections_compete_for_one_start(self):
        start = threading.Barrier(3)
        entered = threading.Event()
        release = threading.Event()
        lock = threading.Lock()

        def blocking_effect(request):
            with lock:
                self.accepted.append(request)
            entered.set()
            if not release.wait(timeout=5):
                raise TimeoutError("test coordination failed")
            return "concurrent-receipt"

        def contender():
            start.wait(timeout=5)
            try:
                result = execute(self.db, self.approval_id, self.request,
                                 blocking_effect, clock=lambda: 101)
                return "success", result
            except GateRejected as error:
                return "rejected", error.reason

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(contender) for _ in range(2)]
            try:
                start.wait(timeout=5)
                self.assertTrue(entered.wait(timeout=5))
                done, _ = wait(futures, timeout=5, return_when=FIRST_COMPLETED)
                self.assertEqual(len(done), 1)
                self.assertEqual(next(iter(done)).result(), ("rejected", "ALREADY_STARTED"))
                self.assertEqual(self.state(), "RUNNING")
            finally:
                release.set()
            results = [future.result(timeout=5) for future in futures]
        self.assertEqual(sorted(result[0] for result in results), ["rejected", "success"])
        self.assertEqual(len(self.accepted), 1)
        self.assertEqual(self.state(), "SUCCEEDED")

    def test_timeout_after_acceptance_is_unknown_and_not_retried(self):
        def accepted_then_timeout(request):
            self.accepted.append(request)
            raise TimeoutError("response lost after acceptance")

        with self.assertRaises(OutcomeUnknown) as caught:
            execute(self.db, self.approval_id, self.request, accepted_then_timeout, clock=lambda: 101)
        self.assertEqual(caught.exception.reason, "EFFECT_RAISED")
        self.assertEqual(self.state(), "UNKNOWN")
        self.assert_rejected("ALREADY_STARTED", clock=lambda: 102)
        self.assertEqual(len(self.accepted), 1)

    def test_empty_receipt_is_unknown_and_not_retried(self):
        def empty_receipt(request):
            self.accepted.append(request)
            return ""

        with self.assertRaises(OutcomeUnknown) as caught:
            execute(self.db, self.approval_id, self.request, empty_receipt, clock=lambda: 101)
        self.assertEqual(caught.exception.reason, "INVALID_RECEIPT")
        self.assertEqual(self.state(), "UNKNOWN")
        self.assert_rejected("ALREADY_STARTED")
        self.assertEqual(len(self.accepted), 1)

    def test_interrupt_leaves_running_and_is_not_retried(self):
        def interrupted(request):
            self.accepted.append(request)
            raise KeyboardInterrupt()

        with self.assertRaises(KeyboardInterrupt):
            execute(self.db, self.approval_id, self.request, interrupted, clock=lambda: 101)
        self.assertEqual(self.state(), "RUNNING")
        self.assert_rejected("ALREADY_STARTED")
        self.assertEqual(len(self.accepted), 1)

    def test_invalid_request_values_and_limits(self):
        invalid = [
            ("", "destination", "body"), ("send", " ", "body"),
            ("send", "destination", ""), ("send", "destination", 123),
            ("send now", "destination", "body"),
            ("s" * 65, "destination", "body"),
            ("send", "d" * 257, "body"),
            ("send", "destination", "あ" * 21_846),
            ("send", "destination", "\ud800"),
        ]
        for values in invalid:
            with self.subTest(values_type=tuple(type(value).__name__ for value in values)):
                with self.assertRaises(ValueError):
                    Request(*values)

    def test_invalid_ttl_timestamp_and_approval_id(self):
        for ttl in (0, -1, True, 1.5, gate.MAX_TTL_SECONDS + 1):
            with self.subTest(ttl=ttl), self.assertRaises(ValueError):
                approve(self.db, self.request, clock=lambda: 100, ttl_seconds=ttl)
        for now in (-1, True, 1.5, 2**63):
            with self.subTest(now=now), self.assertRaises(ValueError):
                approve(self.db, self.request, clock=lambda: now, ttl_seconds=10)
        with self.assertRaises(ValueError):
            approve(self.db, self.request, clock=None, ttl_seconds=10)
        with self.assertRaises(ValueError):
            execute(self.db, self.approval_id, self.request, self.effect, clock=None)
        with self.assertRaises(ValueError):
            execute(self.db, "not-an-approval", self.request, self.effect, clock=lambda: 101)
        with self.assertRaises(ValueError):
            approve(self.db, {"action": "send"}, clock=lambda: 100, ttl_seconds=10)

    def test_canonical_json_has_named_fields_and_schema_version(self):
        expected = json.dumps(
            {"schema_version": 1, "action": "send_message",
             "destination": "team@example.invalid", "body": "架空のお知らせ"},
            ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        ).encode("utf-8")
        import hashlib
        self.assertEqual(gate.request_digest(self.request), hashlib.sha256(expected).hexdigest())
        first = Request("send", "ab", "c")
        second = Request("send", "a", "bc")
        self.assertNotEqual(gate.request_digest(first), gate.request_digest(second))

    def test_audit_omits_body_destination_and_exception_text(self):
        def fail(request):
            raise RuntimeError("private-error-detail-123")

        with self.assertRaises(OutcomeUnknown):
            execute(self.db, self.approval_id, self.request, fail, clock=lambda: 101)
        with sqlite3.connect(self.db) as connection:
            events = connection.execute("SELECT * FROM events").fetchall()
            approvals = connection.execute("SELECT * FROM approvals").fetchall()
        stored = repr(events + approvals)
        for forbidden in (self.request.destination, self.request.body, "private-error-detail-123"):
            self.assertNotIn(forbidden, stored)
        self.assertIn("EFFECT_RAISED", repr(events))

    def test_effect_success_followed_by_db_error_is_unknown_and_not_retried(self):
        with patch("gate._finish", side_effect=sqlite3.OperationalError("simulated disk error")):
            with self.assertRaises(OutcomeUnknown) as caught:
                execute(self.db, self.approval_id, self.request, self.effect, clock=lambda: 101)
        self.assertEqual(caught.exception.reason, "RESULT_RECORD_FAILED")
        self.assertEqual(self.state(), "RUNNING")
        self.assert_rejected("ALREADY_STARTED")
        self.assertEqual(len(self.accepted), 1)

    def test_non_callable_effect_does_not_consume_approval(self):
        with self.assertRaises(ValueError):
            execute(self.db, self.approval_id, self.request, None, clock=lambda: 101)
        self.assertEqual(self.state(), "APPROVED")
        self.assertEqual(self.accepted, [])


if __name__ == "__main__":
    unittest.main()
