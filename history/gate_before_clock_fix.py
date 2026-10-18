"""Approval-bound execution demo. No network or external service is used."""

from dataclasses import dataclass
import hashlib
import hmac
import json
import re
import sqlite3
from typing import Callable
import uuid


SCHEMA_VERSION = 1
MAX_BODY_BYTES = 65_536
MAX_TTL_SECONDS = 86_400


@dataclass(frozen=True, slots=True)
class Request:
    action: str
    destination: str
    body: str

    def __post_init__(self):
        for field, maximum in (
            ("action", 64), ("destination", 256), ("body", MAX_BODY_BYTES)
        ):
            value = getattr(self, field)
            if type(value) is not str or not value.strip():
                raise ValueError(f"INVALID_{field.upper()}")
            try:
                size = len(value.encode("utf-8"))
            except UnicodeEncodeError:
                raise ValueError(f"INVALID_{field.upper()}") from None
            if size > maximum:
                raise ValueError(f"TOO_LARGE_{field.upper()}")
        if re.fullmatch(r"[a-z][a-z0-9_]*", self.action) is None:
            raise ValueError("INVALID_ACTION")


class GateRejected(RuntimeError):
    def __init__(self, reason: str):
        self.reason = reason
        super().__init__(reason)


class OutcomeUnknown(RuntimeError):
    def __init__(self, reason: str):
        self.reason = reason
        super().__init__(reason)


def _validate_request(request):
    if type(request) is not Request:
        raise ValueError("INVALID_REQUEST")
    request.__post_init__()


def _validate_now(now):
    # Trusted runner / deterministic test seam, not a client-supplied timestamp.
    if type(now) is not int or not 0 <= now <= 2**63 - 1 - MAX_TTL_SECONDS:
        raise ValueError("INVALID_NOW")


def request_digest(request: Request) -> str:
    _validate_request(request)
    encoded = json.dumps(
        {"schema_version": SCHEMA_VERSION, "action": request.action,
         "destination": request.destination, "body": request.body},
        ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _destination_digest(request):
    return hashlib.sha256(request.destination.encode("utf-8")).hexdigest()


def _connect(db_path):
    connection = sqlite3.connect(db_path, timeout=5, isolation_level=None)
    connection.row_factory = sqlite3.Row
    connection.executescript("""
        CREATE TABLE IF NOT EXISTS approvals (
            approval_id TEXT PRIMARY KEY,
            schema_version INTEGER NOT NULL,
            digest TEXT NOT NULL,
            action TEXT NOT NULL,
            destination_digest TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            expires_at INTEGER NOT NULL,
            state TEXT NOT NULL CHECK (
                state IN ('APPROVED', 'RUNNING', 'SUCCEEDED', 'UNKNOWN')
            ),
            receipt TEXT
        );
        CREATE TABLE IF NOT EXISTS events (
            event_id INTEGER PRIMARY KEY AUTOINCREMENT,
            approval_id TEXT NOT NULL,
            state TEXT NOT NULL,
            reason TEXT NOT NULL,
            observed_at INTEGER NOT NULL
        );
    """)
    return connection


def _event(connection, approval_id, state, reason, now):
    connection.execute(
        "INSERT INTO events(approval_id,state,reason,observed_at) VALUES(?,?,?,?)",
        (approval_id, state, reason, now),
    )


def approve(db_path, request: Request, *, now: int, ttl_seconds: int) -> str:
    """Called only after the trusted runner has obtained human approval."""
    _validate_request(request)
    _validate_now(now)
    if type(ttl_seconds) is not int or not 1 <= ttl_seconds <= MAX_TTL_SECONDS:
        raise ValueError("INVALID_TTL")
    approval_id = uuid.uuid4().hex
    connection = _connect(db_path)
    try:
        connection.execute("BEGIN IMMEDIATE")
        connection.execute(
            """INSERT INTO approvals VALUES(?,?,?,?,?,?,?,'APPROVED',NULL)""",
            (approval_id, SCHEMA_VERSION, request_digest(request), request.action,
             _destination_digest(request), now, now + ttl_seconds),
        )
        _event(connection, approval_id, "APPROVED", "HUMAN_APPROVAL_RECORDED", now)
        connection.commit()
    finally:
        connection.close()
    return approval_id


def _reject(connection, approval_id, state, reason, now):
    _event(connection, approval_id, state, reason, now)
    connection.commit()
    raise GateRejected(reason)


def _finish(db_path, approval_id, state, reason, receipt, now):
    connection = _connect(db_path)
    try:
        connection.execute("BEGIN IMMEDIATE")
        changed = connection.execute(
            "UPDATE approvals SET state=?,receipt=? WHERE approval_id=? AND state='RUNNING'",
            (state, receipt, approval_id),
        ).rowcount
        if changed != 1:
            raise sqlite3.DatabaseError("STATE_TRANSITION_FAILED")
        _event(connection, approval_id, state, reason, now)
        connection.commit()
    finally:
        connection.close()


def _finish_or_unknown(db_path, approval_id, state, reason, receipt, now):
    try:
        _finish(db_path, approval_id, state, reason, receipt, now)
    except sqlite3.Error:
        # The effect may have succeeded. Do not retry the effect on DB failure.
        raise OutcomeUnknown("RESULT_RECORD_FAILED") from None


def execute(db_path, approval_id: str, request: Request,
            effect: Callable[[Request], str], *, now: int) -> str:
    """Claim one execution start, then invoke the trusted effect once."""
    _validate_request(request)
    _validate_now(now)
    if type(approval_id) is not str or re.fullmatch(r"[0-9a-f]{32}", approval_id) is None:
        raise ValueError("INVALID_APPROVAL_ID")
    if not callable(effect):
        raise ValueError("INVALID_EFFECT")

    connection = _connect(db_path)
    try:
        # The writer lock plus the conditional UPDATE serializes contenders.
        connection.execute("BEGIN IMMEDIATE")
        row = connection.execute(
            "SELECT * FROM approvals WHERE approval_id=?", (approval_id,)
        ).fetchone()
        if row is None:
            _reject(connection, approval_id, "REJECTED", "NOT_APPROVED", now)
        if row["state"] != "APPROVED":
            _reject(connection, approval_id, row["state"], "ALREADY_STARTED", now)
        if now >= row["expires_at"]:
            _reject(connection, approval_id, row["state"], "EXPIRED", now)
        if row["action"] != request.action:
            _reject(connection, approval_id, row["state"], "ACTION_MISMATCH", now)
        if not hmac.compare_digest(row["destination_digest"], _destination_digest(request)):
            _reject(connection, approval_id, row["state"], "DESTINATION_MISMATCH", now)
        if (row["schema_version"] != SCHEMA_VERSION
                or not hmac.compare_digest(row["digest"], request_digest(request))):
            _reject(connection, approval_id, row["state"], "CONTENT_MISMATCH", now)

        changed = connection.execute(
            """UPDATE approvals SET state='RUNNING'
               WHERE approval_id=? AND state='APPROVED' AND expires_at>?
                 AND schema_version=? AND digest=?""",
            (approval_id, now, SCHEMA_VERSION, request_digest(request)),
        ).rowcount
        if changed != 1:
            _reject(connection, approval_id, "REJECTED", "CLAIM_FAILED", now)
        _event(connection, approval_id, "RUNNING", "EXECUTION_CLAIMED", now)
        # Commit BEFORE the effect. A crash after this point must not re-send.
        connection.commit()
    finally:
        connection.close()

    try:
        # Use this same validated, frozen value; never re-read a mutable file.
        receipt = effect(request)
    except Exception:
        _finish_or_unknown(db_path, approval_id, "UNKNOWN", "EFFECT_RAISED", None, now)
        raise OutcomeUnknown("EFFECT_RAISED") from None

    if type(receipt) is not str or not receipt.strip() or len(receipt) > 256:
        _finish_or_unknown(db_path, approval_id, "UNKNOWN", "INVALID_RECEIPT", None, now)
        raise OutcomeUnknown("INVALID_RECEIPT")
    _finish_or_unknown(db_path, approval_id, "SUCCEEDED", "RECEIPT_RECORDED", receipt, now)
    return receipt
