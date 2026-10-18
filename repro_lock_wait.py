"""Supplementary, controlled-clock lock-wait reproduction; local SQLite only.

Run with ``python3 -B repro_lock_wait.py``. This is separate from the original
wall-clock reproduction: the trusted test clock advances from 100 to 132 while
an approval expiring at 130 waits for a real SQLite writer lock.
"""

import hashlib
import importlib.util
import json
from pathlib import Path
import sqlite3
import sys
from tempfile import TemporaryDirectory
import threading


ROOT = Path(__file__).resolve().parent
BEFORE_SHA256 = "103faa086d533acfa9fb3190f63fbae092f2007d815dfd905351821e8c9ab624"
WAIT_SECONDS = 6  # Longer than gate.py's bounded five-second SQLite timeout.


def load_gate(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def reproduce(module, db_path, *, fixed):
    clock_now = 100

    def trusted_clock():
        return clock_now

    request = module.Request("send_message", "local-stub", "Approved demo body")
    approval_id = module.approve(
        db_path, request, ttl_seconds=30,
        **({"clock": trusted_clock} if fixed else {"now": 100}),
    )
    with sqlite3.connect(db_path) as connection:
        expires_at = connection.execute(
            "SELECT expires_at FROM approvals WHERE approval_id=?", (approval_id,)
        ).fetchone()[0]
    if expires_at != 130:
        raise RuntimeError(f"Unexpected approval deadline: {expires_at}")

    begin_reached = threading.Event()
    original_connect = module._connect

    def traced_connect(path):
        connection = original_connect(path)
        connection.set_trace_callback(
            lambda sql: begin_reached.set() if sql.strip().upper() == "BEGIN IMMEDIATE" else None
        )
        return connection

    module._connect = traced_connect
    effect_calls = []
    outcome = {}

    def effect(validated_request):
        effect_calls.append(validated_request)
        return "local-stub-receipt"

    def run_execute():
        try:
            outcome["receipt"] = module.execute(
                db_path, approval_id, request, effect,
                **({"clock": trusted_clock} if fixed else {"now": trusted_clock()}),
            )
        except module.GateRejected as error:
            outcome["rejected"] = error.reason
        except BaseException as error:
            outcome["error"] = error

    blocker = sqlite3.connect(db_path, timeout=5, isolation_level=None)
    worker = threading.Thread(target=run_execute, name="lock-wait-execute")
    started = False
    try:
        blocker.execute("BEGIN IMMEDIATE")
        worker.start()
        started = True
        # SQLite traces BEGIN before acquiring the lock, which blocker holds.
        if not begin_reached.wait(WAIT_SECONDS):
            raise RuntimeError("Timed out waiting for execute to reach BEGIN IMMEDIATE")
        clock_now = 132
    finally:
        # Always unblock and join, including failures before the clock advances.
        try:
            blocker.rollback()
        finally:
            blocker.close()
            if started:
                worker.join(WAIT_SECONDS)
            module._connect = original_connect
        if started and worker.is_alive():
            raise RuntimeError("Timed out waiting for the execute thread to stop")

    if "error" in outcome:
        raise RuntimeError("Execute thread failed") from outcome["error"]
    with sqlite3.connect(db_path) as connection:
        state = connection.execute(
            "SELECT state FROM approvals WHERE approval_id=?", (approval_id,)
        ).fetchone()[0]
        reason, observed_at = connection.execute(
            "SELECT reason,observed_at FROM events WHERE approval_id=? ORDER BY event_id DESC LIMIT 1",
            (approval_id,),
        ).fetchone()

    actual = (state, reason, observed_at, len(effect_calls))
    expected = ("APPROVED", "EXPIRED", 132, 0) if fixed else (
        "SUCCEEDED", "RECEIPT_RECORDED", 100, 1
    )
    if actual != expected or outcome != (
        {"rejected": "EXPIRED"} if fixed else {"receipt": "local-stub-receipt"}
    ):
        raise RuntimeError(f"Unexpected reproduction result: {actual!r}, {outcome!r}")
    return {
        "clock_before": 100, "clock_after": clock_now, "expires_at": expires_at,
        "begin_reached_while_blocked": begin_reached.is_set(),
        "state": state, "reason": reason, "observed_at": observed_at,
        "effect_calls": len(effect_calls),
    }


def main():
    paths = {"before": ROOT / "history/gate_before_clock_fix.py", "after": ROOT / "gate.py"}
    original_bytes = {name: path.read_bytes() for name, path in paths.items()}
    hashes = {name: hashlib.sha256(data).hexdigest() for name, data in original_bytes.items()}
    if hashes["before"] != BEFORE_SHA256:
        raise RuntimeError("Historical source SHA-256 does not match the recorded original")
    modules = {name: load_gate(f"lock_wait_{name}", path) for name, path in paths.items()}
    with TemporaryDirectory(prefix="approval-lock-wait-") as temporary:
        results = {
            name: reproduce(module, str(Path(temporary) / f"{name}.sqlite3"), fixed=name == "after")
            for name, module in modules.items()
        }
    if any(path.read_bytes() != original_bytes[name] for name, path in paths.items()):
        raise RuntimeError("A gate source file changed during reproduction")
    print(json.dumps({"source_sha256": hashes, **results}, indent=2))


if __name__ == "__main__":
    main()
