"""Compare a bare approval flag and a bound one-start gate, entirely locally."""

import json
from pathlib import Path
import tempfile

from gate import GateRejected, OutcomeUnknown, Request, approve, execute


APPROVED = Request("send_message", "demo@example.invalid", "架空の承認済み本文")
CHANGED = Request(APPROVED.action, APPROVED.destination, "差し替えた架空の本文")


def run():
    rows = []
    # Deliberately weak baseline: the flag is not tied to the actual input.
    old_outbox = []
    approval_flag = True
    if approval_flag:
        old_outbox.append(CHANGED)
    with tempfile.TemporaryDirectory() as temporary:
        db = Path(temporary) / "gate.sqlite3"

        def effect(request):
            new_outbox.append(request)
            return "demo-receipt"

        new_outbox = []
        approval_id = approve(db, APPROVED, clock=lambda: 100, ttl_seconds=30)
        try:
            execute(db, approval_id, CHANGED, effect, clock=lambda: 101)
        except GateRejected as error:
            result = error.reason
        rows.append({"case": "body_replacement", "approval_flag_effects": len(old_outbox),
                     "bound_gate_effects": len(new_outbox), "bound_gate_result": result})

        old_outbox = []
        for _ in range(2):
            if approval_flag:
                old_outbox.append(APPROVED)
        new_outbox = []
        approval_id = approve(db, APPROVED, clock=lambda: 100, ttl_seconds=30)
        execute(db, approval_id, APPROVED, effect, clock=lambda: 101)
        try:
            execute(db, approval_id, APPROVED, effect, clock=lambda: 102)
        except GateRejected as error:
            result = error.reason
        rows.append({"case": "repeat_execution", "approval_flag_effects": len(old_outbox),
                     "bound_gate_effects": len(new_outbox), "bound_gate_result": result})

        old_outbox = []
        for _ in range(2):
            try:
                if approval_flag:
                    old_outbox.append(APPROVED)
                    raise TimeoutError("simulated acceptance, response lost")
            except TimeoutError:
                pass  # The weak baseline blindly retries on the second pass.
        new_outbox = []

        def accepted_then_timeout(request):
            new_outbox.append(request)
            raise TimeoutError("simulated acceptance, response lost")

        approval_id = approve(db, APPROVED, clock=lambda: 100, ttl_seconds=30)
        try:
            execute(db, approval_id, APPROVED, accepted_then_timeout, clock=lambda: 101)
        except OutcomeUnknown:
            result = "UNKNOWN"
        try:
            execute(db, approval_id, APPROVED, accepted_then_timeout, clock=lambda: 102)
        except GateRejected as error:
            retry = error.reason
        rows.append({"case": "accepted_then_timeout", "approval_flag_effects": len(old_outbox),
                     "bound_gate_effects": len(new_outbox), "bound_gate_result": result,
                     "retry_result": retry})
    return rows


if __name__ == "__main__":
    print(json.dumps(run(), ensure_ascii=False, indent=2))
