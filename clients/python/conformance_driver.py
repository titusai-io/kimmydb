#!/usr/bin/env python3
"""The Python client's conformance driver.

One of three programs that answer the same questions in three languages. The
runner (`clients/conformance/run.py`) executes every scenario against every
driver and compares what comes back to expectations declared once, in
`clients/conformance/scenarios.json`.

**A driver reports observations; it does not decide whether they are right.**
Three clients that each judged themselves would be three opinions, and what is
wanted is one oracle and three answers.

    conformance_driver.py list
    conformance_driver.py run <scenario> <base-url> [dead-url]

Output is a single JSON object on stdout. Anything else goes to stderr.
"""

from __future__ import annotations

import json
import os
import sys
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "src"))

from kimmydb import Client, KimmyError, Pages, TransportError  # noqa: E402

SCENARIOS = [
    "capabilities",
    "documents_round_trip",
    "unlimited_find_is_a_page",
    "paging_walks_everything",
    "walk_ends_on_empty_page",
    "cursor_refuses_what_it_cannot_page",
    "creating_a_collection_twice_is_a_conflict",
    "duplicate_key_is_typed",
    "token_is_renewed",
    "failover_past_a_dead_endpoint",
    "write_is_not_retried_elsewhere",
    "change_stream_delivers",
    "change_stream_resumes",
    "dropped_collection_ends_stream",
    "recreated_collection_serves_its_own_history",
    "stale_resume_token_is_refused",
]

PASSWORD = os.environ.get("KIMMY_ROOT_PASSWORD", "conformance-password")


def connect(base: str) -> Client:
    return Client(base, user="root", password=PASSWORD)


def seed(db: Client, n: int) -> None:
    db.create_collection("shop", "orders")
    if n:
        db.insert_many("shop", "orders", [{"_id": i, "qty": i} for i in range(n)])


def next_event(stream, timeout: float = 15.0):
    """The next event, or a failure rather than a hang."""
    box: list = []

    def read():
        for event in stream:
            box.append(event)
            return

    reader = threading.Thread(target=read, daemon=True)
    reader.start()
    reader.join(timeout)
    if not box:
        raise TimeoutError("timed out waiting for a change event")
    return box[0]


def run(scenario: str, base: str, dead: str) -> dict:
    if scenario == "capabilities":
        db = connect(base)
        return {
            "protocol": db.version()["protocol"],
            "has_cursor_paging": db.has_capability("cursor-paging"),
            "has_invented_capability": db.has_capability("a-capability-nobody-has"),
        }

    if scenario == "documents_round_trip":
        db = connect(base)
        seed(db, 5)
        found = db.get("shop", "orders", 3)
        return {
            "qty": found["qty"],
            "missing_is_absent": db.get("shop", "orders", 999) is None,
            "count": db.count("shop", "orders"),
        }

    if scenario == "unlimited_find_is_a_page":
        db = connect(base)
        seed(db, 150)
        page = db.find("shop", "orders")
        return {
            "page": page["count"],
            "offers_cursor": "nextCursor" in page,
            "total": db.count("shop", "orders"),
        }

    if scenario == "paging_walks_everything":
        db = connect(base)
        seed(db, 250)
        ids = [d["_id"] for page in db.pages("shop", "orders", limit=50) for d in page]
        return {
            "documents_seen": len(ids),
            "first_id": ids[0] if ids else -1,
            "last_id": ids[-1] if ids else -1,
            "ordered": all(a < b for a, b in zip(ids, ids[1:])),
        }

    if scenario == "walk_ends_on_empty_page":
        db = connect(base)
        seed(db, 100)
        pages = list(db.pages("shop", "orders", limit=100))
        return {"pages": len(pages), "documents_seen": sum(len(p) for p in pages)}

    if scenario == "cursor_refuses_what_it_cannot_page":
        db = connect(base)
        seed(db, 10)
        try:
            list(db.pages("shop", "orders", sort={"qty": 1}))
            refused = False
        except ValueError:
            refused = True
        allowed = bool(list(db.pages("shop", "orders", sort={"_id": 1}, limit=5)))
        return {"sorted_walk_refused": refused, "id_sort_allowed": allowed}

    if scenario == "creating_a_collection_twice_is_a_conflict":
        db = connect(base)
        first = db.create_collection("shop", "orders")
        try:
            db.create_collection("shop", "orders")
            raise AssertionError("creating an existing collection must be a conflict")
        except KimmyError as e:
            return {
                "first_created": first["created"] == "orders",
                "second_code": e.code,
                "second_status": e.status,
            }

    if scenario == "duplicate_key_is_typed":
        db = connect(base)
        seed(db, 1)
        try:
            db.insert("shop", "orders", {"_id": 0})
            raise AssertionError("a duplicate _id must be refused")
        except KimmyError as e:
            return {"code": e.code, "retry": e.retry.value, "status": e.status}

    if scenario == "token_is_renewed":
        db = connect(base)
        first = db.token
        # The node this scenario runs against issues one-second tokens.
        time.sleep(1.2)
        succeeded = True
        try:
            db.request("GET", "/v1/databases")
        except Exception:
            succeeded = False
        return {"token_changed": db.token != first, "request_succeeded": succeeded}

    if scenario == "failover_past_a_dead_endpoint":
        # The dead address is first, so even logging in has to move on.
        db = Client(dead, endpoints=[base], user="root", password=PASSWORD)
        answered = True
        try:
            db.request("GET", "/v1/databases")
        except Exception:
            answered = False
        return {"answered": answered, "live_endpoint_first_after": db.endpoints[0] == base}

    if scenario == "write_is_not_retried_elsewhere":
        live = connect(base)
        seed(live, 1)
        db = Client(dead, endpoints=[base], token=live.token)
        try:
            db.insert("shop", "orders", {"_id": 99})
            raise AssertionError("an unsafe write must not move to another node")
        except TransportError as e:
            retry = e.retry.value
        idempotent = True
        try:
            db.request(
                "POST", "/v1/db/shop/coll/orders/docs", json={"_id": 99}, idempotent=True
            )
        except Exception:
            idempotent = False
        return {
            "write_failed": True,
            "retry_class": retry,
            "idempotent_retry_succeeded": idempotent,
        }

    if scenario == "change_stream_delivers":
        db = connect(base)
        seed(db, 1)
        stream = db.watch("shop", "orders", full_document=True)
        events = iter(stream)

        def write():
            for i in range(100, 103):
                db.insert("shop", "orders", {"_id": i})

        threading.Thread(target=write, daemon=True).start()

        seen = [next(events) for _ in range(3)]
        stream.close()
        return {
            "events": len(seen),
            "ids": [e.document_id for e in seen],
            "all_inserts": all(e.operation == "insert" for e in seen),
            "has_full_document": all(e.full_document is not None for e in seen),
        }

    if scenario == "change_stream_resumes":
        db = connect(base)
        seed(db, 1)
        first = db.watch("shop", "orders")
        db.insert("shop", "orders", {"_id": 200})
        token = next_event(first).resume_token
        first.close()

        # Written while nothing is listening.
        db.insert("shop", "orders", {"_id": 201})

        resumed = db.watch("shop", "orders", resume_after=token)
        missed = next_event(resumed)
        resumed.close()
        return {"resumed_id": missed.document_id}

    if scenario == "dropped_collection_ends_stream":
        db = connect(base)
        seed(db, 1)
        stream = db.watch("shop", "orders")
        db.request("DELETE", "/v1/db/shop/coll/orders")
        event = next_event(stream)
        stream.close()
        return {"operation": event.operation, "reason": event.raw.get("reason")}

    if scenario == "recreated_collection_serves_its_own_history":
        db = connect(base)
        seed(db, 1)
        db.request("DELETE", "/v1/db/shop/coll/orders")
        db.create_collection("shop", "orders")
        db.insert("shop", "orders", {"_id": 99})

        stream = db.watch("shop", "orders", from_start=True)
        event = next_event(stream)
        stream.close()
        return {"first_id": event.document_id}

    if scenario == "stale_resume_token_is_refused":
        db = connect(base)
        seed(db, 1)
        stream = db.watch("shop", "orders")
        db.insert("shop", "orders", {"_id": 5})
        token = next_event(stream).resume_token
        stream.close()

        db.request("DELETE", "/v1/db/shop/coll/orders")
        db.create_collection("shop", "orders")

        try:
            db.watch("shop", "orders", resume_after=token)
            return {"code": None}
        except KimmyError as e:
            return {"code": e.code}

    raise ValueError(f"unknown scenario {scenario!r}")


def main() -> int:
    if len(sys.argv) >= 2 and sys.argv[1] == "list":
        print(json.dumps(SCENARIOS))
        return 0
    if len(sys.argv) >= 4 and sys.argv[1] == "run":
        scenario, base = sys.argv[2], sys.argv[3]
        dead = sys.argv[4] if len(sys.argv) > 4 else "http://127.0.0.1:1"
        try:
            print(json.dumps(run(scenario, base, dead)))
            return 0
        except Exception as e:  # noqa: BLE001 - a driver reports, it does not judge
            print(json.dumps({"error": f"{type(e).__name__}: {e}"}))
            return 1
    print("usage: conformance_driver.py list | run <scenario> <base-url> [dead-url]",
          file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())
