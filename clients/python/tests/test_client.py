"""The Python client, against a real node.

Deliberately the same scenario list as the Rust client's tests. Two clients
that pass the same scenarios independently are evidence about the protocol; two
clients tested differently are two opinions.
"""

from __future__ import annotations

import threading
import time

import pytest

from kimmydb import Client, KimmyError, NoNodeAvailable, Pages, Retry, TransportError

from conftest import ROOT_PASSWORD

DEAD = "http://127.0.0.1:1"  # reserved; nothing listens there


def seed(db, n):
    db.create_collection("shop", "orders")
    db.insert_many(
        "shop", "orders", [{"_id": i, "sku": f"s{i % 3}", "qty": i} for i in range(n)]
    )


def test_a_client_built_with_credentials_holds_a_token(db):
    assert db.token is not None
    assert isinstance(db.request("GET", "/v1/databases")["databases"], list)


def test_documents_round_trip(db):
    seed(db, 5)

    assert db.get("shop", "orders", 3)["qty"] == 3
    # A missing document is None, not an exception: asking whether something
    # exists is an ordinary thing to do.
    assert db.get("shop", "orders", 999) is None
    assert db.count("shop", "orders") == 5


def test_paging_walks_the_whole_collection(db):
    # The reason the client exists rather than a `find` call: an unlimited
    # `find` returns 100 documents and says nothing about the rest.
    seed(db, 250)

    seen = [d["_id"] for page in db.pages("shop", "orders", limit=50) for d in page]
    assert seen == list(range(250))

    # And the one-document-at-a-time shape, which is what most callers want.
    assert sum(1 for _ in db.documents("shop", "orders")) == 250


def test_an_unlimited_find_is_a_page_not_the_collection(db):
    # Stated rather than hidden: the server's default is 100, and a client that
    # papered over it would only move the surprise.
    seed(db, 150)

    page = db.find("shop", "orders")
    assert page["count"] == 100
    assert "nextCursor" in page
    assert db.count("shop", "orders") == 150


def test_a_walk_ends_on_an_empty_page_not_a_missing_token(db):
    # A collection whose size is an exact multiple of the page size: the last
    # full page still carries a token, so a loop that stopped when the token
    # stopped arriving would read one page too few.
    seed(db, 100)

    pages = list(db.pages("shop", "orders", limit=100))
    assert len(pages) == 1
    assert len(pages[0]) == 100


def test_a_query_a_cursor_cannot_page_is_refused_before_the_walk(db):
    seed(db, 10)

    with pytest.raises(ValueError, match="_id order"):
        list(db.pages("shop", "orders", sort={"qty": 1}))

    # `pages()` takes no `skip` — the two contradict, so the convenience method
    # does not offer the combination at all. `Pages` is public, though, so the
    # guard is still reachable and still checked.
    with pytest.raises(ValueError, match="skip"):
        Pages(db, "shop", "orders", {"skip": 5})

    # `_id` ascending is the order a cursor already pages in.
    assert list(db.pages("shop", "orders", sort={"_id": 1}, limit=5))


def test_a_refusal_arrives_typed(db):
    seed(db, 1)

    with pytest.raises(KimmyError) as caught:
        db.insert("shop", "orders", {"_id": 0})

    error = caught.value
    assert error.code == "duplicate_key"
    assert error.retry is Retry.NO
    assert error.status == 409


def test_an_unknown_code_is_still_actionable():
    # How a code added after this client shipped stays additive: the code is
    # unrecognized and the class still says what to do.
    error = KimmyError.from_response(
        503, {"error": "shed_load", "message": "busy", "retry": "wait"}
    )
    assert error.code == "shed_load"
    assert error.retry is Retry.WAIT

    # And a server older than the retry class falls back to the status.
    assert KimmyError.from_response(500, {"error": "internal"}).retry is Retry.ELSEWHERE
    assert KimmyError.from_response(400, {"error": "bad_request"}).retry is Retry.NO


def test_a_client_with_a_bad_token_and_no_credentials_says_so(node):
    with Client(node.base, token="not-a-token") as db:
        with pytest.raises(KimmyError) as caught:
            db.request("GET", "/v1/databases")
        assert caught.value.is_unauthorized


def test_an_expired_token_is_replaced_without_the_caller_noticing(short_lived_node):
    # The point of holding credentials. A one-second lifetime makes the renewal
    # happen on the second request rather than in an hour.
    with Client(short_lived_node.base, user="root", password=ROOT_PASSWORD) as db:
        first = db.token
        time.sleep(1.2)
        assert isinstance(db.request("GET", "/v1/databases")["databases"], list)
        assert db.token != first


def test_an_unreachable_node_is_skipped_for_one_that_answers(node):
    # Failover, without a cluster: a dead address in front of a live one is the
    # same situation as a node that stopped. The client must survive its own
    # construction failing over — logging in is the first request it makes, and
    # a login that could not move on would make every other endpoint useless.
    with Client(
        DEAD, endpoints=[node.base], user="root", password=ROOT_PASSWORD
    ) as db:
        assert isinstance(db.request("GET", "/v1/databases")["databases"], list)
        # The node that answered is now first, so the next request does not
        # re-walk the dead one.
        assert db.endpoints[0] == node.base


def test_a_write_is_not_retried_elsewhere_automatically(node):
    # `elsewhere` says this node could not answer, not that the work did not
    # happen. A helpful retry of an insert would apply it twice, and no status
    # distinguishes that from one that never landed.
    with Client(node.base, user="root", password=ROOT_PASSWORD) as db:
        seed(db, 1)
        token = db.token

    with Client(DEAD, endpoints=[node.base], token=token) as db:
        with pytest.raises(TransportError) as caught:
            db.insert("shop", "orders", {"_id": 99})
        assert caught.value.retry is Retry.ELSEWHERE

        # The caller decides — and here it can, because the document carries an
        # `_id`, so a repeat is a fact rather than a guess.
        created = db.request(
            "POST",
            "/v1/db/shop/coll/orders/docs",
            json={"_id": 99},
            idempotent=True,
        )
        assert created["insertedId"] == 99


def test_every_endpoint_dead_is_reported_as_such():
    with pytest.raises((TransportError, NoNodeAvailable)):
        Client(DEAD, user="root", password="whatever")


def test_version_and_topology(node, db):
    version = db.version()
    assert version["protocol"] == "v1"
    assert db.has_capability("cursor-paging")
    assert not db.has_capability("a-capability-nobody-has")

    # A single node with no advertised endpoint still lists itself, so
    # discovery cannot leave a client with nowhere to go.
    assert db.topology()["count"] == 1
    assert db.refresh_topology() == [node.base]


def test_a_change_stream_delivers_and_carries_a_resume_token(db):
    seed(db, 1)

    stream = db.watch("shop", "orders", full_document=True)
    events = iter(stream)

    def write():
        time.sleep(0.3)
        for i in range(100, 103):
            db.insert("shop", "orders", {"_id": i, "sku": "live"})

    threading.Thread(target=write, daemon=True).start()

    seen = [next(events) for _ in range(3)]
    assert [e.operation for e in seen] == ["insert"] * 3
    assert [e.document_id for e in seen] == [100, 101, 102]
    assert all(e.full_document is not None for e in seen)
    assert stream.resume_token is not None
    stream.close()


def test_a_change_stream_resumes_from_where_it_stopped(db):
    # What makes reconnection safe: a token carries no server state, so a
    # second stream started from it sees what the first missed rather than
    # everything since the beginning.
    seed(db, 1)

    first = db.watch("shop", "orders")
    events = iter(first)
    db.insert("shop", "orders", {"_id": 200})
    token = next(events).resume_token
    first.close()

    # Written while nothing is listening.
    db.insert("shop", "orders", {"_id": 201})

    resumed = db.watch("shop", "orders", resume_after=token)
    missed = next(iter(resumed))
    assert missed.document_id == 201
    resumed.close()


def test_a_dropped_collection_ends_the_stream(db):
    """A drop invalidates the streams watching that collection.

    It did not, once: the stream went silent with no event, no close and no
    error, and — because ids are derived from `(database, name)` — a collection
    recreated under the same name would silently adopt it. Found while writing
    this file's first version, which asserted the MongoDB behaviour and then
    hung for ten minutes.
    """
    seed(db, 1)

    stream = db.watch("shop", "orders")
    events = iter(stream)
    db.request("DELETE", "/v1/db/shop/coll/orders")

    final = None
    for event in events:
        final = event
    assert final is not None
    assert final.is_invalidate
    assert final.raw["reason"] == "CollectionDropped"
    stream.close()
