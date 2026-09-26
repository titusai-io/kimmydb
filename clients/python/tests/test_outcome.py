"""What the client makes of a failure, against a server that fails on purpose.

The one place this suite uses something other than a real node: a real node
cannot be made to close a connection at a chosen moment, or to answer a chosen
envelope, on demand. Each fake here does one thing the wire allows and nothing
the client could have told it to.
"""

from __future__ import annotations

import socket
import threading
import time
from typing import Callable, List

import httpx
import pytest

from kimmydb import Client, KimmyError, OutcomeUnknown, Retry, TransportError

DEAD = "http://127.0.0.1:1"  # reserved; nothing listens there


def fake_server(handle: Callable[[socket.socket], None]) -> str:
    """Serve each connection with ``handle``; return the base URL."""
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen()

    def serve() -> None:
        while True:
            try:
                conn, _ = listener.accept()
            except OSError:
                return
            threading.Thread(target=_serve_one, args=(conn, handle), daemon=True).start()

    threading.Thread(target=serve, daemon=True).start()
    return f"http://127.0.0.1:{listener.getsockname()[1]}"


def _serve_one(conn: socket.socket, handle: Callable[[socket.socket], None]) -> None:
    with conn:
        try:
            handle(conn)
        except OSError:
            pass


def read_request(conn: socket.socket) -> bool:
    """Read one whole request, its head and a Content-Length body. False once
    the client has closed the connection."""
    reader = conn.makefile("rb")
    length = 0
    while True:
        line = reader.readline()
        if not line:
            return False
        if line in (b"\r\n", b"\n"):
            break
        name, _, value = line.decode("latin-1").partition(":")
        if name.strip().lower() == "content-length":
            length = int(value.strip())
    reader.read(length)
    return True


def answer(conn: socket.socket, status: int, body: str, headers: str = "") -> None:
    conn.sendall(
        (
            f"HTTP/1.1 {status} X\r\nContent-Type: application/json\r\n"
            f"Content-Length: {len(body)}\r\n{headers}\r\n{body}"
        ).encode()
    )


def test_a_write_whose_connection_closes_after_it_was_sent_has_an_unknown_outcome():
    base = fake_server(read_request)
    with Client(base, token="a-token") as db:
        with pytest.raises(OutcomeUnknown) as caught:
            db.insert("shop", "orders", {"_id": 1})
    assert caught.value.endpoint == base
    assert caught.value.retry is Retry.VERIFY
    # Not a TransportError: code that resends on one must not resend this.
    assert not isinstance(caught.value, TransportError)


def test_a_read_whose_connection_closes_after_it_was_sent_is_a_transport_failure():
    base = fake_server(read_request)
    with Client(base, token="a-token") as db:
        with pytest.raises(TransportError):
            db.version()


def test_a_write_to_a_node_that_refuses_the_connection_was_not_sent():
    with Client(DEAD, token="a-token") as db:
        with pytest.raises(TransportError):
            db.insert("shop", "orders", {"_id": 1})


def test_a_write_whose_body_could_not_be_written_was_not_sent():
    # Larger than any socket buffer, so the write really does fail partway when
    # the server stops reading and closes.
    base = fake_server(lambda conn: conn.recv(1024))
    with Client(base, token="a-token") as db:
        with pytest.raises(TransportError):
            db.insert("shop", "orders", {"_id": 1, "big": "x" * (32 << 20)})


def test_a_write_whose_answer_is_cut_off_has_an_unknown_outcome():
    # The status arrived and the body did not: the node received the write.
    def handle(conn: socket.socket) -> None:
        if read_request(conn):
            conn.sendall(
                b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                b"Content-Length: 100\r\n\r\n{\"inser"
            )

    with Client(fake_server(handle), token="a-token") as db:
        with pytest.raises(OutcomeUnknown):
            db.insert("shop", "orders", {"_id": 1})


def test_the_servers_outcome_unknown_is_typed():
    def handle(conn: socket.socket) -> None:
        if read_request(conn):
            answer(conn, 500, '{"error":"outcome_unknown","message":"m","retry":"verify"}')

    with Client(fake_server(handle), token="a-token") as db:
        with pytest.raises(OutcomeUnknown) as caught:
            db.insert("shop", "orders", {"_id": 1})
    assert isinstance(caught.value.cause, KimmyError)
    assert caught.value.cause.code == "outcome_unknown"


def _through(transport: httpx.BaseTransport) -> Client:
    db = Client("http://127.0.0.1:9", token="a-token")
    db._http.close()
    db._http = httpx.Client(transport=transport)
    return db


def test_a_failure_with_no_evidence_either_way_is_unknown_for_a_write():
    # A transport that ignores the trace extension, so it reports no events at
    # all, then fails as a dropped connection does. Silence is not "not sent".
    def drop(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadError("the connection was reset", request=request)

    with _through(httpx.MockTransport(drop)) as db:
        with pytest.raises(OutcomeUnknown):
            db.insert("shop", "orders", {"_id": 1})


def test_a_failure_that_says_it_never_connected_was_not_sent():
    def refuse(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused", request=request)

    with _through(httpx.MockTransport(refuse)) as db:
        with pytest.raises(TransportError):
            db.insert("shop", "orders", {"_id": 1})


PURGING = '{"error":"collection_purging","message":"m","retry":"wait"}'


def test_a_wait_is_ridden_out_on_the_same_node_for_a_write():
    # A single endpoint, refused twice with `wait` and then served: the write
    # goes to the same node again, since `wait` says nothing was done.
    seen: List[int] = []

    def handle(conn: socket.socket) -> None:
        while read_request(conn):
            seen.append(1)
            if len(seen) <= 2:
                answer(conn, 503, PURGING, "Retry-After: 1\r\n")
            else:
                answer(conn, 200, '{"insertedId":1}')

    with Client(fake_server(handle), token="a-token") as db:
        started = time.monotonic()
        assert db.insert("shop", "orders", {"_id": 1}) == {"insertedId": 1}
        assert time.monotonic() - started >= 2, "each after the Retry-After it was given"
    assert len(seen) == 3


def test_a_wait_that_outlasts_the_budget_is_returned():
    seen: List[int] = []

    def handle(conn: socket.socket) -> None:
        while read_request(conn):
            seen.append(1)
            answer(conn, 503, PURGING, "Retry-After: 1\r\n")

    with Client(fake_server(handle), token="a-token", wait_budget=1.5) as db:
        with pytest.raises(KimmyError) as caught:
            db.create_collection("shop", "orders")
    assert caught.value.code == "collection_purging"
    assert len(seen) == 3, "one attempt, then two within a 1.5 s budget of 1 s waits"
