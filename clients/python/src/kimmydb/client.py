"""The client itself: authentication, failover, and the request path."""

from __future__ import annotations

import time
from typing import Any, Dict, Iterator, List, Mapping, Optional, Sequence

import httpx

from .errors import (
    KimmyError,
    NoNodeAvailable,
    ProtocolError,
    Retry,
    TransportError,
)
from .pages import Pages
from .watch import ChangeStream

#: Seconds before expiry at which the client renews its token.
#:
#: Not zero, because a token that expires between the check and the server
#: reading it fails for a reason the client could have avoided; and not
#: minutes, because it would spend most of a short lifetime refreshing.
RENEW_BEFORE = 60.0


class Client:
    """A connection to a KimmyDB cluster.

    ::

        from kimmydb import Client

        db = Client("http://localhost:7878", user="root", password="hunter2")
        db.insert("shop", "orders", {"sku": "widget", "qty": 5})

        for page in db.pages("shop", "orders", limit=500):
            for document in page:
                print(document)

    Synchronous, deliberately: an async client is a second class over the same
    request path rather than a different design, and both libraries underneath
    have async APIs waiting for the day it is wanted.
    """

    def __init__(
        self,
        endpoint: str,
        *,
        user: Optional[str] = None,
        password: Optional[str] = None,
        token: Optional[str] = None,
        endpoints: Sequence[str] = (),
        discover_nodes: bool = False,
        timeout: float = 30.0,
        verify: bool = True,
    ) -> None:
        self._endpoints: List[str] = [_normalize(endpoint)] + [
            _normalize(e) for e in endpoints
        ]
        self._credentials = (user, password) if user is not None else None
        self._token: Optional[str] = token
        # A supplied token has no stated lifetime, so nothing is renewed on a
        # schedule. If it expires the server says so, which is the honest
        # outcome for a credential this client did not obtain and cannot
        # obtain again.
        self._renew_at = float("inf") if token else 0.0
        self._http = httpx.Client(timeout=timeout, verify=verify)

        if self._credentials:
            self._authenticate()
        if discover_nodes:
            self.refresh_topology()

    # -- lifecycle ---------------------------------------------------------

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> "Client":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    @property
    def endpoints(self) -> List[str]:
        """The nodes this client will try, in the order it will try them."""
        return list(self._endpoints)

    @property
    def token(self) -> Optional[str]:
        return self._token

    # -- the node itself ---------------------------------------------------

    def version(self) -> Dict[str, Any]:
        """What this node is and what it can do.

        Ask before assuming a feature exists: in a cluster mid-upgrade the node
        answering the next request may be older than this one.
        """
        return self.request("GET", "/v1/version")

    def has_capability(self, capability: str) -> bool:
        return capability in self.version().get("capabilities", [])

    def topology(self) -> Dict[str, Any]:
        """The nodes this one knows about."""
        return self.request("GET", "/v1/topology")

    def refresh_topology(self) -> List[str]:
        """Re-read the cluster's node list and adopt it.

        Skips entries with no advertised endpoint — a node that has not been
        told what to advertise cannot be dialled — and entries that are not
        ``live``, since the point of the list is somewhere to go *now*. The
        current endpoint stays first either way.
        """
        body = self.topology()
        discovered = [
            _normalize(node["endpoint"])
            for node in body.get("nodes", [])
            if node.get("status") == "live" and node.get("endpoint")
        ]
        current = self._endpoints[0]
        discovered = [e for e in discovered if e != current]
        self._endpoints = [current] + discovered
        return self.endpoints

    # -- documents ---------------------------------------------------------

    def create_collection(self, db: str, collection: str) -> Dict[str, Any]:
        """Create a collection.

        Creating one that already exists raises :class:`KimmyError` with code
        ``conflict`` rather than succeeding a second time — so "make sure this
        exists" means catching that, not assuming the call is idempotent.

        Still marked idempotent for *retry* purposes: repeating it cannot
        create two collections, because the id is derived from the name.
        """
        return self.request(
            "POST",
            f"/v1/db/{db}/collections",
            json={"name": collection},
            idempotent=True,
        )

    def insert(self, db: str, collection: str, document: Mapping[str, Any]) -> Dict[str, Any]:
        """Insert one document.

        **Not retried automatically.** An insert whose answer was lost may have
        landed, and repeating it would insert a second document under a new
        ``_id``. Give the document an ``_id`` and pass ``idempotent=True`` to
        :meth:`request` if you want that: a repeat then fails with
        ``duplicate_key``, which is a fact rather than a guess.
        """
        return self.request("POST", f"/v1/db/{db}/coll/{collection}/docs", json=document)

    def insert_many(
        self, db: str, collection: str, documents: Sequence[Mapping[str, Any]]
    ) -> Dict[str, Any]:
        """Insert many documents in one commit — all of them, or none."""
        return self.request(
            "POST", f"/v1/db/{db}/coll/{collection}/bulk", json=list(documents)
        )

    def get(self, db: str, collection: str, id: Any) -> Optional[Dict[str, Any]]:
        """One document by ``_id``, or ``None`` when there is none.

        A missing document is not an error: asking whether something exists is
        an ordinary thing to do, and raising would make callers catch an
        exception to find out.
        """
        try:
            return self.request("GET", f"/v1/db/{db}/coll/{collection}/docs/{id}")
        except KimmyError as e:
            if e.is_not_found:
                return None
            raise

    def find(
        self,
        db: str,
        collection: str,
        filter: Optional[Mapping[str, Any]] = None,
        *,
        sort: Optional[Mapping[str, Any]] = None,
        projection: Optional[Mapping[str, Any]] = None,
        limit: Optional[int] = None,
        skip: Optional[int] = None,
        cursor: Optional[str] = None,
        explain: bool = False,
    ) -> Dict[str, Any]:
        """One page of a query.

        **Omitting ``limit`` returns 100 documents, not all of them.** That is
        the server's behaviour; hiding it here would only move the surprise.
        Use :meth:`pages` to read a whole collection.
        """
        body = _query_body(filter, sort, projection, limit, skip, explain)
        if cursor is not None:
            body["cursor"] = cursor
        return self.request(
            "POST", f"/v1/db/{db}/coll/{collection}/find", json=body, idempotent=True
        )

    def pages(
        self,
        db: str,
        collection: str,
        filter: Optional[Mapping[str, Any]] = None,
        *,
        sort: Optional[Mapping[str, Any]] = None,
        projection: Optional[Mapping[str, Any]] = None,
        limit: Optional[int] = None,
    ) -> Pages:
        """Walk a collection, one page at a time.

        Iterating yields lists of documents::

            for page in db.pages("shop", "orders", limit=500):
                ...
        """
        return Pages(
            self,
            db,
            collection,
            _query_body(filter, sort, projection, limit, None, False),
        )

    def documents(
        self,
        db: str,
        collection: str,
        filter: Optional[Mapping[str, Any]] = None,
        **kwargs: Any,
    ) -> Iterator[Dict[str, Any]]:
        """Every matching document, one at a time, paging underneath.

        The shape most callers want, and the one that makes forgetting to page
        impossible::

            for document in db.documents("shop", "orders"):
                ...
        """
        for page in self.pages(db, collection, filter, **kwargs):
            yield from page

    def count(
        self, db: str, collection: str, filter: Optional[Mapping[str, Any]] = None
    ) -> int:
        """How many documents match. No page cap — a count sees everything."""
        body = self.request(
            "POST",
            f"/v1/db/{db}/coll/{collection}/count",
            json={"filter": filter or {}},
            idempotent=True,
        )
        count = body.get("count")
        if not isinstance(count, int):
            raise ProtocolError("count did not return a number")
        return count

    def update(
        self,
        db: str,
        collection: str,
        filter: Mapping[str, Any],
        update: Mapping[str, Any],
        *,
        multi: bool = False,
    ) -> Dict[str, Any]:
        return self.request(
            "POST",
            f"/v1/db/{db}/coll/{collection}/update",
            json={"filter": filter, "update": update, "multi": multi},
        )

    def delete(
        self,
        db: str,
        collection: str,
        filter: Mapping[str, Any],
        *,
        multi: bool = False,
    ) -> Dict[str, Any]:
        return self.request(
            "POST",
            f"/v1/db/{db}/coll/{collection}/delete",
            json={"filter": filter, "multi": multi},
        )

    def aggregate(
        self, db: str, collection: str, pipeline: Sequence[Mapping[str, Any]]
    ) -> Dict[str, Any]:
        return self.request(
            "POST",
            f"/v1/db/{db}/coll/{collection}/aggregate",
            json={"pipeline": list(pipeline)},
            idempotent=True,
        )

    # -- change streams ----------------------------------------------------

    def watch(
        self,
        db: str,
        collection: str,
        *,
        resume_after: Optional[str] = None,
        from_start: bool = False,
        full_document: bool = False,
    ) -> ChangeStream:
        """Follow a collection's changes.

        Iterating yields events until the stream ends::

            for event in db.watch("shop", "orders", full_document=True):
                print(event.operation, event.document_id)

        Reconnects on its own, resuming from the last token it saw.
        """
        return ChangeStream(
            self,
            db,
            collection,
            resume_after=resume_after,
            from_start=from_start,
            full_document=full_document,
        )

    # -- the escape hatch --------------------------------------------------

    def request(
        self,
        method: str,
        path: str,
        *,
        json: Any = None,
        idempotent: bool = False,
    ) -> Any:
        """Any route, by path.

        Present because a client that covers a subset of an API and cannot
        reach the rest sends people back to ``curl`` for one call. Everything
        above is a convenience over this.

        ``idempotent`` is the caller's claim that repeating the request cannot
        change the outcome. Reads set it; writes do not, because
        ``retry: elsewhere`` says *this node* did not answer, not that the work
        did not happen — and no status distinguishes an insert that failed
        before its commit from one that failed after it.
        """
        self._authenticate()
        # GET is idempotent by definition, so a caller never has to say so.
        idempotent = idempotent or method == "GET"

        tried: List[str] = []
        last: Optional[BaseException] = None
        relogged = False

        for endpoint in list(self._endpoints):
            tried.append(endpoint)
            while True:
                try:
                    body = self._send(endpoint, method, path, json)
                    self._promote(endpoint)
                    return body
                except (KimmyError, TransportError) as e:
                    error = e

                # A token the server has stopped accepting: log in again once,
                # in case it merely expired. Once, because a loop here is how a
                # client hammers a login endpoint forever.
                if (
                    isinstance(error, KimmyError)
                    and error.is_unauthorized
                    and not relogged
                    and self._credentials
                ):
                    relogged = True
                    try:
                        self._login()
                        continue
                    except Exception:
                        raise error from None

                retry = error.retry
                if retry is Retry.WAIT and idempotent:
                    delay = min(getattr(error, "retry_after", None) or 1, 30)
                    time.sleep(delay)
                    last = error
                    break
                if retry is Retry.ELSEWHERE and idempotent:
                    last = error
                    break
                # Either nothing to retry, or a write — which is the caller's
                # to decide about. The server's own error is more useful than
                # one invented here.
                raise error

        if last is not None:
            raise last
        raise NoNodeAvailable(tried)

    def download(self, path: str) -> bytes:
        """Raw bytes, for the routes that are not JSON — the backup."""
        self._authenticate()
        endpoint = self._endpoints[0]
        try:
            response = self._http.get(f"{endpoint}{path}", headers=self._headers())
        except httpx.HTTPError as e:
            raise TransportError(endpoint, e) from e
        if response.status_code >= 400:
            raise KimmyError.from_response(response.status_code, _body(response))
        return response.content

    # -- authentication ----------------------------------------------------

    def _authenticate(self) -> None:
        """Ensure there is a usable token, logging in or refreshing as needed.

        Refresh is preferred over a fresh login: it costs no password
        verification — the login limiter exists to bound that work — and an
        application that stored credentials should be able to forget them for
        as long as it stays connected.
        """
        if time.monotonic() < self._renew_at:
            return
        if self._token is not None:
            try:
                self._refresh()
                return
            except Exception:
                # Not fatal: the token may still be good, and if it is not the
                # next request says so with the server's own reason.
                pass
        if self._credentials:
            self._login()

    def _login(self) -> None:
        user, password = self._credentials  # type: ignore[misc]
        body = self._send_any(
            "POST", "/v1/auth/login", {"user": user, "password": password}, token=None
        )
        self._adopt(body)

    def _refresh(self) -> None:
        body = self._send_any("POST", "/v1/auth/refresh", None, token=self._token)
        self._adopt(body)

    def _adopt(self, body: Any) -> None:
        token = body.get("token") if isinstance(body, Mapping) else None
        if not token:
            raise ProtocolError("no token in the response")
        self._token = token
        # `expiresIn` rather than decoding the token: it is opaque, and a
        # client that parses one depends on a shape nothing promised it.
        lifetime = float(body.get("expiresIn", 3600))
        self._renew_at = time.monotonic() + max(lifetime - RENEW_BEFORE, 1.0)

    def _send_any(
        self, method: str, path: str, json: Any, *, token: Optional[str]
    ) -> Any:
        """A request that must reach *some* node, tried against each in turn.

        Login and refresh use this, and it has to fail over: a client handed a
        list whose first address is dead could otherwise not authenticate at
        all, which is the one failure that makes every other endpoint useless.
        The Rust client shipped without this and a test caught it; it is here
        from the start for that reason.

        Only transport failures move on. A *refusal* is the same everywhere:
        one cluster, one signing secret, one user store.
        """
        tried: List[str] = []
        last: Optional[BaseException] = None
        for endpoint in list(self._endpoints):
            tried.append(endpoint)
            try:
                body = self._send(endpoint, method, path, json, token=token)
                self._promote(endpoint)
                return body
            except TransportError as e:
                last = e
        if last is not None:
            raise last
        raise NoNodeAvailable(tried)

    # -- the wire ----------------------------------------------------------

    def _headers(self, token: Optional[str] = ...) -> Dict[str, str]:  # type: ignore[assignment]
        chosen = self._token if token is ... else token
        return {"Authorization": f"Bearer {chosen}"} if chosen else {}

    def _send(
        self,
        endpoint: str,
        method: str,
        path: str,
        json: Any = None,
        *,
        token: Optional[str] = ...,  # type: ignore[assignment]
    ) -> Any:
        try:
            response = self._http.request(
                method, f"{endpoint}{path}", json=json, headers=self._headers(token)
            )
        except httpx.HTTPError as e:
            raise TransportError(endpoint, e) from e

        if response.status_code >= 400:
            retry_after = response.headers.get("retry-after")
            raise KimmyError.from_response(
                response.status_code,
                _body(response),
                int(retry_after) if retry_after and retry_after.isdigit() else None,
            )
        if not response.content:
            return None
        try:
            return response.json()
        except ValueError as e:
            raise ProtocolError(f"the body from {endpoint} is not JSON: {e}") from e

    def _promote(self, endpoint: str) -> None:
        """Move an endpoint to the front, so the next request starts where the
        last one succeeded rather than re-walking the dead ones."""
        if self._endpoints and self._endpoints[0] == endpoint:
            return
        self._endpoints = [endpoint] + [e for e in self._endpoints if e != endpoint]


def _body(response: httpx.Response) -> Any:
    try:
        return response.json()
    except ValueError:
        return None


def _query_body(
    filter: Optional[Mapping[str, Any]],
    sort: Optional[Mapping[str, Any]],
    projection: Optional[Mapping[str, Any]],
    limit: Optional[int],
    skip: Optional[int],
    explain: bool,
) -> Dict[str, Any]:
    """Only what was given: an unset field is absent, not null."""
    body: Dict[str, Any] = {"explain": explain}
    for key, value in (
        ("filter", filter),
        ("sort", sort),
        ("projection", projection),
        ("limit", limit),
        ("skip", skip),
    ):
        if value is not None:
            body[key] = value
    return body


def _normalize(endpoint: str) -> str:
    """Trim a trailing slash, so joining a path never doubles one."""
    return endpoint.rstrip("/")
