"""Change streams, and surviving the socket dropping.

A change stream is a WebSocket that stays open for as long as the application
wants events — which is longer than networks stay up. So the interesting part
of this module is not opening one; it is what happens after it closes.

**Resume tokens are portable across nodes**, verified on a real cluster, so a
reconnect may land somewhere else and continue correctly. That is what makes
automatic reconnection safe here where it would not be in a system whose
cursors belong to a session on one machine.
"""

from __future__ import annotations

import json as jsonlib
import time
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional
from urllib.parse import urlencode

from websockets.sync.client import connect as ws_connect

from .errors import KimmyError

if TYPE_CHECKING:  # pragma: no cover
    from .client import Client


class ChangeEvent:
    """One event from a collection."""

    def __init__(self, raw: Dict[str, Any]) -> None:
        self.raw = raw
        #: ``insert``, ``update``, ``replace``, ``delete``, ``uniqueViolation``
        #: or ``invalidate``.
        self.operation: str = raw.get("operationType", "")
        #: Where to resume. Absent on ``invalidate``, which cannot be resumed
        #: past.
        self.resume_token: Optional[str] = raw.get("resumeToken")

    @property
    def document_id(self) -> Any:
        """The changed document's ``_id``, when the event has one."""
        return (self.raw.get("documentKey") or {}).get("_id")

    @property
    def full_document(self) -> Optional[Dict[str, Any]]:
        """The post-image, when it was asked for and the event carried one.

        **An oversized event drops it and still arrives**, so absence does not
        mean the document is gone.
        """
        return self.raw.get("fullDocument")

    @property
    def is_invalidate(self) -> bool:
        """Whether the stream cannot continue past this event."""
        return self.operation == "invalidate"

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"<ChangeEvent {self.operation} _id={self.document_id!r}>"


class ChangeStream:
    """A live change stream that reconnects on its own.

    ::

        for event in db.watch("shop", "orders", full_document=True):
            print(event.operation, event.document_id)

    Iteration ends for exactly two reasons: an ``invalidate``, which is the
    collection going away, and a resume point that has fallen past the
    retention horizon — which cannot be waited out, because retrying the same
    token loops forever.
    """

    #: How many times to reconnect before giving up.
    ATTEMPTS = 5

    def __init__(
        self,
        client: "Client",
        db: str,
        collection: str,
        *,
        resume_after: Optional[str] = None,
        from_start: bool = False,
        full_document: bool = False,
    ) -> None:
        self._client = client
        self._db = db
        self._collection = collection
        self._configured_resume = resume_after
        self._from_start = from_start
        self._full_document = full_document
        self._resume: Optional[str] = None
        self._socket: Any = None
        self._ended = False
        # Connected here rather than on first iteration, and it matters more
        # than it looks. Python's natural shape is a lazy generator, which
        # would open the socket when the caller first reads — so anything
        # written between `watch()` and that first read would be missed, with
        # no error and nothing to see. Found by a test that wrote a document
        # immediately after opening a stream and then waited forever for it.
        self._reconnect()

    @property
    def resume_token(self) -> Optional[str]:
        """The token this stream would resume from.

        Worth storing if the application will restart: it is portable, so the
        next run may hand it to a different node.
        """
        return self._resume or self._configured_resume

    def close(self) -> None:
        if self._socket is not None:
            try:
                self._socket.close()
            finally:
                self._socket = None

    def __enter__(self) -> "ChangeStream":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def __iter__(self) -> Iterator[ChangeEvent]:
        while not self._ended:
            if self._socket is None:
                self._reconnect()
            try:
                message = self._socket.recv()
            except Exception:
                # A dropped socket, a timeout, a close frame — all three mean
                # the same thing: reconnect and resume.
                self.close()
                self._reconnect()
                continue

            if isinstance(message, bytes):  # pragma: no cover - server sends text
                message = message.decode()
            event = ChangeEvent(jsonlib.loads(message))
            # Recorded before the event is handed over, so a caller that stops
            # iterating mid-event resumes at the last one it actually saw.
            if event.resume_token:
                self._resume = event.resume_token
            if event.is_invalidate:
                self._ended = True
            yield event

    # -- connecting --------------------------------------------------------

    def _query(self) -> str:
        params: List[tuple] = []
        # A resume point learned while streaming wins over the configured one:
        # it is where this stream actually got to. Resuming from the configured
        # point would replay everything since, which for a stream that has been
        # up for a day is a day of events delivered twice.
        resume = self._resume or self._configured_resume
        if resume:
            params.append(("resume_after", resume))
        elif self._from_start:
            params.append(("from_start", "true"))
        if self._full_document:
            params.append(("full_document", "true"))
        return f"?{urlencode(params)}" if params else ""

    def _reconnect(self) -> None:
        delay = 0.1
        for attempt in range(self.ATTEMPTS):
            if attempt:
                time.sleep(delay)
                delay = min(delay * 2, 5.0)
            try:
                self._connect()
                return
            except KimmyError as e:
                # A resume point past the retention horizon cannot be waited
                # out, and the caller has to decide what to do about the gap.
                if e.code == "resume_token_expired":
                    self._ended = True
                    raise
                if attempt + 1 == self.ATTEMPTS:
                    raise
            except Exception:
                if attempt + 1 == self.ATTEMPTS:
                    raise

    def _connect(self) -> None:
        endpoint = self._client.endpoints[0]
        token = self._client.token
        url = (
            endpoint.replace("https://", "wss://", 1).replace("http://", "ws://", 1)
            + f"/v1/db/{self._db}/coll/{self._collection}/watch"
            + self._query()
        )
        headers = {"Authorization": f"Bearer {token}"} if token else {}
        try:
            self._socket = ws_connect(url, additional_headers=headers)
        except Exception as e:
            # The server refuses before upgrading with the ordinary error
            # envelope, so the reason survives if it can be recovered.
            status = getattr(getattr(e, "response", None), "status_code", None)
            if status is not None:
                body = getattr(e.response, "body", None)
                parsed = None
                if body:
                    try:
                        parsed = jsonlib.loads(body)
                    except ValueError:
                        parsed = None
                raise KimmyError.from_response(status, parsed) from e
            raise
