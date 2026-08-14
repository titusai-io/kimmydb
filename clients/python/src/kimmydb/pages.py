"""Reading a collection, rather than reading its first hundred documents."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Mapping

if TYPE_CHECKING:  # pragma: no cover - import cycle only matters to type checkers
    from .client import Client


class Pages:
    """A walk through a collection, one page at a time.

    ::

        for page in db.pages("shop", "orders", limit=500):
            print(len(page))

    **The walk ends on a short or empty page, not on a missing token.** A final
    page that is exactly full still carries one — the server cannot know it is
    the last without looking further — so a loop that stopped when the token
    stopped arriving would read one page too few. This handles that; a
    hand-rolled loop is where it gets forgotten.
    """

    def __init__(
        self,
        client: "Client",
        db: str,
        collection: str,
        query: Mapping[str, Any],
    ) -> None:
        self._client = client
        self._db = db
        self._collection = collection
        self._query = dict(query)
        self._check_pageable()

    def _check_pageable(self) -> None:
        """The server's rule, applied here so a walk fails before it starts.

        Otherwise the first page succeeds and the second is refused, which
        reads as a transient failure partway through a loop.
        """
        if self._query.get("skip"):
            raise ValueError(
                "`skip` and a cursor both say where to resume; use one"
            )
        sort = self._query.get("sort")
        if sort and sort != {"_id": 1}:
            raise ValueError(
                "a cursor pages in _id order, so it takes no other `sort`; "
                "sorting by another field still uses `skip`"
            )

    def __iter__(self) -> Iterator[List[Dict[str, Any]]]:
        cursor = None
        while True:
            body = dict(self._query)
            if cursor is not None:
                body["cursor"] = cursor
            response = self._client.request(
                "POST",
                f"/v1/db/{self._db}/coll/{self._collection}/find",
                json=body,
                idempotent=True,
            )
            documents = response.get("documents") or []
            if not documents:
                return
            yield documents
            cursor = response.get("nextCursor")
            if cursor is None:
                return
