#!/usr/bin/env python3
"""`shelf` — a small library catalogue, in Python.

One application written three times; see README.md for the other two and for
why the embedding is deliberately a toy.

    KIMMY_URL=http://localhost:7878 KIMMY_ROOT_PASSWORD=hunter2 \
        uv run --project clients/python python examples/shelf.py
"""

from __future__ import annotations

import math
import os
import pathlib
import sys
import threading
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "clients" / "python" / "src"))

from kimmydb import Client, KimmyError  # noqa: E402

#: Width of the toy embedding. Small on purpose: it is a hash, not a model.
DIM = 16

CATALOGUE = [
    (1, "The Long Way to a Small Angry Planet", 2014, "a crew tunnels wormholes between the stars"),
    (2, "A Memory Called Empire", 2019, "an ambassador arrives at a vast interstellar empire"),
    (3, "Ancillary Justice", 2013, "a starship intelligence in a single human body seeks revenge"),
    (4, "The Dispossessed", 1974, "a physicist travels between twin worlds divided by politics"),
    (5, "Piranesi", 2020, "a man lives alone in an infinite house of statues and tides"),
    (6, "The Left Hand of Darkness", 1969, "an envoy on a frozen world learns its people"),
    (7, "Station Eleven", 2014, "a travelling troupe performs after a collapse"),
    (8, "Klara and the Sun", 2021, "an artificial friend watches a family from a shop window"),
    (9, "Project Hail Mary", 2021, "a lone astronaut wakes on a ship between the stars"),
    (10, "The Fifth Season", 2015, "a continent breaks and a mother searches for her child"),
]


def embed(text: str) -> list[float]:
    """A deterministic bag-of-words hash, normalized.

    **Not an embedding.** It has no semantic understanding: two texts are near
    each other when they share words. It is here so the *pipeline* is real
    without needing an API key, and it is the same algorithm in all three
    languages so the three applications agree.
    """
    vector = [0.0] * DIM
    for raw in text.split():
        word = "".join(c for c in raw if c.isalnum()).lower()
        if not word:
            continue
        # FNV-1a, like the webhook ownership hash: stable across versions.
        h = 0xCBF29CE484222325
        for byte in word.encode():
            h = ((h ^ byte) * 0x100000001B3) & 0xFFFFFFFFFFFFFFFF
        vector[h % DIM] += 1.0
    length = math.sqrt(sum(v * v for v in vector))
    return [v / length for v in vector] if length else vector


def main() -> int:
    url = os.environ.get("KIMMY_URL", "http://localhost:7878")
    password = os.environ.get("KIMMY_ROOT_PASSWORD", "hunter2")

    # One address is all a client needs; the rest of the cluster comes from
    # /v1/topology, and the token is kept alive from here on.
    db = Client(url, user="root", password=password, discover_nodes=True)

    version = db.version()
    print(f"connected to {url} — protocol {version['protocol']}, build {version['version']}")

    # -- the shelf ---------------------------------------------------------
    try:
        db.create_collection("library", "books")
    except KimmyError as e:
        if e.code != "conflict":
            raise

    books = [
        {"_id": i, "title": title, "year": year, "blurb": blurb}
        for i, title, year, blurb in CATALOGUE
    ]

    # One commit for the whole catalogue: the commit is the cost, so batching is
    # worth roughly two orders of magnitude over inserting one at a time.
    #
    # A second run finds them already there. Branching on the *code* rather than
    # the status is the point of the error taxonomy — and a batch is all or
    # nothing, so one duplicate means none of them landed.
    try:
        db.insert_many("library", "books", books)
        print(f"shelved {len(books)} books in one commit")
    except KimmyError as e:
        if e.code != "duplicate_key":
            raise
        print("the shelf is already stocked; carrying on")

    # -- what is on it -----------------------------------------------------
    by_decade = db.aggregate(
        "library",
        "books",
        [
            {"$group": {"_id": {"$subtract": ["$year", {"$mod": ["$year", 10]}]},
                        "books": {"$sum": 1}}},
            {"$sort": {"_id": 1}},
        ],
    )
    decades = " ".join(f"{g['_id']}s={g['books']}" for g in by_decade["documents"])
    print(f"by decade: {decades}")

    # Paging, because a `find` with no limit is a page rather than the shelf.
    pages = list(db.pages("library", "books", limit=5))
    print(f"walked {sum(len(p) for p in pages)} books in {len(pages)} pages")

    # -- semantic search ---------------------------------------------------
    #
    # `byo` is the default provider: the client supplies the vectors, which is
    # what makes this run with no API key and no model.
    db.request(
        "POST",
        "/v1/db/library/coll/books/vector",
        json={"fields": ["blurb"], "provider": {"kind": "byo"}, "dim": DIM},
        idempotent=True,
    )
    for book in books:
        text = f"{book['title']} {book['blurb']}"
        db.request(
            "PUT",
            f"/v1/db/library/coll/books/docs/{book['_id']}/vectors",
            json=[{"chunk": 0, "vector": embed(text), "text": text}],
            idempotent=True,
        )

    query = "ships between the stars"
    hits = db.request(
        "POST",
        "/v1/db/library/coll/books/vector_search",
        json={"vector": embed(query), "k": 3},
        idempotent=True,
    )
    titles = {book["_id"]: book["title"] for book in books}
    print(f"\nnearest to {query!r}:")
    for hit in hits["matches"]:
        print(f"  {hit['score']:.3f}  {titles.get(hit['_id'], '?')}")

    # -- watching it change ------------------------------------------------
    stream = db.watch("library", "books", full_document=True)
    events = iter(stream)

    def arrive():
        time.sleep(0.2)
        # Replaced rather than inserted, so a second run still produces an
        # event rather than a duplicate key.
        db.request(
            "PUT",
            "/v1/db/library/coll/books/docs/999?upsert=true",
            json={"title": "A Late Arrival", "year": 2026,
                  "blurb": "arrived after the shelf was read"},
            idempotent=True,
        )

    threading.Thread(target=arrive, daemon=True).start()

    print("\nwatching for changes...")
    event = next(events)
    title = (event.full_document or {}).get("title", "(no post-image)")
    print(f"  {event.operation} {event.document_id} — {title}")
    stream.close()

    print("\ndone.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
