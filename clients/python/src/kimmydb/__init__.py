"""Python client for KimmyDB.

::

    from kimmydb import Client

    db = Client("http://localhost:7878", user="root", password="hunter2")
    db.insert("shop", "orders", {"sku": "widget", "qty": 5})

    for document in db.documents("shop", "orders"):
        print(document)

This package talks to the protocol in ``docs/openapi.yaml`` and to nothing
else. It shares no code with the server and no code with the Rust client — the
three are independent readers of one specification, which is what makes a
disagreement between them mean something.

What it does, and each is a promise the server makes:

* keeps a token alive, refreshing before expiry using ``expiresIn`` rather than
  by decoding a token it is told to treat as opaque;
* fails over between nodes, discovered from ``/v1/topology``;
* pages with cursors, ending a walk on an empty page rather than a missing
  token;
* raises typed errors carrying the retry class, so an unfamiliar code is still
  actionable;
* resumes change streams from the last token seen, which is safe only because
  those tokens are portable between nodes.

And one thing it deliberately does not do: **retry a write**. ``elsewhere``
means *this node* did not answer, not that the work did not happen.
"""

from .client import Client
from .errors import (
    KimmyError,
    NoNodeAvailable,
    ProtocolError,
    Retry,
    TransportError,
)
from .pages import Pages
from .watch import ChangeEvent, ChangeStream

__all__ = [
    "ChangeEvent",
    "ChangeStream",
    "Client",
    "KimmyError",
    "NoNodeAvailable",
    "Pages",
    "ProtocolError",
    "Retry",
    "TransportError",
]

__version__ = "0.1.0"
