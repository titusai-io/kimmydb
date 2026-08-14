# kimmydb — Python client for KimmyDB

```bash
pip install kimmydb
```

```python
from kimmydb import Client

db = Client("http://localhost:7878", user="root", password="hunter2")
db.create_collection("shop", "orders")
db.insert("shop", "orders", {"sku": "widget", "qty": 5})

for document in db.documents("shop", "orders"):
    print(document)

for event in db.watch("shop", "orders", full_document=True):
    print(event.operation, event.document_id)
```

Synchronous. Both libraries underneath (`httpx`, `websockets`) have async APIs,
so an async client is a second class over the same request path rather than a
different design — but it is a decision for when someone wants it, not an
assumption made now.

See [docs/clients.md](../../docs/clients.md) for what every first-party client
is expected to do, and [docs/openapi.yaml](../../docs/openapi.yaml) for the
protocol itself.

## Development

`uv.lock` is committed for the same reason `Cargo.lock` is: CI resolves the
same versions every run, so a failure is about this repository rather than
about what PyPI published this morning.


```bash
cargo build                       # the tests drive a real kimmyd
uv run --extra dev pytest         # from clients/python
```
