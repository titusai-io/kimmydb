# kimmydb — Go client for KimmyDB

```bash
go get github.com/titusai-io/kimmydb/clients/go
```

```go
import "github.com/titusai-io/kimmydb/clients/go/kimmydb"

db, err := kimmydb.New(ctx, "http://localhost:7878",
    kimmydb.WithCredentials("root", "hunter2"),
    kimmydb.WithDiscovery(true),
)
defer db.Close()

_, err = db.Insert(ctx, "shop", "orders", map[string]any{"sku": "widget", "qty": 5})

for document, err := range db.Documents(ctx, "shop", "orders", kimmydb.Query{}) {
    if err != nil {
        return err
    }
    fmt.Println(document)
}

for event, err := range db.Watch(ctx, "shop", "orders", kimmydb.WatchOptions{FullDocument: true}) {
    if err != nil {
        return err
    }
    fmt.Println(event.Operation, event.DocumentID())
}
```

**One dependency**, and it is the WebSocket framing. Everything else is the
standard library: `net/http` pools connections, so the argument that ruled out
Python's stdlib does not apply here.

`coder/websocket` rather than `gorilla/websocket` because it handshakes through
an ordinary `*http.Client` — so a change stream inherits the same client, TLS
configuration and timeouts as every other request, instead of dialling its own
socket with a second configuration that can drift.

See [docs/clients.md](../../docs/clients.md) for what every first-party client
is expected to do, and [docs/openapi.yaml](../../docs/openapi.yaml) for the
protocol.

## Development

```bash
cargo build --release   # the tests drive a real kimmyd
go test ./...
```
