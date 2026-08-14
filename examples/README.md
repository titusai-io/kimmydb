# Examples

One application, written three times.

`shelf` is a small library catalogue that uses the features KimmyDB exists for
rather than the ones every database has: bulk insert in one commit, cursor
paging, an aggregation, **semantic search over client-supplied vectors**, and a
**change stream** watching the collection while it is written to.

| Language | Run it |
|---|---|
| Rust | `cargo run --example shelf -p kimmy-client` |
| Python | `cd clients/python && uv run --extra dev python ../../examples/shelf.py` |
| Go | `cd clients/go && go run ../../examples/shelf.go` |

All three take the node's address and root password from the environment:

```bash
export KIMMY_URL=http://localhost:7878
export KIMMY_ROOT_PASSWORD=hunter2
```

## Why it is one application rather than three snippets

The deferred decision that became M10 named *running KimmyDB on something real*
as the trigger for judging whether the client direction was right. A snippet
that inserts one document does not test that; an application that has to find
something, page through results and react to a change does.

Writing the same one three times is also the cheapest honest comparison of the
three clients. Where they differ, the difference is the language rather than
the protocol — and where they *cannot* express the same thing, that is worth
knowing before someone builds on it.

## The embedding is a toy, and says so

Semantic search needs vectors. The default provider is `byo` — the client
supplies them — so `shelf` computes its own with a deterministic bag-of-words
hash, in the same way in all three languages.

**It is not a real embedding.** It has no semantic understanding at all; two
documents are near each other when they share words. It is here because it
makes the *pipeline* real — configure vectors, store them per document, search
by nearest neighbour, get scored results — without the example needing an API
key, a network call, or a model download to run.

For a real one, configure a provider ([Vectors](../docs/vectors.md)) and let
the server embed. The application code above the search does not change.

## They are run in CI

An example nobody runs rots into a document that used to be true. These are
executed against a real node on every push, and a failure is a build failure.
