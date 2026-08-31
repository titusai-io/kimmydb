# Vectors and Auto-Embeddings

[← Documentation index](README.md)

Every collection can have embeddings maintained for it automatically. Enable it
once, and inserts, updates and deletes keep the vectors in step with no
pipeline, no scheduler, and nothing on the write path.

This document explains how that works, what it costs, and where it is
deliberately approximate.

---

## The idea

A vector-enabled collection gets a **shadow collection** — an ordinary
collection named `{coll}.__vectors`, stored the same way as any other, holding
one document per *chunk* of one source document.

Nothing special maintains it. A background worker subscribes to the oplog as an
ordinary change-stream consumer and reacts to what it sees:

```mermaid
graph LR
    W["POST /docs"] --> T["One redb transaction"]
    T --> D[("docs")]
    T --> O[("oplog")]
    T -->|"200 OK"| C["Client"]

    O -.->|"subscribes"| EW["Embedding worker"]
    EW -->|"embed"| P["Provider"]
    P --> EW
    EW --> V[("docs.__vectors")]

    style O fill:#4a5568,color:#fff
    style C fill:#2f855a,color:#fff
```

Two properties fall out of that shape, and both matter:

**Writes never wait on a model.** The request returns as soon as the oplog entry
is durable. Embedding is *observed* work, not *inline* work. A slow or
unavailable provider delays search freshness; it cannot delay or fail a write.

**Backfill is not a special case.** The worker starts from its recorded oplog
position, or from the beginning of the log on first run. Enabling embedding on a
collection that already holds a million documents is the same code path as
embedding the next insert — the worker is simply behind.

This is the same reuse described in [Architecture](architecture.md): the oplog
was built for change streams, and the embedding pipeline is its second consumer.

---

## Enabling it

```http
POST /v1/db/{db}/coll/{coll}/vector
```

```json
{
  "fields": ["title", "body"],
  "provider": { "kind": "ollama",
                "model": "nomic-embed-text",
                "endpoint": "http://localhost:11434" },
  "dim": 768,
  "metric": "cosine",
  "chunk": { "max_chars": 2000, "overlap": 200 }
}
```

| Field | Meaning |
|---|---|
| `fields` | Which document paths to embed. Joined with a blank line, so a chunk boundary between two fields cannot glue unrelated sentences together |
| `provider` | Where vectors come from. A tagged object — `{"kind": "..."}` plus whatever that provider needs — because the remote ones carry an endpoint and a model name alongside the choice |
| `dim` | Vector width. Pinned here; changing it requires reconfiguring |
| `metric` | `cosine`, `euclidean`, or `dot` |
| `chunk` | How long documents are split |
| `document_prefix` | Text put in front of every chunk before the provider sees it — the task marker many models are trained on (`passage: ` for E5 and BGE, `title: none \| text: ` for EmbeddingGemma, an instruction line for Nemotron). Never stored, never returned in a hit; changing it reindexes |
| `query_prefix` | The same for `query` text on a search (`query: `, `task: search result \| query: `). Not applied to a caller-supplied `vector` |

`GET` returns the current configuration; `DELETE` disables embedding, with
`?drop_vectors=true` to discard the stored vectors as well as the config.

### Providers

| Provider | Needs | Notes |
|---|---|---|
| `byo` | nothing | **The default.** The client supplies vectors through [the ingest route](#supplying-your-own-vectors); the server never embeds |
| `open_ai` | an API key | Any OpenAI-compatible `/v1/embeddings` endpoint. **Voyage is this** — `{"kind":"open_ai","model":"voyage-3","endpoint":"https://api.voyageai.com","api_key_env":"VOYAGE_API_KEY"}` |

| `ollama` | a reachable Ollama | Local or remote |
| `cohere` | an API key | Cohere `/v2/embed`. Sends `input_type: search_document`; accepts both v1 and v2 response shapes ([ADR-047](decisions.md)) |
| `gemini` | an API key | Google `:batchEmbedContents`. Key goes in the `x-goog-api-key` header ([ADR-047](decisions.md)) |
| `custom_http` | an endpoint | Accepts `{"input": [...]}`, returns `{"embeddings": [[...]]}`. The escape hatch for anything the named dialects miss |
| `local` | `--features local-embeddings` | In-process ONNX. **Not in the default build** — see below |

For `open_ai`, `endpoint` is a base URL and `/v1/embeddings` is appended —
unless the setting already names the embeddings route, in which case it is
used verbatim. That is how providers that mount the compatible API under a
prefix are reached: `"endpoint": "https://api.deepinfra.com/v1/openai/embeddings"`
or Azure's `…/openai/deployments/<name>/embeddings?api-version=…`.

`open_ai` also takes an optional `dimensions`, sent as the request field of
that name for Matryoshka-trained models (OpenAI `text-embedding-3-*`,
Nemotron-3-Embed, Qwen3-Embedding, EmbeddingGemma, Voyage): the provider
returns that width instead of the model's native one, and `dim` must equal
it. Left out, the model's native width is what comes back and `dim` must
match that. A provider that ignores the field fails the dimension check on
the first embed rather than storing the wrong shape.

Default key variables: `OPENAI_API_KEY`, `COHERE_API_KEY`, `GEMINI_API_KEY`;
override with `api_key_env`. The dialects were audited against each provider's
documented API shape and pinned with fixture tests ([ADR-047](decisions.md)),
which is where a shape drift shows up.

> **Cohere embeds asymmetrically.** The server always uses
> `input_type: search_document`, because it only ever embeds documents. If you
> embed a *query* to search with — client-side, since queries arrive here as
> raw vectors — use `search_query`, or recall suffers.

API keys are read from the environment by *variable name*. The name is stored in
collection metadata; the key itself never is, so a metadata dump cannot leak a
credential.

`local` is rejected at configuration time in a default build rather than
failing later on the first write — a misconfiguration should surface when you
make it, not when traffic arrives. The reason it is not the default at all is
recorded in [Deviations](deviations.md): its dependencies pull native ONNX
Runtime *and* OpenSSL — hundreds of megabytes and a separate runtime. Note the
default build is **not** free of a native toolchain and has not been since M2;
that claim was corrected in [ADR-016](decisions.md). ONNX is still gated because
it is a different order of cost, not because the build is otherwise pristine.

### Chunking

Long documents are split into overlapping windows, each embedded separately, so
a match can point at the passage that matched rather than the whole document.

Chunks overlap by `overlap` characters so a sentence spanning a boundary is
still wholly present in one of them.

The default is 2000 characters ≈ 512 tokens. **It counts characters, not
tokens** — a real token count depends on the model's tokenizer, which the
storage layer has no business knowing — and that proxy assumes prose at about
four characters per token. Dense text runs at one to two: code, JSON, CJK.
Seen live: a chunk cut at 2000 characters came to 1073 tokens and was refused
by a provider with a 1024-token input limit, on every scan, with one `WARN`
per scan as the only trace.

`max_tokens` closes that gap. When set, a chunk is also cut once its
*estimated* token count reaches it, estimated as one token per two bytes of
UTF-8 — conservative for every common script (prose ≈ 4 bytes/token, code ≈
2.5, CJK ≈ 3), so the estimate errs toward shorter chunks. Set it to the
provider's per-input limit:

```json
{ "chunk": { "max_chars": 2000, "overlap": 200, "max_tokens": 1024 } }
```

Overlap stays in characters. Existing configurations without the field keep
the character rule alone, exactly as before. A document that still cannot be
embedded is skipped and named — database, collection and `_id` are on the
`WARN`, and `kimmy_embed_failures_total` counts it (with
`kimmy_embed_provider_errors_total{kind}` naming what failed to connect,
time out or reset) — rather than stalling the
rest of the collection.

---

## Supplying your own vectors

With `byo`, nothing populates the shadow collection unless you do. Embed
however you like, then store the result against the document it came from:

```bash
curl -XPUT localhost:7878/v1/db/shop/coll/orders/docs/42/vectors -H "$A" -d '[
  { "chunk": 0, "vector": [0.01, -0.22, …], "text": "first chunk of the document" },
  { "chunk": 1, "vector": [0.44,  0.10, …], "text": "second chunk" }
]'
```

```json
{ "stored": 2, "_id": "42" }
```

**The body is the complete set of chunks for that document.** Anything
previously stored under it and not named here is removed — the same replace-all
semantics the embedding worker uses, and the only ones that stop a shortened
document from leaving orphan chunks that still match text it no longer contains.

The server fills in the rest of each record: which document it belongs to, and
the document's current HLC. That second part is why staleness detection keeps
working for `byo` exactly as it does for a server-side provider — the version a
chunk was derived from is the document's own, not something a client could get
wrong.

| | |
|---|---|
| `GET` | Read back what is stored for one document |
| `PUT` | Replace the whole set |
| `DELETE` | Remove every chunk for that document |

Requires `write` on the collection — this is derived data about a document you
can already write, not an administrative act. The document must exist (there is
no HLC to attach otherwise, so a missing one is `404`), and every vector must
match the configured `dim` or the request is `400`.

---

## Staleness, and why re-embedding is idempotent

Each `VectorRecord` carries the **HLC of the source document** it was made from:

```rust
struct VectorRecord {
    source: DocId,     // the source document
    chunk: u32,        // which chunk
    source_hlc: Hlc,   // the version this was embedded from
    vector: Vec<f32>,
    text: String,
}
```

That one field does the work of a queue:

- **Staleness is a comparison, not a state machine.** If a document's current
  HLC exceeds its vectors' `source_hlc`, the vectors are stale. Nothing tracks
  pending work; the answer is derivable at any moment from what is stored.
- **Re-processing is free.** Replay the same oplog entry twice and the second
  pass sees vectors already at that HLC and does nothing. That is what makes the
  worker safe to restart, and safe to run behind an at-least-once log.

The worker records its oplog position **after** doing the work, never before.
Crashing mid-embed replays the entry; crashing after writing vectors but before
recording the position also replays it — and the HLC check makes that a no-op.
Recording the position first would silently skip documents instead.

A provider failure that could plausibly succeed on retry — a transport error, a
rate limit — retries the same entry after a delay rather than advancing past it.
A failure that will fail identically forever — a wrong dimension, a missing API
key — does not, because retrying it would stall every document queued behind it.

---

## Search

```http
POST /v1/db/{db}/coll/{coll}/vector_search
POST /v1/db/{db}/coll/{coll}/hybrid_search
```

```json
{
  "query": "how do I rotate a token",
  "k": 10,
  "filter": { "status": "published" },
  "per_document": 1
}
```

Send `query` text to have the server embed it, or `vector` to supply a
pre-computed one. A `byo` collection must send `vector` — the server has no
provider to embed the query with, and says so rather than returning an empty
result that looks like "no matches".

A collection with **no vectors stored at all** is refused with `409 no_vectors`
rather than returning an empty result. Those two are indistinguishable to a
caller, and the difference is between refining a query forever and learning that
ingestion never happened.

`filter` is an ordinary query-language document. It runs first, and its matching
ids restrict the search — which is what lets semantic search compose with
structured querying instead of being a separate world.

It is planned the way a `find` is: a filter that pins `_id` is a primary-key
read, a filter on an indexed field uses the index, anything else scans the
collection — every candidate rechecked against the full filter either way —
and only the ids are kept, a page at a time. An index on a field that searches
filter by speeds the search up exactly as it speeds up `find`, and `find` with
`explain: true` on the same filter shows which strategy the search will get
([Indexes](indexes.md#did-it-get-used)).

What happens next depends on how many documents the filter admitted:

- **At most 1,000**: the search reads those documents' chunks from the shadow
  collection by key — a document's chunks are one contiguous run under its id —
  and scores them exactly. The cost is the size of the admitted set, not the
  collection, and the answer is exact whichever path the collection would
  otherwise take.
- **More than 1,000**: the search runs as it would without a filter, exact
  scan or graph walk, and discards hits outside the set. The graph is asked for
  eight times the candidates when a filter is present so that discarding still
  leaves `k`; an unselective filter discards little, which is what makes this
  the right direction for it.

The boundary is a count rather than a fraction of the collection because the
first direction's cost does not depend on the collection: a thousand
documents' chunks read by key is the same work over a million documents as
over two thousand. Below it the join is bounded and exact; above it the set is
large enough that a graph walk finds mostly admitted candidates. A filter that
admits nothing returns nothing without touching a vector. The rule and its
alternatives are [ADR-102](decisions.md).

`per_document` caps how many chunks of one document may occupy result slots.
Without it, a single long document can fill every slot with its own chunks.

### Hybrid search

`hybrid_search` runs a dense (vector) and a lexical (keyword) search, then fuses
them with **Reciprocal Rank Fusion**:

```
score(d) = Σ  1 / (60 + rank_i(d))
```

RRF ranks by *position*, not raw score, which is what makes it work across two
signals whose scores are not comparable — a cosine similarity and a term-overlap
count have no shared scale.

Each half is retrieved 4× wider than `k` before fusing, so a document ranked
moderately by both can beat one ranked first by only one.

The lexical half is **term overlap, not BM25**. Since RRF only consumes the
ordering, the absolute scores need not be principled — but a real BM25 would
rank better on its own. Recorded in [Deviations](deviations.md).

### There is no minimum score

k-NN returns the `k` nearest vectors, and "nearest" does not mean "similar". A
query against wholly unrelated content still returns `k` results, with scores
near zero. **Callers must threshold themselves.**

---

## The two search paths

Search is either an exhaustive scan or an approximate graph walk. Callers do not
choose, and cannot tell which ran except by speed.

```mermaid
graph TD
    Q["vector_search"] --> A{"IndexCache::access"}
    A -->|"metric is dot"| E["Exact scan<br/>score every vector"]
    A -->|"< 500 vectors"| E
    A -->|"build failed"| E
    A -->|"otherwise"| H["HNSW walk<br/>candidates only"]
    H --> S["Rescore from storage"]
    E --> R["Top k"]
    S --> R

    style E fill:#4a5568,color:#fff
    style H fill:#2b6cb0,color:#fff
```

The exact path scores every stored vector — O(n), no recall loss, no index to
keep consistent. It is both the path small collections take and the **oracle**
the approximate path is tested against. It holds only the best `k` while it
scans: a bounded set with the per-document cap applied as chunks arrive, so
its memory is the size of the answer rather than of the collection. The
lexical half of hybrid search is ranked the same way. Ties on score are broken
by the chunk's key, so equal scores come back in a stable order.

The approximate path walks an HNSW graph. Crucially, the graph only supplies
*candidates*: every candidate is then re-scored from the vector currently in
storage, and a candidate whose record no longer exists is skipped.

### When each is used

`kimmy_vector::IndexCache` owns that decision — one place, not scattered through
the search path. It is held in the server's shared state, so one graph serves
every request rather than being rebuilt per query.

| Rule | Threshold | Why |
|---|---|---|
| Minimum size | 500 vectors | **Measured** — see [Benchmarks](benchmarks.md). The graph is faster at every size tested, so this is not a crossover; it is the point where one build repays itself in about a dozen queries |
| Staleness detection | a per-collection generation counter | Counting vectors would be O(n) per query, and a count cannot see a delete-then-add that leaves the total unchanged. A counter is exact and free |
| Rebuild interval | 30 s | Rebuilding per write would rebuild continuously under load, and each rebuild is O(n log n) |
| Build failure | fall back to exact | An optimisation that cannot be built must not fail a query |

The "too small" verdict is cached under the same rule as the graph. The count
behind it is O(n), so recomputing it per query would make the check that exists
to *avoid* a full scan perform one.

### Why a stale vector index is safe when a stale secondary index is not

A stale secondary index returns **wrong documents** — it is the source of truth
for what matched. A stale vector index does not, because of the two properties
above:

- **Scores are recomputed** from the current stored vector, never taken from the
  graph's distances. An updated document scores by its new vector.
- **Missing records are skipped.** A chunk whose record is gone is dropped from
  the candidates, even though its node is still in the graph.
- **A deleted document cannot surface.** The chunks themselves outlive the
  document: a delete commits, and the embedding worker removes the chunks when
  it reaches that entry in the stream — later, or much later if the worker is
  behind. So every hit is checked against the source collection after ranking,
  and one whose document no longer exists is dropped. A result can therefore be
  shorter than `k` by the number of deletions the worker has not caught up with
  ([ADR-091](decisions.md)).

So the only effect of staleness is that a document written in the last 30
seconds may not be found yet. That is **bounded recall loss on new data, never
incorrect data** — which is what makes a rebuild interval an acceptable trade
rather than a silent correctness hole.

This asymmetry is the whole justification for the caching policy. If it did not
hold, the index would have to be maintained transactionally like a secondary
index, on the write path.

### The `dot` metric has no index

`anndists::DistDot` computes `1 - dot` and *asserts the result is
non-negative*, which only holds for unit-length vectors. A real embedding would
abort the process. Dot-product collections therefore always take the exact path.

Normalizing vectors on the way in would make it work, but would silently change
what a dot-product search means — so it is refused rather than redefined.

---

## What is not built

**No `$vectorSearch` aggregation stage.** Search is its own endpoint; it does
not yet compose inside a pipeline.

Tracked in [Deviations](deviations.md). Two former entries here are built:
graphs **persist across restarts** (M8 — loaded before any rebuild is paid,
validated by vector count), and **reindexing is just `POST /vector` again** —
every configuration change triggers a backfill that scans the collection and
re-embeds what the new configuration demands. Changing the dimension in place
is legal for server-embedded collections (old-width vectors are invisible to
search while the backfill replaces them) and refused for `byo`, whose vectors
the server cannot regenerate — drop those first.

---

## How this is verified

The exact path is the oracle for everything approximate:

| Invariant | How |
|---|---|
| The graph finds what a scan finds | Recall measured, not assumed: ≥ 90% at k=10, and the nearest neighbour agrees with an exact scan exactly |
| Dispatch does not change results | `the_approximate_path_agrees_with_the_exact_one` asserts both paths return the same nearest neighbour with a **byte-identical score** |
| Re-embedding is idempotent | Replaying an oplog entry after vectors exist at that HLC is a no-op |
| A crash does not lose embeddings | The recorded position always trails completed work |
| Retry does not stall the queue | Retryable and terminal provider failures are distinguished and tested apart |

See [Testing](testing.md) for the philosophy behind measuring rather than
assuming.

---

## Related

- [Architecture](architecture.md) — why the oplog makes this need no scheduler
- [Oplog](oplog.md) — the log and its three consumers
- [Storage](storage.md) — how shadow collections are stored
- [HTTP API](http-api.md) — endpoint reference
- [Deviations](deviations.md) — every simplification named above, in one place
