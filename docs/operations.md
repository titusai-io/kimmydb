# Operations

[← Documentation index](README.md)

Configuring, deploying, and running KimmyDB.

---

## Configuration

Three sources, lowest precedence first:

```mermaid
graph LR
    D["Built-in defaults"] --> F["TOML file<br/>--config"] --> E["CLI flags<br/>(each also reads KIMMY_*)"]
    style E fill:#2d3748,color:#fff
```

Flags win because they are the most specific thing the operator typed. Every
flag also reads an environment variable, so containers need no config file.

Check what a combination actually resolves to, without starting the server:

```bash
kimmyd --config kimmy.example.toml check-config
```

This validates and prints the effective configuration, then exits non-zero if
anything is wrong — useful as a container entrypoint check or a CI step, and it
fails fast on a bad volume mount.

### Settings

| TOML | Env | Default | Notes |
|---|---|---|---|
| `server.bind` | `KIMMY_BIND` | `0.0.0.0:7878` | HTTP, WebSocket, and MCP |
| `storage.data_dir` | `KIMMY_DATA_DIR` | `/var/lib/kimmy` | Holds `kimmy.redb` |
| `storage.tombstone_retention_secs` | — | `86400` | **Must exceed your worst tolerable partition.** Governs deleted documents *and* dropped collections |
| `storage.oplog_retention_secs` | — | `86400` | Bounds resume and peer catch-up |
| `storage.gc_interval_secs` | — | `600` | How often retention is enforced. `0` disables it |
| `storage.ttl_interval_secs` | — | `60` | How often TTL indexes are checked for expired documents. Separate from `gc_interval_secs`: that reclaims *garbage*, this deletes *live documents* a policy says are due. `0` leaves any TTL index defined but inert |
| `cluster.sync_interval_secs` | — | `5` | How often to run an anti-entropy round against each peer |
| `cluster.discovery_interval_secs` | — | `30` | How often to re-resolve seeds. Must repeat, or a node never sees peers that joined later |
| `cluster.fanout` | — | `3` | Peers contacted per round. A cap, not a quota — a smaller cluster contacts everyone |
| `cluster.membership` | — | `true` | Gossip liveness over UDP. Off falls back to discovery-only peers |
| `server.tls.cert_file` | `KIMMY_TLS_CERT` | — | PEM chain, leaf first. TLS is on when this and the key are both set. Re-read on SIGHUP, or within 60s of changing |
| `server.tls.key_file` | `KIMMY_TLS_KEY` | — | PEM private key (PKCS#8, PKCS#1 or SEC1) |
| `server.rate_limit.login_per_ip` | — | `10` | Failed logins per client address per window. `0` disables |
| `server.rate_limit.login_per_ip_window_secs` | — | `60` | |
| `server.rate_limit.login_per_user` | — | `0` | Failed logins per username across all addresses. Off by default — it is a real defence and a real lockout, see [Security](security.md#login-rate-limiting) |
| `server.rate_limit.login_per_user_window_secs` | — | `300` | |
| `server.rate_limit.trusted_proxy_header` | — | — | Unset means use the socket peer. **Only set it if a proxy you control rewrites the header** |
| `server.rate_limit.max_tracked_keys` | — | `100000` | Bounds the limiter's own memory; the key space is attacker-controlled |
| `auth.root_user` | `KIMMY_ROOT_USER` | `root` | First start only |
| `auth.root_password` | `KIMMY_ROOT_PASSWORD` | — | Required unless `--insecure-no-auth` |
| `auth.jwt_secret` | `KIMMY_JWT_SECRET` | — | **Identical on every node.** ≥16 bytes |
| `auth.token_ttl_secs` | — | `3600` | Also the revocation delay |
| `auth.insecure_no_auth` | `KIMMY_INSECURE_NO_AUTH` | `false` | Loopback binds only |
| `cluster.enabled` | `KIMMY_CLUSTER_ENABLED` | `false` | Naming seeds implies it. In containers also set `cluster.bind` |
| `cluster.bind` | `KIMMY_CLUSTER_BIND` | `0.0.0.0:7900` | Gossip |
| `cluster.seeds` | `KIMMY_SEEDS` | `[]` | Naming seeds implies `enabled` |
| `cluster.cluster_secret` | `KIMMY_CLUSTER_SECRET` | — | Required when clustering |
| `webhooks.allowed_hosts` | — | `[]` | Hosts a webhook may target beyond the public internet. Empty means public addresses only |
| `webhooks.max_concurrent_deliveries` | — | `8` | Deliveries in flight at once. A bound, and what stops one dead endpoint delaying the others |
| `webhooks.max_payload_bytes` | — | `1048576` | Largest request body. Batches are trimmed; a single oversized document is sent without `fullDocument` |
| `audit.mode` | — | `denials` | `off`, `denials`, `writes` or `all`. Records go to the `kimmy::audit` target |
| `log.level` | `KIMMY_LOG_LEVEL` | `info` | `RUST_LOG` overrides |
| `log.format` | `KIMMY_LOG_FORMAT` | `pretty` | `pretty` or `json` |

[`kimmy.example.toml`](../kimmy.example.toml) documents every setting inline.

### Refused at startup

These are configuration errors, caught before serving rather than surfacing as
runtime confusion:

| Combination | Why |
|---|---|
| `insecure_no_auth` + non-loopback bind | Would expose an unauthenticated database to the network |
| No root password, no `insecure_no_auth` | Nothing could authenticate |
| `cluster.enabled` with no seeds | A node with no discovery source can never find peers |
| `cluster.enabled` with no `cluster_secret` | Peers would accept replication from anyone |
| `cluster.enabled` with no `jwt_secret` | Tokens issued by one node would be rejected by the next |
| `oplog_retention_secs = 0` | Change streams could never resume |
| `tombstone_retention_secs = 0` | A peer that never saw a delete could resurrect the document immediately |
| `gc_interval_secs` > `oplog_retention_secs` | Records would outlive their window by up to a whole interval, so the retention setting would not mean what it says |
| An unknown `audit.mode` | A typo would produce a server recording nothing, which looks exactly like a server nobody has attacked |
| A rate-limit window of `0` with a non-zero burst | The burst would divide by a clamped one-millisecond window, making the limit decorative. Disable a limiter by setting its burst to `0` |
| `max_tracked_keys = 0` | A limiter that can remember nothing cannot limit anything |
| Exactly one of `server.tls.cert_file` / `key_file` | The node would start and serve plaintext on a port an operator believes is encrypted |
| A TLS certificate or key that is missing or unreadable | The failure would otherwise land on the first client to connect, not on the operator watching the boot |
| An empty `trusted_proxy_header` | Reads as a header whose name is empty, so it never matches — an operator would believe forwarding was configured when it was not |

Boolean flags are one-way: passing `--insecure-no-auth` turns it on, but
omitting it does **not** turn off what the config file asked for.

---

## Running

```bash
# From source
KIMMY_ROOT_PASSWORD=change-me KIMMY_JWT_SECRET=$(openssl rand -base64 32) \
  cargo run --bin kimmyd -- --bind 127.0.0.1:7878 --data-dir ./data

# Local development, no auth (loopback only)
cargo run --bin kimmyd -- --insecure-no-auth --bind 127.0.0.1:7878 --data-dir ./data
```

### Docker

```bash
docker build -t kimmydb .
docker run -d --name kimmy -p 7878:7878 \
  -e KIMMY_ROOT_PASSWORD=change-me \
  -e KIMMY_JWT_SECRET=a-long-random-secret \
  -v kimmy-data:/var/lib/kimmy \
  kimmydb
```

Image is ~106 MB (Debian slim runtime). Notes:

- Runs as **uid 10001**, not root. **Anything you mount must be readable by that
  uid** — a TLS key at mode `0600` owned by you makes the node refuse to start
  with `Permission denied`, naming the file. Either `chown 10001` the key or
  give it a group the container can read.
- `kimmyd` is PID 1 with **no shell wrapper**, so it receives `SIGTERM` directly
  from `docker stop` and Kubernetes. Measured: `docker stop` returns in ~290 ms
  with exit code 0. It also takes **`SIGHUP` to reload the TLS certificate**
  (`docker kill -s HUP`) — though under an orchestrator you rarely need to,
  since a changed file is picked up within 60 seconds either way
  ([Security](security.md)).
- `/var/lib/kimmy` is a volume. **Losing it loses node identity**, not just data.
- Ports: `7878/tcp` (HTTP), `7900/tcp` (replication) **and** `7900/udp` (SWIM membership). Both are needed when clustering.

> **Upgrading a cluster to a version with replication TLS.** Replication is now
> encrypted always ([ADR-040](decisions.md)), and a node speaking TLS cannot
> talk to one speaking plaintext. A cluster therefore cannot be upgraded one
> node at a time across that boundary: stop the cluster, upgrade every node,
> start it again. Nodes will not lose data — each holds a full copy and
> anti-entropy reconciles on restart — but replication stops for the duration.

> **Upgrading a cluster to a version whose SWIM identity carries a node id.**
> The same shape of cutover, for the same reason. Membership identities are
> encoded with postcard, which is not self-describing, so the added field
> changes the wire format ([ADR-051](decisions.md)): a new node **rejects** an
> old node's identity outright, and an old node silently ignores the new
> field. A mixed-version cluster therefore does not form membership at all —
> it does not merely disagree about webhook ownership. Stop the cluster,
> upgrade every node, start it again. Replication still runs from discovery
> while membership is down, so data keeps moving; what stops is failure
> detection and webhook ownership.

> **Upgrading a cluster to a version that authenticates SWIM.** The third
> cutover of this shape, for the same reason. Membership datagrams now carry an
> HMAC over the payload ([ADR-053](decisions.md)), and a tagged datagram is not
> a valid untagged one, so old and new nodes cannot gossip. Stop the cluster,
> upgrade every node, start it again. Replication is unaffected while
> membership is down — it falls back to discovery — but failure detection and
> webhook ownership are.
>
> **Rotating `cluster_secret` is the same operation.** A node holding a
> different secret is now refused by membership as well as by replication,
> which is the point: before, it joined the member set and silently won
> ownership of a share of the webhook subscriptions it could not deliver. Roll
> the secret with the cluster stopped, not one node at a time.

**Clustering in containers needs an explicit `KIMMY_CLUSTER_BIND`.** It defaults
to the wildcard `0.0.0.0:7900`, and a wildcard is a listening instruction rather
than an identity, so the node refuses to announce it and advertises loopback
with a warning ([ADR-037](decisions.md)). Inside a container that tells every
peer to reach this node at `127.0.0.1`, which is their own container.

The symptom is specific and easy to misread as working: **replication still
converges**, because anti-entropy dials the addresses discovery resolved. What
is lost is SWIM — learning peers nobody configured, and a shared opinion about
which nodes are alive. Nothing is ever declared down; only each node's private
backoff notices. Set it to the container's routable address:

```bash
docker run ... -e KIMMY_CLUSTER_BIND=172.28.0.11:7900 ...
```

`cluster.bind` is a socket address and takes an IP literal, not a hostname,
which is why [`docker-compose.yml`](../docker-compose.yml) pins a subnet and
gives each node a fixed address. On Kubernetes use the downward API — see below.

### Kubernetes

Use a **StatefulSet** with a headless Service — stable identity and per-pod
storage both matter here.

```yaml
apiVersion: v1
kind: Service
metadata:
  name: kimmy-headless
spec:
  clusterIP: None          # headless: resolves to every ready pod IP
  selector: { app: kimmy }
  ports:
    - { name: http,   port: 7878 }
    - { name: cluster,    port: 7900, protocol: TCP }
    - { name: membership, port: 7900, protocol: UDP }
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: kimmy
spec:
  serviceName: kimmy-headless
  replicas: 3
  selector: { matchLabels: { app: kimmy } }
  template:
    metadata: { labels: { app: kimmy } }
    spec:
      terminationGracePeriodSeconds: 30
      containers:
        - name: kimmy
          image: kimmydb:latest
          ports:
            - { containerPort: 7878 }
            - { containerPort: 7900 }
          env:
            # Required for gossip. `cluster.bind` defaults to the wildcard
            # 0.0.0.0:7900, and a wildcard is a listening instruction rather
            # than an identity, so the node refuses to announce it and falls
            # back to advertising loopback with a warning (ADR-037). In a pod
            # that means every peer is told to reach this node at 127.0.0.1 —
            # which is their own container. Replication still converges via
            # discovery; SWIM does not.
            - name: POD_IP
              valueFrom: { fieldRef: { fieldPath: status.podIP } }
            - name: KIMMY_CLUSTER_BIND
              value: "$(POD_IP):7900"
            - name: KIMMY_SEEDS
              value: "k8s:kimmy-headless.default.svc.cluster.local"
            - name: KIMMY_JWT_SECRET
              valueFrom: { secretKeyRef: { name: kimmy, key: jwt-secret } }
            - name: KIMMY_ROOT_PASSWORD
              valueFrom: { secretKeyRef: { name: kimmy, key: root-password } }
            - name: KIMMY_CLUSTER_SECRET
              valueFrom: { secretKeyRef: { name: kimmy, key: cluster-secret } }
          livenessProbe:
            httpGet: { path: /healthz, port: 7878 }
          readinessProbe:
            httpGet: { path: /readyz, port: 7878 }
          volumeMounts:
            - { name: data, mountPath: /var/lib/kimmy }
  volumeClaimTemplates:
    - metadata: { name: data }
      spec:
        accessModes: [ReadWriteOnce]
        resources: { requests: { storage: 10Gi } }
```

> **`KIMMY_CLUSTER_BIND` is not optional here.** Without it the node advertises
> loopback and SWIM never forms — replication still converges through discovery,
> which is what makes the misconfiguration look healthy. The downward-API
> snippet above is the fix.

A headless Service resolving to every ready pod IP is exactly the seed set a
SWIM member needs, which is why `k8s:` discovery is a one-liner.

### Discovery formats

| Form | Meaning |
|---|---|
| `k8s:kimmy-headless.default.svc.cluster.local` | Headless Service, one A record per pod |
| `dns:seeds.example.com` | A/AAAA records, port defaults to 7900 |
| `dns-srv:_kimmy._tcp.example.com` | SRV records carry their own ports |
| `static:10.0.0.1:7900,10.0.0.2:7900` | Explicit list |
| `10.0.0.1:7900` | Shorthand for one static peer |

All four resolve. Every form is re-resolved each `discovery_interval_secs`, so
a peer that appears later is found without a restart.

**`dns-srv:` is the one form where peers need not agree on a port**, because
each record carries its own:

```
_kimmy._tcp.example.com. 60 IN SRV 0 10 7911 node-a.example.com.
_kimmy._tcp.example.com. 60 IN SRV 0 10 7922 node-b.example.com.
```

Each target is then resolved to addresses, and every address is paired with the
port from the record that named it. Priority and weight are read but not acted
on — every peer is contacted, because this is a peer set rather than a
failover list.

Resolution uses `/etc/resolv.conf`, so a container inherits its cluster's
resolver with nothing configured. A target that will not resolve is skipped and
the rest are kept: one pod mid-restart should not cost a node every other peer.
A name that exists with no SRV records is an empty set rather than an error —
the normal state of a cluster before its first node registers
([ADR-050](decisions.md)).

---

## Observability

### Logs

`tracing`, with `RUST_LOG` taking precedence over `log.level` — that is what an
operator reaches for when debugging a running container.

```bash
RUST_LOG=info,kimmy_storage=debug kimmyd …
KIMMY_LOG_FORMAT=json kimmyd …          # one JSON object per line
```

### Health

| Endpoint | Meaning | Probe |
|---|---|---|
| `/healthz` | The process is alive | liveness |
| `/readyz` | The **storage engine responds** | readiness |

`/readyz` performing a real storage read is the point: a node with a wedged
database is taken out of rotation rather than served traffic it cannot handle.

### Metrics

Unauthenticated, like the health endpoints, and deliberately **counts only** —
exposing collection *names* would leak your schema to anything that can reach the
port.

| Series | |
|---|---|
| `kimmy_up` | Always 1; presence means the node is serving |
| `kimmy_uptime_seconds` | Since this process started |
| `kimmy_databases`, `kimmy_collections` | Counts, not names |
| `kimmy_storage_bytes` | Size of the database file |
| `kimmy_requests_total` | HTTP requests handled |
| `kimmy_responses_total{class}` | `2xx`, `4xx`, `5xx` |
| `kimmy_authz_denied_total` | Refused by RBAC |
| `kimmy_auth_failures_total` | Rejected credentials and tokens |
| `kimmy_rate_limited_total` | Refused by a rate limit |
| `kimmy_unique_violations` | Constraints broken by merging replicated writes |
| `kimmy_backups_total` | Backups served |
| `kimmy_cluster_members` | Peers SWIM currently considers alive; 0 with clustering off. A formed three-node cluster reads 2 on every node |
| `kimmy_replication_lag_seconds` | Seconds of peer oplog history not yet **seen** locally, worst peer in the last sync round. **Alert on this**: 0 is the caught-up steady state, and it climbing means the backlog exceeds a sync batch. Holds its last value while no peer is reachable — an outage has *unknown* lag, not zero. Measured against what this node has processed rather than what it could re-serve, or entries it correctly discarded would pin it non-zero forever ([ADR-054](decisions.md)) |
| `kimmy_request_duration_seconds` | End-to-end latency histogram; buckets measured, not guessed ([ADR-046](decisions.md)). Health and metrics routes are excluded so scrapes do not crowd the buckets real traffic lands in |
| `kimmy_tls_reloads_total{outcome}` | `ok` / `failed` certificate reloads. **Alert on `failed`**: the node keeps serving the certificate it already had, so a botched renewal is invisible until that one expires and every client drops at once ([ADR-049](decisions.md)) |

Counters render at zero before their first event, so a dashboard shows "nothing
has gone wrong yet" rather than "no data".

The two absences ADR-043 recorded — latency histograms and oplog lag — are
filled by the last two rows, each on the terms that kept it out
([ADR-046](decisions.md)).

---

## The audit log

A structured record of **authorization decisions** — who was allowed or refused
what, on which collection.

```toml
[audit]
mode = "denials"   # off | denials | writes | all
```

| Mode | Records |
|---|---|
| `off` | Nothing |
| `denials` | Refusals only. **The default** |
| `writes` | Refusals, plus anything that wrote or administered |
| `all` | Every decision, including reads |

`all` writes one line per authorized operation, which on a read-heavy node is one
per request. That is why it is not the default; a denial is rare and is the event
worth watching for.

Records go to the **`kimmy::audit`** tracing target, so they can be routed
separately from the application log:

```bash
# JSON lines, audit at info, everything else quieter
KIMMY_LOG_FORMAT=json KIMMY_LOG_LEVEL='warn,kimmy::audit=info' kimmyd run
```

Each record carries `user`, `action`, `db`, `collection`, `decision`, and
`unauthenticated` — the last distinguishing "root did this" from "the server was
started with authentication disabled".

Emitted from the single authorization point rather than from each route, so a new
route is audited by virtue of being authorized at all ([ADR-042](decisions.md)).
An unknown mode is refused at startup, because a typo would otherwise produce a
server that records nothing — indistinguishable from one nobody has attacked.

**Logins are not in this stream.** A failed login is not an authorization
decision; it is logged separately and counted as `kimmy_auth_failures_total`.

---

## Backup and restore

### Taking a backup

The node takes it, while it is serving:

```bash
curl -H "Authorization: Bearer $TOKEN" \
     -o kimmy.backup \
     https://your-node:7878/v1/admin/backup
```

Requires **`admin` over `*`** — a backup is every document on the node, so a
lesser grant would read past its own scope. There is no grant-filtered backup: a
partial backup that looks whole is a restore that silently loses data.

It runs inside a read transaction, so it is a consistent snapshot of one instant
and writers are neither blocked nor affected. The response is buffered before
sending rather than streamed as it is produced, so a slow client cannot pin
redb's pages by reading slowly.

> **Still do not copy `kimmy.redb` from a running node.** redb is rewriting
> pages underneath the copy, and the result is not a state the database was ever
> in. The endpoint above exists so you do not have to.

### Restoring

Offline, because redb allows one process to hold a database:

```bash
# The node must not be running, and the data directory must not already
# contain kimmy.redb.
kimmyd --data-dir /var/lib/kimmy restore --from kimmy.backup
```

Restore **refuses to overwrite an existing database**. An in-place restore turns
a mistyped path into data loss; remove the file yourself if that is what you
mean.

### Point-in-time restore

Restore a backup and rewind it to an earlier instant:

```bash
kimmyd --data-dir /var/lib/kimmy restore \
       --from kimmy.backup \
       --until 1786250131859      # milliseconds since the epoch
```

Take the backup **after** the incident — its oplog is what describes the
incident, and the rewind undoes it.

**What it can undo.** Any document change whose *previous* value is still in the
oplog. `storage.oplog_retention_secs` is therefore the real point-in-time
window.

**What it refuses, rather than guessing.**

| Refusal | Why |
|---|---|
| A target before the oplog horizon | Nothing describes the database before that point |
| A schema change after the target | Dropping a collection purges its documents, and purged documents are not in the oplog either |
| A document whose earlier value was collected | It existed at the target with a value that now exists nowhere. It is named in the error |

The last one is the important one. The oplog stores what a document *became*,
never what it was, and a delete stores nothing at all — so a document untouched
since before the horizon and then changed cannot be put back. Leaving it at its
later value would produce a database that looks restored and is not, so the
whole rewind is refused instead. **Nothing is written until every check has
passed**, so a refusal leaves the file exactly as the restore wrote it.

> ⚠️ **A rewound database must not rejoin a cluster that still holds the undone
> writes.** Anti-entropy would put them straight back. Run it standalone, or
> rewind every node.

### The identity comes back with it

A backup carries the node's id, and a restore keeps it. That is what you want
when replacing a node: the id is the tiebreak half of every write's stamp, so a
node that restored under a new identity would become a stranger to its own
history.

> ⚠️ **Restoring one backup onto two nodes gives them the same identity**, and
> the cluster cannot tell them apart — which breaks the tiebreak that makes
> convergence deterministic. Restore is for **replacing** a node, not cloning
> one. To add a node, start an empty one and let anti-entropy fill it.

There is deliberately no flag to mint a fresh identity on restore: it would be
one keystroke between recovering and corrupting a cluster's identity space.

### What a backup contains

Everything the node holds: documents, collection and index metadata, secondary
index entries, the oplog and its arrival index, tombstones for deleted documents
and dropped collections, version vectors, the user store, and the node id.

Restoring an older backup onto a newer build is supported while the format
version matches; a backup from a *newer* build is refused by name rather than
partially read.

---

## Capacity

| Aspect | Behaviour today |
|---|---|
| Query cost | Index-backed where a secondary index applies, otherwise a collection scan. `POST …/find` with `"explain": true` reports which |
| `skip` | O(n) even with an index; deep paging is expensive |
| Oplog growth | Bounded by `oplog_retention_secs`, enforced every `gc_interval_secs` |
| Tombstone growth | Bounded by `tombstone_retention_secs`, same pass |
| TTL expiry | At most 1,000 documents per collection per pass, so a backlog drains over several ticks rather than holding the single writer. **One node expires a given collection**; if it is partitioned that collection stops expiring until ownership moves. Watch `kimmy_ttl_expired_total` and `kimmy_ttl_skipped_total` |
| Change-stream buffer | 1024 events per subscriber; lag recovers from disk |
| `find` result cap | 100 default, 10,000 maximum |

Oplog entries carry full post-images, so update-heavy workloads on large
documents grow the log quickly: 10 KB documents updated once a second is roughly
860 MB/day. Retention caps that at one window's worth, so provision for the data
plus roughly `oplog_retention_secs` of log.

**A collection pass temporarily grows the file before it shrinks it.** redb is
copy-on-write, so the transaction that removes records allocates new pages
before the old ones are freed. Measured on 2,000 documents of 4 KB, all deleted
and then collected:

| | File size |
|---|---|
| Before collection | 52.7 MB |
| Immediately after | 105.4 MB |
| After writing 2,000 fresh documents | 53.3 MB |

So the space *is* reclaimed and the file *does* shrink — but the peak comes
during collection, not before it. **Keep free space at least equal to the
volume a single pass will collect.** A first pass on a database that has never
been collected is the largest one, which is exactly when headroom is tightest;
a shorter `gc_interval_secs` keeps each pass small.

---

## Upgrades

The on-disk **schema version** is checked on open, and the two directions are
treated differently on purpose:

| Stored version | Behaviour |
|---|---|
| Older than this build | **Migrated in place** on open, logged at `info` |
| Equal | Opens normally |
| Newer | **Refuses to start** |

Refusing on a *newer* schema is the right failure: a build cannot know a layout
that did not exist when it was written, and guessing corrupts further. Migrating
an *older* one is equally right — a user with data has no other route forward.

Current schema is **3**. Migrations run in sequence, so an older database steps
through each one rather than needing its own path to the latest.

| Step | What it does |
|---|---|
| 1 → 2 | Collections renumbered to ids derived from their names ([ADR-031](decisions.md)) — rewrites document keys, index entries, and the collection field of every oplog entry |
| 2 → 3 | Indexes renumbered to ids derived from their names ([ADR-032](decisions.md)) — rewrites index-entry keys |

Both are idempotent and run before the node serves anything.

> **Back up the data directory before a version-crossing upgrade.** The
> migration is transactional per step, but a rollback to the older build is not
> possible once it has run — the older build will refuse the newer schema.

### Rebuild vector indexes after upgrading past 2026-08-15

**Builds before this date could produce an HNSW index that silently lost part
of its collection.** About one index build in 250 left 10–24% of the vectors
unreachable from the graph, so those documents were never returned by any
vector search, at any `k`, for any query. Nothing failed and nothing was
logged — the searches simply came back without them.

Newer builds verify a finished graph can retrieve its own data and rebuild one
that cannot, so **no new index has this problem**. But the check runs at build
time, and it does not repair a graph that is already cached in a running
process or persisted as a snapshot on disk.

**What to do**, on each node, if a collection has vector search enabled and was
indexed by an older build:

```bash
# Stop the node first: a running process serves the graph it has in memory,
# so deleting the snapshots under it changes nothing until it restarts.
rm -rf <data_dir>/hnsw/
# Start it again; each collection's graph rebuilds on the next search.
```

Rebuilding costs O(n log n) per collection and happens on the next search that
needs the index. There is no data to recover — the *vectors* were always
stored correctly and the exact scan could always see them; only the approximate
index was incomplete.

**How to tell whether you were affected**: search for a document you know is
present, by its own embedding. If exact search finds it and vector search does
not, the index was one of the bad ones. `docs/deviations.md` has the full
measurement.

---

## Troubleshooting

| Symptom | Cause |
|---|---|
| `no root password configured` | Set `KIMMY_ROOT_PASSWORD` or pass `--insecure-no-auth` on loopback |
| `insecure_no_auth is set but the server binds to 0.0.0.0` | Bind to `127.0.0.1` or configure auth |
| `on-disk format version N is not supported` | Data directory written by a **newer** build; older ones migrate automatically |
| 401 on a token that worked a moment ago | Token expired (1 h default), or the node has a different `jwt_secret` |
| 403 where you expected 404 | Deliberate — authorization does not reveal existence |
| `410 resume_token_expired` | Resume point passed out of the retained oplog; resubscribe |
| Second `Engine::open` fails | redb allows one handle per file; share an `Arc<Engine>` |
| Queries slow on a large collection | Check `find` with `"explain": true`; if `strategy` is `collectionScan`, add an index |

---

## Next

- [Security](security.md) — the deployment checklist
- [HTTP API](http-api.md) — endpoint reference
- [Roadmap](roadmap.md) — what closes these gaps
