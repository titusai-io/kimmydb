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
| `server.bind` | `KIMMY_BIND` | `0.0.0.0:7878` | HTTP, WebSocket, and (M3) MCP |
| `storage.data_dir` | `KIMMY_DATA_DIR` | `/var/lib/kimmy` | Holds `kimmy.redb` |
| `storage.tombstone_retention_secs` | — | `86400` | **Must exceed your worst tolerable partition.** Governs deleted documents *and* dropped collections |
| `storage.oplog_retention_secs` | — | `86400` | Bounds resume and peer catch-up |
| `storage.gc_interval_secs` | — | `600` | How often retention is enforced. `0` disables it |
| `cluster.sync_interval_secs` | — | `5` | How often to run an anti-entropy round against each peer |
| `cluster.discovery_interval_secs` | — | `30` | How often to re-resolve seeds. Must repeat, or a node never sees peers that joined later |
| `cluster.fanout` | — | `3` | Peers contacted per round. A cap, not a quota — a smaller cluster contacts everyone |
| `cluster.membership` | — | `true` | Gossip liveness over UDP. Off falls back to discovery-only peers |
| `server.tls.cert_file` | `KIMMY_TLS_CERT` | — | PEM chain, leaf first. TLS is on when this and the key are both set |
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
| `cluster.enabled` | `KIMMY_CLUSTER_ENABLED` | `false` | 📋 M4 |
| `cluster.bind` | `KIMMY_CLUSTER_BIND` | `0.0.0.0:7900` | Gossip |
| `cluster.seeds` | `KIMMY_SEEDS` | `[]` | Naming seeds implies `enabled` |
| `cluster.cluster_secret` | `KIMMY_CLUSTER_SECRET` | — | Required when clustering |
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
  with exit code 0.
- `/var/lib/kimmy` is a volume. **Losing it loses node identity**, not just data.
- Ports: `7878/tcp` (HTTP), `7900/tcp` (replication) **and** `7900/udp` (SWIM membership). Both are needed when clustering.

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

> Clustering lands in M4. Until then each pod is an **independent database** —
> the seeds setting is accepted and validated but nothing replicates. Run
> `replicas: 1` for now unless you genuinely want independent instances.

A headless Service resolving to every ready pod IP is exactly the seed set a
SWIM member needs, which is why `k8s:` discovery is a one-liner.

### Discovery formats

| Form | Meaning |
|---|---|
| `k8s:kimmy-headless.default.svc.cluster.local` | Headless Service, one A record per pod |
| `dns:seeds.example.com` | A/AAAA records, port defaults to 7900 |
| `dns-srv:_kimmy._udp.example.com` | SRV records carry their own ports |
| `static:10.0.0.1:7900,10.0.0.2:7900` | Explicit list |
| `10.0.0.1:7900` | Shorthand for one static peer |

Parsing is implemented and tested; resolution lands in M4.

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

```
kimmy_databases 2
kimmy_collections 5
kimmy_up 1
```

Unauthenticated, like the health endpoints. Deliberately **counts only** —
exposing collection *names* would leak your schema to anything that can reach
the port.

> Richer metrics (request rates, latency, oplog lag, storage size) are 📋
> planned. What is here is honest rather than complete.

---

## Backup and restore

⛔ No built-in backup yet (📋 M5). Today:

```bash
# Cold: stop the server, copy the file
docker stop kimmy
cp /var/lib/docker/volumes/kimmy-data/_data/kimmy.redb backup.redb
docker start kimmy
```

> **Do not copy `kimmy.redb` while the server is running.** redb is ACID but a
> naive file copy can capture a torn state. Use a filesystem or volume snapshot
> if you need a hot backup.

**Restoring carries node identity with it.** That is usually what you want. But
restoring the *same* backup onto two nodes gives them the same identity, which
breaks last-writer-wins tiebreaks. Restore to one node only.

---

## Capacity

| Aspect | Behaviour today |
|---|---|
| Query cost | Index-backed where a secondary index applies, otherwise a collection scan. `POST …/find` with `"explain": true` reports which |
| `skip` | O(n) even with an index; deep paging is expensive |
| Oplog growth | Bounded by `oplog_retention_secs`, enforced every `gc_interval_secs` |
| Tombstone growth | Bounded by `tombstone_retention_secs`, same pass |
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
