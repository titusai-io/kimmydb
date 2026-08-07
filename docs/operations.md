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
| `storage.tombstone_retention_secs` | — | `86400` | **Must exceed your worst tolerable partition** |
| `storage.oplog_retention_secs` | — | `86400` | Bounds resume and peer catch-up |
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

Image is ~93 MB (Debian slim runtime). Notes:

- Runs as **uid 10001**, not root.
- `kimmyd` is PID 1 with **no shell wrapper**, so it receives `SIGTERM` directly
  from `docker stop` and Kubernetes. Verified: exits cleanly in ~20 ms.
- `/var/lib/kimmy` is a volume. **Losing it loses node identity**, not just data.
- Ports: `7878/tcp` (HTTP), `7900/tcp+udp` (gossip, M4).

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
    - { name: gossip, port: 7900 }
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
| Query cost | **O(n) — every query is a collection scan.** Indexes ⛔ not implemented |
| `skip` | O(n); deep paging is expensive |
| Oplog growth | Unbounded — retention is ⛔ not enforced yet |
| Change-stream buffer | 1024 events per subscriber; lag recovers from disk |
| `find` result cap | 100 default, 10,000 maximum |

Oplog entries carry full post-images, so update-heavy workloads on large
documents grow the log quickly: 10 KB documents updated once a second is roughly
860 MB/day. Provision disk with that in mind until retention lands.

---

## Upgrades

The on-disk format version is checked on open. A mismatch **refuses to start**
rather than misreading records — the right failure, since a silent
misinterpretation corrupts further.

There is no migration tooling yet. While the format is at version 1 and
pre-release, treat a format bump as "start from a fresh data directory".

---

## Troubleshooting

| Symptom | Cause |
|---|---|
| `no root password configured` | Set `KIMMY_ROOT_PASSWORD` or pass `--insecure-no-auth` on loopback |
| `insecure_no_auth is set but the server binds to 0.0.0.0` | Bind to `127.0.0.1` or configure auth |
| `on-disk format version N is not supported` | Data directory written by a different build |
| 401 on a token that worked a moment ago | Token expired (1 h default), or the node has a different `jwt_secret` |
| 403 where you expected 404 | Deliberate — authorization does not reveal existence |
| `410 resume_token_expired` | Resume point passed out of the retained oplog; resubscribe |
| Second `Engine::open` fails | redb allows one handle per file; share an `Arc<Engine>` |
| Queries slow on a large collection | Expected — no indexes yet |

---

## Next

- [Security](security.md) — the deployment checklist
- [HTTP API](http-api.md) — endpoint reference
- [Roadmap](roadmap.md) — what closes these gaps
