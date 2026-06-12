# Candybox — Operations Guide

How to run, tune, and reason about a Candybox cluster, plus its failure modes and current limitations.
For architecture and on-ledger formats see [`DESIGN.md`](DESIGN.md); for build/test see [`README.md`](README.md).

## Topology

A Candybox cluster is a set of **storage nodes** over a shared **Apache BookKeeper** ensemble and a
shared **ZooKeeper** ensemble:

- **BookKeeper** stores all ledgers — WALs, SSTables, Syrups (Candy bytes), and the per-partition
  manifests.
- **ZooKeeper** (via the coordination SPI) holds cluster membership, each Box's **descriptor**
  (its creation-time-fixed partition count), per-partition ownership **leases**, the versioned
  pointer to each partition's current manifest ledger, and the balancer's election lease +
  assignment table.
- Each **Box** is split into hash **partitions** (`partitions.per.box.default`, default 8); each
  partition is owned by exactly one node at a time (a fenced, movable lease) and is a full LSM
  engine. The owner is the sole writer of that partition's WAL, memtable flushes, and manifest.
  Clients route each request to the owner of the key's partition, so one Box's writes spread across
  the cluster. The **balancer** (`balancer.interval.millis` > 0; one node coordinates under the
  `cluster/balancer` lease) keeps partition ownership evenly distributed, fails over dead nodes'
  partitions, and rate-limits migrations away from live owners
  (`balancer.max.moves.per.round`).

A node is `CandyboxNode(nodeId, config, ledgerStore, coordination, clock, advertisedAddress)` wired
behind a `TcpTransportServer`; the advertised `host:port` is published to membership so the
cluster-aware `CandyboxClient(transport, coordination, config)` can route to and re-route between owners.

## Configuration reference (`CandyboxConfig`)

All knobs are configurable; pick defaults unless a workload demands otherwise.

| Knob | Default | What it controls |
|---|---|---|
| `sizeLimits.chunkSizeBytes` | 1 MiB | Syrup entry payload size. Must stay below BookKeeper's max frame size. |
| `sizeLimits.maxCandyKeyBytes` | 1 KiB | Max CandyKey length (UTF-8). |
| `sizeLimits.maxUserMetadataBytes` | 8 KiB | Max total user metadata per Candy. |
| `sizeLimits.maxLocatorBytes` | 64 KiB | Hard cap on a serialized CandyLocator. |
| `sizeLimits.maxCandySizeBytes` | 0 (unbounded) | Max Candy size; `0` = no limit. |
| quorum `WAL` / `MANIFEST` | 3/3/2 (E/Qw/Qa) | Recovery sources: all-replica write, majority ack. |
| quorum `SSTABLE` / `SYRUP` | 3/2/2 | Durable but read/throughput-optimized; replaceable via re-compaction. |
| `bloomBitsPerKey` | 10 | SSTable bloom filter sizing (~1% FPR). |
| `memtableFlushThresholdBytes` | 4 MiB | Memtable size that triggers a flush to an L0 SSTable. |
| `syrupRolloverBytes` | 1 GiB | Open Syrup size before rolling to a fresh one. |
| `maxFrameSizeBytes` | 16 MiB | Protocol frame cap; oversized/malformed frames are rejected pre-allocation. |
| `ownershipLeaseTtlMillis` | 10 s | Partition ownership lease TTL; must be renewed within it. |
| `leaseRenewIntervalMillis` | 3 s | Lease heartbeat interval; `0` disables the background heartbeat. |
| `routerCacheTtlMillis` | 5 s | Client partition→owner routing-cache TTL. |
| `partitionsPerBoxDefault` | 8 | Hash-partition count for a new Box when the creator passes none; fixed for the Box's lifetime. |
| `balancerIntervalMillis` | 0 (disabled) | Partition-balancing round period; **set > 0 in production** (shipped conf: 5 s). |
| `balancerMaxMovesPerRound` | 4 | Max partitions migrated away from live owners per round (failover is unlimited). |
| `compactionIntervalMillis` | 0 (disabled) | Background compaction+GC tick; **set > 0 in production**. |
| `l0CompactionTrigger` | 4 | L0 SSTable count that triggers a compaction. |
| `l0StallThreshold` | 12 | L0 SSTable count at which writes are rejected with `BUSY`. |
| `maxClockSkewMillis` | 5 min | HLC skew-rejection bound on observed timestamps. |
| `tombstoneGcGraceMillis` | 24 h | Late-write window before a bottommost tombstone may be dropped. |
| `ledgerGcGraceMillis` | 5 min | Grace before an obsoleted ledger (compaction input, dead Syrup, rotated WAL) is deleted. |

Leveled compaction also takes a per-level byte budget (`levelBaseBytes` 10 MiB, `levelMultiplier` 10):
level N's budget is `levelBaseBytes × multiplier^(N-1)`.

> **Important:** background compaction/GC are **off by default** (`compactionIntervalMillis = 0`) so
> tests are deterministic. A production node must set it positive, otherwise L0 grows until writes hit
> `BUSY` and nothing reclaims obsolete ledgers. Compaction and GC can also be triggered manually via
> `CandyboxNode.compactOwnedBoxesOnce()` / `collectGarbageOnce()`.

## Consistency & backpressure

- **Per-key linearizable on the owner.** Every key maps to one partition, and a single fenced owner
  serializes all writes for a partition and stamps a Hybrid Logical Clock at ingest; reads resolve
  to the highest HLC (LWW, nodeId tiebreaker). The only per-key eventual-consistency window is
  ownership handover. Cross-partition operations are weaker: listings are scatter-gather merges,
  `deleteRange` is one tombstone per partition (idempotent, not atomic), and a cross-partition
  rename is copy-then-delete.
- **Backpressure.** When a Box accumulates `l0StallThreshold` L0 SSTables, writes return a retriable
  `BUSY` (protocol `RESPONSE_BUSY`) instead of blocking. Clients should back off and retry; the stall
  clears once compaction drains L0.

## Failure modes & recovery

| Situation | Behaviour |
|---|---|
| **Bookie loss, quorum still met** | Writes/reads continue (entries reach ack-quorum on surviving bookies). |
| **Bookie loss, ensemble can't form** | A new-ledger write fails cleanly with a `StorageException` (no hang); it succeeds again once enough bookies return. |
| **Open Syrup sealed under the writer** | The engine abandons the dead Syrup and rolls to a fresh one on the next write; already-written Candies stay readable. |
| **Owner crash / lease loss** | Another node acquires the lease, **recover-opens (fences)** the prior WAL and manifest, replays them, advances its HLC past the max observed, opens fresh WAL/manifest ledgers, and CAS-advances the ZK manifest pointer. The old owner is fenced: its writes fail with `FencedException`. |
| **Zombie owner / zombie compactor** | A fencing token on every manifest append, plus BookKeeper recover-open, reject any commit from a superseded owner. |
| **Client hits the wrong node** | The node replies `RESPONSE_MOVED(ownerNodeId)`; the client re-resolves via coordination and retries. |
| **Clock skew** | HLC ordering survives a regressing wall clock; an observed HLC leading local time by more than `maxClockSkewMillis` is rejected. |

With the balancer enabled (`balancerIntervalMillis > 0`), a dead node's partitions are reassigned
and taken over automatically within a round or two of its leases expiring. With it disabled,
recovery requires some node to take over unowned partitions manually
(`CandyboxNode.openPartition` / `openBox`).

## Garbage collection

Run only by a Box's owner, against the committed manifest, after `ledgerGcGraceMillis`:

- **SSTables** removed by a committed compaction are deleted.
- **Syrups** no longer referenced by any SSTable, the memtable, or the open write Syrup are
  whole-ledger-deleted (v1 reclaims a Syrup only once *every* segment in it is dead — no defragmentation,
  so deletes/overwrites cause Syrup space amplification until the whole ledger dies).
- **WAL** ledgers rotated out at flush are deleted once their mutations are durable in an SSTable.

GC tracking is in-memory, so ledgers orphaned by a prior owner that crashed before GC leak until an
enumeration backstop is added (future).

## Observability

`BoxEngine.stats()` returns a `BoxEngineStats` snapshot: cumulative `puts`, `deletes`, `gets`, `heads`,
`lists`, `flushes`, `compactions`, and `stallRejections`. Logging is SLF4J with box / key / ledger
context. Each node also exposes a small Prometheus exposition on its HTTP health port
(`/metrics`, port 9710 by default), and the gateway's health port (9712 by default) does the same.

### Admin / dashboard API (`candybox-admin-api`)

A stateless HTTP service that fans out reads against the cluster and serves a React+MUI web
dashboard. Like the S3 gateway it's a *client* of the cluster — it never touches BookKeeper
directly. Single port (default `9713`); the SPA lives at `/ui/` and the JSON API at `/api/*`.

| Variable | Default | Purpose |
|---|---|---|
| `CANDYBOX_ADMIN_PORT` | `9713` | TCP port for the JSON API + UI. |
| `CANDYBOX_ADMIN_BIND` | `0.0.0.0` | Bind interface. |
| `CANDYBOX_ADMIN_CORS` | `*` | `Access-Control-Allow-Origin`. Tighten in production. |
| `CANDYBOX_ADMIN_UI` | (enabled) | Set to `false` to disable `/ui/*` and run headless. |
| `CANDYBOX_ADMIN_ZK` | (unset) | ZooKeeper connect string. Enables the live cluster/box reads; unset = empty-data demo mode. |
| `CANDYBOX_ADMIN_SCRAPE_TARGETS` | (empty) | Comma-separated Prometheus URLs (e.g. each node's `http://host:9710/metrics`). Drives the Metrics page. |
| `CANDYBOX_ADMIN_SCRAPE_INTERVAL_MS` | `5000` | Scrape interval. |
| `CANDYBOX_ADMIN_SCRAPE_WINDOW` | `60` | Samples retained per series (≈ 5 minutes at the default interval). |

Routes:

| Method · Path | Returns |
|---|---|
| `GET /api/cluster` | nodes, owned-box counts, ownerless boxes |
| `GET /api/boxes` | all box names + owner |
| `GET /api/boxes/{name}` | one box (owner, metadata) |
| `GET /api/boxes/{name}/objects?prefix=&startAfter=&max=` | candy listing |
| `GET /api/lsm` | per-box manifest version + fencing token (coordination-derived) |
| `GET /api/metrics` | passthrough of the latest scrape text |
| `GET /api/metrics/timeseries?names=a,b,...` | the rolling per-series window |
| `GET /healthz`, `GET /readyz` | mirror the per-node probes |
| `GET /ui/*`, `GET /` | static SPA bundle (or a placeholder if `-Pfrontend` was off) |

v1 has **no authentication** (matching the rest of candybox today); the deploy assumption is a
trusted network. The single mutating dashboard operation in v1 — deleting an object — goes through
the S3 gateway from the browser, not through this API. See `WEB_DASHBOARD_PLAN.md`.

## Security

Candybox secures every channel with SASL authentication, per-Box/per-object ACLs, and in-process
TLS. The full design lives in [`AUTH_PLAN.md`](AUTH_PLAN.md); this section is the operator view.
All keys below follow the usual convention: any key can come from the environment as
`CANDYBOX_<KEY>` (dots to underscores, upper-cased).

### Clients / S3 gateway / admin API → storage nodes (SASL + TLS)

On each node:

```properties
auth.enabled=true                  # SASL on the TCP listener
auth.required=true                 # false = unauthenticated connections pass as anonymous
auth.sasl.mechanisms=PLAIN,SCRAM-SHA-256
auth.credentials.file=/etc/candybox/credentials.properties
auth.super.users=Gateway:s3-gw,Admin:admin-api

tls.enabled=true                   # PEM paths; the key must be UNENCRYPTED PKCS#8
tls.cert.path=/etc/candybox/tls/tls.crt
tls.key.path=/etc/candybox/tls/tls.key
tls.ca.path=/etc/candybox/tls/ca.crt
tls.client.auth=false              # true = demand a client certificate (mTLS)
```

Generate credential-file entries with `candybox make-credentials <user>`; the file is reloaded on
change (rotation needs no restart), and `plain:` verifiers are accepted for dev fixtures. Choose
**PLAIN only on TLS listeners** (the password crosses the wire; the server stores a one-way hash);
**SCRAM-SHA-256** never sends the password and mutually authenticates, so it is the safer choice
when TLS terminates elsewhere. Dev certificates: `examples/security/gen-dev-certs.sh`.

Anything dialing the nodes uses the client side of the same surface (gateway, admin API, CLI):

```properties
auth.client.mechanism=PLAIN
auth.client.username=s3-gw
auth.client.password.file=/etc/candybox/s3-gw.password
tls.enabled=true
tls.ca.path=/etc/candybox/tls/ca.crt
```

The CLI takes the same via `--user/--password-file/--mechanism/--tls/--tls-ca` or
`CANDYBOX_AUTH_*`/`CANDYBOX_TLS*` env vars.

### Authorization (ACLs)

Every request is authorized against the caller's principal (`User:alice`, `Gateway:s3-gw`, ...).
A Box's ACL document (owner + additive grants, stored in ZooKeeper) governs READ / WRITE /
READ_ACP / WRITE_ACP / ADMIN; the creator owns the Box, and `ListBoxes` only reveals READable
Boxes. Objects additionally carry their own owner + grants inside the `CandyLocator` (the S3
union rule: an object grant can open READ on a single object inside a private Box). Manage with:

```
candybox acl get|set|grant|revoke <box> [--object <key>]
e.g. candybox acl grant photos AllUsers:READ          # public-read Box
```

`auth.super.users` principals bypass ACLs entirely — list the S3 gateway and admin API service
accounts there. A Box created before authorization (no document) falls back to
authenticated-full-access.

### Candybox → ZooKeeper

```properties
zookeeper.auth.scheme=digest
zookeeper.auth.credentials=candybox:change-me
zookeeper.acl.enabled=true         # defaults to true once a scheme is set
```

With ACLs on, every znode candybox creates is `CREATOR_ALL` — so **all candybox processes of one
cluster must authenticate as the same ZooKeeper identity**. For SASL (Kerberos/DIGEST-MD5) set
`CANDYBOX_JAAS_CONF` to a JAAS file with a `Client` section (see
`examples/security/jaas.conf.example`); that one section also authenticates BookKeeper's internal
ZooKeeper client. ZooKeeper client TLS uses ZooKeeper's standard system properties via
`CANDYBOX_EXTRA_OPTS` (`-Dzookeeper.client.secure=true ...`).

### Candybox → BookKeeper

Everything goes through the verbatim `bookkeeper.client.*` passthrough (BK's own camelCase keys —
via env, the suffix keeps its case: `CANDYBOX_BOOKKEEPER_CLIENT_tlsProvider`):

```properties
bookkeeper.client.clientAuthProviderFactoryClass=org.apache.bookkeeper.sasl.SASLClientProviderFactory
bookkeeper.client.tlsProvider=OpenSSL
bookkeeper.client.tlsTrustStoreType=PEM
bookkeeper.client.tlsTrustStore=/etc/candybox/tls/ca.crt
```

The bookie side (`bookieAuthProviderFactoryClass`, bookie TLS, the bookies' own ZooKeeper auth) is
configured in each bookie's `bookkeeper.conf` per the BookKeeper security docs — Candybox only
brings the client half.

### Admin API & metrics endpoints

Set `CANDYBOX_ADMIN_AUTH_TOKEN` on the admin API: every `/api/*` route (cluster state, box
browsing, uploads) then demands `Authorization: Bearer <token>`; `/healthz`, `/readyz` and the
static SPA shell stay open. The dashboard captures the token once from `/ui/?token=<token>`
(stored in localStorage, stripped from the URL; `/ui/?token=` clears it). Give the admin API an
explicit `CANDYBOX_ADMIN_CORS` origin instead of `*` when it serves anything but same-origin.
The listener serves HTTPS when the `tls.*` keys are set in its environment.

Node and gateway `/metrics` accept an optional guard: set `metrics.auth.token` (or
`CANDYBOX_METRICS_AUTH_TOKEN`) on the node/gateway, and give the admin API's scraper the same
value via `CANDYBOX_ADMIN_SCRAPE_TOKEN` (Prometheus: `authorization.credentials`). The
`/healthz`/`/readyz` probes stay unauthenticated plain-HTTP — they reveal only a boolean, and
Kubernetes probes them directly.

### Ledger password vs. authentication

`ledger.password` predates all of the above: it is BookKeeper's per-ledger access password, a
shared secret among the candybox nodes — keep it secret, but do not mistake it for user
authentication.

## Known limitations / deferred work

- **Streaming on the wire (Phase 2.5):** large objects are buffered in a single frame (≤ 16 MiB);
  chunked multi-frame PUT/GET is not yet implemented.
- **Automatic failover needs the balancer:** with `balancerIntervalMillis = 0`, a crashed owner's
  partitions are served again only when some node calls `openPartition`/`openBox`.
- **Fixed partition count per Box:** set at creation, no re-partitioning; ordered listings
  scatter-gather every partition per page.
- **Distributed compaction offload:** the owner produces and commits its own compactions; offloading
  output production to non-owners under a ZK task lease is future work.
- **GC enumeration backstop** and **Syrup defragmentation** (see above).
- **Real multi-bookie chaos testing:** fault injection is covered deterministically on the in-memory
  fakes; a Jepsen-style harness against a real cluster is future work.
