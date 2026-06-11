# Box Partitioning — Implementation Plan

## Goal

Remove the single-node write bottleneck per Box. A Box is split into a **fixed number of hash
partitions**; each partition is an independent LSM engine (its own WAL / memtable / manifest /
SSTables / Syrups) with its own ZooKeeper-fenced ownership lease, so the partitions of one Box are
owned and served by **multiple nodes concurrently**. An elected coordinator spreads partition
ownership evenly across the cluster (rate-limited when moving partitions away from live owners), and
clients route each request to the owner of the key's partition, re-routing on `MOVED`.

Decisions locked with the project owner:

1. **Hash partitioning** (CRC32C of the key's UTF-8 bytes, mod partition count).
2. **Partition count fixed at Box creation**, with a configurable cluster default (8).
3. **Elected coordinator** computes the desired assignment; every node acts on it.
4. **Rate-limited migration** — at most `balancer.max.moves.per.round` partitions are moved away
   from live owners per balancing round (configurable).
5. Cross-partition `copy`/`rename` degrade to a **client-side byte copy** (rename additionally
   deletes the source afterwards, i.e. it is no longer atomic across partitions). Same-partition
   copy/rename keep the zero-copy server-side path.
6. **No migration / backward compatibility** with the pre-partitioning ZK layout or wire format
   (not in production yet).
7. Scope is **write scaling only** — reads still go to each partition's owner; compaction still
   runs on each partition's owner.

## Design

### Consistency

Unchanged in substance: every key maps to exactly one partition, each partition has exactly one
fenced owner, so there is still a single fenced writer per key — per-key linearizability on the
owner, HLC/LWW semantics, and the handover replay + `observe` sequence all carry over verbatim at
partition granularity. What changes is *which* keys share a writer.

Operations that used to be one engine call under one lock and now span partitions are weaker, by
design:

- `deleteRange` / `deleteRangeByPrefix` — one range tombstone **per partition** (fanned out by the
  client); not atomic across partitions, but idempotent and LWW-safe to retry.
- cross-partition `rename` — copy then delete; a crash in between leaves both keys live.
- `listCandies` / `listMultipartUploads` — scatter-gather merge in the client; each partition's
  page is internally consistent, the merged page is not a snapshot.

### Coordination layout (ZooKeeper)

```
boxes/<box>/meta                      versioned KV: BoxDescriptor {formatVersion, partitionCount}
boxes/<box>/partitions/<p>/owner      ownership lease of partition p (fenced, TTL'd) — was boxes/<box>/owner
boxes/<box>/partitions/<p>/manifest   manifest-ledger pointer of partition p   — was boxes/<box>/manifest
cluster/balancer                      the coordinator election lease
cluster/assignment                    versioned KV: desired PartitionAssignment table
members/<nodeId>                      unchanged
```

The Box descriptor is immutable after creation (the partition count cannot change in v1) and is the
routing source of truth for servers and clients. `CoordinationService` gains one operation,
`children(path)` (ZK `getChildren`; prefix scan in the in-memory fake), used to enumerate Boxes for
the balancer and for cluster-wide `listBoxes`.

### Partition mapping

`Partitioning.partitionOf(key, n) = (crc32c(utf8(key)) & 0x7fffffff) % n` — deterministic across
JVMs/nodes/clients (CRC32C is already a dependency-free primitive in `candybox-common`).

### Ownership & engines (server)

- `BoxOwnership` becomes `PartitionOwnership` — the same lease + recover + pointer-CAS sequence,
  keyed by `(box, partition)` and using the per-partition ZK keys. `BoxEngine` is reused as the
  per-partition engine unchanged (it is already fully self-contained).
- `CandyboxNode` keeps a `ConcurrentMap<(box,partition), PartitionOwnership>`:
  - `createBox(box, partitionCount)`: CAS-create `boxes/<box>/meta` (conflict ⇒
    `BoxAlreadyExists`), then create all partitions' engines locally (the balancer spreads them
    afterwards). Partial failure ⇒ best-effort rollback of created partitions + the descriptor.
  - `openPartition` / `releasePartition`: per-partition takeover and graceful release (release
    flushes the memtable first so the next owner's WAL replay is small).
  - `engine(box, key)`: descriptor → `partitionOf(key)` → locally owned engine or throw.
  - `deleteBox`: takes over every partition it does not own, checks emptiness across all
    partitions (unless `force`), deletes each manifest pointer, then the descriptor.
  - `boxExists`/`listBoxes` answer from coordination (descriptor existence / `children("boxes")`),
    so any node can answer and listing is finally cluster-wide.
  - Lease heartbeat, compaction, GC, and the multipart TTL sweeper iterate partitions —
    mechanical.

### Assignment & balancing (new)

`PartitionBalancer`, scheduled on **every** node (`balancer.interval.millis`, 0 disables):

1. **Coordinate** — try to acquire/renew the `cluster/balancer` lease; the holder enumerates all
   `(box, partition)` pairs and live members, computes a target assignment, and CAS-writes it to
   `cluster/assignment`:
   - capacity = ⌈partitions / members⌉; current live holders keep their partitions up to capacity
     (stickiness — no gratuitous shuffling);
   - unowned partitions (new Box, dead node, over-capacity overflow) go to the least-loaded
     members;
   - at most `balancer.max.moves.per.round` partitions whose current holder is **live** are
     reassigned per round (rate-limited migration); partitions with no live holder are failover,
     not moves, and are never rate-limited.
2. **Apply** — every node reads the assignment table:
   - owned here but assigned elsewhere ⇒ flush + release (the handover drain);
   - assigned here but not owned ⇒ acquire once the lease is free (the previous owner released or
     expired), via the normal fenced recover path.

Convergence is via polling rounds: a move takes one round for the old owner to release and the next
round (or the same one, ordering permitting) for the new owner to acquire. Fencing safety never
depends on the balancer — it only decides who *tries* to acquire.

### Protocol

No back-compat constraints, so fields are added in place (no version bump):

- `CreateBoxRequest` gains `partitionCount` (0 ⇒ server default).
- `ListCandiesRequest`, `DeleteRangeRequest`, `ListMultipartUploadsRequest` gain `partition` — the
  client fans these out per partition.
- New `BoxInfoRequest(box)` → `BoxInfoResponse(partitionCount)`: served from the descriptor by any
  node; the client uses it to learn (and cache) a Box's partition count.
- `MovedResponse(ownerNodeId)` is unchanged — the client already knows which partition it asked
  for.
- Keyed requests are unchanged on the wire; the server derives the partition from the key.

### Client routing

- `Router.callBox(box, …)` becomes `callPartition(box, partition, …)`. `ClusterRouter` resolves
  `boxes/<box>/partitions/<p>/owner` → member address, caches per `(box, partition)` with the
  existing TTL, and re-routes on `MOVED` exactly as before. `DirectRouter` ignores the partition
  (single-node).
- `CandyboxClient` caches each Box's partition count (immutable ⇒ cache forever; invalidated on
  Box deletion/not-found) and:
  - routes keyed ops (put/get/head/delete/range-get and **all multipart ops** — every multipart
    request already carries the object key) to `partitionOf(key)`;
  - fans out `deleteRange`/`deleteRangeByPrefix` to every partition;
  - scatter-gathers `listCandies` (merge by unsigned-UTF-8 key order, honoring reverse; the
    `lastKey` continuation token still works — each page re-fans-out) and
    `listMultipartUploads` (merge by `(key, uploadId)`);
  - same-partition copy/rename/uploadPartCopy stay server-side zero-copy; cross-partition fall
    back to get+put (+delete for rename) / range-get+uploadPart.

### Configuration

| Key | Default | Meaning |
|---|---|---|
| `partitions.per.box.default` | 8 | Partition count for `createBox` when the caller passes 0. |
| `balancer.interval.millis` | 0 (off) | Balancing round period on every node; enabled in the shipped conf (5000). |
| `balancer.max.moves.per.round` | 4 | Max partitions moved away from live owners per round. |

The default partition count of 8 bounds per-Box resource amplification (each partition is a full
engine: WAL + manifest ledgers, a 4 MiB memtable, an open Syrup) while spreading writes across up
to 8 nodes. The balancer interval defaults to 0 in `CandyboxConfig` so the in-JVM tests retain
deterministic, manually-driven ownership; the packaged `candybox.properties` enables it.

## Work items

1. **common** — `Partitioning` (hash), `CandyboxConfig` keys (`partitionsPerBoxDefault`,
   `balancerIntervalMillis`, `balancerMaxMovesPerRound`).
2. **coordination** — partitioned `CandyboxKeys` + `cluster/*` keys; `BoxDescriptor`
   encode/decode; `CoordinationService.children` in the fake, ZK impl, and the shared contract
   test.
3. **protocol** — message/codec changes above + codec tests.
4. **server** — `PartitionOwnership`; `CandyboxNode` partition map, descriptor cache,
   create/open/release/delete per partition, coordination-backed `boxExists`/`listBoxes`;
   `PartitionAssignment` table; `PartitionBalancer` (coordinate + apply, rate-limited);
   `NodeRequestHandler` partition resolution and per-partition `MOVED`; `ServerConfig` keys.
5. **client** — `Router.callPartition`, `ClusterRouter` per-partition resolution/caching,
   `CandyboxClient` partition-count cache, fan-out list/delete-range/list-uploads merge,
   cross-partition copy/rename/uploadPartCopy fallback, `createBox(box, partitionCount)`.
6. **tests** — update coordination contract, codec, server (handover/node/handler), client
   (router/client) tests for the partitioned layout; new tests: `Partitioning` determinism +
   spread, `BoxDescriptor` round-trip, `PartitionBalancer` (even spread, failover, rate limit,
   stickiness, non-coordinator no-op), cross-partition list/delete-range/copy/rename through the
   loopback transport, per-partition `MOVED` re-routing.
7. **docs/conf** — DESIGN.md (consistency §3, ownership §7, new partitioning section, drop the
   §12 "no sub-Box partitioning" simplification), shipped `candybox.properties` examples,
   OPERATIONS.md note on the balancer.

Out of scope (unchanged v1 simplifications): read scaling off owners, cross-node compaction
scheduling, watch-based coordination (the balancer polls), Syrup defragmentation, streaming.
