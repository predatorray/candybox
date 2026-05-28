# Candybox — Operations Guide

How to run, tune, and reason about a Candybox cluster, plus its failure modes and current limitations.
For architecture and on-ledger formats see [`DESIGN.md`](DESIGN.md); for build/test see [`README.md`](README.md).

## Topology

A Candybox cluster is a set of **storage nodes** over a shared **Apache BookKeeper** ensemble and a
shared **ZooKeeper** ensemble:

- **BookKeeper** stores all ledgers — WALs, SSTables, Syrups (Candy bytes), and the per-Box manifest.
- **ZooKeeper** (via the coordination SPI) holds cluster membership, per-Box ownership **leases**, and
  the versioned pointer to each Box's current manifest ledger.
- Each **Box** is owned by exactly one node at a time (a fenced, movable lease). The owner is the sole
  writer of that Box's WAL, memtable flushes, and manifest. Clients route requests to the owner.

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
| `ownershipLeaseTtlMillis` | 10 s | Box ownership lease TTL; must be renewed within it. |
| `leaseRenewIntervalMillis` | 3 s | Lease heartbeat interval; `0` disables the background heartbeat. |
| `routerCacheTtlMillis` | 5 s | Client Box→owner routing-cache TTL. |
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

- **Per-key linearizable on the owner.** A single fenced owner serializes all writes for a Box and
  stamps a Hybrid Logical Clock at ingest; reads resolve to the highest HLC (LWW, nodeId tiebreaker).
  The only eventual-consistency window is ownership handover.
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

Recovery currently requires some node to take over an unowned Box (`CandyboxNode.openBox`); automatic
failover (watch lease loss → elect a new owner) is future work.

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
context. A metrics-system exporter is not yet wired.

## Known limitations / deferred work

- **Streaming on the wire (Phase 2.5):** large objects are buffered in a single frame (≤ 16 MiB);
  chunked multi-frame PUT/GET is not yet implemented.
- **Automatic failover:** a crashed owner's Box is served again only when some node calls `openBox`.
- **Cluster-wide `listBoxes`:** returns the contacted node's Boxes only.
- **Distributed compaction offload:** the owner produces and commits its own compactions; offloading
  output production to non-owners under a ZK task lease is future work.
- **GC enumeration backstop** and **Syrup defragmentation** (see above).
- **Real multi-bookie chaos testing:** fault injection is covered deterministically on the in-memory
  fakes; a Jepsen-style harness against a real cluster is future work.
