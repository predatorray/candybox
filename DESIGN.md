# Candybox — Design

Candybox is a distributed, S3-like object store whose storage engine is a distributed LSM tree built
on **Apache BookKeeper** ledgers. This document captures the architecture, on-ledger/record formats
(with their version bytes), size limits, the compaction model, the manifest/GC design, the local
decisions made (with rationale), and the deliberate v1 simplifications.

It is written against the implementation in this repository: **Phase 0 and Phase 1 are implemented and
tested; Phases 2–4 are scaffolded** with interfaces, message types, working cores where cheap, and
`// TODO(phase-N)` markers.

## 1. Domain glossary

| Term | S3 analogue | Meaning |
|---|---|---|
| **Box** | Bucket | A named container of Candies (`BoxName`, 3–63 chars `[a-z0-9-]`). |
| **Candy** | Object | An immutable blob + user metadata, addressed by a **CandyKey** within a Box. |
| **CandyKey** | Object key | Within-Box UTF-8 key; ordered by **unsigned UTF-8 byte** order everywhere. |
| **CandyLocator** | — | The small LSM *value*: a pointer to where a Candy's bytes live, plus metadata. |
| **Syrup** | — | A BookKeeper ledger holding raw Candy bytes, chunked across entries. |
| SSTable / memtable / WAL / manifest | — | Conventional LSM names. |

## 2. Architecture & module map

A cluster of **storage nodes** owns writes, reads, and compaction; a **thin client** routes requests
to nodes over a minimal framed TCP protocol. Every external dependency sits behind a narrow SPI with
an in-memory fake, so the whole engine is testable without BookKeeper, ZooKeeper, or sockets.

```
candybox-common         Domain types, versioned serialization, HLC, config, CRC32C, bloom filter.
candybox-bookkeeper     LedgerStore SPI + in-memory fake + real BookKeeper impl. (only module touching raw BK)
candybox-coordination   Coordination SPI (membership, leases w/ fencing tokens, CAS kv) + in-memory fake + ZK scaffold.
candybox-lsm            Memtable, WAL, SSTable, Syrup chunking, manifest, merge/read path, compaction. Depends only on the two SPIs.
candybox-protocol       Framed TCP codec + message types + Transport SPI (TCP impl + loopback fake).
candybox-server         Storage node: wires LSM behind the protocol; compaction service; GC scaffold.
candybox-client         Thin client over Transport.
candybox-integration-tests  Embedded BookKeeper (LocalBookKeeper, bundles in-JVM ZooKeeper) end-to-end tests.
```

Dependency rule: `candybox-lsm` depends **only** on the two SPIs (`bookkeeper`, `coordination`) and
`common`; it never sees the raw BookKeeper or ZooKeeper clients.

## 3. Consistency model (locked decision)

- Every mutation carries a **Hybrid Logical Clock (HLC)** timestamp `(physicalMillis, logicalCounter,
  nodeId)`. Reads resolve a key to the **highest HLC** (LWW); the deterministic final tiebreaker is
  `nodeId`. HLC beats a raw wall clock (a fast clock cannot permanently win or resurrect deletes) and
  beats a global counter (no coordination bottleneck).
- **Leased single-owner-per-Box.** At any instant one node owns a Box and is the sole writer of its
  WAL, memtable flushes, and manifest. Ownership is a movable, ZK-leased, **fenced** assignment.
- **The owner stamps the HLC at ingest** (never the client), so client clock skew is irrelevant.
- Because a single fenced owner serializes all writes for a Box, the system is **effectively per-key
  linearizable on the owner**. The eventual-consistency window is confined to ownership handover (a
  fenced old owner may briefly serve a stale memtable read, but its *writes* fail). This is the
  guarantee Candybox documents — not general eventual consistency.

### HLC recovery on handover (critical correctness point)

`HybridLogicalClock.tick()` is monotonic even if the wall clock regresses (it bumps the logical
counter at the frozen physical time). On handover the new owner **replays the prior WAL/manifest,
observes the maximum durable HLC, and advances its clock past it (`observe`) before stamping anything.**
Skipping this would let a newer write get a *lower* HLC and be silently dropped by the read path.
Covered by `BoxEngineTest.handoverWithRegressedClockDoesNotLoseLatestWrite` (fake) and
`Phase1EngineIT.unflushedWritesAreRecoveredFromTheWal` (real BookKeeper). Because the WAL is rotated
on every flush, the current WAL always holds the most-recent mutations, so its max HLC dominates the
flushed SSTables — observing the WAL max suffices.

## 4. Ledger roles & lifecycle

Three distinct ledger roles, kept separate in code:

- **WAL ledger** — per-Box write-ahead log. Each entry is a serialized `Mutation`, appended (ack-quorum
  durable) before the memtable acknowledges. On handover the new owner uses **recover-open** (fence +
  seal at a deterministic LAC), *not* a plain read-open, so a resurrected old owner cannot keep
  appending. Rotated to a fresh ledger on each flush.
- **SSTable ledger** — an immutable sorted run from a flush or compaction (see §6 format).
- **Syrup (data) ledger** — raw Candy bytes, fixed-size chunks (one chunk = one entry), each with its
  own crc32c. One Syrup fills across many Candies and rolls to a new one at a size cap; a Candy that
  straddles a rollover is described by multiple `SegmentRef`s.
- **Manifest ledger** — append-only LSM metadata (Pulsar managed-ledger style); see §7.

Integrity: WAL / SSTable-block / manifest entries rely on **BookKeeper's per-entry digest** (CRC32C).
App-level CRC is used only for **end-to-end** Candy validation: a per-chunk crc on each Syrup entry
(partial validation on streaming/retry) and a whole-object crc in the locator (checked on `getCandy`).

## 5. Record formats (all versioned)

Every persisted record begins with a **format version byte** for forward compatibility. Encoding is
big-endian via `BinaryWriter`/`BinaryReader`; length prefixes and small counts use unsigned LEB128
varints, and every decode is bounds-checked (a corrupt buffer cannot over-read or over-allocate).

**CandyLocator** (`CandyLocatorSerializer`, version 1), the LSM value, capped at 64 KiB:
```
byte    formatVersion (=1)
byte    type (1=PUT, 2=DELETE tombstone)
long    hlc.physicalMillis
varint  hlc.logicalCounter
int     hlc.nodeId
varlong contentLength
varint  chunkSize
int     crc32c (whole-object)
varlong createdAtMillis
byte    contentType present? [+ string]
varint  userMetadata count  [+ {string key, string value}]
varint  segment count       [+ {varlong syrupId, varlong firstEntryId, varlong lastEntryId}]
```
The segment list is O(number of Syrups), not O(number of chunks), thanks to fixed `chunkSize` +
contiguous entries. A DELETE tombstone carries no segments and zero length.

**Mutation** (`MutationSerializer`, version 1) — WAL entry and SSTable record: `version | bytes(key) |
bytes(serialized CandyLocator)`.

**Syrup chunk entry**: `int crc32c | payload[<= chunkSize]`. The crc covers the payload only.

**ManifestEdit** (`ManifestSerializer`, version 1): `version | addedTables[] | removedTableLedgerIds[]
| addedSyrups[] | removedSyrups[] | (bool, varlong) newWalLedgerId`, where each `SSTableMeta` is
`varlong ledgerId | varint level | bytes minKey | bytes maxKey | varlong entryCount`.

**Protocol frame** (`FrameCodec`): `magic(2)=0xCB0F | version(1)=1 | opcode(1) | length(4) | payload`.
**Message body** (`MessageCodec`): `bodyVersion(1) | <per-opcode fields>`.

### SSTable on-ledger layout (`SSTableFormat`, footer version 1)

```
entry 0 .. B-1   data blocks   (each: varint count + [varint len, Mutation bytes]*, key-ascending)
entry B          bloom block   (serialized BloomFilter over all keys)
entry B+1        index block   (varint count + [bytes lastKey, varlong dataBlockEntryId]*)
entry B+2 (=LAC) footer        (int magic=0x53535442 | byte version | varlong bloomEntryId |
                                varlong indexEntryId | varint numDataBlocks | varlong numEntries |
                                bytes minKey | bytes maxKey)
```
One block ⇒ one ledger entry; the data-block size target is ~64 KiB. The reader finds the footer at
`lastAddConfirmed()`, loads the index + bloom eagerly, and reads data blocks lazily. Point lookups
consult the bloom filter, binary-search the index, then scan one block; range scans iterate blocks
from the one containing the start key. Within a single SSTable each key appears once (the memtable and
merge de-duplicate by LWW), so the file holds unique, ascending keys.

## 6. Read / write / merge path (Phase 1)

- **put**: validate → stream bytes into Syrups (`SyrupManager`) → stamp HLC → build `CandyLocator` →
  append to WAL → apply to memtable (LWW). Auto-flush when the memtable exceeds its byte threshold.
- **delete**: stamp HLC → write a DELETE tombstone the same way.
- **flush**: seal the memtable into a new L0 SSTable, record referenced Syrups + the rotated WAL id in
  one `ManifestEdit`, open a reader for the new table.
- **get/head**: resolve the key to the highest-HLC locator across the memtable and overlapping L0
  SSTables (range + bloom pre-filter); a tombstone or absence ⇒ `CandyNotFound`. `getCandy` then
  streams bytes from Syrups and validates the whole-object crc.
- **list**: a `MergingIterator` over the memtable + SSTables (key-ascending, LWW, tombstones
  suppressed), honoring prefix/startAfter/maxKeys and returning a continuation cursor (`lastKey`).

Phase 1 keeps an **L0-only** merged read path (honest scope); the multi-level structure lands with
compaction. Under write-stall (too many L0 SSTables, `l0StallThreshold`) writes return a retriable
`BUSY`.

## 7. Manifest, fencing & ownership

The manifest is an **append-only metadata ledger**; a Box's manifest has exactly one writer (the
owner). `ManifestLog` appends serialized edits; `Manifest` keeps an in-memory `ManifestState` in
lock-step (append durably, then advance state). Handover (`Manifest.recover`):

1. **recover-open** (fence + seal) the prior manifest ledger;
2. replay its edits to rebuild `ManifestState`;
3. open a **fresh** manifest ledger (a sealed BK ledger can't be appended) and seed it with a
   self-contained checkpoint of the recovered state.

In production the ZK pointer to the current manifest ledger is advanced with a **compare-and-set on
the expected ZK version** (never a blind set), and every state-mutating append carries the owner's
**fencing token** (ZK lease version). A zombie former owner's appends fail because its ledger was
recover-open-fenced — see `ManifestTest.fencedOwnerCannotCommitFurtherEdits`. The `CoordinationService`
fake models lease expiry, supersession, and CAS conflicts so these tests are not vacuous
(`InMemoryCoordinationServiceTest`).

## 8. Compaction model

`CompactionStrategy` is a pluggable SPI (Cassandra-style) with **LevelDB-style leveled compaction** as
the default (`LeveledCompactionStrategy`): L0 reaching the trigger count merges all L0 (+ overlapping
L1) into L1; a level L≥1 over its table budget merges a table (+ overlapping L+1) into L+1. The
`Compactor` opens the inputs, merges by LWW, writes the output SSTable, and returns the `ManifestEdit`
to commit. `CompactionService` (server) runs one step end-to-end today; distributed scheduling via ZK
task claims/leases and the fencing-gated commit are **TODO(phase-3)**.

**Tombstone-drop rule** (LevelDB + late-write window): a DELETE is dropped only when the compaction is
at the **bottommost** level holding overlapping data **and** the tombstone is older than the configured
GC grace; otherwise it is preserved. Dropping early would resurrect deleted Candies. Covered by
`CompactionTest`.

## 9. Garbage collection (design; Phase 3)

Reference-counted GC of obsoleted ledgers, run **only by the Box's manifest owner against a committed
manifest snapshot** (never a stale tail), gated on the owner's fencing token, after a grace period
(Pulsar-style):

- (a) deletion edit + physical delete are fenced;
- (b) a tombstone may be dropped only under the bottommost + time-bound rule (§8);
- (c) orphaned-Syrup GC is driven by a **pending-orphan list** recorded at supersede/dedupe time (a
  retried or conflicting put knows its losing segments immediately); a full live-locator scan is only
  a backstop;
- (d) **v1 Syrup reclamation policy**: because BookKeeper deletes whole ledgers only, a single live
  Candy pins an otherwise-dead Syrup. **v1 documents and accepts this** — a Syrup is reclaimed only
  once *every* segment in it is dead. Syrup defragmentation (copying survivors into a fresh Syrup) is
  deferred. See `GarbageCollector` (scaffold).

## 10. Size limits (configurable; enforced at client and node)

| Limit | Default | Where |
|---|---|---|
| `chunkSize` (Syrup entry payload) | 1 MiB | < BK max frame minus overhead |
| Max CandyKey length | 1 KiB UTF-8 | client + node |
| Box name | 3–63 chars `[a-z0-9-]` | `BoxName` |
| Max user-metadata total | 8 KiB | inside locator |
| Max CandyLocator serialized size | 64 KiB hard cap | `CandyLocatorSerializer` |
| Max Candy size | unlimited (`maxCandySizeBytes=0`) | configurable |
| Max protocol frame | 16 MiB | `FrameCodec`, rejected before allocation |

Per-role BK quorum (E/Qw/Qa) defaults: **WAL 3/3/2, Manifest 3/3/2, SSTable 3/2/2, Syrup 3/2/2**
(`QuorumConfig`).

## 11. Local decisions & rationale (`CandyboxConfig` defaults)

| Decision | Choice | Rationale |
|---|---|---|
| Serialization | Manual big-endian + LEB128 varints, version byte per record | Compact, dependency-free, forward-compatible, bounds-checked. |
| SSTable block→entry | One block per ledger entry; ~64 KiB block target; footer at LAC | Simple, lets the reader find the footer with no side metadata. |
| Bloom filter | 10 bits/key, k≈7, LevelDB hash | ~1% FPR; deterministic decode across nodes/arch. |
| WAL granularity | Per-Box | Matches single-owner-per-Box; simplest correct recovery unit. |
| Memtable structure | `ConcurrentSkipListMap`, LWW merge | Sorted, lock-free reads during the flush scan. |
| Manifest checkpoint | On handover (fresh ledger seeded with full-state checkpoint) | Bounds replay; aligns with "can't append a sealed ledger". |
| Syrup rollover | 1 GiB | Bounds per-ledger size; large objects span multiple Syrups. |
| HLC logical width / skew bound | 32-bit logical; 5-min skew rejection | Overflow needs ~2.1e9 events/ms; bound stops a wild clock dragging us forward. |
| Ownership lease TTL / fencing token | 10 s; monotonic per-resource counter (ZK version) | Short enough for quick failover; strictly increasing tokens fence zombies. |
| Tombstone-GC time bound | 24 h | Covers in-flight late writes before a delete is reclaimable. |
| Continuation token | `lastKey` (+ advisory manifest-version, Phase 2) | `lastKey` alone resumes a range scan; version stays advisory so GC isn't blocked. |
| LWW tiebreaker | `nodeId` (locked) | Deterministic, coordination-free. |
| TCP opcodes | incl. dedicated `RESPONSE_BUSY` | Backpressure is a first-class, retriable signal. |

## 12. Deliberate v1 simplifications (escape hatches)

- **Single owner per Box, no sub-Box partitioning** — caps a Box's write throughput at one node and
  makes it unavailable during the handover fence+replay window. Manifest/ownership structures are kept
  **partition-shaped** (one partition per Box now) so key-range tablets can be added later.
- **Reads served by the Box owner** — simple and correct. Sealed SSTables/Syrups are immutable and
  replicated, so any node *could* serve reads of flushed data; only unflushed-memtable read-your-writes
  needs the owner. Not built in v1.
- **No small-object inlining** — all bytes go to Syrups (an extra BK round trip + fragmentation for
  tiny Candies). The 64 KiB locator cap leaves room to inline small bytes later.
- **No Syrup defragmentation, no multipart upload** — see §9(d); large-object streaming over the wire
  and resumable writes are future work (client currently buffers).

## 13. Open questions / future work

- Phase 2: ZooKeeper-backed coordination (`ZooKeeperCoordinationService`), the manifest ZK pointer with
  versioned CAS, cluster membership + Box assignment/routing, full server wiring, and chunked
  PUT/GET bodies on the wire (today the protocol inlines bytes for small objects).
- Phase 3: distributed compaction scheduling (ZK task claims/leases, fenced commit) and the GC engine.
- Phase 4: fault-injection/hardening, broader recovery paths, operational docs.
- Reading flushed data from any replica (read scaling) once ownership/routing exists.
