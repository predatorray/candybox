# Candybox — Design

Candybox is a distributed, S3-like object store whose storage engine is a distributed LSM tree built
on **Apache BookKeeper** ledgers. This document captures the architecture, on-ledger/record formats
(with their version bytes), size limits, the compaction model, the manifest/GC design, the local
decisions made (with rationale), and the deliberate v1 simplifications.

It is written against the implementation in this repository: **Phases 0–4 are implemented and
tested** — the LSM engine, ZooKeeper-backed coordination, fenced Box ownership/handover, the framed
TCP protocol with cluster membership + client-side routing, multi-level leveled compaction, and
reference-counted GC all run, with the storage node, CLI, and a packaged distribution (Docker /
Kubernetes) on top. A handful of deliberate v1 simplifications remain (true on-the-wire streaming,
distributed cross-node compaction scheduling, watch-based coordination, a GC enumeration backstop);
they are called out inline and collected in §12–§13, and a few are still marked `// TODO(phase-N)` in
code.

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
candybox-coordination   Coordination SPI (membership, leases w/ fencing tokens, CAS kv) + in-memory fake + real ZooKeeper impl.
candybox-lsm            Memtable, WAL, SSTable, Syrup chunking, manifest, merge/read path, compaction. Depends only on the two SPIs.
candybox-protocol       Framed TCP codec + message types + Transport SPI (TCP impl + loopback fake).
candybox-server         Storage node: wires LSM behind the protocol; fenced Box ownership + handover; background compaction + GC workers; health/metrics; runnable entrypoint.
candybox-client         Thin client over Transport, cluster-aware router, and the `candybox` command-line tool.
candybox-dist           Packages the runnable distribution (`bin/ lib/ conf/`) + the Docker/Kubernetes assets.
candybox-integration-tests  Embedded BookKeeper (LocalBookKeeper, bundles in-JVM ZooKeeper) end-to-end + contract ITs.
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

**CandyLocator** (`CandyLocatorSerializer`, **version 2**), the LSM value, capped at 256 KiB:
```
byte    formatVersion (=2)
byte    type (1=PUT, 2=DELETE tombstone)
long    hlc.physicalMillis
varint  hlc.logicalCounter
int     hlc.nodeId
varlong createdAtMillis
byte    contentType present? [+ string]
varint  userMetadata count   [+ {string key, string value}]
varint  part count           [+ Part records]
  Part:
    varlong partLength
    varint  chunkSize
    int     crc32c (per-part end-to-end)
    varint  segment count    [+ {varlong syrupId, varlong firstEntryId, varlong lastEntryId}]
```
The locator is a list of *parts*. A single-PUT or `copy/rename` produces a one-element list; a
multipart-uploaded Candy stitches its parts together in part-number order. Each Part is internally
uniform-chunked with its own end-to-end CRC32C — short-tail chunks therefore only sit at part
boundaries, where the read path knows to look (see `MULTIPART_RANGE_PLAN.md`). A DELETE tombstone
carries an empty parts list. The 256 KiB cap accommodates a 10,000-part multipart Candy
(~200 KiB).

**Mutation** (`MutationSerializer`, version 1) — point WAL record and SSTable record: `version |
bytes(key) | bytes(serialized CandyLocator)`.

**RangeTombstone** (`RangeTombstoneSerializer`, version 1) — the `deleteRange` marker: `version |
nullable bytes(startInclusive) | nullable bytes(endExclusive) | long physicalMillis | varint
logicalCounter | int nodeId`. Carries no Syrup segments; shadows every key in `[start, end)` whose
locator HLC is older.

**WAL entry** (`WalEntrySerializer`) — the WAL is a kind-tagged log so point mutations and range
deletes replay in one ordered pass: `byte kind (1=point mutation, 2=range delete) | <Mutation or
RangeTombstone payload>`. Replay reports the max HLC across **both** kinds, so handover's HLC
`observe` cannot lose a range delete.

**Syrup chunk entry**: `int crc32c | payload[<= chunkSize]`. The crc covers the payload only.

**ManifestEdit** (`ManifestSerializer`, **version 2**): `version | addedTables[]
| removedTableLedgerIds[] | addedSyrups[] | removedSyrups[] | (bool, varlong) newWalLedgerId
| ownerFencingToken | addedUploads[] | upsertParts[] | removedUploads[]`, where each `SSTableMeta`
is `varlong ledgerId | varint level | bytes minKey | bytes maxKey | varlong entryCount`. The
multipart-tracking trailing fields (Phase 5) carry in-flight upload state: a `MultipartUploadState`
record per `addedUpload`, a `(uploadId, partNumber, Part)` triple per `upsertParts`, and a string
per `removedUpload`. Replayed on handover so a takeover sees the upload exactly where the prior
owner left it.

**Protocol frame** (`FrameCodec`): `magic(2)=0xCB0F | version(1)=1 | opcode(1) | length(4) | payload`.
**Message body** (`MessageCodec`): `bodyVersion(1) | <per-opcode fields>`.

### SSTable on-ledger layout (`SSTableFormat`, footer version 2)

```
entry 0 .. B-1   data blocks    (each: varint count + [varint len, Mutation bytes]*, key-ascending)
entry B          bloom block    (serialized BloomFilter over all keys)
entry B+1        index block     (varint count + [bytes lastKey, varlong dataBlockEntryId]*)
entry [B+2]      range-del block (v2, optional: varint count + [bytes RangeTombstone]*, by start)
entry (=LAC)     footer          (int magic=0x53535442 | byte version | varlong bloomEntryId |
                                  varlong indexEntryId | varint numDataBlocks | varlong numEntries |
                                  bytes minKey | bytes maxKey | bool hasRangeDel [+ varlong entryId])
```
One block ⇒ one ledger entry; the data-block size target is ~64 KiB. The reader finds the footer at
`lastAddConfirmed()`, loads the index + bloom + range tombstones eagerly, and reads data blocks
lazily. Point lookups consult the bloom filter, binary-search the index, then scan one block; forward
scans iterate blocks from the one containing the start key, reverse scans walk blocks high-to-low.
Within a single SSTable each key appears once (the memtable and merge de-duplicate by LWW), so the
file holds unique, ascending keys. **Footer v1 (no range-del block) is still readable**; v2 adds the
optional range-tombstone block, and a table may be *range-only* (zero data blocks). A table's
`minKey/maxKey` bound its point keys; range tombstones can reach beyond them, so the read path
consults range tombstones across all tables rather than pruning by point range.

## 6. Read / write / merge path

- **put**: validate → stream bytes into Syrups (`SyrupManager`) → stamp HLC → build `CandyLocator` →
  append to WAL → apply to memtable (LWW). Auto-flush when the memtable exceeds its byte threshold.
- **delete**: stamp HLC → write a DELETE tombstone the same way.
- **flush**: seal the memtable into a new L0 SSTable, record referenced Syrups + the rotated WAL id in
  one `ManifestEdit`, open a reader for the new table.
- **get/head**: resolve the key to the highest-HLC locator across the memtable and the overlapping
  SSTables at every level (range + bloom pre-filter); a point tombstone, a covering **range tombstone** newer than
  that locator, or absence ⇒ `CandyNotFound`. `getCandy` then streams bytes from Syrups and validates
  the whole-object crc.
- **list / scan**: a `MergingIterator` over the memtable + SSTables (LWW, tombstones suppressed),
  driven by a `ScanQuery` — an optional `[start, end)` window, optional prefix, **forward or reverse**
  direction, page size, and a continuation cursor (`lastKey`, exclusive in the scan direction). Keys
  covered by a newer range tombstone are suppressed too (the union of range tombstones across the
  memtable and all SSTables is gathered per scan; they are few).
- **deleteRange**: stamp one HLC and append a single `RangeTombstone` `[start, end)` to the WAL +
  memtable — an O(1) delete of the whole interval. Shadowed keys are reclaimed lazily at compaction.
  Either bound may be unbounded; `deleteRangeByPrefix` maps `prefix → [prefix, prefixSuccessor)`.
- **copy / rename**: write a fresh PUT at the destination reusing the source locator's *parts*
  verbatim (zero byte copy and zero locator rebuild — a multipart source becomes a multipart-shaped
  copy); `rename` also tombstones the source. Both commit atomically under the single owner's
  write lock (same Box only). Because GC is reference-counted by Syrup id, several keys may share
  one segment set safely.
- **range get**: `getCandyRange(key, firstByte, lastByte)` over HTTP `Range: bytes=…` semantics
  (inclusive on both ends). Prefix-sums the part lengths to find the first/last parts, then indexes
  into chunks by `byteWithinPart / part.chunkSize`. Chunks at the slice boundary are read whole —
  the per-chunk CRC still validates — and trimmed before emission. Multi-range
  (`bytes=A-B,C-D`) is rejected; a single absent/unparseable header falls back to a full 200 per
  RFC 9110 §14.2.
- **multipart upload**: `CreateMultipartUpload(key, contentType, userMetadata) → uploadId`,
  `UploadPart(uploadId, partNumber, body) → partCrc`, `CompleteMultipartUpload(uploadId, parts) →
  metadata`, `AbortMultipartUpload(uploadId)`, plus `UploadPartCopy` (same-Box only) and
  `ListMultipartUploads`/`ListParts`. In-flight state lives in the manifest's `multipartUploads`
  section (see §7), so it is fencing-gated, replayed on handover, and pins its parts' Syrups via
  the existing reference-count walk. `Complete` assembles a multi-part `CandyLocator` at the target
  key in one fenced manifest edit; superseded parts (re-uploading the same `partNumber`) and
  aborted uploads enter the pending-orphan path for GC. The TTL sweeper on the
  compaction/GC worker aborts uploads older than `multipart.upload.ttl.millis` (default 7 days).

The merged read path spans **all levels** (L0 plus the compaction-produced L1+); within a level keys
are non-overlapping, while L0 tables may overlap, so L0 is consulted table-by-table. Under
write-stall (too many L0 SSTables, `l0StallThreshold`) writes return a retriable `BUSY`, the
dedicated `RESPONSE_BUSY` opcode.

## 7. Manifest, fencing & ownership

The manifest is an **append-only metadata ledger**; a Box's manifest has exactly one writer (the
owner). `ManifestLog` appends serialized edits; `Manifest` keeps an in-memory `ManifestState` in
lock-step (append durably, then advance state). Handover (`Manifest.recover`):

1. **recover-open** (fence + seal) the prior manifest ledger;
2. replay its edits to rebuild `ManifestState`;
3. open a **fresh** manifest ledger (a sealed BK ledger can't be appended) and seed it with a
   self-contained checkpoint of the recovered state.

The ZK pointer to the current manifest ledger is advanced with a **compare-and-set on the expected
ZK version** (never a blind set), and every state-mutating append carries the owner's **fencing
token** (the lease version). A zombie former owner's appends fail because its ledger was
recover-open-fenced — see `ManifestTest.fencedOwnerCannotCommitFurtherEdits`. The pointer and lease
live in the `CoordinationService` SPI: an in-memory fake that models lease expiry, supersession, and
CAS conflicts (so these tests are not vacuous — `InMemoryCoordinationServiceTest`), and the real
`ZooKeeperCoordinationService`. Both run the shared `CoordinationServiceContract` (as a
`*ContractTest` against the fake and a `*ContractIT` against a live ZooKeeper), so the fast tests are
a faithful stand-in. `BoxOwnership` (server) ties the lease, fencing token, and manifest pointer
together; `CandyboxNode.createBox`/`openBox` are the take-ownership and failover/handover entry
points, with a background heartbeat renewing leases within the TTL.

## 8. Compaction model

`CompactionStrategy` is a pluggable SPI (Cassandra-style) with **LevelDB-style leveled compaction** as
the default (`LeveledCompactionStrategy`): L0 is scored by **file count** — reaching the trigger
count merges all L0 (+ overlapping L1) into L1 — while each level L≥1 has a **byte budget**
(`levelBaseBytes × levelMultiplier^(L-1)`, defaults 10 MiB × 10); the level most over budget has one
table (+ overlapping L+1) merged into L+1. The `Compactor` opens the inputs, merges by LWW, writes
the output SSTable, and returns the `ManifestEdit` to commit. `CompactionService` (server) runs one
step end-to-end; `CandyboxNode` drives it on a background worker (`compactionIntervalMillis`), running
a **bounded** number of passes per owned Box per tick so one Box can't starve the others, then a GC
pass (§9). The commit is **fencing-gated** by the manifest, so a Box whose ownership was lost
mid-round fails its commit (`FencedException`) and is skipped — a zombie owner cannot corrupt state.
Distributed *cross-node* scheduling (claiming work via ZK task leases so compaction can run off the
owner) is still **TODO(phase-3)**; today each owner compacts its own Boxes in-process. Covered by
`CompactionTest` and the end-to-end `CompactionGcCycleIT`.

**Tombstone-drop rule** (LevelDB + late-write window): a DELETE is dropped only when the compaction is
at the **bottommost** level holding overlapping data **and** the tombstone is older than the configured
GC grace; otherwise it is preserved. Dropping early would resurrect deleted Candies. The same rule
governs **range tombstones**: the compactor collects them from all inputs and carries them forward
into the output, except an aged one at the bottommost level — which is dropped together with the
(necessarily older) point locators it covers, so nothing is resurrected. Covered by `CompactionTest`.

## 9. Garbage collection (`GarbageCollector`, server)

Reference-counted GC of obsoleted ledgers, run **only by the Box's manifest owner against a committed
manifest snapshot** (never a stale tail), gated on the owner's fencing token, after a grace period
(`ledgerGcGraceMillis`, Pulsar-style — a margin for in-flight readers / continuation tokens). Three
reclaim sources, each whole-ledger-deleted via `LedgerStore.deleteLedger` (idempotent — a missing
ledger is treated as already gone), driven on the same background worker as compaction (§8):

- **SSTables** removed from the manifest by a committed compaction;
- **Syrups** no longer referenced by any SSTable, the memtable, or the open write Syrup — dropped from
  the live set first via a fencing-gated manifest edit, then deleted;
- **WAL** ledgers rotated out at flush, whose mutations are now durable in an SSTable.

The detailed rules:

- (a) the live-set-drop edit + the physical delete are fenced;
- (b) a tombstone may be dropped only under the bottommost + time-bound rule (§8);
- (c) orphaned-Syrup GC is driven by per-SSTable referenced-Syrup tracking plus a **pending-orphan
  list** recorded at supersede/dedupe time (a retried or conflicting put knows its losing segments
  immediately); the pending sets are in-memory today, so an **enumeration backstop** for ledgers
  orphaned by a *prior* owner that crashed before GC is still **TODO(phase-3+)**;
- (d) **v1 Syrup reclamation policy**: because BookKeeper deletes whole ledgers only, a single live
  Candy pins an otherwise-dead Syrup. **v1 documents and accepts this** — a Syrup is reclaimed only
  once *every* segment in it is dead. Syrup defragmentation (copying survivors into a fresh Syrup) is
  deferred;
- (e) **shared segments from copy/rename**: orphan detection counts Syrup references by actual
  `SegmentRef`s across the manifest's SSTables and the memtable, so several keys pointing at one
  segment set (a `copyCandy`, or the in-flight state of a `renameCandy`) keep that Syrup live until
  *all* of them are gone — no per-key refcount table is needed. A range tombstone holds no segments,
  so a Candy it shadows is reclaimed only once compaction drops the covered point locator (as with a
  point delete).

The full compaction-then-GC cycle is covered end-to-end by `CompactionGcCycleIT`. One gap: deleting a
Box drops its manifest pointer but does not yet reclaim that Box's SSTable/Syrup/WAL/manifest ledgers
(**TODO(phase-3)**).

## 10. Size limits (configurable; enforced at client and node)

| Limit | Default | Where |
|---|---|---|
| `chunkSize` (Syrup entry payload) | 1 MiB | < BK max frame minus overhead |
| Max CandyKey length | 1 KiB UTF-8 | client + node |
| Box name | 3–63 chars `[a-z0-9-]` | `BoxName` |
| Max user-metadata total | 8 KiB | inside locator |
| Max CandyLocator serialized size | 256 KiB hard cap | `CandyLocatorSerializer` (room for 10k-part multipart) |
| Min multipart part size (except last) | 5 MiB | S3 parity; `multipart.min.part.bytes` |
| Max multipart parts per upload | 10,000 | S3 parity; `multipart.max.parts` |
| Multipart upload TTL | 7 days | sweeper auto-aborts; `multipart.upload.ttl.millis` |
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
| Memtable flush threshold | 4 MiB | Bounds WAL replay and L0 table size. |
| Syrup rollover | 1 GiB | Bounds per-ledger size; large objects span multiple Syrups. |
| L0 compaction trigger / stall | 4 tables / 12 tables | Trigger starts compaction; stall returns `BUSY` (must be ≥ trigger). |
| Leveled level base / multiplier | 10 MiB / 10× | LevelDB-style growing per-level byte budgets for L≥1. |
| HLC logical width / skew bound | 32-bit logical; 5-min skew rejection | Overflow needs ~2.1e9 events/ms; bound stops a wild clock dragging us forward. |
| Ownership lease TTL / renew / fencing token | 10 s TTL; 3 s renew; monotonic per-resource counter (ZK version) | Short TTL for quick failover; renew well inside it; strictly increasing tokens fence zombies. |
| Compaction + GC worker interval | configurable; 0 disables | Background maintenance cadence on the owner. |
| Tombstone-GC time bound | 24 h | Covers in-flight late writes before a delete is reclaimable. |
| Ledger-GC grace | 5 min | Margin for in-flight readers / continuation tokens before a physical delete. |
| Client router cache TTL | 5 s | How long the client caches a Box→owner mapping before re-resolving. |
| Continuation token | `lastKey` | `lastKey` alone resumes a range/reverse scan, exclusive in the scan direction. |
| LWW tiebreaker | `nodeId` (locked) | Deterministic, coordination-free. |
| TCP opcodes | incl. dedicated `RESPONSE_BUSY` and `RESPONSE_MOVED` | Backpressure and re-routing are first-class signals. |

## 12. Deliberate v1 simplifications (escape hatches)

- **Single owner per Box, no sub-Box partitioning** — caps a Box's write throughput at one node and
  makes it unavailable during the handover fence+replay window. Manifest/ownership structures are kept
  **partition-shaped** (one partition per Box now) so key-range tablets can be added later.
- **Reads served by the Box owner** — simple and correct. Sealed SSTables/Syrups are immutable and
  replicated, so any node *could* serve reads of flushed data; only unflushed-memtable read-your-writes
  needs the owner. Not built in v1.
- **No small-object inlining** — all bytes go to Syrups (an extra BK round trip + fragmentation for
  tiny Candies). The 256 KiB locator cap leaves room to inline small bytes later.
- **No Syrup defragmentation** — see §9(d). Syrup compaction (copying survivors into a fresh Syrup)
  is future work.
- **Buffered wire protocol** — PUT/GET/UploadPart bodies are inlined in one framed message (16 MiB
  cap). Multipart upload partially substitutes for streaming, but each part is still bounded by the
  frame cap. True on-the-wire chunked streaming remains `TODO(phase-2)`.
- **UploadPartCopy buffers in memory** — the engine reads the source slice and writes it as a fresh
  part rather than sharing Syrup segments when the range aligns to chunk boundaries; the optimization
  is a strictly-internal future change.

## 13. What's done vs. remaining

**Implemented (Phases 0–4):** the LSM engine (memtable/WAL/SSTable/Syrup, merged multi-level read
path, range tombstones, zero-copy copy/rename); ZooKeeper-backed coordination
(`ZooKeeperCoordinationService`) with the manifest ZK pointer under versioned CAS and fenced
ownership/handover; cluster membership + client-side routing over the framed TCP protocol
(`RESPONSE_MOVED`); multi-level leveled compaction on a background worker; reference-counted GC of
SSTable/Syrup/WAL ledgers; the storage node with health/metrics, the `candybox` CLI, and a packaged
distribution (Docker image + Compose + Kubernetes manifests). Fault-injection / failure-path
hardening and operational docs (`OPERATIONS.md`) landed in Phase 4.

**Remaining future work:**

- **True on-the-wire streaming.** The protocol still inlines a Candy's bytes in one framed message and
  the client buffers a stream in memory; chunked PUT/GET/UploadPart bodies are future work
  (`TODO(phase-2)` in `Message`/`CandyboxClient`). Multipart upload partially fills the
  "resumable / parallel upload" gap, but each part is still bounded by the frame cap.
- **Distributed cross-node compaction scheduling.** Compaction runs in-process on each Box's owner;
  claiming work via ZK task leases so it can run off the owner is `TODO(phase-3)` (the *commit* is
  already fencing-gated).
- **Watch-based coordination.** The `CoordinationService` callers poll today; ZK watches to avoid
  polling are `TODO(phase-2)`.
- **GC enumeration backstop + Box-delete reclamation.** Pending-orphan sets are in-memory, so ledgers
  orphaned by a prior owner that crashed before GC, and the ledgers of a deleted Box, are not yet
  reclaimed (§9, `TODO(phase-3)`).
- **Read scaling.** Sealed SSTables/Syrups are immutable and replicated, so any node *could* serve
  reads of flushed data; only unflushed-memtable read-your-writes needs the owner. Serving reads off
  the owner is not built in v1.
- **Syrup defragmentation and small-object inlining** — see §9(d) and §12.
