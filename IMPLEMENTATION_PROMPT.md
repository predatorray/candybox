# Implementation Prompt: Candybox — a distributed S3-like object store on Apache BookKeeper

You are implementing **Candybox**, a distributed object store whose storage engine is a distributed LSM tree built on **Apache BookKeeper ledgers**. Work methodically and favor correctness, clear interfaces, and testability over feature breadth. Build **phase by phase** (see the Roadmap); in this run deliver Phases 0–1 fully working and scaffold Phases 2–4 with interfaces, stubs, and design notes. Do not silently skip anything — if you defer something, leave a `// TODO(phase-N):` marker and record it in `DESIGN.md`.

## 1. Mission & non-goals

- A distributed, S3-like object store providing basic object operations through a **Java client library** and a **minimal TCP wire protocol**.
- The storage engine is a **distributed LSM tree**: each SSTable is a BookKeeper **ledger**. Model the standalone LSM mechanics on **LevelDB** (memtable → WAL → leveled SSTables, block-based SSTable format, bloom filters). Model the **distributed compaction** on **Cassandra** (pluggable compaction strategies, leveled by default). Model **append-only handling of mutations/deletes and ledger lifecycle/rollover/GC** on **Apache Pulsar's managed-ledger** approach (BookKeeper cannot mutate or delete individual entries in place).
- **Non-goals (v1):** S3 API/protocol compatibility, authentication/authorization, encryption, multi-region, transactions across keys, user-facing object versioning.

## 2. Tech stack & hard constraints

- **Java 17**, **Maven** (multi-module reactor build).
- **Apache BookKeeper** latest stable 4.17.x line; it brings a **ZooKeeper** dependency that you will reuse.
- **JUnit 5 (Jupiter)** for all tests. **AssertJ** is allowed for assertions.
- **No Mockito, no PowerMock, no bytecode-mocking of any kind.** Achieve testability through **dependency injection and hand-written test doubles (fakes)**. Every external dependency (BookKeeper client, ZooKeeper/coordination, clock, ID generation, network transport) must sit behind a narrow interface that you can implement with an in-memory fake.
- No Lombok. Prefer plain Java records, sealed interfaces, and explicit constructors. Keep dependencies minimal (SLF4J for logging; a small CRC/checksum and bloom-filter lib only if clearly justified — otherwise implement them).

## 3. Domain glossary (use these names everywhere — types, APIs, protocol)

- **Box** = the S3 "Bucket". A named container of Candies.
- **Candy** = the S3 "Object". An immutable blob of bytes plus user metadata, addressed by a **candy key** within a Box.
- **Syrup** = a BookKeeper **ledger that stores Candy bytes** (the data ledger). A Candy's bytes are chunked across entries of one or more Syrups; the LSM tree never holds Candy bytes, only pointers into Syrups.
- **CandyKey** = the within-Box key (UTF-8 string). **CandyLocator** = the small LSM value pointing to where a Candy's bytes live (which Syrups and entry ranges).
- Other internal terms (SSTable, memtable, WAL, manifest, compaction, ledger) keep their conventional names.

## 4. Locked architectural decisions (do not redesign these)

1. **Topology — server cluster + thin client.** Candybox runs as a cluster of **storage nodes** that own writes, reads, and compaction. The client library is thin and routes requests to nodes.
2. **Wire protocol — a minimal, framed binary TCP protocol** between client and nodes, and for node-to-node control where needed. Keep it small and versioned (magic + version + opcode + length-prefixed framing). Put it behind a `Transport` interface so an **in-JVM/loopback implementation** exists for tests alongside the real TCP one.
3. **Metadata & coordination — ZooKeeper + an append-only metadata ledger.** ZooKeeper holds: cluster membership, Box ownership/assignment, leader election, compaction task claims/locks, and a pointer to the current **manifest ledger**. The **manifest** itself (the LSM state: which ledgers exist, their level, key range, sequence ranges, reference counts) is an **append-only metadata ledger** in Pulsar managed-ledger style — appended to on changes, periodically checkpointed/compacted into a fresh manifest ledger, with the ZK pointer atomically advanced.
4. **Consistency — eventually consistent, last-writer-wins (LWW).** Every mutation carries a monotonic **logical timestamp / sequence number**; reads resolve to the highest timestamp for a key. Concurrent writers are allowed; conflicts resolve by (timestamp, then a deterministic tiebreaker such as node-id). No single-writer ownership requirement, though Box assignment to nodes is used for routing.
5. **Compaction — pluggable `CompactionStrategy` SPI with LevelDB-style leveled compaction as the default**, runnable distributed across nodes (claim work via ZK).
6. **Large objects — chunk Candy bytes across entries in dedicated Syrups; large objects allowed (up to GBs).** The LSM stores only the small CandyLocator; the bytes never enter the LSM tree.

## 5. Data model & on-ledger formats

**Three ledger roles (keep them distinct in code):**
- **WAL ledger(s):** per-node (or per-Box) write-ahead log. Each entry = a serialized mutation (PUT-locator or DELETE-tombstone) appended before the memtable acknowledges. Used for recovery of un-flushed memtables.
- **SSTable ledgers:** an immutable sorted run produced by a memtable flush or a compaction. Model the on-ledger layout on LevelDB's SSTable: sorted **data blocks**, a **bloom filter** block, an **index block**, and a **footer**, mapped onto ledger entries (e.g., one block per entry or a documented block-to-entry mapping). Entries are keyed/ordered by CandyKey then descending sequence number. SSTables store **CandyLocators**, never Candy bytes.
- **Syrups (data ledgers):** hold raw Candy bytes, split into fixed-size **chunks** (one chunk = one entry). A large Candy occupies a contiguous entry range in one Syrup; roll to a new Syrup when one reaches a configured size/entry-count cap.

**CandyLocator (the LSM value — keep it compact):**
- `sequenceNumber` (long, LWW), `type` (PUT | DELETE tombstone), `contentLength`, `chunkSize`, `contentType`, small `userMetadata` map, `crc32c`, `createdAtMillis`, and a **compact segment layout**: list of `{syrupId, firstEntryId, lastEntryId}`. Prefer fixed `chunkSize` + contiguous entries so the segment list is O(number of Syrups), not O(number of chunks).

Define a single, versioned **serialization** module for these records (manual binary or a compact scheme of your choice — document it). Every persisted record carries a format version byte for forward compatibility.

## 6. Size limits (you asked for these — enforce and make them configurable)

BookKeeper bounds entry size by `nettyMaxFrameSizeBytes` (default ~5 MB) and performs best with entries ≤ ~1 MB. Derive Candybox limits from that:

| Limit | Default | Rationale / where enforced |
|---|---|---|
| `chunkSize` (Syrup entry payload) | 1 MiB | Must be < BK max frame size minus overhead; validated against the BK client's configured max on startup. |
| Max CandyKey length | 1 KiB (UTF-8 bytes) | Keys are stored repeatedly (memtable, WAL, SSTable index) — keep small. Reject longer keys at the client and node. |
| Box name | 3–63 chars, `[a-z0-9-]`, S3-like | Must be safely encodable in ZK paths and ledger metadata. |
| Max user-metadata total | 8 KiB | Stored inside CandyLocator; keep LSM values small. |
| Max CandyLocator serialized size | 64 KiB (hard cap) | Guarantees a locator fits comfortably in one SSTable data-block entry. |
| Max Candy size | effectively unbounded; documented practical guidance | Bounded only by Syrup rollover policy; expose `maxCandySizeBytes` config (default unlimited). |

Validate at the client (fail fast) **and** at the node (authoritative). Surface a clear `CandyboxException` hierarchy for limit violations.

## 7. Maven module layout (multi-module reactor)

- `candybox-common` — domain types (Box, CandyKey, CandyLocator, sequence numbers, exceptions), serialization, config, checksums, bloom filter.
- `candybox-bookkeeper` — narrow SPI over BookKeeper (`LedgerStore`: create/open/append/read/close/delete ledgers, list entries) **plus an in-memory fake** implementation. No other module touches the raw BookKeeper client.
- `candybox-coordination` — SPI over ZooKeeper (membership, leader election, locks/leases, task-claim, key-value for the manifest pointer) **plus an in-memory fake**.
- `candybox-lsm` — memtable, WAL, SSTable read/write, manifest (metadata-ledger backed), iterators/merge, read path (memtable → immutable memtables → leveled SSTables), Syrup chunk write/read, `CompactionStrategy` SPI + leveled default. Depends only on the two SPIs above.
- `candybox-protocol` — versioned framed TCP message definitions + codec; `Transport` SPI with a TCP implementation and an in-JVM/loopback fake.
- `candybox-server` — the storage node: wires LSM + coordination + transport, handles requests, routing/Box assignment, background flush & compaction workers, GC.
- `candybox-client` — thin client library exposing the public API over `Transport`.
- `candybox-integration-tests` — end-to-end tests on **embedded BookKeeper + in-process ZooKeeper**.

## 8. Public client API surface (v1)

Box: `createBox`, `deleteBox` (empty-only unless `force`), `listBoxes`, `headBox`.
Candy: `putCandy` (byte[] and **streaming `InputStream`** variants), `getCandy` (streaming `OutputStream`/`InputStream`), `headCandy` (metadata only), `deleteCandy`, and `listCandies(box, prefix, startAfter, maxKeys)` returning a page + continuation token (LSM range scan over the merged view, tombstones suppressed). All operations carry LWW semantics; document read-after-write as best-effort under eventual consistency.

## 9. Testability mandate (no mocking frameworks)

- Constructor-inject every dependency behind an interface. Provide an in-memory fake for: `LedgerStore`, the coordination SPI, `Transport`, `Clock`, and sequence/ID generation.
- **Unit tests** run entirely on fakes — no BookKeeper, no ZK, no sockets. Cover: memtable ordering & LWW, WAL replay, SSTable write/read round-trip, bloom filter, merge iterator with tombstones, manifest apply/checkpoint, leveled compaction picking, Syrup chunking/reassembly, size-limit validation, protocol codec round-trip.
- **Integration tests** (`candybox-integration-tests`) use **BookKeeper's in-JVM LocalBookKeeper + an in-process ZooKeeper test server**, exercising real ledgers/coordination: full put/get/delete/list, large-object chunking across Syrups, recovery from WAL, a real compaction cycle, and GC of obsoleted ledgers. Keep them runnable in CI with no external services or Docker.

## 10. Phase roadmap

- **Phase 0 — Skeleton & contracts (this run):** Maven reactor + all modules; domain types & serialization in `candybox-common`; the `LedgerStore`, coordination, and `Transport` SPIs **with in-memory fakes**; `DESIGN.md` capturing architecture, formats, and any local decisions. Everything compiles; fakes are unit-tested.
- **Phase 1 — Single-node core LSM read/write path (this run):** memtable + WAL ledger + L0 SSTable flush + leveled read path + Candy chunking into Syrups + LWW timestamps + tombstones. A working put/get/delete/list against the **fake** `LedgerStore` (unit) and against **embedded BookKeeper** (integration). No networking yet — exercise the engine in-process.
- **Phase 2 — Server node, manifest, transport, client (scaffold now, implement next):** metadata-ledger manifest with ZK pointer; cluster membership & Box assignment/routing; the **minimal framed TCP protocol** + thin client; node-side validation. (This is the in-scope wire protocol you chose — interfaces and message definitions land in this run, full wiring next.)
- **Phase 3 — Distributed compaction & GC (scaffold now):** `CompactionStrategy` SPI finalized + leveled default; distributed scheduling via ZK leader election + task claims/leases; reference-counted GC of obsoleted ledgers (SSTables and orphaned Syrups) after a grace period (Pulsar-style ledger deletion).
- **Phase 4 — Hardening:** failure/recovery paths, backpressure, broader integration & fault-injection tests, operational docs.

## 11. Deliverables for THIS run

1. Full Maven reactor that builds (`mvn -q -DskipTests package` succeeds) and passes `mvn test`.
2. Phase 0 + Phase 1 implemented and tested as described.
3. Phases 2–4: interfaces, protocol message types, and stubs in place with `// TODO(phase-N)` markers.
4. `DESIGN.md` (architecture, ledger/record formats with version bytes, limits table, compaction model, manifest/GC design, open questions) and a `README.md` (build/test instructions, module map).
5. A short closing report: what's working end-to-end, what's stubbed, how to run unit vs integration tests, and any deviations from this prompt with rationale.

## 12. Conventions

- Clear package structure under `me.predatorray.candybox.*`. Sealed interfaces for record/message variants. Records for immutable data. Defensive validation at module boundaries. SLF4J logging with meaningful context (box, candy key, ledger/syrup id, sequence). No premature optimization; document complexity assumptions. Every public type and SPI method gets a concise Javadoc stating its contract and threading expectations.

## 13. Decisions you may make yourself — and must document in `DESIGN.md`

Exact binary serialization scheme; block-to-entry mapping inside SSTable ledgers; bloom filter parameters; WAL granularity (per-node vs per-Box); memtable data structure (e.g., concurrent skiplist); manifest checkpoint cadence; Syrup rollover thresholds; TCP frame opcode set; the LWW tiebreaker. Pick reasonable defaults, keep them configurable, and record the rationale.
