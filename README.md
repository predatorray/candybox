# Candybox

A distributed, S3-like object store whose storage engine is a **distributed LSM tree built on Apache
BookKeeper**. Each SSTable is a BookKeeper ledger; object bytes live in dedicated data ledgers
("Syrups") and never enter the LSM tree — the tree stores only compact pointers (`CandyLocator`s).

- **Standalone LSM mechanics** modelled on LevelDB (memtable → WAL → leveled SSTables, block-based
  SSTable format, bloom filters).
- **Distributed compaction** modelled on Cassandra (pluggable `CompactionStrategy`, leveled default).
- **Append-only mutations/deletes and ledger lifecycle** modelled on Apache Pulsar's managed-ledger
  approach (BookKeeper cannot mutate or delete individual entries in place).

> Vocabulary: **Box** = bucket, **Candy** = object, **CandyKey** = object key, **Syrup** = data
> ledger holding Candy bytes. See [`DESIGN.md`](DESIGN.md) for the full design.

## Status

| Phase | Scope | State |
|---|---|---|
| 0 | Modules, domain types, serialization, SPIs + in-memory fakes | ✅ implemented & tested |
| 1 | Single-node core LSM: memtable, WAL, L0 SSTable flush, Syrup chunking, merged read path, LWW + tombstones | ✅ implemented & tested (fakes **and** embedded BookKeeper) |
| 2 | Server node, ledger-backed manifest + ZK pointer, framed TCP protocol + thin client, fenced ownership & routing | ✅ implemented & tested (fakes **and** embedded BookKeeper + ZooKeeper). Deferred: chunked-streaming wire path (2.5), automatic failover, cluster-wide listBoxes. See `PHASE2_PLAN.md`. |
| 3 | Distributed compaction & reference-counted GC | ✅ implemented & tested (fakes **and** embedded BookKeeper). Owner-driven background compaction (byte-size leveled scoring, fencing-gated commit); GC of obsolete SSTables, orphaned Syrups, rotated WALs. Deferred: distributed compaction offload, GC enumeration backstop, Syrup defrag. See `PHASE3_PLAN.md`. |
| 4 | Hardening, fault injection, ops docs | ⛔ not started |

## Requirements

- **Java 17+** (the build targets Java 17 bytecode; it has been validated building and testing on a
  JDK 23 toolchain).
- **Maven 3.9+**.
- No external services or Docker: integration tests run an in-JVM BookKeeper (`LocalBookKeeper`, which
  bundles an in-process ZooKeeper).

## Build & test

```bash
# Compile and package everything (no tests):
mvn -q -DskipTests package

# Run all UNIT tests (fakes only — no BookKeeper, ZooKeeper, or sockets):
mvn test

# Run the INTEGRATION tests too (embedded BookKeeper + in-JVM ZooKeeper).
# These are *IT.java, bound to the failsafe plugin, and run on `verify`:
mvn verify
```

`mvn test` runs only the fast, dependency-free unit suites. The integration module's unit phase is
skipped; its `*IT` tests run under `mvn verify` via maven-failsafe. The failsafe execution adds the
`--add-opens` flags modern JDKs require for BookKeeper/ZooKeeper's reflective access to JDK internals.

To run a single module's tests, e.g. the LSM engine:

```bash
mvn -pl candybox-lsm test
mvn -pl candybox-integration-tests verify   # embedded-BookKeeper end-to-end
```

## Module map

| Module | Responsibility |
|---|---|
| `candybox-common` | Domain types (`BoxName`, `CandyKey`, `CandyLocator`, `Hlc`, …), versioned serialization, `CandyboxConfig`, CRC32C, bloom filter, exceptions. |
| `candybox-bookkeeper` | `LedgerStore` SPI + adversarial in-memory fake + real BookKeeper-backed impl. The only module that touches the raw BookKeeper client. Publishes a shared contract test-jar. |
| `candybox-coordination` | Coordination SPI (membership, leases with fencing tokens, CAS key-value) + in-memory fake + ZooKeeper scaffold. |
| `candybox-lsm` | The LSM engine: memtable, WAL, SSTable read/write, Syrup chunking, manifest, merge/read path, `CompactionStrategy` + leveled default, `BoxEngine`. Depends only on the two SPIs. |
| `candybox-protocol` | Versioned framed TCP codec, typed messages, `Transport` SPI (TCP impl + loopback fake). |
| `candybox-server` | Storage node wiring the engine behind the protocol; compaction service; GC scaffold. |
| `candybox-client` | Thin client over the `Transport` SPI. |
| `candybox-integration-tests` | End-to-end tests on embedded BookKeeper + in-JVM ZooKeeper. |

## Testing philosophy

No mocking frameworks. Every external dependency (BookKeeper, coordination, transport, clock) sits
behind a narrow interface with a **hand-written in-memory fake that models adversarial/failure
semantics** — ledger sealing/LAC, recover-open fencing, ack-quorum loss, lease expiry, and CAS
conflicts. A **shared contract-test suite** (`LedgerStoreContract`) runs identically against the fake
and the real BookKeeper-backed store, so the fast unit tests are a faithful stand-in for the hard
fencing/handover scenarios.
