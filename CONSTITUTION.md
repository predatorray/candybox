# Candybox Constitution

Version: **1.0.0**

This constitution turns Candybox's documented promises into constraints for
spec-driven development. It applies to specifications, implementation plans,
code, tests, and user documentation. **MUST** and **MUST NOT** are requirements;
**SHOULD** permits a documented, justified exception.

The product principles below are derived from existing repository documentation.
The specification and amendment process establishes how to preserve those
principles in future work; it does not claim that every historical change used
that process. Source links identify the basis of each principle, not proof that
the implementation is free of defects.

## I. Preserve the storage architecture

Candybox MUST remain a self-hosted, distributed, S3-like object store with a sorted
LSM index. Box means bucket, Candy means object, CandyKey means object key,
CandyLocator means the index value pointing to bytes, and Syrup means the data
ledger containing those bytes.

- Object bytes MUST stay in Syrups, outside the LSM tree. Compaction operates on
  locators; it MUST NOT require rewriting object bodies.
- WALs, SSTables, manifests, and Syrups use replicated, append-only BookKeeper
  ledgers. Updates and deletes append new state rather than mutate stored data.
- The LSM engine MUST depend only on `common`, `LedgerStore`, and
  `CoordinationService`. Raw BookKeeper access belongs in `candybox-bookkeeper`;
  raw ZooKeeper access belongs behind the coordination SPI.
- Clients route to partition owners. The S3 gateway and admin API MUST remain
  stateless adapters rather than introduce a separate authoritative object store.

Sources: [README architecture](README.md#architecture), [DESIGN §§1–2, 4](DESIGN.md),
[AGENTS architecture](AGENTS.md#architecture).

## II. Fence ownership and recover acknowledged state

- A Box's partition count is fixed at creation. Key placement MUST use CRC32C of
  UTF-8 key bytes modulo that count. Each partition has an independent LSM engine
  and at most one valid, leased, fenced owner.
- Every state-changing append MUST carry the owner's fencing token. Handover MUST
  recover-open (fence and seal) the old WAL and manifest, replay durable state,
  and publish a fresh manifest pointer using the expected coordination version
  in a compare-and-set. A stale owner or compactor MUST NOT commit new state.
- Writes MUST make object bytes durable and append their mutation to the WAL
  before acknowledging the memtable mutation. Manifest state MUST advance only
  after its edit is durably appended.
- Recovery MUST preserve acknowledged writes across repeated handovers. A
  non-empty WAL replay MUST become durable in an SSTable in the edit rotating the
  WAL; retaining replayed writes only in the successor's memtable is insufficient.
- Failed recovery or a lost pointer CAS MUST NOT authorize deleting the previous
  recovery roots. Loss of quorum MUST fail the operation without claiming success.

Sources: [DESIGN §§3–4, 7, 9](DESIGN.md),
[Operations failure modes](OPERATIONS.md#failure-modes--recovery).

## III. State consistency at its actual scope

- The owner MUST stamp mutations with an HLC at ingest; clients do not supply the
  ordering clock. Reads choose the greatest `(physicalMillis, logicalCounter,
  nodeId)` with deterministic last-writer-wins resolution.
- Before accepting mutations after recovery, the owner MUST observe
  `max(manifest maxHlc, replayed WAL maxHlc)`. Flushes MUST preserve the HLC
  high-water mark even when the WAL becomes empty. Clock regression MUST NOT
  reverse ordering; observations beyond the configured skew bound fail recovery.
- Per-key operations are linearizable on the owner, with the documented stale-read
  window during ownership handover. Specifications MUST NOT claim unconditional
  cluster-wide linearizability or snapshot listings.
- Same-partition rename MUST be atomic. Cross-partition rename is eventually
  atomic: durable intents and rendezvous markers drive recovery to completion;
  readers may temporarily see both keys. Completion MUST NOT delete a newer
  re-PUT of the source. An abandoned intent without a destination marker leaves
  the source live.
- Cross-partition listings are merged partition pages, not a global snapshot.
  Range deletion is idempotent and independently committed per partition, not
  atomic across the Box.

Sources: [DESIGN §§3, 6](DESIGN.md),
[Operations consistency](OPERATIONS.md#consistency--backpressure).

## IV. Keep ordered operations and zero-copy behavior

- CandyKeys MUST use unsigned UTF-8 byte order consistently in storage, scans,
  and client merges. Scans MUST support prefix filtering, `[start, end)` bounds,
  forward/reverse order, and exclusive continuation in the scan direction.
  Delimiter listings MUST roll up common prefixes and resume past their keys.
- Range deletion MUST use one range tombstone per participating partition rather
  than enumerate and delete every matching key. Reads and scans MUST suppress
  points covered by newer tombstones.
- Same-Box copy and rename MUST reuse stored Syrup segments, including across
  partitions. Cross-partition transfer MUST use an owner-controlled locator relay
  bound to the principal; clients MUST NOT supply trusted segment or ACL contents.
- Multipart state MUST survive handover, pin uploaded parts, and publish the
  completed object without exposing a partially assembled object. Abort,
  replacement parts, and expiry MUST release parts through safe GC bookkeeping.
  Range reads MUST validate the chunks they read; full reads validate part CRC32C.
- The zero-copy copy/rename promise does not extend to `UploadPartCopy`, which
  currently buffers and rewrites bytes, or to unsupported cross-Box copying.

Sources: [README sorted operations](README.md#operations-the-sorted-lsm-tree-makes-cheap),
[DESIGN §6](DESIGN.md), [Multipart/range plan](MULTIPART_RANGE_PLAN.md),
[Cross-partition zero-copy plan](CROSS_PARTITION_ZERO_COPY_PLAN.md).

## V. Prefer retaining garbage to losing live data

- Compaction MUST preserve point and range tombstones until both the GC age
  condition and bottommost-overlap safety rules are satisfied. It MUST NOT
  resurrect a deleted object, including when older overlapping data lies outside
  the compaction inputs.
- GC MUST derive eligibility from committed state and honor the configured ledger
  grace period. Removal from committed state and entry into pending deletes MUST
  be recorded together in a fenced edit. Pending deletion MUST survive handover
  and remain retryable until physical deletion succeeds or the ledger is absent.
- Syrup liveness MUST include SSTables, memtables, open writes, multipart parts,
  pending locator transfers, and references held by every partition of the Box.
  A reintroduced reference MUST invalidate stale orphan eligibility; the grace
  period follows the last release anywhere in the Box.
- Uncertain or undecodable liveness evidence MUST retain potentially live data.
  Whole-ledger reclamation MUST wait until all its segments are dead; Syrup
  defragmentation is not an existing capability.
- When allocation-epoch enumeration is enabled, deletion MUST additionally require
  a retired allocating epoch, recognized matching identity, sufficient age, and
  no active retention pins. Pinning and permanent deletion claims MUST serialize
  through the retention CAS. Wall-clock age alone cannot retire a writer epoch.
  Missing, legacy, or ambiguous evidence MUST hold the allocation.
- Backup integrations MUST pin before capturing roots or bytes and abort capture
  on pin failure. Retention support MUST NOT be advertised as a backup/restore
  implementation. Enumeration remains opt-in and does not reclaim legacy
  untagged allocations by assumption.

Sources: [DESIGN §§8–9 and allocation-epoch enumeration](DESIGN.md),
[Operations ledger enumeration](OPERATIONS.md#ledger-enumeration-backstop).

## VI. Bound work and make failures actionable

- Record and frame decoding MUST validate versions, lengths, and configured limits
  before unsafe allocation. Client and node validation MUST enforce the documented
  key, metadata, locator, object, and multipart bounds. Persisted records MUST be
  versioned; format changes MUST bump the version and preserve old-format decoding.
- Specifications changing wire opcodes, persisted formats, or configuration MUST
  describe mixed-version behavior, upgrade order, and rollback restrictions.
  Read compatibility with old records does not imply old binaries can read new ones.
- L0 write stalls MUST return retriable `BUSY`; wrong-owner routing MUST use
  `MOVED`. Temporarily unowned partitions MUST report unavailable, not missing.
  Retry behavior MUST distinguish known routing failures from requests whose
  outcome is ambiguous; in-flight mutations MUST NOT be blindly replayed.
- Per-connection queued work and output MUST remain bounded. Blocking client calls
  MUST NOT run on gateway Netty event-loop threads. Shutdown MUST stop admission
  before the bounded drain of in-flight requests.
- Operations MUST remain diagnosable through liveness/readiness probes, metrics,
  and contextual logs. Configuration changes MUST document defaults and the
  applicable `CANDYBOX_` environment override, including passthrough exceptions.

Sources: [DESIGN §§5, 10](DESIGN.md),
[Operations](OPERATIONS.md), [Gateway threading model](S3_GATEWAY_PLAN.md#4-architecture--request-flow).

## VII. Preserve authorization and an honest compatibility boundary

- Changes MUST preserve the documented SASL/TLS, SigV4, Box/object ACL, and
  service-principal trust boundaries when those controls are enabled. Copy,
  rename, and part-copy MUST enforce source READ and destination WRITE permissions.
  Client-supplied identity MUST NOT bypass principal checks.
- Admin bearer-token protection and optional metrics-token protection MUST remain
  effective when configured. Health/readiness probes remain open as documented.
  Credentials and ledger passwords MUST NOT be treated as interchangeable forms
  of end-user authorization.
- S3 support MUST be described as a tested subset. The checked-in compatibility
  allowlist is a regression contract; a feature change MUST NOT silently shrink
  it to hide failures. Report calibration revisions and supported operations
  rather than imply full AWS S3 parity.
- Path-style addressing, single-range GET, and the documented multipart behavior
  are existing scope. Versioning, SSE, POST object, lifecycle, bucket policy,
  CORS, and conditional GET MUST NOT be advertised as implemented without a
  specification, implementation, and compatibility evidence.

Sources: [Operations security](OPERATIONS.md#security), [Auth implementation notes](AUTH_PLAN.md),
[S3 gateway docs](docs/content/docs/s3-gateway/_index.md),
[S3 compatibility contract](compat/s3-tests/README.md#the-allowlist-as-a-contract).

## Specification and delivery gates

Before implementing a behavior change, its specification MUST record:

1. The user-visible outcome, scope, non-goals, and source promises being preserved
   or deliberately amended; reference applicable principles I–VII.
2. Observable acceptance scenarios, including relevant boundary, failure, retry,
   concurrency, and recovery cases. State the partition/atomicity scope explicitly.
3. Effects on durable state, fencing, HLC ordering, tombstones, GC roots,
   authorization, resource bounds, and compatibility. Mark unaffected areas with
   a brief reason rather than inventing requirements.
4. Validation and operational changes: tests, rollout/rollback constraints,
   configuration, observability, and documentation updates. List unresolved
   questions instead of silently choosing weaker guarantees.

The implementation plan MUST map requirements to affected modules and tests. An
implementation MUST satisfy its acceptance criteria and update affected public
docs before being called complete. Small fixes may keep this material in the
issue or PR; this constitution requires no particular generator or spec directory.

Testing MUST follow [AGENTS.md](AGENTS.md#testing-conventions): hand-written
in-memory fakes rather than mocking frameworks, shared SPI contracts for fake and
real implementations, and `*IT.java` tests in `candybox-integration-tests`. Changes
to recovery, fencing, or GC MUST include relevant failure-path evidence, not only
happy-path assertions. SPI behavior changes MUST update the shared contract.

Run checks appropriate to the affected surface. Java delivery uses unit tests and
`mvn verify` (integration tests and license checks), with sibling test-jars
installed as described in [AGENTS commands](AGENTS.md#commands). Frontend changes
use lint, format checks, tests, and build; S3 changes use the compatibility harness
where relevant. Record commands, outcomes, and any unrun checks accurately.
Documentation-only edits need link/content and whitespace checks, not a full
runtime test suite.

## Interpretation and amendments

This constitution governs future specifications. Detailed formats and tunable
defaults remain in DESIGN and the operations/configuration references. A change
to a numeric default is not automatically a constitutional amendment; weakening
one of the guarantees above is.

Historical plans are context, not automatic evidence of current support. Known
examples: the byte-copy fallback in `BOX_PARTITIONING_PLAN.md` is superseded by
zero-copy; the original gateway plan's deferral of multipart/range is superseded;
the dashboard plan's no-auth posture and parts of the auth plan are superseded
by the operational security documentation and implementation notes. DESIGN and
OPERATIONS also retain older enumeration-TODO statements alongside later
allocation-epoch sections. Use those later sections for the opt-in backstop while
preserving their legacy-allocation limitations. Documentation conflicts MUST be
called out and resolved with implementation/test evidence before relying on a
disputed behavior; this document is not an implementation audit.

True chunked TCP streaming, off-owner compaction, watch-based coordination,
repartitioning, off-owner read scaling, small-object inlining, and Syrup
defragmentation remain future work. Multipart does not remove the per-frame bound.
A future feature may address these limits through a specification that preserves
the principles above; listing them here does not promise delivery.

Amendments MUST accompany the change that needs them, identify the affected
principle and rationale, assess migration and compatibility impact, and update
dependent specs, tests, and docs. A reviewer MUST be able to see any deliberate
weakening of a promise; implementation drift alone does not amend this document.
Use a major constitution version for removed or weakened guarantees, a minor
version for new principles or materially stronger requirements, and a patch
version for clarifications that do not change obligations.
