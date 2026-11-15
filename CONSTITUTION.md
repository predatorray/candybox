# Candybox Constitution

Version: **1.0.0**

This constitution defines Candybox's product guarantees, architecture, and rules
for spec-driven development. Specifications, plans, code, tests, and user
documentation MUST follow it. **MUST** and **MUST NOT** are requirements;
**SHOULD** permits a documented, justified exception.

This document is the starting point for development. Its requirements stand on
their own; understanding them MUST NOT require reading another document.

## I. Product and architecture

Candybox MUST be a self-hosted, distributed, S3-like object store with a sorted
log-structured merge tree (LSM) index. A Box is a bucket, a Candy is an object,
and a CandyKey is an object's key. A CandyLocator is the small index value that
points to object bytes and carries their metadata. A Syrup is a data ledger
containing those bytes.

Object bytes MUST stay in Syrups, outside the index. Copy and rename reuse
locators' segment references, and index compaction MUST NOT rewrite object bodies.
Updates and deletes append new state rather than mutate persisted data.

BookKeeper provides replicated, append-only ledgers for object bytes and index
state. ZooKeeper provides cluster membership, Box descriptors, ownership leases,
manifest pointers, assignments, access-control lists, and transfer coordination.
These responsibilities MUST remain behind narrow service interfaces with
in-memory implementations for deterministic testing.

### Component responsibilities

Candybox uses Java 17 and a Maven multi-module build.

- `candybox-common` supplies domain types, configuration, versioned serialization,
  clocks, checksums, authentication, authorization, and TLS support.
- `candybox-bookkeeper` defines `LedgerStore`, its in-memory fake, and its
  BookKeeper implementation. Only this module uses the raw BookKeeper client.
- `candybox-coordination` defines `CoordinationService`, its in-memory fake, and
  its ZooKeeper implementation for membership, fenced leases, and versioned
  metadata updates. Raw ZooKeeper access belongs behind this interface.
- `candybox-lsm` implements one `BoxEngine` per partition. It depends only on
  `common` and the two service interfaces, never the raw backend clients.
- `candybox-protocol` defines framed TCP messages, authentication exchanges, and
  the transport interface, including network implementations and a loopback fake.
- `candybox-server` connects requests to engines, manages ownership, runs
  balancing and maintenance, and exposes health and metrics.
- `candybox-client` supplies the Java API, command-line interface, and routing.
  Keyed requests go to the key's owner; listings merge results from all partitions.
- `candybox-s3-gateway` translates path-style S3 HTTP requests into client calls.
  `candybox-admin-api` provides a JSON API and serves the `candybox-web` React
  and TypeScript dashboard at `/ui/`. Both services MUST remain stateless
  adapters without a separate authoritative object store.
- `candybox-dist` packages runnable binaries and deployment assets.
  `candybox-integration-tests` holds tests against real embedded backends.

### Storage and request flow

Each partition has a write-ahead log (WAL), a sorted in-memory table (memtable),
immutable sorted tables (SSTables), Syrups, and a manifest. The manifest is the
durable inventory of tables, logs, uploads, and pending maintenance work.

A put validates input, writes object chunks durably to Syrups, stamps a mutation,
appends its locator to the WAL, and applies it to the memtable. The WAL MUST be
acknowledgment-quorum durable before the mutation is acknowledged. Manifest state
MUST advance only after the corresponding edit is durably appended.

A flush writes the memtable to an L0 SSTable and commits a manifest edit recording
the table, referenced Syrups, and rotated WAL. Compaction merges sorted tables
into deeper levels. L0 tables may overlap; deeper levels keep non-overlapping key
ranges within each level. Compaction and garbage collection run on the owner.

Get and head resolve the newest visible locator across the memtable and relevant
tables at every level. A tombstone is an appended deletion record. Absence, a
point tombstone, or a newer covering range tombstone means the object is missing.
Get reads and validates the resolved Syrup bytes; head returns metadata.

## II. Ownership, durability, and recovery

A Box has a fixed partition count set at creation, with a default of eight.
Placement MUST use CRC32C of the key's UTF-8 bytes modulo that count. At most one
valid owner holds a partition's lease. A fencing token increases on each fresh
lease acquisition so a superseded owner can be rejected.

Every state-changing append MUST carry the owner's fencing token. Handover MUST
recover-open the old WAL and manifest: fence their writers and seal them at a
recoverable boundary. Reading without fencing is insufficient.

The successor MUST replay committed state, create a fresh manifest containing a
complete checkpoint, and replace the manifest pointer using a compare-and-set
against the expected coordination version. A stale owner or compactor MUST NOT
commit new state. A failed recovery or lost pointer update MUST NOT authorize
deleting previous recovery roots.

A non-empty WAL replay MUST become durable in an SSTable in the edit rotating
the WAL. Keeping recovered writes only in the successor's memtable can lose
acknowledged writes on another handover and is forbidden. Loss of quorum MUST
fail the operation without claiming success.

An elected balancer assigns partitions, prioritizes failover, and limits moves
away from live owners. Automatic takeover requires balancing to be enabled;
otherwise an operator must initiate takeover. The partition can be unavailable
while fencing, replay, and publication run.

## III. Ordering and consistency

The owner MUST stamp mutations at ingest with a hybrid logical clock (HLC);
clients do not supply the ordering clock. An HLC contains physical milliseconds,
a logical counter, and a node identifier. Reads select the greatest tuple, using
the node identifier as the deterministic final tiebreaker.

Before accepting mutations after recovery, the owner MUST advance its clock past
the maximum HLC in both the manifest and replayed WAL. Flushes MUST preserve that
high-water mark even when the WAL becomes empty. A regressing wall clock MUST
NOT reverse ordering. Observed timestamps beyond the configured clock-skew
bound fail recovery.

Per-key operations are linearizable on the owner. During handover, the fenced old
owner may briefly return stale memtable reads, but its writes fail. Specifications
MUST state this boundary rather than promise unconditional cluster-wide
linearizability.

Same-partition rename MUST be atomic under the owner's write lock.
Cross-partition rename is eventually atomic: the source records a durable intent,
the destination records a coordination marker after its put is durable, and the
source completes the conditional deletion. Recovery and maintenance MUST resume
unfinished intents. Readers may temporarily see both keys. Completion MUST NOT
delete a newer re-PUT of the source. An abandoned intent without a destination
marker leaves the source live.

Cross-partition listings merge independently consistent partition pages and are
not global snapshots. Range deletion is idempotent and independently committed
per partition, not atomic across a Box.

## IV. Ordered operations and shared bytes

CandyKeys MUST use unsigned UTF-8 byte order throughout storage, scans, and client
merges. Scans support prefix filters, start-inclusive and end-exclusive bounds,
forward or reverse order, and exclusive continuation in the scan direction.
Delimiter listings group keys into common prefixes; continuation past a common
prefix MUST skip all keys beneath it.

A listing page queries every partition, merges sorted results, and deduplicates
common prefixes. Range deletion MUST append one range tombstone per partition
rather than enumerate matching keys. Reads and scans MUST hide points covered
by newer tombstones.

Same-Box copy and rename MUST reuse stored Syrup segments across both same- and
cross-partition operations. A source owner publishes a locator relay bound to the
requesting principal and gives the client a token. The destination resolves that
token and reuses the segments. Clients MUST NOT supply trusted segment or ACL
contents. The relay MUST pin its bytes until consumption or safe abandonment.

Multipart uploads support create, upload part, complete, abort, list uploads,
list parts, and same-Box part-copy. State MUST survive handover and pin its parts.
Re-uploading a part number replaces that part. Completion validates the ordered
part list and checksums and publishes a complete object without exposing an
intermediate assembly. Abort, replacement, and expiry release parts through safe
garbage-collection bookkeeping.

Range GET supports one inclusive byte interval, an open-ended interval, or a
suffix length and returns HTTP 206 with Content-Range when satisfied. Multiple
ranges are rejected. Range reads MUST validate the chunks read; full reads
validate each part's CRC32C.

The zero-copy copy/rename guarantee does not extend to `UploadPartCopy`, which
buffers and rewrites bytes. Cross-Box copying is outside the supported scope.

## V. Safe compaction and garbage collection

Compaction MUST preserve point and range tombstones until their age exceeds the
configured deletion grace and they are at the bottommost level containing
overlapping data. Bottommost means no deeper overlapping data still needs the
deletion record to hide an older value. A range tombstone MUST also remain while
an overlapping table outside the inputs might contain an older or unknown-age
point that would become visible. Compaction MUST NOT resurrect deleted objects.

Garbage collection (GC) MUST derive eligibility from committed state and honor
the ledger grace period. Removal from committed state and entry into pending
deletes MUST be recorded together in a fenced edit. Pending deletion MUST survive
handover and remain retryable until physical deletion succeeds or the ledger is
already absent.

Syrup liveness includes tables, memtables, open writes, multipart parts, pending
transfers, and every partition's references within the Box. A reintroduced
reference MUST cancel stale orphan eligibility. Each partition publishes its
held Syrups, including recently released references, so the grace period follows
the last release anywhere in the Box. GC MUST recheck its own references when
claiming an orphan. Unknown or undecodable liveness evidence MUST retain
potentially live bytes.

BookKeeper deletes whole ledgers. One live segment therefore pins the whole
Syrup. Reclaiming partially live Syrups by copying survivors is outside the
baseline capabilities.

### Allocation enumeration and retention

Allocation enumeration is an opt-in backstop for ledgers that normal manifest
tracking missed. An allocation epoch identifies a fenced writer generation and
Box incarnation; a retention pin is a durable claim that a ledger is needed.

Enumeration MUST require a retired allocating epoch, a recognized matching
identity, sufficient age, and no active retention pins before deletion. Missing,
legacy, or ambiguous evidence MUST retain the allocation. Age alone MUST NOT
retire a writer epoch. Recovery MUST fence writers and pin all recovered roots
before retiring their epochs. Failed or ambiguous pointer publication leaves
epochs protected. Recreating a Box MUST use a distinct incarnation.

Pins and permanent deletion claims MUST serialize through a coordination
compare-and-set. A deletion claim blocks new references; failed physical deletes
remain retryable. Backup integrations MUST pin before capturing roots or bytes
and abort capture on pin failure. Retention support is not a backup/restore
implementation, and enumeration does not assume legacy untagged allocations
are collectible.

## VI. Formats, limits, and bounded work

Persisted records MUST begin with a format version byte. Encoding uses big-endian
integers and LEB128 variable-length integers, with bounds-checked decoding.
Format changes MUST bump the version and retain old-format decoding.
BookKeeper entry digests protect logs, manifests, and table entries; Syrup chunks
and object parts use CRC32C for object validation.

Client and server MUST enforce these baseline limits:

- Box names contain 3–63 lowercase letters, digits, or hyphens.
- Keys use at most 1 KiB of UTF-8; user metadata totals at most 8 KiB.
- Serialized locators have a 256 KiB cap; Syrup chunk payloads default to 1 MiB.
- Protocol frames allow 16 MiB of data plus 64 KiB reserved for metadata.
- Object size has no separate default cap, but buffered requests still obey
  frame limits.
- Multipart uploads allow up to 10,000 parts, require at least 5 MiB per part
  except the last, and expire after seven days by default.

A specification changing a configurable limit MUST state its new value and
enforcement on both ends. Decoders MUST validate versions and lengths before
unsafe allocation. Wire, format, or configuration changes MUST explain mixed
versions, upgrade order, and rollback restrictions. Reading old records does
not imply old binaries can read newly written records.

Excess L0 tables MUST produce retriable `BUSY` responses. Wrong-owner routing
uses `MOVED`; a temporarily unowned partition reports unavailable, not missing.
Retry handling MUST distinguish routing failures from ambiguous outcomes.
In-flight mutations whose outcome is unknown MUST NOT be blindly replayed.

Queued work and buffered output MUST remain bounded per connection. Blocking
client calls MUST NOT run on Netty event-loop threads. Shutdown stops admission
before draining in-flight work for a bounded time.

### Configuration and observability

Properties can be overridden by environment variables, with the environment
taking precedence. Ordinary keys use `CANDYBOX_` followed by the uppercase key
with dots replaced by underscores. BookKeeper passthrough preserves the suffix's
case: `bookkeeper.client.tlsProvider` uses
`CANDYBOX_BOOKKEEPER_CLIENT_tlsProvider`.

Expose `/healthz` for liveness, `/readyz` for readiness, and `/metrics` for
Prometheus metrics. Readiness MUST fail while coordination is unreachable.
Logs SHOULD include Box, key, and ledger context without credentials.
Configuration changes MUST specify defaults and environment overrides.

## VII. Authentication and compatibility

SASL authenticates TCP clients using PLAIN or SCRAM-SHA-256. PLAIN MUST run over
TLS because it transmits the password. Credential-file rotation supports reload
without process restart. TLS protects TCP, S3, and admin connections when enabled.

Principals identify users and trusted services, with virtual groups for all users
and authenticated users. Box ACLs contain an owner and additive READ, WRITE,
READ_ACP, WRITE_ACP, and ADMIN grants. Objects carry their own owners and grants;
an object grant can permit access inside a private Box. Explicit super-users
bypass ACLs. Legacy Boxes without an ACL fall back to authenticated full access.
Copy, rename, and part-copy require source READ and destination WRITE.

The gateway verifies SigV4 header or presigned requests, authorizes the S3 caller,
and connects to nodes as a trusted gateway service principal. Nodes MUST trust
supplied owner fields only from authorized service principals. Unsigned requests
use the anonymous principal and ACL checks; an anonymous-access switch can reject
them entirely. S3 ListBuckets reveals only buckets owned by the caller.

Every admin `/api/*` route MUST enforce its configured bearer token. Metrics MUST
enforce their optional token when configured. Static dashboard files and health
probes remain open; probes expose only health status and remain plain HTTP.
Cross-origin access SHOULD use an explicit allowed origin.

ZooKeeper uses its native authentication and creator ACLs; cluster processes use
the same ZooKeeper identity. BookKeeper uses native client authentication and TLS
configuration. Ledger passwords MUST NOT substitute for end-user authentication.
Changes MUST preserve these trust boundaries when the controls are enabled.

S3 compatibility is a tested subset, not full AWS S3 parity. Supported scope
includes bucket create/head/delete, object put/get/head/delete, multi-object
delete, ordered prefix/delimiter listing, single-range reads, multipart upload,
metadata, SigV4, and ACLs. The checked-in external compatibility allowlist is a
regression contract and MUST NOT silently shrink to hide failures. Calibration
MUST record the external suite revision and endpoint.

Versioning, server-side encryption, POST object, lifecycle, bucket policy, CORS,
and conditional GET MUST NOT be advertised as implemented without a
specification, implementation, and compatibility evidence.

## Specification and delivery gates

Before implementing a behavior change, its specification MUST state:

1. The user-visible outcome, scope, non-goals, and applicable principles I–VII,
   including any guarantee deliberately amended.
2. Observable acceptance scenarios for relevant boundaries, failures, retries,
   concurrency, and recovery. State the partition and atomicity scope explicitly.
3. Effects on durable state, ownership, clocks, tombstones, retained bytes,
   authorization, resource limits, and compatibility. Explain unaffected areas
   briefly and record unresolved questions.
4. Validation, rollout, rollback, configuration, observability, and documentation
   changes needed to deliver the behavior.

Plans MUST map requirements to modules and tests. Implementation MUST satisfy
acceptance criteria and update affected user documentation before completion.
Small fixes may keep this material in an issue or PR; no particular generator
or specification directory is required.

### Testing and build rules

Unit tests MUST use JUnit 5 and AssertJ with hand-written fakes rather than
mocking frameworks. Use `InMemoryLedgerStore`,
`InMemoryCoordinationService`, `LoopbackTransport`, and `ManualClock` to model
storage, coordination, transport, and time.

Shared `LedgerStoreContract` and `CoordinationServiceContract` suites MUST run
against both fake and real implementations. Interface behavior changes MUST
update the contract. Tests named `*IT.java` belong only in
`candybox-integration-tests` and start embedded BookKeeper and ZooKeeper.
Recovery, fencing, and GC changes MUST include relevant failure-path evidence.

Builds require Java 17+ and Maven 3.9+. Run `mvn -q -DskipTests install` first
when integration tests need sibling test-jars. `mvn test` runs fast unit tests;
`mvn verify` also runs integration tests and license checks. Java source and
test files MUST carry the Apache license header; `mvn license:format` applies it.
Keep `@{argLine}` first when modifying integration-test JVM arguments so the
coverage agent remains active.

Frontend validation in `candybox-web` uses `npm install`, `npm run lint`,
`npm run format:check`, `npm test`, and `npm run build`. Maven's `frontend`
profile packages dashboard assets. Distribution script changes use
`bash candybox-dist/src/bin/test/run-bats.sh`; Helm packaging requires Helm 3.17+.
Relevant S3 changes use `compat/s3-tests/run.sh` against a running gateway;
`allowlist.txt` defines its expected pass set.

Run checks appropriate to the changed surface and report commands, outcomes, and
unrun checks accurately. Documentation-only changes require content and
whitespace checks rather than a full runtime suite.

## Scope and amendments

True chunked TCP streaming, off-owner compaction, watch-based coordination,
repartitioning, off-owner read scaling, small-object inlining, and Syrup
defragmentation are outside baseline capabilities. Multipart does not remove the
per-frame bound. A future specification may add these capabilities while
preserving the guarantees above; this list does not promise their delivery.

Supporting documentation MUST explain this constitution consistently.
Contradictions require explicit resolution, evidence, and an amendment when a
guarantee changes. Implementation drift alone does not change the requirements.

Amendments MUST accompany the change that needs them, identify the principle and
rationale, assess migration and compatibility impact, and update dependent
specifications, tests, and documentation. Changes to tunable defaults do not
automatically require an amendment; weakening a guarantee does.

Use a major version for removed or weakened guarantees, a minor version for new
principles or materially stronger requirements, and a patch version for
clarifications that do not change obligations.
