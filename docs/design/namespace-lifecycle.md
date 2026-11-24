# Namespace lifecycle contract (issue #194)

Proposed implementation contract, written before code. A small number of shared,
partitioned **managed Boxes** contain many namespace identities. A managed Box has
an immutable descriptor version unsupported by older servers. Ordinary Boxes keep
their existing encoding and behavior. Every public object/multipart/copy/rename/
range path is refused on managed Boxes; only namespace operations and explicit
Box administration are supported. Direct engine access is trusted administration.

All operations for address A route by its encoded lifecycle key, including item
operations. UTF-8 hex encoding separates lifecycle keys `l/<address>` from item
keys `i/<address>/<item>` without delimiter collisions. Empty item names are
allowed. Address and encoded-key limits are validated before mutation. No timer,
Box, ZooKeeper node or in-memory map is allocated per namespace.

State machine: absent -> active(version 1, creation, idle TTL, deadline) ->
active(version + 1, renewed deadline) -> retired. Retired is terminal. A lifecycle
record is a **live PUT locator with no payload**, including after retirement;
it is never a tombstone. Compaction/GC cannot discard this identity barrier.
Duplicate creates report EXISTS; they never change TTL or deadline. Retention
cost grows with all identities ever created. Destroying a Box is explicit,
destructive administration outside identity guarantees for that Box incarnation.

Every namespace operation takes the owner engine write lock, including reads.
PUT validates bounds and writes Syrup bytes, then samples admission time again,
checks live state, and commits item locator plus renewal in one WAL batch. WAL
append is the commit point; the two memtable mutations publish under the same
lock before ACK. A lost ACK can mean committed; no exactly-once retry token is
promised. Any uncertain WAL/publication failure poisons namespace service until
owner recovery, avoiding reads or a second mutation against an incomplete view.
Payload bytes remain only in Syrups. Uncommitted payloads are existing GC orphans.

CLAIM checks version first and then time, under that same lock. A stale claim
cannot retire a renewed identity. At deadline equality the namespace is expired.
An operation observing expiry persists retirement before returning EXPIRED; a
backend failure is never interpreted as expiry. Retirement-first rejects delayed
PUT, including one paused during Syrup writing. PUT-first changes the version and
invalidates the old claim. Recovery fences the old WAL before exposing replayed
state. Publication crashes recover all or none of a batch. Reads return value
and lifecycle metadata from the same locked view.

Owner time is max(nonnegative wall milliseconds, recovered/local HLC physical
high-water). This is owner-admission time, not response time or trusted UTC.
Backward jumps cannot lower a durable high-water; observing a deadline durably
retires the identity before returning. A forward jump may expire identities early
and irreversibly. Recovery whose clock trails persisted HLC beyond the configured
maxClockSkewMillis fails closed. Clock accuracy and bounded skew are operational
requirements for correspondence to seven *real* days; HLC alone does not prove
UTC accuracy. Unobserved physical time passing is not a committed expiry event.

GET/list/DELETE/clear never renew. Listings are paginated, non-snapshot; each page
checks the lifecycle. Clear is a sequence of bounded delete batches, non-atomic
as a whole, each checking live state. Cleanup requires durable retirement, always
restarts from the first remaining item page, and uses bounded point-tombstone WAL
batches. Repetition/restart needs no persisted cursor and cannot touch another
address. A bounded per-partition lifecycle enumeration supports external sweepers;
there is no automatic per-namespace scheduler.

GC still reclaims whole Syrup ledgers after normal retention, not individual
entries; a small long-lived value can pin dead bytes in a mixed ledger. BookKeeper
entry-log reclamation adds independent lag. Do not shorten these safety windows.
WAL batch v3 and locator v3 remain unchanged; lifecycle metadata carries its own
version. Managed Box descriptor v3 and new wire opcodes require upgraded clients
and owners. Enable only after a stopped-cluster upgrade; rollback requires export
and a forward-compatible migration, never opening managed Boxes with old owners.

## Native use and operational procedure

```java
client.createNamespaceBox("sessions", 8); // provision once, not per address
var created = client.createNamespace("sessions", randomAddress, Duration.ofDays(7).toMillis());
var written = client.putNamespaceItem("sessions", randomAddress, "key", bytes);
var read = client.getNamespaceItem("sessions", randomAddress, "key");
// Inspect outcome before consuming data. read.state() belongs to the same view as read.data().
var claim = client.claimNamespaceExpiry("sessions", randomAddress, written.state().version());
if (claim.outcome().equals("RETIRED")) {
    do {
        var page = client.cleanupNamespace("sessions", randomAddress, 128);
        if (page.cursor() == null) break;
    } while (true);
}
```

`namespace(...)` also exposes HEAD, LIST, DELETE, CLEAR and per-partition ENUMERATE.
Use page sizes 1..128. For LIST/CLEAR, pass the returned opaque cursor unchanged;
for CLEANUP, always restart with no cursor. Repeated cleanup is safe. Enumeration
returns lifecycle physical keys: decode the suffix after `l/` with
`NamespaceKeys.decode`, then HEAD/CLAIM each address. Scan each partition of the
shared Box, persist the last successful page cursor externally, and restart a
scan from the beginning periodically so new identities behind the cursor are
visited. ENUMERATE, CLAIM and CLEANUP require Box ADMIN permission. The service
must schedule bounded passes externally; this change adds no scheduler.

Outcomes: OK; EXISTS (create-only conflict, including retired identity); MISSING
(no lifecycle); ITEM_MISSING (live namespace, missing item); EXPIRED (terminal);
STALE (claim version changed); NOT_DUE; RETIRED (claim succeeded or already
retired); NOT_RETIRED (cleanup refused). Backend, transport, ownership and limit
errors remain errors, never MISSING/EXPIRED. A timeout/error after a durable
append can mean committed. Read authoritative state after recovery; retrying PUT
is another write and another renewal. Retrying CREATE can return EXISTS whether
it was the original caller or another creator. No durable request-token window
or exactly-once guarantee is provided. On an uncertain namespace WAL/publication
failure, drain/recover that partition before resuming requests; do not reset the
poisoned engine in place. Recovery logs retain normal ledger/HLC context, and
server errors retain their cause chain and reference without logging payloads.

Maximum address: 128 UTF-8 bytes, nonempty, well-formed Unicode. Items can be
empty; physical key size is `3 + 2*addressBytes + 2*itemBytes`. With the default
1024-byte key limit and a 16-byte address, an item name fits 494 UTF-8 bytes.
Hex ordering preserves UTF-8 ordering. Inline values use the existing frame/data
budget and Candy size limit. TTL is 1ms..365 days per renewal, with no maximum
active lifetime. Lifecycle metadata v1 uses six small string fields, no segments;
retirement keeps the locator permanently. Capacity-plan this as O(all addresses
ever created), plus SSTable/WAL overhead, and reject new creation at the service
boundary before exhausting disk. This is not an unbounded namespace heap cache.

Prometheus namespace counters use existing node/Box-partition labels only:
renewals, rejected_claims, retirements, cleanup_batches, cleanup_items, failures.
Counters reset on recovery. Obtain cleanup backlog during the bounded sweeper
scan (retired lifecycle plus nonempty cleanup page); there is no exact global
snapshot backlog gauge. A failed scan must not be reported as an empty backlog.

The deterministic mixed-Syrup regression stores a 32768-byte dead value and a
1-byte live value in one ledger. Logical cleanup and compaction cannot reclaim
that ledger: at least 32777 bytes of payload/CRC remain for one reachable byte,
excluding BK replication/framing. Ledger pinning can last indefinitely, so there
is no finite physical-reclamation SLA. This is a source-native retention
measurement, not a real-disk or three-host reclamation-lag benchmark. BookKeeper
entry-log deletion lag and operational workload amplification need deployment
measurement independently; logical expiry correctness does not establish them.
