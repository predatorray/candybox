# Cross-Partition Zero-Copy Copy/Rename — Implementation Plan

## Goal

Restore the **zero-copy** promise for `copy`/`rename` across partition boundaries, and make a
cross-partition `rename` **eventually atomic** (it converges to "destination present, source gone"
even across crashes) instead of the current best-effort "copy-then-delete that can leave both keys
live forever".

Box partitioning (commit `7c1a70e`) split each Box into hash partitions, each an independent LSM
engine with its own fenced owner and its own reference-counted Syrup GC. That broke two things:

1. **Zero-copy** — a same-partition `copy`/`rename` reuses the source locator's Syrup *segments*
   verbatim (no byte copy). Across partitions the client falls back to `get`+`put` (a full byte
   copy) because Syrup GC is reference-counted *within one manifest*: the source partition's GC
   cannot see a destination partition's references, so segment sharing would let the source delete a
   Syrup the destination still points at.
2. **Atomicity** — a cross-partition `rename` is `byteCopy` + `deleteCandy`, two RPCs to two
   independently-fenced owners. A crash between them leaves both keys live, permanently.

The key insight is that **the blob layer is already physically global**: a `SegmentRef` resolves
bytes purely by `ledgerStore.openLedger(syrupId)` (a raw BookKeeper ledger read keyed only on the
globally-unique Syrup id — see `SyrupReader`), with no Box/partition scoping. So no bytes ever need
to move across partitions; what broke is purely the *bookkeeping* (GC liveness) and the
*coordination* (cross-owner atomicity). This plan fixes exactly those two.

## Decisions

1. **Box-global Syrup GC (Option A).** A Syrup is physically reclaimed only once **no partition of
   the Box references it**. Each partition owner publishes its referenced-Syrup set to coordination;
   a partition holding the Syrup deletes it only when it is in no sibling partition's published set.
   This subsumes the existing within-manifest reference counting and is safe by construction (the
   liveness decision is always recomputed from currently-committed references).
2. **Cross-partition zero-copy via a coordination relay.** The source owner stashes the source's
   `CandyLocator` in coordination (new `GetCandyLocator` op) and returns only a relay token; the
   client hands the token to the destination owner (new `ZeroCopyPut` op), which reads the relay and
   writes a destination locator reusing the source's segments verbatim. The client never carries the
   parts or ACL, so the destination need not trust it (#47). No bytes move. Plain `copy` needs nothing more.
3. **Rename intent journal, owner-local completion via a ZK rendezvous.** A cross-partition `rename`
   records a durable **rename intent** in the *source* partition's manifest (the owed conditional
   delete), the destination writes a durable **completion marker** in coordination when its
   zero-copy put commits, and the **source partition's owner finalizes its own intents** — on its
   maintenance loop and after handover replay — by reading that marker and tombstoning the source
   (LWW-conditioned on the source's HLC, so a legitimately recreated source is never clobbered).
   The destination PUT and receipt share one WAL entry; recovery republishes missing markers.
   Claimed transfers roll forward when committed. Only unclaimed CAS abort or fenced recovery
   proving no commit may abort a preparation; elapsed time alone cannot decide uncertainty.
   - *Why not the "coordinator-driven resumer" originally sketched:* completing a rename means
     deleting the source key on a possibly-different partition's owner. The architecture has **no
     server-to-server RPC** (the server module doesn't depend on the client, and `CandyboxNode` has
     no outbound transport). Owner-local completion via a ZK rendezvous delivers the identical
     roll-forward guarantee using only the existing `CoordinationService` primitives, with no new
     cross-node call path, and is strictly simpler. The coordinator (balancer holder) retains a
     no role in committing participant state; owner-local maintenance reconciles receipts and cleanup.

## Design

### Consistency

- **Zero-copy correctness** is unchanged from the same-partition path: the destination locator
  shares the source's segments; reads stream those segments directly from BookKeeper regardless of
  which partition's owner serves the read.
- **Cross-partition `copy`** is atomic-enough already (no source mutation): a crash leaves the
  destination present or absent, and the idempotency token makes retry safe.
- **Cross-partition `rename`** becomes **eventually atomic** (converges to completed). A reader
  between the destination commit and the source delete may still momentarily observe both keys —
  this is eventual, not linearizable, atomicity. A destination receipt committed atomically with
  the PUT closes the former pre-marker crash window. Convergence requires participant recovery
  and successful maintenance/coordination, but does not require the original client to retry.
- **Recreate-safety:** the source delete is LWW-conditioned on the source HLC captured at intent
  time, so a source key legitimately re-`put` after the rename began is never deleted by a delayed
  completion.

### Box-global Syrup GC (Option A)

Today `BoxEngine.recomputeOrphanSyrupsLocked` computes a partition's *referenced* set (manifest
SSTable refs ∪ in-flight multipart refs ∪ memtable refs ∪ open write Syrup) and marks any
`liveSyrups` not in it as a pending orphan; `GarbageCollector.collect(engine)` deletes orphans aged
past the grace period. A Syrup only ever lives in `liveSyrups` of the partition that *created* it,
so only that partition ever tries to delete it.

Changes:

- **Publish:** each partition owner writes its referenced-Syrup set to
  `boxes/<box>/partitions/<p>/refs` (a versioned KV) whenever the set changes materially — at
  minimum once per GC tick, and **synchronously as part of committing a `ZeroCopyPut`** (so a new
  cross-partition reference is visible before the rename's source delete can run).
- **Gate deletion globally:** `GarbageCollector` deletes an orphan Syrup `S` of partition `(box,p)`
  only if `S` is in **no** sibling partition's published refs. The local manifest live-set drop
  (`claimReclaimableSyrups`) still happens per partition and re-checks the partition's own current
  references under the write lock; only the physical `deleteLedger` is gated by the Box-global union.

Safety: a Syrup referenced cross-partition was referenced by the destination (and published) before
the rename's source delete committed. A zero-copy put can also point a partition back at a Syrup it
already marked as a pending orphan (rename `a`→`b`→`a`), so a re-referenced Syrup leaves the pending
set and the claim re-validates local references; otherwise the stale entry would delete a live Syrup.
Still open: a locator resolved before its Syrup was claimed and zero-copy-put after the delete (#65).
Leaks are already the accepted v1 failure mode (DESIGN §9d). A partition with no live owner keeps
its last-published refs in ZK (conservative), and a new owner republishes on takeover.

### Cross-partition zero-copy protocol

New wire ops:

- `GetCandyLocatorRequest(box, key)` → `CandyLocatorResponse(relayToken, hlc)` — the source owner
  writes the resolved locator, the requesting principal and the source key to
  `boxes/<box>/partitions/<p>/relays/<relayToken>` (a server-minted UUID).
- `ZeroCopyPutRequest(box, dstKey, srcPartition, relayToken, owner, grants, idempotencyToken)` →
  `HeadCandyResponse` — the destination owner reads the relay (it must belong to the same
  principal, be younger than half of `rename.intent.abandon.millis`, and a rename relay must name
  this `dstKey`), writes a destination locator reusing the
  relayed parts verbatim. A rename PUT and receipt (token, incarnation, relay token, source
  partition/key/HLC, destination key, resulting locator) commit in one WAL entry. It then publishes
  refs and a versioned commit marker. A copy deletes its relay after publication; a rename retains
  it until source acknowledgement. An uncertain append keeps its claim until fenced recovery. A copy is owned by the requester
  (`owner`/`grants` follow the plain-put rules); a rename keeps the relayed ACL.
- `CompleteRenameRequest(box, srcKey, srcPartition, renameToken, srcHlc)` → `OkResponse` — the
  source owner validates its durable intent and commit marker, conditionally tombstones the
  intent's expected HLC, acknowledges the durable delete in coordination, then clears the intent. (Same code path the maintenance loop uses; this is just the synchronous fast path.)

Client `renameCandy` (cross-partition):

1. `PrepareRename(src, dst, token)` on the source owner → relay token + `srcHlc`; records the rename
   intent and a rename relay.
2. `ZeroCopyPut(dst, srcPartition, relayToken)` on the destination owner.
3. `CompleteRename(token)` on the source owner.

Client `copyCandy` (cross-partition): `GetCandyLocator` then `ZeroCopyPut`, no intent.

### Rename intent journal

A `RenameIntent { token, srcKey, srcHlc, dstKey, dstPartition }` is stored in the **source**
partition's `ManifestState`, serialized in `ManifestEdit` as a trailing field exactly parallel to
the existing in-flight multipart upload state (`addedUploads`/`upsertParts`/`removedUploads`), and
therefore replayed on handover — a new source owner re-acquires the obligation automatically.

Lifecycle on the source owner:

- **Record** at prepare (step 1) via a manifest edit.
- **Finalize** (step 3, maintenance, or post-handover): match the marker to the durable intent,
  conditionally delete the recorded source HLC, publish ACK after that durable delete, then clear
  the intent. Destination maintenance removes receipt, relay/pin, and marker, in that order.
  Receipt removal first flushes/rotates its WAL so recovery cannot resurrect a cleaned receipt.
- **Abort** past the preparation window only by CAS-deleting an unclaimed relay. A claimed relay
  and source intent never expire. A newer destination owner may abort its predecessor's claim
  only after recover-open fences its WAL and no receipt is found. A receipt instead republishes
  the commit marker. Relays are bounded by existing per-partition count/node-byte caps; every
  cleanup step is idempotent and retryable after a crash, including after receipt removal.

Crash windows (all converge; none lose data):

| Crash point | State | Resolution |
|---|---|---|
| before step 1 durable | src live, dst absent | client retry |
| after step 1, before step 2 | src live (intent PENDING), dst absent, no marker | CAS-abort if unclaimed; otherwise destination fencing/recovery decides commit or abort |
| after step 2, before step 3 (old W2) | both keys live, intent PENDING, **marker present** | maintenance finalizes → src tombstoned |
| after step 3 | dst live, src gone | done |
| dst committed, marker not yet durable, crash | both keys live, no marker | receipt replay republishes marker → conditional source delete, no client retry |

### Coordination layout (additions)

```
boxes/<box>/partitions/<p>/refs       versioned KV: this partition's referenced-Syrup id set (Box-global GC)
boxes/<box>/renames/<token>           rename commit/ack marker: {incarnation, dstKey, srcKey, srcPartition, srcHlc, completed}
boxes/<box>/partitions/<p>/relays/<t> source locator snapshot for one zero-copy put (consumed once)
```

### Configuration

| Key | Default | Meaning |
|---|---|---|
| `rename.intent.abandon.millis` | 60000 | Age after which unclaimed preparations/relays may be CAS-aborted; claimed transfers never time out. |

Box-global GC reuses the existing `ledger.gc.grace.millis`.

## Work items

1. **protocol** — `GetCandyLocatorRequest`/`CandyLocatorResponse`, `ZeroCopyPutRequest`,
   `CompleteRenameRequest`, `PrepareRenameRequest` (or a `forRename` flag on `GetCandyLocator`);
   opcodes + `MessageCodec` encode/decode for `Part`/`SegmentRef` lists; codec round-trip tests.
2. **coordination** — `CandyboxKeys.partitionRefsKey(box, p)` and `renameMarkerKey(box, token)`;
   `children` already suffices for enumeration.
3. **lsm** — `ManifestEdit`/`ManifestState`/`ManifestSerializer` (v3): `addedRenameIntents` +
   `removedRenameIntents`, replayed on handover; `BoxEngine`: `resolveLocator(key)` (for
   `GetCandyLocator`), `zeroCopyPut(dstKey, parts, meta, acl, token)`, `recordRenameIntent`,
   `clearRenameIntent`, `listRenameIntents`, `deleteCandyConditional(key, expectedHlc)`, and a
   public `referencedSyrups()` accessor for publishing.
4. **server** — `CandyboxNode`: publish per-partition refs; Box-global GC gate in
   `GarbageCollector` (consult sibling `refs`); a rename-intent maintenance sweep (finalize via
   marker / CAS-abort unclaimed preparations) on the existing maintenance worker; replay-driven finalize after
   takeover. `NodeRequestHandler`: handlers for the new ops. `ServerConfig`/`CandyboxConfig`:
   `renameIntentAbandonMillis`.
5. **client** — `CandyboxClient.renameCandy`/`copyCandy` cross-partition zero-copy via
   `GetCandyLocator` + `ZeroCopyPut` (+ `CompleteRename` for rename); `Router` plumbing for the new
   ops (they are partition-keyed).
6. **tests** —
   - lsm: zero-copy put from relayed parts; conditional delete LWW; rename-intent record/clear/list
     + serializer round-trip + handover replay; `referencedSyrups` accessor.
   - server: Box-global GC keeps a cross-referenced Syrup alive then reclaims it once unreferenced;
     intent finalize via marker; CAS-abort unclaimed preparations; handover replays intents and receipts, republishing missing markers.
   - protocol: codec round-trips for the new messages.
   - client: cross-partition copy/rename now stay zero-copy (no `get`+`put`); the 3-step rename flow;
     idempotent retry.
   - integration: cross-partition zero-copy copy/rename through the routing transport (bytes shared,
     not re-stored); a crash-injected rename (drop step 3) converges via the maintenance sweep;
     Box-global GC across two partitions on a real BookKeeper/ZK cluster.
   - **regression:** the full existing suite stays green (same-partition zero-copy, the prior
     cross-partition byte-copy tests are updated to assert the new zero-copy behavior).
7. **docs** — DESIGN.md (§6 copy/rename, §9 GC, consistency §3), README.md, OPERATIONS.md (GC +
   rename intents + new config), the Hugo site (`architecture`, `client`, `operations`,
   `reference/configuration`), and this plan.

Out of scope (unchanged v1 simplifications): zero-copy `UploadPartCopy` (still buffers, even
same-partition); read scaling off owners; Syrup defragmentation; watch-based coordination (polling).
