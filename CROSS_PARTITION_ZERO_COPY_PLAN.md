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
   the Syrup's creating partition deletes it only when it is in no sibling partition's published set.
   This subsumes the existing within-manifest reference counting and is safe by construction (the
   liveness decision is always recomputed from currently-committed references).
2. **Cross-partition zero-copy via client relay.** The client fetches the source's `CandyLocator`
   parts from the source owner (new `GetCandyLocator` op) and hands them to the destination owner
   (new `ZeroCopyPut` op), which writes a destination locator reusing the source's segments
   verbatim. No bytes move. Plain `copy` needs nothing more.
3. **Rename intent journal, owner-local completion via a ZK rendezvous.** A cross-partition `rename`
   records a durable **rename intent** in the *source* partition's manifest (the owed conditional
   delete), the destination writes a durable **completion marker** in coordination when its
   zero-copy put commits, and the **source partition's owner finalizes its own intents** — on its
   maintenance loop and after handover replay — by reading that marker and tombstoning the source
   (LWW-conditioned on the source's HLC, so a legitimately recreated source is never clobbered).
   Roll-forward only; no abort.
   - *Why not the "coordinator-driven resumer" originally sketched:* completing a rename means
     deleting the source key on a possibly-different partition's owner. The architecture has **no
     server-to-server RPC** (the server module doesn't depend on the client, and `CandyboxNode` has
     no outbound transport). Owner-local completion via a ZK rendezvous delivers the identical
     roll-forward guarantee using only the existing `CoordinationService` primitives, with no new
     cross-node call path, and is strictly simpler. The coordinator (balancer holder) retains a
     minor janitor role: reaping abandoned intents/markers past a long timeout.

## Design

### Consistency

- **Zero-copy correctness** is unchanged from the same-partition path: the destination locator
  shares the source's segments; reads stream those segments directly from BookKeeper regardless of
  which partition's owner serves the read.
- **Cross-partition `copy`** is atomic-enough already (no source mutation): a crash leaves the
  destination present or absent, and the idempotency token makes retry safe.
- **Cross-partition `rename`** becomes **eventually atomic** (converges to completed). A reader
  between the destination commit and the source delete may still momentarily observe both keys —
  this is eventual, not linearizable, atomicity, and matches what "move" usually means. The one
  residual non-atomic window (destination committed, completion marker not yet durable, then a
  crash) degrades to "both keys live", i.e. exactly today's behavior — never data loss.
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
  (`dropSyrups`) still happens per partition; only the physical `deleteLedger` is gated by the
  Box-global union.

Safety: a Syrup referenced cross-partition was referenced by the destination (and published) before
the rename's source delete committed; the grace period plus "delete only if globally unreferenced"
means a partial/crashed rename can only *retain* a Syrup (a leak), never delete a referenced one.
Leaks are already the accepted v1 failure mode (DESIGN §9d). A partition with no live owner keeps
its last-published refs in ZK (conservative), and a new owner republishes on takeover.

### Cross-partition zero-copy protocol

New wire ops (trusted intra-cluster client only):

- `GetCandyLocatorRequest(box, key)` → `CandyLocatorResponse(parts, contentType, userMetadata, hlc,
  createdAtMillis, acl)` — exposes the resolved locator's segment list so the client can relay it.
- `ZeroCopyPutRequest(box, dstKey, parts, contentType, userMetadata, acl, idempotencyToken,
  renameToken?, srcKey?, srcPartition?, srcHlc?)` → `HeadCandyResponse` — the destination owner
  writes a destination locator reusing `parts` verbatim, commits it to WAL + memtable, publishes its
  refs, and (when `renameToken` is set) writes the completion marker
  `boxes/<box>/renames/<token>` = `{dstKey, srcKey, srcPartition, srcHlc}` after the locator is
  durable.
- `CompleteRenameRequest(box, srcKey, srcPartition, renameToken, srcHlc)` → `OkResponse` — the
  source owner finalizes the rename: LWW-conditioned tombstone of `srcKey`, clear the intent, delete
  the marker. (Same code path the maintenance loop uses; this is just the synchronous fast path.)

Client `renameCandy` (cross-partition):

1. `GetCandyLocator(src)` from the source owner → parts + `srcHlc`. Also records the rename intent
   on the source owner (folded into `GetCandyLocator` via a `forRename` flag, or a dedicated
   `PrepareRename` — see work items).
2. `ZeroCopyPut(dst, parts, renameToken=token, src..., srcHlc)` on the destination owner.
3. `CompleteRename(token)` on the source owner.

Client `copyCandy` (cross-partition): steps 1–2 only, no `renameToken`, no intent.

### Rename intent journal

A `RenameIntent { token, srcKey, srcHlc, dstKey, dstPartition }` is stored in the **source**
partition's `ManifestState`, serialized in `ManifestEdit` as a trailing field exactly parallel to
the existing in-flight multipart upload state (`addedUploads`/`upsertParts`/`removedUploads`), and
therefore replayed on handover — a new source owner re-acquires the obligation automatically.

Lifecycle on the source owner:

- **Record** at prepare (step 1) via a manifest edit.
- **Finalize** (step 3, or the maintenance loop, or post-handover replay): read
  `boxes/<box>/renames/<token>`. If present → tombstone `srcKey` LWW-conditioned on `srcHlc`, clear
  the intent (manifest edit), delete the marker. If absent and the intent is older than
  `rename.intent.abandon.millis` → drop the intent (the rename never reached the destination; the
  source stays live). Idempotent throughout.

Crash windows (all converge; none lose data):

| Crash point | State | Resolution |
|---|---|---|
| before step 1 durable | src live, dst absent | client retry |
| after step 1, before step 2 | src live (intent PENDING), dst absent, no marker | maintenance abandons the intent; retry restarts |
| after step 2, before step 3 (old W2) | both keys live, intent PENDING, **marker present** | maintenance finalizes → src tombstoned |
| after step 3 | dst live, src gone | done |
| dst committed, marker not yet durable, crash | both keys live, no marker | abandoned → both live (== today's behavior, no data loss) |

### Coordination layout (additions)

```
boxes/<box>/partitions/<p>/refs       versioned KV: this partition's referenced-Syrup id set (Box-global GC)
boxes/<box>/renames/<token>           rename completion marker: {dstKey, srcKey, srcPartition, srcHlc}
```

### Configuration

| Key | Default | Meaning |
|---|---|---|
| `rename.intent.abandon.millis` | 60000 | Age after which a source-side rename intent with no completion marker is abandoned. |

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
   marker / abandon by age) on the existing maintenance worker; replay-driven finalize after
   takeover. `NodeRequestHandler`: handlers for the new ops. `ServerConfig`/`CandyboxConfig`:
   `renameIntentAbandonMillis`.
5. **client** — `CandyboxClient.renameCandy`/`copyCandy` cross-partition zero-copy via
   `GetCandyLocator` + `ZeroCopyPut` (+ `CompleteRename` for rename); `Router` plumbing for the new
   ops (they are partition-keyed).
6. **tests** —
   - lsm: zero-copy put from relayed parts; conditional delete LWW; rename-intent record/clear/list
     + serializer round-trip + handover replay; `referencedSyrups` accessor.
   - server: Box-global GC keeps a cross-referenced Syrup alive then reclaims it once unreferenced;
     intent finalize via marker; intent abandon by age; handover replays an intent and finalizes.
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
