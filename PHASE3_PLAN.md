# Candybox — Phase 3 Implementation Plan

**Scope:** distributed compaction & reference-counted GC. The compaction *core* (pluggable
`CompactionStrategy`, `LeveledCompactionStrategy`, `Compactor` with the bottommost + time-bound
tombstone-drop rule) already exists from Phase 1, and the **fencing-gated commit** is in place from
Phase 2 (`ManifestEdit.ownerFencingToken`, `Manifest.apply` rejects regressions). Phase 3 schedules
compaction in the background and reclaims obsoleted ledgers.

## Decisions

| # | Decision | Call |
|---|---|---|
| P1 | Who compacts a Box | **Owner-driven in v1**: the Box owner produces and commits its own compactions (the commit is fencing-gated). Distributed offload — a non-owner produces the output SSTable under a ZK task lease and the owner commits — is a documented future refinement; single-owner-per-Box already serializes manifest writes. |
| P2 | Scheduling | A per-node background worker (`compactionIntervalMillis`, `0` disables) iterates owned Boxes; a commit that loses ownership fails with `FencedException` and is skipped. |
| P3 | Leveled scoring | LevelDB-style: L0 by file count (`l0CompactionTrigger`); level N by total bytes vs a budget `levelBaseBytes × levelMultiplier^(N-1)`. |
| P4 | SSTable GC | Input ledgers removed by a committed compaction are deleted after a grace period, by the owner only, gated on its fencing token, against a committed manifest snapshot. |
| P5 | Syrup GC | Each SSTable records the Syrups it references; a Syrup unreferenced by any committed SSTable (and the memtable) past grace is whole-ledger-deleted (v1: no defrag). The pending-orphan-list optimization (avoid a full reference recompute) is future. |
| P6 | WAL GC | A rotated WAL ledger is deleted once its mutations are durable in a committed SSTable. |

## Workstreams

- **WS1 (this change)** — byte-size leveled scoring (`SSTableMeta.sizeBytes`) + a background,
  owner-driven, fencing-gated compaction worker on `CandyboxNode`.
- **WS2** — reference-counted GC of obsoleted SSTable ledgers (delete compaction inputs after grace,
  gated on the fencing token).
- **WS3** — per-SSTable referenced-Syrup tracking + orphan-Syrup GC (whole-ledger delete once dead).
- **WS4** — WAL GC + a full compaction-and-GC cycle integration test on embedded BookKeeper.

## Test strategy
- Fakes (`mvn test`): strategy scoring; the node's single-shot compaction pass; fenced-commit rejection.
- Integration (`mvn verify`): a real compaction cycle (WS4) on embedded BookKeeper, asserting input
  ledgers are deleted and data remains readable.

## WS1 status (this change)

- `SSTableMeta` carries `sizeBytes` (populated by `SSTableWriter`); `ManifestSerializer` round-trips it.
- `LeveledCompactionStrategy` scores L0 by count and level N by bytes against a per-level budget,
  compacting the most-over-budget level.
- `CandyboxNode` runs a background compaction worker (`compactionIntervalMillis`, `0` disables) over
  owned Boxes via `CompactionService`; `compactOwnedBoxesOnce()` is exposed for deterministic tests.
  A commit by a node that has lost ownership is rejected by the manifest fence and skipped.

## WS2 status (this change)

Reference-counted GC of obsoleted SSTable ledgers:
- `BoxEngine.applyCompaction` records each removed input ledger (with the time it left the committed
  manifest) in a pending-deletion set; `reclaimableSSTables(asOf)` / `forgetObsoleteSSTable(id)` expose it.
- `GarbageCollector.collect(engine)` deletes reclaimable ledgers past `ledgerGcGraceMillis` via
  `LedgerStore.deleteLedger` (idempotent on already-gone ledgers).
- `CandyboxNode` owns a `GarbageCollector`; the background worker now compacts then GCs each tick
  (`runMaintenance`), and `collectGarbageOnce()` is exposed for tests. GC runs only for Boxes this node
  still owns, so the physical delete is gated on the owner's lease (the inputs are already out of the
  fenced, committed manifest).
- New `CandyboxConfig.ledgerGcGraceMillis` (default 5 min).

Known v1 limitation: the pending-deletion set is in-memory, so input ledgers removed by a prior owner
that crashed before GC leak until an enumeration backstop is added (future). Tests: `CandyboxNodeTest`
adds a GC pass that deletes compacted input ledgers (verified via `LedgerStore.listLedgers`) while data
stays readable. `mvn test` and `mvn verify` (22 ITs) pass.
