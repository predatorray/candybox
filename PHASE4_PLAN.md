# Candybox — Phase 4 Implementation Plan

**Scope:** hardening — failure/recovery paths, backpressure, broader fault-injection tests, and
operational docs. Phases 0–3 built the functionality; Phase 4 proves it behaves under failure and
documents how to run and reason about it.

## Approach / decisions

| # | Decision | Call |
|---|---|---|
| H1 | Where fault injection lives | **Primarily on the in-memory fakes** (`InMemoryLedgerStore.setAvailableBookies`, the coordination fake's lease expiry / CAS conflicts) — fast, deterministic, and exactly what the fakes were built to model. A focused real-bookie-kill IT is a heavier follow-up. |
| H2 | No new heavy dependencies | Observability is lightweight in-process counters, not a metrics framework. |
| H3 | Orphans from failed mid-writes | A put that fails after writing Syrup bytes leaves an orphan Syrup; this is **reclaimed by the Phase 3 orphan-Syrup GC**, and an idempotency token makes a retry a no-op. Phase 4 verifies these, rather than adding new machinery. |

## Workstreams

- **WS1 (this change)** — fault-injection & failure-path test suite on the fakes: ack-quorum loss then
  recovery, zombie-owner fencing on flush/compaction commit, write-stall → compact → resume, and
  idempotent-retry-writes-no-second-Syrup. Fix any gaps the tests expose.
- **WS2** — a real-bookie fault-injection IT: stop/restart a bookie in the embedded cluster and assert
  writes survive quorum loss (ack-quorum still met) and fail cleanly when it is not.
- **WS3** — lightweight observability: in-process counters on the engine/node (puts, gets, flushes,
  compactions, GC deletions, stalls) exposed for operators; a logging pass.
- **WS4** — `OPERATIONS.md`: config tuning reference, failure modes & recovery, running a cluster,
  backpressure behaviour, and the documented deferred items / known limitations.

## Test strategy
- Fakes (`mvn test`): all of WS1 — deterministic failure injection at the engine/node level.
- Integration (`mvn verify`): WS2 real-bookie fault injection on the embedded cluster.

## WS1 status (this change)

`EngineFaultInjectionTest` (LSM, fakes) covers:
- ack-quorum loss fails a `putCandy` cleanly and the engine recovers once bookies return;
- a zombie owner (fenced by a new owner's recovery) cannot `flush`/commit — `FencedException`;
- write-stall returns `BUSY`, and after a compaction reduces L0 writes resume;
- a retried `putCandy` with the same idempotency token writes no second Syrup (no orphan on retry).

## WS2 status (this change)

While prototyping a real-bookie fault-injection IT against `LocalBookKeeper.removeBookie/addBookie`, a
genuine hardening bug surfaced: **`SyrupManager` could get permanently wedged** if BookKeeper sealed the
open Syrup ledger underneath it (which happens after sufficient bookie loss). Every subsequent write
re-appended to the same dead ledger and failed with `FencedException` forever.

Fix: on an append failure, the manager **abandons** the current Syrup (clears its handle + byte
counter) so the next write rolls to a fresh ledger. Already-written Candies in the abandoned Syrup
stay readable and referenced; the in-flight Candy's partial chunks are left as orphans for GC.

Tested deterministically on the fake (`SyrupTest.recoversWhenOpenSyrupIsSealedUnderneath`):
recover-open the open Syrup → the next write fails (`FencedException`) → a retry rolls to a fresh Syrup
and succeeds → the previously-written Candy is still readable from the (now sealed) old Syrup.

The real-bookie IT itself proved **flaky and pathologically slow on this embedded setup** (BookKeeper
read failover after `removeBookie` depends on watcher-propagation timing; the test JVM teardown after
bookie churn hung for tens of minutes). Decision: **drop the flaky IT** — deterministic fault-injection
coverage lives on the fakes (WS1 + this WS), and a real multi-bookie chaos harness (Jepsen-style) is
documented as future work. `mvn test` and `mvn verify` (23 ITs) pass.
