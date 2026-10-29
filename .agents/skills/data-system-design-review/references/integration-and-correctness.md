# Integrating many stores and keeping data correct

Use this when the design writes to more than one store (database plus cache, search index, warehouse, other services), when correctness depends on retries or deduplication, or when the system holds personal data.

## Source of truth and derived data

No single product serves every access pattern well, so real systems combine a primary database, caches, search indexes, analytics stores, and more. The design must make one thing clear: **for each piece of data, which store is the source of truth, and which stores hold copies derived from it?**

- The source of truth is the copy every other copy must match. When they disagree, the other copy is wrong.
- Derived data (caches, indexes, materialized views, aggregates, ML features) can be rebuilt from the source. If it cannot be rebuilt, it is not derived; it is a second source of truth, and that needs justifying.

If the design has two stores that both accept writes for the same data, flag it.

## The dual-write problem

Application code writes to the database, then writes to the search index (or cache, or another service). This breaks in two ways:

- **Partial failure**: the first write succeeds and the second fails, or the process crashes between them. The stores now disagree with no record that they do.
- **Races**: two clients update the same record. Client A writes X to the database, client B writes Y to the database, B writes Y to the index, A writes X to the index. Database says Y, index says X, permanently.

Fixes, in rough order of preference:

1. **Derive from one ordered log.** Write only to the source of truth; capture its changes (change data capture) and have each derived store consume them in order. All stores apply the same changes in the same order.
2. **Transactional outbox.** In the same database transaction as the business write, insert an event into an outbox table. A separate relay publishes outbox rows to the message broker. The event is published if and only if the write committed. Consumers must handle duplicates.
3. **Write to an event log first**, and have every store (including what was the primary database) consume from it. Strongest decoupling, but reads of your own write go through a consumer, so plan for read-your-writes.

Distributed transactions across heterogeneous stores are a last resort (see `consistency-and-consensus.md`).

Caches have the same problem. "Update database, then delete cache key" still races with concurrent readers repopulating the cache with stale data. Use short TTLs as a backstop, versioned cache entries, or invalidation driven by the change stream.

## Batch and stream together

A common, healthy arrangement:

- streams keep derived views fresh as changes happen
- batch reprocessing rebuilds views from the full history when logic changes or bugs are found

Changing a derived view without downtime: build the new view alongside the old one from the same input, compare them, shift readers over gradually, and keep the old one until the new one is proven. This only works if the raw input is kept. Prefer one codebase for batch and streaming logic where the tools allow it, so the two paths do not drift.

## Idempotency end to end

Retries happen at every layer: the browser resubmits, the load balancer retries, the client library retries, the consumer reprocesses after a crash. TCP-level or database-level deduplication only covers their own hop. **Duplicate suppression has to be end to end.**

Pattern:

- The client generates a unique request ID (a UUID) once, when the user initiates the action, and sends it with every retry.
- The server records the ID with the effect, in the same transaction, under a unique constraint. A second request with the same ID returns the original result instead of applying the effect again.
- The ID travels downstream with every derived event so consumers can deduplicate too.

Check that payment, order creation, and other non-repeatable operations have this. "We retry on failure" without it means "we sometimes double-charge".

## Enforcing constraints without global coordination

Uniqueness ("one account per email"), capacity ("no more than 200 seats"), and balance rules need someone to decide between conflicting requests. Options:

- **Single decider per key.** Partition requests by the constrained key (email, seat ID, account) into a log partition. One consumer processes each partition in order and decides. Scales across keys, needs no locks. Requests that touch several keys can be split into steps, each handled by the right partition, linked by the request ID.
- **Database constraint** on a single node or partition.
- **Accept then compensate.** Some constraints can be checked after the fact: allow the action, detect a violation shortly afterward, and fix it (cancel, refund, apologize, offer a substitute). A ride-hailing app that briefly assigns two drivers to one rider can cancel one of them. This is valid when the cost of the occasional violation is lower than the cost of coordinating every request. Make the compensation an explicit, tested path.

## Timeliness vs integrity

These two get lumped together as "consistency" but differ in importance:

- **Timeliness**: readers see recent data. Violations are temporary; waiting fixes them.
- **Integrity**: data is not lost, duplicated, corrupted, or contradictory. Violations are permanent until someone repairs them.

Most products tolerate brief staleness. Few tolerate a double charge or a lost order. A design that spends heavily on timeliness (synchronous cross-region writes) while being careless about integrity (no idempotency, dual writes) has its priorities backwards. Event-log architectures with idempotent consumers can give strong integrity with relaxed timeliness and less coordination.

## Verify, do not assume

Software has bugs; disks and memory corrupt data rarely but not never; replication and migrations have edge cases. Build in checks:

- Reconciliation jobs that compare the source of truth with derived stores and report differences.
- Checksums on stored and transferred data.
- Periodic restore tests: backups that have never been restored are a hope, not a backup.
- End-to-end audit: for important data (money, inventory), keep an append-only record that lets you recompute balances and compare.

A review should ask: "How would you find out if the index and the database disagreed? How long until someone noticed?"

## Privacy and responsibility

Data systems affect people. A reviewer should raise:

- **Data minimization**: is every collected field needed for a stated purpose? Data not collected cannot leak.
- **Retention**: is there a deletion schedule? Logs and event streams are often kept forever by default.
- **Deletion requests**: when a user asks to be deleted, how is their data removed from the primary store, replicas, caches, search indexes, event logs, warehouses, and backups? Immutable logs make this hard. Options include per-user encryption keys that can be destroyed, or compaction that rewrites history without the deleted records.
- **Access control and auditing** for sensitive fields.
- **Automated decisions**: if data drives decisions about people (credit, hiring, pricing, moderation), are there checks for bias, a way for people to see why, and a way to appeal?

## Red flags

- Application code writing to two stores in sequence with no outbox or change capture.
- No stated source of truth, or derived data that cannot be rebuilt.
- Non-idempotent operations retried at any layer.
- Request deduplication only at one hop.
- Cache invalidation logic with an obvious race and no TTL backstop.
- Raw history discarded, so derived views cannot be rebuilt.
- No reconciliation or audit for money, inventory, or other critical data.
- Personal data in immutable logs with no deletion strategy.
