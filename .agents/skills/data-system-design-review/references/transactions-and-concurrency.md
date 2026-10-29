# Transactions and concurrency control

Use this when the design does read-modify-write, enforces invariants (no overbooking, no negative balance, unique names), updates several records together, or relies on a particular isolation level.

## What transactions are for

A transaction lets the application act as if certain failures and races cannot happen. A crash halfway through, a lost connection, or a concurrent writer is turned into a simple abort that the caller retries. Without transactions, every combination of partial failure and interleaving must be reasoned about by hand, and denormalized data drifts out of sync.

The four classic properties, in plain terms:

- **Atomic**: all of the transaction's writes happen or none do. This is about aborting safely, not about concurrency.
- **Consistent**: invariants hold. This is mostly the application's job; the database only enforces the constraints you declare.
- **Isolated**: concurrent transactions do not see each other's half-done work. How much protection you get depends on the isolation level, and the names vary between databases.
- **Durable**: committed data survives crashes, to whatever extent disks, fsync, and replication actually deliver.

When a design says "we use transactions", ask which isolation level and which database, then check the anomalies below.

## Anomalies and what prevents them

| Anomaly | What happens | Prevented by |
|---|---|---|
| Dirty read | Transaction sees another's uncommitted write, which may later roll back | Read committed and above |
| Dirty write | Transaction overwrites another's uncommitted write | Almost every database |
| Read skew | Transaction reads part of the data before another commits and part after, seeing a state that never existed | Snapshot isolation and above |
| Lost update | Two transactions read a value, both modify it, the second write erases the first | Some snapshot implementations detect it; otherwise atomic ops, locks, or compare-and-set |
| Write skew | Two transactions read the same data, each decides based on it, each writes to different records; together they break an invariant | Serializable only (or explicit locking) |
| Phantom | A write changes the result of another transaction's search query | Serializable, index-range locks, or materialized conflicts |

Read skew matters most for long reads: backups, analytics queries, and integrity checks that see a mix of old and new data.

### Lost update examples and fixes

Two workers increment a page-view counter by reading 41 and writing 42. One increment is lost.

- Use an atomic operation: `UPDATE ... SET views = views + 1`.
- Lock the row: `SELECT ... FOR UPDATE`, then update.
- Compare-and-set: `UPDATE ... SET status = 'shipped' WHERE id = ? AND status = 'packed'`, and check the affected row count. Make sure the comparison is against the current value, not a snapshot.
- In replicated stores without a single leader, locks and compare-and-set do not work; use commutative operations (counters, set adds) or keep concurrent versions and merge.

ORMs that load an object, change a field, and save the whole object are a common source of lost updates.

### Write skew examples and fixes

- Stock reservation: two orders each check that 1 unit is left in two different warehouses and each reserves from a different warehouse, but the business rule was "at most 1 unit total across both". Each transaction touched a different row, so neither saw a conflict.
- Account limit: a customer has two accounts with a combined overdraft limit. Two withdrawals, one from each account, each check the combined balance and pass.
- One-time coupon: a customer redeems a promo code from a phone and a laptop at once. Each request checks that no redemption row exists, and each inserts one.

Fixes:

- Use serializable isolation for these transactions.
- Lock the rows the decision depends on (`FOR UPDATE` on all rows read for the check).
- When the problem is a phantom (the check is "no row exists"), there is nothing to lock. Either create rows ahead of time that can be locked, or let the database enforce it: a unique constraint on `(customer_id, coupon_id)` solves the coupon case with no extra code. Prefer a declared constraint where one fits.

## Ways to get serializability

- **Literally serial execution.** One thread runs transactions one at a time against in-memory data. Works when transactions are short and run as stored procedures with no network round trips inside, and the data fits in memory. Scaling past one core needs partitioning so most transactions touch one partition.
- **Two-phase locking.** Readers block writers and writers block readers until commit. Correct and well understood, but throughput drops and tail latency rises under contention, and deadlocks must be detected and retried.
- **Optimistic serializable snapshot isolation.** Transactions run on snapshots without blocking; at commit the database checks whether anything they read has changed and aborts if so. Good when contention is low; under high contention the abort rate climbs. Long read-write transactions are more likely to abort.

Whatever the mechanism, the application must retry aborted transactions, with a limit and backoff, and only if the work inside is safe to repeat (no emails sent inside the transaction).

## Practical guidance for reviews

- Weak isolation is the default in most databases. Assume read committed unless told otherwise, and check every read-then-write path against the table above.
- Name the invariant for each critical write path, then ask how it is protected under concurrency.
- Prefer constraints the database enforces (unique, foreign key, check, exclusion) over checks in application code.
- Keep transactions short. No user think-time or remote calls inside them.
- Single-object atomicity (atomic increments, compare-and-set on one document) covers many cases in stores without multi-object transactions. Check whether the design really needs more.
- A cache in front of the database is outside the transaction. Reads from it can show states the transaction model forbids.

## Red flags

- Check-then-act logic in application code ("if count < limit then insert") with no locking or constraint.
- Read-modify-write through an ORM's load-and-save.
- "We use transactions" without naming the isolation level.
- Long-running transactions holding locks, or remote calls inside a transaction.
- No retry logic for aborts, or retry of side-effecting work.
- Uniqueness enforced only by a read check before insert.
- Invariants spanning multiple rows guarded by snapshot isolation alone.
