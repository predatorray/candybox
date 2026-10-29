# Example review

A short, made-up design and the review it should get. Use it to calibrate tone, severity, and level of detail.

## The design (as submitted)

> **Loyalty points service.** Customers earn points on purchases and spend them at checkout. Expected 2M customers, 300 writes/s at peak (holiday sales), 3,000 reads/s (balance shown on every page).
>
> - Postgres primary in us-east, two async read replicas.
> - Balance stored as a `points` column on the `customers` row.
> - Earn: the order service calls `POST /earn`; we read the balance, add points, and write the new value.
> - Spend: checkout calls `POST /spend`; we read the balance from a replica, check it covers the amount, then update the primary.
> - After each change we update Redis (balance cache) and Elasticsearch (support agents search customers by balance tier).
> - The order service retries `/earn` up to 3 times on timeout.
> - We plan to shard by customer ID next year for scale.

## The review

**Summary.** A balance service on one Postgres primary with replicas, a cache, and a search index. The size is fine for a single primary. Verdict: needs rework before launch. Top risks: balances can go negative or lose points under concurrency, retries can award points twice, and the cache and search index will drift from the database.

**1. Blocker: spend check reads from a replica.**
Where: `POST /spend`.
Problem: a customer with 500 points spends 500. The replica still shows 500 for a few hundred ms. A second spend request (double-click, or two tabs) reads 500 from the replica and passes the check. Balance ends at -500.
Fix: do the check and update in one statement on the primary: `UPDATE customers SET points = points - :amt WHERE id = :id AND points >= :amt`, and treat zero rows updated as "insufficient points". No read needed.

**2. Blocker: earn is a read-modify-write.**
Where: `POST /earn`.
Problem: two purchases complete at the same moment. Both read 1,000, one adds 50 and one adds 80, and the second write wins with 1,080 instead of 1,130.
Fix: atomic increment (`SET points = points + :amt`). Same statement shape as the spend fix.

**3. Blocker: retried earn calls award points twice.**
Where: order service retry policy.
Problem: `/earn` commits, the response is lost, the order service times out and retries. Points are added again. Up to 4x on a bad network day.
Fix: require an idempotency key (the order ID works). Store earn and spend as rows in a `points_ledger` table with a unique constraint on `(order_id, type)`, inserted in the same transaction as the balance update. A duplicate insert fails, and the endpoint returns the original result.

**4. Major: a single mutable balance column with no history.**
Where: data model.
Problem: when a bug or a dispute happens, there is no record of how the balance got to its value, and no way to recompute it.
Fix: the ledger from finding 3 becomes the source of truth; the `points` column is a cached sum, updated in the same transaction. Add a nightly job that recomputes balances from the ledger and reports mismatches.

**5. Major: dual writes to Redis and Elasticsearch.**
Where: post-update step.
Problem: if the process dies after the Postgres commit, the cache keeps the old balance until something else touches the key. Two concurrent updates can land in Redis in the opposite order from Postgres, leaving the cache permanently wrong.
Fix: capture changes from Postgres (logical replication / CDC) or write to an outbox table in the same transaction, and have a consumer update Redis and Elasticsearch in commit order. Add a TTL on cache entries as a backstop. For checkout, never trust the cache for the spend decision (finding 1 already reads the primary).

**6. Minor: sharding plan is premature.**
Where: future plans.
Problem: 2M rows and 300 writes/s is small for one Postgres instance. Sharding adds cross-shard queries for support search and rebalancing work with no current need.
Fix: drop it from the plan; revisit if write load passes a measured threshold. If it happens, `customer_id` is a reasonable key since every hot path is per-customer.

**7. Question: failover data loss.**
Async replicas mean a primary failure can lose recently committed earns and spends. Is that acceptable? If a lost spend means a customer gets free goods, consider one synchronous replica, and make sure the ledger can be reconciled against order records after a failover.

**Open questions**
- Do points expire? Expiry adds a scheduled write path that must use the same atomic and idempotent pattern.
- Is the customer-facing balance allowed to be a few seconds stale? If yes, replica or cache reads are fine for display.
- Who can adjust balances manually, and is that audited?

**Assumptions:** single region; the order service can pass a stable order ID; Postgres default isolation (read committed).
