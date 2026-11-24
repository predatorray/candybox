# Review checklist

Use this for a fast full pass over a design. Each section names the file to open if something looks wrong. A "no" or "not stated" is a finding or an open question, not automatically a blocker; judge severity against the requirements.

## 1. Requirements (`requirements-and-load.md`)

- [ ] Source of truth named for each kind of data
- [ ] Peak load numbers stated: reads/s, writes/s, data size, growth
- [ ] Latency targets given as percentiles for named operations
- [ ] Acceptable data loss and downtime stated
- [ ] Acceptable anomalies stated (stale reads, duplicates, reordering)
- [ ] Complexity matches the numbers; no distributed design for single-node data

## 2. Data model and storage (`data-models-and-storage.md`)

- [ ] Data model fits the relationships (many-to-many, trees, graphs)
- [ ] Every denormalized copy has a sync mechanism
- [ ] Indexes map to known query paths
- [ ] Analytics workload separated from the transactional store
- [ ] Storage engine suits the read/write mix; background maintenance monitored

## 3. Evolution (`encoding-and-evolution.md`)

- [ ] Message and storage formats have schemas with compatibility rules
- [ ] Both backward and forward compatibility handled during rolling deploys
- [ ] Unknown fields preserved by old code that writes back
- [ ] Breaking changes use expand, migrate, contract
- [ ] Remote calls have timeouts; retried calls are idempotent

## 4. Replication (`replication.md`)

- [ ] Sync vs async choice stated, with the data-loss window on failover
- [ ] Failover is fenced against split brain
- [ ] Replica reads handle read-your-writes and monotonic reads where users would notice
- [ ] Multi-writer setups have an explicit conflict strategy; last-write-wins only where loss is acceptable

## 5. Partitioning (`partitioning.md`)

- [ ] Partitioning justified by numbers
- [ ] Partition key chosen from query patterns; hot paths hit one partition
- [ ] Largest tenant or hottest key estimated, with a plan
- [ ] No monotonic leading key on range partitions; no `mod N`
- [ ] Secondary index strategy (local or global) stated, with staleness
- [ ] Rebalancing is throttled and has human oversight

## 6. Concurrency (`transactions-and-concurrency.md`)

- [ ] Isolation level named for each critical path
- [ ] Every read-modify-write protected (atomic op, lock, compare-and-set)
- [ ] Every multi-row invariant protected (serializable, locks, or constraint)
- [ ] Uniqueness enforced by a constraint, not a read check
- [ ] Transactions short; aborts retried safely

## 7. Distributed failure (`distributed-failures.md`)

- [ ] Timeouts on every remote call, with backoff and jitter on retries
- [ ] Locks and leases use fencing tokens checked by the resource
- [ ] No ordering of cross-node events by wall-clock time
- [ ] Degraded (slow) nodes detected, not only dead ones
- [ ] Leadership and membership decided by a quorum or coordination service

## 8. Consistency and coordination (`consistency-and-consensus.md`)

- [ ] Consistency claims use precise terms
- [ ] Places that need linearizability identified (locks, uniqueness, cross-channel races)
- [ ] Behavior during a network partition described per operation
- [ ] No home-grown consensus; coordination service holds only small metadata
- [ ] Distributed transactions avoided or their failure modes handled

## 9. Batch (`batch-processing.md`)

- [ ] Inputs immutable and retained; outputs written to new locations and switched atomically
- [ ] No per-record remote calls inside jobs
- [ ] Join strategy and skew handling considered
- [ ] Jobs deterministic and rerunnable

## 10. Streams (`stream-processing.md`)

- [ ] Broker type fits ordering and replay needs
- [ ] Partition key preserves required ordering
- [ ] Backpressure, lag alerting, and retention defined
- [ ] Event time vs processing time chosen deliberately; late data policy set
- [ ] Exactly-once claims checked end to end, including external side effects

## 11. Integration and correctness (`integration-and-correctness.md`)

- [ ] No dual writes; derived stores fed from one ordered change stream or outbox
- [ ] End-to-end request IDs for non-repeatable operations
- [ ] Constraint enforcement strategy for each invariant
- [ ] Reconciliation, checksums, and restore tests in place
- [ ] Personal data has retention, deletion across all copies, and access control

## 12. Operations (`requirements-and-load.md`)

- [ ] Metrics reflect user impact (errors, latency, lag)
- [ ] Runbooks for adding capacity, replacing nodes, restoring backups
- [ ] Staged rollout and fast rollback for code and config
- [ ] Load tests use open-loop traffic at target rates
- [ ] Failure injection or game days planned
