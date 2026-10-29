# Partitioning (sharding)

Use this when data or write load is too large for one node and the design splits it across nodes, or when a partition key, hot key, or rebalancing plan needs review.

First check whether partitioning is needed at all. Partitioning adds cross-shard queries, rebalancing, and routing. If the data and write rate fit on one machine with comfortable headroom, a replicated single-node database is usually better.

Partitioning and replication are combined in practice: each partition has its own replicas, and each node hosts many partitions, leading some and following others.

## Choosing the partition key

The goal is even spread of both data and load, while keeping data that is queried together in one partition. Ask the author for the main query patterns, then check:

- Does the most frequent query hit one partition, or all of them?
- Can a single key get a big share of traffic?
- Do transactions or joins need to cross partitions?

### By key range

Partitions own contiguous ranges of sorted keys.

- Range scans are efficient (all events for a sensor between two times).
- Boundaries must adapt to the data, or partitions end up uneven.
- Sequential keys (timestamps, auto-increment IDs) send all new writes to the last partition. Fix by prefixing with something that spreads writes (sensor ID, then timestamp), at the cost of needing one range query per prefix to read across all of them.

### By hash of key

A hash function maps keys to partitions.

- Spreads load evenly regardless of key patterns.
- Range queries on the key now hit every partition.
- Use a hash that is stable across processes and languages. Some built-in language hashes vary per process.
- Do not use `hash(key) mod N` with N = number of nodes; adding a node moves almost every key.

### Compound keys

Hash the first part to pick the partition and sort by the rest within it. Example: partition by `account_id`, sort by `created_at`. Gives cheap range queries within one account, spread across accounts. A common and good default for multi-tenant data.

## Hot keys and skew

Hashing does not help when one key itself is hot: a celebrity account, a viral item, a tenant ten times bigger than the rest. Options:

- Split a known hot key into k sub-keys (append a random digit 0-9 on write), and read all k on read. Only do it for the keys that need it, and track which ones those are.
- Put very large tenants on dedicated partitions.
- Cache reads of hot keys in front of the store.
- Aggregate writes before they hit the store (count in memory for a second, then write once).

Ask: what is the largest single key or tenant, as a share of total traffic? If no one knows, measure before choosing.

## Secondary indexes

Secondary indexes do not map neatly to partitions. Two approaches:

- **Local index (per partition).** Each partition indexes its own records. Writes touch one partition. Reads by the secondary attribute must ask every partition and merge (scatter-gather), so tail latency grows with partition count.
- **Global index (partitioned by the indexed value).** Reads hit one index partition. A write may need to update several index partitions on other nodes, which is usually done asynchronously, so the index lags behind the data.

The design should say which it uses, and whether stale index reads are acceptable.

## Rebalancing

Partitions must move when nodes are added, removed, or overloaded. Approaches:

- **Many fixed partitions** (far more partitions than nodes, say 10-100 per node). Move whole partitions between nodes; keys never change partition. Pick the count up front with growth in mind: too few and partitions become huge; too many and per-partition overhead adds up.
- **Dynamic splitting** (split a partition when it passes a size threshold, merge when small). Adapts to data volume. Pre-split new tables, or all early writes land on one partition.
- **Partitions proportional to nodes** (each node owns a fixed number, splitting existing ones when a node joins).

Automatic rebalancing combined with automatic failure detection is risky: a slow node gets marked dead, its partitions move, the move adds load, more nodes look slow. Many teams let the system propose moves and require a human to approve them. Rebalancing should be throttled and observable.

## Routing requests

Something must know which node holds which partition:

- clients connect to any node, which forwards if needed
- a routing tier that knows the partition map
- partition-aware clients

The partition map must be agreed on by everyone, which usually means a coordination service or a gossip protocol (see `consistency-and-consensus.md`). Check what happens when a client's map is stale during a move.

## Cross-partition operations

- Queries that hit every partition get slower as you add partitions. Make the hot paths single-partition.
- Transactions across partitions need distributed commit or a redesign (see `consistency-and-consensus.md` and `integration-and-correctness.md`).
- Uniqueness on a non-partition-key attribute (unique email when partitioned by user ID) needs a separate partitioned index keyed by that attribute, with its own consistency story.

## Red flags

- Sharding proposed with no evidence one node is insufficient.
- Partition key chosen without listing the query patterns.
- Monotonic keys (timestamps, sequence numbers) as the leading part of a range-partitioned key.
- `mod N` hashing.
- No answer for the largest tenant or hottest key.
- Hot-path queries that scatter to every partition.
- A fixed partition count too small for expected growth.
- Fully automatic rebalancing triggered by failure detection.
