# Data models and storage engines

Use this when the design picks a database, defines a schema, adds indexes, or mixes transactional and analytical workloads.

## Picking a data model

The data model shapes what is easy and what is awkward for the life of the system. Match it to the relationships in the data.

**Relational (tables, joins, SQL).** Strong when records refer to each other in many directions (orders to products, products to categories, users to groups), ad hoc queries, and constraints. The query planner picks access paths, so new queries rarely need code changes. Default choice when unsure.

**Document (JSON-like records).** Good when data comes in self-contained trees that are loaded and saved as a unit: an order with its line items, a user profile with its settings. Better locality (one read gets everything) and a closer match to application objects. Weak at joins and at many-to-many relationships; those get pushed into application code, which often means N+1 queries or denormalized copies that drift out of sync. Watch out when the document grows without bound or when most reads need only a small part of a large document.

**Graph (nodes and edges).** Good when anything can relate to anything and queries follow paths of unknown length: social connections, fraud rings, dependency graphs, permission inheritance. Expressing "follow edges until..." in SQL is possible with recursive queries but clumsy.

**Others worth knowing:** key-value (fast point lookup, nothing else), wide-column (huge write volume, queries planned around the partition key), time-series (append-mostly, time-range queries, downsampling), search indexes (full text, relevance ranking, faceting), vector stores (similarity search).

Many systems end up using several of these, each fed from one source of truth. That is fine when the sync path is designed carefully (see `integration-and-correctness.md`).

### Normalize or duplicate?

- Store a fact in one place and refer to it by ID when it can change and many records point to it (a city name, a product title). Changes happen once; reads need a join.
- Duplicate when the data is effectively immutable at the time of writing (the price on a past invoice should not change) or when read performance demands it and you have a reliable way to keep copies in sync.
- An ID has no meaning to humans, so it never needs to change. A human-readable value stored in many places will need a mass update someday.

### Schema on write vs schema on read

"Schemaless" stores still have a schema; it lives in the application code that reads the data. Schema on write (enforced by the database) catches bad data early and documents structure. Schema on read is handy when records are truly heterogeneous or controlled by outside parties. Either way, the design should say how old-shaped records are handled after the shape changes (see `encoding-and-evolution.md`).

### Declarative vs imperative queries

Declarative queries (SQL and similar) let the engine optimize and parallelize without app changes. Imperative access (loops in app code over a key-value API) locks in one access path. Prefer declarative where the store allows it.

## How storage engines work, at the level a reviewer needs

### Two families for transactional workloads

**Log-structured (LSM-tree style).** Writes go to an in-memory sorted buffer and an append-only log; the buffer is flushed to immutable sorted files, which are merged and compacted in the background. Strengths: very high write throughput, sequential I/O, good compression. Costs: a read may check several files (bloom filters help), compaction uses I/O and can fall behind under sustained heavy writes, causing read slowdowns and disk growth. Deletes are tombstones that linger until compaction.

**Page-oriented (B-tree style).** Data lives in fixed-size pages organized as a balanced tree; updates overwrite pages in place, protected by a write-ahead log for crash recovery. Strengths: predictable reads, each key lives in one place (helps with range locks and transactions), mature tooling. Costs: random writes, write amplification from page rewrites, fragmentation.

Rule of thumb: LSM for write-heavy workloads where throughput matters most; B-tree for read-heavy workloads where steady latency matters. Benchmark with the real access pattern before trusting either rule.

Things a review should check:

- Write amplification and compaction behavior under peak write load, not just average.
- Whether background maintenance (compaction, vacuum) has headroom and is monitored.
- Crash safety: is there a write-ahead log, and is it fsynced where durability is promised?

### Indexes

- Every index speeds up some reads and slows down every write. Each one should map to a query that needs it.
- Secondary indexes on high-churn columns cost more than people expect.
- A covering index (includes the needed columns) avoids a second lookup.
- Composite indexes work for prefixes of the column list only. Multi-dimensional queries (lat/long ranges, time plus another range) need specialized indexes.
- Fuzzy and full-text search need a different structure entirely; do not fake it with `LIKE '%term%'`.

### In-memory stores

Fast mainly because they skip encoding data for disk, not only because they avoid disk reads. Durability comes from snapshots, logs, or replication. Check what happens on restart: how long does reload take, and how much data is lost?

## Transactions vs analytics

| | Transactional (OLTP) | Analytical (OLAP) |
|---|---|---|
| Typical query | Fetch or update a few records by key | Scan and aggregate millions of rows |
| Bottleneck | Seek time, lock contention | Scan bandwidth |
| Users | End users through the app | Analysts, dashboards, ML jobs |
| Data | Current state | History, often append-only |

Running heavy analytical queries on the transactional database hurts user-facing latency. The usual answer is a separate analytical store fed by extract-load jobs or change streams.

**Columnar storage** stores each column together, so a query reading 4 of 200 columns reads only those 4. Columns compress well (similar values side by side), and vectorized execution makes scans fast. Writes are slower and usually batched. Sort order within a columnar store matters: sort by the column most queries filter on.

**Star schema**: a large fact table of events (each sale, each click) with foreign keys into dimension tables (product, store, date). Simple and well supported by analytical engines.

**Precomputed aggregates** (rollups, materialized views, cubes) make known queries fast and unknown ones impossible. Keep the raw data too.

## Red flags

- A document store chosen for data full of many-to-many relationships, with joins done in app code.
- Denormalized copies with no stated mechanism to keep them in sync.
- "Schemaless" with no plan for reading old records.
- Analytics queries running against the primary transactional database.
- Indexes added "just in case", or no index plan at all for the main query paths.
- An LSM-based store under sustained heavy writes with no compaction monitoring.
- A store chosen for one feature (fast writes) while the dominant workload is something else (complex reads).
