# Batch processing

Use this when the design includes offline jobs, ETL, backfills, building search indexes or ML features, large joins, or any job that reads a bounded dataset and produces output.

## Principles that make batch jobs safe

- **Inputs are immutable.** The job reads, never modifies, its input. This means a failed or buggy job can be rerun without damage.
- **Outputs replace, not mutate.** Write the full output to a new location, then switch readers to it atomically (rename a directory, flip a pointer, swap a table alias). Keep the previous version for rollback. Avoid writing record by record into a live production database from a batch job: it overloads the database, partial failures leave half-written results, and reruns cause duplicates.
- **Jobs are deterministic where possible.** The same input gives the same output, so retries and recomputation are safe. Watch for hidden non-determinism: unordered map iteration, wall-clock time, random IDs, external lookups.
- **Composability.** Each job does one thing and writes output that any later job can read, in a common format. Pipelines are chains of these jobs, with explicit dependencies managed by a workflow scheduler.
- **Separate logic from wiring.** Job code should not know where its input comes from or where output goes. This makes testing and reuse easy.

The result is that mistakes are cheap: fix the code, rerun, compare. A review should check that a design keeps this property.

## How distributed batch frameworks work

The common shape:

1. Split input into chunks; run one task per chunk, close to where the data is stored.
2. Each task emits key-value pairs.
3. Repartition by key ("shuffle"), so all records with the same key land on the same task.
4. Process each key's group and write output.

Modern dataflow engines chain many such stages in one job and keep intermediate data in memory or local disk instead of writing every stage to distributed storage. That is faster, but a failure can require recomputing more stages. Deterministic operators make that recomputation safe; non-deterministic ones force a restart from durable checkpoints.

Higher-level APIs (dataframes, SQL) let the engine choose join strategies and push down filters. Prefer them over hand-written low-level code unless there is a reason.

## Joins at scale

Picking the join strategy is the main performance lever:

- **Sort-merge (shuffle) join.** Both sides are repartitioned and sorted by the join key, then merged. Works for any sizes. Most expensive because everything moves over the network.
- **Broadcast hash join.** One side (a lookup table, say) fits comfortably in each task's RAM. Load it into a hash table on each task and stream the large side past it. No shuffle of the large side.
- **Partitioned hash join.** Both sides are already partitioned the same way (same key, same hash, same partition count). Join partition by partition with no shuffle. Requires that upstream jobs agreed on partitioning.

Rule: do not join against a remote database one record at a time from inside a batch job. It is slow, it hammers the database, and the result depends on the database's state during the run, which breaks determinism. Take a snapshot of the database into the batch system and join locally.

### Skew

If a few keys have huge numbers of records (one very active user, a null join key), the tasks handling them run far longer than the rest and the job waits. Fixes: detect hot keys with a sampling pass and split them across several tasks (replicating the other side's matching records to each), or handle hot keys with a broadcast join. Also check for null or default keys that accidentally group millions of records.

## Outputs worth reviewing

- **Building indexes or key-value files for serving.** Build immutable files in the batch job, then load them into the serving system in bulk. Serving nodes switch to the new files atomically.
- **Feeding a database.** Bulk load into a staging table and swap, rather than many single-row writes.
- **ML features and training data.** Version the input snapshot along with the output so results can be reproduced.

## Batch systems vs analytical databases

Massively parallel analytical databases are faster for SQL queries over well-structured data because they optimize storage and execution together. Batch frameworks are better for arbitrary code, messy or unstructured input, and long jobs that must survive task failures without restarting. Many teams use both: raw data lands in cheap storage, batch jobs clean and reshape it, and the analytical database serves queries. A design should say which work goes where.

Storing raw data first and deciding how to interpret it later is useful because the producers and consumers do not have to agree on a schema up front. It is only useful if the raw data is documented and kept.

## Red flags

- Batch jobs that update records in place in a live database.
- Jobs that make remote calls per record (database lookups, HTTP calls).
- Output written directly to the location readers use, with no atomic switch or rollback.
- Non-deterministic logic in a pipeline that relies on rerunning failed tasks.
- No handling for skewed or null keys in large joins.
- Raw input deleted after processing, so a bug cannot be fixed by reprocessing.
- Pipelines with implicit dependencies (job B assumes job A finished) and no scheduler.
- No record of which input version produced which output.
