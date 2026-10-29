# Stream processing

Use this when the design uses message queues or event logs, captures database changes, does event sourcing, computes windowed aggregates, joins streams, or claims exactly-once processing.

A stream is a batch input that never ends. Most batch principles still apply (immutable input, deterministic processing, rebuildable output); what changes is that you cannot wait for all the input, and time becomes a first-class problem.

## Two kinds of message broker

**Queue style (tasks).** The broker hands each message to one consumer, waits for an acknowledgment, and deletes it after. Messages can be processed in parallel by many consumers in any order. Good for job queues where order does not matter and each message is expensive. Downsides: no replay after acknowledgment, and redelivery after a consumer crash breaks order.

**Log style (partitioned append-only log).** Messages are appended to partitions and kept for a retention period. Each consumer group tracks an offset per partition. Order is guaranteed within a partition. Parallelism is capped by the partition count, since one partition is read by one consumer in a group. Good for high-throughput event streams, for feeding several independent consumers, and for replay. Downsides: one slow message blocks its partition (head-of-line blocking).

Choose by asking: does order matter, does anyone need to reread old messages, and are messages cheap or expensive to process?

Questions for any broker setup:

- What happens when producers outpace consumers? Options are drop, buffer (to what limit?), or apply backpressure. Unbounded buffering eventually runs out of memory or disk.
- What is the retention, and is it long enough to recover a consumer that has been down over a long weekend?
- Is consumer lag monitored and alerted on?
- Which key decides the partition, and does it keep together the events that must stay in order?
- Is the partition count sized for future consumer parallelism? Increasing it later changes key-to-partition mapping.

## Databases as streams

### Change data capture (CDC)

Read the database's replication log and publish every committed change as an event. Downstream systems (search index, cache, warehouse) consume the events and stay in sync, in the same order as the database. This avoids dual writes (see `integration-and-correctness.md`).

Details to check:

- An initial snapshot is needed to start a new consumer, and it must line up exactly with a log position.
- Log compaction (keep only the latest value per key) lets a topic hold a full current copy of a table, so new consumers can start from offset zero without a separate snapshot.
- Schema changes in the source table flow through to every consumer. The CDC events are now a public interface; version them.
- Deletes must be represented (tombstones), or downstream copies keep deleted data.

### Event sourcing

Store every change as an immutable domain event ("item added to cart", "order cancelled") and derive current state from the event log. The events record intent, not just the resulting row values.

Benefits: full history and audit trail, ability to build new read models later by replaying, easy debugging of "how did it get into this state". Costs: replay time grows (needs snapshots), event schemas must stay readable forever, deleting data for legal reasons is hard, and querying current state needs separate read models.

Distinguish **commands** (requests that may be rejected after validation) from **events** (facts that already happened and cannot be rejected). Validation happens before an event is written.

### Immutability in general

Keeping an append-only log of changes and deriving views from it means you can have several read-optimized views of the same data, rebuild them when logic changes, and recover from bugs by replaying. The cost is storage and the difficulty of true deletion (see the privacy section of `integration-and-correctness.md`).

## Reasoning about time

- **Event time**: when the event happened (stamped by the producer). **Processing time**: when the processor sees it. They can differ by seconds, or by days for mobile devices that were offline. Windowing by processing time gives wrong answers whenever the processor falls behind or restarts.
- **Late events**: you never know for sure that all events for a window have arrived. Options: declare a cutoff (watermark) and drop or side-output later events, or emit a result and then emit corrections when late events arrive. The design must choose and downstream consumers must handle corrections.
- **Device clocks are wrong.** For client events, estimate each device's clock offset from how its send timestamps compare with the server's receive times, and correct event times by that offset rather than trusting them raw.

Window types: tumbling (fixed, non-overlapping), hopping (fixed, overlapping), sliding (all events within some interval of each other), session (grouped by gaps in activity). The design should name which and why.

## Stream joins

- **Stream-stream join** (match search events with click events within 1 hour): both sides buffered in state for the window duration.
- **Stream-table join** (enrich each order event with customer data): keep a local copy of the table, updated from its change stream, rather than querying a database per event.
- **Table-table join** (maintain a materialized view that depends on two changing tables): both change streams feed state that emits updates to the view.

Joins over changing data depend on order. If the customer's address changes around the time of an order, which address does the order get? Pin the version on purpose (record the address version in the order event) if it matters.

## Fault tolerance and "exactly once"

A stream processor that crashes will reprocess some input. The useful goal is **effectively once**: the visible result is as if each event were processed once, even though some were processed more than once.

Ways to get there:

- **Micro-batching or checkpointing**: periodically snapshot operator state together with input offsets. On failure, restore and replay from the checkpoint.
- **Atomic commit of output with offsets**: write output and the consumer offset in one transaction, within one system.
- **Idempotent writes**: make applying an event twice have the same effect as once, for example by including the source offset or event ID and having the sink ignore ones it has seen.

The weak point is side effects outside the processor: sending emails, calling payment APIs, writing to an external store. Checkpoints cannot undo them. These need idempotency keys honored by the external system, or a deduplication table.

Any claim of "exactly-once" should be checked end to end, from producer retries to the final sink.

## Uses worth recognizing

- Monitoring and alerting (fraud signals, anomaly detection, threshold breaches).
- Pattern matching over sequences (complex event processing).
- Rolling aggregates and metrics (approximate algorithms are fine for many of these).
- Keeping derived stores and caches in sync.

## Red flags

- Queue-style broker used where consumers need replay or strict ordering.
- Ordering assumed across partitions, or a partition key that splits events that must stay ordered.
- No backpressure strategy, no lag alert, or retention shorter than realistic outage recovery.
- Windows defined by processing time for business metrics.
- No policy for late or out-of-order events.
- Per-event lookups to a remote database for enrichment.
- "Exactly-once" claims with non-idempotent external side effects.
- Event schemas with no versioning plan; deletes not propagated through CDC.
