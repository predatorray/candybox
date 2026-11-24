# Data ownership and cross-service workflows

Use this file for anything about who owns data, how other services get it, and how business processes that span several services complete or fail.

## One owner per piece of data

Each service owns its data store, and nothing else reads or writes it directly. This is what lets a team change its schema, storage engine, or internal model without coordinating. A shared database is the most common way designs lose independent deployability.

Signs of trouble in a design:
- Two services connect to the same database or schema.
- A "shared data" or "common" service exposes raw tables through a generic API (a database with a network hop).
- A reporting or analytics job reads production tables of several services directly.
- A service caches another service's records and treats its copy as the source of truth.

## How other services get the data

| Need | Option | Trade-off |
|---|---|---|
| Current value, on demand | Call the owner's API | Simple and fresh. Adds latency and temporal coupling. |
| React when something changes | Owner publishes events; consumers subscribe | Loose coupling, no load on the owner per read. Consumers see changes with some delay and must handle duplicates and ordering. |
| Fast local reads of another service's data | Consumer keeps its own read model built from events | Fast and resilient. Data is eventually consistent, and the consumer must handle replay and rebuilds. |
| Bulk analytics and reporting | Owner publishes a dedicated reporting view or data feed | Keeps internal schema private. Someone has to maintain the feed as a public contract. |
| Small static reference data (country codes) | Config file, library constant, or a tiny dedicated service | Copying is fine when the data rarely changes and staleness is harmless. |

A reporting database is a good pattern when done this way: each service pushes data into a separate store, in a schema designed for reporting, which it treats as a public interface. The service's internal tables stay private.

## Transactions across services

Inside one service, use local ACID transactions as normal. Across services, they do not exist.

**Distributed transactions (two-phase commit)** try to coordinate a commit across several stores. Avoid them between services:
- Participants hold locks while waiting for the coordinator, which hurts throughput and can deadlock.
- If the coordinator fails at the wrong moment, participants are stuck or end up inconsistent.
- They tie the availability of all participants together.
- Many modern data stores and brokers do not support them.

If two pieces of data must change atomically, the strongest option is to put them in the same service. If that is not possible, use a saga.

## Sagas

A saga splits a business process into steps, each of which is a local transaction inside one service. If a later step fails, the saga runs compensating actions for the steps already done, or retries forward until it succeeds.

Example, booking a package trip: hold room (Booking) → charge card (Payments) → issue tickets (Ticketing) → send confirmation (Notifications). If Ticketing fails, compensate: refund card, release room.

Things to check in any saga design:

- **Compensation is a business action, not a rollback.** The charge happened; a refund is a new, visible event. Some actions cannot be undone at all (an email was sent, a package shipped). Plan for those: send a follow-up message, or move the step later.
- **Order steps to fail early.** Put the steps most likely to fail, and the cheapest to undo, first. Put irreversible steps last. In the example, if ticket availability is the main failure, check or reserve it before charging.
- **Backward vs forward recovery.** For each step, decide whether a failure should trigger compensation (backward) or retries until success (forward). Some failures are business outcomes ("card declined"), some are technical ("Payments timed out"). They need different handling.
- **Intermediate states are visible.** Between steps, other parts of the system can see a half-finished process. Model those states explicitly ("room held, payment pending") so that other services and users see something sensible.
- **Every step is idempotent.** Retries and duplicate messages will happen. A step called twice must not charge twice.
- **There is a correlation ID.** Every step, log line, and event carries the saga's ID so the whole process can be traced.
- **Someone can see the state.** Whether the process is stuck, and where, must be answerable without reading five services' logs.
- **Timeouts and abandonment.** Decide what happens if a step never answers: how long to wait, and who gets alerted.

### Orchestration vs choreography

**Orchestrated:** one service (the orchestrator) tells each participant what to do and tracks progress.
- Easy to follow: the whole process is in one place.
- Risk: the orchestrator collects business logic that belongs in the participants, and participants shrink to CRUD services. Keep decisions about each domain inside its own service; the orchestrator only sequences.
- Works best when one team owns the whole process.

**Choreographed:** no central coordinator. Each service listens for events and reacts ("PaymentCaptured" → Ticketing issues tickets).
- Lower coupling: services don't know about each other, only about events.
- Risk: the overall process is implicit. Nobody can see it end to end without tooling. Build a view of saga state from the events, keyed by correlation ID.
- Works well when several teams contribute to one process.

Mixing is fine: a choreographed process at the top level, with one participant running an orchestrated sub-process internally.

In reviews, the main failure to look for is a multi-service process with no explicit design at all, where each service "just calls the next one" and nobody owns what happens when step three fails.

## Consistency choices

- Decide per piece of data how stale it may be, and design to that. Not everything needs to be current.
- Keep strong invariants local. If two facts must never disagree, one service should own both.
- Make eventual consistency visible to users where it shows ("your booking is being confirmed") rather than pretending it is instant.
- See [resilience.md](resilience.md) for the consistency vs availability trade-off under network partitions.

## Review checklist

- [ ] Every data store has exactly one owning service.
- [ ] No service reads another's tables, including reporting jobs.
- [ ] Each cross-service data need uses an explicit mechanism (API, events, read model, reporting feed).
- [ ] No distributed transactions across services.
- [ ] Every multi-service process is written down as a saga with an owner.
- [ ] Each saga step has defined failure handling, compensation, or forward retry.
- [ ] Irreversible steps are late in the sequence.
- [ ] Steps are idempotent and carry a correlation ID.
- [ ] Saga state can be observed.

Related: [communication.md](communication.md) for event design, [boundaries.md](boundaries.md) for aggregates and invariants.
