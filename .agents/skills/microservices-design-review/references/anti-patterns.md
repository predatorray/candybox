# Anti-patterns

Quick lookup of common problems in service-based designs. Each entry: how to spot it, why it hurts, what to do, and where to read more.

## Distributed monolith
- **Spot it:** releases need several services deployed together; a "release train" bundles service versions; a change in one service routinely needs changes in others.
- **Why it hurts:** you pay the cost of distribution (network, ops, debugging) and keep the coupling of a monolith.
- **Fix:** find the coupling (shared data, shared domain library, breaking interface changes, lockstep test suite) and remove it one source at a time. Consider merging services that always change together.
- **More:** [fundamentals.md](fundamentals.md), [communication.md](communication.md)

## Shared database
- **Spot it:** two or more services connect to the same schema; a reporting job reads several services' tables.
- **Why it hurts:** no service can change its schema safely; unclear who enforces rules; one service's queries slow others.
- **Fix:** one owner per table. Others use the owner's API, events, or a published reporting feed.
- **More:** [data-and-workflows.md](data-and-workflows.md)

## Splitting by technical layer
- **Spot it:** services named for layers ("API service", "logic service", "persistence service").
- **Why it hurts:** every feature crosses every service and every team.
- **Fix:** split by business capability.
- **More:** [boundaries.md](boundaries.md)

## Anemic entity services with a central brain
- **Spot it:** many CRUD services (one per entity) and one service that calls all of them and holds the rules.
- **Why it hurts:** the central service becomes a monolith with network hops; invariants that span entities can't be enforced locally; very chatty traffic.
- **Fix:** group entities that share rules into one service that owns both the data and the behavior.
- **More:** [boundaries.md](boundaries.md)

## Too many, too small
- **Spot it:** more services than engineers; services with a single function each; most changes touch several.
- **Why it hurts:** overhead of pipelines, on-call, and network calls outweighs any gain.
- **Fix:** merge into services sized around capabilities one team can own.
- **More:** [fundamentals.md](fundamentals.md)

## Deep synchronous chains
- **Spot it:** one user request passes through four or more services in series.
- **Why it hurts:** latency adds up, availability multiplies down, slowness cascades.
- **Fix:** local read models or caches, parallel fan-out, events for non-urgent work, or rethink boundaries.
- **More:** [communication.md](communication.md), [resilience.md](resilience.md)

## God orchestrator
- **Spot it:** a workflow service makes decisions that belong to the domain services it calls.
- **Why it hurts:** domain logic leaks out of its owners; the orchestrator becomes a shared bottleneck.
- **Fix:** keep the orchestrator to sequencing; push decisions into participants, or switch to choreography when several teams share the process.
- **More:** [data-and-workflows.md](data-and-workflows.md)

## Unowned multi-service process
- **Spot it:** each service "calls the next one"; no document says what happens if step three fails.
- **Why it hurts:** partial failures leave data inconsistent with no way to see or fix it.
- **Fix:** model it as a saga with an owner, compensation, idempotent steps, a correlation ID, and visible state.
- **More:** [data-and-workflows.md](data-and-workflows.md)

## Distributed transactions across services
- **Spot it:** two-phase commit or XA across service databases.
- **Why it hurts:** locks, stuck states on coordinator failure, coupled availability.
- **Fix:** move the data into one service, or use a saga.
- **More:** [data-and-workflows.md](data-and-workflows.md)

## Shared domain library
- **Spot it:** a common package with business models used by all services.
- **Why it hurts:** model changes force all services to upgrade, often together.
- **Fix:** each service owns its own model; share only technical libraries that can be upgraded independently.
- **More:** [communication.md](communication.md)

## Events that are commands, or raw table dumps
- **Spot it:** events named as instructions ("SendEmail"), or events that mirror internal table rows.
- **Why it hurts:** the emitter is coupled to a specific consumer, or every consumer is coupled to the emitter's internal schema.
- **Fix:** past-tense domain events with a designed schema; use requests for commands.
- **More:** [communication.md](communication.md)

## Business logic in the gateway or mesh
- **Spot it:** the edge gateway transforms payloads, merges calls, or applies business rules.
- **Why it hurts:** a shared component every team must change; logic is far from its owner.
- **Fix:** move aggregation into BFFs owned by UI teams, and rules into domain services.
- **More:** [communication.md](communication.md), [frontend.md](frontend.md)

## Retry storm
- **Spot it:** retries at client, mesh, and server; no backoff; no budget; no timeouts.
- **Why it hurts:** a small slowdown turns into overload that keeps the dependency down.
- **Fix:** timeouts everywhere, retries at one layer with backoff, jitter, and a budget, plus circuit breakers.
- **More:** [resilience.md](resilience.md)

## Every dependency required
- **Spot it:** the design has no degraded mode; any dependency down means an error page.
- **Why it hurts:** system availability equals the product of all dependencies'.
- **Fix:** classify dependencies as required or optional per operation; define fallbacks.
- **More:** [resilience.md](resilience.md)

## Cache layers stacked up
- **Spot it:** caches in client, gateway, and service, each with its own TTL.
- **Why it hurts:** staleness adds up; bugs are hard to diagnose; unclear which layer to flush.
- **Fix:** keep one or two cache layers at most, each with an explicit freshness bound.
- **More:** [scaling-and-caching.md](scaling-and-caching.md)

## End-to-end suite as release gate
- **Spot it:** all services must pass a shared end-to-end suite before any deploys; suite is slow and flaky.
- **Why it hurts:** changes pile up, releases get bundled, independent deploy is lost.
- **Fix:** contract tests and schema checks per service; testing in production; a small, owned end-to-end set.
- **More:** [delivery-and-testing.md](delivery-and-testing.md)

## No correlation ID
- **Spot it:** logs can't be joined across services for one request.
- **Why it hurts:** incidents take hours to diagnose.
- **Fix:** generate at the edge, propagate everywhere, log in every line.
- **More:** [observability.md](observability.md)

## Trusted internal network
- **Spot it:** services accept any call from inside the cluster; no service identity.
- **Why it hurts:** one compromised service can reach everything; confused deputy attacks.
- **Fix:** authenticate service calls, pass end-user identity downstream, authorize in the owning service.
- **More:** [security.md](security.md)

## Orphaned or multi-owned services
- **Spot it:** nobody can name the owning team; or three teams all change it.
- **Why it hurts:** nobody maintains it, or everyone queues on it.
- **Fix:** assign one owner; add a contribution model for others.
- **More:** [organization.md](organization.md)

## Big-bang rewrite
- **Spot it:** the migration plan has one cut-over date and no production traffic on new services until then.
- **Why it hurts:** all problems show up at once, under pressure, with no rollback.
- **Fix:** extract incrementally with a routing layer, toggles, and parallel runs.
- **More:** [migration.md](migration.md)

## Microservices as the goal
- **Spot it:** the design doc's motivation is "adopt microservices" or "modernize", with no measurable outcome.
- **Why it hurts:** no way to choose boundaries, order work, or know when to stop.
- **Fix:** state the outcome; compare with cheaper options.
- **More:** [fundamentals.md](fundamentals.md), [migration.md](migration.md)
