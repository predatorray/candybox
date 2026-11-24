# Distributed styles

Covers: service-based architecture, microservices, in-memory data-grid architectures, and centrally orchestrated service buses. Event-driven designs have their own file (`event-driven.md`). Costs shared by every distributed design are in `distributed-tradeoffs.md`; read that too.

## Service-based (coarse domain services)

A handful of separately deployed services, each covering a large business area (for a parking-operator platform: Bookings, Payments, Site Operations, Reporting), usually behind one UI or a few, often sharing one database. Typical count is somewhere between four and a dozen.

Why it is popular: you get much of the benefit of splitting (independent deploys per area, a failure in Reporting does not take down Bookings, scale only the services that need it) without most of the pain. Each service is big enough that a business request is handled inside one service, so local ACID transactions still work and services rarely call each other.

Things to check:

- **Inter-service calls.** If services call each other often, the boundaries are wrong or the style is drifting toward microservices without the supporting work.
- **The shared database.** One shared library of entity classes used by every service means every schema change forces every service to rebuild. Better: split the schema into logical areas, give each area its own small data-access library, and have each service depend only on the areas it uses. Treat tables used by all services as a controlled shared contract with a named owner.
- **Splitting the database.** Possible per service or per group of services, but only when the data is not needed elsewhere; otherwise you have created cross-service queries or data copies.
- **UI and API layer.** One UI, a UI per area, or an API gateway in front. A gateway is useful for external exposure and shared concerns like auth and metrics.
- **Security zones.** A common good move is separate deployments and databases for public-facing and internal parts, with one-way data flow from internal to public.

## Microservices

Many small services, each modeling one bounded business context and owning its own data. The guiding goal is independence: teams can change, deploy and scale their service without coordinating with others. To get that, the style accepts duplication over sharing. Two services may each have their own `Address` type rather than depend on a common one.

What a sound design looks like:

- **Boundaries follow business capabilities.** Each service has one clear purpose.
- **Data isolation.** No shared tables. Other services get data by asking the owner, by subscribing to its events, or by keeping a local replica. Pick one owner for each fact.
- **Sizing by behavior, not by line count.** Services that need to be updated atomically together, or that call each other on every request, belong together. "Micro" is a label, not a size target. Too-small services are the most common failure: they create chatty call graphs, distributed transactions, and long latency chains.
- **No business logic in the API gateway.** The gateway routes, authenticates, rate-limits. Workflow logic belongs inside services; a gateway that orchestrates becomes a central bottleneck owned by nobody.
- **Shared operational concerns delivered once.** Logging, metrics, tracing, mTLS, retries and circuit breaking via a sidecar, service mesh, or common platform, so upgrades do not require every team to change code.
- **Frontends.** Either one UI calling many services through the gateway, or micro-frontends where each service team also owns its slice of UI. The second extends team ownership but is harder to keep visually and functionally coherent.
- **Coordination.** Choreography (services react to each other's events, no central controller) keeps services decoupled but makes the overall workflow hard to see and errors hard to handle. Orchestration (a dedicated coordinator service for one workflow) centralizes control and error handling at the cost of coupling to the coordinator. Use a per-workflow orchestrator for complex flows with compensation; use choreography for simple fan-out. Watch for a service that nominally participates in choreography but has drifted into being the orchestrator for everyone.
- **Transactions.** If many workflows need atomic updates across services, the services are too fine. Merge them. Use sagas only for the few cases where two parts truly need separate services and still must agree (see `distributed-tradeoffs.md`).
- **Polyglot freedom used sparingly.** Teams may choose a different datastore or language when it fits their problem, but each extra stack raises the cost of operations and hiring.

Prerequisites: automated provisioning, CI/CD per service, centralized logging and tracing, good on-call practice. Without these, the style is a net loss.

## In-memory data grid with async persistence

For extreme or unpredictable concurrency where the database is the bottleneck (ticket drops, flash sales, live bidding), some designs take the database off the synchronous path. Each processing unit keeps the working data in memory, replicated across instances. Updates go to the database asynchronously through a queue. Units start and stop with load. On cold start, data is reloaded from the database.

It gives large elasticity, at real cost:

- **Collisions.** With several active instances updating the same keys, two updates can cross during replication and one overwrites the other. The risk grows with the number of instances, the update rate on hot keys and the replication lag, and shrinks as the data spreads over more keys. Estimate it at peak load. For data that must be exact (stock counts, seat inventory), use a central distributed cache or partition the keys so one instance owns each key.
- **Replicated vs central cache.** Replicated in-process caches are fast and survive a node loss, but each instance holds all the data, so they suit small, mostly static data. A central distributed cache holds large or fast-changing data consistently, but adds a network hop and a dependency. Many designs need both for different data.
- **Durability.** Updates live in memory until the async writer persists them. Check the queue is durable and the writer is idempotent, or a crash loses data.
- **Testing and cost.** Load testing at the target scale is hard and often ends up happening in production. Caching products and the extra instances cost money.

Use this style only when a simpler design with caching and a well-tuned database clearly cannot meet the elasticity target.

## Central service bus with layered services

An older enterprise pattern: services organized into a reuse-driven taxonomy (business-facing services, shared enterprise services, app-specific services, infrastructure services) with a central orchestration engine routing every call and managing transactions. It aimed for maximum reuse of shared building blocks.

It is mostly a cautionary example today, and its failure modes still appear in new designs:

- The central engine becomes both a single operational unit (nothing can be more available or scalable than it) and an organizational bottleneck (one integration team must approve every change).
- Maximum reuse of domain services means maximum coupling. A change to a shared `Customer` service ripples into every consumer, which forces coordinated testing and releases.
- A single business change spreads across many services and tiers.
- Shared canonical models grow to carry every consumer's fields.

If a new design has a central bus or gateway where workflow logic lives, or a "canonical" domain service that everything must call, raise these points.

## Red flags

- Services sharing tables while the doc claims independent deploys.
- A shared domain-entity library across services.
- Services that cannot answer a request without calling three others synchronously.
- Cross-service transactions in many workflows.
- Business logic in the API gateway or service bus.
- Microservices without per-service CI/CD, tracing and centralized logs.
- An in-memory grid holding exact counters replicated active-active with no collision analysis.
- A canonical model service that every team depends on.
