# Example review

A worked example showing the expected depth and format. The design is invented.

## The design under review

A company sells a fleet-maintenance product to delivery-van operators. Today it is one Kotlin application with one PostgreSQL database, built by six engineers. Vans send telemetry (location, engine codes, mileage) every 10 seconds; about 8,000 vans today, with a sales target of 60,000 in 18 months. Fleet managers use a web dashboard to see vehicle health and schedule repairs; mechanics use a tablet app to close work orders.

The proposal: split into 11 microservices (Telemetry, Vehicle, Driver, Fleet, Alert, WorkOrder, Parts, Mechanic, Notification, Reporting, Billing), all on the existing PostgreSQL instance "for now", talking over REST. The dashboard's vehicle-health page calls Fleet, which calls Vehicle, which calls Telemetry and Alert. Work orders are created by Alert publishing an event that WorkOrder, Parts and Notification each react to. Stated goals: "scalability and faster delivery".

## Review

**Summary.** A telemetry ingestion and fleet-maintenance product moving from a single application to eleven services. Verdict: needs rework. The real scaling problem is ingestion, which grows 7.5x; the rest of the product has modest load. The proposal splits everything but keeps the shared database and adds synchronous chains, so it pays the full cost of distribution while leaving the ingestion bottleneck where it is. Top risks: ingestion writes saturating the shared database; a four-hop synchronous chain on the main dashboard page; a work-order flow with no owner for failures.

**1. blocker: ingestion load lands on the shared database.**
Where: Telemetry service, shared PostgreSQL.
Problem: 60,000 vans at one message per 10 seconds is 6,000 writes per second sustained, plus bursts when vans reconnect after dead zones and flush buffers. These writes share one instance with every transactional query in the product. When ingestion falls behind, dashboard queries and work-order updates slow down with it. Splitting the code into services does not split this load. Hurts scalability and availability of the whole product.
Options: (a) Give ingestion its own path: a durable log in front (Kafka or a managed equivalent) and a time-series or columnar store owned by Telemetry, with only derived vehicle state written to the main database. Cost: one new store and a stream to operate. (b) Partition the telemetry table and move it to a separate PostgreSQL instance. Cheaper, buys perhaps a year. Recommend (a), because ingestion is the one part with clearly different needs.

**2. major: eleven services do not match the team or the load.**
Where: overall decomposition.
Problem: six engineers will own eleven services, each with its own pipeline, alerts and on-call surface. Apart from ingestion, the services share one database and call each other synchronously, so they deploy, scale and fail together. The "faster delivery" goal is not served: a schema change still needs coordinated deploys, and debugging now crosses network hops. Hurts deployability and operability.
Options: (a) Keep one deployable unit for the product side, reorganized as a modular monolith with modules matching the proposed services and enforced boundaries (no cross-module table access, dependency checks in CI); carve out ingestion only (finding 1). (b) A few coarse services (Ingestion, Fleet Operations, Billing) with separate schemas. Recommend (a) now, with (b) as a later step if a second team forms. Write down the trigger for splitting, such as a second team or a module needing different scale.

**3. major: four-hop synchronous chain on the vehicle-health page.**
Where: dashboard → Fleet → Vehicle → Telemetry and Alert.
Problem: each hop adds network latency and a failure point. If Telemetry is slow because of ingestion load (finding 1), the main dashboard page times out. The page's availability can be no better than the least available service in the chain. No latency budget or timeouts are given. Hurts performance and availability.
Options: (a) Maintain a precomputed vehicle-health read model updated from the telemetry stream and alert events; the page reads one place. (b) Call Telemetry and Alert in parallel from one aggregator with strict timeouts and partial results. Recommend (a); it also removes dashboard reads from the ingestion path.

**4. major: work-order creation has no owner for failures.**
Where: Alert event consumed by WorkOrder, Parts, Notification.
Problem: if WorkOrder fails on an event, Parts still reserves parts and Notification still tells the fleet manager a repair is booked. Nobody tracks whether the work order exists, and the event cannot simply be replayed because Parts would reserve twice. Hurts data integrity.
Options: (a) Make WorkOrder the owner: it consumes the alert, creates the order in its own transaction, then publishes `work-order-created`, which Parts and Notification react to. (b) A small orchestrator for the flow with persisted state. Recommend (a); the flow is linear and has one natural owner. In either case: idempotent consumers, a dead-letter queue, and an outbox so the event is published only if the order was saved.

**5. minor: goals have no numbers.**
Where: goals section.
Problem: "scalability and faster delivery" cannot be checked. Suggested targets to confirm: ingest 6,000 messages per second sustained and 20,000 per second for 5 minutes; vehicle-health page p95 under 800 ms; deploy the product side daily with under 10% rollback rate.

**6. minor: no decision records.**
Where: whole proposal.
Problem: the reasons for the split, the shared database "for now", and REST everywhere are not recorded. The "for now" in particular needs a record with an exit condition, or it will become permanent.
Options: one short record each for the ingestion split, the modular-monolith choice with its split trigger, and the work-order ownership.

**Open questions.**
- How long may telemetry be delayed before a fleet manager notices or an alert is late?
- Do any customers require their data to stay in a specific region?
- Is there a plan for reconnect storms when many vans come back online at once?

**Assumptions.** One message per van per 10 seconds; growth to 60,000 vans within 18 months; one team of six for that period.
