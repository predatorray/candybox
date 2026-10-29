# Distributed trade-offs

Covers: the costs every distributed design pays, latency and bandwidth arithmetic, security surface, distributed transactions and sagas, contracts and versioning, and observability. Read this for any design with more than one deployable unit.

## Wrong assumptions about the network

Teams moving from one process to many tend to assume, without saying so, that the network behaves like a function call. It does not. Check the design against each of these:

- **It is not reliable.** Requests get lost, and responses get lost after the work was done. Every remote call needs a timeout, a decision on retry, and idempotency if retried. Circuit breakers stop a failing dependency from exhausting the caller.
- **It is not instant.** A local call is nanoseconds; a remote call is milliseconds. Know the real round-trip at p50 and p99 in your environment. Ten sequential calls at 40 ms each is 400 ms before any work. Tail latency compounds: if each of five sequential dependencies has a 1% chance of a slow response, about 5% of requests are slow.
- **Bandwidth is not free.** Returning full objects when callers need two fields multiplies traffic. A 300 KB response at 3,000 requests per second is about 900 MB/s on one link. Fix with field selection, purpose-built endpoints, or contracts shaped by the consumer.
- **It is not secure.** Every service endpoint is an attack surface, including internal ones. Each needs authentication and authorization, which adds latency to every hop.
- **The topology changes.** Network gear, routes, DNS and firewall rules change under you. A "minor" network change can invalidate timeout settings overnight. Keep in touch with whoever runs the network.
- **There is more than one owner.** In a large organization many teams run parts of the network and platform. Know who to ask about latency, quotas and changes.
- **Transport costs money.** More services mean more gateways, load balancers, proxies, cross-zone traffic and egress charges. Budget for it.
- **The network is not uniform.** Different vendors, regions, cloud zones and links behave differently. Do not assume numbers measured in one zone hold in another.

## Latency budget

For any user-facing path, add up the hops. Write the budget down: target p99, then each synchronous dependency's contribution. If the sum is over budget, options are parallelizing calls, caching, denormalizing data into the caller, or merging services. Async messaging improves responsiveness (the user gets an acknowledgment quickly) without improving total processing time; be clear which one the requirement needs.

## Distributed transactions

Inside one database, a transaction makes several changes all-or-nothing. Across services with their own databases, there is no cheap equivalent. Options, from most preferred:

1. **Avoid the need.** If two services must change atomically on most requests, they are one service. Merge them.
2. **Accept eventual consistency** with a clear statement of which inconsistencies users may see and for how long, and how they are repaired.
3. **Sagas** for the few flows that need coordination: a sequence of local transactions, each with a compensating action that undoes it if a later step fails. Run them either with a coordinator that tracks state (easier to reason about, single place for error handling) or by choreographed events (looser, harder to trace). Costs to call out: compensations are often harder to write than the forward steps; intermediate states are visible to other requests (a reserved seat, a pending charge); concurrent sagas on the same data need care; and the coordination traffic adds load.

Ask in the review: for each multi-service write, what happens if step 3 of 4 fails, if the coordinator crashes mid-flow, and if a step is delivered twice?

A design where most workflows need sagas has drawn boundaries in the wrong place.

## Contracts and versioning

Between services owned by different teams, the API or message schema is the main coupling. Check:

- Who owns each contract and how changes are announced.
- Compatibility rules: additive changes only, tolerant readers, explicit versions for breaking changes.
- How long old versions are supported and how consumers are told to migrate.
- Whether contracts are tested on both sides (consumer-driven contract tests catch breaks before deploy).
- Contract shape: strict typed schemas give safety and tooling at the cost of more coordination; loose key-value contracts are easier to evolve but push validation onto every consumer.

## Observability

One process has one log. Fifty services have fifty logs in different formats. Without correlation IDs passed through every call and message, central log collection, distributed tracing and per-service metrics, root-causing a failed request takes hours. Treat these as part of the architecture, not something to add later.

## Operational coupling

A distributed design is only as decoupled as its weakest link. Check for hidden shared dependencies: one database cluster, one message broker, one auth service, one config store, one region. Each is a shared failure point that can take everything down at once, and each needs its own availability plan.

## Red flags

- Remote calls with no timeouts or with retries on non-idempotent operations.
- No latency budget for a user-facing path that crosses several services.
- Average latency quoted, tail latency unknown.
- Large response objects returned to callers that use a few fields.
- Internal endpoints without auth "because they are behind the firewall".
- Multi-service writes with no stated failure behavior.
- Sagas everywhere.
- No correlation IDs or tracing.
- One shared broker or database silently backing every service.
