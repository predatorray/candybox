# Communication between services

Use this file to review how services talk to each other: interaction style, API and event design, how interfaces change over time, shared code, and the infrastructure in between.

## A network call is not a method call

Designs often draw service calls as if they were function calls. They differ in ways that change the design:

- **Latency.** Each call adds network time plus serialization. Ten calls in a chain can add hundreds of milliseconds.
- **Payload size.** Passing a large object in-process is free. Over the network it costs time and bandwidth. Send what the receiver needs.
- **Partial failure.** The call can fail after the receiver did the work, and the caller can't tell. The receiver can be slow, down, or return nonsense. See [resilience.md](resilience.md).
- **Change cost.** Changing a method signature is one refactor. Changing a service interface may need several teams.

## Interaction styles

| Style | What it is | Good for | Watch out for |
|---|---|---|---|
| Synchronous request-response | Caller sends a request and waits | Reads the user is waiting on; simple commands needing an immediate answer | Temporal coupling, long chains, cascading slowness |
| Asynchronous request-response | Caller sends a request over a queue and handles the reply later, maybe on another instance | Long-running work; smoothing load spikes | Correlating replies; caller must store context |
| Events | Service announces that something happened; anyone interested reacts | Notifying many consumers; decoupling who reacts from who acts | Harder to see the whole flow; ordering, duplicates, schema contracts |
| Shared data store | One service writes files or records, others read them | Large batch transfers, data lakes, legacy integration | Hidden coupling to format; latency; ownership of the store |

Most systems mix styles. A service can expose a request-response API and also publish events. The question for each interaction is which style suits that use, not which style the whole system should use.

### Synchronous chains

Count the depth of sync calls on the main user paths. If serving one request requires A → B → C → D, then:
- Latency adds up along the chain.
- Availability multiplies: four services at 99.9% each give about 99.6% for the chain, before counting the network.
- A slow D ties up threads in C, B, and A.

Ways to shorten: have upstream services cache or keep a local copy of what they need, fan out in parallel instead of in series, move non-urgent work to events, or reconsider whether the boundaries are right (a long chain for one feature can mean low cohesion).

### Events

- **An event is a fact about the past**, named in past tense: `ReservationConfirmed`, `PaymentRefunded`. It is not an instruction to another service. If the emitter expects a particular consumer to do a particular thing, it is a command, and should probably be a request.
- **The emitter doesn't know or care who listens.** That is what makes events loosely coupled. If adding a consumer requires changing the emitter, coupling has crept back in.
- **What to put in the event.**
  - Thin events (ID only) force consumers to call back for details, which adds load and temporal coupling.
  - Full events (the data consumers need) let consumers act alone, but every field becomes part of the public contract.
  - A middle path: include the fields consumers commonly need, and treat the event schema as carefully as an API.
- **Delivery guarantees.** Assume at-least-once delivery. Consumers must be idempotent. Don't assume ordering across partitions or keys unless the broker guarantees it for your setup.
- **Publishing reliably.** Writing to the database and publishing an event are two operations; one can fail. Use a pattern that ties them together, such as writing events to an outbox table in the same transaction and relaying them.
- **Don't publish your internal model.** Emitting change-data-capture of raw tables exposes the internal schema to everyone. Publish domain events designed for consumers.

## Interface design

A good service interface:

1. **Makes backward-compatible change easy.** Adding a field or endpoint should not break anyone.
2. **Is explicit.** There is a schema or spec (OpenAPI, protobuf, Avro, JSON Schema, AsyncAPI) that says what the service accepts and returns, including events.
3. **Is technology-neutral.** Consumers are not forced into one language or framework. Avoid protocols that leak platform-specific types.
4. **Is simple for consumers.** Shaped around what consumers need, with sensible defaults and clear errors.
5. **Hides implementation.** No internal IDs, table structure, or enum values that only make sense internally.

On technology choice: RPC frameworks with generated clients (gRPC and similar) give speed and strong schemas but couple client and server tooling. HTTP/JSON APIs are widely supported and easy to inspect. GraphQL suits client-facing aggregation (see [frontend.md](frontend.md)) more than service-to-service calls. Message brokers suit events and async work. A review should check that the choice fits the use, not that one choice is used everywhere.

## Changing interfaces without lockstep releases

### Avoid breaking changes

- **Add, don't change.** New fields, new optional parameters, new endpoints. Never change the meaning or type of an existing field.
- **Tolerant readers.** Consumers read only the fields they need and ignore unknown ones. A consumer that fails on an extra field turns every addition into a breaking change.
- **Two kinds of break.** Structural breaks (a field removed or retyped) can be caught automatically. Semantic breaks (the field still exists, but "price" now includes tax) can't, and are often worse. Call out semantic changes explicitly in reviews.
- **Catch breaks in CI.** Check each schema change against the previous version for compatibility before it can merge. Consumer-driven contract tests catch more ([delivery-and-testing.md](delivery-and-testing.md)).

### When a break is unavoidable

| Approach | How it works | Verdict |
|---|---|---|
| Lockstep deploy | Producer and all consumers deploy together | Avoid. This is a distributed monolith. |
| Run two versions of the service | Old and new deployments side by side, consumers routed by version | Avoid as a long-term state. Bug fixes go to both; data may be split. |
| Support both interfaces in one service | The service serves `v1` and `v2` endpoints (or both event versions) from one deployment, with `v1` adapting to the new internals | Usually the best option. One codebase, one data set. Remove `v1` when usage drops to zero. |

Supporting the change also needs agreement between teams:
- How consumers learn about the change, and how long the old version stays.
- Who helps consumers migrate.
- How usage of the old version is tracked (log or metric per consumer identity) so that removal is based on data.

## Shared code

Inside a service, avoid duplication as usual. Across services, sharing code creates coupling:

- **Shared domain libraries** (a common `Customer` model used by every service) mean a change to the model forces all services to upgrade, often together. Prefer each service owning its own model of the concept.
- **Shared technical libraries** (logging, metrics, HTTP client setup) are fine if each service can upgrade on its own schedule and old versions keep working.
- **Generated or hand-written client libraries** are acceptable if they stay thin, the service team doesn't use them to push logic into consumers, and consumers can call the API without them.

Some duplication between services costs less than the coupling that removing it would cause.

## Finding services

- **DNS** is simple and universal. Watch TTLs and client caching during deploys.
- **Service registries** (or a container platform's built-in discovery) track live instances and update fast.
- People need to find services too: keep a catalog listing each service, its owner, its docs, and its API spec.

## Gateways and service meshes

- **API gateway** sits at the edge (north-south traffic): routing external requests, TLS termination, authentication, rate limits. Useful.
- **Service mesh** handles service-to-service traffic (east-west): mTLS, retries, timeouts, traffic shifting, metrics, often through sidecar proxies. Useful at scale, but it adds operational complexity. Small systems may not need one.
- **Keep business logic out of both.** When a gateway starts transforming payloads, combining calls, and applying business rules, it becomes a shared component every team must change, with the delivery contention that implies. Aggregation for specific clients belongs in a backend-for-frontend owned by that client's team ([frontend.md](frontend.md)).

## Review checklist

- [ ] Each interaction's style is chosen for its use and explained.
- [ ] Sync call depth on main paths is known and small.
- [ ] Events describe facts, are named in past tense, and are not commands in disguise.
- [ ] Event and API schemas are explicit and versioned.
- [ ] Consumers are tolerant readers; producers make additive changes.
- [ ] Compatibility is checked automatically in CI.
- [ ] There is a written plan for breaking changes that avoids lockstep deploys.
- [ ] No shared domain-model library forces joint upgrades.
- [ ] Gateways and meshes carry no business logic.
- [ ] Events are published reliably with respect to the owning service's database writes.

Related: [resilience.md](resilience.md), [data-and-workflows.md](data-and-workflows.md), [anti-patterns.md](anti-patterns.md).
