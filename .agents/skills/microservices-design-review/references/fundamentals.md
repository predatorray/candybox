# Fundamentals: what a microservice is and when to use them

Use this file to judge whether a service-based design is justified, and to check that the services in it have the properties that make the approach pay off.

## Working definition

A microservice is a separately deployable unit, normally its own process, that implements one business capability and exposes it over the network. Four properties do most of the work:

1. **Independent deployability.** A team can change the service, deploy it, and release the change to users without anyone else deploying at the same time. Most other benefits depend on this one. If a design does not have it, the system is distributed but still has to be released as one piece.
2. **Built around a business capability.** A service maps to something the business does (pricing, room inventory, refunds), not to a technical layer. Most feature changes then touch one service, not three.
3. **Owns its state.** The service is the only thing that reads and writes its data store. Others get data through its interface. This lets the owner change the schema without coordinating.
4. **Hides its internals.** The interface exposes as little as consumers need. Language, framework, storage engine, and internal model can change without consumers noticing.

## Size

Line count is a poor measure. Better tests:

- One team can own it, run it, and keep the whole thing in their heads.
- It could be rewritten in a few weeks if the design turned out wrong.
- A typical feature change touches only this service.

The number of services should grow with the organization's ability to run them. A team that has never run five services in production should not start with fifty. Each new service adds a pipeline, dashboards, alerts, on-call surface, and network calls that can fail.

## Kinds of monolith

"Monolith" means the whole system is deployed as one unit. That is not automatically bad.

- **Single-process monolith.** One deployable, usually one database. Simple to develop, test, deploy, and debug. Problems start when many teams contend to change and release the same artifact.
- **Modular monolith.** One deployable, but internally split into modules with enforced boundaries, ideally with each module owning its own tables or schema. Gives much of the design benefit of services without the operational cost. Often the right first step, and a good staging ground for later extraction.
- **Distributed monolith.** Many services that must change and deploy together. It has the costs of distribution and the coupling of a monolith. This is the most common failure outcome of a microservice project. Check for it in every review.

## What services can buy you, and what each benefit requires

| Benefit | Only real if |
|---|---|
| Teams ship independently | Interfaces are stable, data is not shared, and each service has its own pipeline |
| Choose different tech per service | The organization can support the extra stacks (hiring, patching, on-call knowledge) |
| Failure isolation | Callers handle slow and failed dependencies; otherwise one failure spreads |
| Scale hot parts separately | The hot part is actually a separate service with its own data path |
| Smaller, safer deploys | Releases are not batched; testing does not depend on a shared end-to-end environment |
| Reuse capabilities in new products | Interfaces are designed for consumers, not shaped by one UI |

When reviewing, check each claimed benefit against its right-hand column.

## What services cost

- **Developer experience.** Running the system locally gets harder as services multiply. Engineers need stubs, shared dev environments, or remote environments.
- **Technology sprawl.** Freedom to pick tools leads to many tools. Each needs patching, expertise, and on-call knowledge.
- **Money.** More processes, more network, more environments, more tooling. Expect short-term cost to rise.
- **Reporting.** Data spread across many stores is hard to query together. Reporting needs a deliberate design ([data-and-workflows.md](data-and-workflows.md)).
- **Troubleshooting.** A single request may cross many processes. Without aggregated logs and tracing, finding the cause takes hours ([observability.md](observability.md)).
- **Security.** More network traffic and more endpoints to protect ([security.md](security.md)).
- **Testing.** End-to-end tests become slow, flaky, and hard to own ([delivery-and-testing.md](delivery-and-testing.md)).
- **Latency.** In-process calls become network calls with serialization. Long sync chains add up.
- **Consistency.** No transactions across services. Some data will be stale for a while, and multi-step processes need explicit failure handling.

## When microservices tend to fit

- Several teams need to work on the same product without stepping on each other's releases.
- Parts of the system have very different load, availability, or compliance needs, so isolating them saves money or risk.
- The business wants to expose capabilities to many channels or partners.
- The domain is reasonably well understood, so boundaries are unlikely to move a lot.
- The organization already has, or is willing to build, automated deployment and good monitoring.

## When they tend not to

- A new product where the domain is still being discovered. Boundaries drawn now will be wrong, and moving them between services is far more expensive than moving them inside one codebase.
- A small team (one or two teams) with no contention problems. The overhead outweighs the gain.
- Software that customers install and run themselves. Asking customers to operate many services is a heavy burden.
- An organization without deployment automation or production monitoring. Fix that first.

## Questions to ask about fit

1. Which specific problem does splitting solve, and is there a cheaper fix (modular monolith, more hardware, read replicas, better CI)?
2. How many teams will own services, and how do the service counts compare?
3. What is the plan to keep services independently deployable a year from now?
4. Does the organization already run at least one service in production with automated deploys and centralized logs?
5. What would make the team stop, merge services back, or change direction?

If the answers are weak, a good review says so plainly and proposes the simpler alternative, even if the rest of the design is well drawn.
