# Migrating from a monolith

Use this file when the design replaces or breaks up an existing system.

## Start with the goal

"Move to microservices" is not a goal. Ask what outcome the migration is for, then check whether there is a cheaper way to get it:

| Stated goal | Cheaper options to rule out first |
|---|---|
| More teams shipping without blocking each other | Modular monolith with clear module ownership, better CI, trunk-based development |
| Scale under load | Bigger machines, more copies behind a load balancer, read replicas, caching |
| Better uptime | Redundant instances, fixing known single points of failure, better deploy practices |
| Use new technology | Rewrite one module inside the current deployable |
| Shorter time to release | Faster pipeline, smaller batches, feature toggles |

The goal also decides what to extract first and when to stop. A migration for scale extracts the hot path; a migration for team autonomy extracts the areas with the most contention between teams.

## Incremental, not big bang

Many problems with services appear only in production. A rewrite that lands everything on one cut-over date discovers them all at once, under pressure, with no way back. Prefer:

1. Pick one piece. Extract it. Put it in production with real traffic.
2. Learn from it: deployment, monitoring, data sync, team workflow.
3. Decide whether to continue, change approach, or stop.

Each step should be small enough to reverse. A plan should say how to roll back each step.

Splitting too early is a real risk. If the team does not yet understand the domain well, boundaries will be wrong, and wrong boundaries between services are expensive to move. In that case, build or restructure as a modular monolith first and extract later.

## What to extract first

Plot candidates on two axes: how much extracting them helps the goal, and how hard they are to pull out (how tangled their code and data are). Start with something reasonably easy and reasonably valuable, so the team learns on a low-risk piece and still shows progress. Leave the most tangled core for later, when the team has experience.

## Code first or data first

- **Code first:** move the logic into a new service, leave the data in the monolith's database for now. Quick wins, but the hard part is postponed. Have a plan for the data, or you end up with a shared database for good.
- **Data first:** split the schema inside the monolith first, so the module owns its tables, then move the code. Surfaces data problems (joins, integrity, transactions) early. Slower to show value.
- **Both at once:** the largest step. Usually too big.

Code first is more common; just make sure the data move is scheduled, not left as "phase 2".

## Patterns for incremental extraction

**Strangler fig.** Put a proxy or router in front of the old system. Move one capability at a time to a new service and redirect its calls there. The old system shrinks until it can be retired. Works well when the old system can be intercepted at its edges (HTTP routes, message queues). Keep the proxy dumb; it should route, not hold business logic.

**Branch by abstraction.** When the capability to extract is deep inside the monolith, first put an interface in front of it inside the code. Build the new implementation (which calls the new service) behind that interface, switch callers over with a toggle, then delete the old implementation.

**Parallel run.** Call both the old and new implementation, use the old result, and compare. Good for high-risk logic such as pricing or billing, where you want proof that results match before switching. Compare asynchronously to avoid adding latency, and be careful with side effects: only one implementation should send the email or charge the card.

**Feature toggles.** Switch traffic between old and new paths at runtime, by percentage or by user group. Makes rollback a config change instead of a deploy.

## Data concerns when splitting

Moving data out of a shared database changes things that used to be free:

- **Joins become calls.** A query that joined orders and products now needs a call to another service, or a local copy of the data the service needs. Watch for N+1 call patterns and plan bulk endpoints or local read models.
- **Foreign keys go away.** The database can no longer stop you from referencing a deleted record. Decide how each reference is handled: soft-delete, keep a snapshot of needed fields, or tolerate dangling references and handle them in code.
- **Transactions split.** What was one transaction across two tables may now span two services. See [data-and-workflows.md](data-and-workflows.md) for sagas.
- **Reporting breaks.** Reports that queried everything directly need a new source, such as a reporting database fed by the services. See [data-and-workflows.md](data-and-workflows.md).
- **Tooling.** Schema migrations now run per service. Each service needs its own migration process.

## Review checklist

- [ ] The goal is stated, and cheaper alternatives were considered.
- [ ] The first extraction is chosen for both value and ease.
- [ ] Each step is small, goes to production, and can be rolled back.
- [ ] There is a dated plan for moving the data, not only the code.
- [ ] Lost joins, foreign keys, transactions, and reports each have a replacement.
- [ ] Risky logic is verified with a parallel run or similar before switching.
- [ ] There is a point at which the team will re-evaluate whether to continue.

Related: [fundamentals.md](fundamentals.md), [boundaries.md](boundaries.md), [delivery-and-testing.md](delivery-and-testing.md) for toggles and progressive delivery.
