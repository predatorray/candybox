# Choosing an architecture style

Covers: a decision procedure for picking a style, a rough comparison of common styles, matching the problem's shape to the style's shape, and why styles go in and out of fashion. Use it when the design must justify its overall shape, or when the user asks "which style should we use?"

## Inputs before choosing

Do not pick a style until these are known, at least roughly:

- **The domain**: main workflows, how tightly they depend on each other, what changes most often.
- **Quality attributes that affect structure**, and whether they differ across parts of the system (`quality-attributes.md`).
- **Data**: existing databases that must be used, how much data is shared across workflows, consistency needs.
- **Organization**: team count and skills, budget, deadlines, plans for acquisitions or new markets.
- **Engineering maturity**: automated tests, CI/CD, infrastructure as code, on-call and monitoring. Fine-grained distributed styles assume all of these. Without them the style will hurt more than help.

## Three decisions

1. **One deployable unit or several?** If one set of qualities fits everything, start with one. If parts need clearly different qualities, consider several. See `components-and-boundaries.md`.
2. **Where does data live?** In a monolith, usually one database. In a distributed design, decide which unit owns each piece of data and how others get it (ask the owner, receive events, keep a replica).
3. **How do units talk?** Default to synchronous calls because they are easier to build, test and debug. Use asynchronous messaging where you need it: to absorb load spikes, to decouple availability, to let slow work finish after responding, or to broadcast facts to unknown consumers. Async brings ordering, duplication, error-handling and consistency problems, so use it with a reason.

## Comparison

A rough guide, not a scorecard. Real implementations shift these with effort and money.

| Style | Partitioned by | Good at | Weak at | Typical fit |
|---|---|---|---|---|
| Layered monolith | Technical layer | Low cost, simplicity, familiarity | Deployability, scaling parts independently, fault isolation, staying clean as it grows | Small apps, tight budgets, unclear requirements at the start |
| Modular monolith | Business area | Low cost, simple ops, easy to split later, ACID transactions | Independent scaling and deploys, isolating failures | Most new products with one team or a few teams |
| Pipeline | Processing step | Simple composition, adding steps, testing steps alone | Interactive workloads, independent scale | ETL, document conversion, data ingestion, build tools |
| Microkernel (plug-in) | Core + plug-ins | Extensibility, isolating variant logic, product customization | Scale, fault isolation (single process) | Products with many variants: per-region rules, per-customer configs, tool ecosystems |
| Service-based | Business area, coarse services, often shared DB | Good balance of modularity and simplicity, local transactions, moderate independent deploys | Fine-grained scaling, schema changes when DB is shared | Business apps with 4 to 12 clear domains, teams wanting some independence without microservice overhead |
| Event-driven | Event processors | Throughput, responsiveness, decoupled extensions, absorbing spikes | Testability, reasoning about end state, error handling, consistency | High-volume reactive flows, many consumers of the same facts |
| Data-grid (in-memory, async persistence) | Processing units + replicated memory | Extreme elasticity and concurrency | Cost, complexity, testing at load, consistency of shared data | Very spiky, high-concurrency workloads where the database is the bottleneck |
| Microservices | Business area, fine-grained, own data | Independent deploys, scaling per service, team autonomy, fault isolation | Latency, operational cost, distributed data and transactions, debugging | Larger orgs with mature DevOps and real differences between parts |

Details and red flags per style: `monolithic-styles.md`, `distributed-styles.md`, `event-driven.md`.

## Match the shape of the problem

Some problems have the same shape as a style:

- A stable core process with many local variations (rules per state, per country, per customer tier) looks like a microkernel: core plus plug-ins.
- A sequence of transformations on data looks like a pipeline.
- Many independent business workflows with little shared state look like services.
- Many parties reacting to the same stream of facts looks like event-driven pub/sub.

And some mismatches are expensive:

- A domain with heavy semantic coupling, such as a long multi-step application form where each step depends on earlier answers, fights a fine-grained, decoupled style. Coarser services or a modular monolith fit better.
- A system that must handle huge concurrent load fights a single large monolith with one database.

When the user's system mixes shapes, a hybrid is normal: a modular monolith that publishes events, microservices with a plug-in rules service, a service-based system with one high-scale service carved out.

## Fashion

Styles get popular because they solved the pain of the previous style in a particular era and environment. Microservices became affordable once open-source stacks, containers and automated provisioning made one database per service cheap; the same design would have been absurd when every server and database needed a paid license. Before adopting a style because "everyone does it", ask whether your constraints resemble the ones that made it work elsewhere.

## Red flags

- A style chosen before the quality attributes were written down.
- Microservices for a single small team, a new product with unclear domain boundaries, or an org without CI/CD and good monitoring.
- Event-driven everything, including simple reads that need an immediate answer.
- A layered monolith expected to scale its hot path 50x on its own.
- The doc names one style but the structure is another (calls itself microservices but has one shared database and a central orchestrator).
- No mention of how the system can evolve if the first choice turns out wrong.
