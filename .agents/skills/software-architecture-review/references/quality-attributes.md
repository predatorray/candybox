# Quality attributes

Covers: finding the qualities a system must have beyond its features, turning vague goals into numbers, keeping the list short, and scoping qualities to the parts of the system that need them. Read this first when a design doc has few or no concrete targets.

## What counts

Features say what the system does. Quality attributes say how well it must do it under real conditions: how many users, how fast, how often it may be down, how quickly a change can ship, how hard it is to break into. A quality earns a place in the architecture discussion when all three hold:

1. It is not a feature of the business domain itself.
2. Supporting it changes the structure (a separate component, a different deployment model, a different data store), not just the coding style.
3. Missing it would sink the product.

Security is the usual test case. Every system needs basic hygiene: encrypted transport, hashed secrets, least privilege. That is coding practice. Security becomes architectural when you must isolate something, for example keeping cardholder data in its own service and network zone so the rest of the system stays out of audit scope. If payments go to a third-party processor and you never touch card numbers, it probably is not.

## Where they come from

Look in three places:

- **Stated requirements.** "Support 50,000 concurrent drivers" is a scalability target even if nobody wrote the word.
- **Business goals.** Stakeholders talk in business terms. Translate:
  - "We want to ship features faster than competitors" means deployability, testability and modularity together. Teams often hear only "agility" and miss that slow tests or a monthly release train will cap it.
  - "We plan to acquire other companies" means integration and interoperability: open contracts, ability to plug in other systems.
  - "We have a tight budget and six months" means simplicity and feasibility. This one rules out many styles.
  - "Users are leaving because the app is flaky" means availability and fault tolerance, and often performance.
- **Domain knowledge nobody wrote down.** Ask how load really arrives. A tax-filing service gets most of its traffic in the last week before the deadline. A sports-streaming app spikes at kickoff. A payroll system runs a heavy batch twice a month and must finish before a bank cutoff. These implied facts change the design more than the stated ones.

A single business concern usually maps to several qualities. "Payroll must be correct and on time every pay period" needs more than performance. It also needs availability on the run date, the ability to resume a run that failed halfway, scalability as customers grow, and auditability so errors can be traced.

## Keep the list short

Each quality you design for adds structure and cost, and qualities pull against each other (more security checks add latency; more isolation adds operational overhead). A design that supports everything is heavy and slow to build.

One approach that works: collect candidates, then ask stakeholders to pick the three that matter most, in no particular order. Ranking a full list causes long arguments; picking three is fast and starts the right conversation. Then ask which candidate you would drop first if forced. Implicit qualities (basic availability, basic security) rarely get dropped; explicit wish-list items often do.

If the author lists ten qualities with equal weight, that is a finding.

## Make them measurable

A quality that cannot be measured cannot be reviewed. Push each one to a number and a way to check it:

- **Performance**: per-operation latency targets at a named percentile (p95, p99), not averages. An average of 80 ms can hide a p99 of 2 s. Budgets can be per page or per API, including payload size for mobile clients.
- **Scalability**: the load the system must handle with acceptable performance, and the growth path (2x next year? 10x?).
- **Elasticity**: how fast load can rise and fall. This is different from scalability. A hotel booking engine may need to handle a lot of constant traffic (scalable) without big sudden spikes. A flash-sale site must go from idle to 40x normal load in a minute (elastic). Elastic systems usually need to be scalable too, but not the reverse.
- **Availability**: convert "nines" into downtime, because people underestimate them. Roughly:

  | Target | Downtime per year | Per day |
  |---|---|---|
  | 99% | ~3.65 days | ~14 min |
  | 99.9% | ~8.8 hours | ~86 s |
  | 99.99% | ~53 min | ~9 s |
  | 99.999% | ~5 min | ~0.9 s |

  When someone asks for five nines, ask if they mean "we can never lose more than five minutes a year, including deploys and database failover". Usually they mean "it must not go down during business hours".
- **Recoverability**: how long to restore service (RTO) and how much data may be lost (RPO) after a disaster.
- **Deployability**: how often the team deploys, how long it takes, what fraction of deploys fail or roll back.
- **Testability**: how long the test suite takes and what share of a change can be verified before production. Coverage numbers alone say little; weak assertions can reach 100%.
- **Maintainability / modularity**: structural metrics such as dependency cycles, size and complexity of modules, coupling counts. See `modularity-and-coupling.md`.

Track trends as well as snapshots. A metric drifting the wrong way over months tells you more than its value today.

## Scope qualities to parts of the system

Older thinking assumed one set of qualities for the whole system. In practice, different parts need different levels. In a ride-hailing platform, the dispatch path that matches riders to drivers needs low latency and high availability; the monthly driver-earnings report needs neither. The admin console for support staff needs strong authorization but tiny scale.

For each part, ask: if this were down for an hour, who would notice and what would it cost? When the answers differ sharply, the parts are candidates for separate deployable units with their own scaling and failure behavior. When they are the same, a single deployable unit is often fine. This is one of the strongest inputs to the monolith-or-distributed decision; see `components-and-boundaries.md`.

Note that a synchronous call chains qualities together. If the dispatch service calls the pricing service synchronously, dispatch cannot be more available than pricing, and pricing must scale with dispatch. Putting them in separate services does not separate their qualities unless the call becomes asynchronous or the data is replicated.

## Red flags

- No numbers anywhere. "Highly scalable, highly available, secure" with no load, latency or downtime targets.
- Average latency targets with no percentile.
- One availability target for the whole system when parts clearly differ.
- Qualities listed but nothing in the structure supports them (claims elasticity but runs one fixed-size cluster with a single database).
- Structure supports qualities nobody asked for (a plug-in framework with one plug-in, multi-region active-active for an internal tool).
- Scalability and elasticity treated as the same thing.
- A hard external limit ignored: a third-party API capped at 200 requests per second sitting on the hot path of a system expected to spike to 5,000.
- "Agility" or "faster time to market" claimed while the release process stays manual and the test suite takes four hours.
