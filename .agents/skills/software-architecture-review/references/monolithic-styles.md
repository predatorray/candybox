# Monolithic styles

Covers: layered, modular monolith, pipeline, and microkernel (plug-in) architectures. All run as a single deployable unit, usually with one database. Use this file to check whether a monolithic design is well structured and whether it is being asked to do things the style handles badly.

Shared traits: cheap to build and run, simple to debug (one process, one log), local transactions, no network between parts. Shared limits: the whole thing scales, deploys and fails as one. One memory leak or runaway thread takes everything down, and restart time for large monoliths can be many minutes, which hurts availability.

## Layered

Code is grouped by technical role into horizontal layers, commonly presentation, business, persistence and database. Each layer talks only to its neighbors.

**Closed and open layers.** A closed layer must be passed through; an open layer may be skipped. Closing layers isolates change: if presentation can only reach business logic, the persistence code can be rewritten without touching the UI. Opening a layer for convenience (letting the UI query the database directly "for speed") breaks that isolation and couples the top of the stack to the bottom. Mixed designs are fine, for example an open shared-services layer that business code may use or skip, as long as the doc states which layers are open and why. Undocumented openness is how layered systems turn into tangles.

**Pass-through layers.** When most requests travel through every layer without any layer doing real work, the layers cost memory and latency for nothing. Some pass-through is normal. If it is most of the traffic, the layered style is probably the wrong fit, or the layers should be opened deliberately.

**Where it fits.** Small and simple apps, tight budgets, and as a starting point when requirements are unclear. It degrades as the app grows: every business change touches every layer, deploys get bigger and riskier, and scaling one hot path means scaling everything.

**If you might split later**, keep reuse between features low, keep inheritance shallow, and organize code inside layers by business area so it can be lifted out.

## Modular monolith

One deployable unit, partitioned at the top by business area (Orders, Inventory, Billing), each module with its own internals. This keeps the low cost of a monolith while making most changes local to one module.

What separates a good one from a big ball of code:

- Modules expose explicit interfaces; other modules do not reach into their internals.
- Each module owns its tables, even in a shared database. Cross-module joins are avoided or go through a read model.
- Dependency rules are enforced automatically (no cycles, no access to another module's internal packages). See `decisions-and-governance.md`.

With those in place, extracting a module into a service later is mostly mechanical. Without them, the modular monolith is just a monolith with nicer folder names.

## Pipeline

A chain of independent steps connected by one-way channels. Each step does one job and passes its output on. Steps come in a few kinds: sources that produce data, transformers that change it, filters or routers that decide whether and where it goes next, and sinks that write the final result.

Steps should be stateless and unaware of each other, so they can be reordered, replaced, tested in isolation and reused. Payloads between steps should be small.

**Where it fits.** ETL, log and metrics processing, document format conversion, media processing, build and deploy tooling. Any flow that is naturally "read, transform, decide, write".

**Watch for.** Steps that share state behind the scenes, steps that call back up the chain, and error handling that is not designed (what happens to one malformed record: drop, quarantine, or halt?). Even when steps run as separate workers, a pipeline that runs in one process still scales and fails as one unit.

## Microkernel (plug-in)

A minimal core handles the main flow; plug-ins add features or hold the logic that varies. The core finds plug-ins through a registry (anything from a map in code to a service registry) and talks to them through a fixed contract.

**Where it fits.** Products with many variants of the same process: tax or compliance rules per jurisdiction, pricing per customer tier, device- or format-specific handlers, IDEs and CI tools with extensions. A good sign is a core method full of `if region == ... else if region == ...`; each branch is a plug-in waiting to happen.

Good practice:

- Plug-ins do not depend on each other. If they must share something, it belongs in the core or in a shared contract.
- Plug-ins do not read the core's database directly. The core passes in the data they need, so a schema change touches only the core. A plug-in may own a private store for its own rules.
- Third-party plug-ins with their own contracts sit behind adapters, so the core speaks one contract.
- Plug-ins can be compiled in (simple, needs a redeploy to change) or loaded at runtime (hot-swappable, needs a framework and versioning discipline).

Plug-ins can also run as remote services, which allows independent scaling and async calls, but the core still sits on every request path, so the core caps scale and availability. The system also becomes distributed, with all the costs in `distributed-tradeoffs.md`.

## Red flags across monolithic styles

- A monolith expected to give one feature 10x the scale of the rest.
- Layers declared closed but bypassed in practice, or nobody knows which are open.
- Most requests pass through layers that do nothing.
- "Modular monolith" with shared tables across modules and no enforced dependency rules.
- Pipeline steps that share mutable state or call each other directly.
- Plug-ins that talk to each other or to the core database.
- Long startup times with no plan for zero-downtime deploys while the availability target is high.
