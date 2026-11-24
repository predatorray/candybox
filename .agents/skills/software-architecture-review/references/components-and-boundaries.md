# Components and boundaries

Covers: top-level partitioning, discovering components, sizing them, the link between team structure and system structure, and deciding between one deployable unit and many. Use it when the review question is "are the boxes in the right place?"

## Top-level partitioning

The first structural decision is how the code is split at the top. Two broad options:

**By technical role.** Presentation, business logic, persistence, integration. Familiar, easy to explain, and matches how many organizations group people (frontend team, backend team, DBAs). The cost: a typical business change ("add a delivery window to checkout") touches every layer, so every change crosses every team.

**By business area.** Checkout, Catalog, Fulfillment, Accounts. Each area may have its own internal layers, but the top level follows the business. A typical change lands in one area. Teams can own areas end to end. Moving an area to its own service later is easier because its code and data are already together. The cost: shared technical concerns (customization, auditing) may be repeated in several areas, and people used to layers need to adjust.

Neither is wrong. Business-area partitioning fits most products that change along business lines, which is why it has become the more common default. Technical partitioning fits systems where the technical concern itself is what varies most, for example a reporting engine where the main changes are new output formats.

Team structure and system structure pull toward each other. Organizations tend to build systems that mirror how their teams communicate. If you want business-area components, you usually need business-area teams, and the reverse: reorganizing teams is one of the most effective ways to change an architecture. When a design and the org chart disagree, flag it; the org chart usually wins.

## Discovering components

Start from how the system is used, not from the database schema.

- **Actors and actions**: list who uses the system (customer, warehouse worker, support agent, the system itself for scheduled work) and what each does. Group related actions into components.
- **Workflows**: list the main end-to-end flows (place order, return item, restock) and the steps in each. Components form around steps that belong together.
- **Events**: list the business events that happen (order placed, payment captured, parcel delivered) and which parts react to them. Useful when the system will be message-driven.

Then iterate:

1. Draft components.
2. Assign requirements or user stories to them. Components with nothing assigned may not exist; components with half the stories are too big.
3. Check roles: does a component serve one kind of user or a mix with different needs?
4. Check quality attributes: does one component hold parts with very different needs? A "capture input" component that serves both a handful of internal operators and a million public users probably needs splitting, because the two halves need different scale, availability and security.
5. Restructure and repeat. The first draft is always wrong in places. Implementation will reveal more.

**The table-per-component trap.** A design with `CustomerManager`, `OrderManager`, `ProductManager`, one per table, usually means the workflows were never analyzed. Such components are wrappers around CRUD, give no guidance on where logic goes, and grow into tangled middles because every workflow needs several of them. If the system really is just CRUD over tables, use a framework that generates it and skip the architecture work. Otherwise, design around workflows.

## Granularity

Too fine: components or services that cannot do anything useful without calling several others. Each call adds latency, a failure point and a contract to maintain.

Too coarse: one component with many unrelated reasons to change. Tests take longer, deploys carry more risk, and unrelated teams step on each other.

Signals that two candidate pieces belong together:

- They must change state together atomically (an order and its line items).
- They call each other on almost every request.
- They share most of their data.
- They always change in the same commits.

Signals they should be apart:

- Different quality needs (scale, availability, security zone).
- Different rates of change (a pricing-rules engine changed weekly next to a ledger changed yearly).
- Different owners.
- Fault isolation: one failing should not take down the other.

Weigh both lists. If the only argument for splitting is "services should be small", do not split.

## One deployable unit or many

Group components by the operational behavior they need. A set of components that must be up together, scale together and share a database forms one operational unit, regardless of how many processes it runs in. Synchronous calls and shared databases pull components into the same unit; async messaging with separate data lets them be separate units.

Then:

- If one set of quality attributes fits the whole system, a single deployable unit (a well-modularized monolith) is usually the cheapest and safest choice. It can still be split later if the modules are clean.
- If parts need clearly different qualities (see `quality-attributes.md`), a distributed design starts to pay for itself, because it lets each part have its own scaling, failure handling and release cadence.

Many splits fail because they produce several services that still share a database and call each other synchronously. That is one operational unit with network calls added. Check what the split actually separates.

## Red flags

- One component per database table.
- Components named after technical patterns only (`Processor`, `Handler`, `Engine`) with no business meaning.
- A single component serving users with very different scale or security needs.
- Services that cannot complete any request without calling three others.
- A "split into microservices" plan where the services keep a shared schema and call each other synchronously.
- Team boundaries that cut across the proposed component boundaries.
- No iteration plan: the component design is presented as final before any code exists.
