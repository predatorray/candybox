# Modularity and coupling

Covers: cohesion, coupling between modules and services, the different ways parts can depend on each other, reuse versus independence, and structural metrics. Use it when judging whether boundaries in a design are in the right place.

## Why it needs attention

Nobody writes "keep the modules clean" in a requirements doc, yet every long-lived codebase depends on it. Structure decays by default: each shortcut import, each shared helper, each "just this once" database read across a boundary makes the next change harder. Good modularity has to be designed and then defended (see `decisions-and-governance.md` for automated checks).

A module here means any grouping of code: a package, namespace, library, component or service. Get the logical grouping right before worrying about the physical one. A monolith with clear internal modules can later be split; a monolith where everything imports everything cannot, short of a rewrite.

## Cohesion

A cohesive module holds things that change together and need each other to do their job. The test: if you split it, would the halves have to call each other constantly to get anything done? If yes, keep them together. If the halves never touch, they were never one module.

Signs of weak cohesion:

- Grab-bag modules named `utils`, `common`, `helpers`, `misc`. Their contents share a data type or a vague theme, not a purpose. They tend to become dependencies of everything and block later splits.
- A class or service where groups of methods touch disjoint sets of fields or tables. Each group could live alone.
- Modules organized by when things run ("startup tasks", "nightly jobs") rather than what they do.

Cohesion is partly judgment. A `Shipment` module with "create shipment", "track shipment" and "list shipments for an account" is clearly cohesive. Whether "refund a shipment fee" belongs there or in `Billing` depends on how much billing knowledge the refund needs and how big each module is likely to grow. State the deciding factor in the review rather than declaring an answer.

## Coupling: how much and what kind

Counting dependencies helps: how many modules depend on this one (incoming), and how many does it depend on (outgoing). A module with many outgoing dependencies breaks when any of them changes. A module with many incoming dependencies is expensive to change because many callers feel it. A module that is both depended on heavily and full of concrete detail is the most painful to work on; it should either become more abstract (stable interfaces) or have fewer dependents.

Counting is not enough. The form of the dependency tells you more. A useful way to think about it is to ask: if I change X, what else must change at the same time to keep the system correct? Forms, roughly from easy to hard to manage:

Visible in the code (static):

- **Name**: callers depend on a function or field name. Cheap; refactoring tools rename everywhere.
- **Type**: callers depend on a type. Also cheap with a compiler.
- **Meaning**: callers must agree on what a value means, such as status code `3` meaning "suspended", or `-1` meaning "unknown". Fix by naming it (an enum or constant), which turns it back into a name dependency.
- **Position**: callers must pass arguments in the right order. `transfer(fromId, toId, amount)` with two IDs of the same type is a bug waiting to happen. Named parameters or small value types fix it.
- **Algorithm**: both sides must run the same algorithm, for example a client and server that each compute the same request signature or checksum. Changing one silently breaks the other. Put the algorithm in one shared, versioned place or move it to one side.

Visible only at runtime (dynamic), harder to see and test:

- **Order**: calls must happen in a sequence (`init()` before `start()`, header before body).
- **Timing**: correctness depends on when things run, as in race conditions.
- **Values that must change together**: several stored values must stay consistent, such as an order total in one database and its line items in another. Across processes this needs transactions or careful compensation. It is the most expensive kind to keep correct in a distributed system.
- **Shared identity**: two components must operate on the same instance of something, such as the same queue or the same row.

Three rules of thumb:

1. Prefer weaker forms. Refactor meaning, position and algorithm dependencies into name and type dependencies where you can.
2. The farther apart two pieces of code are, the weaker the coupling between them should be. Order dependencies between two classes in the same file are tolerable. The same dependency between two services owned by different teams is a recurring incident.
3. The more places a coupling reaches, the more it hurts. A strong coupling between two modules is a local problem; the same coupling across forty is structural.

## Coupling between deployable units

Inside one process, coupling is about code. Between services, add these:

- **Synchronous calls** tie the caller's availability, latency and scaling to the callee's for the duration of the call. Two services joined by blocking calls behave operationally like one unit, wherever they are deployed.
- **Asynchronous messages** let each side have different throughput and uptime, with a queue absorbing the difference. The coupling moves into the message contract and into eventual consistency.
- **Shared database** couples schema, deploy timing and often locking behavior. Services that share tables are one unit for any schema change.
- **Shared library of domain classes** (a common `Customer` model jar used by every service) means one change forces every consumer to rebuild and redeploy, or to drift onto different versions.
- **Contract shape**: returning a 60-field object when the caller needs two fields couples the caller to all sixty and wastes bandwidth.

## Reuse versus independence

Reuse is coupling. A shared component saves writing code twice and in exchange ties every consumer to its release cycle and its model of the world. That trade is good for stable, generic things (logging, auth clients, date handling) and often bad for domain concepts. A "single canonical Customer" shared by sales, support and billing ends up carrying every field any team ever needed, and nobody can change it without a meeting.

Prefer duplication of small domain models across boundaries when independence is worth more than consistency of code. Each context keeps only the fields it uses and translates at the edges. Keep sharing for cross-cutting technical concerns, ideally delivered in a way that does not force lockstep deploys (a sidecar, a versioned client, a platform service).

## Metrics worth tracking

- Dependency cycles between packages or modules. Any cycle means the modules in it can only be understood, tested and reused together.
- Incoming and outgoing dependency counts per module, and how abstract (interfaces) versus concrete each heavily used module is.
- Complexity per function (branch count). High values deserve a look, but some domains are inherently complex; the question is whether the complexity comes from the problem or from poor factoring.
- Size and churn: modules that are both large and frequently changed are where decay concentrates.

Metrics need interpretation. Use them to find places to look, then read the code.

## Red flags

- A `common` or `shared` module that every other module depends on and that contains domain logic.
- Two services that must be deployed together for most changes.
- A shared entity library across services.
- Magic values or positional parameters on a cross-team API.
- Duplicate algorithms that must stay in sync on client and server with no shared test.
- Values that must stay consistent across two databases with no stated mechanism.
- Cyclic dependencies between top-level modules.
- "Reuse" given as the main justification for a central domain service.
