# Service boundaries

How to judge where the lines between services are drawn. Boundaries are the most expensive decision to change later, so give them the most attention in a review.

## What a good boundary looks like

**Information hiding.** A service should expose a small, stable interface and keep everything else private: its data model, storage, helper logic, and internal workflow. The less a consumer can see, the more the owner can change without telling anyone. Ask of every field and endpoint: does some consumer need this, or is it exposed because it was easy?

**High cohesion.** Things that change together live together. If a typical feature request ("let guests add a late check-out") needs changes in three services, the code for that capability is spread too thin. Look at the last few features or the planned roadmap and count how many services each one touches.

**Low coupling.** A change in one service should rarely force a change in another. Coupling can't be zero, since services that work together must know something about each other, but it should be deliberate and minimal.

Cohesion and coupling pull on each other. Splitting cohesive code across services raises coupling between them. Stable boundaries usually come from grouping by business capability, because business capabilities change for business reasons that tend to stay inside one area.

## Kinds of coupling, from loosest to tightest

Use this ladder to classify what you see. Moving a dependency one rung down is usually worth doing.

1. **Domain coupling.** Service A calls service B because it needs B's capability. Normal and unavoidable. Watch for one service that depends on very many others; it may be doing too much.

2. **Temporal coupling.** A can only work while B is up and responding right now. Common with sync calls. Reduce it with async messaging, caching, or by letting A complete work and reconcile later. See [communication.md](communication.md).

3. **Pass-through coupling.** A sends data to B only because B passes it on to C. Now a change to what C needs ripples through A and B. Fixes, in rough order of preference:
   - Let A talk to C directly, if that does not drag workflow logic into A.
   - Make B fully own the concept, so A gives B its own inputs and B builds whatever C needs.
   - If B must pass it through, treat it as an opaque blob that B never parses.

4. **Shared data coupling.** Several services read or write the same table, file, cache key space, or config record. Any schema change, or any change to what values mean, can break others. It is also unclear who enforces the rules. Most serious when more than one service writes. Fix by giving the data a single owner that others call or subscribe to. Read-only shared reference data (country codes) is lower risk, but still needs an owner.

5. **Reaching into internals.** A service reads or changes another's private store directly, or depends on its internal behavior. The owner can no longer change its own implementation safely. Treat this as a blocker when team autonomy is a goal.

## Domain modeling, the parts that help most

You don't need a full methodology. These ideas are enough to find and test boundaries.

**Shared language.** Code, APIs, and docs should use the words the business uses. When the design invents its own vocabulary, or the same word means different things in different services without anyone noticing, boundaries are probably confused.

**Aggregates.** A cluster of data with its own lifecycle and rules, usually a real-world thing with states: a reservation goes from held to confirmed to checked-in to completed or cancelled. One service owns each aggregate and is the only place its state changes. Other services can ask it to change state, and the owner may refuse. Rules that must hold at every instant ("a room can't be double-booked for the same night") belong inside one aggregate, inside one service, where a local transaction can enforce them.

**Bounded contexts.** A larger area of the business with its own model and language: Booking, Pricing, Payments, Guest Profile, Housekeeping. Inside a context, terms have one meaning. The same real-world thing can look different in two contexts: to Booking a "guest" has stay dates and a room; to Loyalty the same person has a tier and a points balance. Each context keeps its own model and shares only what others need through its interface.

**Mapping to services.** Start with one service per bounded context. Split a context into smaller services later, along aggregate lines, when there is a reason (team growth, different scaling needs). Starting coarse is cheaper because merging services is harder than splitting a well-modularized one. When a context is split, consider hiding that from the outside: consumers still see one interface.

**Collaborative discovery.** If the domain is unclear, a workshop with domain experts and engineers mapping business events on a timeline ("reservation held", "payment captured", "room assigned") is a fast way to find aggregates and contexts. Clusters of events around the same things suggest boundaries.

## Other reasons to draw a line

Business domain is the default, but other forces can justify a boundary. They usually add to the domain split rather than replace it.

- **Volatility.** Separate the parts that change weekly from the parts that change yearly, so fast-moving code isn't held back.
- **Data sensitivity.** Put regulated data (card numbers, health records) in a small, isolated service to shrink the audit and attack surface.
- **Technology.** A part that needs a different runtime or hardware (GPU inference, a search engine) may warrant its own service.
- **Organization.** Teams in different locations or reporting lines work best on separate services. Be careful: org charts change more often than domains.

Splitting purely by technical layer (a "frontend service", a "business logic service", a "database service") is almost always wrong. Every feature then crosses every layer, and every team has to coordinate.

## Worked example

A hotel booking design has: `GuestService` (CRUD for guests), `RoomService` (CRUD for rooms), `ReservationService` (CRUD for reservations), and a `BookingOrchestrator` that holds all the rules and calls the other three for every request.

Problems a reviewer should name:
- The three CRUD services hold data with no behavior. The business logic sits in the orchestrator, which becomes the place every change lands. That is a monolith with extra network hops.
- Availability rules need room and reservation data together, so every check makes several network calls and cannot use a local transaction to prevent double-booking.

Better: a `Booking` service that owns reservations and room availability together (the invariant lives in one place), a `Guest Profile` service that owns identity and preferences, and a `Pricing` service that owns rates. Booking calls Pricing for a quote and emits `ReservationConfirmed` for others to react to.

## Review checklist

- [ ] Each service has a one-sentence responsibility in business terms.
- [ ] Recent or planned features each touch one service, maybe two.
- [ ] No service is a CRUD wrapper whose logic lives somewhere else.
- [ ] Rules that must always hold are enforced within one service.
- [ ] Interfaces expose only what consumers need.
- [ ] Coupling has been classified, and the tighter kinds have a plan.
- [ ] The split is not by technical layer.
- [ ] Where the domain is unclear, the design starts coarse.

Related: [data-and-workflows.md](data-and-workflows.md) for data ownership, [anti-patterns.md](anti-patterns.md) for common smells, [migration.md](migration.md) for extracting boundaries from existing code.
