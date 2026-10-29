# Review process

How to run a design review of a service-based system, from intake to the written report. Also covers the case where the user wants help designing a system rather than reviewing one.

## 1. Intake

Before judging anything, get answers to these. Many come from the doc itself; ask for the rest. If the user wants a quick read, state your assumptions instead of asking.

Goal and context
- What outcome is this design supposed to produce? (ship features faster with more teams, scale one hot path, isolate a regulated part of the system, replace aging tech, improve uptime)
- Why is the current system not enough? What has been tried?
- How will anyone know it worked? Look for a measurable signal.

People
- How many teams will build and run this, and how big are they?
- Who owns each service after launch, including on-call?
- Is there a platform or infrastructure team, and what does it already provide?

Load and limits
- Traffic now and expected in 12 to 24 months, with peaks.
- Latency and availability targets per user-facing capability.
- Data volume, retention, and any residency or compliance rules (payments, health, personal data).
- Hard deadlines and what happens if they slip.

Current state
- Is this greenfield or a change to an existing system?
- What does the deployment, monitoring, and CI setup look like today?

## 2. Build the model

Write down, or sketch, the system as you understand it:

- Services, each with a one-line responsibility and an owning team.
- Data stores, and which service writes to each. Flag any store with more than one writer or with readers outside its owning service.
- Synchronous calls as arrows, events as dashed arrows with topic names.
- External dependencies (third-party APIs, payment providers, identity providers).

Then trace two or three of the most important user journeys through the model. For each step note: sync or async, what happens if this step is slow, what happens if it fails, and who notices.

If you cannot draw the model from the doc, that is itself a finding: the design is not yet clear enough to build.

## 3. Walk the lenses

Go through each lens that applies. Load the matching reference file for depth.

| Lens | Main question | File |
|---|---|---|
| Fit | Is splitting into services justified by the goal? | fundamentals.md |
| Boundaries | Does each service own a cohesive business capability and hide its internals? | boundaries.md |
| Data | Does each piece of data have one owner? How do others get it? | data-and-workflows.md |
| Workflows | How do multi-service processes finish, fail, and recover? | data-and-workflows.md |
| Communication | Are the interaction styles suited to each use? Can interfaces evolve without lockstep releases? | communication.md |
| Failure | What happens when each dependency is slow, down, or wrong? | resilience.md |
| Scale | Where are the hot paths, and is the scaling plan proportionate? | scaling-and-caching.md |
| Delivery | Can each service be built, tested, deployed, and released on its own? | delivery-and-testing.md |
| Operations | Can someone on call tell what is broken and why? | observability.md |
| Security | Who can call what, with whose authority, and what happens if one service is compromised? | security.md |
| UI | Does the frontend undo the decoupling done on the backend? | frontend.md |
| People | Do team boundaries match service boundaries? Who decides cross-cutting standards? | organization.md |
| Migration | If replacing a system, is the path incremental and reversible? | migration.md |

You don't need every lens for every review. A design for one new service rarely needs the UI or organization lens.

## 4. Rate findings

Use four levels:

- **Blocker**: the design will not meet its stated goal, or carries a serious risk of data loss, security exposure, or outage. Must be resolved before building.
- **Major**: likely to cause real pain within months (lockstep releases, cascading failures, no clear owner). Should be resolved before building or have a dated plan.
- **Minor**: a better option exists, but the current choice is workable.
- **Question**: you need information to judge. Not a defect yet.

Rate by impact on the stated goal. A shared database is a blocker if the goal is team autonomy, and may only be minor for a two-person team building an internal tool.

## 5. Write each finding

Each finding has:

1. A short title naming the problem.
2. Where it is: the service, call, table, or section of the doc.
3. What goes wrong and under what conditions. Use a concrete scenario ("when Payments is slow, Checkout threads block, and Search, which shares the same pool, stops responding").
4. A recommended change, and its cost.
5. Alternatives, if there is a real choice to make.

Avoid findings that only say "consider X". Say what to do, or say what information would decide it.

## 6. Report template

```markdown
## Verdict
<2-3 sentences: does the design meet its goal; the one or two biggest risks.>

## Understanding
<One short paragraph restating goal, constraints, and the system as you modeled it.
 Lets the authors correct misreadings quickly.>

## Findings

### [Blocker] <title>
**Where:** <component / section>
**Problem:** <what goes wrong, concrete scenario>
**Recommendation:** <change, and its cost>

### [Major] <title>
...

## What works well
<Short list of decisions worth keeping, so they survive the next revision.>

## Open questions
<Numbered questions for the authors.>
```

Keep "What works well" honest and short. Its purpose is to stop good decisions from being reversed, not to soften criticism.

## Tone

- Write to the authors as peers. Explain reasoning; don't just cite a rule.
- Say "this will" when you are confident and "this might" when you are not. Don't hedge everything.
- Don't demand the ideal end state. Offer a path: what to do now, what can wait, and what signal means it's time.
- Don't pad. If a section of the design is fine, skip it.

## Designing instead of reviewing

When the user asks for a design rather than a review:

1. Do the intake above. Push for the goal and team structure; they shape everything else.
2. Start from business capabilities and the main user journeys, not from technology. Find the domain areas and their language first ([boundaries.md](boundaries.md)).
3. Propose the smallest number of services that meets the goal. Explain why each boundary exists. Prefer starting coarse and splitting later over starting fine and merging.
4. For each service, state: responsibility, owned data, public interface (calls and events), owning team.
5. Choose interaction styles per journey ([communication.md](communication.md)), and design workflows that span services with explicit failure handling ([data-and-workflows.md](data-and-workflows.md)).
6. Cover the operational baseline: timeouts and degraded modes, logging with correlation IDs, per-service pipelines, auth between services.
7. List the decisions that are cheap to change later and the ones that are not. Spend review effort on the second group.
8. Run your own design through the review lenses before presenting it.
