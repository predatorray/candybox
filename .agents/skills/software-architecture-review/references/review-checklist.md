# Review checklist

A quick pass over a whole design. Each item points to the file with more detail. Mark each one: fine, finding, or unknown (ask). Unknowns are findings too when they block judgment.

## Purpose and qualities (`quality-attributes.md`)

- [ ] Can you state in a few sentences what the system does, for whom, and its main workflows?
- [ ] Are the top quality attributes named, and are there three or so rather than ten?
- [ ] Does each have a number: load, p99 latency, downtime budget, recovery time, deploy frequency?
- [ ] Are implied qualities covered (deadline spikes, bursty traffic, audit needs, regulatory isolation)?
- [ ] Do different parts of the system need different levels, and does the design reflect that?
- [ ] Are external limits known (third-party rate limits, SLAs, quotas)?

## Structure (`components-and-boundaries.md`, `modularity-and-coupling.md`)

- [ ] Is the top-level split by business area or by technical layer, and does that fit how the system changes?
- [ ] Does a typical feature change land in one component or several?
- [ ] Are components built around workflows rather than one per table?
- [ ] Are there `common`/`utils` modules with domain logic that everything depends on?
- [ ] Are there dependency cycles between modules?
- [ ] Is shared code limited to technical concerns, with domain models owned per context?
- [ ] Do team boundaries match component boundaries?

## Style (`choosing-a-style.md`, `monolithic-styles.md`, `distributed-styles.md`)

- [ ] Which style is it really, regardless of what the doc calls it?
- [ ] Do the style's natural strengths match the top qualities?
- [ ] Does the problem's shape fit the style (variants → plug-ins, transformations → pipeline, many reactors → events)?
- [ ] Is distribution justified by different needs across parts, or only by fashion?
- [ ] Does the team have the engineering maturity the style assumes?

## Runtime paths (`distributed-tradeoffs.md`, `event-driven.md`)

- [ ] For the most important request, how many synchronous hops, and what is the p99 budget?
- [ ] Does every remote call have a timeout, retry policy and idempotency where retried?
- [ ] Which deployable units share a database or call each other synchronously, and so behave as one?
- [ ] For each multi-service write, what happens on partial failure?
- [ ] For each queue or topic: durable? acked after processing? consumers idempotent? dead-letter path?
- [ ] Is ordering required, and does the broker guarantee it at that scope?
- [ ] Are payloads sized for what callers need?
- [ ] Are internal endpoints authenticated?
- [ ] Are correlation IDs, tracing and central logs in place?
- [ ] What shared infrastructure (one broker, one DB cluster, one region) could take everything down?

## Decisions and governance (`decisions-and-governance.md`)

- [ ] Are the main choices recorded with context, reasons and consequences?
- [ ] Do the records include business reasons?
- [ ] Are structural rules enforced automatically (cycles, layer access, module APIs)?
- [ ] Are performance budgets or SLOs checked continuously?
- [ ] Does any proposed change reverse an earlier decision, and does it address the original reason?

## Risk (`risk-assessment.md`)

- [ ] Are the top risks rated by impact and likelihood?
- [ ] Are unknown or unproven technologies treated as high risk until shown otherwise?
- [ ] Does each high risk have a mitigation option with a cost?

## Communication (`communicating.md`)

- [ ] Does the diagram label every box and arrow, show sync versus async, and show data store ownership?
- [ ] Is there an overview before the detail views?
- [ ] Do the diagram and the text agree?
