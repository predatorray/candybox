# Risk assessment

Covers: rating architecture risk consistently, building a risk summary per area of the system, running a risk session with several reviewers, and choosing mitigations that the business will pay for. Use it to prioritize findings or when the user asks "what are the biggest risks in this design?"

## Rate impact and likelihood separately

"High risk" means little unless people agree on how they got there. Rate each risk on two scales, 1 (low) to 3 (high):

- **Impact**: how bad is it if this happens? Consider users affected, money, data loss, legal exposure, recovery time.
- **Likelihood**: how likely is it to happen in a given period, given what is actually in place (redundancy, limits, track record)?

Multiply. 1 to 2 is low, 3 to 4 is medium, 6 to 9 is high. Judge impact first, then likelihood; starting with likelihood tends to make people dismiss severe but rare problems too early.

Example: the single primary database for a booking platform. Impact if it is down: 3, nothing works. Likelihood: it runs as a managed multi-zone cluster with automatic failover tested quarterly, so 1. Risk 3, medium. The same database without failover: likelihood 2, risk 6, high.

When nobody on the team has run a technology in production, or a dependency's behavior is unknown, rate likelihood high until someone shows evidence otherwise. Unknowns are risk.

## Summarize by area and by risk type

Build a grid: rows are areas of the system (services or workflows), columns are risk types (availability, scalability, elasticity, performance, security, data integrity, unproven technology, single points of failure). Fill each cell with the score. Totals per row show the riskiest areas; totals per column show the weakest qualities.

When presenting, filter. A stakeholder meeting about "what could hurt us this quarter" needs only the high cells, not the whole grid. When tracking over time, show direction explicitly: write the old and new score ("4 → 6") rather than arrows or plus signs, which readers interpret in opposite ways. Feed direction from measurements where possible (latency trends, error budgets) rather than opinion.

## Run the session in two passes

One reviewer misses things. A group that discusses before rating anchors on whoever speaks first. Do it in two passes:

1. **Alone.** Each participant gets the current diagram and one risk type to focus on (say, availability), and marks risks with scores independently. Include senior developers and people who operate the system, not only architects; they know which component falls over every Black Friday.
2. **Together.** Put everyone's marks on the same diagram. Where scores agree, move on. Where they differ, the person with the outlier explains. Sometimes they convince the room (they have seen that component fail under load). Sometimes the outlier reveals a knowledge gap (they had never heard of the caching product), which is itself a finding: the team needs training or the technology is less understood than assumed.

Keep each session to one risk type if you can. Mixing types makes the same box collect marks for unrelated reasons.

Repeat after major changes or on a regular cadence. Risk changes as the system does.

## Mitigate with costs on the table

For each high or medium risk, list mitigations with rough cost and how much each reduces the score. Typical moves:

- Remove a single point of failure: replicas, a second zone, a separate database for a critical subset of data.
- Isolate by user type: separate gateways or deployments so public traffic cannot reach admin or sensitive paths.
- Add back pressure: a queue in front of a slow dependency, with priority lanes for the users who matter most (staff over anonymous visitors, paying over free tier).
- Remove load from a limited dependency: cache what it returns, precompute common answers, rate-limit callers.
- Check the external contract: an external service's published SLA may already make the risk acceptable, or show it is worse than assumed. Note the SLA on the diagram.
- Replace or prototype an unproven technology before betting on it.

Mitigation costs money or time, so the business decides which risks to buy down. Offer options at different prices. If the full fix is refused, a cheaper partial fix often cuts most of the risk (splitting off the one critical table onto its own replicated database instead of clustering everything).

## Worked example

A city's recreation-program registration platform: resident web app, staff app, a registration service, a notification service, one shared PostgreSQL instance, and a third-party SMS provider with a published limit of 100 messages per second. Registration for summer programs opens at 9:00 on one morning each April, and traffic jumps about 20x for two hours.

| Area | Availability | Elasticity | Security |
|---|---|---|---|
| Shared database | 3 x 2 = 6 | 3 x 2 = 6 | 2 |
| Registration service | 2 | 3 x 2 = 6 | 2 |
| SMS provider | 2 x 2 = 4 | 3 x 3 = 9 | 1 |
| Single gateway for residents and staff | 2 | 2 | 3 x 2 = 6 |

Findings: the SMS limit guarantees a backlog during the April spike (9); the shared database is on the critical path for everything (6); residents and staff share one gateway, so a routing bug could expose staff endpoints, which can edit payments and personal records (6). Mitigations: a durable queue in front of SMS with confirmations delayed rather than dropped, plus email as a fallback channel; a read replica for seat-availability lookups and a scheduled scale-up before 9:00; separate gateways for resident and staff traffic.

## Red flags

- Risks labeled high or low with no stated impact and likelihood.
- No risk owner or mitigation plan.
- A risk review done by one person.
- External dependencies on the critical path with no known SLA or rate limit.
- New technology on the critical path with no prototype or production experience.
- The same risks reported every quarter with no direction shown.
