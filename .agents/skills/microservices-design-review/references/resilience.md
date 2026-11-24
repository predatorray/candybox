# Resilience

Use this file to review how a design behaves when parts of it are slow, down, or wrong. In a distributed system something is always failing somewhere; the design has to assume that.

## Four aspects to cover

Resilience is broader than "add retries". Check that the design addresses each of these:

1. **Absorbing expected failures.** Known problems (a dependency times out, an instance dies, a disk fills) are handled automatically without user impact. This is where most technical patterns below apply.
2. **Recovering after a bad event.** Backups exist and are tested, runbooks exist, and the team knows how to restore service and data.
3. **Coping with the unexpected.** When something nobody planned for happens, people with enough context and authority can act. Too much automation, or a team stripped of slack, makes this worse.
4. **Learning and adapting.** Incidents lead to changes. Postmortems are blameless, and findings get acted on.

Services can help with the first two by isolating failure. The last two depend on people and culture, and splitting into services does nothing for them by itself.

## Decide how much is enough

Uptime targets have costs. Ask for targets per capability, not one number for the whole system:
- Search being slow for a minute is an annoyance. Payments being down is lost revenue.
- What latency is acceptable at the median and at the 99th percentile?
- How long can each capability be down before real damage? How much data loss is acceptable?

Without targets, reviewers can't tell whether a design is over- or under-built.

## Degrade, don't fail

For each user-facing page or operation, list its dependencies and decide what happens when each one is unavailable:
- Product page with the recommendations service down: show the page without recommendations.
- Checkout with the loyalty service down: let the purchase go through, award points later from a queue.
- Checkout with payments down: stop, with a clear message. Some failures should block.

If the design doesn't say, every dependency is in practice required, and the system is only as available as its least available part.

## Stability patterns

**Timeouts on every outbound call.** No call should wait forever. Base the value on the dependency's normal latency (for example, a little above its p99), not a round default. Also set an overall budget for the whole request, and pass the remaining time downstream so deep calls don't keep working after the user has given up.

**Retries, carefully.**
- Retry only operations that are safe to repeat (idempotent), and only errors that might succeed on retry (timeouts, 503s, not validation errors).
- Use exponential backoff with jitter.
- Cap the total: a retry budget per caller, or a limit on retries as a share of requests.
- Retry at one layer only. If the client, the mesh, and the server library each make up to three attempts, one failing call can turn into 27 calls at the bottom of the stack.

**Bulkheads.** Separate resource pools (threads, connections, queues) per downstream dependency, so one slow dependency can't use up resources needed for others. Also load-shed: reject excess work early with a clear error instead of queuing until everything is slow.

**Circuit breakers.** After repeated failures to a dependency, stop calling it for a while and fail fast or use a fallback. Probe periodically and close the circuit when it recovers. This protects both the caller (no wasted threads) and the struggling dependency (load relief). Decide what the caller does while the circuit is open.

**Isolation.** Services that depend on each other less fail together less. Async communication through a queue lets a consumer be down without the producer noticing. Separate hosts or containers stop one service's resource use from starving another.

**Redundancy.** Several instances of each service, spread across failure zones. No single instance, zone, or shared component (one database primary, one broker node) whose loss takes down unrelated capabilities.

**Middleware guarantees.** Brokers can provide durable delivery and buffering, but they become critical infrastructure with their own failure modes. Prefer managed offerings where possible, and know what happens to producers when the broker is unreachable.

**Idempotency.** Design write operations so repeating them has the same effect as doing them once: client-supplied request IDs, natural keys, or recording processed message IDs. Without this, retries and at-least-once delivery cause duplicate charges, emails, or records.

## Spread risk

- Check what the "separate" services share: same host, same rack, same zone, same database cluster, same DNS provider. Shared infrastructure is shared failure.
- Read the provider's availability guarantees for each managed service, and design for its documented failure modes.

## Consistency vs availability

When the network splits two nodes that hold copies of data, a system can either keep serving (and risk inconsistent answers) or refuse some requests (and stay consistent). It can't do both. The choice is per data set, not per system:
- Product catalog, profile pictures, recommendations: serve stale data and stay up.
- Account balances, inventory counts for limited stock, uniqueness of usernames: may need to refuse or queue writes during a split.

The review question: for each important data set, has the design chosen, and does the user experience match the choice? Even "consistent" choices often relax under real conditions; make sure the guarantees claimed match what the chosen database actually provides.

## Practice failure

- **Game days.** Planned exercises where the team simulates a failure (kill a dependency, lose a zone) and works through it. Tests both the system and the runbooks.
- **Production experiments.** Controlled fault injection in production, starting small, with a clear stop button. Only after the basics (timeouts, monitoring, rollback) are in place.

## After incidents

- Blameless reviews that ask how the system let the problem happen, not who caused it.
- Track the follow-up actions to completion.
- Watch for teams that hide incidents; that usually means blame is still in play.

## Review checklist

- [ ] Availability and latency targets exist per capability.
- [ ] Each user operation lists required vs optional dependencies, with degraded behavior defined.
- [ ] Every outbound call has a timeout; requests have an overall deadline.
- [ ] Retries are limited, backed off, jittered, only on safe operations, and at one layer.
- [ ] Slow dependencies are isolated with bulkheads or circuit breakers.
- [ ] No single shared component can take down unrelated capabilities.
- [ ] Writes are idempotent where retries or redelivery are possible.
- [ ] The consistency/availability choice is made per data set.
- [ ] Backups are tested; restore time is known.
- [ ] There is a plan to practice failure.

Related: [communication.md](communication.md) for sync chains, [scaling-and-caching.md](scaling-and-caching.md) for caches as a resilience tool, [observability.md](observability.md) for detecting failure.
