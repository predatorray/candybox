# Scaling and caching

Use this file to review capacity plans, partitioning, caching, and autoscaling.

## Four ways to scale

| Approach | What it means | Cost | Notes |
|---|---|---|---|
| Bigger machines | More CPU, memory, faster disks | Low effort | Quick win, especially in the cloud. Has a ceiling and doesn't add redundancy. Only helps if the software can use the extra resources. |
| More copies | Run several identical instances behind a load balancer, or have workers pull from a queue | Low to moderate | Needs stateless services or externalized state. The shared database often becomes the next bottleneck. Read replicas help for reads. |
| Partition by data | Split work by key (customer, region, tenant) so each partition handles a subset | Moderate to high | Scales writes. Choose the key carefully: uneven keys create hot partitions. Cross-partition queries get hard. Can also isolate failures to one partition. |
| Split by function | Move a capability into its own service with its own resources | High | Scales the part that needs it, lets it use different tech. This is the most expensive option and should be justified by more than scale alone. |

Guidance for reviews:
- **Try the cheap options first.** If a bigger box or more copies solve the problem, a redesign isn't needed yet.
- **Combine.** It is common to partition by tenant and then run several copies of each partition.
- **Measure first.** Ask for load numbers and where the bottleneck actually is. Designs often scale the wrong component.
- **Don't build for 100× on day one.** Design for the next order of magnitude. When load grows far beyond that, expect to rethink the architecture; that is normal, not a failure of the original design.

## Caching

### Why cache

- **Latency:** avoid a slow call or computation.
- **Load:** reduce work on the origin service or database.
- **Resilience:** keep serving (stale) data when the origin is down.

Say which purpose each cache serves. It decides how stale the data may be and what happens on a miss.

### Where to cache

- **In the client service:** fastest, removes the network call entirely. Each client has its own copy, so they may disagree. Invalidation is harder.
- **In the owning service:** one place to manage, consistent for all callers. Still costs a network call.
- **In a proxy or CDN in front of the service:** transparent to both sides, good for HTTP GETs and public content.
- **In the browser or app:** good for static or per-user content.

### Keeping caches fresh

- **Time-based expiry (TTL).** Simple, predictable staleness bound. Pick the TTL from how stale each piece of data may be.
- **Conditional requests.** The client asks "has this changed since version X?" and gets a small "not modified" response if not.
- **Notification-based.** The owner emits an event when data changes; caches drop or update the entry. Fresher, but needs reliable event delivery and a fallback TTL in case a notification is lost.
- **Write-through.** Writes update the cache and the store together. Keeps the cache current for that writer only.

### Rules of thumb

- **Keep cache layers to a minimum.** Each layer adds staleness that stacks: a 5-minute client cache in front of a 5-minute server cache can serve 10-minute-old data. Layers also make bugs hard to reason about.
- **Agree on freshness with the business.** "Real time" is rarely required everywhere. Ask what staleness is acceptable for each data set, and design to it.
- **Never cache forever by accident.** A long or missing expiry set on a response can get stuck in browsers and proxies you don't control, with no way to flush it. Set explicit, bounded cache headers.
- **Plan for a cold cache.** After a restart or flush, all traffic hits the origin at once. Make sure the origin can survive it, or warm caches gradually.
- **Don't hide a capacity problem.** If the origin can't survive without the cache, the cache is now a critical dependency and needs the same redundancy as the origin.

## Autoscaling

- **Reactive:** add instances when CPU, latency, or queue depth crosses a threshold. Needs instances that start quickly.
- **Scheduled or predictive:** scale ahead of known peaks (business hours, a sale).
- Load-test to find the right signals and thresholds. CPU is often the wrong signal for I/O-bound services; queue depth or request latency may be better.
- Set upper limits. Unbounded scaling can overwhelm a shared database or run up a large bill.
- Scaling down too fast causes thrashing; add cooldowns.

## Review checklist

- [ ] There are load numbers, current and projected, with peaks.
- [ ] The bottleneck is identified by measurement, not assumed.
- [ ] Cheaper scaling options were tried or ruled out before functional splits.
- [ ] Partition keys are chosen to avoid hot spots, and cross-partition needs are handled.
- [ ] Each cache has a stated purpose, location, and freshness bound.
- [ ] There are few cache layers, and their staleness adds up to an acceptable total.
- [ ] Cache headers have explicit bounded lifetimes.
- [ ] The origin can survive a cold cache.
- [ ] Autoscaling uses tested signals and has limits.

Related: [resilience.md](resilience.md), [fundamentals.md](fundamentals.md).
