# Requirements, load, and operability

Use this to turn a vague design into one that can be judged. Most weak designs are weak because nobody wrote down the numbers they depend on.

## Three properties to hold the design against

- **Correct under faults.** The system keeps doing the right thing when hardware fails, software has bugs, and operators make mistakes. Faults are expected; the question is whether they turn into user-visible failures.
- **Holds up under growth.** There is a plan for what happens when traffic, data volume, or fan-out grows, and the plan is tied to a measured load parameter.
- **Easy to run and change.** Operators can see what it is doing and fix it; developers can understand it and change it without fear.

A review that checks only performance misses two thirds of the job.

## Faults: kinds and how they behave

- **Hardware faults** (disk, memory, power, NIC) are mostly random and independent. Redundancy handles them: RAID, replicas, multiple zones. At cloud scale whole instances vanish without warning, so the design must tolerate losing a node, not just a disk.
- **Software faults** are correlated. One bad input, a leap second, a leaked file handle, or a slow dependency can take down every node at once because they all run the same code. Redundancy does not help here. Look for: limits on resource use, timeouts on every remote call, isolation between tenants, kill switches, and gradual rollout.
- **Human faults** cause a large share of real outages, mostly through configuration and deploys. Look for: staged rollouts, fast rollback, sandboxes with real-shaped data, guard rails on dangerous commands, and monitoring that catches the mistake early.

Ask the author: which faults does this design tolerate, and which does it just hope will not happen?

## Describing load

Load must be stated as specific parameters, chosen for what actually stresses this system. Examples:

- writes per second and reads per second, peak and average
- read/write ratio
- number of concurrent connections or active users
- data size now and in 2 years, and record size distribution
- fan-out: how many records one request touches or one write must update
- cache hit rate

Fan-out is often the parameter that decides the architecture. Example: an activity inbox in a work app, where each project update notifies everyone watching the project. Building each inbox at read time means querying every watched project on every page load. Copying each update into every watcher's inbox at write time makes reads a single lookup, but a company-wide project with 40,000 watchers turns one update into 40,000 writes. A common middle ground is copying for normal projects and merging the few very large ones in at read time. A design should say which side it pays on and why.

## Describing performance

- Use **percentiles**, not averages. The median tells you the typical case; p99 and p999 tell you what your heaviest users (often your most valuable ones) see.
- **Tail latency multiplies.** If one page load calls 20 backends in parallel, the page is as slow as the slowest call. Even a 1% slow rate per backend makes a slow page common. Look for: hedged requests, timeouts with fallbacks, fewer sequential hops.
- Measure latency **from the client side**. A server that queues requests can report fast service time while users wait. Head-of-line blocking hides here.
- Load tests must keep sending at the target rate regardless of response time. A test client that waits for each reply before sending the next will make an overloaded system look healthy.
- Throughput targets matter for batch systems; response time matters for online systems. Make sure the design states the right one.

Percentiles cannot be averaged across machines or time windows. Aggregate the histograms instead.

## Scaling approaches

- **Scale up** (bigger machine) is simple and often enough for far longer than people expect. A single modern server has a lot of RAM, cores, and NVMe throughput.
- **Scale out** (more machines) is needed for very large data, very high write rates, fault tolerance across zones, or users spread across the world. It costs complexity, mostly in coordination.
- Stateless services scale out easily. Stateful ones (databases, stream processors) do not. Most of the design effort goes into the stateful parts.
- There is no generic scalable architecture. A system that handles 100k small requests per second looks very different from one that handles 3 requests per minute of 2 GB each. Check that the design is shaped by its own load parameters.
- Elastic autoscaling helps with unpredictable load but adds failure modes (scaling storms, cold caches, rebalancing during peaks). Manual scaling with alerts is fine for predictable load.

## Operability and evolvability

Check for:

- **Visibility**: metrics, logs, and traces for every component, plus health that reflects real user impact (error rate, lag, latency), not just "process is up".
- **Routine operations**: how to add capacity, replace a node, rotate credentials, restore from backup, run a schema migration. If these are not described, they will be improvised during an incident.
- **Predictable behavior**: few surprises, sensible defaults, the ability to override automation by hand.
- **Simplicity**: complexity that comes from the problem is fine. Complexity from the implementation (tangled dependencies, special cases, hacks around a bad abstraction) is debt. A good abstraction hides detail behind a clean interface without leaking it.
- **Change**: requirements will shift. Can the data model be extended? Can a new consumer be added without touching producers? Can data be reprocessed if logic changes? See `encoding-and-evolution.md` and `integration-and-correctness.md`.

## Questions to ask the author

- What is the source of truth for each kind of data?
- What are the peak numbers, and how were they estimated?
- What latency percentile is the target, and for which operations?
- How much data loss is acceptable on a crash: none, last second, last minute?
- How much downtime per month is acceptable? Is read-only mode acceptable during failures?
- Which anomalies can users tolerate (stale reads, duplicates, out-of-order events)?
- Who operates this, and what do they already know how to run?
- What is expected to change in the next year?

## Red flags

- No numbers anywhere, or only average latency.
- "Highly scalable" or "fault tolerant" claimed without saying which load parameter or which fault.
- A distributed design for data that fits on one node with room to spare.
- Load tests that use closed-loop clients.
- No plan for backups, restore drills, or schema migration.
- Health checks that only test whether the process responds.
