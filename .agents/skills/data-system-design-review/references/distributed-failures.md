# What goes wrong between machines

Use this when the design depends on timeouts, retries, heartbeats, leases, locks, leader election, timestamps, or any assumption about how fast or reliable other nodes are.

The defining trait of distributed systems is **partial failure**: some parts work while others do not, and you often cannot tell which. A single machine tends to either work or crash. A cluster does both at once, in different places, and the behavior is non-deterministic.

If the problem fits on one machine, keeping it there avoids this whole category. When it does not, assume every item below will happen.

## The network

Most data center and cloud networks are asynchronous packet networks: no upper bound on delay, no delivery guarantee. When a request gets no reply, any of these may have happened:

- the request was lost
- the request is queued somewhere and will arrive later
- the remote node crashed, or is paused
- the remote node did the work and the reply was lost or delayed

The sender cannot tell these apart. Consequences for design:

- Every remote call needs a timeout. There is no correct value; pick one from measured round-trip distribution plus headroom, and prefer adaptive timeouts in systems with variable latency.
- After a timeout, the operation's outcome is unknown. Retrying is only safe if it is idempotent (see `integration-and-correctness.md`).
- Delays come mostly from queueing: switch buffers, OS socket buffers, CPU scheduling on busy hosts, noisy neighbors in shared clouds, and TCP retransmission and flow control. They get worse exactly when the system is under load.
- Network partitions happen in practice, including one-way partitions where A can reach B but not the reverse. Test for them deliberately.

## Detecting failure

Timeouts cannot distinguish a dead node from a slow one or a slow network. Declaring a node dead too early causes double work (its tasks run twice) and failover cascades. Too late, and users wait.

Worse than dead nodes are **degraded nodes**: a disk at 1% of normal speed, a NIC dropping most packets, a process stuck in GC. They pass health checks and drag down every request routed to them. Look for latency-based outlier detection, not only liveness checks.

## Clocks

Each machine has two kinds of clock:

- **Wall clock (time of day)**: synced via NTP, can jump forward or back, may be off by tens to hundreds of milliseconds or much more if NTP is misconfigured or blocked. Leap seconds and VM migrations cause jumps.
- **Monotonic clock**: only moves forward, good for measuring durations on one machine, meaningless across machines.

Design rules:

- Use monotonic clocks for timeouts and durations.
- Do not order events across machines by wall-clock timestamps. Last-write-wins by timestamp can drop a write that truly happened later, because the node that wrote it had a slower clock.
- If a design needs timestamps across nodes, it needs a known error bound and must account for it (some systems wait out the uncertainty window before committing). Ask what the bound is and how it is measured.
- Monitor clock offset and remove nodes whose clock drifts too far. A drifting clock fails silently.
- Logical clocks (counters that increase with each event and are carried on messages) give an order consistent with causality without trusting wall time. See `consistency-and-consensus.md`.

## Process pauses

A thread can stop at any point for seconds or longer: GC pauses, VM suspension or live migration, the OS swapping, a laptop lid closing, a slow disk on a synchronous log write, a SIGSTOP. Then it resumes with no idea that time passed.

Classic bug: a node acquires a lease, checks that it is still valid, pauses for 15 seconds, then writes as if it still holds the lease. Another node got the lease in the meantime. Both write.

## Fencing

The fix for pauses and split brain is to have the protected resource reject stale holders, not to trust the holder to know it is stale.

- The lock or lease service issues a **fencing token**: a number that increases every time the lock is granted.
- The holder sends the token with every write.
- The storage system rejects writes carrying a token lower than one it has already seen.

If the storage cannot check tokens, the lock is advisory and cannot guarantee mutual exclusion under pauses. A review should ask: "If the lock holder pauses for 30 seconds after acquiring, what stops it from corrupting data?"

## Majorities decide

A node cannot trust its own view of whether it is alive or still the leader. Decisions such as "who is leader" or "is this node dead" should be made by a quorum (more than half) of nodes. A node that finds itself in the minority must step down, even if it feels fine.

## Byzantine faults

Nodes that lie (send wrong data on purpose or through corruption) need special protocols that most internal systems do not use. Within a trusted data center, defend against the cheap cases instead: checksums on network and disk data, input validation at service boundaries, and sanity checks on configuration. For systems with untrusted participants (public peer-to-peer, client devices), never trust client-reported state.

## System models for reasoning

When judging an algorithm's claims, ask which assumptions it makes:

- timing: synchronous (bounded delays), partially synchronous (usually bounded, sometimes not), or asynchronous (no bounds)
- node failure: crash-stop, crash-recovery (nodes come back, possibly with only durable state), or Byzantine

Realistic systems are partially synchronous with crash-recovery. An algorithm proven only for crash-stop may break when nodes come back with stale memory. Also check that "durable" state really survives (fsync, disk firmware, replication).

A correct algorithm has **safety** properties (nothing bad happens, such as two leaders at once) that must hold always, and **liveness** properties (something good eventually happens, such as a leader gets elected) that may wait for the network to recover.

## Red flags

- Remote calls without timeouts, or a single fixed timeout chosen without data.
- Retries without backoff and jitter (retry storms), or without idempotency.
- Distributed locks used for correctness with no fencing token checked by the resource.
- Wall-clock timestamps used to order writes from different nodes.
- Durations measured with the wall clock.
- A node deciding on its own that it is the leader.
- Health checks that only test liveness, letting degraded nodes keep taking traffic.
- No fault-injection or partition testing.
