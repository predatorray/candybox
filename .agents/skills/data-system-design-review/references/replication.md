# Replication

Use this when the design keeps copies of data on several machines: read replicas, standby databases, multi-region deployments, offline-capable clients, or leaderless stores.

Why replicate: survive node loss, serve reads from more machines, and put data close to users. All the difficulty comes from the data changing. Copying static data is easy.

## Three topologies

### Single leader

One node accepts writes and streams changes to followers. Reads can go to any node.

- Simple, widely supported, and the only topology that easily gives one agreed order of writes.
- The leader caps write throughput and is a single point for writes until failover.
- Cross-region writes pay a round trip to the leader's region.

### Multi-leader

Several nodes accept writes and replicate to each other asynchronously. Common for multi-region deployments, offline-first clients (each device is a leader), and collaborative editing.

- Writes stay local and survive a region being cut off.
- The same record can be changed in two places at once. **Conflicts are not an edge case; they are the main design problem.** If the design has no conflict plan, it is not done.
- Constraints such as uniqueness or "balance must not go negative" cannot be enforced across leaders without coordination.

### Leaderless

Clients (or a coordinator) send each write to several replicas and read from several. Success needs w acknowledgments on write and r responses on read, out of n replicas.

- Tolerates slow or dead nodes without failover.
- With w + r > n, the set of nodes read and the set written share at least one member, so a read should see the newest write. That overlap is weaker than it sounds: concurrent writes, a write that failed on some nodes but was not rolled back, a node restored from a stale replica, or sloppy quorums (writing to any reachable node during an outage) can all return old values. Treat quorums as making stale reads rare, not impossible.
- Repair happens on read (fix stale replicas when noticed) and through background anti-entropy. Rarely-read data may stay stale long without the background process.

## Synchronous vs asynchronous

- **Synchronous**: the leader waits for the follower before confirming. No data loss on leader failure, but one slow follower stalls all writes. Usually used for one follower only (semi-synchronous).
- **Asynchronous**: the leader confirms immediately. Fast and unaffected by slow followers, but a leader crash loses writes that clients were told succeeded.

Ask: on leader failure, how many confirmed writes can be lost, and is that written down?

## Failover hazards

Automatic failover sounds safe and often is not. Check for:

- **Lost writes.** A new leader promoted from an async follower lacks the old leader's last writes. If the old leader comes back, its unreplicated writes are usually thrown away. If other systems already acted on those writes (an external cache, an ID handed out), they now disagree with the database. A known failure mode: reused auto-increment IDs that collide with keys already stored elsewhere.
- **Split brain.** Two nodes both believe they are leader and both accept writes. Needs fencing (see `distributed-failures.md`).
- **Timeout tuning.** Too short and a load spike triggers needless failover, which adds load. Too long and recovery is slow.

## Replication lag anomalies

With async followers, reads from a follower can be behind. Usually by milliseconds; under load or network trouble, by minutes. Name the anomalies the product cannot tolerate:

- **Read-your-own-writes.** A user saves a change, reloads, and sees the old value. Fixes: read data the user can edit from the leader; read from the leader for a short window after a user's write; have the client remember the position of its last write and only read from replicas that have caught up to it. Across devices, that position must be stored server-side.
- **Monotonic reads.** A user sees a new comment, refreshes, and it is gone because the second read hit a staler replica. Fix: pin each user to one replica (for example by hashing user ID), with a fallback if it fails.
- **Consistent prefix.** A reader sees an answer before the question, because the two writes went to different partitions that lag differently. Fix: write causally related data to the same partition, or track causal dependencies.

A design that sends reads to replicas should list which of these it handles and how. "Eventually consistent" with no bound and no mitigation is a gap.

## Conflict handling (multi-leader and leaderless)

Options, roughly from simplest to most correct:

- **Avoid conflicts** by routing all writes for a given record to one home leader. Works until the home leader moves.
- **Last write wins** by timestamp. Simple, but it silently discards concurrent writes, and clock skew means "last" may be wrong. Acceptable only for data where losing an update is fine (caches, presence indicators) or keys that are written once.
- **Merge in the application.** Keep all concurrent versions (siblings) and resolve on read or write with domain logic. Example: labels on a support ticket edited from two regions; take the union, and record removed labels as tombstones so a merge does not bring them back.
- **Conflict-free data types** (counters, sets, maps, text sequences designed to merge automatically). Good for collaborative editing and counters, but each type has semantics that must match the product.
- **Operational transformation** for real-time text editing.

To tell "concurrent" from "one happened after the other", replicas need version vectors (a counter per replica). Wall-clock timestamps cannot do this.

## Adding followers and replication log formats

- New followers start from a consistent snapshot plus the log from that snapshot's position. Check that the snapshot and log position are tied together.
- Statement-based replication breaks on non-deterministic statements (now(), random(), autoincrement with concurrency). Write-ahead-log shipping ties replicas to the same storage engine version, complicating upgrades. Row-based (logical) logs decouple replicas from storage internals and also serve as a change stream for other systems (see `stream-processing.md`).

## Red flags

- Async replication with no stated data-loss window.
- Reads from replicas with no plan for read-your-writes on user-edited data.
- Multi-leader or multi-region active-active writes with no conflict strategy, or with last-write-wins on data that matters.
- Quorum settings presented as a guarantee of linearizable reads.
- Automatic failover without fencing.
- Auto-increment IDs used across systems where failover can roll them back.
