# Consistency models and consensus

Use this when the design claims a consistency level, needs a single agreed value across nodes (leader, lock, unique name, configuration), uses distributed transactions, or runs a coordination service.

## Consistency is a spectrum

From weakest to strongest, the useful reference points:

- **Eventual consistency**: replicas converge if writes stop. Says nothing about what you read before then.
- **Session guarantees**: read-your-writes, monotonic reads, consistent prefix (see `replication.md`). Often all the product needs.
- **Causal consistency**: if one operation could have influenced another, everyone sees them in that order. Concurrent operations can appear in different orders on different replicas. Can be provided without blocking during network partitions, and is the strongest model with that property.
- **Linearizability**: reads and writes act like there is only one copy of the data, and each operation takes effect at one instant between when it starts and when it returns. Once any client has seen a new value, no client sees the old one.

Do not confuse linearizability with serializability. Serializable is about transactions on many objects appearing to run one at a time in some order. Linearizable is about the latest value of one object. A system can have either, both, or neither.

## When you need linearizability

- Leader election and locks: everyone must agree who holds it.
- Uniqueness constraints: two users must not both get the same username or seat.
- Invariants that must hold at all times: an account balance that must never go negative.
- Cross-channel timing: a service saves a new config version to one store and publishes a "config changed" event. A consumer gets the event, reads from a replica that has not caught up, and caches the old version until the next change. Look for these "two paths race each other" bugs whenever a message refers to data in another store.

## What it costs

- **Availability during partitions.** If replicas cannot talk, a linearizable system must stop serving some requests on the side without a majority. A system that keeps serving on both sides is not linearizable. This is the real content of the CAP theorem; the theorem itself is narrow (it only covers one consistency model and one kind of fault) and is not useful for choosing databases. Ask instead: during a partition, which operations must keep working, and which may fail?
- **Latency.** Linearizable operations need coordination, so their latency is tied to network delay. Across regions this is large. Many systems give up linearizability for speed even without partitions.

What gives linearizability:

- single-leader replication with reads from the leader (and a leader that is truly the only one, which needs fencing)
- consensus-based systems
- not leaderless quorums, despite claims, and not multi-leader replication

## Ordering and causality

Causality is the "happened before" relation: a reply depends on the message, an update depends on the read that preceded it. Many bugs are causality violations: a reader sees an effect before its cause.

Tools:

- **Logical timestamps**: a (counter, node ID) pair where each node takes the max of its counter and any counter it receives, plus one. Gives a total order consistent with causality without trusting clocks.
- **Version vectors**: tell whether two versions are concurrent or one descends from the other.

A total order from timestamps is not enough to enforce uniqueness. You only know the order after you have collected every operation, but a node needs to decide now whether to accept a username. For that you need agreement in real time, which is consensus.

## Total order broadcast

Deliver every message to every node, reliably, in the same order. If you have it, you can build a replicated state machine: every replica applies the same log of commands in the same order and ends in the same state. Database replication logs, event logs, and consensus logs are all versions of this idea. It is equivalent to consensus.

## Consensus

Making a group of nodes settle on one value that never changes afterward, tolerating some nodes failing. Many problems reduce to it:

- electing a single leader
- granting a lock or lease
- deciding whether a distributed transaction commits
- enforcing a uniqueness constraint
- agreeing on cluster membership and which nodes are alive
- linearizable compare-and-set

Fault-tolerant consensus algorithms (the Raft and Paxos families) need a majority of nodes alive and connected. With 3 nodes you tolerate 1 failure; with 5, 2. They stop making progress, rather than giving wrong answers, when a majority is unreachable. They are sensitive to network delay: unstable networks can cause frequent leader elections and stalls.

Most teams should not implement consensus. Use a coordination service (ZooKeeper, etcd, and similar) or a database built on one. These services:

- are meant for small, slowly changing data: leader identity, partition maps, configuration, locks
- offer fencing tokens (the transaction ID or version number), sessions with heartbeats, and change notifications
- should not hold application data or high-volume writes

## Distributed transactions and two-phase commit

Two-phase commit (2PC) makes a transaction across several databases or partitions atomic: a coordinator asks all participants to prepare, then tells them all to commit or abort.

The problem: once a participant votes yes, it must wait for the coordinator's decision, holding locks. If the coordinator crashes, participants are stuck ("in doubt") until it recovers, and their locks block other work. Some systems need manual intervention to clear them.

Additional practical costs: lower throughput from extra round trips and fsyncs, the coordinator becomes a critical stateful component, and heterogeneous 2PC (across different products via XA) works only as well as the weakest participant and blocks the whole set if one fails.

2PC within one database system (across its own partitions) is more manageable than across products.

Alternatives to consider before 2PC:

- redesign so the transaction touches one partition
- a log-based approach: write once to a durable log, derive everything else from it, with idempotent consumers (see `integration-and-correctness.md`)
- saga-style steps with compensating actions, accepting that intermediate states are visible

## Red flags

- "Strongly consistent" without saying linearizable, serializable, or both.
- Leaderless quorum reads presented as linearizable.
- A message that references data in another store, with no guarantee the data is visible when the message arrives.
- Uniqueness or locking built on timestamps or best-effort checks.
- Home-grown leader election or consensus.
- Application data or high write volume in a coordination service.
- Cross-system 2PC on the critical path with no plan for coordinator failure.
- CAP cited as the justification for a design choice without describing the actual partition behavior.
