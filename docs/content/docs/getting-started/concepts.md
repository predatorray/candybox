---
title: Concepts
weight: 2
---

# Concepts

Candybox borrows the bucket/object model from S3 but gives the pieces its own names.

## Vocabulary

| Term | Meaning |
|---|---|
| **Box** | a bucket — a named, flat namespace of keys |
| **Candy** | an object — the bytes plus metadata stored under a key |
| **CandyKey** | an object key |
| **Syrup** | a data ledger that holds object bytes |
| **CandyLocator** | the small pointer the index stores to find a Candy's bytes inside a Syrup |

*(Candy in a box — that's the whole theme.)*

## The data model

A **Box** is a flat, sorted key space. Keys are stored in sorted order, which is what makes range
scans, prefix listing, and directory-walk workloads cheap sequential reads rather than scatter-gather.

Each Box is split into a fixed number of **hash partitions**. At any moment exactly one node *owns* a
partition and serves its reads and writes; ownership is spread evenly across the cluster by an elected
balancer, so a single Box's traffic is served by many nodes.

## Durability and consistency in one paragraph

Object data and index both live in **BookKeeper ledgers** — append-only, replicated, and
self-fencing. Candybox never mutates data in place: updates and deletes are new appends (with
tombstones). Consistency rests on **single, fenced ownership per partition**: the owner holds a
ZooKeeper lease with a *fencing token*, and every state-changing operation carries that token, so a
stale former owner cannot corrupt a partition after ownership moves during a failure. Each write is
stamped with a hybrid logical clock for last-writer-wins ordering across nodes.

For the full picture — record formats, the read/merge path, and the handover protocol — see
[Concepts & architecture]({{< relref "/docs/architecture" >}}).
