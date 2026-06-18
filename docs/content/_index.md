---
title: Candybox
type: docs
bookToc: false
---

# Candybox

> [!TIP]
> **A self-hosted, distributed object store with a sorted LSM-tree index on Apache BookKeeper.**

Candybox is a **distributed, S3-like object store** written in Java. You create *buckets* and store
*objects* in them through a small TCP API, a command-line client, or an S3-compatible HTTP gateway;
Candybox keeps those objects durable and replicated across a cluster.

Under the hood it is a **distributed LSM tree built on [Apache BookKeeper](https://bookkeeper.apache.org/)**:
object data and index live in BookKeeper's replicated, append-only ledgers, and a single fenced owner
per bucket partition keeps reads and writes consistent during failover, with partitions spread evenly
across the cluster.

Because keys are stored sorted and object bytes live behind small pointers, Candybox offers a few
operations an S3-style store cannot do cheaply: bounded / reverse-order range scans, zero-copy
`copy` / `rename`, and `delete-range` over a whole prefix or key window in constant work.

## Where to start

- 🚀 **[Get started]({{< relref "/docs/getting-started" >}})** — spin up a full cluster with Docker
  Compose, store your first object, and learn the vocabulary.
- 🧩 **[Concepts & architecture]({{< relref "/docs/architecture" >}})** — how the LSM engine,
  BookKeeper ledgers, and fenced ownership fit together.
- ☁️ **[S3 gateway]({{< relref "/docs/s3-gateway" >}})** — use Candybox from any S3 SDK or tool, with
  optional SigV4 auth and ACLs.

## Vocabulary

| Term | Meaning |
|---|---|
| **Box** | a bucket |
| **Candy** | an object |
| **CandyKey** | an object key |
| **Syrup** | a data ledger that holds object bytes |

*(Candy in a box — that's the whole theme.)*

---

Candybox is open source under the [Apache License 2.0](https://github.com/predatorray/candybox/blob/main/LICENSE).
The source lives on [GitHub](https://github.com/predatorray/candybox).
