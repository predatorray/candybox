---
title: Configuration
weight: 1
---

# Configuration

A node reads `conf/candybox.properties`. **Every key can be overridden by an environment variable**
named `CANDYBOX_<KEY>` (dots become underscores, upper-cased) — for example
`CANDYBOX_ZOOKEEPER_CONNECT` — and the environment value wins. This makes it easy to ship one image
and configure each instance through the environment.

## Common keys

| Key | Meaning | Default |
|---|---|---|
| `node.id` | Cluster-unique node id. Falls back to the trailing number in `$HOSTNAME` (so a Kubernetes pod `candybox-2` becomes node `2`). | — |
| `zookeeper.connect` | ZooKeeper connect string, shared by BookKeeper and Candybox coordination. | `127.0.0.1:2181` |
| `server.bind` | Address clients connect to. | `0.0.0.0:9709` |
| `server.advertised` | Address published to the cluster for routing (set to a reachable hostname). | bind address |
| `health.port` | HTTP port for `/healthz`, `/readyz`, `/metrics`. | `9710` |
| `server.io.threads` | Netty event loops framing client traffic. `0` uses Netty's default (twice the available processors). | `0` |
| `server.handler.threads` | Threads running the request handler, off the event loops. Each connection is pinned to one of them. | `max(32, cores × 4)` |
| `server.max.inflight.per.connection` | Requests one connection may have queued for the handler before the node stops reading from it. | `8` |
| `server.drain.timeout.millis` | How long a shutdown lets in-flight requests finish before their connections are cut. | `10000` |
| `quorum.*` | BookKeeper replication per ledger role (`E/Qw/Qa`). | `3/3/2` (WAL, manifest), `3/2/2` (data) |
| `multipart.upload.ttl.millis` | How long an abandoned multipart upload is kept before a background sweep aborts it. | 7 days |
| `rename.intent.abandon.millis` | Cross-partition rename: how long the source owner keeps a rename intent whose destination rendezvous marker never appears before dropping it (the source stays live). | 60000 (60 s) |

## Full reference

The complete, commented list of keys lives in
[`candybox.properties.example`](https://github.com/predatorray/candybox/blob/main/candybox-dist/src/conf/candybox.properties.example),
and the `CandyboxConfig` section of
[`OPERATIONS.md`](https://github.com/predatorray/candybox/blob/main/OPERATIONS.md#configuration-reference-candyboxconfig)
documents the operational meaning of each tunable, including the security, consistency, and garbage
collection settings.
