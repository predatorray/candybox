---
title: Operations
weight: 60
---

# Operations

This page is a quick orientation for running Candybox. The authoritative operations guide is
[`OPERATIONS.md`](https://github.com/predatorray/candybox/blob/main/OPERATIONS.md) in the repository,
which covers topology, consistency & backpressure, failure modes & recovery, garbage collection,
observability, and security in depth.

## Topology

A Candybox deployment is a set of identical storage nodes plus their dependencies:

- **ZooKeeper** — coordination and metadata (shared with BookKeeper).
- **BookKeeper bookies** — the durable, replicated ledger store.
- **Candybox nodes** — stateless-on-restart storage processes; ownership of Box partitions is leased
  and balanced across them.
- Optionally the **S3 gateway** and the **admin / dashboard API**, both stateless and horizontally
  scalable behind a load balancer.

## Observability

Each node exposes an HTTP endpoint (default port `9710`) with `/healthz`, `/readyz`, and `/metrics`.
Wire `/healthz` and `/readyz` to your liveness/readiness probes and scrape `/metrics`.

### Admin / dashboard API

The `candybox-admin-api` service exposes cluster topology, boxes, LSM internals, and metrics as JSON
under `/api/*`, and serves the [web dashboard]({{< relref "/docs/getting-started/quickstart" >}}) at
`/ui/`. It is stateless — run as many replicas as you like.

## Security

Candybox supports SASL + TLS between clients / gateway / admin API and the storage nodes, ACL-based
authorization, and authenticated connections to ZooKeeper and BookKeeper. See the
[Security section of `OPERATIONS.md`](https://github.com/predatorray/candybox/blob/main/OPERATIONS.md#security)
for the full configuration.

## Running on Kubernetes

A multi-stage `Dockerfile` at the repo root builds a node image straight from source
(`docker build -t candybox:latest .`). The image is **dual-mode**: it defaults to the storage node,
but `docker run … <image> candybox <args…>` runs the bundled client CLI instead.

A `StatefulSet` + headless `Service` manifest lives under
[`examples/kubernetes/`](https://github.com/predatorray/candybox/tree/main/examples/kubernetes) (also
bundled into the distribution tarball under `examples/`). The StatefulSet gives each pod a stable
identity, so `node.id` and the advertised address derive automatically from the pod name, and
liveness/readiness probes hit the health endpoint.

For a self-contained local cluster (ZooKeeper + bookies + nodes), use the
[`docker-compose.yml`](https://github.com/predatorray/candybox/blob/main/docker-compose.yml) described
in the [quick start]({{< relref "/docs/getting-started/quickstart" >}}).
