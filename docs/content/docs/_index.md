---
title: Documentation
type: docs
weight: 1
bookFlatSection: false
---

# Candybox documentation

Welcome to the Candybox manual. Candybox is a distributed, S3-like object store backed by a
distributed LSM tree on Apache BookKeeper.

Use the navigation on the left to browse, or jump to a common starting point:

- **[Getting started]({{< relref "/docs/getting-started" >}})** — run a cluster and store your first object.
- **[Installation & downloads]({{< relref "/docs/installation" >}})** — Docker image, distribution tarball, and client library.
- **[Concepts & architecture]({{< relref "/docs/architecture" >}})** — the LSM engine, ledgers, and fenced ownership.
- **[Command-line client]({{< relref "/docs/client" >}})** — the `candybox` CLI and the Java client library.
- **[S3 gateway]({{< relref "/docs/s3-gateway" >}})** — the S3-compatible HTTP endpoint.
- **[Operations]({{< relref "/docs/operations" >}})** — running, monitoring, and Kubernetes.
- **[Configuration reference]({{< relref "/docs/reference/configuration" >}})** — every tunable key.

> [!WARNING]
> Candybox is pre-`1.0` (current version `0.1.0-SNAPSHOT`). APIs, wire protocol, and on-disk formats
> may still change between releases.
