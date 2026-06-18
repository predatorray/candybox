---
title: Installation & downloads
weight: 20
---

# Installation & downloads

There are three ways to get Candybox: the Docker image (available today), a distribution tarball
built from source, and the Java client library.

## Docker image

The container image is published to Docker Hub and is the recommended way to run Candybox:

```bash
docker pull zetaplusae/candybox:latest
```

The image is **dual-mode** — it runs a storage node by default, and `docker run … zetaplusae/candybox
candybox <args…>` runs the bundled client CLI instead. See the
[quick start]({{< relref "/docs/getting-started/quickstart" >}}) for a full Compose stack.

## Distribution tarball

> [!WARNING]
> **Not yet published.** Tagged releases and pre-built distribution archives are not available on
> [GitHub Releases](https://github.com/predatorray/candybox/releases) yet. Until the first release is
> cut, build the distribution from source (below). This page will be updated with download links when
> releases are published.

The intended download, once releases are published, will look like:

```bash
# Placeholder — not yet published
curl -LO https://github.com/predatorray/candybox/releases/download/v0.1.0/candybox-0.1.0-bin.tar.gz
tar xzf candybox-0.1.0-bin.tar.gz
```

### Build it from source

Requirements: **Java 17+** and **Maven 3.9+**. No external services are needed to build — the
integration tests run an in-JVM BookKeeper (which bundles an in-process ZooKeeper).

```bash
git clone https://github.com/predatorray/candybox.git
cd candybox
mvn -q -DskipTests package   # builds the distribution archive under candybox-dist/target/
```

This produces a `bin/ lib/ conf/` distribution (and the Docker/Kubernetes assets) from the
`candybox-dist` module.

## Java client library

To talk to Candybox programmatically, use the `CandyboxClient` class from the `candybox-client`
module.

> [!WARNING]
> **Not yet published to Maven Central.** The coordinates below are a placeholder for the upcoming
> first release. Until then, build and `mvn install` the project locally (`mvn -q -DskipTests install`)
> to resolve the artifact from your local repository.

{{< tabs "client-dep" >}}
{{< tab "Maven" >}}
```xml
<!-- Placeholder — not yet published to Maven Central -->
<dependency>
  <groupId>me.predatorray.candybox</groupId>
  <artifactId>candybox-client</artifactId>
  <version>0.1.0</version>
</dependency>
```
{{< /tab >}}
{{< tab "Gradle" >}}
```groovy
// Placeholder — not yet published to Maven Central
implementation 'me.predatorray.candybox:candybox-client:0.1.0'
```
{{< /tab >}}
{{< /tabs >}}

See the [command-line client]({{< relref "/docs/client" >}}) page for the CLI built on the same library.
