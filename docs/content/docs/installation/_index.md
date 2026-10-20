---
title: Installation & downloads
weight: 20
---

# Installation & downloads

There are three ways to get Candybox: the Docker image, the distribution tarball from
GitHub Releases, and the Java client library from Maven Central.

## Docker image

The container image is published to Docker Hub and is the recommended way to run Candybox:

```bash
docker pull zetaplusae/candybox:latest
```

The image is **dual-mode** — it runs a storage node by default, and `docker run … zetaplusae/candybox
candybox <args…>` runs the bundled client CLI instead. See the
[quick start]({{< relref "/docs/getting-started/quickstart" >}}) for a full Compose stack.

## Distribution tarball

Pre-built distribution archives are attached to each
[GitHub Release](https://github.com/predatorray/candybox/releases). The latest release is
**{{< param "candyboxRelease" >}}**:

```bash
curl -LO https://github.com/predatorray/candybox/releases/download/v{{< param "candyboxRelease" >}}/candybox-{{< param "candyboxRelease" >}}.tar.gz
tar xzf candybox-{{< param "candyboxRelease" >}}.tar.gz
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

The client is published to [Maven Central](https://central.sonatype.com/namespace/me.predatorray.candybox)
as of **{{< param "candyboxRelease" >}}**; no extra repository configuration is needed.

{{< tabs "client-dep" >}}
{{% tab "Maven" %}}
```xml
<dependency>
  <groupId>me.predatorray.candybox</groupId>
  <artifactId>candybox-client</artifactId>
  <version>{{< param "candyboxRelease" >}}</version>
</dependency>
```
{{% /tab %}}
{{% tab "Gradle" %}}
```groovy
implementation 'me.predatorray.candybox:candybox-client:{{< param "candyboxRelease" >}}'
```
{{% /tab %}}
{{< /tabs >}}

See the [command-line client]({{< relref "/docs/client" >}}) page for the CLI built on the same library.
