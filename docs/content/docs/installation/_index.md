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

The full API reference is on javadoc.io:
[`CandyboxClient` Javadoc](https://javadoc.io/doc/me.predatorray.candybox/candybox-client/latest/me/predatorray/candybox/client/CandyboxClient.html)
(the [`candybox-client` package index](https://javadoc.io/doc/me.predatorray.candybox/candybox-client/latest/index.html)
lists everything else). The `latest` link always resolves to the newest release published on Maven
Central, never a snapshot.

### Connecting

The examples below assume you started the stack with `docker compose up -d`, as in the
[quick start]({{< relref "/docs/getting-started/quickstart" >}}). A Box is split into partitions that
are spread across the nodes, so use the **cluster-aware** client: it discovers the nodes through
ZooKeeper (published on `:2181`) and sends each request to the node that owns the key's partition.

```java
import me.predatorray.candybox.client.CandyboxClient;
import me.predatorray.candybox.common.SystemClock;
import me.predatorray.candybox.common.config.CandyboxConfig;
import me.predatorray.candybox.coordination.CoordinationService;
import me.predatorray.candybox.coordination.zk.ZooKeeperCoordinationService;
import me.predatorray.candybox.protocol.transport.TcpTransport;
import me.predatorray.candybox.protocol.transport.Transport;

try (Transport transport = new TcpTransport();
     CoordinationService zk = new ZooKeeperCoordinationService("localhost:2181", SystemClock.INSTANCE);
     CandyboxClient client = new CandyboxClient(transport, zk, CandyboxConfig.defaults())) {
    // ... use the client, see below
}
```

{{% hint warning %}}
The nodes advertise their Compose hostnames (`candybox-1:9709`, …), which only resolve **inside** the
Compose network. Run your application there too, for example
`docker run --rm --network candybox_default -v "$PWD:/app" -w /app eclipse-temurin:17 …`,
or from another container attached to `candybox_default`. Pointing `ZooKeeperCoordinationService` at
`zookeeper:2181` from inside the network works the same way.
{{% /hint %}}

### Storing and reading objects

```java
import java.nio.charset.StandardCharsets;
import java.util.Map;

client.createBox("photos");                               // BoxAlreadyExistsException if it exists

byte[] bytes = "hello candybox".getBytes(StandardCharsets.UTF_8);
client.putCandy("photos", "greetings/hello.txt", bytes, "text/plain",
        Map.of("author", "me"), null);                   // user metadata, idempotency token (optional)

byte[] back = client.getCandy("photos", "greetings/hello.txt");

CandyboxClient.CandyInfo info = client.headCandy("photos", "greetings/hello.txt");
System.out.println(info.contentLength() + " bytes, " + info.contentType());

client.deleteCandy("photos", "greetings/hello.txt");
```

A missing object or Box raises `CandyNotFoundException` / `BoxNotFoundException`, both unchecked
subclasses of `CandyboxException`.

### Listing and range reads

Keys are kept sorted, so listing by prefix, paging, and byte-range reads are cheap:

```java
// Page through everything under a prefix, 100 keys at a time.
String cursor = null;
do {
    CandyboxClient.Listing page = client.listCandies("photos", "greetings/", cursor, 100);
    page.entries().forEach(e -> System.out.println(e.key() + " " + e.contentLength()));
    cursor = page.nextStartAfter();                       // null on the last page
} while (cursor != null);

// First 5 bytes of an object (both ends inclusive, like an HTTP Range header).
byte[] head = client.getCandyRange("photos", "greetings/hello.txt", 0, 4).data();
```

### Zero-copy copy, rename and bulk delete

Copy and rename re-point the new key at the existing data instead of rewriting bytes, and a prefix
delete is a single tombstone per partition:

```java
client.copyCandy("photos", "greetings/hello.txt", "backup/hello.txt", null);
client.renameCandy("photos", "backup/hello.txt", "archive/hello.txt", null);
client.deleteRangeByPrefix("photos", "archive/");
```

See the [command-line client]({{< relref "/docs/client" >}}) page for the CLI built on the same library.
