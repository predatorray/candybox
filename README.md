# Candybox

Candybox is a **distributed, S3-like object store**. You create *buckets* and store *objects* in
them through a small TCP API or a command-line client; Candybox keeps those objects durable and
replicated across a cluster.

Under the hood it is a **distributed LSM tree built on [Apache BookKeeper](https://bookkeeper.apache.org/)**:
object data and index live in BookKeeper's replicated, append-only ledgers, and a single fenced owner
per bucket keeps reads and writes consistent during failover.

> **Vocabulary.** A **Box** is a bucket, a **Candy** is an object, a **CandyKey** is an object key,
> and a **Syrup** is a data ledger that holds object bytes. (Candy in a box — that's the whole theme.)

## Quick start

Candybox stores its data in **Apache BookKeeper** and coordinates ownership through **ZooKeeper**, so
those need to be running first. The fastest way to get a local environment is BookKeeper's bundled
local mode, which starts an in-process ZooKeeper plus some bookies in one command:

```bash
# 1. Start ZooKeeper + 3 bookies (download the Apache BookKeeper 4.17 binary distribution first).
bookkeeper-4.17.1/bin/bookkeeper localbookie 3
```

Then build and unpack a Candybox distribution:

```bash
# 2. Build the distribution archive.
mvn -q -DskipTests package
tar xzf candybox-dist/target/candybox-*.tar.gz
cd candybox-*/

# 3. Create a config from the example and point it at ZooKeeper.
cp conf/candybox.properties.example conf/candybox.properties
#   In conf/candybox.properties set at least:
#     node.id=1
#     zookeeper.connect=127.0.0.1:2181

# 4. Start the node (runs in the foreground; Ctrl-C / SIGTERM stops it gracefully).
bin/candybox-server
```

The node listens for clients on `9709` and serves health/metrics on `9710`:

```bash
curl -s localhost:9710/healthz    # -> ok
curl -s localhost:9710/readyz     # -> ready
curl -s localhost:9710/metrics    # Prometheus counters
```

### Single bookie (smallest possible setup)

By default each ledger is replicated across **3 bookies**, so the cluster needs at least three. To
run against a **single bookie** for local development, lower the quorum in `candybox.properties`:

```properties
quorum.wal=1/1/1
quorum.manifest=1/1/1
quorum.sstable=1/1/1
quorum.syrup=1/1/1
```

(The three numbers are *ensemble / write-quorum / ack-quorum*; `1/1/1` means "one copy, no
replication" — fine for a laptop, not for production.)

## Storing and retrieving objects

`bin/candybox` is a command-line client. It talks to one node over TCP, defaulting to
`127.0.0.1:9709` (override with `-s host:port` or the `CANDYBOX_SERVER` environment variable):

```bash
bin/candybox create-box photos
bin/candybox put  photos cat.jpg ./cat.jpg --content-type image/jpeg
bin/candybox get  photos cat.jpg ./out.jpg
bin/candybox head photos cat.jpg            # size, content-type, checksum, metadata
bin/candybox list photos                    # keys in the box
bin/candybox list-boxes
bin/candybox help                           # full command list
```

`put` reads from a file or, if you omit the path, from standard input; `get` writes to a file or to
standard output. Programmatically, the same operations are available through the `CandyboxClient`
class in the `candybox-client` module.

## Configuration

The node reads `conf/candybox.properties`. **Every key can be overridden by an environment
variable** named `CANDYBOX_<KEY>` (dots become underscores, upper-cased) — for example
`CANDYBOX_ZOOKEEPER_CONNECT` — and the environment value wins. This makes it easy to ship one image
and configure each instance through the environment. The most common keys:

| Key | Meaning | Default |
|---|---|---|
| `node.id` | Cluster-unique node id. Falls back to the trailing number in `$HOSTNAME` (so a Kubernetes pod `candybox-2` becomes node `2`). | — |
| `zookeeper.connect` | ZooKeeper connect string, shared by BookKeeper and Candybox coordination. | `127.0.0.1:2181` |
| `server.bind` | Address clients connect to. | `0.0.0.0:9709` |
| `server.advertised` | Address published to the cluster for routing (set to a reachable hostname). | bind address |
| `health.port` | HTTP port for `/healthz`, `/readyz`, `/metrics`. | `9710` |
| `quorum.*` | BookKeeper replication per ledger role (`E/Qw/Qa`). | `3/3/2` (WAL, manifest), `3/2/2` (data) |

See `conf/candybox.properties.example` for the full, commented list, and
[`OPERATIONS.md`](OPERATIONS.md) for operational guidance.

## Running on Kubernetes

The distribution ships container assets under `conf/k8s/`: a `Dockerfile` that builds an image from
the release tarball, and a `StatefulSet` + headless `Service` manifest. The StatefulSet gives each
pod a stable identity, so `node.id` and the advertised address derive automatically from the pod
name, and liveness/readiness probes hit the health endpoint.

## How it works (high level)

Candybox blends three well-known designs:

- **A LevelDB-style LSM tree** for the index. Writes land in an in-memory *memtable* fronted by a
  write-ahead log; when it fills, it is flushed to an immutable, sorted **SSTable** and later merged
  into larger ones by background **compaction**. Reads merge the memtable and SSTables, newest wins.

- **Object data kept out of the tree.** Object bytes are written to dedicated data ledgers
  (*Syrups*); the LSM tree stores only a small **pointer** to where each object lives. This keeps the
  index tiny and compaction cheap no matter how large the objects are.

- **BookKeeper ledgers as the durable medium.** Every SSTable, WAL, manifest, and Syrup is a
  BookKeeper ledger — append-only, replicated, and self-fencing. Candybox never mutates data in
  place; updates and deletes are new appends (with tombstones), Apache-Pulsar-style.

Consistency rests on **single, fenced ownership**: at any moment exactly one node owns a Box, holding
a ZooKeeper lease with a *fencing token*. Every state-changing operation carries that token, so if
ownership moves during a failure, a stale former owner can no longer corrupt the Box. Each write is
stamped with a hybrid logical clock for last-writer-wins ordering across nodes. The full record
formats and the reasoning behind the fencing/handover protocol are in [`DESIGN.md`](DESIGN.md).

## Building from source

Requirements: **Java 17+** and **Maven 3.9+**. No external services are needed to build or test — the
integration tests run an in-JVM BookKeeper (which bundles an in-process ZooKeeper).

```bash
mvn -q -DskipTests package   # compile and build the distribution archive
mvn test                     # fast unit tests (in-memory fakes only)
mvn verify                   # also run integration tests on embedded BookKeeper + ZooKeeper
```

Unit tests use hand-written in-memory fakes and stay fast and dependency-free; the integration tests
(`*IT.java`) exercise the real backends. A **shared contract-test suite** runs identically against
the fakes and the real BookKeeper-backed store, so the fast tests are a faithful stand-in for the
hard fencing/handover scenarios. No mocking frameworks are used anywhere.

## Project layout

| Module | Responsibility |
|---|---|
| `candybox-common` | Domain types, versioned serialization, configuration, CRC32C, bloom filter. |
| `candybox-bookkeeper` | The `LedgerStore` abstraction over BookKeeper — the only module that touches the raw BookKeeper client — with an in-memory fake. |
| `candybox-coordination` | Membership, fenced leases, and CAS key-value over ZooKeeper, with an in-memory fake. |
| `candybox-lsm` | The LSM engine: memtable, WAL, SSTables, Syrup chunking, manifest, merge/read path, compaction. |
| `candybox-protocol` | The framed TCP wire protocol and transport. |
| `candybox-server` | The storage node: wires the engine behind the protocol, plus the runnable entrypoint, health/metrics, and ownership. |
| `candybox-client` | The thin client library and the `candybox` command-line tool. |
| `candybox-dist` | Packages the runnable distribution (`bin/ lib/ conf/`) and the Docker/Kubernetes assets. |
| `candybox-integration-tests` | End-to-end tests on embedded BookKeeper + ZooKeeper. |
