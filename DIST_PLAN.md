# DIST_PLAN — `candybox-dist` distribution & runnable node

> **Status: implemented.** All five workstreams below are complete; `mvn test` (incl. the bats
> script tests) and `mvn verify` (embedded BookKeeper + ZooKeeper) are green, and the assembled
> `.tar.gz`/`.zip` boots through config loading + logging from the bundled `lib/`.


Goal: package Candybox as a self-contained `.tar.gz`/`.zip` distribution
(`bin/ lib/ conf/ logs/`) with a real process entrypoint, file+env configuration,
graceful shutdown, health/metrics endpoints, and cloud-native deployment artifacts
(Dockerfile + k8s StatefulSet).

## Decisions (locked)

- **Config format:** `.properties` with `CANDYBOX_*` environment-variable overrides (12-factor; env wins).
- **Node identity:** `node.id` resolves `CANDYBOX_NODE_ID` → trailing ordinal of `HOSTNAME`
  (StatefulSet `pod-N`) → config value. Advertised address resolves `CANDYBOX_ADVERTISED`
  → config → bind host.
- **ZooKeeper topology:** one shared `zookeeper.connect`; BookKeeper `metadataServiceUri` and the
  coordination connect string default to it and are independently overridable.
- **Cloud-native scope (all in):** graceful SIGTERM shutdown hook; HTTP health/readiness endpoint;
  Dockerfile + k8s manifests; Prometheus metrics exporter.
- **Zero-extra-dep discipline:** health/metrics use the JDK `com.sun.net.httpserver.HttpServer`
  and a hand-rolled Prometheus text exposition — no Prometheus client lib. Only **logback-classic**
  is added as the one production SLF4J binding (test scope keeps `slf4j-simple`).
- **Foreground process only** (PID 1, logs to stdout); the orchestrator owns restarts. No daemon/pidfile.
- **`main` lives in `candybox-server`**; `candybox-dist` is a pure assembly module.

## What exists today (verified)

- `CandyboxNode(nodeId, CandyboxConfig, LedgerStore, CoordinationService, Clock, advertisedAddress)`
  is the full node assembly — but only ever built in tests (`BoxClusterHandoverIT`).
- Backends: `BookKeeperLedgerStore(ClientConfiguration, password)`,
  `ZooKeeperCoordinationService(connectString, Clock)`, `TcpTransportServer(port, RequestHandler, FrameCodec)`.
- `CandyboxConfig` is a *tuning-only* builder (no endpoints/ports/paths/nodeId).
- `BoxEngine.stats()` → `BoxEngineStats(puts, deletes, gets, heads, lists, flushes, compactions, stallRejections)`.
- BK metadata URI form: `zk://<host:port>/ledgers`.
- The 6 JVM `--add-opens` flags BK/ZK need on modern JDKs live in the IT pom `argLine`.
- No `main()`, no config file parsing, no production logging binding — all confirmed absent.

## Workstreams

### WS1 — Runtime entrypoint & config (`candybox-server`)
- `ServerConfig`: load `candybox.properties`, apply `CANDYBOX_*` env overrides, resolve node identity
  & advertised address, expose endpoints/ports/paths, and map tuning keys onto `CandyboxConfig.Builder`.
- `CandyboxServer` (`public static void main`): build BK `ClientConfiguration` (metadataServiceUri from
  shared ZK) → `BookKeeperLedgerStore` → `ZooKeeperCoordinationService` → `CandyboxNode` →
  `TcpTransportServer` → `HealthServer`; install a shutdown hook closing all in reverse order; block.
- `HealthServer`: JDK `HttpServer` with `/healthz`, `/readyz`, `/metrics` (Prometheus text from
  `BoxEngineStats` across owned boxes). Add a node accessor to enumerate owned boxes + stats if needed.

### WS2 — `candybox-dist` assembly module
- New `pom`-packaging module; depends on `candybox-server` + `logback-classic`.
- `maven-assembly-plugin` + `src/assembly/dist.xml` → `candybox-<ver>/{bin,lib,conf,logs}`, `tar.gz` + `zip`.
- `conf/candybox.properties.example` (every key documented), `conf/logback.xml`.

### WS3 — `bin/` scripts + bats tests
- `bin/candybox-env.sh`: JAVA opts incl. the 6 `--add-opens`, `lib/*` classpath, `CANDYBOX_CONF_DIR`/`CANDYBOX_LOG_DIR`.
- `bin/candybox-server`: foreground `exec java …`.
- `bin/candybox`: thin client CLI wrapper.
- `bin/test/*.bats`: classpath resolves, refuses to start without config, `--add-opens` present, env overrides honored. Guarded so absence of `bats` is a no-op.

### WS4 — Cloud-native artifacts
- root `Dockerfile` (multi-stage build from source, foreground entrypoint; Docker Hub friendly).
- `examples/kubernetes/statefulset.yaml` (StatefulSet + headless Service; stable ids → clean nodeId/advertised derivation; probes hit `health.port`).

### WS5 — Wiring & docs
- Root `pom.xml`: add `candybox-dist` to `<modules>` and logback to `<dependencyManagement>`.
- `README.md`: "Running a node" + dist layout; status note. Brief `DESIGN.md` pointer if format-relevant (it isn't).

## Config keys (initial surface)

```
node.id=                      # optional; else HOSTNAME ordinal / CANDYBOX_NODE_ID
server.bind=0.0.0.0:9709      # TCP listen
server.advertised=            # host:port published to membership (CANDYBOX_ADVERTISED wins)
health.port=9710             # HTTP health/metrics
zookeeper.connect=127.0.0.1:2181
bookkeeper.metadataServiceUri=  # default zk://${zookeeper.connect}/ledgers
coordination.connect=           # default ${zookeeper.connect}
ledger.password=candybox
data.dir=./data
log.dir=./logs
# tuning passthrough → CandyboxConfig (memtableFlushThresholdBytes, syrupRolloverBytes,
#   compactionIntervalMillis, ownershipLeaseTtlMillis, leaseRenewIntervalMillis, ledgerGcGraceMillis, ...)
```

## Validation

- `mvn -q -DskipTests package` builds the dist artifacts.
- Unpack the tarball; `bin/candybox-server` starts, `/healthz` returns 200, fails fast on missing config.
- `bats bin/test` passes where `bats` is installed.
- Existing `mvn test` / `mvn verify` stay green (no test renamed/retyped).
