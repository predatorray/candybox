# AGENTS.md

Guidance for coding agents working in this repository.

## Skills

Shared skills live in `.agents/skills/<skill-name>/SKILL.md` and follow the open
[Agent Skills specification](https://agentskills.io/specification). Edit skills there;
`.claude/skills` is a relative symlink to `../.agents/skills` for Claude compatibility.
Skill inputs come from the user's request, so they do not require agent-specific argument substitution.

Queue skills: `$issue-loop` continuously selects requester-authored GitHub issues,
dispatches isolated `$resolve-issue NUMBER` workers, independently judges PRs, and
squash-merges only after checks pass, using `PR title (#number)` with an empty
commit body. See
[issue-loop](.agents/skills/issue-loop/SKILL.md) for persistent claims and restart
handoffs; use `$resolve-issue NUMBER` directly for a single issue.

## Project

Candybox is a distributed, S3-like object store in Java 17 (Maven multi-module). Its storage engine is
a distributed LSM tree whose WAL, SSTables, manifest and object data all live in Apache BookKeeper
ledgers, with ZooKeeper used for coordination. Vocabulary used throughout the code: **Box** = bucket,
**Candy** = object, **CandyKey** = object key, **CandyLocator** = the LSM value (a pointer to the
bytes), and **Syrup** = a data ledger that holds the object bytes.

## Commands

Requires Java 17+ and Maven 3.9+. The chart module also requires Helm 3.17+ for
`package` and later phases (`-Dcandybox.charts.skip=true` skips it for image-only builds).
No external services are needed for the Java tests: the ITs start an in-JVM BookKeeper
and ZooKeeper.

```bash
mvn -q -DskipTests package      # build everything, including the distribution archive
mvn -q -DskipTests install      # what CI runs first; ITs need the sibling test-jars installed
mvn test                        # fast unit tests (in-memory fakes only)
mvn verify                      # unit + integration tests (*IT.java via failsafe) + license check

# single unit test (-am builds the upstream modules it depends on)
mvn -pl candybox-lsm -am test -Dtest=BoxEngineTest -Dsurefire.failIfNoSpecifiedTests=false
mvn -pl candybox-lsm -am test -Dtest='BoxEngineTest#handoverWithRegressedClockDoesNotLoseLatestWrite' -Dsurefire.failIfNoSpecifiedTests=false

# single integration test (after `mvn install -DskipTests`)
mvn -pl candybox-integration-tests verify -Dit.test=Phase1EngineIT

mvn license:format              # add the Apache header (etc/license-header.txt) to new .java files
mvn -DskipTests -Pfrontend package   # also build the React dashboard into candybox-web.jar
mvn -Prelease,frontend -DskipUpload=true deploy   # Maven Central dry run (needs a GPG key); see RELEASING.md
```

- `verify` runs `license-maven-plugin:check`, so every `src/{main,test}/java/**/*.java` file needs the
  license header.
- Integration tests only run under `verify`. Surefire is disabled in `candybox-integration-tests`, and
  failsafe forks a fresh JVM for each IT class with extra `--add-opens` flags. If you change that
  `argLine`, keep `@{argLine}` first, because JaCoCo injects its agent through it.
- `candybox-dist` runs bats tests for the `bin/` scripts (`bash candybox-dist/src/bin/test/run-bats.sh`),
  and skips them cleanly when `bats` is not installed.
- Dashboard (`candybox-web/`, React + TS + MUI + Vite): `npm install`, then `npm run dev` (port 5173,
  proxies `/api` to the admin API on :9713), `npm run lint`, `npm run format:check`, `npm test` (vitest),
  `npm run build`.
- Full local stack: `docker compose up -d` (ZooKeeper, 3 bookies, 3 nodes, S3 gateway on :9711, and the
  admin/dashboard on :9713/ui/). The S3 compatibility harness is `compat/s3-tests/run.sh`.
- Docs site: Hugo under `docs/`, with the theme as a git submodule (`git submodule update --init docs/themes/hugo-book`).

## Architecture

Layering, top to bottom: the client/protocol/server wrap a per-partition LSM engine, which talks only
to two narrow SPIs, and those SPIs run on BookKeeper and ZooKeeper.

- `candybox-common`: domain types, versioned `BinaryWriter`/`BinaryReader` serialization, HLC, config
  (`CandyboxConfig`), auth, TLS, and the bloom filter. Every layer uses it.
- `candybox-bookkeeper`: the `LedgerStore` SPI, an `InMemoryLedgerStore` fake, and
  `BookKeeperLedgerStore`. **This is the only module that touches the raw BookKeeper client.**
- `candybox-coordination`: the `CoordinationService` SPI (membership, fenced leases, CAS kv), an
  in-memory fake, and the ZooKeeper implementation. ZK key layout is in `CandyboxKeys`.
- `candybox-lsm`: `BoxEngine` (one per partition), WAL, memtable, SSTables, Syrup chunking, manifest,
  `MergingIterator`, leveled compaction. **It depends only on `common` and the two SPIs.** Never pull
  in the raw BK or ZK clients here.
- `candybox-protocol`: the framed TCP codec, `Opcode`/`Message` types, and the `Transport` SPI (Netty
  server, blocking client, `LoopbackTransport` fake). Also SASL auth.
- `candybox-server`: `CandyboxNode` wires the engines behind the protocol (`NodeRequestHandler`).
  `PartitionOwnership` ties together the lease, the fencing token and the manifest pointer, and
  `PartitionBalancer` (an elected coordinator) spreads partitions across nodes. It also has
  `CompactionService`, `GarbageCollector`, and `HealthServer` (`/healthz`, `/readyz`, `/metrics`).
- `candybox-client`: `CandyboxClient` with a cluster-aware `ClusterRouter` (sends keyed ops to the
  owner of the key's partition, scatter-gathers listings across partitions, follows `RESPONSE_MOVED`),
  plus the `candybox` CLI (`CandyboxCli`).
- `candybox-s3-gateway`: a stateless, path-style S3 HTTP gateway (Netty) on top of the client, with
  optional SigV4 auth and S3 ACLs.
- `candybox-admin-api` and `candybox-web`: a stateless JSON admin API, plus the SPA that it serves from
  the classpath at `/ui/`.
- `candybox-dist` packages the `bin/ lib/ conf/` distribution. `candybox-integration-tests` holds all
  the `*IT` tests.

Core invariants (details in `DESIGN.md`):

- **Hash-partitioned Boxes with a single fenced owner per partition.** A Box has a fixed partition
  count, set at creation (`BoxDescriptor`, CRC32C(key) mod N). Each partition is an independent LSM
  with its own WAL, memtable, manifest and SSTables. Exactly one node owns a partition, under a ZK
  lease, and every state-changing append carries its fencing token. Handover uses BookKeeper
  **recover-open** (fence and seal) on the prior WAL and manifest, never a plain read-open. The
  manifest pointer advances only by versioned CAS.
- **HLC last-writer-wins.** The owner stamps the HLC at ingest, never the client. On recovery the new
  owner must `observe` `max(manifest maxHlc, WAL max)` before it stamps anything, or newer writes can
  lose to older ones.
- **Object bytes never enter the LSM tree.** They go to Syrups, and the tree stores only
  `CandyLocator` pointers. That is why copy and rename are zero-copy: they share `SegmentRef`s, and GC
  reference-counts Syrups by segment, Box-globally. A cross-partition rename is only eventually atomic
  (rename-intent journal plus a ZK rendezvous marker).
- **Every persisted record starts with a format version byte**: big-endian, LEB128 varints, and
  bounds-checked decoding. If you change a record format, bump its version and keep the old version
  decodable.
- Data is never mutated in place. Updates and deletes are new appends: tombstones and O(1) range
  tombstones, reclaimed by compaction.

## Testing conventions

- **No mocking frameworks.** Unit tests use the hand-written in-memory fakes (`InMemoryLedgerStore`,
  `InMemoryCoordinationService`, `LoopbackTransport`, `ManualClock`) with JUnit 5 and AssertJ.
- **Shared contract suites.** `LedgerStoreContract` and `CoordinationServiceContract` (published as
  test-jars) run against the fakes as `*ContractTest` and against the real backends as `*ContractIT`.
  When you change SPI behavior, update the contract so that the fake and the real implementation stay
  equivalent.
- Integration tests (`*IT.java`) live only in `candybox-integration-tests`.

## Further docs

`RELEASING.md` covers publishing to Maven Central. `DESIGN.md` covers record formats, read/write/merge paths, manifest and fencing, compaction, GC, size
limits, and the deliberate v1 simplifications. `OPERATIONS.md` covers configuration and operations
(every config key can be overridden by a `CANDYBOX_<KEY>` env var). The `*_PLAN.md` files are design
plans for individual features (partitioning, cross-partition zero-copy, S3 gateway, multipart/range,
auth, dashboard).
