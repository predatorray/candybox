# Candybox — Phase 2 Implementation Plan

**Scope (from the roadmap):** server node, metadata-ledger manifest with a ZooKeeper pointer, cluster
membership & Box assignment/routing, the minimal framed TCP protocol + thin client, node-side
validation. The cores (ledger-backed manifest, leveled compaction execution, framed codec, transports,
node handler, thin client) already exist from Phase 1; Phase 2 makes them distributed and durable.

## 0. Exit criteria

A 2+ node cluster on embedded BookKeeper + in-process ZooKeeper where:
- a Box is created, owned under a **fenced ZK lease**, with its manifest reachable via a **versioned ZK pointer**;
- the **thin client routes** each request to the current owner and re-routes on ownership change;
- killing the owner's lease causes another node to **acquire → recover (replay + fence) → CAS the pointer → resume**, while the old owner's writes are **fenced**;
- `mvn test` stays fake-only and green; new `*IT`s cover the handover on real BK + ZK.

## 1. Decisions (locked)

| # | Decision | Call |
|---|---|---|
| D1 | Fencing-token type & checks | `long` monotonic per resource (from `Lease.fencingToken()`). **Double-fence**: BK recover-open is the hard fence; additionally every `ManifestEdit` carries `ownerFencingToken` and `Manifest.apply` rejects `token < state.maxToken`. |
| D2 | Manifest pointer | Coordination KV key `boxes/<box>/manifest` → `{ledgerId, ownerToken}`, advanced only via `compareAndSet(expectedVersion)`. |
| D3 | Ownership | **Opportunistic, no central assignor.** Any node may `tryAcquireLease("boxes/<box>/owner")`; holder owns it; on expiry another takes over. |
| D4 | Routing | **Client-side routing + redirect.** Client caches box→owner from ZK; a non-owner node replies `RESPONSE_MOVED(nodeId)`; client refreshes and retries. |
| D5 | Large objects on the wire | Inline single frame for ≤ maxFrame; multi-frame chunked PUT/GET (`*_BEGIN/_CHUNK/_END`) as a separable sub-stream (may slip to 2.5). |
| D6 | Manifest checkpoint cadence | Roll to a fresh manifest ledger after `manifestCheckpointEdits` (default 1000) or on handover. |
| D7 | `BoxEngine` ↔ coordination boundary | Engine stays **coordination-free**: it takes only a `long fencingToken`; the node/`BoxOwnership` does all ZK. Preserves "`candybox-lsm` depends only on the two SPIs". |
| D8 | Lease mechanism | **TTL + CAS + monotonic fencing token** over a regular znode, *not* ephemeral-session leases — so the fake and the ZooKeeper impl have identical, clock-driven semantics and share one contract suite. Safety comes from the fencing token, not session liveness. |

## 2. Workstreams (dependency order)

### WS1 — Coordination contract + real ZooKeeper impl  *(foundational; implemented first)*
- `CoordinationServiceContract` shared test base (published as a test-jar), exercising CAS create/update/delete conflicts, TTL-lease acquire/expiry/supersession with monotonic tokens (driven by an injected `Clock`), renew, release, and membership.
- `ZooKeeperCoordinationService` (Apache Curator): versioned KV via znode `dataVersion`; TTL-CAS leases over a persistent znode storing `{owner, token, expiry, released}`; membership under `/members`. Takes a `Clock` so lease TTL is clock-driven and testable.
- Validate the contract against **both** `InMemoryCoordinationService` (unit) and `ZooKeeperCoordinationService` on an embedded ZooKeeper (`curator-test` `TestingServer`, IT).

### WS2 — Fencing-token plumbing  *(parallel; uses the fake)*
- `ManifestEdit` gains `long ownerFencingToken`; `Manifest` tracks `maxToken` and `apply` throws `FencedException` on regression.
- `BoxEngine.createNew/recover` take a `long fencingToken` stamped into every edit.

### WS3 — Ownership + manifest pointer + ZK-driven recovery  *(needs WS1, WS2)*
- New `BoxOwnership` (server): acquire lease → read pointer → `createNew` or `recover` → `create`/`compareAndSet` the pointer (abort + release on CAS conflict) → lease-renew heartbeat → quiesce on lease loss.
- `CandyboxNode` uses `BoxOwnership` instead of owning every box unconditionally.

### WS4 — Protocol completeness  *(independent; early)*
- Add `HeadCandyResponse`, `ListBoxesResponse`, `RESPONSE_MOVED(int ownerNodeId)`; fill the `MessageCodec` default branch.
- (D5) `PutBegin/Chunk/End`, `GetBegin/Chunk/End`. Codec round-trip tests for each.

### WS5 — Membership + routing  *(needs WS1, WS4)*
- Server registers `members/<nodeId>=host:port`, heartbeats, deregisters on close; handler returns `RESPONSE_MOVED` when it isn't the owner.
- Client `ClusterRouter`: box→owner→address from coordination, cached with TTL, invalidated on `MOVED`, bounded retries. `CandyboxClient` swaps its single `Connection` for `Transport` + `ClusterRouter`.

### WS6 — Wire-up + integration
- `CandyboxConfig` knobs: `manifestCheckpointEdits`, `leaseRenewIntervalMillis`, `routerCacheTtlMillis`.
- 2-node handover IT on embedded BK + ZK (see exit criteria).

## 3. New / changed signatures (summary)

```
common:        CandyboxConfig + manifestCheckpointEdits, leaseRenewIntervalMillis, routerCacheTtlMillis
lsm:           ManifestEdit + long ownerFencingToken; Manifest.apply enforces token monotonicity;
               BoxEngine.createNew/recover gain long fencingToken   (still no coordination dependency)
coordination:  ZooKeeperCoordinationService (implemented); CoordinationServiceContract (shared test-jar)
protocol:      Opcode + RESPONSE_MOVED (+ streaming); Message + HeadCandyResponse/ListBoxesResponse/MovedResponse
server:        BoxOwnership; CandyboxNode uses it; NodeRequestHandler returns MOVED when not owner
client:        ClusterRouter; CandyboxClient over Transport + ClusterRouter
```

## 4. Test strategy
- **Fakes (`mvn test`)**: coordination contract vs fake; ownership handover via lease expiry with `ManualClock`; stale-token manifest rejection; router redirect via a `LoopbackTransport` that returns `MOVED` once.
- **Integration (`mvn verify`)**: coordination contract vs `ZooKeeperCoordinationService` on embedded ZK; 2-node handover on embedded BK + ZK.

## 5. Sequencing & risks
- Critical path: WS1 → WS2 → WS3 → WS6. WS4 and the fake-backed parts of WS5 run in parallel.
- Biggest risk: the recover→CAS-pointer race — the lease (only the holder recovers) plus CAS-abort-on-conflict is the single serialization point.
- Invariant to protect: the engine never imports coordination (D7); all ZK lives in `candybox-server`.
- Deferrable: D5 streaming can ship as Phase 2.5.

---

## WS1 status (this change)

Implemented:
- `CoordinationServiceContract` (shared test base, published as a `candybox-coordination` test-jar).
- `InMemoryCoordinationServiceContractTest` — the fake passes the contract.
- `ZooKeeperCoordinationService` — Curator-backed KV + TTL-CAS leases + membership, `Clock`-driven.
- `ZooKeeperCoordinationServiceContractIT` — the real impl passes the same contract on an embedded ZooKeeper.

`curator-framework` is an **optional** compile dependency of `candybox-coordination` (the fake needs
nothing); modules that want the ZK impl declare Curator themselves.
