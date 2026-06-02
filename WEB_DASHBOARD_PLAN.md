# Web Dashboard Plan

A React + TypeScript + MUI dashboard for Candybox, shipped as a Maven submodule and served
in-process by a new admin HTTP API.

## Decisions (locked)

| Topic | Choice |
| --- | --- |
| Branch | `feat/web-dashboard` |
| Backend surface | S3 gateway (object data plane) **+** new `candybox-admin-api` module (JSON for cluster / LSM / metadata) |
| Views in v1 | Cluster overview · Box & object browser · LSM internals · Metrics & charts |
| Auth | **None** (matches current "trusted network" posture) |
| Mutating ops in UI v1 | Object delete (already unauthenticated via S3 today). **No** compaction-trigger, forced-handover, etc. — those require auth to land. |
| Live updates | Polling via TanStack Query (5 s default); no ZK-watch streaming |
| Frontend stack | Vite · React 18 · TypeScript strict · MUI v6 · Emotion · React Router · TanStack Query · Recharts · Zod · ESLint · Prettier · Vitest |
| UX defaults | Dark mode default with light toggle · density toggle (comfortable / compact) · Inter font |
| Maven module | `candybox-web` (the frontend) and `candybox-admin-api` (the backend) |
| Build wiring | `frontend-maven-plugin` (eirslett) — pinned Node + pnpm. Activated by `-Pfrontend` profile only, so plain `mvn -DskipTests package` stays fast. CI adds a second job with the profile. |
| Static-asset hosting | Admin API serves the bundle under `/ui/*` and redirects `/` → `/ui/`. Single port, single container, `docker compose up` Just Works. |
| Admin API port | `9712` (configurable via `admin.api.port`) |
| CORS | `*` (auth = none, single-tenant) |

## Module layout

```
candybox/
├── candybox-admin-api/        # new — Java HTTP module
│   ├── pom.xml
│   └── src/main/java/me/predatorray/candybox/admin/
│       ├── AdminApiServer.java         # jdk HttpServer, /api/* + /ui/*
│       ├── AdminApiConfig.java
│       ├── AdminApiMain.java           # CLI entrypoint
│       ├── ClusterView.java            # ZK-backed read of nodes / owners
│       ├── BoxAdminService.java        # wraps CandyboxClient for list/head/delete
│       ├── LsmView.java                # manifest / ledger / compaction snapshot
│       ├── MetricsProxy.java           # passthrough to per-node /metrics
│       ├── StaticUiHandler.java        # serves classpath:/ui/*
│       └── JsonCodec.java              # tiny hand-written JSON (no Jackson)
└── candybox-web/              # new — React + TS + MUI
    ├── pom.xml                # invokes frontend-maven-plugin under -Pfrontend
    ├── package.json
    ├── pnpm-lock.yaml
    ├── tsconfig.json
    ├── vite.config.ts
    ├── index.html
    └── src/
        ├── main.tsx
        ├── App.tsx
        ├── api/               # generated-ish typed clients + Zod schemas
        ├── components/        # AppShell, NavRail, DensityToggle, …
        ├── pages/
        │   ├── ClusterPage.tsx
        │   ├── BoxesPage.tsx
        │   ├── BoxDetailPage.tsx
        │   ├── LsmPage.tsx
        │   └── MetricsPage.tsx
        ├── theme/
        └── lib/
```

`candybox-admin-api` is what the new top-level `<module>` list grows by — it depends on
`candybox-client`, `candybox-coordination`, `candybox-common`, and (at runtime, classpath-only)
the `candybox-web` jar so the bundle ships with it.

## API surface (read-only unless flagged)

| Method | Path | Notes |
| --- | --- | --- |
| GET | `/api/cluster` | nodes, health, ZK status, owned-box count per node |
| GET | `/api/boxes` | all box names + owner + size summary |
| GET | `/api/boxes/{name}` | manifest version, fencing token, HLC, byte count, key count |
| GET | `/api/boxes/{name}/objects?prefix=&limit=` | wraps `CandyboxClient.listKeys` |
| GET | `/api/boxes/{name}/objects/{key}` | HEAD-equivalent metadata (size / etag / content-type / custom md) |
| DELETE | `/api/boxes/{name}/objects/{key}` | mutating; thin wrapper over `CandyboxClient.delete` (unauthenticated, matching gateway) |
| GET | `/api/lsm` | per-box LSM snapshot: SSTable / Syrup / WAL ledger inventory, in-flight compactions, GC backlog |
| GET | `/api/metrics` | passthrough of node `/metrics` (Prometheus text) |
| GET | `/api/metrics/timeseries?names=...` | last N (in-process, no persistence) samples for charting |
| GET | `/healthz`, `/readyz` | mirrors the existing per-node endpoints |
| GET | `/ui/*` | static SPA bundle from classpath |
| GET | `/` | 302 → `/ui/` |

Object **uploads and downloads** are not on this list — those go straight to the S3 gateway from
the browser. The admin API only handles list / metadata / delete.

## Implementation phases

1. **Scaffolding & branch hygiene** *(this commit)*
   - Cut `feat/web-dashboard`.
   - Add `candybox-admin-api` and `candybox-web` to root `pom.xml`.
   - Write this plan file.

2. **`candybox-admin-api` skeleton**
   - JDK `HttpServer`, `AdminApiConfig`, `AdminApiMain`.
   - `/healthz`, `/readyz`, `/api/cluster` (stub returning local node only).
   - `StaticUiHandler` serving from classpath `/ui/`.
   - Unit tests in the SPI+fake style (no mocks).

3. **`candybox-web` scaffolding**
   - Vite + React 18 + TS strict + MUI v6.
   - `frontend-maven-plugin` under `-Pfrontend`.
   - Bundle output copied into `target/classes/ui/` so the JAR carries it.
   - Theme, AppShell, NavRail, dark/light + density toggles, empty pages for each view.

4. **Cluster + Boxes views**
   - Admin API: `/api/cluster`, `/api/boxes`, `/api/boxes/{name}`, `/api/boxes/{name}/objects`.
   - UI: ClusterPage, BoxesPage, BoxDetailPage with sortable/searchable tables.

5. **LSM internals view**
   - Admin API: `/api/lsm` reading from `LsmEngine` snapshot (read-only).
   - UI: LsmPage with grouped tables (per box → ledgers/compactions/GC).

6. **Metrics & charts view**
   - Admin API: `/api/metrics` passthrough + `/api/metrics/timeseries` (rolling in-memory window).
   - UI: MetricsPage with Recharts time-series for a curated metric set.

7. **Docker & docs**
   - Extend `Dockerfile` and `docker-compose.yml` to expose `:9712` and run `admin-api`.
   - README section: "Dashboard at `http://localhost:9712`".
   - `OPERATIONS.md` note on the new port.

Each phase is one commit, with unit tests in the same commit. The integration test for the admin
API lives in `candybox-integration-tests` and uses the embedded BookKeeper / in-JVM ZooKeeper.

## Things deliberately out of scope for v1

- Authn / authz, audit log, RBAC.
- ZK-watch live updates (polling only).
- Persistent metrics storage (rolling window only).
- Mutating operations beyond `DELETE object` (no compaction trigger, no handover, no abort
  multipart) — those want auth first.
- Time-series metrics retention beyond a process-local rolling window.

## Open follow-ups (after v1 lands)

- Bearer-token auth on mutating endpoints, then re-add compaction / handover controls.
- Multipart-uploads view (in-flight, abandoned, TTL).
- ZK-watch-driven live updates for cluster topology.
- Embed Prometheus or push metrics into a real TSDB so the charts have history past process restart.
