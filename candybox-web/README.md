# candybox-web

React + TypeScript + MUI dashboard for Candybox. Lives as a Maven submodule; the build is driven by
`frontend-maven-plugin` only under `-Pfrontend` so plain `mvn -DskipTests package` stays fast.

## Layout

```
src/
├── api/          # typed API clients (Zod-validated against the admin API JSON shape)
├── components/   # shared UI (AppShell, NavRail, density/theme toggles…)
├── pages/        # one file per route — Cluster, Boxes, BoxDetail, Lsm, Metrics
├── theme/        # MUI theme factory (dark default, density toggle)
├── lib/          # small framework-agnostic helpers
├── App.tsx
└── main.tsx
```

## Local dev

```bash
# from candybox-web/
npm install
npm run dev         # vite dev server on :5173, proxies /api → :9712
```

In another terminal, run the admin API standalone so the dev server has something to proxy:

```bash
mvn -pl candybox-admin-api -am compile exec:java \
    -Dexec.mainClass=me.predatorray.candybox.admin.AdminApiMain
```

## Production build (inside the maven reactor)

```bash
mvn -Pfrontend -DskipTests package
```

That puts the built bundle into `target/classes/ui/`, which `maven-jar-plugin` packages into
`candybox-web-*.jar` at `/ui/...`. The admin-api's `StaticUiHandler` reads from that exact path.
