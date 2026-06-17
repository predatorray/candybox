# S3 compatibility tests (ceph/s3-tests)

This harness runs the de-facto industry-standard S3 compatibility suite,
[`ceph/s3-tests`](https://github.com/ceph/s3-tests), against the Candybox S3 gateway
(`candybox-s3-gateway`). It is the same suite used to validate Ceph RGW, Garage, SeaweedFS and
most other S3 clones, so a passing allowlist is a meaningful, externally-recognised statement of
"S3-compatible — for this subset."

The suite itself is **not** vendored. `run.sh` clones it on demand, pins it to a git ref, builds a
throwaway virtualenv, and points it at a gateway endpoint. Everything it generates lives under
`.work/` (git-ignored).

## What can pass

The suite calibrates the gateway with **SigV4 auth + S3 ACL enforcement enabled** (Phase D/E) — the
`docker-compose.ci.yml` stack signs every request as the `s3tests` accounts (see
`gateway-credentials.properties` / `s3tests.conf.in`), so the multi-user, ACL, and cross-account
access tests are reachable. The gateway is **path-style only** and implements a deliberately small
slice of S3:

| Supported (allowlist candidates)        | Not supported in v1 (excluded)                          |
|------------------------------------------|---------------------------------------------------------|
| Bucket create / head / delete            | Versioning                                              |
| ListObjectsV2: prefix, delimiter,        | Bucket policy / public-access block                     |
| max-keys, continuation-token, start-after| Conditional GET (If-Match / If-None-Match / If-*-Since) |
| Object PUT / GET / HEAD / DELETE         | Tagging, lifecycle, CORS, SSE                           |
| Multi-object delete                      | POST object (browser-style form upload)                 |
| Range GET (Phase 5)                      | Object Lock / retention / legal hold (3 trivial passes) |
| Multipart upload — create / parts / complete / abort / list (Phase 5) | `UploadPartCopy` / multipart-copy        |
| `x-amz-meta-*` user metadata, CRC32C ETag| Object attributes / checksums (SHA-256, CRC*)           |
| **SigV4 auth + canned/grant ACLs, multi-user isolation, cross-account access** | Lifecycle / inventory / replication |

> **ListBuckets is ownership-scoped.** `ListAllMyBuckets` returns only the buckets a principal owns,
> never buckets merely granted READ — without that, a `public-read` bucket from one account leaks
> into every account's listing and the suite's cross-account teardown cascades into errors. Earlier
> auth-enabled calibration attempts hit exactly this; the fix (gateway `S3AccessControl.owns`) is
> what makes a clean 0-error auth-mode run possible.

## Running

The gateway needs a live cluster behind it. Two ways to stand one up:

| Stack | Brings up | When |
|-------|-----------|------|
| `docker-compose.yml` (repo root) | ZK + 3 bookies + **3** nodes + gateway, from the **published** `zetaplusae/candybox` image | quick demo against a release image |
| `compat/s3-tests/docker-compose.ci.yml` | ZK + 3 bookies + **1** node + gateway, **built from your local source** | the gate, and the recommended path on a laptop |

On an Apple-Silicon laptop, prefer the **CI compose**: building from source produces a native
arm64 candybox image (the published one is amd64-only and runs emulated), so only the upstream
BookKeeper image emulates. Both expose the gateway on `:9711`.

```bash
# From the repo root. Build the candybox image from source, then start the slimmed stack.
docker compose -f compat/s3-tests/docker-compose.ci.yml build
docker compose -f compat/s3-tests/docker-compose.ci.yml up -d

# Wait until the gateway actually serves (PUT a throwaway bucket until it answers 200).
until [ "$(curl -s -o /dev/null -w '%{http_code}' -X PUT localhost:9711/preflight)" = "200" ]; do sleep 3; done
curl -s -o /dev/null -X DELETE localhost:9711/preflight

# First run: discover exactly what passes and record it as the allowlist.
compat/s3-tests/run.sh --calibrate

# Thereafter: regression-gate against the calibrated allowlist (non-zero exit on any failure).
compat/s3-tests/run.sh

# Tear down (–v also wipes the data volumes).
docker compose -f compat/s3-tests/docker-compose.ci.yml down -v
```

### Modes

| Command                         | What it does                                                              |
|---------------------------------|---------------------------------------------------------------------------|
| `run.sh`                        | Runs only `allowlist.txt` — the compatibility gate.                       |
| `run.sh --all`                  | Runs the full boto3 functional suite and reports (expect many failures).  |
| `run.sh --calibrate`            | Runs `--all`, then rewrites `allowlist.txt` with the tests that PASSED, stamping the suite commit + endpoint. |
| `run.sh -k test_bucket_list_empty` | Passes `-k EXPR` straight through to pytest for poking at one test.    |

### Environment overrides

| Var           | Default                                       | Purpose                                  |
|---------------|-----------------------------------------------|------------------------------------------|
| `S3_ENDPOINT` | `http://127.0.0.1:9711`                       | Gateway base URL. Use an **IP** host, not `localhost`, so boto3 stays path-style. |
| `S3TESTS_REF` | `master`                                      | `ceph/s3-tests` ref to pin. Calibration records the resolved SHA in the allowlist header. |
| `S3TESTS_REPO`| `https://github.com/ceph/s3-tests.git`        | Clone URL.                               |
| `TEST_PATH`   | `s3tests/functional/test_s3.py`         | Suite path to run.                       |
| `PYTEST_ARGS` | _(empty)_                                     | Extra args appended to pytest.           |

## The allowlist as a contract

`allowlist.txt` is the checked-in, authoritative pass set — the same "shared external suite defines
the bar" philosophy as the project's `LedgerStoreContract` / `CoordinationServiceContract`. The
seed shipped here is **provisional** (marked as such in the file) until a `--calibrate` run pins it
to a real suite revision. When the gateway gains a feature (e.g. multipart in a later phase),
re-calibrate and the allowlist grows on its own.

Requirements on the runner host: `python3` (with `venv`), `git`, and network access to clone the
suite and pip-install boto3/pytest the first time.

## Latest calibration

Last calibrated against ceph/s3-tests `master` @ `5522d1c` on **2026-06-17**, against the gateway with
**SigV4 auth + S3 ACLs enabled** over a single-node, single-bookie in-JVM stack (full suite path
`s3tests/functional/test_s3.py`, all 838 items, **0 errors** — every test ran to a clean pass/fail
with no fixture/teardown breakage). The result is reproducible: re-running `--calibrate` against the
same source produces a byte-identical `allowlist.txt` apart from the timestamp header.

| Outcome | Count | Δ vs prev | Notes |
|---|---:|---:|---|
| **Passed** (in `allowlist.txt`) | **192** | +28 | The compatibility gate. |
| Failed | 552 | −28 | Features the v1 gateway doesn't yet implement (breakdown below). |
| Skipped | 94 | — | Suite-level `pytest.mark` opt-outs — never executed (bucket logging, cloud-tier lifecycle, restore-status). |
| Collected | 838 | — | All of `s3tests/functional/test_s3.py`. |

What changed since the previous calibration (164 passes, after Phase 5's multipart + Range GET):
turning **SigV4 auth + S3 ACLs** on (Phase D/E) — plus the `ListBuckets` ownership-scoping fix that
makes a clean auth-mode run possible — brought a **net +28**, **28 newly passing, 0 regressions**.
The new passes are exactly the tests real authentication unlocks:

- **ACLs (`bucket_acl_*`, `object_acl_*`, `*_canned_*`):** default ACLs, canned-ACL round-trips,
  full-control attribute checks, concurrent canned-ACL sets.
- **Cross-account access (`access_bucket_private_*`, `object_copy_not_owned_*`):** a private
  bucket/object correctly denies another account; public-read/-write grants correctly allow it.
- **Auth negatives (`list_buckets_bad_auth`, `list_buckets_invalid_auth`):** bad/garbage SigV4
  credentials are rejected with the right error.
- **Anonymous access (`bucket_list*_objects_anonymous*`, `object_anon_put`):** unsigned requests are
  allowed or denied per ACL.
- Plus the not-found / error-response-shape edges (`bucket_notexist`, `object_copy_bucket_not_found`,
  `object_write_to_nonexist_bucket`, …).

The 552 remaining failures are the **growth surface** — implementing any of these would expand the
allowlist on the next `--calibrate`. Each failing test is counted once (first matching family):

| Failures | Feature family |
|---:|---|
| 133 | Server-side encryption — SSE-C / SSE-S3 / SSE-KMS (incl. the `copy_enc[…]` matrix) |
| 51 | Versioning (`versioning_*`, `delete_marker_*`, `*_versioned`) |
| 43 | ACLs / grants / ownership edges still unimplemented (beyond the basics now passing) |
| 43 | Lifecycle / expiration / non-current-version rules |
| 41 | Bucket policy / block-public-access |
| 36 | Object Lock / retention / legal hold / governance bypass (beyond the few that pass) |
| 31 | POST object (browser-style form uploads) |
| 28 | Bucket logging (the bits not behind the `pytest.mark` skip) |
| 22 | Conditional GET/PUT — `If-Match` / `If-None-Match` / `If-*-Since` |
| 20 | `ListObjects(V2)` edge cases — encoding-type, exotic keys, sort order, markers |
| 18 | Bucket create / naming negative + misc bucket-level |
| 14 | CORS preflight + actual cross-origin |
| 14 | Tagging beyond the basic put/get already in the allowlist |
| 14 | Object attributes / checksums (SHA-256, CRC*) |
| 13 | Object copy edges (metadata-directive, content-type override, ACL on copy) |
| 12 | Multipart upload edges — `copy_part` / `multipart_copy`, checksum-on-complete, resume / list-parts pagination |
| 19 | Other — `expected_bucket_owner`, account-usage, raw-HTTP negatives, misc PUT/GET/HEAD/DELETE response-shape edges |

The headers in `allowlist.txt` always record the exact suite SHA + endpoint a result was calibrated
against, so the table above can be reproduced verbatim from the checked-in artifact.

### The compatibility badge

`run.sh --calibrate` also writes `badge.json` (a [Shields endpoint](https://shields.io/badges/endpoint-badge)
payload) next to `allowlist.txt`, so the **S3 compatibility** badge in the top-level README — which
points at `badge.json` on `main` — tracks the calibrated `passed / collected` percentage
automatically. Re-calibrate, commit `badge.json` alongside `allowlist.txt`, and the badge follows.

## In CI

[`.github/workflows/s3-compat.yml`](../../.github/workflows/s3-compat.yml) runs this gate on **every
PR and every push to `main`**, with **no path filter** — a change in any module (LSM, BookKeeper
SPI, client, protocol) can break the gateway end-to-end, so the gate always runs. It builds
`candybox:ci` from the checkout (never pulls), starts `docker-compose.ci.yml`, runs the suite, and
posts a sticky pass/fail comment on the PR plus a job summary.

Two behaviours keep it honest:

- **Provisional vs calibrated.** While `allowlist.txt` still carries the `PROVISIONAL SEED` marker,
  the workflow runs `--all` in **report-only** mode (never red) and the comment nudges you to
  calibrate. Once you commit a calibrated allowlist (the marker is gone), it becomes a real **gate**
  that fails the build on any regression.
- **Calibrate on a native runner.** Trigger the workflow manually (Actions → *S3 compatibility* →
  Run workflow → `calibrate = true`) to run `--calibrate` on a native amd64 runner — faster than an
  emulated laptop — then download the `allowlist.txt` artifact and commit it.
