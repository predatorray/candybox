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

The v1 gateway is **anonymous** (the `Authorization` header is accepted and ignored) and
**path-style only**, and it implements a deliberately small slice of S3:

| Supported (allowlist candidates)        | Not supported in v1 (excluded)                          |
|------------------------------------------|---------------------------------------------------------|
| Bucket create / head / delete            | Versioning                                              |
| ListObjectsV2: prefix, delimiter,        | Multipart upload                                        |
| max-keys, continuation-token, start-after| ACLs / bucket policy / ownership                        |
| Object PUT / GET / HEAD / DELETE         | Range GET and conditional GET (If-Match/If-None-Match)  |
| Multi-object delete                      | Tagging, lifecycle, CORS, SSE                           |
| `x-amz-meta-*` user metadata, CRC32C ETag| Per-user auth / multi-account isolation                 |

Because auth is ignored, the suite's `[s3 alt]` / `[s3 tenant]` accounts all collapse onto one
anonymous namespace — multi-user tests can't pass and aren't in the allowlist.

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

Last calibrated against ceph/s3-tests `master` @ `5522d1c` on **2026-05-31** (full suite path
`s3tests/functional/test_s3.py`, 7m 52s wall on an Apple-Silicon laptop). The result is
reproducible — re-running `--calibrate` against the same source produces a byte-identical
`allowlist.txt` apart from the timestamp header.

| Outcome | Count | Notes |
|---|---:|---|
| **Passed** (in `allowlist.txt`) | **149** | The compatibility gate. |
| Failed | 595 | Features the v1 gateway doesn't yet implement (breakdown below). |
| Skipped | 94 | Suite-level `pytest.mark` opt-outs — never executed (bucket logging, cloud-tier lifecycle, restore-status). |
| Collected | 838 | All of `s3tests/functional/test_s3.py`. |

The 595 failures are the **growth surface** — implementing any of these would expand the allowlist
on the next `--calibrate`:

| Failures | Feature family |
|---:|---|
| 106 | Server-side encryption — SSE-C / SSE-S3 / SSE-KMS (incl. the 64-test `copy_enc[…]` matrix) |
| 51 | Multipart upload + `copy_part` / `multipart_copy` |
| 40 | ACLs / ownership controls (`object_acl`, `bucket_acl`, `access_bucket_*`) |
| 38 | Versioning (`versioning_*`, `delete_marker_*`, `*_versioned`) |
| 36 | Object Lock / retention / legal hold (beyond the 3 that already pass) |
| 36 | POST object (browser-style form uploads) |
| 33 | Lifecycle / expiration / non-current-version rules |
| 29 | Bucket policy / block-public-access |
| 29 | Bucket logging (the bits not behind the `pytest.mark` skip) |
| 25 | `ListObjects(V2)` edge cases — encoding-type, exotic keys, sort order |
| 13 | CORS preflight + actual cross-origin |
| 13 | Anonymous raw-HTTP negative tests |
| 11 | Object copy edge (metadata-directive, content-type override, ACL on copy) |
| 10 | Range GET + `If-Match` / `If-Modified-Since` matrix |
| 9 | Bucket create / naming negative + target-bucket setup |
| 8 | Tagging beyond the basic put/get already in the allowlist |
| 67 | Other — object attributes (checksum SHA-256, CRC64NVMe), public-access-block, `expected_bucket_owner`, bucket-recreate ACL, account-usage, misc PUT/GET/HEAD/DELETE response-shape edges |

The headers in `allowlist.txt` always record the exact suite SHA + endpoint a result was calibrated
against, so the table above can be reproduced verbatim from the checked-in artifact.

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
