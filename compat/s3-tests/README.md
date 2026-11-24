# S3 compatibility tests (ceph/s3-tests)

This harness runs [`ceph/s3-tests`](https://github.com/ceph/s3-tests) against the
Candybox S3 gateway (`candybox-s3-gateway`). The checked-in allowlist defines a
regression-tested subset of S3 behavior, not full AWS S3 compatibility.

The suite is **not** vendored. `run.sh` clones it on demand, checks out
`S3TESTS_REF`, and creates a reusable virtualenv under `.work/` (git-ignored).
Rendered configuration and calibration output also live there. Calibration
rewrites the tracked `allowlist.txt` and `badge.json` beside this README.

## Supported scope

The [CI Compose stack](docker-compose.ci.yml) enables **SigV4 authentication and
S3 ACL enforcement**, using the accounts in
[gateway-credentials.properties](gateway-credentials.properties) and
[s3tests.conf.in](s3tests.conf.in). Tests include signed and anonymous requests;
ACLs determine which anonymous and cross-account operations are allowed.
The gateway supports path-style addressing only.

| Implemented scope (individual edge cases may still fail) | Unsupported scope |
|---|---|
| Bucket create / head / delete, ownership-scoped ListBuckets | Object versioning |
| ListObjects v1/v2 with prefix, delimiter, pagination and start-after (v2) | Bucket policy / public-access block |
| Object PUT / GET / HEAD / DELETE, multi-object delete | Conditional GET (`If-Match`, `If-None-Match`, `If-*-Since`) |
| Single-range GET, including open-ended and suffix ranges | Lifecycle, CORS, server-side encryption |
| Multipart create / upload part / complete / abort / list uploads / list parts | POST object (browser form upload) |
| Same-bucket CopyObject and UploadPartCopy (including source ranges) | Cross-bucket CopyObject / UploadPartCopy |
| `x-amz-meta-*` user metadata, CRC32C-based ETags | Full object attributes / checksum APIs, MD5-based ETags |
| SigV4 header/presigned authentication, canned/grant ACLs and cross-account access | Object Lock / retention / legal hold, inventory / replication |

Same-bucket UploadPartCopy buffers and rewrites bytes; it does not have the
zero-copy guarantee of CopyObject. The gateway also verifies CRC32/CRC32C
trailers for supported AWS chunked payloads; this is not full S3 checksum API
support. Some unsupported subresources have compatibility stubs, so an isolated
passing test (for example, tagging or Object Lock) does not establish feature support.

**ListBuckets is ownership-scoped when ACL enforcement is enabled:** it returns
only buckets the caller owns, not buckets merely granted READ.

## Running

Use a disposable cluster: the suite creates and deletes buckets and objects.
The runner host needs `python3` with `venv`, `git`, and network access to fetch
the suite and install its requirements. CI uses Python 3.12. The local example
also needs Docker Compose and the AWS CLI for the signed readiness probe.

| Stack | Brings up | Use |
|---|---|---|
| [`docker-compose.yml`](../../docker-compose.yml) | ZooKeeper, 3 bookies, 3 nodes, gateway and admin API/dashboard from `zetaplusae/candybox:latest` | Demo stack; S3 auth is disabled by default, so it does not reproduce the authenticated gate |
| [`docker-compose.ci.yml`](docker-compose.ci.yml) | ZooKeeper, 3 bookies, 1 node and an authenticated gateway built from local source | Compatibility gate and recommended local setup |

Both expose S3 on port 9711, so run only one at a time. The image publishing
workflow now builds both `linux/amd64` and `linux/arm64`; published Candybox images
are no longer described as amd64-only. The CI stack builds Candybox for the local
architecture; upstream images may still need emulation on an arm64 host.

Run these commands from the repository root in Bash:

```bash
# Build from the checkout and start the stack. Avoid --wait: bk-init is a one-shot job.
docker compose -f compat/s3-tests/docker-compose.ci.yml build
docker compose -f compat/s3-tests/docker-compose.ci.yml up -d

# Probe the full gateway -> node -> bookies path using the configured test account.
# An unsigned bucket PUT returns 403 with auth enabled; waiting for 200 would never finish.
(
  export AWS_ACCESS_KEY_ID=candybox AWS_SECRET_ACCESS_KEY=candybox
  export AWS_DEFAULT_REGION=us-east-1 AWS_PAGER=""
  unset AWS_SESSION_TOKEN AWS_SECURITY_TOKEN
  bucket="compat-preflight-$(date +%s)-$$"
  for attempt in {1..60}; do
    if aws --endpoint-url http://127.0.0.1:9711 s3api create-bucket \
        --bucket "$bucket" >/dev/null 2>&1; then
      aws --endpoint-url http://127.0.0.1:9711 s3api delete-bucket --bucket "$bucket"
      exit $?
    fi
    sleep 5
  done
  echo "Gateway did not become ready; inspect docker compose logs." >&2
  exit 1
) && compat/s3-tests/run.sh

# Tear down (-v also removes any attached non-external volumes).
docker compose -f compat/s3-tests/docker-compose.ci.yml down -v
```

The checked-in allowlist is already calibrated: a normal first run should run
the gate, not overwrite it. To investigate the full suite or deliberately
refresh the contract, use the modes below.

### Modes

| Command | Behavior |
|---|---|
| `compat/s3-tests/run.sh` | Runs `allowlist.txt`; a pytest failure produces a non-zero exit. |
| `compat/s3-tests/run.sh --all` | Runs `TEST_PATH` in report-only mode; pytest failures do not make the runner fail. |
| `compat/s3-tests/run.sh --calibrate` | Runs `TEST_PATH`, records passing node IDs in `allowlist.txt`, and writes `badge.json`; pytest failures are tolerated. |
| `compat/s3-tests/run.sh -k test_bucket_list_empty` | Filters the allowlist with pytest `-k`. |
| `compat/s3-tests/run.sh --all -k test_bucket_list_empty` | Filters the full suite, including tests outside the allowlist. |

### Environment overrides

| Variable | Default | Purpose |
|---|---|---|
| `S3_ENDPOINT` | `http://127.0.0.1:9711` | Use `http://<IPv4>:<port>` for path-style requests. `localhost` is rewritten to `127.0.0.1`. The template sets `is_secure = False`; an HTTPS URL does not enable TLS. |
| `S3TESTS_REF` | `master` | Suite ref to check out. Set an explicit commit for a repeatable suite revision; the runner does not read the pin from the allowlist. |
| `S3TESTS_REPO` | `https://github.com/ceph/s3-tests.git` | Clone URL used when `.work/s3-tests` does not exist. |
| `TEST_PATH` | `s3tests/functional/test_s3.py` | Suite path for `--all` / `--calibrate`; the gate uses node IDs from the allowlist. |
| `PYTEST_ARGS` | _(empty)_ | Extra whitespace-separated pytest arguments. |

The virtualenv is reused across runs. If a suite revision changes dependencies,
remove `.work/venv` to force dependency installation on the next run.

## The allowlist as a contract

[allowlist.txt](allowlist.txt) is the authoritative expected pass set. It is
**calibrated**, not a provisional seed. Do not shrink it to hide regressions.
Before accepting a calibration, inspect `.work/calibrate.out` for collection,
fixture and teardown errors, and review every removed test against the previous
allowlist. Do not use `-k` or other test-selection filters when producing a full
calibration: the runner would replace the contract with only the selected passes.

To use the suite revision recorded by the current artifact:

```bash
S3TESTS_REF=5522d1c compat/s3-tests/run.sh
# Deliberate recalibration after reviewing code/suite changes:
S3TESTS_REF=5522d1c compat/s3-tests/run.sh --calibrate
git diff -- compat/s3-tests/allowlist.txt compat/s3-tests/badge.json
```

### Recorded calibration

The checked-in header records **2026-06-17T05:30:57Z**, suite ref
`master (5522d1c)`, and endpoint `http://127.0.0.1:9711`.
There are **192** test node IDs in `allowlist.txt`; [badge.json](badge.json)
records **22% (192/838)** (integer percentage rounded down).

These are historical artifacts, not a fresh measurement of the current gateway
or upstream `master`. The allowlist does not retain failed/skipped test results,
fixture errors, or the gateway source revision/topology. Those details, and a
current failure-family breakdown, require the corresponding run log or a new
full calibration. Later implementation work, including same-bucket part-copy,
does not automatically expand the checked-in pass set.

### The compatibility badge

`--calibrate` writes a [Shields endpoint](https://shields.io/badges/endpoint-badge)
payload to `badge.json`. The top-level README badge reads it from `main`.
Review and commit **both** `allowlist.txt` and `badge.json` after a local calibration.
The badge is a recorded full-suite pass ratio, not the status of the latest gate.

## In CI

[`.github/workflows/s3-compat.yml`](../../.github/workflows/s3-compat.yml) runs on
pushes and PRs targeting **`main` or `release/**`**, without a path filter. It
builds `candybox:ci` from the checkout, starts the CI stack, performs a signed
readiness probe, runs the suite, and writes a job summary and sticky PR comment.
It currently uses the runner's default `S3TESTS_REF=master`, not the historical
SHA in the allowlist header.

- The current calibrated allowlist makes this a regression gate. The workflow
  retains a report-only fallback if an allowlist contains `PROVISIONAL SEED`.
- For manual calibration, select **Actions → S3 compatibility → Run workflow →
  calibrate = true**. This mode records results without gating on pytest failures.
  Review the run log and download the `allowlist.txt` artifact before committing.
  The workflow currently uploads **only the allowlist**, not `badge.json`; obtain
  the matching badge from a local calibration or reconstruct its payload from
  that same run's passed/collected counts using the calculation in `run.sh`.
