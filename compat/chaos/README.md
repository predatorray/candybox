# Disposable failure harness (initial schedules)

This opt-in harness runs **separate real containers on one Docker host**: a
three-member ZooKeeper ensemble, three BookKeeper 4.17.1 bookies (default 3/3/2
quorums), two Candybox owners and an S3 gateway. It does not target an existing
endpoint. Nothing is added to `mvn test`.

This is incremental work toward #167, **not completion of its acceptance list or
proof of production durability**. There is no stable scheduled CI gate yet.

## Contract and plan

Applicable constitution principles: II (acknowledged durability and fencing), III
(stale reads during handover), V (retained bytes), VI (bounded work). The initial
acceptance condition is that every acknowledged PUT's SHA-256 and user metadata
remain readable after a bounded stabilization period, following each fault.
Unique keys avoid ambiguous overwrite ordering. Ordinary PUT timeouts are unknown
outcomes and are never automatically retried. Any successful PUT during quorum
loss is included in subsequent checks. GET unavailability is tolerated during
recovery; wrong bytes or metadata for these unique keys are violations. Listings
are not checked as global snapshots. Observation times measure the first completed
full verification pass after each fault; they do not measure lease-transfer time
or establish a production recovery SLA.

The harness changes only test tooling and operations documentation. It changes no
formats, owner logic, clocks, tombstones, retained-byte eligibility, authorization,
configuration defaults or runtime compatibility. No production rollback or
migration is needed; deleting these test files removes the harness. The cluster
uses anonymous S3 on a randomly assigned loopback port, synthetic bytes and fresh
volumes. Existing safety graces and replication quorums are not reduced.

Implementation mapping: `compose.yml` provides the isolated topology and explicit
versions; `run.py` records history outside containers, injects faults, checks
acknowledged data and tears down; `test_harness.py` checks the oracle and destructive
target guards without services. These tests do not prove the Docker experiments.

## Run

Requires Python 3.9+, Java 17+, Maven 3.9+, Docker Engine and Compose v2. Budget at
least 6 GiB of available Docker memory. From the repository root:

```sh
mvn -DskipTests -Dcandybox.charts.skip=true clean package
docker build -f compat/chaos/Dockerfile -t candybox:chaos \
  --build-arg SOURCE_REVISION="$(git rev-parse HEAD)" \
  --build-arg DISTRIBUTION_SHA256="$(sha256sum candybox-dist/target/candybox-*.tar.gz | cut -d ' ' -f 1)" .
CANDYBOX_CHAOS_DISPOSABLE=yes python3 compat/chaos/run.py \
  --namespace chaos-local --image candybox:chaos --seed 167 \
  --output /tmp/candybox-chaos
```

Candybox is resolved to an existing local immutable image ID before startup (no
pull of a `latest` Candybox binary). The runtime image is version-pinned and backend
images are version-pinned in Compose; `containers.json` records the actual image
IDs; `image.json` records the image labels with source revision and verified
distribution SHA-256. The driver also records its own revision and dirty state.
Use a clean source checkout and the clean build above: a label alone cannot prove
which sources produced an archive. The rendered `compose.yml` records configuration. Archive those artifacts
with the distribution/source revision to reproduce a run; tags alone are not
content-addressed release evidence. Builds use the local source, not a published
release. Do not describe two such builds as a mixed-version test.

The explicit environment marker and `chaos-...` namespace are required. Every run
appends a random suffix, refuses an existing project, and checks both Compose
project and disposable labels immediately before faulting containers. Controls act
on those exact container IDs; restoration does not rerun the one-shot BookKeeper
metadata initializer or start unrelated dependencies. Run against
a dedicated development Docker daemon: no production endpoint argument exists.

The fixed schedules are:

1. Create a bucket and PUT 16 seeded objects, recording bytes' SHA-256 and metadata.
2. SIGKILL each owner separately; verify all acknowledged objects while it remains
   down, restart it, and verify again.
3. Pause owner 1 for 15 seconds (lease TTL is explicitly 10 seconds), verify through
   the survivor, acknowledge a new object, resume the owner and recheck everything.
4. Stop two bookies, attempt one PUT, restore them and verify every acknowledged
   object. A lost/failed response is not treated as an acknowledged write.

The pause test observes externally visible integrity after owner resumption. It
does not deterministically hold a write inside a stale owner and prove its fencing
rejection. The bookie test removes write quorum; it does not partition individual
network paths or prove all network fault modes.

Each subprocess has a timeout. Bucket creation gets one bounded 120-second request
(`--setup-seconds`); the gateway node read budget is 120 seconds to accommodate
creating all eight partitions on a cold backend. Bucket creation is never retried,
and HTTP 409 is a setup failure. Ordinary HTTP requests have a 15-second timeout; object
count is capped at 100. Stabilization checks have a configured deadline (default
90 seconds), with at most one request-timeout overrun. SIGINT/SIGTERM unwind through
restoration and teardown. SIGKILL of the harness or daemon failure cannot execute
cleanup: use the archived exact project/config to unpause and `down -v` manually.
Cleanup removes project containers, networks and anonymous volumes. Repeated
`docker compose -p EXACT_PROJECT -f ARCHIVED_COMPOSE down -v` is safe; never
substitute another project's name.

## Evidence and exit codes

The unique output directory contains `history.jsonl` (fsynced invocations,
responses, operation IDs, key/version/hash and fault timestamps), `report.json`,
rendered `compose.yml`, container inspection with exact image IDs, and timestamped
logs from all components. This synthetic history has no credentials or user data.
`report.json` always lists unimplemented acceptance scenarios.

- `0`: only the initial schedules passed, cleanup completed.
- `1`: wrong acknowledged content/metadata, or acknowledged data failed to become
  readable within the recovery bound. A deadline failure needs operator analysis
  of logs to distinguish a product outage from host starvation.
- `2`: setup, control, interruption or cleanup failed; inspect the evidence before
  attributing it to storage correctness.

Run service-free harness checks with:

```sh
python3 -m unittest discover -s compat/chaos -p 'test_*.py'
```

## Remaining #167 gates

No passing initial report closes #167. The following are deliberately unimplemented:
ownership-aware stale-writer rejection, selected-path delay/drop, a reply lost after a verified commit plus explicit-token
retry, deterministic rename boundary crashes, a delayed zero-copy consumer while
source deletion/compaction/destructive GC runs, metrics snapshots, and real
released-artifact mixed-version activation. #157 still owns the absent runtime
reader/activation gate, so a supported rolling-upgrade scenario cannot yet be
written honestly. These require additional schedules and actual measured runs;
none can be replaced with a passing smoke check or embedded test. Add a manually
triggered/scheduled CI job only once the real Docker schedules are reliable.
