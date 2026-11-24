# Measured initial schedules — 2026-11-24

Two fresh-project runs passed the initial schedules, with **17 acknowledged objects
per run**, all SHA-256 and metadata checks correct, and no cleanup errors.
This is single-host synthetic evidence, not production durability proof or completion
of #167. The driver did not retry ambiguous mutations. The pause schedule does not
prove deterministic stale-writer rejection.

## Provenance

- Main ancestor: `852a076a54c6cbadab8178bd22f063fc599bf15b`.
- Runtime binary source: `e7799dc1042680d2696b792560df48957ebe426d`, built with
  Temurin 17.0.17+10 and Maven 3.9.9 using a private artifact cache and clean package.
- Distribution SHA-256: `39cddd601970a419946f485d77b046eff1de411c1f9d6f08d1764725f289faf2`.
- Runtime image: `sha256:65b162ae164caf77cbc59668352a768c04aef90890e579a0ae5e458804e4d929`.
- Both successful runs used clean driver revision `15bab2344c9ea719a56053342907af2a03eda8f0`.
  Changes between the binary and driver revisions touch only harness tooling.
  The commit adding this evidence changes reports/documentation only; the runtime
  was not built from that later evidence commit.
- Linux x86_64, Docker Engine 28.4.0, Compose 2.40.3; Docker memory 10,452,459,520 bytes.
  Backend images and exact container/image IDs are in the archived inspections.

Setup creates the empty bucket once with balancing disabled, replaces only the two
owners with balancing at 1000ms, restarts the gateway and refreshes its random
loopback port, then waits for read-only empty-key readiness. Both transition records
show **zero acknowledged objects**. All baseline writes and measured fault schedules
follow that transition, with 10s ownership TTL, 2s renewal and normal 3/3/2 replication.
No safety grace or quorum was reduced.

## Measured observations

Seconds from the start of each stabilization check through its first complete
successful GET/hash/metadata pass. These are not lease-transfer durations or SLAs.
The pause check starts **after** the explicit 15-second hold; fault invocation and
response times are preserved separately in history.

| Full verification stage | Seed 167 (seconds) | Seed 168 (seconds) |
| --- | ---: | ---: |
| baseline | 0.581 | 0.537 |
| kill-owner1 | 17.473 | 17.340 |
| restart-owner1 | 1.514 | 0.404 |
| kill-owner2 | 18.271 | 17.970 |
| restart-owner2 | 0.491 | 0.615 |
| owner1-paused-past-ttl | 2.895 | 3.257 |
| owner1-resumed | 0.399 | 0.286 |
| bookkeeper-quorum-restored | 9.163 | 9.603 |

The bookie-quorum-loss PUT returned HTTP500 in both runs, was not retried, and did
not enter acknowledged obligations. Both runs acknowledged the survivor PUT during
the pause, then verified all 17 objects after owner resumption and quorum restoration.

## Attempts and cleanup

There were **six fresh-project experiments** in this resume, not six retries of a
mutation or one cluster. Four failed setup/control before the final two passed:

1. `chaos-issue167-current-e14e78ae0a4b`: 16 baseline ACKs and a completed first
   owner-kill check, then Compose restoration reran the one-shot metadata initializer.
   Fixed the harness to control only exact validated container IDs.
2. `chaos-issue167-restored-25d18691cf17`: bucket creation HTTP500, zero data ACKs.
   Concurrent creation/balancing remains [#226](https://github.com/predatorray/candybox/issues/226).
3. `chaos-issue167-bootstrap-66a50e0ec41f`: bounded read-only readiness HTTP503,
   zero data ACKs after owner IPs swapped. Existing gateway pools/routing remain
   [#227](https://github.com/predatorray/candybox/issues/227).
4. `chaos-issue167-final-bed55e9cff4c`: zero data ACKs; gateway restart reallocated
   its ephemeral host port, making the saved binding obsolete. Fixed binding refresh.
5. `chaos-issue167-verified-0157a72a0738`: complete initial pass, seed167, 17 ACKs.
6. `chaos-issue167-repeat-69a441f7a47d`: complete initial pass, seed168, 17 ACKs.

Every report has `cleanup_errors: []`. Additional `cleanup-proof.json` files compare
exact recorded IDs/names with `docker ps -aq --no-trunc`,
`docker network ls -q --no-trunc`, and `docker volume ls -q`: **no recorded containers,
networks or volumes remain for any of the six projects**. Setup-only owner recreation
and gateway reset are disclosed above; measured kill/start schedules retain IDs/IPs.
Older November16 failed setups are documented in the PR timeline; their expired
ephemeral artifacts are not included or presented as current evidence.

## Read and reproduce

[summary.json](summary.json) contains readable per-run reports, exact driver/image
provenance, mutation results and bootstrap/binding events. [runs.tar.gz](runs.tar.gz)
contains complete fsynced histories, both rendered configurations and inspection
generations, logs, reports, cleanup proofs, build provenance and diagnostics for all
six runs. [manifest.json](manifest.json) hashes every archived file;
[SHA256SUMS](SHA256SUMS) hashes the archive itself. Inspect/extract into a fresh
directory; do not run archived Compose configuration against another project.

```sh
cd compat/chaos/evidence/2026-11-24
sha256sum -c SHA256SUMS
tar -tzf runs.tar.gz
```

To reproduce a fresh run, use the clean build/image command and guarded invocation
in [the harness README](../../README.md), selecting seed167 or168. Exact hashes
identify the tested artifacts; a new source build need not be byte-identical because
build timestamps are not normalized. Tags are not release-artifact evidence.

Remaining #167 gates stay open: ownership-aware stale-writer rejection, selected-path
network faults, delayed zero-copy consumption with destructive GC, rename crash
boundaries, committed lost-response explicit-token replay, metrics, release-artifact
mixed-version activation (requires #157), and a stable opt-in CI job. Neither these
two passes nor bootstrap isolation claims to satisfy those gates.
