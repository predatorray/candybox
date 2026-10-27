# Candybox Helm chart

`candybox-charts` is a normal Maven reactor module. Its chart deploys Candybox storage
nodes, the S3 gateway and the admin/dashboard API. It optionally deploys ZooKeeper
and BookKeeper, or connects to existing clusters.

## Build and version

Target Kubernetes 1.32+ (the ZooKeeper dependency uses StatefulSet pod-index labels).
Install Java 17+, Maven 3.9+, Bash and Helm 3.17+ (CI uses 3.17.3), then:

```bash
mvn package                        # includes the chart in the parent lifecycle
mvn -pl candybox-charts verify      # build only the chart
```

The module stages the chart under `target/candybox-chart`, substitutes the parent
POM version into `Chart.yaml`, resolves dependencies from `Chart.lock`, lints and
renders all profiles, then writes `target/candybox-chart-<version>.tgz`. It attaches
that package with Maven classifier `helm`. Generated files never change the source.
Build from Maven rather than installing the source directory containing version
placeholders. Use `-Dhelm.executable=/path/to/helm` for a custom binary.

Both chart `version` and `appVersion` equal the parent POM version:

| Parent POM | Chart package |
| --- | --- |
| `0.2.0-SNAPSHOT` | `candybox-chart-0.2.0-SNAPSHOT.tgz` |
| `0.2.0-rc.1` | `candybox-chart-0.2.0-rc.1.tgz` |
| `0.2.0` | `candybox-chart-0.2.0.tgz` |

Snapshots are local/CI artifacts and are not published. They need an existing image
via `--set image.tag=sha-<commit>` or `image.digest`; there is no automatically
published `0.2.0-SNAPSHOT` container image. Release charts default to `appVersion`
as the image tag. A nonempty `image.digest` takes precedence over the tag.

The application Dockerfile explicitly uses `-Dcandybox.charts.skip=true`: building
an application container does not need to package its deployment chart. That flag
also skips attaching the chart artifact. Ordinary Maven package/install/verify
builds include it. Charts are excluded from Maven Central and attached to GitHub
Releases instead.

## Install a personal demo

After a chart-bearing release has been published:

```bash
helm install candybox oci://registry-1.docker.io/zetaplusae/candybox-chart \
  --version <release-version> --namespace candybox --create-namespace \
  -f candybox-charts/candybox-chart/values-demo.yaml --wait --timeout 10m
```

For a local snapshot package, substitute
`candybox-charts/target/candybox-chart-0.2.0-SNAPSHOT.tgz` for the OCI reference,
omit `--version`, and set `--set image.tag=<available-image-tag>`.
The example values are also inside the package (`helm pull ... --untar`).

The demo runs one ZooKeeper, one bookie and one Candybox node with `1/1/1` quorums,
plus the gateway and dashboard. It uses `emptyDir` storage: restarting a bookie or
ZooKeeper pod can lose data. It has no authentication or redundancy. Allow roughly
3 GiB of available memory plus Kubernetes overhead. Candybox and BookKeeper images
currently require amd64 workers; the defaults select that architecture.

```bash
kubectl -n candybox port-forward svc/candybox-candybox-chart-gateway 9711:9711
# In another terminal:
kubectl -n candybox port-forward svc/candybox-candybox-chart-admin 9713:9713
# Dashboard: http://localhost:9713/ui/
helm test candybox -n candybox --logs --timeout 5m
```

The test creates a bucket, writes/reads/deletes an object, then deletes the bucket.
Resource names can be shortened with `--set fullnameOverride=candybox` (which does
not rename the upstream ZooKeeper dependency).

## Production configuration

`values.yaml` is a persistent, replicated baseline with cluster-internal Services.
`values-production.yaml` additionally enables application/S3 authentication,
requires operator-created Secrets, spreads workloads across workers, and uses four
bookies so a failed bookie can be replaced in a three-member ensemble. It is a
starting point, not a guarantee of production readiness.

Prepare at least three amd64 workers, a default StorageClass (or configure
`bookkeeper.persistence.storageClass` and `zookeeper.persistence.storageClass`),
and capacity for the requested journal/ledger/ZooKeeper PVCs. Choose worker and
zone placement appropriate for your failure domains. The example scheduling
selectors assume the default `nameOverride`; adjust them if changing it or sharing
a namespace with other releases.

The production example references these pre-existing Secrets in the release namespace:

| Secret | Contents |
| --- | --- |
| `candybox-credentials` | `credentials.properties` containing SASL users/principals and S3 access keys; mounted at `/etc/candybox-security` |
| `candybox-node-env` | Node-specific `CANDYBOX_*` settings, such as `CANDYBOX_LEDGER_PASSWORD` |
| `candybox-gateway-env` | `CANDYBOX_AUTH_CLIENT_MECHANISM`, `CANDYBOX_AUTH_CLIENT_USERNAME`, `CANDYBOX_AUTH_CLIENT_PASSWORD` for the `s3-gw` service user |
| `candybox-admin-env` | The same client authentication variables for `admin-api`, plus `CANDYBOX_ADMIN_AUTH_TOKEN` |

Follow [OPERATIONS.md](../OPERATIONS.md#security) for the exact credentials format,
SASL, ACL, ZooKeeper auth and BookKeeper TLS settings. Do not put passwords in
values files or ConfigMaps. Supply Secrets through `commonEnvFrom`, component
`envFrom`, or `env` entries with `valueFrom.secretKeyRef`. Add certificate/JAAS
Secret volumes using `volumes`/`volumeMounts` (Candybox) and the corresponding
BookKeeper or upstream ZooKeeper values. `node.config`, `gateway.config` and
`bookkeeper.config` expose non-secret configuration. Ports, discovery and quorums
have dedicated settings and cannot be overridden through those config maps.

Configure transport TLS and ZooKeeper/BookKeeper authentication for your network
and certificates before exposing a production installation. The bundled backends
are not secured merely by selecting the production values file. Restrict backend
access with your cluster's network policy; do not expose bookie or ZooKeeper ports.
S3 HTTPS can terminate at an ingress: configure `gateway.ingress` with its host,
class and TLS Secret, and any controller-specific upload size/time limits. The
admin service stays ClusterIP by default; use a private authenticated proxy for
remote access. If enabling TLS on the admin listener itself, also set
`admin.probeScheme=HTTPS`.

```bash
helm install candybox oci://registry-1.docker.io/zetaplusae/candybox-chart \
  --version <release-version> --namespace candybox --create-namespace \
  -f candybox-charts/candybox-chart/values-production.yaml \
  -f my-cluster-values.yaml --wait --timeout 15m
```

For `helm test` against authenticated nodes, set `test.envFrom` to a Secret with
CLI variables `CANDYBOX_AUTH_USERNAME`, `CANDYBOX_AUTH_PASSWORD`, optionally
`CANDYBOX_AUTH_MECHANISM`, `CANDYBOX_TLS=true`, and `CANDYBOX_TLS_CA` (mounted CA
path). CLI names differ from server/client configuration names. Use a principal
permitted to create buckets and read/write/delete their objects.

## Storage and dependencies

ZooKeeper is the pinned [CloudPirates chart](https://github.com/CloudPirates-io/helm-charts/tree/main/charts/zookeeper)
`0.15.3`, using the official ZooKeeper image; its common library is bundled too.
The package contains dependencies, so installation does not download charts from
upstream (container images are still pulled). Override upstream settings under
`zookeeper`, including resources, persistence, scheduling and authentication.

The BookKeeper charts evaluated in Apache Pulsar's repository are integrated with
Pulsar's image, helpers and metadata initialization. To avoid installing that stack,
this chart manages a small BookKeeper StatefulSet using `apache/bookkeeper:4.17.1`.
It provides separate journal/ledger PVCs, stable advertised DNS names, probes,
idempotent first-start metadata initialization and an AutoRecovery process.
Initialization checks the existing instance ID and uses `initnewcluster` only;
it never formats or resets an existing cluster. All bookies start in parallel.
Candybox node identities come from StatefulSet ordinals. Its persistent application
data lives in BookKeeper; node logs and temporary files use disposable volumes.

To use operator-managed storage instead:

```yaml
zookeeper:
  enabled: false
externalZookeeper:
  servers: zk-0.example:2181,zk-1.example:2181,zk-2.example:2181
bookkeeper:
  enabled: false
externalBookkeeper:
  metadataServiceUri: zk://bk-zk.example:2181/ledgers
```

The external BookKeeper cluster must already be initialized, its bookie addresses
must be reachable from Candybox, and it must have enough writable bookies for all
four `node.quorums` settings. The chart rejects invalid quorum ordering and
ensembles larger than its own managed bookie count. Existing ledgers retain their
original quorums; reducing replicas does not rewrite or migrate data.

PVCs created by StatefulSet claim templates survive Helm uninstall by Kubernetes
default. Back up ZooKeeper metadata and establish a BookKeeper recovery procedure
before upgrades. Never delete PVCs or change metadata paths/advertised bookie names
as an upgrade shortcut. Avoid switching from the ephemeral demo to production in
place; install a new cluster and migrate data. Storage resizing and changes to
immutable StatefulSet fields may need an operator-managed migration.

To update the dependency intentionally, stage the chart with Maven, edit its
source `Chart.yaml` dependency version, stage the changed metadata in `target`,
run `helm dependency update candybox-charts/target/candybox-chart`, and copy the
resulting `Chart.lock` back to the source. Review the upstream changes and rerun
installation/upgrade checks. Normal builds use `dependency build`, not update.

## CI and publishing

`helm.yml` packages through Maven, checks configuration contracts, builds the
matching application image, and installs/upgrades/tests the demo in Kind.
`docker-publish.yml` publishes a release chart only after pushing the matching
application image, using the existing `DOCKERHUB_USERNAME`/`DOCKERHUB_TOKEN` secrets.
The release workflow already dispatches that publisher explicitly because its
`GITHUB_TOKEN`-created tags do not trigger push workflows. Manual backfills use
that same workflow with an existing release tag; tags predating this module skip
the chart step. Existing chart versions are retained on reruns.

The destination is `oci://registry-1.docker.io/<DockerHub-user>/candybox-chart`,
separate from the `candybox` image repository to prevent identical version tags
from overwriting each other. Ensure that repository is public and the existing
token has write permission. Keep `image.repository` aligned with your image
publisher if forking the project. Neither PRs nor main snapshots publish charts.
