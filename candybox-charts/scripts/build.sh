#!/usr/bin/env bash
set -euo pipefail
version="$1"
build_dir="$2"
helm="$3"
module_dir="$(cd "$(dirname "$0")/.." && pwd)"
# Helm validates the complete SemVer; this also keeps sed/path arguments safe.
[[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+([+-][a-zA-Z0-9.-]+)*$ ]] || {
  echo "Parent version must be a Helm-compatible SemVer: $version" >&2; exit 1;
}
command -v "$helm" >/dev/null || {
  echo 'Helm is required for mvn package. Install Helm 3.17+ or set -Dhelm.executable=/path/to/helm.' >&2
  exit 1
}
chart="$build_dir/candybox-chart"
mkdir -p "$build_dir"
rm -rf "$chart"
cp -R "$module_dir/candybox-chart" "$chart"
cp "$module_dir/README.md" "$chart/README.md"
sed -i "s/@project.version@/$version/g" "$chart/Chart.yaml"
"$helm" dependency build "$chart"
for profile in values.yaml values-demo.yaml values-production.yaml; do
  "$helm" lint --strict "$chart" -f "$chart/$profile"
  "$helm" template validation "$chart" -f "$chart/$profile" >/dev/null
done
"$helm" template external "$chart" --set zookeeper.enabled=false \
  --set externalZookeeper.servers=zk.example:2181 --set bookkeeper.enabled=false \
  --set externalBookkeeper.metadataServiceUri=zk://zk.example:2181/ledgers >/dev/null
"$helm" package "$chart" --destination "$build_dir"
