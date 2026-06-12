#!/usr/bin/env bash
#
# Generates a DEV-ONLY private CA and per-service certificates for a local secured Candybox stack:
# PEM cert/key pairs (keys in unencrypted PKCS#8, as candybox requires) with SANs covering
# localhost and the docker-compose service names. Do NOT use these in production — bring real
# certificates (e.g. cert-manager) instead.
#
# Usage: ./gen-dev-certs.sh [output-dir]   (default ./dev-certs)
set -euo pipefail

OUT="${1:-dev-certs}"
mkdir -p "$OUT"
cd "$OUT"

DAYS=3650
SAN="DNS:localhost,IP:127.0.0.1"
for s in candybox-1 candybox-2 candybox-3 s3-gateway admin-api zookeeper bookie-1 bookie-2 bookie-3; do
  SAN="$SAN,DNS:$s"
done

if [[ ! -f ca.pem ]]; then
  openssl req -x509 -newkey rsa:2048 -keyout ca.key -out ca.pem -days "$DAYS" -nodes \
    -subj "/CN=candybox-dev-ca" 2>/dev/null
  echo "generated CA: $OUT/ca.pem"
fi

gen() {
  local name="$1"
  openssl req -newkey rsa:2048 -keyout "$name-pkcs1.key" -out "$name.csr" -nodes \
    -subj "/CN=$name" 2>/dev/null
  openssl x509 -req -in "$name.csr" -CA ca.pem -CAkey ca.key -CAcreateserial \
    -out "$name.pem" -days "$DAYS" -extfile <(printf "subjectAltName=%s" "$SAN") 2>/dev/null
  openssl pkcs8 -topk8 -nocrypt -in "$name-pkcs1.key" -out "$name.key"
  rm -f "$name-pkcs1.key" "$name.csr"
  echo "generated $OUT/$name.pem + $OUT/$name.key"
}

gen node       # shared by the storage nodes (SAN covers all compose hostnames)
gen s3-gateway
gen admin-api
gen client     # for mTLS-enabled deployments / CLI

echo
echo "Done. Point candybox at them, e.g.:"
echo "  tls.enabled=true"
echo "  tls.cert.path=$PWD/node.pem"
echo "  tls.key.path=$PWD/node.key"
echo "  tls.ca.path=$PWD/ca.pem"
