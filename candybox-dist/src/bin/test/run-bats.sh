#!/usr/bin/env bash
#
# Runs the bin/ script tests with bats-core if it is installed, otherwise skips cleanly. Invoked by
# the Maven build (test phase of candybox-dist) and usable directly: `bash bin/test/run-bats.sh`.
#
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if command -v bats >/dev/null 2>&1; then
  echo "Running Candybox bin/ script tests with bats in $DIR"
  exec bats "$DIR"
else
  echo "bats not found on PATH; skipping bin/ script tests. Install bats-core to run them:"
  echo "  https://github.com/bats-core/bats-core"
fi
