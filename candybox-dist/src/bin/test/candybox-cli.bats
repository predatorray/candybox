#!/usr/bin/env bats
#
# Behaviour tests for the bin/candybox client launcher (via CANDYBOX_DRY_RUN; no JVM is launched).

setup() {
  BIN_DIR="$(cd "$BATS_TEST_DIRNAME/.." && pwd)"
  CLI="$BIN_DIR/candybox"

  CANDYBOX_HOME="$(mktemp -d)"
  export CANDYBOX_HOME
  mkdir -p "$CANDYBOX_HOME/conf" "$CANDYBOX_HOME/lib"
  export CANDYBOX_DRY_RUN=1
}

teardown() {
  [[ -n "${CANDYBOX_HOME:-}" && -d "$CANDYBOX_HOME" ]] && rm -rf "$CANDYBOX_HOME"
}

@test "candybox resolves the CLI main class, classpath and forwards arguments" {
  run "$CLI" -s 127.0.0.1:9709 create-box photos
  [ "$status" -eq 0 ]
  [[ "$output" == *"me.predatorray.candybox.client.CandyboxCli"* ]]
  [[ "$output" == *"$CANDYBOX_HOME/lib/*"* ]]
  [[ "$output" == *"create-box photos"* ]]
  [[ "$output" == *"-s 127.0.0.1:9709"* ]]
}

@test "candybox carries the --add-opens JVM options" {
  run "$CLI" list-boxes
  [ "$status" -eq 0 ]
  [[ "$output" == *"--add-opens java.base/java.nio=ALL-UNNAMED"* ]]
}
