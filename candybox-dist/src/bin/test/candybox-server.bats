#!/usr/bin/env bats
#
# Tests for the bin/ launch scripts. Run with `bats bin/test` from an unpacked distribution, or
# `bats candybox-dist/src/bin/test` from the source tree. These exercise script behaviour only
# (classpath/JVM-option assembly, config validation) via CANDYBOX_DRY_RUN — no JVM is launched.

setup() {
  BIN_DIR="$(cd "$BATS_TEST_DIRNAME/.." && pwd)"
  SERVER="$BIN_DIR/candybox-server"

  # A throwaway install root with a conf/ and an (empty) lib/.
  CANDYBOX_HOME="$(mktemp -d)"
  export CANDYBOX_HOME
  mkdir -p "$CANDYBOX_HOME/conf" "$CANDYBOX_HOME/lib" "$CANDYBOX_HOME/logs"
  cat > "$CANDYBOX_HOME/conf/candybox.properties" <<'EOF'
node.id=1
zookeeper.connect=127.0.0.1:2181
EOF
  export CANDYBOX_DRY_RUN=1
}

teardown() {
  [[ -n "${CANDYBOX_HOME:-}" && -d "$CANDYBOX_HOME" ]] && rm -rf "$CANDYBOX_HOME"
}

@test "candybox-env.sh exports install paths and a lib/* classpath" {
  source "$BIN_DIR/candybox-env.sh"
  [ "$CANDYBOX_CONF_DIR" = "$CANDYBOX_HOME/conf" ]
  [ "$CANDYBOX_LOG_DIR" = "$CANDYBOX_HOME/logs" ]
  [[ "$CLASSPATH" == *"$CANDYBOX_HOME/lib/*"* ]]
  [[ "$CLASSPATH" == *"$CANDYBOX_HOME/conf"* ]]
}

@test "JVM options include all six BookKeeper/ZooKeeper --add-opens flags" {
  source "$BIN_DIR/candybox-env.sh"
  local joined="${CANDYBOX_JVM_OPTS[*]}"
  [[ "$joined" == *"java.base/java.lang=ALL-UNNAMED"* ]]
  [[ "$joined" == *"java.base/java.io=ALL-UNNAMED"* ]]
  [[ "$joined" == *"java.base/java.nio=ALL-UNNAMED"* ]]
  [[ "$joined" == *"java.base/sun.nio.ch=ALL-UNNAMED"* ]]
  [[ "$joined" == *"java.base/java.util=ALL-UNNAMED"* ]]
  [[ "$joined" == *"java.base/java.util.concurrent=ALL-UNNAMED"* ]]
}

@test "candybox-server resolves the main class, classpath and config path" {
  run "$SERVER"
  [ "$status" -eq 0 ]
  [[ "$output" == *"me.predatorray.candybox.server.CandyboxServer"* ]]
  [[ "$output" == *"$CANDYBOX_HOME/conf/candybox.properties"* ]]
  [[ "$output" == *"--add-opens java.base/sun.nio.ch=ALL-UNNAMED"* ]]
  [[ "$output" == *"$CANDYBOX_HOME/lib/*"* ]]
}

@test "candybox-server honours an explicit config path argument" {
  cp "$CANDYBOX_HOME/conf/candybox.properties" "$CANDYBOX_HOME/custom.properties"
  run "$SERVER" "$CANDYBOX_HOME/custom.properties"
  [ "$status" -eq 0 ]
  [[ "$output" == *"$CANDYBOX_HOME/custom.properties"* ]]
}

@test "candybox-server fails fast when the config file is missing" {
  rm -f "$CANDYBOX_HOME/conf/candybox.properties"
  run "$SERVER"
  [ "$status" -eq 1 ]
  [[ "$output" == *"configuration file not found"* ]]
}

@test "CANDYBOX_EXTRA_OPTS are passed through to the JVM" {
  export CANDYBOX_EXTRA_OPTS="-Xmx2g -XX:+UseZGC"
  run "$SERVER"
  [ "$status" -eq 0 ]
  [[ "$output" == *"-Xmx2g"* ]]
  [[ "$output" == *"-XX:+UseZGC"* ]]
}
