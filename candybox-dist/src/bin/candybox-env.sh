#!/usr/bin/env bash
#
# Sourced by the Candybox launch scripts. Resolves install paths, the runtime classpath, and the
# JVM options (including the --add-opens flags BookKeeper/ZooKeeper require on Java 17+). Override
# any of the CANDYBOX_* variables in the environment before launching.
#

# CANDYBOX_HOME is the install root (the parent of this bin/ directory).
if [[ -z "${CANDYBOX_HOME:-}" ]]; then
  CANDYBOX_HOME="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
fi
export CANDYBOX_HOME
export CANDYBOX_CONF_DIR="${CANDYBOX_CONF_DIR:-$CANDYBOX_HOME/conf}"
export CANDYBOX_LOG_DIR="${CANDYBOX_LOG_DIR:-$CANDYBOX_HOME/logs}"

# Java launcher: prefer $JAVA_HOME, fall back to PATH.
if [[ -n "${JAVA_HOME:-}" ]]; then
  JAVA="$JAVA_HOME/bin/java"
else
  JAVA="java"
fi
export JAVA

# Runtime classpath: every jar in lib/, plus conf/ so logback.xml is found without an explicit flag.
export CLASSPATH="$CANDYBOX_CONF_DIR:$CANDYBOX_HOME/lib/*"

# --add-opens flags for BookKeeper/ZooKeeper reflective access to JDK internals (mirror the IT pom).
CANDYBOX_ADD_OPENS=(
  --add-opens java.base/java.lang=ALL-UNNAMED
  --add-opens java.base/java.io=ALL-UNNAMED
  --add-opens java.base/java.nio=ALL-UNNAMED
  --add-opens java.base/sun.nio.ch=ALL-UNNAMED
  --add-opens java.base/java.util=ALL-UNNAMED
  --add-opens java.base/java.util.concurrent=ALL-UNNAMED
)

# Assembled JVM options. CANDYBOX_HEAP_OPTS / CANDYBOX_EXTRA_OPTS let operators add heap/GC/etc.
CANDYBOX_JVM_OPTS=(
  "${CANDYBOX_ADD_OPENS[@]}"
  "-Dlogback.configurationFile=$CANDYBOX_CONF_DIR/logback.xml"
)
# JAAS login config for SASL to ZooKeeper and/or BookKeeper (one file covers both: a `Client`
# section authenticates every ZooKeeper connection in the JVM — Candybox's and BK's internal one —
# and a `BookKeeper` section drives BK's SASL client auth provider).
[[ -n "${CANDYBOX_JAAS_CONF:-}" ]] && \
  CANDYBOX_JVM_OPTS+=("-Djava.security.auth.login.config=$CANDYBOX_JAAS_CONF")
# ZooKeeper client TLS is configured through ZooKeeper's standard system properties; pass them via
# CANDYBOX_EXTRA_OPTS, e.g.:
#   -Dzookeeper.client.secure=true
#   -Dzookeeper.clientCnxnSocket=org.apache.zookeeper.ClientCnxnSocketNetty
#   -Dzookeeper.ssl.trustStore.location=/etc/candybox/tls/ca.crt   (PEM is accepted)
# shellcheck disable=SC2206
[[ -n "${CANDYBOX_HEAP_OPTS:-}" ]] && CANDYBOX_JVM_OPTS+=(${CANDYBOX_HEAP_OPTS})
# shellcheck disable=SC2206
[[ -n "${CANDYBOX_EXTRA_OPTS:-}" ]] && CANDYBOX_JVM_OPTS+=(${CANDYBOX_EXTRA_OPTS})
export CANDYBOX_JVM_OPTS
