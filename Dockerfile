# syntax=docker/dockerfile:1
#
# Multi-stage build for a Candybox storage node, built straight from the git source.
#
# Lives at the repo root so Docker Hub's automated build (which clones the repo and runs
# `docker build .` from the root) needs no extra configuration. To build locally:
#
#   docker build -t candybox:latest .
#
# Stage 1 compiles the project with Maven and assembles the runtime distribution
# (bin/ lib/ conf/ logs/); stage 2 ships just that distribution on a JRE.

# ---------------------------------------------------------------------------
# Stage 1: build the distribution from source.
# ---------------------------------------------------------------------------
FROM maven:3.9-eclipse-temurin-17 AS build

WORKDIR /src

# Resolve dependencies first against just the POMs so this layer is cached and only
# re-runs when a pom.xml changes, not on every source edit.
COPY pom.xml ./
COPY candybox-common/pom.xml            candybox-common/
COPY candybox-bookkeeper/pom.xml        candybox-bookkeeper/
COPY candybox-coordination/pom.xml      candybox-coordination/
COPY candybox-lsm/pom.xml               candybox-lsm/
COPY candybox-protocol/pom.xml          candybox-protocol/
COPY candybox-server/pom.xml            candybox-server/
COPY candybox-client/pom.xml            candybox-client/
COPY candybox-integration-tests/pom.xml candybox-integration-tests/
COPY candybox-dist/pom.xml              candybox-dist/
RUN --mount=type=cache,target=/root/.m2 \
    mvn -q -B -DskipTests dependency:go-offline

# Now the sources. Build and assemble the distribution tarball (tests run via `mvn verify`
# in CI, not here, so the image build stays fast and dependency-free).
COPY . .
RUN --mount=type=cache,target=/root/.m2 \
    mvn -q -B -DskipTests package

# Unpack the assembled distribution to a stable, version-independent path so the runtime
# stage does not need to know the project version.
RUN mkdir -p /opt/candybox \
 && tar -xzf candybox-dist/target/candybox-*.tar.gz -C /opt/candybox --strip-components=1

# ---------------------------------------------------------------------------
# Stage 2: runtime image.
# ---------------------------------------------------------------------------
# Temurin 17 JRE on pinned Ubuntu 22.04: glibc (BookKeeper's RocksDB JNI and Netty native
# transports are glibc-built), includes bash for the launch scripts, and OS-pinned for
# reproducible rebuilds.
FROM eclipse-temurin:17-jre-jammy

# Unprivileged runtime user.
RUN useradd --system --uid 10001 --create-home --home-dir /opt/candybox candybox

COPY --from=build --chown=candybox:candybox /opt/candybox /opt/candybox

RUN mkdir -p /opt/candybox/data /opt/candybox/logs \
 && chown candybox:candybox /opt/candybox/data /opt/candybox/logs

ENV CANDYBOX_HOME=/opt/candybox \
    CANDYBOX_CONF_DIR=/opt/candybox/conf \
    CANDYBOX_LOG_DIR=/opt/candybox/logs \
    PATH=/opt/candybox/bin:${PATH}

USER candybox
WORKDIR /opt/candybox

# Client (TCP) and health/metrics (HTTP).
EXPOSE 9709 9710

# Foreground process; Kubernetes/Docker delivers SIGTERM for graceful lease release.
ENTRYPOINT ["candybox-server"]
