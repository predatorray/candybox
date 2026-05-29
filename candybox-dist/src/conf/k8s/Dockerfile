# Builds a Candybox node image from the assembled distribution tarball.
#
# Build (from the repo root, after `mvn -DskipTests package`):
#   docker build -f candybox-dist/src/conf/k8s/Dockerfile \
#     --build-arg DIST=candybox-dist/target/candybox-0.1.0-SNAPSHOT.tar.gz \
#     -t candybox:0.1.0 .
#
FROM eclipse-temurin:17-jre

ARG DIST=candybox-dist/target/candybox-0.1.0-SNAPSHOT.tar.gz

# Unpack the distribution to /opt/candybox (strip the top-level versioned directory).
WORKDIR /opt
COPY ${DIST} /tmp/candybox-dist.tar.gz
RUN mkdir -p /opt/candybox \
 && tar -xzf /tmp/candybox-dist.tar.gz -C /opt/candybox --strip-components=1 \
 && rm /tmp/candybox-dist.tar.gz \
 && useradd --system --uid 10001 candybox \
 && mkdir -p /opt/candybox/data /opt/candybox/logs \
 && chown -R candybox:candybox /opt/candybox

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
