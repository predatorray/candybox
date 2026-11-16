# Package an already-built distribution, avoiding a second Maven/frontend build.
FROM eclipse-temurin:17.0.17_10-jre-jammy
COPY candybox-dist/target/candybox-*.tar.gz /tmp/candybox.tar.gz
RUN mkdir -p /opt/candybox && tar -xzf /tmp/candybox.tar.gz -C /opt/candybox --strip-components=1 && rm /tmp/candybox.tar.gz
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
ENV CANDYBOX_HOME=/opt/candybox CANDYBOX_CONF_DIR=/opt/candybox/conf CANDYBOX_LOG_DIR=/opt/candybox/logs PATH=/opt/candybox/bin:${PATH}
WORKDIR /opt/candybox
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
