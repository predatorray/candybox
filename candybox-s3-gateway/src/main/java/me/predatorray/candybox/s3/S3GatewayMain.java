/*
 * Copyright (c) 2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package me.predatorray.candybox.s3;

import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import me.predatorray.candybox.client.CandyboxClient;
import me.predatorray.candybox.common.SystemClock;
import me.predatorray.candybox.common.config.CandyboxConfig;
import me.predatorray.candybox.common.config.SecurityConfig;
import me.predatorray.candybox.coordination.CoordinationService;
import me.predatorray.candybox.coordination.zk.ZkAuth;
import me.predatorray.candybox.coordination.zk.ZooKeeperCoordinationService;
import me.predatorray.candybox.protocol.FrameCodec;
import me.predatorray.candybox.protocol.auth.AuthenticatingTransport;
import me.predatorray.candybox.protocol.transport.TcpTransport;
import me.predatorray.candybox.protocol.transport.Transport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Process entrypoint for the S3 gateway: turns an {@link S3GatewayConfig} into a running, stateless,
 * cluster-aware gateway — a {@link TcpTransport} + ZooKeeper-backed {@link CoordinationService} behind a
 * cluster {@link CandyboxClient}, fronted by the Netty {@link S3GatewayServer} and a
 * {@link GatewayHealthServer}.
 *
 * <p>Runs in the foreground; SIGTERM/Ctrl-C marks it not-ready (so the load balancer drains it) and
 * closes every component in reverse order.
 */
public final class S3GatewayMain {

    private static final Logger LOG = LoggerFactory.getLogger(S3GatewayMain.class);

    private S3GatewayMain() {
    }

    public static void main(String[] args) {
        Path confFile = resolveConfigFile(args);
        LOG.info("Loading S3 gateway configuration from {}", confFile.toAbsolutePath());
        run(S3GatewayConfig.load(confFile));
    }

    private static Path resolveConfigFile(String[] args) {
        if (args.length > 0 && !args[0].isBlank()) {
            return Path.of(args[0]);
        }
        String confDir = System.getenv("CANDYBOX_CONF_DIR");
        if (confDir != null && !confDir.isBlank()) {
            return Path.of(confDir, "candybox-s3-gateway.properties");
        }
        return Path.of("conf", "candybox-s3-gateway.properties");
    }

    static void run(S3GatewayConfig config) {
        SecurityConfig security = config.security();
        LOG.info("Starting Candybox S3 gateway (bind={}:{}, zk={}, region={}, node-tls={}, node-auth={})",
                config.bindHost(), config.bindPort(), config.zookeeperConnect(), config.region(),
                security.tlsEnabled(), security.clientUsername() != null);

        Transport tcp = new TcpTransport(new FrameCodec(), security.clientSslContext(),
                security.tlsVerifyEndpoint());
        Transport transport = security.clientUsername() == null ? tcp
                : new AuthenticatingTransport(tcp, security.clientMechanism(),
                        security.clientUsername(), security.clientPassword());
        CoordinationService coordination = new ZooKeeperCoordinationService(
                config.zookeeperConnect(), SystemClock.INSTANCE,
                new ZkAuth(security.zkAuthScheme(), security.zkAuthCredentials(),
                        security.zkAclEnabled()));
        CandyboxConfig clientConfig = CandyboxConfig.builder()
                .routerCacheTtlMillis(config.routerCacheTtlMillis())
                .build();
        CandyboxClient client = new CandyboxClient(transport, coordination, clientConfig);

        S3Gateway gateway = new S3Gateway(config, client);
        gateway.start();

        AtomicBoolean ready = new AtomicBoolean(true);
        GatewayHealthServer health = new GatewayHealthServer(config.healthPort(), ready::get);
        health.start();

        LOG.info("Candybox S3 gateway is up: S3 on {}, health on {}", gateway.port(), health.port());

        CountDownLatch shutdown = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutdown signal received; draining S3 gateway");
            ready.set(false);
            closeQuietly("health server", health);
            closeQuietly("S3 gateway", gateway);
            closeQuietly("candybox client", client);
            closeQuietly("coordination", coordination);
            closeQuietly("transport", transport);
            shutdown.countDown();
        }, "candybox-s3-gateway-shutdown"));

        try {
            shutdown.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        LOG.info("Candybox S3 gateway stopped");
    }

    private static void closeQuietly(String what, AutoCloseable closeable) {
        try {
            closeable.close();
        } catch (Exception e) {
            LOG.warn("Error closing {}: {}", what, e.toString());
        }
    }
}
