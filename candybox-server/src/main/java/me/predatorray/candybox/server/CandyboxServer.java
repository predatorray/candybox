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
package me.predatorray.candybox.server;

import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import me.predatorray.candybox.bookkeeper.LedgerStore;
import me.predatorray.candybox.bookkeeper.bk.BookKeeperLedgerStore;
import me.predatorray.candybox.common.SystemClock;
import me.predatorray.candybox.common.auth.AuthenticationProviders;
import me.predatorray.candybox.common.auth.FileCredentialStore;
import me.predatorray.candybox.common.config.SecurityConfig;
import me.predatorray.candybox.coordination.CoordinationService;
import me.predatorray.candybox.coordination.zk.ZooKeeperCoordinationService;
import me.predatorray.candybox.protocol.FrameCodec;
import me.predatorray.candybox.protocol.auth.AuthenticatingRequestHandler;
import me.predatorray.candybox.protocol.transport.RequestHandler;
import me.predatorray.candybox.protocol.transport.TcpTransportServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Process entrypoint for a Candybox storage node — the composition root that turns a
 * {@link ServerConfig} into a running node: real BookKeeper-backed {@link LedgerStore},
 * ZooKeeper-backed {@link CoordinationService}, a {@link CandyboxNode}, a {@link TcpTransportServer}
 * for client traffic and a {@link HealthServer} for orchestration probes/metrics.
 *
 * <p>The process runs in the <em>foreground</em> (no daemonization); a SIGTERM/Ctrl-C triggers the
 * registered shutdown hook, which marks the node not-ready and closes every component in reverse
 * order so the ownership leases are released cleanly rather than waiting to expire.
 */
public final class CandyboxServer {

    private static final Logger LOG = LoggerFactory.getLogger(CandyboxServer.class);

    private CandyboxServer() {
    }

    public static void main(String[] args) {
        Path confFile = resolveConfigFile(args);
        LOG.info("Loading Candybox configuration from {}", confFile.toAbsolutePath());
        ServerConfig config = ServerConfig.load(confFile);
        run(config);
    }

    /** Resolves the config file: first CLI argument, else {@code $CANDYBOX_CONF_DIR/candybox.properties}. */
    private static Path resolveConfigFile(String[] args) {
        if (args.length > 0 && !args[0].isBlank()) {
            return Path.of(args[0]);
        }
        String confDir = System.getenv("CANDYBOX_CONF_DIR");
        if (confDir != null && !confDir.isBlank()) {
            return Path.of(confDir, "candybox.properties");
        }
        return Path.of("conf", "candybox.properties");
    }

    /** Wires the node, installs the shutdown hook, and blocks the calling thread until shutdown. */
    static void run(ServerConfig config) {
        SecurityConfig security = config.security();
        LOG.info("Starting Candybox node {} (bind={}:{}, advertised={}, zk={}, tls={}, auth={})",
                config.nodeId(), config.bindHost(), config.bindPort(), config.advertisedAddress(),
                config.zookeeperConnect(), security.tlsEnabled(),
                security.authEnabled() ? security.saslMechanisms() : "off");

        LedgerStore ledgerStore =
                BookKeeperLedgerStore.create(config.metadataServiceUri(), config.ledgerPassword());
        CoordinationService coordination =
                new ZooKeeperCoordinationService(config.coordinationConnect(), SystemClock.INSTANCE);
        CandyboxNode node = new CandyboxNode(config.nodeId(), config.tuning(), ledgerStore, coordination,
                SystemClock.INSTANCE, config.advertisedAddress());
        RequestHandler handler = node.requestHandler();
        if (security.authEnabled()) {
            handler = new AuthenticatingRequestHandler(handler,
                    AuthenticationProviders.forMechanisms(security.saslMechanisms()),
                    new FileCredentialStore(security.credentialsFile()), security.authRequired());
        }
        TcpTransportServer transport = new TcpTransportServer(config.bindPort(), handler,
                new FrameCodec(), security.serverSslContext(), security.tlsClientAuth());

        AtomicBoolean ready = new AtomicBoolean(true);
        HealthServer health = new HealthServer(config.healthPort(), config.nodeId(), ready::get,
                node::ownedBoxStats);
        health.start();

        LOG.info("Candybox node {} is up: serving on {}, health on {}", config.nodeId(),
                transport.port(), health.port());

        CountDownLatch shutdown = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutdown signal received; draining node {}", config.nodeId());
            ready.set(false);
            closeQuietly("health server", health);
            closeQuietly("transport server", transport);
            closeQuietly("node (releasing leases)", node);
            closeQuietly("coordination", coordination);
            closeQuietly("ledger store", ledgerStore);
            shutdown.countDown();
        }, "candybox-shutdown"));

        try {
            shutdown.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        LOG.info("Candybox node {} stopped", config.nodeId());
    }

    private static void closeQuietly(String what, AutoCloseable closeable) {
        try {
            closeable.close();
        } catch (Exception e) {
            LOG.warn("Error closing {}: {}", what, e.toString());
        }
    }
}
