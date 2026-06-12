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
package me.predatorray.candybox.admin;

import java.net.URI;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import me.predatorray.candybox.client.CandyboxClient;
import me.predatorray.candybox.common.SystemClock;
import me.predatorray.candybox.common.config.CandyboxConfig;
import me.predatorray.candybox.common.config.SecurityConfig;
import me.predatorray.candybox.coordination.CoordinationService;
import me.predatorray.candybox.coordination.zk.ZkAuth;
import me.predatorray.candybox.coordination.zk.ZooKeeperCoordinationService;
import me.predatorray.candybox.protocol.auth.AuthenticatingTransport;
import me.predatorray.candybox.protocol.transport.TcpTransport;
import me.predatorray.candybox.protocol.transport.Transport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CLI entrypoint. Reads a handful of environment-variable overrides on top of
 * {@link AdminApiConfig#defaults()} and runs the server until SIGTERM.
 *
 * <p>Wiring mirrors {@code S3GatewayMain}: when {@code CANDYBOX_ADMIN_ZK} is set, build a
 * {@link ZooKeeperCoordinationService} + {@link TcpTransport} + cluster-aware
 * {@link CandyboxClient} and hand them to {@link LiveDashboardData}. Without it, the server still
 * boots with {@link EmptyDashboardData} so the dashboard can be demoed UI-only.
 *
 * <p>Recognized env vars:
 *
 * <ul>
 *   <li>{@code CANDYBOX_ADMIN_PORT} — TCP port, default {@code 9713}.</li>
 *   <li>{@code CANDYBOX_ADMIN_BIND} — interface, default {@code 0.0.0.0}.</li>
 *   <li>{@code CANDYBOX_ADMIN_CORS} — {@code Access-Control-Allow-Origin}, default {@code *}.</li>
 *   <li>{@code CANDYBOX_ADMIN_UI} — set to {@code false} to disable {@code /ui/*}.</li>
 *   <li>{@code CANDYBOX_ADMIN_ZK} — ZooKeeper connect string (enables live data).</li>
 *   <li>{@code CANDYBOX_ADMIN_SCRAPE_TARGETS} — comma-separated Prometheus URLs (typically each
 *       node's {@code http://host:port/metrics}). Empty disables the scraper.</li>
 *   <li>{@code CANDYBOX_ADMIN_SCRAPE_INTERVAL_MS} — poll interval, default 5000.</li>
 *   <li>{@code CANDYBOX_ADMIN_SCRAPE_WINDOW} — samples per series, default 60 (≈ 5 min at 5 s).</li>
 * </ul>
 */
public final class AdminApiMain {

    private static final Logger LOG = LoggerFactory.getLogger(AdminApiMain.class);

    private AdminApiMain() {
    }

    public static void main(String[] args) {
        AdminApiConfig config = fromEnv(System.getenv());
        // LIFO close: every component pushes itself; the shutdown hook pops them in reverse.
        Deque<AutoCloseable> stack = new ArrayDeque<>();
        DashboardData data =
                buildDataSource(System.getenv("CANDYBOX_ADMIN_ZK"), stack, System.getenv());

        MetricsScraper scraper = buildScraper(System.getenv());
        if (scraper != null) {
            stack.push(scraper);
            scraper.start();
        }
        String authToken = orDefault(System.getenv("CANDYBOX_ADMIN_AUTH_TOKEN"), null);
        SecurityConfig listenerSecurity = SecurityConfig.resolve(key -> java.util.Optional
                .ofNullable(System.getenv("CANDYBOX_" + key.toUpperCase().replace('.', '_')))
                .filter(v -> !v.isBlank()).map(String::trim));
        AdminApiServer server = new AdminApiServer(config, () -> true, data, scraper, authToken,
                listenerSecurity.serverSslContext());
        if (authToken == null) {
            LOG.warn("CANDYBOX_ADMIN_AUTH_TOKEN is not set — the admin API (including box "
                    + "browsing and uploads) is open to anyone who can reach port {}",
                    config.port());
        }
        stack.push(server);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> closeAll(stack), "admin-api-shutdown"));
        server.start();
        LOG.info("Admin API ready on http://{}:{}/ui/", config.bindAddress(), server.port());
        try {
            Thread.currentThread().join();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    static AdminApiConfig fromEnv(java.util.Map<String, String> env) {
        AdminApiConfig defaults = AdminApiConfig.defaults();
        int port = parseIntOr(env.get("CANDYBOX_ADMIN_PORT"), defaults.port());
        String bind = orDefault(env.get("CANDYBOX_ADMIN_BIND"), defaults.bindAddress());
        String cors = orDefault(env.get("CANDYBOX_ADMIN_CORS"), defaults.corsAllowOrigin());
        boolean ui = !"false".equalsIgnoreCase(env.get("CANDYBOX_ADMIN_UI"));
        return new AdminApiConfig(port, bind, cors, ui);
    }

    static MetricsScraper buildScraper(java.util.Map<String, String> env) {
        String raw = env.getOrDefault("CANDYBOX_ADMIN_SCRAPE_TARGETS", "");
        List<URI> targets = new ArrayList<>();
        for (String t : raw.split(",")) {
            String trimmed = t.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            try {
                targets.add(URI.create(trimmed));
            } catch (IllegalArgumentException e) {
                LOG.warn("Ignoring malformed scrape target {}: {}", trimmed, e.toString());
            }
        }
        if (targets.isEmpty()) {
            return null;
        }
        long interval = parseIntOr(env.get("CANDYBOX_ADMIN_SCRAPE_INTERVAL_MS"), 5000);
        int window = parseIntOr(env.get("CANDYBOX_ADMIN_SCRAPE_WINDOW"), 60);
        return new MetricsScraper(targets, interval, window,
                orDefault(env.get("CANDYBOX_ADMIN_SCRAPE_TOKEN"), null));
    }

    private static DashboardData buildDataSource(String zk, Deque<AutoCloseable> closeStack,
                                                 java.util.Map<String, String> env) {
        if (zk == null || zk.isBlank()) {
            LOG.info("No CANDYBOX_ADMIN_ZK set — running with empty data source (UI-only demo).");
            return new EmptyDashboardData();
        }
        LOG.info("Wiring live data source against ZooKeeper {}", zk);
        // The shared auth.*/tls.*/zookeeper.auth.* surface, resolved from CANDYBOX_* env vars
        // (the admin API is env-configured) — how this process dials nodes and ZooKeeper.
        SecurityConfig security = SecurityConfig.resolve(key -> java.util.Optional
                .ofNullable(env.get("CANDYBOX_" + key.toUpperCase().replace('.', '_')))
                .filter(s -> !s.isBlank()).map(String::trim));
        Transport tcp = new TcpTransport(new me.predatorray.candybox.protocol.FrameCodec(),
                security.clientSslContext(), security.tlsVerifyEndpoint());
        Transport transport = security.clientUsername() == null ? tcp
                : new AuthenticatingTransport(tcp, security.clientMechanism(),
                        security.clientUsername(), security.clientPassword());
        closeStack.push(transport);
        CoordinationService coordination = new ZooKeeperCoordinationService(zk,
                SystemClock.INSTANCE, new ZkAuth(security.zkAuthScheme(),
                        security.zkAuthCredentials(), security.zkAclEnabled()));
        closeStack.push(coordination);
        CandyboxClient client =
                new CandyboxClient(transport, coordination, CandyboxConfig.builder().build());
        closeStack.push(client);
        return new LiveDashboardData(coordination, client);
    }

    private static void closeAll(Deque<AutoCloseable> stack) {
        while (!stack.isEmpty()) {
            AutoCloseable c = stack.pop();
            try {
                c.close();
            } catch (Exception e) {
                LOG.warn("Error closing {}: {}", c.getClass().getSimpleName(), e.toString());
            }
        }
    }

    private static int parseIntOr(String s, int fallback) {
        if (s == null || s.isBlank()) {
            return fallback;
        }
        try {
            return Integer.parseInt(s.trim());
        } catch (NumberFormatException e) {
            LOG.warn("Ignoring non-numeric env override: {}", s);
            return fallback;
        }
    }

    private static String orDefault(String s, String fallback) {
        return (s == null || s.isBlank()) ? fallback : s;
    }
}
