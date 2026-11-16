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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import me.predatorray.candybox.bookkeeper.bk.BookKeeperLedgerStore;
import me.predatorray.candybox.common.SystemClock;
import me.predatorray.candybox.common.auth.AuthenticationProviders;
import me.predatorray.candybox.common.auth.FileCredentialStore;
import me.predatorray.candybox.common.auth.Passwords;
import me.predatorray.candybox.common.config.CandyboxConfig;
import me.predatorray.candybox.common.tls.PemTls;
import me.predatorray.candybox.coordination.zk.ZooKeeperCoordinationService;
import me.predatorray.candybox.it.EmbeddedBookKeeper;
import me.predatorray.candybox.protocol.FrameCodec;
import me.predatorray.candybox.protocol.auth.AuthenticatingRequestHandler;
import me.predatorray.candybox.protocol.transport.TcpTransportServer;
import me.predatorray.candybox.server.CandyboxNode;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/** Exercises the production gateway composition root against real authenticated TLS node traffic. */
@Timeout(60)
class S3GatewayMainIT {
    private static EmbeddedBookKeeper bookKeeper;
    private static TestingServer zookeeper;
    private static BookKeeperLedgerStore ledgers;
    private static ZooKeeperCoordinationService coordination;
    private static CandyboxNode node;
    private static TcpTransportServer listener;
    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5)).build();

    @BeforeAll
    static void start(@TempDir Path tmp) throws Exception {
        bookKeeper = new EmbeddedBookKeeper(3);
        zookeeper = new TestingServer(true);
        ledgers = new BookKeeperLedgerStore(bookKeeper.clientConfiguration(),
                "candybox".getBytes(StandardCharsets.UTF_8));
        coordination = new ZooKeeperCoordinationService(zookeeper.getConnectString(), SystemClock.INSTANCE);
        node = new CandyboxNode(1, CandyboxConfig.builder().build(), ledgers, coordination,
                SystemClock.INSTANCE, "localhost:0");
        Path credentials = tmp.resolve("credentials.properties");
        Files.writeString(credentials, "sasl.user.gateway=" + Passwords.hash("gateway-password") + "\n");
        listener = new TcpTransportServer(0, new AuthenticatingRequestHandler(node.requestHandler(),
                AuthenticationProviders.forMechanisms(List.of("PLAIN")),
                new FileCredentialStore(credentials), true), new FrameCodec(),
                PemTls.serverContext(resource("server.pem"), resource("server.key"), null), false);
        // Publish the actual ephemeral port, never reserve and release a prospective port.
        coordination.registerMember(1, ("localhost:" + listener.port()).getBytes(StandardCharsets.UTF_8));
    }

    @AfterAll
    static void stop() throws Exception {
        Exception failure = null;
        for (AutoCloseable resource : new AutoCloseable[] {
                listener, node, coordination, ledgers, zookeeper, bookKeeper}) {
            if (resource == null) {
                continue;
            }
            try {
                resource.close();
            } catch (Exception e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private static Properties properties() throws Exception {
        Properties props = new Properties();
        props.setProperty("zookeeper.connect", zookeeper.getConnectString());
        props.setProperty("s3.bind", "127.0.0.1:0");
        props.setProperty("health.port", "0");
        props.setProperty("s3.worker-threads", "2");
        props.setProperty("s3.router-unowned-retry-ms", "2000");
        props.setProperty("node.connect.timeout.ms", "2000");
        props.setProperty("node.read.timeout.ms", "3000");
        props.setProperty("auth.client.username", "gateway");
        props.setProperty("auth.client.password", "gateway-password");
        props.setProperty("tls.enabled", "true");
        props.setProperty("tls.cert.path", resource("server.pem").toString());
        props.setProperty("tls.key.path", resource("server.key").toString());
        props.setProperty("tls.ca.path", resource("ca.pem").toString());
        props.setProperty("metrics.auth.token", "gateway-metrics");
        return props;
    }

    @Test
    void configuredCredentialsAndTlsRoundTripAndClose() throws Exception {
        S3GatewayMain.RunningServer running = S3GatewayMain.start(
                S3GatewayConfig.fromProperties(properties(), Map.of()));
        int healthPort = running.healthPort();
        try (running) {
            HttpClient https = HttpClient.newBuilder()
                    .sslContext(PemTls.clientContext(resource("ca.pem"), null, null)).build();
            String base = "https://localhost:" + running.s3Port();
            assertThat(send(https, base + "/bucket", "PUT", "").statusCode()).isEqualTo(200);
            assertThat(send(https, base + "/bucket/key", "PUT", "through the production gateway")
                    .statusCode()).isEqualTo(200);
            HttpResponse<String> get = send(https, base + "/bucket/key", "GET", "");
            assertThat(get.statusCode()).isEqualTo(200);
            assertThat(get.body()).isEqualTo("through the production gateway");
            assertThat(probe(healthPort, "/healthz").statusCode()).isEqualTo(200);
            awaitReadiness(healthPort, 200);
            assertThat(probe(healthPort, "/metrics").statusCode()).isEqualTo(401);
            HttpRequest metrics = HttpRequest.newBuilder(URI.create("http://localhost:" + healthPort + "/metrics"))
                    .timeout(Duration.ofSeconds(5)).header("Authorization", "Bearer gateway-metrics").build();
            assertThat(HTTP.send(metrics, HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(200);
            coordination.unregisterMember(1);
            try {
                awaitReadiness(healthPort, 503);
                assertThat(probe(healthPort, "/healthz").statusCode()).isEqualTo(200);
            } finally {
                coordination.registerMember(1, ("localhost:" + listener.port()).getBytes(StandardCharsets.UTF_8));
            }
        }
        running.close(); // idempotent, like the production node's lifecycle handle
        assertThatThrownBy(() -> probe(healthPort, "/healthz")).isInstanceOf(IOException.class);
    }

    @Test
    void wrongNodePasswordFailsWithinHttpDeadline() throws Exception {
        Properties props = properties();
        props.setProperty("auth.client.password", "wrong-password");
        try (S3GatewayMain.RunningServer running = S3GatewayMain.start(S3GatewayConfig.fromProperties(props, Map.of()))) {
            HttpClient https = HttpClient.newBuilder()
                    .sslContext(PemTls.clientContext(resource("ca.pem"), null, null)).build();
            HttpResponse<String> response = send(https, "https://localhost:" + running.s3Port() + "/wrong-bucket", "PUT", "");
            assertThat(response.statusCode()).isIn(403, 500, 502, 503);
            assertThat(response.body()).contains("<Error>").containsIgnoringCase("auth");
        }
    }

    @Test
    void foregroundRunUsesRegisteredDrainAndPlainUnauthenticatedTransport() throws Exception {
        Properties props = new Properties();
        props.setProperty("zookeeper.connect", zookeeper.getConnectString());
        props.setProperty("s3.bind", "127.0.0.1:0");
        props.setProperty("health.port", "0");
        props.setProperty("s3.worker-threads", "2");
        // Synchronous drain makes the foreground wait observable without a JVM shutdown hook.
        AtomicBoolean drained = new AtomicBoolean();
        S3GatewayMain.run(S3GatewayConfig.fromProperties(props, Map.of()), drain -> {
            drain.run();
            drained.set(true);
        });
        assertThat(drained).isTrue();
    }

    private static HttpResponse<String> send(HttpClient client, String uri, String method, String body)
            throws Exception {
        return client.send(HttpRequest.newBuilder(URI.create(uri)).timeout(Duration.ofSeconds(10))
                .method(method, body.isEmpty() ? HttpRequest.BodyPublishers.noBody()
                        : HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
    }

    private static HttpResponse<String> probe(int port, String path) throws Exception {
        return send(HTTP, "http://localhost:" + port + path, "GET", "");
    }

    private static void awaitReadiness(int port, int expected) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        int status;
        do {
            status = probe(port, "/readyz").statusCode();
            if (status == expected) {
                return;
            }
            Thread.sleep(25); // bounded readiness poll, including the production membership cache TTL
        } while (System.nanoTime() < deadline);
        assertThat(status).isEqualTo(expected);
    }

    private static Path resource(String name) throws Exception {
        return Path.of(S3GatewayMainIT.class.getResource("/tls/" + name).toURI());
    }
}
