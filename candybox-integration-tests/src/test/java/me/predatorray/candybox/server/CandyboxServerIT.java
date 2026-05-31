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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import me.predatorray.candybox.client.CandyboxClient;
import me.predatorray.candybox.it.EmbeddedBookKeeper;
import me.predatorray.candybox.protocol.transport.TcpTransport;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Boots a full Candybox node through its process entrypoint {@link CandyboxServer#run(ServerConfig)} —
 * loaded from a properties file via {@link ServerConfig#load} — against embedded BookKeeper and its
 * in-process ZooKeeper. Verifies the composition root actually stands up: the health/metrics endpoint
 * answers and the TCP server serves a real client round-trip. {@code run} blocks in the foreground, so
 * it is driven on a daemon thread; the forked IT JVM's exit runs the registered shutdown hook. Lives
 * in the {@code me.predatorray.candybox.server} package to reach the package-private {@code run}.
 */
class CandyboxServerIT {

    private static EmbeddedBookKeeper bookKeeper;
    private static TestingServer zookeeper;
    private static int bindPort;
    private static int healthPort;
    private static Thread serverThread;
    private static final AtomicReference<Throwable> serverFailure = new AtomicReference<>();

    @BeforeAll
    static void start(@TempDir Path tmp) throws Exception {
        bookKeeper = new EmbeddedBookKeeper(3);
        zookeeper = new TestingServer(true);
        bindPort = freePort();
        healthPort = freePort();

        Path conf = tmp.resolve("candybox.properties");
        Files.writeString(conf, String.join("\n",
                "node.id=1",
                // Coordination uses a dedicated ZooKeeper; ledger metadata lives in the embedded
                // BookKeeper's own ZK, named explicitly so the two never share an ensemble.
                "zookeeper.connect=" + zookeeper.getConnectString(),
                "bookkeeper.metadataServiceUri=" + bookKeeper.metadataServiceUri(),
                "server.bind=127.0.0.1:" + bindPort,
                "server.advertised=127.0.0.1:" + bindPort,
                "health.port=" + healthPort,
                ""), StandardCharsets.UTF_8);

        ServerConfig config = ServerConfig.load(conf);
        serverThread = new Thread(() -> {
            try {
                CandyboxServer.run(config); // blocks until JVM shutdown
            } catch (Throwable t) {
                serverFailure.set(t);
            }
        }, "candybox-server-under-test");
        serverThread.setDaemon(true);
        serverThread.start();

        awaitHealthy();
    }

    @AfterAll
    static void stop() {
        // run() owns its components and only releases them via its JVM shutdown hook, which fires when
        // this forked IT JVM exits; we just tear down the embedded cluster here.
        closeQuietly(zookeeper);
        closeQuietly(bookKeeper);
    }

    private static void closeQuietly(AutoCloseable c) {
        if (c == null) {
            return;
        }
        try {
            c.close();
        } catch (Exception ignored) {
            // best effort
        }
    }

    @Test
    void healthAndReadinessEndpointsAnswer() throws Exception {
        assertThat(httpStatus("/healthz")).isEqualTo(200);
        assertThat(httpStatus("/readyz")).isEqualTo(200);
        assertThat(httpBody("/metrics")).contains("candybox_owned_boxes");
    }

    @Test
    void bootstrappedNodeServesClientTraffic() {
        try (TcpTransport transport = new TcpTransport();
             CandyboxClient client = new CandyboxClient(transport, "127.0.0.1", bindPort)) {
            client.createBox("server-it-box");
            byte[] data = "via the process entrypoint".getBytes(StandardCharsets.UTF_8);
            client.putCandy("server-it-box", "k", data, "text/plain", Map.of(), null);
            assertThat(client.getCandy("server-it-box", "k")).isEqualTo(data);
        }
    }

    // ---- helpers ---------------------------------------------------------------------------

    private static void awaitHealthy() {
        long deadline = System.currentTimeMillis() + 60_000;
        while (System.currentTimeMillis() < deadline) {
            Throwable failure = serverFailure.get();
            if (failure != null) {
                throw new IllegalStateException("Server failed to start", failure);
            }
            try {
                if (httpStatus("/healthz") == 200) {
                    return;
                }
            } catch (Exception notUpYet) {
                // keep polling
            }
            sleep();
        }
        throw new IllegalStateException("Bootstrapped node did not become healthy in time");
    }

    private static int httpStatus(String path) throws Exception {
        return send(path).statusCode();
    }

    private static String httpBody(String path) throws Exception {
        return send(path).body();
    }

    private static HttpResponse<String> send(String path) throws Exception {
        return HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + healthPort + path)).GET().build(),
                BodyHandlers.ofString());
    }

    private static void sleep() {
        try {
            Thread.sleep(250);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static int freePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
