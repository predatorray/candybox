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

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.function.BooleanSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The gateway's orchestration endpoint, on the JDK {@link HttpServer} (no extra dependency), separate
 * from the S3 listener so the load balancer can health-check it independently:
 *
 * <ul>
 *   <li>{@code GET /healthz} — liveness; {@code 200} while the JVM is up.</li>
 *   <li>{@code GET /readyz} — readiness; {@code 200} when the supplied predicate is true (bound and the
 *       cluster client is usable), else {@code 503}.</li>
 *   <li>{@code GET /metrics} — minimal Prometheus exposition.</li>
 * </ul>
 */
final class GatewayHealthServer implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(GatewayHealthServer.class);

    private final HttpServer http;

    GatewayHealthServer(int port, BooleanSupplier ready) {
        try {
            this.http = HttpServer.create(new InetSocketAddress(port), 0);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to bind gateway health server on port " + port, e);
        }
        http.createContext("/healthz", exchange -> respond(exchange, 200, "ok\n"));
        http.createContext("/readyz", exchange -> {
            boolean ok = ready.getAsBoolean();
            respond(exchange, ok ? 200 : 503, ok ? "ready\n" : "not ready\n");
        });
        http.createContext("/metrics", exchange -> respond(exchange, 200,
                "# HELP candybox_s3_gateway_up Gateway process up.\n"
                        + "# TYPE candybox_s3_gateway_up gauge\n"
                        + "candybox_s3_gateway_up 1\n"));
        http.setExecutor(null);
    }

    void start() {
        http.start();
        LOG.info("Gateway health/metrics endpoint listening on port {}", http.getAddress().getPort());
    }

    int port() {
        return http.getAddress().getPort();
    }

    private static void respond(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }

    @Override
    public void close() {
        http.stop(0);
    }
}
