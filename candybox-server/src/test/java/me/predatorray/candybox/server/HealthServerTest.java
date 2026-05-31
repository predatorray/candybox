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

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import me.predatorray.candybox.lsm.engine.BoxEngineStats;
import org.junit.jupiter.api.Test;

/**
 * Covers the JDK-{@code HttpServer}-backed {@link HealthServer}: the {@code /healthz}, {@code /readyz}
 * and {@code /metrics} endpoints, plus the Prometheus text rendering (labels, counters, gauge, and
 * label escaping). No node or backends required — the readiness predicate and stats are supplied
 * directly.
 */
class HealthServerTest {

    private static BoxEngineStats stats(long puts, long gets) {
        return new BoxEngineStats(puts, 0, gets, 0, 0, 0, 0, 0);
    }

    private static String get(int port, String path) throws Exception {
        HttpClient http = HttpClient.newHttpClient();
        return http.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path)).GET().build(),
                BodyHandlers.ofString()).body();
    }

    private static int status(int port, String path) throws Exception {
        HttpClient http = HttpClient.newHttpClient();
        return http.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path)).GET().build(),
                BodyHandlers.ofString()).statusCode();
    }

    @Test
    void healthzIsAlwaysOkAndReadyzFollowsThePredicate() throws Exception {
        AtomicBoolean ready = new AtomicBoolean(false);
        try (HealthServer server = new HealthServer(0, 1, ready::get, Map::of)) {
            server.start();
            int port = server.port();

            assertThat(status(port, "/healthz")).isEqualTo(200);
            assertThat(get(port, "/healthz")).contains("ok");

            // Not ready yet -> 503.
            assertThat(status(port, "/readyz")).isEqualTo(503);
            assertThat(get(port, "/readyz")).contains("not ready");

            // Flip readiness -> 200.
            ready.set(true);
            assertThat(status(port, "/readyz")).isEqualTo(200);
            assertThat(get(port, "/readyz")).contains("ready");
        }
    }

    @Test
    void metricsEndpointRendersPerBoxCounters() throws Exception {
        Map<String, BoxEngineStats> byBox = Map.of("photos", stats(3, 7));
        try (HealthServer server = new HealthServer(0, 42, () -> true, () -> byBox)) {
            server.start();
            String body = get(server.port(), "/metrics");
            assertThat(body)
                    .contains("# TYPE candybox_puts_total counter")
                    .contains("candybox_puts_total{node=\"42\",box=\"photos\"} 3")
                    .contains("candybox_gets_total{node=\"42\",box=\"photos\"} 7")
                    .contains("candybox_owned_boxes{node=\"42\"} 1");
        }
    }

    @Test
    void renderMetricsEscapesLabelSpecialCharacters() {
        Map<String, BoxEngineStats> byBox = Map.of("a\"b\\c", stats(1, 0));
        String rendered = HealthServer.renderMetrics(5, byBox);
        // The box label's quote and backslash must be escaped for valid Prometheus exposition.
        assertThat(rendered).contains("box=\"a\\\"b\\\\c\"");
        assertThat(rendered).contains("candybox_owned_boxes{node=\"5\"} 1");
    }

    @Test
    void renderMetricsWithNoBoxesStillEmitsTheGauge() {
        String rendered = HealthServer.renderMetrics(9, Map.of());
        assertThat(rendered).contains("candybox_owned_boxes{node=\"9\"} 0");
        // The counter HELP/TYPE headers are present even with no series rows.
        assertThat(rendered).contains("# TYPE candybox_compactions_total counter");
    }
}
