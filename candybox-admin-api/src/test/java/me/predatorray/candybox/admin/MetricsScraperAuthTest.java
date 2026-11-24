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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class MetricsScraperAuthTest {
    @Test
    void sendsBearerTokenAndSkipsUnauthorizedSamples() throws Exception {
        AtomicReference<String> authorization = new AtomicReference<>();
        HttpServer node = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        node.createContext("/metrics", exchange -> {
            String header = exchange.getRequestHeaders().getFirst("Authorization");
            authorization.set(header);
            // A non-200 response contains a valid sample too: it must never be ingested.
            byte[] body = "candybox_owned_boxes 7\n".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders("Bearer tok".equals(header) ? 200 : 401, body.length);
            try (var out = exchange.getResponseBody()) {
                out.write(body);
            }
        });
        node.start();
        URI target = URI.create("http://127.0.0.1:" + node.getAddress().getPort() + "/metrics");
        try (MetricsScraper authenticated = new MetricsScraper(List.of(target), 1000, 4, "tok");
             MetricsScraper anonymous = new MetricsScraper(List.of(target), 1000, 4)) {
            authenticated.scrapeOnce();
            assertThat(authorization.get()).isEqualTo("Bearer tok");
            var series = authenticated.seriesFor(List.of("candybox_owned_boxes")).get("candybox_owned_boxes");
            assertThat(series).hasSize(1);
            assertThat(series.get(0).samples()).extracting(MetricsScraper.Sample::v).containsExactly(7.0);
            assertThat(authenticated.latestText()).contains("candybox_owned_boxes 7");

            assertThatCode(anonymous::scrapeOnce).doesNotThrowAnyException();
            assertThat(authorization.get()).isNull();
            assertThat(anonymous.seriesFor(List.of("candybox_owned_boxes")).get("candybox_owned_boxes")).isEmpty();
            assertThat(anonymous.latestText()).isEmpty();
            assertThat(series.get(0).samples()).hasSize(1);
        } finally {
            node.stop(0);
        }
    }
}
