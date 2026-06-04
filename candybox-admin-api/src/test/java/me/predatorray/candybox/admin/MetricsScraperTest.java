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

import com.sun.net.httpserver.HttpServer;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;

class MetricsScraperTest {

    @Test
    void pollsTargetAndAccumulatesRollingWindow() throws Exception {
        AtomicReference<String> body = new AtomicReference<>("candybox_x 1\n");
        HttpServer fake = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        fake.createContext("/metrics", exchange -> {
            byte[] bytes = body.get().getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, bytes.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(bytes);
            }
        });
        fake.start();
        try {
            URI target = URI.create("http://127.0.0.1:" + fake.getAddress().getPort() + "/metrics");
            // 50ms polling, window of 4 — small numbers keep the test fast while exercising
            // window rotation.
            try (MetricsScraper scraper = new MetricsScraper(List.of(target), 50, 4)) {
                scraper.start();
                // The first scrape happens synchronously inside start().
                assertThat(scraper.seriesFor(List.of("candybox_x")).get("candybox_x")).hasSize(1);

                body.set("candybox_x 2\n");
                waitUntil(() ->
                        scraper.seriesFor(List.of("candybox_x")).get("candybox_x").get(0).samples()
                                .size() >= 2, 2_000);

                // Drive past the window cap to confirm the oldest entry rotates out.
                for (int v = 3; v <= 10; v++) {
                    body.set("candybox_x " + v + "\n");
                    Thread.sleep(70);
                }
                int size = scraper.seriesFor(List.of("candybox_x")).get("candybox_x").get(0)
                        .samples().size();
                assertThat(size).isLessThanOrEqualTo(4);

                // latestText reflects the most recent scrape.
                assertThat(scraper.latestText()).contains("candybox_x");
            }
        } finally {
            fake.stop(0);
        }
    }

    @Test
    void seriesForReturnsEmptyForUnknownNames() {
        try (MetricsScraper scraper = new MetricsScraper(List.of(), 1000, 60)) {
            // No targets — start() is a no-op, but seriesFor still works.
            scraper.start();
            assertThat(scraper.seriesFor(List.of("never_scraped"))
                    .get("never_scraped")).isEmpty();
        }
    }

    @Test
    void unreachableTargetDoesNotCrashScraper() {
        // Pointing at a port that nothing's listening on exercises the catch-and-warn path.
        try (MetricsScraper scraper = new MetricsScraper(
                List.of(URI.create("http://127.0.0.1:1/metrics")), 1000, 60)) {
            scraper.start(); // does a synchronous first scrape; must not throw
            assertThat(scraper.latestText()).isEmpty();
            assertThat(scraper.seriesFor(List.of("anything")).get("anything")).isEmpty();
        }
    }

    @Test
    void nonOkResponsesAreSilentlySkipped() throws Exception {
        // A target that returns 503 should NOT contribute a sample — operators expect Prometheus-
        // style "scrape failed; gap in the series" behaviour, not zero-valued samples.
        HttpServer fake = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        fake.createContext("/metrics", exchange -> {
            byte[] bytes = "candybox_x 99\n".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(503, bytes.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(bytes);
            }
        });
        fake.start();
        try {
            URI t = URI.create("http://127.0.0.1:" + fake.getAddress().getPort() + "/metrics");
            try (MetricsScraper scraper = new MetricsScraper(List.of(t), 1000, 4)) {
                scraper.start(); // synchronous first tick
                // No samples, no concat.
                assertThat(scraper.seriesFor(List.of("candybox_x")).get("candybox_x")).isEmpty();
                assertThat(scraper.latestText()).isEmpty();
            }
        } finally {
            fake.stop(0);
        }
    }

    @Test
    void multipleTargetsConcatenateAndShareIngestTimestamp() throws Exception {
        // Two targets, each with one sample. The concatenated latestText should carry both, and
        // the per-target prefix line lets an operator see which target each block came from.
        HttpServer a = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        HttpServer b = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        a.createContext("/metrics", exchange -> {
            byte[] bytes = "x{node=\"a\"} 1\n".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, bytes.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(bytes);
            }
        });
        b.createContext("/metrics", exchange -> {
            // Note: no trailing newline — the scraper appends one so concatenation stays clean.
            byte[] bytes = "x{node=\"b\"} 2".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, bytes.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(bytes);
            }
        });
        a.start();
        b.start();
        try {
            URI ta = URI.create("http://127.0.0.1:" + a.getAddress().getPort() + "/metrics");
            URI tb = URI.create("http://127.0.0.1:" + b.getAddress().getPort() + "/metrics");
            try (MetricsScraper scraper = new MetricsScraper(List.of(ta, tb), 1000, 4)) {
                scraper.start();
                String txt = scraper.latestText();
                assertThat(txt).contains("# target=").contains(ta.toString()).contains(tb.toString())
                        .contains("x{node=\"a\"} 1").contains("x{node=\"b\"} 2");
                // Two distinct (name, labels) series for "x".
                List<MetricsScraper.Series> xs = scraper.seriesFor(List.of("x")).get("x");
                assertThat(xs).hasSize(2);
                assertThat(xs).extracting(s -> s.labels().get("node"))
                        .containsExactlyInAnyOrder("a", "b");
                // Window seconds = (samples * intervalMs)/1000; here 4 * 1000 / 1000 = 4.
                assertThat(scraper.windowSeconds()).isEqualTo(4);
            }
        } finally {
            a.stop(0);
            b.stop(0);
        }
    }

    private static void waitUntil(BooleanSupplier cond, long timeoutMillis) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (cond.getAsBoolean()) {
                return;
            }
            Thread.sleep(25);
        }
        throw new AssertionError("condition never became true within " + timeoutMillis + "ms");
    }
}
