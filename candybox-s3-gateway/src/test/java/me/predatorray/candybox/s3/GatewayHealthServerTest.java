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

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse.BodyHandlers;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import me.predatorray.candybox.common.ManualClock;
import me.predatorray.candybox.coordination.fake.InMemoryCoordinationService;
import org.junit.jupiter.api.Test;

class GatewayHealthServerTest {

    @Test
    void healthAndReadinessAndMetrics() throws Exception {
        AtomicBoolean ready = new AtomicBoolean(true);
        GatewayHealthServer health = new GatewayHealthServer(0, ready::get);
        health.start();
        try {
            HttpClient http = HttpClient.newHttpClient();
            String base = "http://127.0.0.1:" + health.port();

            assertThat(get(http, base + "/healthz").statusCode()).isEqualTo(200);

            assertThat(get(http, base + "/readyz").statusCode()).isEqualTo(200);
            ready.set(false);
            assertThat(get(http, base + "/readyz").statusCode()).isEqualTo(503);

            var metrics = get(http, base + "/metrics");
            assertThat(metrics.statusCode()).isEqualTo(200);
            assertThat(metrics.body()).contains("candybox_s3_gateway_up");
        } finally {
            health.close();
        }
    }

    @Test
    void readinessTracksCoordinationAndMembershipWhileLivenessStaysOk() throws Exception {
        InMemoryCoordinationService coordination = new InMemoryCoordinationService();
        ManualClock clock = new ManualClock(1_000);
        GatewayReadiness readiness = new GatewayReadiness(coordination::isConnected,
                coordination::members, clock, 1_000, Runnable::run);
        GatewayHealthServer health = new GatewayHealthServer(0, readiness);
        health.start();
        try {
            HttpClient http = HttpClient.newHttpClient();
            String base = "http://127.0.0.1:" + health.port();

            // No node to route to yet.
            assertThat(get(http, base + "/readyz").statusCode()).isEqualTo(503);

            coordination.registerMember(1, "127.0.0.1:9700".getBytes());
            // The membership result is cached until the TTL passes.
            assertThat(get(http, base + "/readyz").statusCode()).isEqualTo(503);
            clock.advance(1_000);
            assertThat(get(http, base + "/readyz").statusCode()).isEqualTo(200);

            // Disconnection is never cached.
            coordination.setConnected(false);
            assertThat(get(http, base + "/readyz").statusCode()).isEqualTo(503);
            assertThat(get(http, base + "/healthz").statusCode()).isEqualTo(200);
            coordination.setConnected(true);
            assertThat(get(http, base + "/readyz").statusCode()).isEqualTo(200);
        } finally {
            health.close();
        }
    }

    @Test
    void membershipReadFailureIsNotReady() {
        ManualClock clock = new ManualClock(1_000);
        GatewayReadiness readiness = new GatewayReadiness(() -> true, () -> {
            throw new IllegalStateException("connection loss");
        }, clock, 1_000, Runnable::run);
        assertThat(readiness.getAsBoolean()).isFalse();
    }

    @Test
    void refreshesInTheBackgroundAndKeepsTheLastSnapshotOnceClosed() throws Exception {
        InMemoryCoordinationService coordination = new InMemoryCoordinationService();
        coordination.registerMember(1, "127.0.0.1:9700".getBytes());
        ManualClock clock = new ManualClock(1_000);
        GatewayReadiness readiness = new GatewayReadiness(coordination, clock, 1_000);
        long deadline = System.nanoTime() + 5_000_000_000L;
        while (!readiness.getAsBoolean()) {
            assertThat(System.nanoTime()).isLessThan(deadline);
            Thread.sleep(10);
        }

        readiness.close();
        clock.advance(1_000);
        // The refresh is rejected after close; the probe still answers from the last snapshot.
        assertThat(readiness.getAsBoolean()).isTrue();
    }

    @Test
    void slowMembershipReadBlocksNeitherReadyzNorHealthz() throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        GatewayReadiness readiness = new GatewayReadiness(() -> true, () -> {
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return List.of(1);
        }, new ManualClock(1_000), 1_000, Executors.newSingleThreadExecutor());
        GatewayHealthServer health = new GatewayHealthServer(0, readiness);
        health.start();
        try {
            HttpClient http = HttpClient.newBuilder().build();
            String base = "http://127.0.0.1:" + health.port();
            Duration timeout = Duration.ofSeconds(2);

            // The refresh is stuck in ZooKeeper: no snapshot yet, so not ready, and promptly.
            assertThat(get(http, base + "/readyz", timeout).statusCode()).isEqualTo(503);
            assertThat(get(http, base + "/healthz", timeout).statusCode()).isEqualTo(200);

            release.countDown();
            long deadline = System.nanoTime() + 5_000_000_000L;
            while (get(http, base + "/readyz", timeout).statusCode() != 200) {
                assertThat(System.nanoTime()).isLessThan(deadline);
                Thread.sleep(20);
            }
        } finally {
            release.countDown();
            health.close();
            readiness.close();
        }
    }

    private static java.net.http.HttpResponse<String> get(HttpClient http, String url,
                                                          Duration timeout) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create(url)).timeout(timeout).GET().build(),
                BodyHandlers.ofString());
    }

    private static java.net.http.HttpResponse<String> get(HttpClient http, String url) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create(url)).GET().build(), BodyHandlers.ofString());
    }
}
