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
import java.util.concurrent.atomic.AtomicBoolean;
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

    private static java.net.http.HttpResponse<String> get(HttpClient http, String url) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create(url)).GET().build(), BodyHandlers.ofString());
    }
}
