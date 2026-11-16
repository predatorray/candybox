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
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class GatewayHealthServerMetricsTokenTest {
    @Test
    void metricsTokenGuardsMetricsButNotProbes() throws Exception {
        AtomicBoolean ready = new AtomicBoolean(true);
        try (GatewayHealthServer server = new GatewayHealthServer(0, ready::get, "tok")) {
            server.start();
            assertThat(get(server, "/metrics", null).statusCode()).isEqualTo(401);
            assertThat(get(server, "/metrics", "Bearer wrong").statusCode()).isEqualTo(401);
            assertThat(get(server, "/metrics", "Basic tok").statusCode()).isEqualTo(401);
            HttpResponse<String> metrics = get(server, "/metrics", "Bearer tok");
            assertThat(metrics.statusCode()).isEqualTo(200);
            assertThat(metrics.body()).contains("candybox_s3_gateway_up 1");
            assertThat(get(server, "/healthz", null).statusCode()).isEqualTo(200);
            assertThat(get(server, "/readyz", null).statusCode()).isEqualTo(200);
            ready.set(false);
            assertThat(get(server, "/readyz", null).statusCode()).isEqualTo(503);
            assertThat(get(server, "/healthz", null).statusCode()).isEqualTo(200);
        }
    }

    private HttpResponse<String> get(GatewayHealthServer server, String path, String auth)
            throws Exception {
        HttpRequest.Builder request = HttpRequest.newBuilder(
                URI.create("http://127.0.0.1:" + server.port() + path)).timeout(Duration.ofSeconds(5));
        if (auth != null) {
            request.header("Authorization", auth);
        }
        return HttpClient.newHttpClient().send(request.GET().build(), HttpResponse.BodyHandlers.ofString());
    }
}
