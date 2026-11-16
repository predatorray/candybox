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

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import me.predatorray.candybox.common.tls.PemTls;
import org.junit.jupiter.api.Test;

class AdminApiHttpsTest {
    private Path tls(String name) throws Exception {
        return Path.of(getClass().getResource("/tls/" + name).toURI());
    }

    @Test
    void servesHealthOverHttpsUsingConfiguredCertificate() throws Exception {
        var ssl = PemTls.serverContext(tls("server.pem"), tls("server.key"), null);
        var clientSsl = PemTls.clientContext(tls("ca.pem"), null, null);
        try (AdminApiServer server = new AdminApiServer(
                new AdminApiConfig(0, "127.0.0.1", "*", false), () -> true,
                new FakeDashboardData(), null, null, ssl)) {
            server.start();
            var client = HttpClient.newBuilder().sslContext(clientSsl)
                    .connectTimeout(Duration.ofSeconds(3)).build();
            var response = client.send(HttpRequest.newBuilder(
                            URI.create("https://127.0.0.1:" + server.port() + "/healthz"))
                    .timeout(Duration.ofSeconds(5)).GET().build(), HttpResponse.BodyHandlers.ofString());
            assertThat(response.statusCode()).isEqualTo(200);
            assertThat(response.body()).isEqualTo("ok\n");
            assertThat(response.sslSession()).isPresent();
        }
    }
}
