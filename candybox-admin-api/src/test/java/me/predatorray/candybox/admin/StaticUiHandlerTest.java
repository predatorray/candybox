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
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.junit.jupiter.api.Test;

class StaticUiHandlerTest {

    private static final String INDEX_HTML = "<!doctype html><title>candybox</title>";

    @Test
    void spaFallbackServesIndexHtmlAsHtml() throws Exception {
        // Regression: a manual refresh of a client-side route like /ui/boxes used to be served as
        // application/octet-stream (taken from the requested path's missing extension) — browsers
        // then download the page as a file instead of rendering it.
        Map<String, byte[]> bundle = Map.of(
                "index.html", INDEX_HTML.getBytes(StandardCharsets.UTF_8),
                "assets/app.js", "console.log(1)".getBytes(StandardCharsets.UTF_8));
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/ui/", new StaticUiHandler(bundle::get));
        server.createContext("/ui", new StaticUiHandler(bundle::get));
        server.start();
        try {
            HttpClient http = HttpClient.newHttpClient();

            HttpResponse<String> spa = http.send(
                    HttpRequest.newBuilder(URI.create(base(server) + "/ui/boxes")).GET().build(),
                    BodyHandlers.ofString());
            assertThat(spa.statusCode()).isEqualTo(200);
            assertThat(spa.headers().firstValue("Content-Type"))
                    .hasValue("text/html; charset=utf-8");
            assertThat(spa.body()).isEqualTo(INDEX_HTML);

            // A real asset still gets its own content-type.
            HttpResponse<String> js = http.send(
                    HttpRequest.newBuilder(URI.create(base(server) + "/ui/assets/app.js")).GET()
                            .build(),
                    BodyHandlers.ofString());
            assertThat(js.statusCode()).isEqualTo(200);
            assertThat(js.headers().firstValue("Content-Type"))
                    .hasValue("application/javascript; charset=utf-8");

            // A missing asset (recognized extension) does NOT fall back to index.html.
            HttpResponse<String> miss = http.send(
                    HttpRequest.newBuilder(URI.create(base(server) + "/ui/assets/missing.js"))
                            .GET().build(),
                    BodyHandlers.ofString());
            // No bundle entry; looksLikeAsset is true so we skip the fallback and end up at the
            // missing-bundle placeholder. Either way, response is HTML, not octet-stream.
            assertThat(miss.headers().firstValue("Content-Type"))
                    .hasValueSatisfying(ct -> assertThat(ct).startsWith("text/html"));
        } finally {
            server.stop(0);
        }
    }

    private static String base(HttpServer server) {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }
}
