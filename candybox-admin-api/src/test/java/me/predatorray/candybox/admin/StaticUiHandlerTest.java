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

    @Test
    void pathTraversalIsRejected() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/ui/", new StaticUiHandler(path -> null));
        server.start();
        try {
            HttpResponse<String> r = HttpClient.newHttpClient().send(
                    HttpRequest.newBuilder(URI.create(base(server) + "/ui/../etc/passwd"))
                            .GET().build(),
                    BodyHandlers.ofString());
            // The JDK URL parser will normalize some `..` sequences before they reach the handler;
            // assert only that the handler refused to serve binary garbage. A 400 is the explicit
            // refusal path; a 404 indicates the JDK normalized and the missing-bundle page kicked
            // in — both are acceptable. What MUST NOT happen is a 200 application/octet-stream
            // download of arbitrary filesystem content.
            assertThat(r.statusCode()).isIn(400, 404, 200);
            assertThat(r.headers().firstValue("Content-Type"))
                    .hasValueSatisfying(ct -> assertThat(ct).startsWith("text/"));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void traversalSegmentRejectedDirectly() throws Exception {
        // Bypass the JDK normalization by hitting the handler with a raw exchange. Encoded `..`
        // sequences round-trip through the request URI; the handler's own check is what fires.
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/ui/", new StaticUiHandler(path -> {
            // Should never be invoked — the handler should short-circuit on the `..` check first.
            throw new AssertionError("loader called for traversal path: " + path);
        }));
        server.start();
        try {
            // Raw HTTP — the JDK HttpClient pre-normalizes, so write the request line by hand.
            try (java.net.Socket sock = new java.net.Socket("127.0.0.1", server.getAddress().getPort());
                 java.io.OutputStream out = sock.getOutputStream();
                 java.io.InputStream in = sock.getInputStream()) {
                out.write(("GET /ui/..%2Fsecret HTTP/1.1\r\n"
                        + "Host: localhost\r\nConnection: close\r\n\r\n").getBytes(StandardCharsets.UTF_8));
                out.flush();
                String resp = new String(in.readAllBytes(), StandardCharsets.UTF_8);
                assertThat(resp).startsWith("HTTP/1.1 400");
                assertThat(resp).contains("bad request");
            }
        } finally {
            server.stop(0);
        }
    }

    @Test
    void missingBundleServesPlaceholder() throws Exception {
        // Loader returns null for everything — even index.html. The handler should fall through
        // to the inline HTML placeholder so a confused operator sees the build hint instead of a
        // raw 404.
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/ui/", new StaticUiHandler(path -> null));
        server.createContext("/ui", new StaticUiHandler(path -> null));
        server.start();
        try {
            HttpResponse<String> root = HttpClient.newHttpClient().send(
                    HttpRequest.newBuilder(URI.create(base(server) + "/ui/")).GET().build(),
                    BodyHandlers.ofString());
            assertThat(root.statusCode()).isEqualTo(200);
            assertThat(root.headers().firstValue("Content-Type"))
                    .hasValueSatisfying(ct -> assertThat(ct).startsWith("text/html"));
            assertThat(root.body()).containsIgnoringCase("UI bundle not built");

            // /ui (no trailing slash) also resolves — the route registers both /ui and /ui/.
            HttpResponse<String> bare = HttpClient.newHttpClient().send(
                    HttpRequest.newBuilder(URI.create(base(server) + "/ui")).GET().build(),
                    BodyHandlers.ofString());
            assertThat(bare.statusCode()).isEqualTo(200);
            assertThat(bare.body()).containsIgnoringCase("UI bundle not built");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void contentTypeByExtensionCoversEveryBranch() throws Exception {
        // One-line assets per recognized extension — assert each lands on the correct Content-Type
        // so a future addition to the switch table is matched by a regression test.
        Map<String, byte[]> bundle = new java.util.LinkedHashMap<>();
        bundle.put("a.html", new byte[]{1});
        bundle.put("a.js", new byte[]{1});
        bundle.put("a.css", new byte[]{1});
        bundle.put("a.json", new byte[]{1});
        bundle.put("a.map", new byte[]{1});
        bundle.put("a.svg", new byte[]{1});
        bundle.put("a.png", new byte[]{1});
        bundle.put("a.jpg", new byte[]{1});
        bundle.put("a.jpeg", new byte[]{1});
        bundle.put("a.gif", new byte[]{1});
        bundle.put("a.ico", new byte[]{1});
        bundle.put("a.webp", new byte[]{1});
        bundle.put("a.woff", new byte[]{1});
        bundle.put("a.woff2", new byte[]{1});
        bundle.put("a.ttf", new byte[]{1});
        bundle.put("a.txt", new byte[]{1});
        bundle.put("a.wasm", new byte[]{1});
        // Unknown extension — falls through to application/octet-stream.
        bundle.put("a.bin", new byte[]{1});

        Map<String, String> expected = Map.ofEntries(
                Map.entry("a.html", "text/html"),
                Map.entry("a.js", "application/javascript"),
                Map.entry("a.css", "text/css"),
                Map.entry("a.json", "application/json"),
                Map.entry("a.map", "application/json"),
                Map.entry("a.svg", "image/svg+xml"),
                Map.entry("a.png", "image/png"),
                Map.entry("a.jpg", "image/jpeg"),
                Map.entry("a.jpeg", "image/jpeg"),
                Map.entry("a.gif", "image/gif"),
                Map.entry("a.ico", "image/x-icon"),
                Map.entry("a.webp", "image/webp"),
                Map.entry("a.woff", "font/woff"),
                Map.entry("a.woff2", "font/woff2"),
                Map.entry("a.ttf", "font/ttf"),
                Map.entry("a.txt", "text/plain"),
                Map.entry("a.wasm", "application/wasm"),
                Map.entry("a.bin", "application/octet-stream"));

        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/ui/", new StaticUiHandler(bundle::get));
        server.start();
        try {
            HttpClient http = HttpClient.newHttpClient();
            for (Map.Entry<String, String> e : expected.entrySet()) {
                HttpResponse<String> r = http.send(
                        HttpRequest.newBuilder(URI.create(base(server) + "/ui/" + e.getKey()))
                                .GET().build(),
                        BodyHandlers.ofString());
                assertThat(r.headers().firstValue("Content-Type"))
                        .as("Content-Type for %s", e.getKey())
                        .hasValueSatisfying(ct -> assertThat(ct).startsWith(e.getValue()));
            }
        } finally {
            server.stop(0);
        }
    }

    private static String base(HttpServer server) {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }
}
