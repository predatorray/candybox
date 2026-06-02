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
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class AdminApiServerTest {

    private static AdminApiServer start(DashboardData data, AtomicBoolean ready, boolean uiEnabled) {
        AdminApiConfig config = new AdminApiConfig(0, "127.0.0.1", "*", uiEnabled);
        AdminApiServer server = new AdminApiServer(config, ready::get, data);
        server.start();
        return server;
    }

    @Test
    void healthReadinessAndCors() throws Exception {
        AtomicBoolean ready = new AtomicBoolean(true);
        try (AdminApiServer server = start(new EmptyDashboardData(), ready, true)) {
            HttpClient http = HttpClient.newHttpClient();
            String base = "http://127.0.0.1:" + server.port();

            assertThat(get(http, base + "/healthz").statusCode()).isEqualTo(200);
            assertThat(get(http, base + "/readyz").statusCode()).isEqualTo(200);
            ready.set(false);
            assertThat(get(http, base + "/readyz").statusCode()).isEqualTo(503);

            HttpResponse<String> cluster = get(http, base + "/api/cluster");
            assertThat(cluster.statusCode()).isEqualTo(200);
            assertThat(cluster.headers().firstValue("Content-Type"))
                    .hasValueSatisfying(v -> assertThat(v).contains("application/json"));
            assertThat(cluster.body()).contains("\"nodes\":[]").contains("\"stub\":true");
            assertThat(cluster.headers().firstValue("Access-Control-Allow-Origin")).hasValue("*");

            HttpResponse<String> preflight = http.send(
                    HttpRequest.newBuilder(URI.create(base + "/api/cluster"))
                            .method("OPTIONS", HttpRequest.BodyPublishers.noBody())
                            .build(),
                    BodyHandlers.ofString());
            assertThat(preflight.statusCode()).isEqualTo(204);
            assertThat(preflight.headers().firstValue("Access-Control-Allow-Methods")).isPresent();
        }
    }

    @Test
    void clusterAndBoxesReadFromDataSource() throws Exception {
        FakeDashboardData data = new FakeDashboardData()
                .withNode(new DashboardData.NodeInfo("1", "host-a:9709", true, 2))
                .withNode(new DashboardData.NodeInfo("2", "host-b:9709", true, 1))
                .withBox(DashboardData.BoxSummary.minimal("photos", "1"))
                .withBox(DashboardData.BoxSummary.minimal("docs", "1"))
                .withBox(DashboardData.BoxSummary.minimal("backups", "2"))
                .withOwnerless("orphan")
                .withCandy("photos", new DashboardData.CandyRow("cat.jpg", 4096, 100))
                .withCandy("photos", new DashboardData.CandyRow("dog.jpg", 8192, 200));
        try (AdminApiServer server = start(data, new AtomicBoolean(true), true)) {
            HttpClient http = HttpClient.newHttpClient();
            String base = "http://127.0.0.1:" + server.port();

            HttpResponse<String> cluster = get(http, base + "/api/cluster");
            assertThat(cluster.statusCode()).isEqualTo(200);
            assertThat(cluster.body()).contains("\"host-a:9709\"").contains("\"host-b:9709\"")
                    .contains("\"boxCount\":3").contains("\"ownerless\":[\"orphan\"]")
                    .contains("\"stub\":false");

            HttpResponse<String> boxes = get(http, base + "/api/boxes");
            assertThat(boxes.statusCode()).isEqualTo(200);
            assertThat(boxes.body()).contains("\"photos\"").contains("\"docs\"").contains("\"backups\"");

            HttpResponse<String> photo = get(http, base + "/api/boxes/photos");
            assertThat(photo.statusCode()).isEqualTo(200);
            assertThat(photo.body()).contains("\"name\":\"photos\"").contains("\"owner\":\"1\"");

            HttpResponse<String> objects = get(http, base + "/api/boxes/photos/objects");
            assertThat(objects.statusCode()).isEqualTo(200);
            assertThat(objects.body()).contains("\"cat.jpg\"").contains("\"dog.jpg\"")
                    .contains("\"contentLength\":4096");

            HttpResponse<String> prefixed = get(http,
                    base + "/api/boxes/photos/objects?prefix=cat");
            assertThat(prefixed.body()).contains("\"cat.jpg\"").doesNotContain("\"dog.jpg\"");

            HttpResponse<String> notFound = get(http, base + "/api/boxes/missing");
            assertThat(notFound.statusCode()).isEqualTo(404);
        }
    }

    @Test
    void lsmEndpointEncodesUnknownsAsNull() throws Exception {
        FakeDashboardData data = new FakeDashboardData()
                .withLsmRow(DashboardData.LsmRow.coordinationOnly("photos", "1", 7, 42))
                .withLsmRow(new DashboardData.LsmRow("docs", "2", 3, 12, 4, 8, 1, 0, 0));
        try (AdminApiServer server = start(data, new AtomicBoolean(true), false)) {
            HttpResponse<String> r = get(HttpClient.newHttpClient(),
                    "http://127.0.0.1:" + server.port() + "/api/lsm");
            assertThat(r.statusCode()).isEqualTo(200);
            // photos row exposes only coordination-derived fields; runtime fields land as null.
            assertThat(r.body()).contains("\"box\":\"photos\"").contains("\"manifestVersion\":7")
                    .contains("\"fencingToken\":42").contains("\"sstableLedgerCount\":null");
            // docs row has runtime fields too — those serialize as numbers, not null.
            assertThat(r.body()).contains("\"box\":\"docs\"").contains("\"sstableLedgerCount\":4")
                    .contains("\"syrupLedgerCount\":8").contains("\"walLedgerCount\":1");
        }
    }

    @Test
    void uiAndRootBehaviour() throws Exception {
        try (AdminApiServer ui = start(new EmptyDashboardData(), new AtomicBoolean(true), true)) {
            HttpClient noFollow = HttpClient.newBuilder()
                    .followRedirects(HttpClient.Redirect.NEVER).build();
            HttpResponse<String> root = noFollow.send(
                    HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + ui.port() + "/"))
                            .GET().build(),
                    BodyHandlers.ofString());
            assertThat(root.statusCode()).isEqualTo(302);
            assertThat(root.headers().firstValue("Location")).hasValue("/ui/");

            HttpResponse<String> placeholder = get(HttpClient.newHttpClient(),
                    "http://127.0.0.1:" + ui.port() + "/ui/");
            assertThat(placeholder.statusCode()).isEqualTo(200);
            assertThat(placeholder.body()).containsIgnoringCase("UI bundle not built");
        }

        try (AdminApiServer headless = start(new EmptyDashboardData(), new AtomicBoolean(true), false)) {
            HttpClient noFollow = HttpClient.newBuilder()
                    .followRedirects(HttpClient.Redirect.NEVER).build();
            HttpResponse<String> root = noFollow.send(
                    HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + headless.port() + "/"))
                            .GET().build(),
                    BodyHandlers.ofString());
            assertThat(root.statusCode()).isEqualTo(302);
            assertThat(root.headers().firstValue("Location")).hasValue("/api/cluster");
        }
    }

    @Test
    void serverErrorsAreSerializedAsJson() throws Exception {
        DashboardData boom = new DashboardData() {
            @Override
            public ClusterSnapshot cluster() {
                throw new IllegalStateException("simulated outage");
            }

            @Override
            public java.util.List<BoxSummary> boxes() {
                return java.util.List.of();
            }

            @Override
            public BoxSummary box(String name) {
                return null;
            }

            @Override
            public CandyListing candies(String name, String prefix, String startAfter, int maxKeys) {
                return new CandyListing(java.util.List.of(), null);
            }

            @Override
            public java.util.List<LsmRow> lsm() {
                return java.util.List.of();
            }
        };
        try (AdminApiServer server = start(boom, new AtomicBoolean(true), false)) {
            HttpResponse<String> r = get(HttpClient.newHttpClient(),
                    "http://127.0.0.1:" + server.port() + "/api/cluster");
            assertThat(r.statusCode()).isEqualTo(500);
            assertThat(r.body()).contains("simulated outage").contains("IllegalStateException");
        }
    }

    @Test
    void createAndDeleteBoxRoundTrip() throws Exception {
        FakeDashboardData data = new FakeDashboardData();
        try (AdminApiServer server = start(data, new AtomicBoolean(true), false)) {
            HttpClient http = HttpClient.newHttpClient();
            String base = "http://127.0.0.1:" + server.port();

            HttpResponse<String> created = http.send(
                    HttpRequest.newBuilder(URI.create(base + "/api/boxes"))
                            .header("Content-Type", "application/json")
                            .POST(HttpRequest.BodyPublishers.ofString("{\"name\":\"sweets\"}"))
                            .build(),
                    BodyHandlers.ofString());
            assertThat(created.statusCode()).isEqualTo(201);
            assertThat(created.body()).contains("\"name\":\"sweets\"");

            HttpResponse<String> listed = get(http, base + "/api/boxes");
            assertThat(listed.body()).contains("\"sweets\"");

            // Re-creating the same name is a conflict, surfaced as 409 (not 500) so the UI can
            // distinguish a benign duplicate from a server failure.
            HttpResponse<String> dup = http.send(
                    HttpRequest.newBuilder(URI.create(base + "/api/boxes"))
                            .header("Content-Type", "application/json")
                            .POST(HttpRequest.BodyPublishers.ofString("{\"name\":\"sweets\"}"))
                            .build(),
                    BodyHandlers.ofString());
            assertThat(dup.statusCode()).isEqualTo(409);
            assertThat(dup.body()).contains("\"error\":\"Conflict\"");

            HttpResponse<String> deleted = http.send(
                    HttpRequest.newBuilder(URI.create(base + "/api/boxes/sweets"))
                            .DELETE().build(),
                    BodyHandlers.ofString());
            assertThat(deleted.statusCode()).isEqualTo(204);

            HttpResponse<String> gone = get(http, base + "/api/boxes/sweets");
            assertThat(gone.statusCode()).isEqualTo(404);
        }
    }

    @Test
    void createBoxRejectsBadJsonAndMissingName() throws Exception {
        try (AdminApiServer server = start(new FakeDashboardData(), new AtomicBoolean(true), false)) {
            HttpClient http = HttpClient.newHttpClient();
            String base = "http://127.0.0.1:" + server.port();

            HttpResponse<String> badJson = http.send(
                    HttpRequest.newBuilder(URI.create(base + "/api/boxes"))
                            .header("Content-Type", "application/json")
                            .POST(HttpRequest.BodyPublishers.ofString("{not json"))
                            .build(),
                    BodyHandlers.ofString());
            assertThat(badJson.statusCode()).isEqualTo(400);
            assertThat(badJson.body()).contains("\"error\":\"InvalidJson\"");

            HttpResponse<String> noName = http.send(
                    HttpRequest.newBuilder(URI.create(base + "/api/boxes"))
                            .header("Content-Type", "application/json")
                            .POST(HttpRequest.BodyPublishers.ofString("{\"other\":\"x\"}"))
                            .build(),
                    BodyHandlers.ofString());
            assertThat(noName.statusCode()).isEqualTo(400);
            assertThat(noName.body()).contains("missing string field 'name'");

            // PATCH (or any unsupported method) on /api/boxes should be a 405, not a 404 — fail
            // verbs explicitly so the operator sees what was wrong.
            HttpResponse<String> badMethod = http.send(
                    HttpRequest.newBuilder(URI.create(base + "/api/boxes"))
                            .method("PATCH", HttpRequest.BodyPublishers.noBody()).build(),
                    BodyHandlers.ofString());
            assertThat(badMethod.statusCode()).isEqualTo(405);
        }
    }

    @Test
    void putAndDeleteCandyRoundTrip() throws Exception {
        FakeDashboardData data = new FakeDashboardData()
                .withBox(DashboardData.BoxSummary.minimal("photos", "1"));
        try (AdminApiServer server = start(data, new AtomicBoolean(true), false)) {
            HttpClient http = HttpClient.newHttpClient();
            String base = "http://127.0.0.1:" + server.port();
            byte[] payload = "hello candybox".getBytes(StandardCharsets.UTF_8);

            HttpResponse<String> uploaded = http.send(
                    HttpRequest.newBuilder(URI.create(base + "/api/boxes/photos/objects/cat.jpg"))
                            .header("Content-Type", "image/jpeg; charset=binary")
                            .PUT(HttpRequest.BodyPublishers.ofByteArray(payload))
                            .build(),
                    BodyHandlers.ofString());
            assertThat(uploaded.statusCode()).isEqualTo(201);
            assertThat(uploaded.body()).contains("\"key\":\"cat.jpg\"")
                    .contains("\"contentLength\":" + payload.length);
            assertThat(data.candyDataOf("photos", "cat.jpg")).containsExactly(payload);

            // Keys with slashes survive the routing — the v1 dashboard treats S3-style prefixes
            // ("folder/file.txt") as opaque keys, so PUT through with the slash unescaped.
            HttpResponse<String> nested = http.send(
                    HttpRequest.newBuilder(URI.create(base + "/api/boxes/photos/objects/album/spring.jpg"))
                            .PUT(HttpRequest.BodyPublishers.ofByteArray(new byte[] {1, 2, 3}))
                            .build(),
                    BodyHandlers.ofString());
            assertThat(nested.statusCode()).isEqualTo(201);
            assertThat(data.candyDataOf("photos", "album/spring.jpg")).containsExactly(1, 2, 3);

            HttpResponse<String> listing = get(http, base + "/api/boxes/photos/objects");
            assertThat(listing.body()).contains("\"cat.jpg\"").contains("\"album/spring.jpg\"");

            HttpResponse<String> removed = http.send(
                    HttpRequest.newBuilder(URI.create(base + "/api/boxes/photos/objects/cat.jpg"))
                            .DELETE().build(),
                    BodyHandlers.ofString());
            assertThat(removed.statusCode()).isEqualTo(204);
            assertThat(data.candyDataOf("photos", "cat.jpg")).isNull();
        }
    }

    @Test
    void uploadBeyondCapReturns413() throws Exception {
        FakeDashboardData data = new FakeDashboardData()
                .withBox(DashboardData.BoxSummary.minimal("photos", "1"));
        AdminApiConfig small = new AdminApiConfig(0, "127.0.0.1", "*", false, 4);
        try (AdminApiServer server = new AdminApiServer(small, () -> true, data)) {
            server.start();
            HttpClient http = HttpClient.newHttpClient();
            HttpResponse<String> r = http.send(
                    HttpRequest.newBuilder(URI.create(
                                    "http://127.0.0.1:" + server.port() + "/api/boxes/photos/objects/big"))
                            .PUT(HttpRequest.BodyPublishers.ofByteArray(new byte[] {1, 2, 3, 4, 5}))
                            .build(),
                    BodyHandlers.ofString());
            assertThat(r.statusCode()).isEqualTo(413);
            assertThat(r.body()).contains("\"error\":\"PayloadTooLarge\"");
            assertThat(data.candyDataOf("photos", "big")).isNull();
        }
    }

    @Test
    void mutatingOpsReturn503WhenBackendIsStub() throws Exception {
        try (AdminApiServer server = start(new EmptyDashboardData(), new AtomicBoolean(true), false)) {
            HttpClient http = HttpClient.newHttpClient();
            String base = "http://127.0.0.1:" + server.port();
            HttpResponse<String> r = http.send(
                    HttpRequest.newBuilder(URI.create(base + "/api/boxes"))
                            .header("Content-Type", "application/json")
                            .POST(HttpRequest.BodyPublishers.ofString("{\"name\":\"x\"}"))
                            .build(),
                    BodyHandlers.ofString());
            assertThat(r.statusCode()).isEqualTo(503);
            assertThat(r.body()).contains("\"error\":\"NotConfigured\"");
        }
    }

    private static HttpResponse<String> get(HttpClient http, String url) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create(url)).GET().build(),
                BodyHandlers.ofString());
    }
}
