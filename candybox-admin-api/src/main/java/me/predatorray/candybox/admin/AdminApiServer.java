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

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;
import me.predatorray.candybox.common.exception.CandyboxException;
import me.predatorray.candybox.common.exception.ValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The admin/dashboard HTTP server. Built on the JDK's {@link HttpServer} so this module stays as
 * dependency-light as {@link me.predatorray.candybox.s3.GatewayHealthServer} — a small Netty
 * pipeline would be overkill for a few JSON routes plus a static-asset mount.
 *
 * <p>Route map (see {@code WEB_DASHBOARD_PLAN.md} for the full v1 contract):
 *
 * <ul>
 *   <li>{@code /healthz}, {@code /readyz} mirror the per-node endpoints so the same probes work.</li>
 *   <li>{@code /api/cluster} — JSON cluster snapshot from {@link DashboardData}.</li>
 *   <li>{@code /api/boxes}, {@code /api/boxes/{name}}, {@code /api/boxes/{name}/objects} — box +
 *       candy listings.</li>
 *   <li>{@code /ui/*} — the React SPA via {@link StaticUiHandler}.</li>
 *   <li>{@code /} → 302 redirect to {@code /ui/} (or {@code /api/cluster} when UI is disabled).</li>
 * </ul>
 *
 * <p>CORS is permissive by default ({@code Access-Control-Allow-Origin: *}) because v1 has no auth
 * — the deploy assumption is a trusted network, same as the rest of candybox.
 */
public final class AdminApiServer implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(AdminApiServer.class);
    /**
     * Shared, thread-safe Jackson mapper for request-body parsing. {@link ObjectMapper} is
     * documented as thread-safe after configuration, so reusing one instance avoids the per-call
     * allocation churn and keeps Jackson's internal caches warm across handlers.
     */
    private static final ObjectMapper JSON = new ObjectMapper();

    private final HttpServer http;
    private final AdminApiConfig config;
    private final DashboardData data;
    private final MetricsScraper metrics;
    private final String authToken;

    public AdminApiServer(AdminApiConfig config, BooleanSupplier ready, DashboardData data) {
        this(config, ready, data, null);
    }

    public AdminApiServer(AdminApiConfig config, BooleanSupplier ready, DashboardData data,
                          MetricsScraper metrics) {
        this(config, ready, data, metrics, null, null);
    }

    /**
     * @param authToken when non-null, every {@code /api/*} route demands
     *                  {@code Authorization: Bearer <token>} (health probes and the static SPA
     *                  stay open); the SPA stores the token after a one-time {@code /ui/?token=...}
     * @param ssl       when non-null, the listener serves HTTPS with this context (PEM via
     *                  {@code tls.*})
     */
    public AdminApiServer(AdminApiConfig config, BooleanSupplier ready, DashboardData data,
                          MetricsScraper metrics, String authToken,
                          javax.net.ssl.SSLContext ssl) {
        this.config = config;
        this.data = data;
        this.metrics = metrics;
        this.authToken = authToken;
        try {
            if (ssl != null) {
                com.sun.net.httpserver.HttpsServer https = com.sun.net.httpserver.HttpsServer
                        .create(new InetSocketAddress(config.bindAddress(), config.port()), 0);
                https.setHttpsConfigurator(new com.sun.net.httpserver.HttpsConfigurator(ssl));
                this.http = https;
            } else {
                this.http = HttpServer.create(
                        new InetSocketAddress(config.bindAddress(), config.port()), 0);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(
                    "Failed to bind admin API on " + config.bindAddress() + ":" + config.port(), e);
        }
        register("/healthz", exchange -> textRespond(exchange, 200, "ok\n"));
        register("/readyz", exchange -> {
            boolean ok = ready.getAsBoolean();
            textRespond(exchange, ok ? 200 : 503, ok ? "ready\n" : "not ready\n");
        });
        register("/api/cluster", this::handleCluster);
        register("/api/boxes", this::handleBoxes);
        register("/api/lsm", this::handleLsm);
        register("/api/metrics", this::handleMetricsText);
        register("/api/metrics/timeseries", this::handleMetricsTimeseries);
        // Single context for all box-scoped routes — the JDK HttpServer's prefix matcher is too
        // coarse to express {name} segments, so we route them ourselves in handleBoxRoute.
        if (config.uiEnabled()) {
            register("/ui", new StaticUiHandler());
            register("/ui/", new StaticUiHandler());
        }
        register("/", this::handleRoot);
        http.setExecutor(null);
    }

    private void register(String path, HttpHandler handler) {
        http.createContext(path, withCors(withAuth(path, handler)));
    }

    /** The bearer-token guard on {@code /api/*}; probes and the static SPA shell stay open. */
    private HttpHandler withAuth(String path, HttpHandler delegate) {
        if (authToken == null || !path.startsWith("/api")) {
            return delegate;
        }
        return exchange -> {
            String header = exchange.getRequestHeaders().getFirst("Authorization");
            String presented = header != null && header.startsWith("Bearer ")
                    ? header.substring("Bearer ".length()).trim() : null;
            if (presented == null || !java.security.MessageDigest.isEqual(
                    presented.getBytes(java.nio.charset.StandardCharsets.UTF_8),
                    authToken.getBytes(java.nio.charset.StandardCharsets.UTF_8))) {
                jsonRespond(exchange, 401, JsonWriter.write(Map.of(
                        "error", "Unauthorized",
                        "message", "This admin API requires Authorization: Bearer <token>")));
                return;
            }
            delegate.handle(exchange);
        };
    }

    public void start() {
        http.start();
        LOG.info("Admin API listening on {}", http.getAddress());
    }

    public int port() {
        return http.getAddress().getPort();
    }

    @Override
    public void close() {
        http.stop(0);
    }

    // ---- handlers ------------------------------------------------------------------------------

    private void handleCluster(HttpExchange exchange) throws IOException {
        if (!"GET".equals(exchange.getRequestMethod())) {
            textRespond(exchange, 405, "method not allowed\n");
            return;
        }
        DashboardData.ClusterSnapshot s = data.cluster();
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("nodes", nodesToJson(s.nodes()));
        body.put("boxCount", s.boxCount());
        body.put("ownerless", s.ownerless());
        body.put("stub", s.stub());
        jsonRespond(exchange, 200, JsonWriter.write(body));
    }

    private void handleBoxes(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        String method = exchange.getRequestMethod();
        // /api/boxes                          → list (GET) / create (POST)
        // /api/boxes/{name}                   → detail (GET) / delete-box (DELETE)
        // /api/boxes/{name}/objects           → candy listing (GET)
        // /api/boxes/{name}/objects/{key…}    → put-candy (PUT) / delete-candy (DELETE)
        if ("/api/boxes".equals(path) || "/api/boxes/".equals(path)) {
            switch (method) {
                case "GET" -> handleBoxList(exchange);
                case "POST" -> handleCreateBox(exchange);
                default -> textRespond(exchange, 405, "method not allowed\n");
            }
            return;
        }
        String tail = path.substring("/api/boxes/".length());
        int slash = tail.indexOf('/');
        String name = slash < 0 ? tail : tail.substring(0, slash);
        String rest = slash < 0 ? "" : tail.substring(slash + 1);
        name = URLDecoder.decode(name, StandardCharsets.UTF_8);
        if (rest.isEmpty()) {
            switch (method) {
                case "GET" -> handleBoxDetail(exchange, name);
                case "DELETE" -> handleDeleteBox(exchange, name);
                default -> textRespond(exchange, 405, "method not allowed\n");
            }
            return;
        }
        if ("objects".equals(rest) || "objects/".equals(rest)) {
            handleBoxObjects(exchange, name);
            return;
        }
        if (rest.startsWith("objects/")) {
            String rawKey = rest.substring("objects/".length());
            // The key may legitimately contain '/' (S3-style "folder/foo.txt") — the routing rule
            // is "everything past objects/ is the key, decoded once". A trailing slash would mean
            // the key ends with '/', which the server treats as a different key; preserve it.
            if (rawKey.isEmpty()) {
                textRespond(exchange, 404, "key required\n");
                return;
            }
            String key = URLDecoder.decode(rawKey, StandardCharsets.UTF_8);
            switch (method) {
                case "PUT" -> handlePutCandy(exchange, name, key);
                case "DELETE" -> handleDeleteCandy(exchange, name, key);
                default -> textRespond(exchange, 405, "method not allowed\n");
            }
            return;
        }
        textRespond(exchange, 404, "not found\n");
    }

    private void handleCreateBox(HttpExchange exchange) throws IOException {
        String name;
        byte[] body;
        try {
            body = readBoundedBody(exchange, 8 * 1024);
        } catch (PayloadTooLargeException e) {
            // Hitting 8 KiB for "create one box" is almost certainly garbage on the wire — treat as
            // a 413 instead of trying to parse a megabyte of JSON.
            jsonError(exchange, 413, "PayloadTooLarge", e.getMessage());
            return;
        }
        try {
            JsonNode root = JSON.readTree(body);
            if (root == null || !root.isObject()) {
                // {@code readTree} returns a {@code MissingNode} for an empty buffer rather than
                // throwing; surface both that and any "[…]" array body as the same 400 so the
                // dashboard can render one error message.
                jsonError(exchange, 400, "InvalidJson", "expected JSON object");
                return;
            }
            JsonNode n = root.get("name");
            if (n == null || !n.isTextual() || n.asText().isEmpty()) {
                jsonError(exchange, 400, "InvalidArgument", "missing string field 'name'");
                return;
            }
            name = n.asText();
        } catch (JacksonException e) {
            jsonError(exchange, 400, "InvalidJson", e.getOriginalMessage());
            return;
        }
        try {
            data.createBox(name);
        } catch (UnsupportedOperationException e) {
            jsonError(exchange, 503, "NotConfigured", e.getMessage());
            return;
        } catch (ValidationException e) {
            jsonError(exchange, 400, "InvalidArgument", e.getMessage());
            return;
        } catch (CandyboxException e) {
            // Most "box already exists" / quorum errors land here; surfacing as 409 lets the UI
            // distinguish a benign duplicate from a 500 it should retry.
            jsonError(exchange, 409, "Conflict", e.getMessage());
            return;
        }
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("name", name);
        jsonRespond(exchange, 201, JsonWriter.write(resp));
    }

    private void handleDeleteBox(HttpExchange exchange, String name) throws IOException {
        Map<String, String> q = parseQuery(exchange.getRequestURI());
        boolean force = "true".equalsIgnoreCase(q.getOrDefault("force", "false"));
        try {
            data.deleteBox(name, force);
        } catch (UnsupportedOperationException e) {
            jsonError(exchange, 503, "NotConfigured", e.getMessage());
            return;
        } catch (ValidationException e) {
            jsonError(exchange, 400, "InvalidArgument", e.getMessage());
            return;
        } catch (CandyboxException e) {
            // No-owner / non-empty-without-force / cross-cluster races land here. 409 keeps a
            // failed delete distinguishable from a 5xx on the UI side.
            jsonError(exchange, 409, "Conflict", e.getMessage());
            return;
        }
        exchange.sendResponseHeaders(204, -1);
        exchange.close();
    }

    private void handlePutCandy(HttpExchange exchange, String box, String key) throws IOException {
        byte[] body;
        try {
            body = readBoundedBody(exchange, config.maxUploadBytes());
        } catch (PayloadTooLargeException e) {
            jsonError(exchange, 413, "PayloadTooLarge", e.getMessage());
            return;
        }
        String contentType = exchange.getRequestHeaders().getFirst("Content-Type");
        if (contentType != null) {
            // Strip any "; charset=..." / "; boundary=..." parameters before handing to the
            // storage layer — the LSM treats Content-Type as an opaque string but operators
            // generally don't want the parameter noise in HEAD output.
            int semi = contentType.indexOf(';');
            if (semi >= 0) {
                contentType = contentType.substring(0, semi).trim();
            }
            if (contentType.isEmpty()) {
                contentType = null;
            }
        }
        try {
            data.putCandy(box, key, body, contentType);
        } catch (UnsupportedOperationException e) {
            jsonError(exchange, 503, "NotConfigured", e.getMessage());
            return;
        } catch (ValidationException e) {
            jsonError(exchange, 400, "InvalidArgument", e.getMessage());
            return;
        } catch (CandyboxException e) {
            jsonError(exchange, 409, "Conflict", e.getMessage());
            return;
        }
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("key", key);
        resp.put("contentLength", body.length);
        jsonRespond(exchange, 201, JsonWriter.write(resp));
    }

    private void handleDeleteCandy(HttpExchange exchange, String box, String key) throws IOException {
        try {
            data.deleteCandy(box, key);
        } catch (UnsupportedOperationException e) {
            jsonError(exchange, 503, "NotConfigured", e.getMessage());
            return;
        } catch (ValidationException e) {
            jsonError(exchange, 400, "InvalidArgument", e.getMessage());
            return;
        } catch (CandyboxException e) {
            jsonError(exchange, 409, "Conflict", e.getMessage());
            return;
        }
        exchange.sendResponseHeaders(204, -1);
        exchange.close();
    }

    private void handleBoxList(HttpExchange exchange) throws IOException {
        if (!"GET".equals(exchange.getRequestMethod())) {
            textRespond(exchange, 405, "method not allowed\n");
            return;
        }
        List<Map<String, Object>> rows = new ArrayList<>();
        for (DashboardData.BoxSummary b : data.boxes()) {
            rows.add(boxToJson(b));
        }
        jsonRespond(exchange, 200, JsonWriter.write(Map.of("boxes", rows)));
    }

    private void handleBoxDetail(HttpExchange exchange, String name) throws IOException {
        if (!"GET".equals(exchange.getRequestMethod())) {
            textRespond(exchange, 405, "method not allowed\n");
            return;
        }
        DashboardData.BoxSummary b = data.box(name);
        if (b == null) {
            textRespond(exchange, 404, "no such box\n");
            return;
        }
        jsonRespond(exchange, 200, JsonWriter.write(boxToJson(b)));
    }

    private void handleBoxObjects(HttpExchange exchange, String name) throws IOException {
        if (!"GET".equals(exchange.getRequestMethod())) {
            textRespond(exchange, 405, "method not allowed\n");
            return;
        }
        Map<String, String> q = parseQuery(exchange.getRequestURI());
        String prefix = q.get("prefix");
        String startAfter = q.get("startAfter");
        int max = parseInt(q.get("max"), 100);
        DashboardData.CandyListing listing = data.candies(name, prefix, startAfter, max);
        List<Map<String, Object>> entries = new ArrayList<>(listing.entries().size());
        for (DashboardData.CandyRow r : listing.entries()) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("key", r.key());
            row.put("contentLength", r.contentLength());
            row.put("createdAtMillis", r.createdAtMillis());
            entries.add(row);
        }
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("entries", entries);
        body.put("nextStartAfter", listing.nextStartAfter());
        jsonRespond(exchange, 200, JsonWriter.write(body));
    }

    private void handleLsm(HttpExchange exchange) throws IOException {
        if (!"GET".equals(exchange.getRequestMethod())) {
            textRespond(exchange, 405, "method not allowed\n");
            return;
        }
        List<Map<String, Object>> rows = new ArrayList<>();
        for (DashboardData.LsmRow r : data.lsm()) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("box", r.box());
            row.put("owner", r.owner());
            row.put("manifestVersion", nullIfNegative(r.manifestVersion()));
            row.put("fencingToken", nullIfNegative(r.fencingToken()));
            row.put("sstableLedgerCount", nullIfNegative(r.sstableLedgerCount()));
            row.put("syrupLedgerCount", nullIfNegative(r.syrupLedgerCount()));
            row.put("walLedgerCount", nullIfNegative(r.walLedgerCount()));
            row.put("inFlightCompactions", nullIfNegative(r.inFlightCompactions()));
            row.put("gcBacklog", nullIfNegative(r.gcBacklog()));
            rows.add(row);
        }
        jsonRespond(exchange, 200, JsonWriter.write(Map.of("boxes", rows)));
    }

    private static Object nullIfNegative(long v) {
        return v < 0 ? null : v;
    }

    private void handleMetricsText(HttpExchange exchange) throws IOException {
        // /api/metrics/timeseries also starts with /api/metrics — the prefix matcher would route
        // it here; bail to let the more specific handler take it.
        String path = exchange.getRequestURI().getPath();
        if (path.startsWith("/api/metrics/timeseries")) {
            handleMetricsTimeseries(exchange);
            return;
        }
        if (!"GET".equals(exchange.getRequestMethod())) {
            textRespond(exchange, 405, "method not allowed\n");
            return;
        }
        String body = metrics == null ? "# scraping disabled\n" : metrics.latestText();
        respond(exchange, 200, "text/plain; version=0.0.4; charset=utf-8",
                body.getBytes(StandardCharsets.UTF_8));
    }

    private void handleMetricsTimeseries(HttpExchange exchange) throws IOException {
        if (!"GET".equals(exchange.getRequestMethod())) {
            textRespond(exchange, 405, "method not allowed\n");
            return;
        }
        Map<String, String> q = parseQuery(exchange.getRequestURI());
        String namesRaw = q.getOrDefault("names", "");
        List<String> names = new ArrayList<>();
        for (String n : namesRaw.split(",")) {
            String trimmed = n.trim();
            if (!trimmed.isEmpty()) {
                names.add(trimmed);
            }
        }
        List<Map<String, Object>> seriesOut = new ArrayList<>();
        int windowSeconds = 0;
        if (metrics != null) {
            windowSeconds = metrics.windowSeconds();
            Map<String, List<MetricsScraper.Series>> matches = metrics.seriesFor(names);
            for (Map.Entry<String, List<MetricsScraper.Series>> e : matches.entrySet()) {
                for (MetricsScraper.Series s : e.getValue()) {
                    Map<String, Object> row = new LinkedHashMap<>();
                    row.put("name", s.name());
                    if (!s.labels().isEmpty()) {
                        row.put("labels", new LinkedHashMap<>(s.labels()));
                    }
                    List<Map<String, Object>> samples = new ArrayList<>(s.samples().size());
                    for (MetricsScraper.Sample sample : s.samples()) {
                        Map<String, Object> point = new LinkedHashMap<>();
                        point.put("t", sample.t());
                        point.put("v", sample.v());
                        samples.add(point);
                    }
                    row.put("samples", samples);
                    seriesOut.add(row);
                }
            }
        }
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("series", seriesOut);
        body.put("windowSeconds", windowSeconds);
        jsonRespond(exchange, 200, JsonWriter.write(body));
    }

    private void handleRoot(HttpExchange exchange) throws IOException {
        if (!"/".equals(exchange.getRequestURI().getPath())) {
            textRespond(exchange, 404, "not found\n");
            return;
        }
        exchange.getResponseHeaders().set("Location", config.uiEnabled() ? "/ui/" : "/api/cluster");
        exchange.sendResponseHeaders(302, -1);
        exchange.close();
    }

    // ---- JSON shaping --------------------------------------------------------------------------

    private static List<Map<String, Object>> nodesToJson(List<DashboardData.NodeInfo> nodes) {
        List<Map<String, Object>> out = new ArrayList<>(nodes.size());
        for (DashboardData.NodeInfo n : nodes) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("nodeId", n.nodeId());
            row.put("address", n.address());
            row.put("ready", n.ready());
            row.put("ownedBoxCount", n.ownedBoxCount());
            out.add(row);
        }
        return out;
    }

    private static Map<String, Object> boxToJson(DashboardData.BoxSummary b) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("name", b.name());
        row.put("owner", b.owner());
        // Optional fields encoded as -1 / null in the record become null in JSON so the UI can
        // render an em-dash without special-casing.
        row.put("candyCount", b.candyCount() < 0 ? null : b.candyCount());
        row.put("sizeBytes", b.sizeBytes() < 0 ? null : b.sizeBytes());
        row.put("manifestVersion", b.manifestVersion() < 0 ? null : b.manifestVersion());
        row.put("fencingToken", b.fencingToken() < 0 ? null : b.fencingToken());
        row.put("hlc", b.hlc());
        return row;
    }

    // ---- CORS / IO helpers ---------------------------------------------------------------------

    private HttpHandler withCors(HttpHandler delegate) {
        return exchange -> {
            applyCorsHeaders(exchange.getResponseHeaders());
            if ("OPTIONS".equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(204, -1);
                exchange.close();
                return;
            }
            try {
                delegate.handle(exchange);
            } catch (RuntimeException e) {
                // The default executor would otherwise swallow this and log a stack trace without
                // ever responding. Surface a 500 with the message so curl/the SPA gets feedback,
                // and log the trace at debug level so an operator can correlate.
                LOG.warn("admin api handler failed: {}", e.toString(), e);
                jsonRespond(exchange, 500,
                        JsonWriter.write(Map.of("error", e.getClass().getSimpleName(),
                                "message", e.getMessage() == null ? "" : e.getMessage())));
            }
        };
    }

    private void applyCorsHeaders(Headers headers) {
        if (config.corsAllowOrigin().isEmpty()) {
            return;
        }
        headers.set("Access-Control-Allow-Origin", config.corsAllowOrigin());
        headers.set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS");
        headers.set("Access-Control-Allow-Headers", "Content-Type, Authorization, If-Match, If-None-Match");
        headers.set("Access-Control-Max-Age", "600");
    }

    private static Map<String, String> parseQuery(URI uri) {
        String raw = uri.getRawQuery();
        if (raw == null || raw.isEmpty()) {
            return Map.of();
        }
        Map<String, String> out = new LinkedHashMap<>();
        for (String pair : raw.split("&")) {
            if (pair.isEmpty()) {
                continue;
            }
            int eq = pair.indexOf('=');
            String k = URLDecoder.decode(eq < 0 ? pair : pair.substring(0, eq), StandardCharsets.UTF_8);
            String v = eq < 0 ? "" : URLDecoder.decode(pair.substring(eq + 1), StandardCharsets.UTF_8);
            out.put(k, v);
        }
        return out;
    }

    private static int parseInt(String s, int fallback) {
        if (s == null || s.isBlank()) {
            return fallback;
        }
        try {
            return Integer.parseInt(s.trim());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    /**
     * Reads the request body fully but caps it at {@code limit} bytes so an unbounded upload can't
     * blow the heap of the dashboard process. Returns the bytes; throws
     * {@link PayloadTooLargeException} when the body would exceed the cap, before allocating it
     * fully. The {@code Content-Length} header is checked first as an early-reject path so well-
     * behaved clients fail fast without streaming.
     */
    private static byte[] readBoundedBody(HttpExchange exchange, long limit)
            throws IOException, PayloadTooLargeException {
        String declared = exchange.getRequestHeaders().getFirst("Content-Length");
        if (declared != null) {
            try {
                long parsed = Long.parseLong(declared.trim());
                if (parsed > limit) {
                    throw new PayloadTooLargeException(
                            "Content-Length " + parsed + " exceeds limit " + limit);
                }
            } catch (NumberFormatException ignored) {
                // Header malformed — fall through to the stream cap; we'll catch oversize there.
            }
        }
        try (InputStream in = exchange.getRequestBody();
             ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            byte[] buf = new byte[8 * 1024];
            long total = 0;
            int n;
            while ((n = in.read(buf)) > 0) {
                total += n;
                if (total > limit) {
                    throw new PayloadTooLargeException(
                            "request body exceeds limit " + limit + " bytes");
                }
                out.write(buf, 0, n);
            }
            return out.toByteArray();
        }
    }

    /** Internal signal that the request body topped the configured upload cap. */
    private static final class PayloadTooLargeException extends Exception {
        PayloadTooLargeException(String message) {
            super(message);
        }
    }

    private static void jsonError(HttpExchange exchange, int status, String code, String message)
            throws IOException {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("error", code);
        body.put("message", message == null ? "" : message);
        jsonRespond(exchange, status, JsonWriter.write(body));
    }

    private static void textRespond(HttpExchange exchange, int status, String body) throws IOException {
        respond(exchange, status, "text/plain; charset=utf-8",
                body.getBytes(StandardCharsets.UTF_8));
    }

    private static void jsonRespond(HttpExchange exchange, int status, String body) throws IOException {
        respond(exchange, status, "application/json; charset=utf-8",
                body.getBytes(StandardCharsets.UTF_8));
    }

    private static void respond(HttpExchange exchange, int status, String contentType, byte[] body)
            throws IOException {
        exchange.getResponseHeaders().set("Content-Type", contentType);
        exchange.sendResponseHeaders(status, body.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
        }
    }
}
