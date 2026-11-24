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

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpPrincipal;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import me.predatorray.candybox.common.exception.ValidationException;
import org.junit.jupiter.api.Test;

class AdminApiServerErrorMappingTest {
    private AdminApiServer start(DashboardData data) {
        var server = new AdminApiServer(new AdminApiConfig(0, "127.0.0.1", "*", false, 4), () -> true, data);
        server.start();
        return server;
    }

    private HttpResponse<String> request(AdminApiServer server, String method, String path, String body)
            throws Exception {
        return HttpClient.newHttpClient().send(HttpRequest.newBuilder(
                        URI.create("http://127.0.0.1:" + server.port() + path))
                .timeout(Duration.ofSeconds(5))
                .method(method, HttpRequest.BodyPublishers.ofString(body)).build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private void error(HttpResponse<String> response, int status, String code, String message) {
        assertThat(response.statusCode()).isEqualTo(status);
        assertThat(response.headers().firstValue("Content-Type")).hasValueSatisfying(
                type -> assertThat(type).contains("application/json"));
        assertThat(response.body()).contains("\"error\":\"" + code + "\"", message);
    }

    @Test
    void mapsBackendValidationErrorsOnEveryWriteRoute() throws Exception {
        FakeDashboardData invalid = new FakeDashboardData() {
            @Override public void createBox(String name) { throw new ValidationException("invalid box name"); }
            @Override public void deleteBox(String name, boolean force) { throw new ValidationException("invalid delete box"); }
            @Override public void putCandy(String box, String key, byte[] body, String type) {
                throw new ValidationException("invalid put key");
            }
            @Override public void deleteCandy(String box, String key) { throw new ValidationException("invalid delete key"); }
        };
        try (var server = start(invalid)) {
            error(request(server, "POST", "/api/boxes", "{\"name\":\"Bad Name!\"}"),
                    400, "InvalidArgument", "invalid box name");
            error(request(server, "DELETE", "/api/boxes/x", ""), 400, "InvalidArgument", "invalid delete box");
            error(request(server, "PUT", "/api/boxes/x/objects/k", "x"), 400, "InvalidArgument", "invalid put key");
            error(request(server, "DELETE", "/api/boxes/x/objects/k", ""), 400, "InvalidArgument", "invalid delete key");
        }
    }

    @Test
    void mapsMissingBoxObjectDeleteToConflict() throws Exception {
        try (var server = start(new FakeDashboardData())) {
            error(request(server, "DELETE", "/api/boxes/missing/objects/k", ""),
                    409, "Conflict", "no such box: missing");
        }
    }

    @Test
    void rejectsOversizedCreateBodyAndUnsupportedMethodsBeforeBackendCalls() throws Exception {
        FakeDashboardData neverCalled = new FakeDashboardData() {
            @Override public void createBox(String name) { throw new AssertionError("backend called"); }
            @Override public void deleteBox(String name, boolean force) { throw new AssertionError("backend called"); }
            @Override public void putCandy(String box, String key, byte[] body, String type) {
                throw new AssertionError("backend called");
            }
        };
        try (var server = start(neverCalled)) {
            error(request(server, "POST", "/api/boxes", "{\"name\":\"" + "a".repeat(8192) + "\"}"),
                    413, "PayloadTooLarge", "exceeds limit");
            assertThat(request(server, "PATCH", "/api/boxes/x", "").statusCode()).isEqualTo(405);
            assertThat(request(server, "POST", "/api/boxes/x/objects/k", "").statusCode()).isEqualTo(405);
        }
    }

    @Test
    void skipsEmptyQueryPairsAndDoesNotInterpretBareForceAsTrue() throws Exception {
        AtomicReference<Boolean> forceSeen = new AtomicReference<>();
        var backend = new FakeDashboardData() {
            @Override public void deleteBox(String name, boolean force) {
                assertThat(name).isEqualTo("x");
                forceSeen.set(force);
            }
        };
        try (var server = start(backend)) {
            assertThat(request(server, "DELETE", "/api/boxes/x?&force=true&", "").statusCode()).isEqualTo(204);
            assertThat(forceSeen.get()).isTrue();
            forceSeen.set(null);
            assertThat(request(server, "DELETE", "/api/boxes/x?force", "").statusCode()).isEqualTo(204);
            assertThat(forceSeen.get()).isFalse();
        }
    }

    @Test
    void malformedContentLengthIsRejectedByJdkListenerBeforeDispatch() throws Exception {
        try (var server = start(new FakeDashboardData());
             var socket = new Socket("127.0.0.1", server.port())) {
            socket.setSoTimeout(5000);
            socket.getOutputStream().write(("PUT /api/boxes/x/objects/k HTTP/1.1\r\n"
                    + "Host: localhost\r\nContent-Length: abc\r\nConnection: close\r\n\r\nx")
                    .getBytes(StandardCharsets.US_ASCII));
            socket.getOutputStream().flush();
            assertThat(new String(socket.getInputStream().readAllBytes(), StandardCharsets.UTF_8))
                    .startsWith("HTTP/1.1 400");
        }
    }

    @Test
    void malformedContentLengthFallsBackToStreamingCapInsideUploadHandler() throws Exception {
        var data = new FakeDashboardData();
        data.createBox("sweets");
        try (var server = start(data)) {
            // The JDK listener rejects nonnumeric Content-Length before the application. Exercise
            // its defensive handler fallback with an in-memory exchange, without changing production visibility.
            var handler = AdminApiServer.class.getDeclaredMethod("handlePutCandy", HttpExchange.class, String.class, String.class);
            handler.setAccessible(true);
            var small = new BodyExchange("1234");
            handler.invoke(server, small, "sweets", "k");
            assertThat(small.getResponseCode()).isEqualTo(201);
            assertThat(small.output.toString(StandardCharsets.UTF_8)).contains("\"contentLength\":4");
            assertThat(data.candyDataOf("sweets", "k")).isEqualTo("1234".getBytes(StandardCharsets.UTF_8));
            var oversized = new BodyExchange("12345");
            handler.invoke(server, oversized, "sweets", "k");
            assertThat(oversized.getResponseCode()).isEqualTo(413);
            assertThat(oversized.output.toString(StandardCharsets.UTF_8)).contains("\"error\":\"PayloadTooLarge\"");
            assertThat(data.candyDataOf("sweets", "k")).isEqualTo("1234".getBytes(StandardCharsets.UTF_8));
        }
    }

    private static final class BodyExchange extends HttpExchange {
        private final Headers request = new Headers();
        private final Headers response = new Headers();
        private final InputStream input;
        private final ByteArrayOutputStream output = new ByteArrayOutputStream();
        private int status;
        BodyExchange(String body) {
            request.set("Content-Length", "abc");
            input = new ByteArrayInputStream(body.getBytes(StandardCharsets.UTF_8));
        }
        @Override public Headers getRequestHeaders() { return request; }
        @Override public Headers getResponseHeaders() { return response; }
        @Override public URI getRequestURI() { return URI.create("/api/boxes/sweets/objects/k"); }
        @Override public String getRequestMethod() { return "PUT"; }
        @Override public HttpContext getHttpContext() { return null; }
        @Override public void close() { }
        @Override public InputStream getRequestBody() { return input; }
        @Override public OutputStream getResponseBody() { return output; }
        @Override public void sendResponseHeaders(int code, long length) { status = code; }
        @Override public InetSocketAddress getRemoteAddress() { return new InetSocketAddress("127.0.0.1", 0); }
        @Override public int getResponseCode() { return status; }
        @Override public InetSocketAddress getLocalAddress() { return new InetSocketAddress("127.0.0.1", 0); }
        @Override public String getProtocol() { return "HTTP/1.1"; }
        @Override public Object getAttribute(String name) { return null; }
        @Override public void setAttribute(String name, Object value) { }
        @Override public void setStreams(InputStream in, OutputStream out) { throw new UnsupportedOperationException(); }
        @Override public HttpPrincipal getPrincipal() { return null; }
    }
}
