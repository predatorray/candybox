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
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Drives the real Netty {@link S3GatewayServer} over a loopback socket with an in-memory
 * {@link FakeCandyStore}. Unlike {@link S3HandlerTest} (which feeds the handler via EmbeddedChannel),
 * this exercises the full pipeline — {@code HttpServerCodec} + {@code HttpObjectAggregator} + the
 * blocking handler — so it covers HTTP framing, keep-alive, aws-chunked uploads, and percent-decoded
 * keys end to end without BookKeeper/ZooKeeper.
 */
class S3GatewayServerTest {

    private static S3GatewayServer server;
    private static HttpClient http;
    private static String baseUri;

    @BeforeAll
    static void start() {
        Properties props = new Properties();
        props.setProperty("zookeeper.connect", "unused:2181");
        props.setProperty("s3.bind", "127.0.0.1:0");
        server = new S3GatewayServer(S3GatewayConfig.fromProperties(props, java.util.Map.of()),
                new FakeCandyStore());
        server.start();
        http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
        baseUri = "http://127.0.0.1:" + server.port();
    }

    @AfterAll
    static void stop() {
        if (server != null) {
            server.close();
        }
    }

    @Test
    void putGetOverRealSocketDefaultsContentType() throws Exception {
        assertThat(send("PUT", "/photos", null).statusCode()).isEqualTo(200);
        byte[] data = "real socket bytes".getBytes(StandardCharsets.UTF_8);
        assertThat(send("PUT", "/photos/obj.bin", data).statusCode()).isEqualTo(200);

        HttpResponse<byte[]> get = http.send(
                HttpRequest.newBuilder(URI.create(baseUri + "/photos/obj.bin")).GET().build(),
                BodyHandlers.ofByteArray());
        assertThat(get.statusCode()).isEqualTo(200);
        assertThat(get.body()).isEqualTo(data);
        // No Content-Type was sent on PUT, so the gateway defaults it.
        assertThat(get.headers().firstValue("Content-Type")).hasValue("application/octet-stream");
    }

    @Test
    void decodesAwsChunkedUploads() throws Exception {
        send("PUT", "/chunked", null);
        // "hello candybox" is 14 (0xe) bytes, wrapped as one signed chunk + the terminating chunk.
        String framed = "e;chunk-signature=deadbeef\r\nhello candybox\r\n0;chunk-signature=cafe\r\n\r\n";
        HttpRequest put = HttpRequest.newBuilder(URI.create(baseUri + "/chunked/obj"))
                .header("Content-Encoding", "aws-chunked")
                .header("x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
                .header("x-amz-decoded-content-length", "14")
                .PUT(BodyPublishers.ofString(framed))
                .build();
        assertThat(http.send(put, BodyHandlers.ofString()).statusCode()).isEqualTo(200);

        HttpResponse<String> get = http.send(
                HttpRequest.newBuilder(URI.create(baseUri + "/chunked/obj")).GET().build(),
                BodyHandlers.ofString());
        assertThat(get.body()).isEqualTo("hello candybox"); // framing stripped, not stored verbatim
    }

    @Test
    void percentEncodedKeysAreDecoded() throws Exception {
        send("PUT", "/docs", null);
        assertThat(send("PUT", "/docs/a%20b%2Bc.txt", "x".getBytes(StandardCharsets.UTF_8)).statusCode())
                .isEqualTo(200);
        // The stored key is the decoded form; listing shows it and a HEAD on the encoded path resolves.
        assertThat(send("GET", "/docs?list-type=2", null).body()).contains("<Key>a b+c.txt</Key>");
        assertThat(send("HEAD", "/docs/a%20b%2Bc.txt", null).statusCode()).isEqualTo(200);
    }

    @Test
    void missingKeyReturns404() throws Exception {
        send("PUT", "/empty", null);
        HttpResponse<String> r = send("GET", "/empty/nope", null);
        assertThat(r.statusCode()).isEqualTo(404);
        assertThat(r.body()).contains("<Code>NoSuchKey</Code>");
        assertThat(r.headers().firstValue("x-amz-request-id")).isPresent();
    }

    private HttpResponse<String> send(String method, String path, byte[] body) throws Exception {
        HttpRequest req = HttpRequest.newBuilder(URI.create(baseUri + path))
                .method(method, body == null ? BodyPublishers.noBody() : BodyPublishers.ofByteArray(body))
                .build();
        return http.send(req, BodyHandlers.ofString());
    }
}
