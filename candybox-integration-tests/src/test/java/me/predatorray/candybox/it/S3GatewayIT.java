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
package me.predatorray.candybox.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import me.predatorray.candybox.bookkeeper.bk.BookKeeperLedgerStore;
import me.predatorray.candybox.client.CandyboxClient;
import me.predatorray.candybox.common.SystemClock;
import me.predatorray.candybox.common.config.CandyboxConfig;
import me.predatorray.candybox.coordination.zk.ZooKeeperCoordinationService;
import me.predatorray.candybox.protocol.FrameCodec;
import me.predatorray.candybox.protocol.transport.TcpTransport;
import me.predatorray.candybox.protocol.transport.TcpTransportServer;
import me.predatorray.candybox.s3.S3Gateway;
import me.predatorray.candybox.s3.S3GatewayConfig;
import me.predatorray.candybox.server.CandyboxNode;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * End-to-end test of the S3 gateway over the full stack: embedded BookKeeper, in-process ZooKeeper, a
 * real Candybox node, and the Netty gateway, driven with the JDK HTTP client (anonymous, path-style —
 * exactly how a load balancer would forward). Exercises the bucket/object lifecycle and the
 * deterministic CRC32C ETag round-trip. See {@code S3_GATEWAY_PLAN.md} §13.
 */
class S3GatewayIT {

    private static EmbeddedBookKeeper bookKeeper;
    private static TestingServer zookeeper;

    private static CandyboxNode node;
    private static TcpTransportServer transportServer;
    private static TcpTransport transport;
    private static ZooKeeperCoordinationService nodeCoord;
    private static ZooKeeperCoordinationService clientCoord;
    private static BookKeeperLedgerStore store;
    private static CandyboxClient client;
    private static S3Gateway gateway;

    private static HttpClient http;
    private static String baseUri;

    @BeforeAll
    static void start() throws Exception {
        bookKeeper = new EmbeddedBookKeeper(3);
        zookeeper = new TestingServer(true);

        CandyboxConfig config = CandyboxConfig.builder().build();
        int nodePort = freePort();
        store = new BookKeeperLedgerStore(bookKeeper.clientConfiguration(), "candybox".getBytes(StandardCharsets.UTF_8));
        nodeCoord = new ZooKeeperCoordinationService(zookeeper.getConnectString(), SystemClock.INSTANCE);
        node = new CandyboxNode(1, config, store, nodeCoord, SystemClock.INSTANCE, "127.0.0.1:" + nodePort);
        transportServer = new TcpTransportServer(nodePort, node.requestHandler(), new FrameCodec());

        transport = new TcpTransport();
        clientCoord = new ZooKeeperCoordinationService(zookeeper.getConnectString(), SystemClock.INSTANCE);
        client = new CandyboxClient(transport, clientCoord, config);

        Properties props = new Properties();
        props.setProperty("zookeeper.connect", zookeeper.getConnectString());
        props.setProperty("s3.bind", "127.0.0.1:0");
        gateway = new S3Gateway(S3GatewayConfig.fromProperties(props, java.util.Map.of()), client);
        gateway.start();

        // The gateway speaks HTTP/1.1 only; pin the client so it doesn't attempt an h2c upgrade.
        http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
        baseUri = "http://127.0.0.1:" + gateway.port();
    }

    @AfterAll
    static void stop() throws Exception {
        closeQuietly(gateway);
        closeQuietly(client);
        closeQuietly(transport);
        closeQuietly(transportServer);
        closeQuietly(node);
        closeQuietly(nodeCoord);
        closeQuietly(clientCoord);
        closeQuietly(store);
        if (zookeeper != null) {
            zookeeper.close();
        }
        if (bookKeeper != null) {
            bookKeeper.close();
        }
    }

    @Test
    void fullBucketAndObjectLifecycle() throws Exception {
        // Create bucket.
        assertThat(send("PUT", "/photos", null).statusCode()).isEqualTo(200);

        // Put an object.
        byte[] data = "hello candybox".getBytes(StandardCharsets.UTF_8);
        HttpResponse<String> put = send("PUT", "/photos/hello.txt", data, "Content-Type", "text/plain");
        assertThat(put.statusCode()).isEqualTo(200);
        String etag = put.headers().firstValue("ETag").orElseThrow();
        assertThat(etag).hasSize(34); // 32 hex chars + surrounding quotes

        // Get it back, bytes and ETag must match.
        HttpResponse<byte[]> get = http.send(
                HttpRequest.newBuilder(URI.create(baseUri + "/photos/hello.txt")).GET().build(),
                BodyHandlers.ofByteArray());
        assertThat(get.statusCode()).isEqualTo(200);
        assertThat(get.body()).isEqualTo(data);
        assertThat(get.headers().firstValue("ETag")).hasValue(etag);
        assertThat(get.headers().firstValue("Content-Type")).hasValue("text/plain");

        // Head.
        HttpResponse<String> head = send("HEAD", "/photos/hello.txt", null);
        assertThat(head.statusCode()).isEqualTo(200);
        assertThat(head.headers().firstValue("Content-Length")).hasValue(Integer.toString(data.length));

        // List (ListObjectsV2).
        HttpResponse<String> list = send("GET", "/photos?list-type=2", null);
        assertThat(list.statusCode()).isEqualTo(200);
        assertThat(list.body()).contains("<Key>hello.txt</Key>").contains("<IsTruncated>false</IsTruncated>");

        // Server-side copy within the bucket.
        HttpResponse<String> copy = send("PUT", "/photos/copy.txt", new byte[0],
                "x-amz-copy-source", "/photos/hello.txt");
        assertThat(copy.statusCode()).isEqualTo(200);
        assertThat(copy.body()).contains("<CopyObjectResult>");
        HttpResponse<byte[]> getCopy = http.send(
                HttpRequest.newBuilder(URI.create(baseUri + "/photos/copy.txt")).GET().build(),
                BodyHandlers.ofByteArray());
        assertThat(getCopy.body()).isEqualTo(data);

        // Delete objects, then the (now empty) bucket.
        assertThat(send("DELETE", "/photos/hello.txt", null).statusCode()).isEqualTo(204);
        assertThat(send("DELETE", "/photos/copy.txt", null).statusCode()).isEqualTo(204);
        assertThat(send("DELETE", "/photos", null).statusCode()).isEqualTo(204);
    }

    @Test
    void getMissingKeyReturnsNoSuchKey() throws Exception {
        send("PUT", "/inbox", null);
        HttpResponse<String> get = send("GET", "/inbox/nope.txt", null);
        assertThat(get.statusCode()).isEqualTo(404);
        assertThat(get.body()).contains("<Code>NoSuchKey</Code>");
    }

    @Test
    void deleteObjectIsIdempotent() throws Exception {
        send("PUT", "/trash", null);
        assertThat(send("DELETE", "/trash/never-existed.txt", null).statusCode()).isEqualTo(204);
    }

    // ---- helpers ---------------------------------------------------------------------------

    private HttpResponse<String> send(String method, String path, byte[] body, String... headers)
            throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(baseUri + path));
        b.method(method, body == null ? BodyPublishers.noBody() : BodyPublishers.ofByteArray(body));
        for (int i = 0; i + 1 < headers.length; i += 2) {
            b.header(headers[i], headers[i + 1]);
        }
        return http.send(b.build(), BodyHandlers.ofString());
    }

    private static int freePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static void closeQuietly(AutoCloseable c) {
        if (c == null) {
            return;
        }
        try {
            c.close();
        } catch (Exception ignored) {
            // best effort
        }
    }
}
