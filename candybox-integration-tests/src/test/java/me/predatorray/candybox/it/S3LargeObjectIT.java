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
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;
import me.predatorray.candybox.bookkeeper.bk.BookKeeperLedgerStore;
import me.predatorray.candybox.client.CandyboxClient;
import me.predatorray.candybox.common.SystemClock;
import me.predatorray.candybox.common.config.CandyboxConfig;
import me.predatorray.candybox.coordination.zk.ZooKeeperCoordinationService;
import me.predatorray.candybox.protocol.FrameCodec;
import me.predatorray.candybox.protocol.Message;
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
 * Objects larger than one protocol frame through the full stack, with the frame cap shrunk so
 * "large" stays small: a multipart object bigger than a frame is readable with a plain GET and a
 * wide Range GET, and a single PUT bigger than a frame is refused up front.
 */
class S3LargeObjectIT {

    private static final int INLINE = 100_000;
    private static final int FRAME = Message.INLINE_DATA_OVERHEAD_BYTES + INLINE;

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

        CandyboxConfig config = CandyboxConfig.builder().maxFrameSizeBytes(FRAME)
                .multipartMinPartBytes(0).build();
        int nodePort = freePort();
        store = new BookKeeperLedgerStore(bookKeeper.clientConfiguration(),
                "candybox".getBytes(StandardCharsets.UTF_8));
        nodeCoord = new ZooKeeperCoordinationService(zookeeper.getConnectString(), SystemClock.INSTANCE);
        node = new CandyboxNode(1, config, store, nodeCoord, SystemClock.INSTANCE, "127.0.0.1:" + nodePort);
        transportServer = new TcpTransportServer(nodePort, node.requestHandler(), new FrameCodec(FRAME));

        transport = new TcpTransport(new FrameCodec(FRAME));
        clientCoord = new ZooKeeperCoordinationService(zookeeper.getConnectString(), SystemClock.INSTANCE);
        client = new CandyboxClient(transport, clientCoord, CandyboxConfig.builder().build());

        Properties props = new Properties();
        props.setProperty("zookeeper.connect", zookeeper.getConnectString());
        props.setProperty("s3.bind", "127.0.0.1:0");
        props.setProperty("max.frame.size.bytes", Integer.toString(FRAME));
        gateway = new S3Gateway(S3GatewayConfig.fromProperties(props, java.util.Map.of()), client);
        gateway.start();

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
    void multipartObjectLargerThanAFrameIsReadableWithAPlainGet() throws Exception {
        assertThat(send("PUT", "/large", null).statusCode()).isEqualTo(200);
        String init = send("POST", "/large/obj?uploads", null).body();
        String uploadId = between(init, "<UploadId>", "</UploadId>");

        byte[] object = new byte[3 * 80_000];
        new Random(7).nextBytes(object);
        StringBuilder complete = new StringBuilder("<CompleteMultipartUpload>");
        for (int part = 1; part <= 3; part++) {
            byte[] bytes = Arrays.copyOfRange(object, (part - 1) * 80_000, part * 80_000);
            HttpResponse<String> up = send("PUT",
                    "/large/obj?partNumber=" + part + "&uploadId=" + uploadId, bytes);
            assertThat(up.statusCode()).isEqualTo(200);
            complete.append("<Part><PartNumber>").append(part).append("</PartNumber><ETag>")
                    .append(up.headers().firstValue("ETag").orElseThrow()).append("</ETag></Part>");
        }
        complete.append("</CompleteMultipartUpload>");
        assertThat(send("POST", "/large/obj?uploadId=" + uploadId,
                complete.toString().getBytes(StandardCharsets.UTF_8)).statusCode()).isEqualTo(200);

        HttpResponse<byte[]> get = http.send(HttpRequest.newBuilder(URI.create(baseUri + "/large/obj"))
                .GET().build(), BodyHandlers.ofByteArray());
        assertThat(get.statusCode()).isEqualTo(200);
        assertThat(get.headers().firstValue("Content-Length")).hasValue(Integer.toString(object.length));
        assertThat(get.body()).isEqualTo(object);

        HttpResponse<byte[]> range = http.send(HttpRequest.newBuilder(URI.create(baseUri + "/large/obj"))
                .header("Range", "bytes=50000-229999").GET().build(), BodyHandlers.ofByteArray());
        assertThat(range.statusCode()).isEqualTo(206);
        assertThat(range.headers().firstValue("Content-Range")).hasValue("bytes 50000-229999/240000");
        assertThat(range.body()).isEqualTo(Arrays.copyOfRange(object, 50_000, 230_000));
    }

    @Test
    void singlePutLargerThanAFrameIsRefusedUpFront() throws Exception {
        assertThat(send("PUT", "/single", null).statusCode()).isEqualTo(200);
        HttpResponse<String> put = send("PUT", "/single/obj", new byte[INLINE + 1]);
        assertThat(put.statusCode()).isEqualTo(400);
        assertThat(put.body()).contains("<Code>EntityTooLarge</Code>");
        assertThat(send("PUT", "/single/obj", new byte[INLINE]).statusCode()).isEqualTo(200);
        assertThat(send("HEAD", "/single/obj", null).headers().firstValue("Content-Length"))
                .hasValue(Integer.toString(INLINE));
    }

    private HttpResponse<String> send(String method, String path, byte[] body) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(baseUri + path));
        b.method(method, body == null ? BodyPublishers.noBody() : BodyPublishers.ofByteArray(body));
        return http.send(b.build(), BodyHandlers.ofString());
    }

    private static String between(String s, String start, String end) {
        int a = s.indexOf(start) + start.length();
        return s.substring(a, s.indexOf(end, a));
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
