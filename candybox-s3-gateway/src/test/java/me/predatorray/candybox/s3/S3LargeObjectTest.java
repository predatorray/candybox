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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import me.predatorray.candybox.common.checksum.Crc32c;
import me.predatorray.candybox.protocol.Message;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Objects larger than one node frame: streamed GETs and early rejection of oversized PUTs. */
class S3LargeObjectTest {

    /** One frame carries at most this many object bytes. */
    private static final int INLINE = 1000;

    private final FakeCandyStore store = new FakeCandyStore();
    private byte[] object;

    @BeforeEach
    void setUp() {
        store.createBox("big");
        object = random(3500, 1);
        store.putCandy("big", "obj", object, "application/x-test", Map.of("k", "v"));
    }

    @Test
    void fullGetLargerThanAFrameIsStreamedInFrameSizedSlices() {
        Streamed r = get("/big/obj", null);
        assertThat(r.head.status().code()).isEqualTo(200);
        assertThat(r.head).isNotInstanceOf(FullHttpResponse.class);
        assertThat(r.head.headers().get(HttpHeaderNames.CONTENT_LENGTH)).isEqualTo("3500");
        assertThat(r.head.headers().get(HttpHeaderNames.ETAG)).isEqualTo(Etag.of(Crc32c.of(object)));
        assertThat(r.head.headers().get(HttpHeaderNames.CONTENT_TYPE)).isEqualTo("application/x-test");
        assertThat(r.head.headers().get("x-amz-meta-k")).isEqualTo("v");
        assertThat(r.complete).isTrue();
        assertThat(r.body).isEqualTo(object);
        assertThat(store.rangeReads).isEqualTo(4);
    }

    @Test
    void rangeGetLargerThanAFrameIsStreamed() {
        Streamed r = get("/big/obj", "bytes=100-2599");
        assertThat(r.head.status().code()).isEqualTo(206);
        assertThat(r.head.headers().get(HttpHeaderNames.CONTENT_RANGE)).isEqualTo("bytes 100-2599/3500");
        assertThat(r.head.headers().get(HttpHeaderNames.CONTENT_LENGTH)).isEqualTo("2500");
        assertThat(r.complete).isTrue();
        assertThat(r.body).isEqualTo(java.util.Arrays.copyOfRange(object, 100, 2600));
    }

    @Test
    void getWithinOneFrameIsASingleFullResponse() {
        Streamed r = get("/big/obj", "bytes=-1000");
        assertThat(r.head).isInstanceOf(FullHttpResponse.class);
        assertThat(r.head.headers().get(HttpHeaderNames.CONTENT_RANGE)).isEqualTo("bytes 2500-3499/3500");
        assertThat(r.body).isEqualTo(java.util.Arrays.copyOfRange(object, 2500, 3500));
        assertThat(store.rangeReads).isEqualTo(1);
    }

    @Test
    void overwriteBeforeTheFirstSliceIsRetriedWithFreshHeaders() {
        byte[] replacement = random(3000, 2);
        store.beforeRangeRead = n -> {
            if (n == 1) {
                store.putCandy("big", "obj", replacement, "text/plain", Map.of());
            }
        };
        Streamed r = get("/big/obj", null);
        assertThat(r.head.status().code()).isEqualTo(200);
        assertThat(r.head.headers().get(HttpHeaderNames.ETAG))
                .isEqualTo(Etag.of(Crc32c.of(replacement)));
        assertThat(r.body).isEqualTo(replacement);
    }

    @Test
    void overwriteMidStreamAbortsTheConnectionInsteadOfMixingVersions() {
        store.beforeRangeRead = n -> {
            if (n == 3) {
                store.putCandy("big", "obj", random(3500, 3), "text/plain", Map.of());
            }
        };
        Streamed r = get("/big/obj", null);
        assertThat(r.head.status().code()).isEqualTo(200);
        assertThat(r.complete).isFalse();
        assertThat(r.body).hasSize(2 * INLINE).isEqualTo(java.util.Arrays.copyOfRange(object, 0, 2 * INLINE));
        assertThat(r.channelOpen).isFalse();
    }

    @Test
    void oversizedPutIsRejectedBeforeItsBodyIsAggregated() {
        EmbeddedChannel ch = new EmbeddedChannel(new S3BodyLimitHandler(INLINE));
        HttpRequest put = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.PUT, "/big/huge");
        put.headers().set(HttpHeaderNames.CONTENT_LENGTH, INLINE + 1);
        ch.writeInbound(put);

        assertThat((Object) ch.readInbound()).isNull();
        FullHttpResponse r = ch.readOutbound();
        assertThat(r.status().code()).isEqualTo(400);
        assertThat(r.content().toString(StandardCharsets.UTF_8)).contains("<Code>EntityTooLarge</Code>");
        r.release();
        assertThat(ch.isOpen()).isFalse();
    }

    @Test
    void awsChunkedPutIsJudgedByItsDecodedLength() {
        EmbeddedChannel ch = new EmbeddedChannel(new S3BodyLimitHandler(INLINE));
        HttpRequest fits = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.PUT, "/big/a");
        fits.headers().set(HttpHeaderNames.CONTENT_LENGTH, INLINE + 200); // framing overhead
        fits.headers().set("x-amz-decoded-content-length", INLINE);
        ch.writeInbound(fits);
        assertThat((Object) ch.readInbound()).isSameAs(fits);
        assertThat((Object) ch.readOutbound()).isNull();

        HttpRequest tooBig = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.PUT, "/big/b");
        tooBig.headers().set(HttpHeaderNames.CONTENT_LENGTH, INLINE);
        tooBig.headers().set("x-amz-decoded-content-length", INLINE + 1);
        ch.writeInbound(tooBig);
        FullHttpResponse r = ch.readOutbound();
        assertThat(r.status().code()).isEqualTo(400);
        r.release();
        ch.finishAndReleaseAll();
    }

    @Test
    void overRealSocketLargeGetStreamsAndOversizedPutIsRejected() throws Exception {
        Properties props = baseProps();
        props.setProperty("s3.bind", "127.0.0.1:0");
        try (S3GatewayServer server = new S3GatewayServer(
                S3GatewayConfig.fromProperties(props, Map.of()), store)) {
            server.start();
            HttpClient http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
            String base = "http://127.0.0.1:" + server.port();

            var get = http.send(java.net.http.HttpRequest.newBuilder(URI.create(base + "/big/obj"))
                    .GET().build(), BodyHandlers.ofByteArray());
            assertThat(get.statusCode()).isEqualTo(200);
            assertThat(get.body()).isEqualTo(object);

            var put = http.send(java.net.http.HttpRequest.newBuilder(URI.create(base + "/big/huge"))
                    .PUT(BodyPublishers.ofByteArray(new byte[INLINE + 1])).build(),
                    BodyHandlers.ofString());
            assertThat(put.statusCode()).isEqualTo(400);
            assertThat(put.body()).contains("<Code>EntityTooLarge</Code>").contains("multipart");

            var ok = http.send(java.net.http.HttpRequest.newBuilder(URI.create(base + "/big/fits"))
                    .PUT(BodyPublishers.ofByteArray(new byte[INLINE])).build(),
                    BodyHandlers.ofString());
            assertThat(ok.statusCode()).isEqualTo(200);
        }
    }

    private static Properties baseProps() {
        Properties p = new Properties();
        p.setProperty("zookeeper.connect", "unused:2181");
        p.setProperty("max.frame.size.bytes",
                Integer.toString(Message.INLINE_DATA_OVERHEAD_BYTES + INLINE));
        return p;
    }

    private Streamed get(String uri, String range) {
        S3GatewayConfig config = S3GatewayConfig.fromProperties(baseProps(), Map.of());
        assertThat(config.maxRequestBodyBytes()).isEqualTo(INLINE);
        EmbeddedChannel ch = new EmbeddedChannel(new S3Handler(store, config));
        DefaultFullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri,
                Unpooled.EMPTY_BUFFER);
        if (range != null) {
            req.headers().set(HttpHeaderNames.RANGE, range);
        }
        ch.writeInbound(req);
        HttpResponse head = ch.readOutbound();
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        boolean complete = head instanceof FullHttpResponse;
        if (complete) {
            append(body, ((FullHttpResponse) head).content());
            ((FullHttpResponse) head).release();
        }
        Object msg;
        while ((msg = ch.readOutbound()) != null) {
            HttpContent c = (HttpContent) msg;
            append(body, c.content());
            complete |= c instanceof LastHttpContent;
            c.release();
        }
        boolean open = ch.isOpen();
        ch.finishAndReleaseAll();
        return new Streamed(head, body.toByteArray(), complete, open);
    }

    private static void append(ByteArrayOutputStream out, ByteBuf buf) {
        byte[] bytes = new byte[buf.readableBytes()];
        buf.getBytes(buf.readerIndex(), bytes);
        out.writeBytes(bytes);
    }

    private static byte[] random(int n, long seed) {
        byte[] b = new byte[n];
        new Random(seed).nextBytes(b);
        return b;
    }

    private record Streamed(HttpResponse head, byte[] body, boolean complete, boolean channelOpen) {
    }
}
