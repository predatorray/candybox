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

import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.function.Consumer;
import me.predatorray.candybox.common.checksum.Crc32c;
import org.junit.jupiter.api.Test;

class S3HandlerTest {

    private final FakeCandyStore store = new FakeCandyStore();

    // ---- buckets ---------------------------------------------------------------------------

    @Test
    void createHeadAndListBuckets() {
        assertThat(put("/photos").status).isEqualTo(200);
        assertThat(head("/photos").status).isEqualTo(200);
        assertThat(head("/missing").status).isEqualTo(404);

        Response list = get("/");
        assertThat(list.status).isEqualTo(200);
        assertThat(list.body).contains("<Name>photos</Name>");
    }

    @Test
    void createDuplicateBucketReturns409() {
        put("/photos");
        assertThat(put("/photos").status).isEqualTo(409);
    }

    @Test
    void deleteNonEmptyBucketReturns409() {
        put("/photos");
        put("/photos/cat.txt", "hi".getBytes(StandardCharsets.UTF_8), null);
        Response r = delete("/photos");
        assertThat(r.status).isEqualTo(409);
        assertThat(r.body).contains("<Code>BucketNotEmpty</Code>");
    }

    // ---- objects ---------------------------------------------------------------------------

    @Test
    void putGetRoundTripWithDeterministicEtag() {
        put("/photos");
        byte[] data = "hello candybox".getBytes(StandardCharsets.UTF_8);
        Response p = put("/photos/hello.txt", data, h -> h.set(HttpHeaderNames.CONTENT_TYPE, "text/plain"));
        assertThat(p.status).isEqualTo(200);
        String expectedEtag = Etag.of(Crc32c.of(data));
        assertThat(p.etag()).isEqualTo(expectedEtag);

        Response g = get("/photos/hello.txt");
        assertThat(g.status).isEqualTo(200);
        assertThat(g.body).isEqualTo("hello candybox");
        assertThat(g.header(HttpHeaderNames.CONTENT_TYPE)).isEqualTo("text/plain");
        assertThat(g.etag()).isEqualTo(expectedEtag);
    }

    @Test
    void userMetadataRoundTrips() {
        put("/photos");
        put("/photos/cat.txt", "x".getBytes(StandardCharsets.UTF_8),
                h -> h.set("x-amz-meta-Owner", "alice"));
        Response h = head("/photos/cat.txt");
        assertThat(h.status).isEqualTo(200);
        assertThat(h.header("x-amz-meta-owner")).isEqualTo("alice");
        assertThat(h.header(HttpHeaderNames.CONTENT_LENGTH)).isEqualTo("1");
    }

    @Test
    void getMissingKeyReturns404NoSuchKey() {
        put("/photos");
        Response r = get("/photos/nope.txt");
        assertThat(r.status).isEqualTo(404);
        assertThat(r.body).contains("<Code>NoSuchKey</Code>");
    }

    @Test
    void deleteIsIdempotent() {
        put("/photos");
        assertThat(delete("/photos/never.txt").status).isEqualTo(204);
    }

    @Test
    void copyObjectSameBucket() {
        put("/photos");
        put("/photos/cat.txt", "meow".getBytes(StandardCharsets.UTF_8), null);
        Response c = put("/photos/copy.txt", new byte[0],
                h -> h.set("x-amz-copy-source", "/photos/cat.txt"));
        assertThat(c.status).isEqualTo(200);
        assertThat(c.body).contains("<CopyObjectResult>");
        assertThat(get("/photos/copy.txt").body).isEqualTo("meow");
    }

    @Test
    void crossBucketCopyNotImplemented() {
        put("/photos");
        put("/other");
        Response c = put("/photos/copy.txt", new byte[0],
                h -> h.set("x-amz-copy-source", "/other/cat.txt"));
        assertThat(c.status).isEqualTo(501);
    }

    // ---- listing ---------------------------------------------------------------------------

    @Test
    void listObjectsWithDelimiterRollsUpCommonPrefixes() {
        put("/photos");
        put("/photos/a/x.txt", "1".getBytes(StandardCharsets.UTF_8), null);
        put("/photos/a/sub/y.txt", "2".getBytes(StandardCharsets.UTF_8), null);
        Response r = get("/photos?prefix=a/&delimiter=/");
        assertThat(r.status).isEqualTo(200);
        assertThat(r.body).contains("<Key>a/x.txt</Key>")
                .contains("<Prefix>a/sub/</Prefix>")
                .doesNotContain("<Key>a/sub/y.txt</Key>");
    }

    @Test
    void listObjectsPaginatesWithContinuationToken() {
        put("/photos");
        for (String k : new String[]{"k1", "k2", "k3"}) {
            put("/photos/" + k, "v".getBytes(StandardCharsets.UTF_8), null);
        }
        Response first = get("/photos?list-type=2&max-keys=2");
        assertThat(first.body).contains("<IsTruncated>true</IsTruncated>")
                .contains("<NextContinuationToken>");
        String token = between(first.body, "<NextContinuationToken>", "</NextContinuationToken>");

        Response second = get("/photos?list-type=2&max-keys=2&continuation-token=" + token);
        assertThat(second.body).contains("<Key>k3</Key>")
                .contains("<IsTruncated>false</IsTruncated>");
    }

    @Test
    void batchDeleteObjects() {
        put("/photos");
        put("/photos/a.txt", "1".getBytes(StandardCharsets.UTF_8), null);
        put("/photos/b.txt", "2".getBytes(StandardCharsets.UTF_8), null);
        String body = "<Delete><Object><Key>a.txt</Key></Object>"
                + "<Object><Key>b.txt</Key></Object></Delete>";
        Response r = exchange(HttpMethod.POST, "/photos?delete",
                body.getBytes(StandardCharsets.UTF_8), null);
        assertThat(r.status).isEqualTo(200);
        assertThat(r.body).contains("<Deleted><Key>a.txt</Key></Deleted>")
                .contains("<Deleted><Key>b.txt</Key></Deleted>");
        assertThat(get("/photos/a.txt").status).isEqualTo(404);
    }

    @Test
    void batchDeleteQuietModeOmitsSuccesses() {
        put("/photos");
        put("/photos/a.txt", "1".getBytes(StandardCharsets.UTF_8), null);
        String body = "<Delete><Quiet>true</Quiet><Object><Key>a.txt</Key></Object></Delete>";
        Response r = exchange(HttpMethod.POST, "/photos?delete",
                body.getBytes(StandardCharsets.UTF_8), null);
        assertThat(r.status).isEqualTo(200);
        assertThat(r.body).contains("<DeleteResult").doesNotContain("<Deleted>");
    }

    // ---- subresources & unsupported --------------------------------------------------------

    @Test
    void cannedSubresources() {
        put("/photos");
        assertThat(get("/photos?location").body).contains("LocationConstraint");
        assertThat(get("/photos?versioning").body).contains("VersioningConfiguration");
        assertThat(get("/photos?acl").body).contains("AccessControlPolicy");
    }

    @Test
    void unsupportedMethodReturns501() {
        Response r = exchange(HttpMethod.PATCH, "/photos/x", new byte[0], null);
        assertThat(r.status).isEqualTo(501);
    }

    @Test
    void everyResponseCarriesRequestId() {
        assertThat(get("/").header("x-amz-request-id")).isNotBlank();
    }

    @Test
    void listBucketsWhenEmpty() {
        Response r = get("/");
        assertThat(r.status).isEqualTo(200);
        assertThat(r.body).contains("<ListAllMyBucketsResult").doesNotContain("<Name>");
    }

    @Test
    void putExceedingMaxObjectBytesReturns400() {
        put("/photos");
        Properties p = new Properties();
        p.setProperty("zookeeper.connect", "unused:2181");
        p.setProperty("s3.max-object-bytes", "4");
        S3GatewayConfig tiny = S3GatewayConfig.fromProperties(p, java.util.Map.of());
        EmbeddedChannel ch = new EmbeddedChannel(new S3Handler(store, tiny));
        DefaultFullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.PUT,
                "/photos/big.bin", Unpooled.wrappedBuffer("too many bytes".getBytes(StandardCharsets.UTF_8)));
        ch.writeInbound(req);
        Response r = Response.capture(ch.readOutbound());
        ch.finishAndReleaseAll();
        assertThat(r.status).isEqualTo(400);
        assertThat(r.body).contains("<Code>EntityTooLarge</Code>");
    }

    @Test
    void malformedCopySourceReturns400() {
        put("/photos");
        Response r = put("/photos/dst.txt", new byte[0], h -> h.set("x-amz-copy-source", "no-slash-here"));
        assertThat(r.status).isEqualTo(400);
        assertThat(r.body).contains("<Code>InvalidArgument</Code>");
    }

    @Test
    void putWithoutContentTypeDefaultsToOctetStream() {
        put("/photos");
        put("/photos/blob", "x".getBytes(StandardCharsets.UTF_8), null);
        assertThat(head("/photos/blob").header(HttpHeaderNames.CONTENT_TYPE))
                .isEqualTo("application/octet-stream");
    }

    // ---- harness ---------------------------------------------------------------------------

    private static S3GatewayConfig config() {
        Properties p = new Properties();
        p.setProperty("zookeeper.connect", "unused:2181");
        return S3GatewayConfig.fromProperties(p, java.util.Map.of());
    }

    private Response get(String uri) {
        return exchange(HttpMethod.GET, uri, null, null);
    }

    private Response head(String uri) {
        return exchange(HttpMethod.HEAD, uri, null, null);
    }

    private Response delete(String uri) {
        return exchange(HttpMethod.DELETE, uri, null, null);
    }

    private Response put(String uri) {
        return exchange(HttpMethod.PUT, uri, new byte[0], null);
    }

    private Response put(String uri, byte[] body, Consumer<HttpHeaders> headers) {
        return exchange(HttpMethod.PUT, uri, body, headers);
    }

    private Response exchange(HttpMethod method, String uri, byte[] body, Consumer<HttpHeaders> headers) {
        EmbeddedChannel channel = new EmbeddedChannel(new S3Handler(store, config()));
        DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method, uri,
                body == null ? Unpooled.EMPTY_BUFFER : Unpooled.wrappedBuffer(body));
        if (headers != null) {
            headers.accept(request.headers());
        }
        channel.writeInbound(request);
        FullHttpResponse response = channel.readOutbound();
        Response captured = Response.capture(response);
        channel.finishAndReleaseAll();
        return captured;
    }

    private static String between(String s, String start, String end) {
        int a = s.indexOf(start) + start.length();
        return s.substring(a, s.indexOf(end, a));
    }

    private static final class Response {
        final int status;
        final String body;
        private final HttpHeaders headers;

        private Response(int status, String body, HttpHeaders headers) {
            this.status = status;
            this.body = body;
            this.headers = headers;
        }

        static Response capture(FullHttpResponse r) {
            String body = r.content().toString(StandardCharsets.UTF_8);
            HttpHeaders headers = r.headers().copy();
            int status = r.status().code();
            r.release();
            return new Response(status, body, headers);
        }

        String header(CharSequence name) {
            return headers.get(name);
        }

        String etag() {
            return headers.get(HttpHeaderNames.ETAG);
        }
    }
}
