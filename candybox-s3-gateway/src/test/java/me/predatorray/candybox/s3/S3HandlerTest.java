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
    void listObjectsV1HonoursMarker() {
        put("/photos");
        for (String k : new String[]{"k1", "k2", "k3"}) {
            put("/photos/" + k, "v".getBytes(StandardCharsets.UTF_8), null);
        }
        // No list-type=2 -> V1 ListObjects: the cursor is `marker` and the response echoes <Marker>,
        // not a continuation token.
        Response first = get("/photos?max-keys=2");
        assertThat(first.body).contains("<Key>k1</Key>").contains("<Key>k2</Key>")
                .doesNotContain("<Key>k3</Key>")
                .contains("<IsTruncated>true</IsTruncated>")
                .contains("<Marker></Marker>")
                .doesNotContain("ContinuationToken");

        Response second = get("/photos?max-keys=2&marker=k2");
        assertThat(second.body).contains("<Key>k3</Key>")
                .doesNotContain("<Key>k1</Key>").doesNotContain("<Key>k2</Key>");
    }

    @Test
    void listObjectVersionsListsAndPaginatesByKeyMarker() {
        put("/photos");
        put("/photos/a.txt", "1".getBytes(StandardCharsets.UTF_8), null);
        put("/photos/b.txt", "2".getBytes(StandardCharsets.UTF_8), null);

        Response all = get("/photos?versions");
        assertThat(all.status).isEqualTo(200);
        assertThat(all.body).contains("<ListVersionsResult")
                .contains("<Key>a.txt</Key>").contains("<Key>b.txt</Key>")
                .contains("<VersionId>null</VersionId>")
                .contains("<IsLatest>true</IsLatest>")
                .contains("<IsTruncated>false</IsTruncated>");

        // key-marker pagination lets a version-aware client drain the bucket key by key.
        Response page = get("/photos?versions&max-keys=1");
        assertThat(page.body).contains("<Key>a.txt</Key>").doesNotContain("<Key>b.txt</Key>")
                .contains("<IsTruncated>true</IsTruncated>")
                .contains("<NextKeyMarker>a.txt</NextKeyMarker>");
        Response next = get("/photos?versions&max-keys=1&key-marker=a.txt");
        assertThat(next.body).contains("<Key>b.txt</Key>").doesNotContain("<Key>a.txt</Key>");
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

    // ---- range GET -------------------------------------------------------------------------

    @Test
    void rangeGetReturns206WithContentRange() {
        put("/photos");
        byte[] data = "hello candybox".getBytes(StandardCharsets.UTF_8); // 14 bytes
        put("/photos/hello.txt", data, h -> h.set(HttpHeaderNames.CONTENT_TYPE, "text/plain"));

        Response r = exchange(HttpMethod.GET, "/photos/hello.txt", null,
                h -> h.set(HttpHeaderNames.RANGE, "bytes=6-9"));
        assertThat(r.status).isEqualTo(206);
        assertThat(r.body).isEqualTo("cand");
        assertThat(r.header(HttpHeaderNames.CONTENT_RANGE)).isEqualTo("bytes 6-9/14");
        assertThat(r.header(HttpHeaderNames.ACCEPT_RANGES)).isEqualTo("bytes");
    }

    @Test
    void rangeGetOpenEndedTailRange() {
        put("/photos");
        byte[] data = "hello candybox".getBytes(StandardCharsets.UTF_8);
        put("/photos/hello.txt", data, null);

        Response r = exchange(HttpMethod.GET, "/photos/hello.txt", null,
                h -> h.set(HttpHeaderNames.RANGE, "bytes=6-"));
        assertThat(r.status).isEqualTo(206);
        assertThat(r.body).isEqualTo("candybox");
        assertThat(r.header(HttpHeaderNames.CONTENT_RANGE)).isEqualTo("bytes 6-13/14");
    }

    @Test
    void rangeGetSuffixRange() {
        put("/photos");
        byte[] data = "hello candybox".getBytes(StandardCharsets.UTF_8);
        put("/photos/hello.txt", data, null);

        Response r = exchange(HttpMethod.GET, "/photos/hello.txt", null,
                h -> h.set(HttpHeaderNames.RANGE, "bytes=-3"));
        assertThat(r.status).isEqualTo(206);
        assertThat(r.body).isEqualTo("box");
        assertThat(r.header(HttpHeaderNames.CONTENT_RANGE)).isEqualTo("bytes 11-13/14");
    }

    @Test
    void rangeGetBeyondEndReturns416() {
        put("/photos");
        byte[] data = "abc".getBytes(StandardCharsets.UTF_8);
        put("/photos/short", data, null);

        Response r = exchange(HttpMethod.GET, "/photos/short", null,
                h -> h.set(HttpHeaderNames.RANGE, "bytes=100-200"));
        assertThat(r.status).isEqualTo(416);
        assertThat(r.body).contains("<Code>InvalidRange</Code>");
    }

    @Test
    void rangeGetMultiRangeReturns501() {
        put("/photos");
        put("/photos/hello.txt", "hello candybox".getBytes(StandardCharsets.UTF_8), null);

        Response r = exchange(HttpMethod.GET, "/photos/hello.txt", null,
                h -> h.set(HttpHeaderNames.RANGE, "bytes=0-3,6-9"));
        assertThat(r.status).isEqualTo(501);
        assertThat(r.body).contains("<Code>NotImplemented</Code>");
    }

    @Test
    void unparseableRangeFallsBackToFull200() {
        put("/photos");
        byte[] data = "hello candybox".getBytes(StandardCharsets.UTF_8);
        put("/photos/hello.txt", data, null);

        Response r = exchange(HttpMethod.GET, "/photos/hello.txt", null,
                h -> h.set(HttpHeaderNames.RANGE, "lemons=0-3"));
        assertThat(r.status).isEqualTo(200);
        assertThat(r.body).isEqualTo("hello candybox");
        assertThat(r.header(HttpHeaderNames.CONTENT_RANGE)).isNull();
    }

    // ---- multipart upload ------------------------------------------------------------------

    @Test
    void multipartUploadRoundTripStitchesPartsTogether() {
        put("/photos");
        Response create = exchange(HttpMethod.POST, "/photos/big.bin?uploads", new byte[0], null);
        assertThat(create.status).isEqualTo(200);
        String uploadId = between(create.body, "<UploadId>", "</UploadId>");

        Response p1 = exchange(HttpMethod.PUT,
                "/photos/big.bin?partNumber=1&uploadId=" + uploadId,
                "hello ".getBytes(StandardCharsets.UTF_8), null);
        Response p2 = exchange(HttpMethod.PUT,
                "/photos/big.bin?partNumber=2&uploadId=" + uploadId,
                "candy".getBytes(StandardCharsets.UTF_8), null);
        Response p3 = exchange(HttpMethod.PUT,
                "/photos/big.bin?partNumber=3&uploadId=" + uploadId,
                "box".getBytes(StandardCharsets.UTF_8), null);
        assertThat(p1.status).isEqualTo(200);
        String etag1 = p1.etag();
        String etag2 = p2.etag();
        String etag3 = p3.etag();
        assertThat(etag1).isNotBlank();

        String completeBody = "<CompleteMultipartUpload>"
                + "<Part><PartNumber>1</PartNumber><ETag>" + etag1 + "</ETag></Part>"
                + "<Part><PartNumber>2</PartNumber><ETag>" + etag2 + "</ETag></Part>"
                + "<Part><PartNumber>3</PartNumber><ETag>" + etag3 + "</ETag></Part>"
                + "</CompleteMultipartUpload>";
        Response complete = exchange(HttpMethod.POST,
                "/photos/big.bin?uploadId=" + uploadId,
                completeBody.getBytes(StandardCharsets.UTF_8), null);
        assertThat(complete.status).isEqualTo(200);
        assertThat(complete.body).contains("<CompleteMultipartUploadResult");

        // Subsequent GET sees the assembled object.
        Response g = get("/photos/big.bin");
        assertThat(g.status).isEqualTo(200);
        assertThat(g.body).isEqualTo("hello candybox");
    }

    @Test
    void abortMultipartUploadDropsTheInFlightUpload() {
        put("/photos");
        Response create = exchange(HttpMethod.POST, "/photos/draft?uploads", new byte[0], null);
        String uploadId = between(create.body, "<UploadId>", "</UploadId>");
        exchange(HttpMethod.PUT, "/photos/draft?partNumber=1&uploadId=" + uploadId,
                "abc".getBytes(StandardCharsets.UTF_8), null);

        Response abort = exchange(HttpMethod.DELETE, "/photos/draft?uploadId=" + uploadId,
                new byte[0], null);
        assertThat(abort.status).isEqualTo(204);

        // GET on the never-completed key is a 404.
        Response g = get("/photos/draft");
        assertThat(g.status).isEqualTo(404);
    }

    @Test
    void listMultipartUploadsReturnsInProgressUploadsAndListPartsReturnsRecordedParts() {
        put("/photos");
        Response create = exchange(HttpMethod.POST, "/photos/movie.mp4?uploads", new byte[0], null);
        String uploadId = between(create.body, "<UploadId>", "</UploadId>");
        exchange(HttpMethod.PUT, "/photos/movie.mp4?partNumber=1&uploadId=" + uploadId,
                "AAAAA".getBytes(StandardCharsets.UTF_8), null);
        exchange(HttpMethod.PUT, "/photos/movie.mp4?partNumber=2&uploadId=" + uploadId,
                "BBBBB".getBytes(StandardCharsets.UTF_8), null);

        Response listUploads = get("/photos/?uploads");
        assertThat(listUploads.status).isEqualTo(200);
        assertThat(listUploads.body).contains("<ListMultipartUploadsResult");
        assertThat(listUploads.body).contains("<UploadId>" + uploadId + "</UploadId>");
        assertThat(listUploads.body).contains("<Key>movie.mp4</Key>");

        Response listParts = get("/photos/movie.mp4?uploadId=" + uploadId);
        assertThat(listParts.status).isEqualTo(200);
        assertThat(listParts.body).contains("<PartNumber>1</PartNumber>");
        assertThat(listParts.body).contains("<PartNumber>2</PartNumber>");
        assertThat(listParts.body).contains("<Size>5</Size>");
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
