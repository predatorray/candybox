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
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpVersion;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import me.predatorray.candybox.client.CandyboxClient.CandyInfo;
import me.predatorray.candybox.client.CandyboxClient.Listing;
import me.predatorray.candybox.client.CandyboxClient.MultipartListing;
import me.predatorray.candybox.client.CandyboxClient.PartListing;
import me.predatorray.candybox.client.CandyboxClient.PartUploadInfo;
import me.predatorray.candybox.client.CandyboxClient.RangeBytes;
import me.predatorray.candybox.common.exception.ValidationException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class S3HandlerEdgeCaseTest {
    private final FakeCandyStore store = new FakeCandyStore();

    private static S3GatewayConfig config(String region) {
        Properties p = new Properties();
        p.setProperty("zookeeper.connect", "unused:2181");
        p.setProperty("s3.region", region);
        return S3GatewayConfig.fromProperties(p, Map.of());
    }

    @Test
    void deleteBatchReportsPerKeyFailureAndContinues() {
        store.createBox("photos");
        for (String key : List.of("before", "failure", "after")) {
            store.putCandy("photos", key, new byte[]{1}, "text/plain", Map.of());
        }
        CandyStore failing = new DeleteFailingStore(store);
        try (var cleanup = new ChannelCleanup(new EmbeddedChannel(new S3Handler(failing, config("us-east-1"))))) {
            Response response = exchange(cleanup.channel, HttpMethod.POST, "/photos?delete",
                    "<Delete><Object><Key>before</Key></Object><Object><Key>failure</Key></Object>"
                    + "<Object><Key>after</Key></Object></Delete>");
            assertThat(response.status).isEqualTo(200);
            assertThat(response.body).contains("<Deleted><Key>before</Key></Deleted>")
                    .contains("<Deleted><Key>after</Key></Deleted>")
                    .contains("<Error><Key>failure</Key><Code>InvalidArgument</Code><Message>blocked key</Message></Error>");
            assertThat(store.listCandies("photos", null, null, null, 100).entries()).extracting(Listing.Entry::key).containsExactly("failure");
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void closesConnectionAfterResponse(boolean http10) {
        EmbeddedChannel channel = new EmbeddedChannel(new S3Handler(store, config("us-east-1")));
        try {
            DefaultFullHttpRequest request = new DefaultFullHttpRequest(
                    http10 ? HttpVersion.HTTP_1_0 : HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
            if (!http10) {
                request.headers().set(HttpHeaderNames.CONNECTION, "close");
            }
            channel.writeInbound(request);
            Response response = capture(channel);
            assertThat(response.status).isEqualTo(200);
            assertThat(response.connection).isEqualTo("close");
            assertThat(channel.isOpen()).isFalse();
        } finally { channel.finishAndReleaseAll(); }
    }

    @ParameterizedTest
    @CsvSource({"POST, /photos/key", "POST, /photos", "PATCH, /photos/key", "BREW, /photos/key"})
    void unsupportedRoutesReturn501(String method, String uri) {
        Response r = request(HttpMethod.valueOf(method), uri, "");
        assertThat(r.status).isEqualTo(501);
        assertThat(r.body).contains("<Code>NotImplemented</Code>");
    }

    @ParameterizedTest
    @CsvSource({
        "PUT, ?partNumber=1&uploadId=x", "POST, ?uploadId=x", "POST, ?uploads",
        "GET, ?uploadId=x", "DELETE, ?uploadId=x"
    })
    void multipartObjectOperationsWithoutKeyReturn405(String method, String query) {
        store.createBox("photos");
        for (String path : List.of("/photos", "/photos/")) {
            Response r = request(HttpMethod.valueOf(method), path + query, "");
            assertThat(r.status).as("%s %s%s", method, path, query).isEqualTo(405);
            assertThat(r.body).contains("<Code>MethodNotAllowed</Code>")
                    .contains("<Message>Missing object key</Message>");
            assertThat(store.headBox("photos")).isTrue();
            assertThat(store.listMultipartUploads("photos", null, null, null, 100).uploads()).isEmpty();
        }
    }

    @Test
    void missingKeyUploadPartCannotCreateBucket() {
        Response r = request(HttpMethod.PUT, "/photos?partNumber=1&uploadId=x", "payload");
        assertThat(r.status).isEqualTo(405);
        assertThat(r.body).contains("<Code>MethodNotAllowed</Code>")
                .contains("<Message>Missing object key</Message>");
        assertThat(store.headBox("photos")).isFalse();
    }

    @Test
    void ordinaryBucketOperationsKeepTheirBehavior() {
        assertThat(request(HttpMethod.PUT, "/photos", "").status).isEqualTo(200);
        assertThat(request(HttpMethod.HEAD, "/photos/", "").status).isEqualTo(200);
        Response uploads = request(HttpMethod.GET, "/photos?uploads", "");
        assertThat(uploads.status).isEqualTo(200);
        assertThat(uploads.body).contains("<ListMultipartUploadsResult");
        Response deleted = request(HttpMethod.POST, "/photos?delete", "<Delete></Delete>");
        assertThat(deleted.status).isEqualTo(200);
        assertThat(deleted.body).contains("<DeleteResult");
        assertThat(request(HttpMethod.DELETE, "/photos", "").status).isEqualTo(204);
        assertThat(store.headBox("photos")).isFalse();
    }

    @Test
    void invalidContinuationTokenReturns400() {
        store.createBox("photos");
        // Encode percent signs so query parsing succeeds and the token decoder sees invalid Base64.
        Response r = request(HttpMethod.GET, "/photos?list-type=2&continuation-token=%25%25%25", "");
        assertThat(r.status).isEqualTo(400);
        assertThat(r.body).contains("<Code>InvalidArgument</Code>")
                .contains("<Message>Invalid continuation-token</Message>");
    }

    @Test
    void nonNumericMaxKeysFallsBackToDefault() {
        store.createBox("photos");
        Response r = request(HttpMethod.GET, "/photos?max-keys=abc", "");
        assertThat(r.status).isEqualTo(200);
        assertThat(r.body).contains("<MaxKeys>1000</MaxKeys>");
    }

    @ParameterizedTest
    @ValueSource(strings = {"a%zzb", "a%4"})
    void malformedPercentEscapeIsStoredLiterally(String key) {
        store.createBox("photos");
        assertThat(request(HttpMethod.PUT, "/photos/" + key, "payload").status).isEqualTo(200);
        assertThat(store.listCandies("photos", null, null, null, 100).entries()).extracting(Listing.Entry::key).containsExactly(key);
        assertThat(store.getCandyRange("photos", key, 0, -1).data())
                .isEqualTo("payload".getBytes(StandardCharsets.UTF_8));
    }

    @ParameterizedTest
    @CsvSource({"eu-west-1, eu-west-1", "us-east-1, ''"})
    void bucketLocationUsesConfiguredRegion(String region, String expected) {
        store.createBox("photos");
        EmbeddedChannel channel = new EmbeddedChannel(new S3Handler(store, config(region)));
        try {
            Response r = exchange(channel, HttpMethod.GET, "/photos?location", "");
            assertThat(r.status).isEqualTo(200);
            assertThat(r.body).contains(">" + expected + "</LocationConstraint>");
        } finally { channel.finishAndReleaseAll(); }
    }

    @ParameterizedTest
    @ValueSource(strings = {"abc", "9"})
    void oversizedBodyIsRejectedForMalformedAndDeclaredLength(String length) {
        store.createBox("photos");
        EmbeddedChannel channel = bodyChannel();
        try {
            streamRequest(channel, length, "1234", "56789");
            Response r = capture(channel);
            assertThat(r.status).isEqualTo(400);
            assertThat(r.body).contains("<Code>EntityTooLarge</Code>");
            assertThat(store.listCandies("photos", null, null, null, 100).entries()).extracting(Listing.Entry::key).isEmpty();
            assertThat(channel.isOpen()).isFalse();
        } finally { channel.finishAndReleaseAll(); }
    }

    @Test
    void malformedLengthSmallBodyStillSucceeds() {
        store.createBox("photos");
        EmbeddedChannel channel = bodyChannel();
        try {
            streamRequest(channel, "abc", "12", "34");
            assertThat(capture(channel).status).isEqualTo(200);
            assertThat(store.getCandyRange("photos", "key", 0, -1).data())
                    .isEqualTo("1234".getBytes(StandardCharsets.UTF_8));
        } finally { channel.finishAndReleaseAll(); }
    }

    private EmbeddedChannel bodyChannel() {
        return new EmbeddedChannel(new S3BodyLimitHandler(8), new HttpObjectAggregator(1024),
                new S3Handler(store, config("us-east-1")));
    }

    private static void streamRequest(EmbeddedChannel channel, String length, String first, String last) {
        DefaultHttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.PUT, "/photos/key");
        request.headers().set(HttpHeaderNames.CONTENT_LENGTH, length);
        channel.writeInbound(request);
        channel.writeInbound(new DefaultHttpContent(Unpooled.copiedBuffer(first, StandardCharsets.UTF_8)));
        channel.writeInbound(new DefaultLastHttpContent(Unpooled.copiedBuffer(last, StandardCharsets.UTF_8)));
    }

    private Response request(HttpMethod method, String uri, String body) {
        EmbeddedChannel channel = new EmbeddedChannel(new S3Handler(store, config("us-east-1")));
        try { return exchange(channel, method, uri, body); }
        finally { channel.finishAndReleaseAll(); }
    }

    private static Response exchange(EmbeddedChannel channel, HttpMethod method, String uri, String body) {
        channel.writeInbound(new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method, uri,
                Unpooled.copiedBuffer(body, StandardCharsets.UTF_8)));
        return capture(channel);
    }

    private static Response capture(EmbeddedChannel channel) {
        FullHttpResponse response = channel.readOutbound();
        assertThat(response).isNotNull();
        try {
            return new Response(response.status().code(), response.content().toString(StandardCharsets.UTF_8),
                    response.headers().get(HttpHeaderNames.CONNECTION));
        } finally { response.release(); }
    }

    private record Response(int status, String body, String connection) { }

    private record ChannelCleanup(EmbeddedChannel channel) implements AutoCloseable {
        public void close() { channel.finishAndReleaseAll(); }
    }

    private static final class DeleteFailingStore implements CandyStore {
        private final CandyStore delegate;
        DeleteFailingStore(CandyStore delegate) { this.delegate = delegate; }

        @Override
        public void createBox(String box) {
            delegate.createBox(box);
        }

        @Override
        public void deleteBox(String box) {
            delegate.deleteBox(box);
        }

        @Override
        public boolean headBox(String box) {
            return delegate.headBox(box);
        }

        @Override
        public List<String> listBoxes() {
            return delegate.listBoxes();
        }

        @Override
        public void putCandy(String box, String key, byte[] data, String contentType, Map<String, String> userMetadata) {
            delegate.putCandy(box, key, data, contentType, userMetadata);
        }

        @Override
        public RangeBytes getCandyRange(String box, String key, long firstByte, long lastByte) {
            return delegate.getCandyRange(box, key, firstByte, lastByte);
        }

        @Override
        public CandyInfo headCandy(String box, String key) {
            return delegate.headCandy(box, key);
        }

        @Override
        public void deleteCandy(String box, String key) {
            if (key.equals("failure")) {
                throw new ValidationException("blocked key");
            }
            delegate.deleteCandy(box, key);
        }

        @Override
        public CandyInfo copyCandy(String box, String srcKey, String dstKey) {
            return delegate.copyCandy(box, srcKey, dstKey);
        }

        @Override
        public Listing listCandies(String box, String prefix, String startAfter, String delimiter, int maxKeys) {
            return delegate.listCandies(box, prefix, startAfter, delimiter, maxKeys);
        }

        @Override
        public String createMultipartUpload(String box, String key, String contentType, Map<String, String> userMetadata) {
            return delegate.createMultipartUpload(box, key, contentType, userMetadata);
        }

        @Override
        public PartUploadInfo uploadPart(String box, String key, String uploadId, int partNumber, byte[] data) {
            return delegate.uploadPart(box, key, uploadId, partNumber, data);
        }

        @Override
        public PartUploadInfo uploadPartCopy(String box, String key, String uploadId, int partNumber, String srcKey, long firstByte, long lastByte) {
            return delegate.uploadPartCopy(box, key, uploadId, partNumber, srcKey, firstByte, lastByte);
        }

        @Override
        public CandyInfo completeMultipartUpload(String box, String key, String uploadId, List<PartUploadInfo> parts) {
            return delegate.completeMultipartUpload(box, key, uploadId, parts);
        }

        @Override
        public void abortMultipartUpload(String box, String key, String uploadId) {
            delegate.abortMultipartUpload(box, key, uploadId);
        }

        @Override
        public MultipartListing listMultipartUploads(String box, String prefix, String keyMarker, String uploadIdMarker, int maxUploads) {
            return delegate.listMultipartUploads(box, prefix, keyMarker, uploadIdMarker, maxUploads);
        }

        @Override
        public PartListing listParts(String box, String key, String uploadId, int partNumberMarker, int maxParts) {
            return delegate.listParts(box, key, uploadId, partNumberMarker, maxParts);
        }
    }
}
