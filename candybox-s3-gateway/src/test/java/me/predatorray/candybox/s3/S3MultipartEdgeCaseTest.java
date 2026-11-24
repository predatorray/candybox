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
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class S3MultipartEdgeCaseTest {

    private final FakeCandyStore store = new FakeCandyStore();

    @ParameterizedTest
    @ValueSource(strings = {"0", "abc"})
    void rejectsInvalidPartNumbers(String partNumber) {
        String upload = startUpload("object");
        Response response = exchange(HttpMethod.PUT,
                "/photos/object?partNumber=" + partNumber + "&uploadId=" + upload, "part", Map.of());
        assertError(response, 400, "InvalidArgument",
                partNumber.equals("0") ? "partNumber must be &gt;= 1" : "Invalid partNumber");
        assertNoParts(upload);
    }

    @Test
    void rejectsPartLargerThanConfiguredBodyLimit() {
        String upload = startUpload("object");
        Properties props = properties();
        props.setProperty("s3.max-object-bytes", "3");
        Response response = exchange(HttpMethod.PUT, partUri(upload), "four", Map.of(),
                S3GatewayConfig.fromProperties(props, Map.of()));
        assertError(response, 400, "EntityTooLarge", "Part exceeds the maximum of 3 bytes");
        assertNoParts(upload);
    }

    @ParameterizedTest
    @ValueSource(strings = {"/photos", "photos/"})
    void rejectsCopySourceWithoutKey(String source) {
        String upload = startUpload("object");
        assertError(exchange(HttpMethod.PUT, partUri(upload), "", Map.of("x-amz-copy-source", source)),
                400, "InvalidArgument", "Malformed x-amz-copy-source");
        assertNoParts(upload);
    }

    @Test
    void rejectsCrossBucketPartCopy() {
        String upload = startUpload("object");
        assertError(exchange(HttpMethod.PUT, partUri(upload), "",
                Map.of("x-amz-copy-source", "/other/source")),
                501, "NotImplemented", "Cross-bucket UploadPartCopy is not supported");
        assertNoParts(upload);
    }

    @Test
    void copiesExactlyTheRequestedSourceRange() {
        String upload = startUpload("object");
        putCopySource();
        Response copied = exchange(HttpMethod.PUT, partUri(upload), "",
                Map.of("x-amz-copy-source", "/photos/source", "x-amz-copy-source-range", "bytes=2-5"));
        assertThat(copied.status()).isEqualTo(200);
        assertThat(copied.body()).contains("<CopyPartResult ");
        String etag = element(copied.body(), "ETag");
        String completion = "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>"
                + etag + "</ETag></Part></CompleteMultipartUpload>";
        assertThat(exchange(HttpMethod.POST, "/photos/object?uploadId=" + upload, completion, Map.of()).status())
                .isEqualTo(200);
        Response object = exchange(HttpMethod.GET, "/photos/object", "", Map.of());
        assertThat(object.status()).isEqualTo(200);
        assertThat(object.body()).isEqualTo("2345");
    }

    @ParameterizedTest
    @ValueSource(strings = {"bytes=abc", "bytes=10000000-20000000"})
    void rejectsMalformedOrUnsatisfiableCopyRanges(String range) {
        String upload = startUpload("object");
        putCopySource();
        Response response = exchange(HttpMethod.PUT, partUri(upload), "",
                Map.of("x-amz-copy-source", "/photos/source", "x-amz-copy-source-range", range));
        boolean malformed = range.equals("bytes=abc");
        assertError(response, malformed ? 400 : 416, malformed ? "InvalidArgument" : "InvalidRange",
                malformed ? "Malformed x-amz-copy-source-range" : "not satisfiable");
        assertNoParts(upload);
    }

    @Test
    void rejectsNonNumericPartNumberMarker() {
        String upload = startUpload("object");
        assertError(exchange(HttpMethod.GET, "/photos/object?uploadId=" + upload + "&part-number-marker=x",
                "", Map.of()), 400, "InvalidArgument", "Invalid part-number-marker");
    }

    @ParameterizedTest
    @ValueSource(strings = {"/photos/object?partNumber=1&uploadId=", "/photos/object?partNumber=&uploadId=known"})
    void rejectsEmptyOrMissingRequiredQueryParameters(String uri) {
        assertError(exchange(HttpMethod.PUT, uri, "part", Map.of()),
                400, "InvalidArgument", "Missing required query parameter");
    }

    @ParameterizedTest
    @ValueSource(strings = {"0", "-1", "5000"})
    void uploadPageSizeDefaultsAndCapsAtOneThousand(String requested) {
        assertThat(exchange(HttpMethod.PUT, "/photos", "", Map.of()).status()).isEqualTo(200);
        for (int i = 0; i < 1001; i++) {
            store.createMultipartUpload("photos", String.format("object-%04d", i), "text/plain", Map.of());
        }
        Response response = exchange(HttpMethod.GET, "/photos?uploads&max-uploads=" + requested, "", Map.of());
        assertThat(response.status()).isEqualTo(200);
        assertThat(response.body()).contains("<MaxUploads>1000</MaxUploads>", "<IsTruncated>true</IsTruncated>");
        assertThat(response.body().split("<Upload>", -1)).hasSize(1001);
    }

    @ParameterizedTest
    @ValueSource(strings = {"0", "-1", "5000"})
    void partPageSizeDefaultsAndCapsAtOneThousand(String requested) {
        String upload = startUpload("object");
        for (int i = 1; i <= 1001; i++) {
            store.uploadPart("photos", "object", upload, i, new byte[]{1});
        }
        Response response = exchange(HttpMethod.GET,
                "/photos/object?uploadId=" + upload + "&max-parts=" + requested, "", Map.of());
        assertThat(response.status()).isEqualTo(200);
        assertThat(response.body()).contains("<MaxParts>1000</MaxParts>", "<IsTruncated>true</IsTruncated>");
        assertThat(response.body().split("<Part>", -1)).hasSize(1001);
    }

    @Test
    void rejectsNonNumericPageSizes() {
        String upload = startUpload("object");
        assertError(exchange(HttpMethod.GET, "/photos?uploads&max-uploads=abc", "", Map.of()),
                400, "InvalidArgument", "Invalid max-uploads/max-parts");
        assertError(exchange(HttpMethod.GET, "/photos/object?uploadId=" + upload + "&max-parts=abc", "", Map.of()),
                400, "InvalidArgument", "Invalid max-uploads/max-parts");
    }

    @Test
    void smallPagesReturnContinuationsThatResumeAfterTheLastEntry() {
        String upload = startUpload("object-a");
        store.createMultipartUpload("photos", "object-b", "text/plain", Map.of());
        store.createMultipartUpload("photos", "object-c", "text/plain", Map.of());
        Response uploads = exchange(HttpMethod.GET, "/photos?uploads&max-uploads=2", "", Map.of());
        assertThat(uploads.status()).isEqualTo(200);
        assertThat(uploads.body()).contains("<IsTruncated>true</IsTruncated>");
        assertThat(uploads.body().split("<Upload>", -1)).hasSize(3);
        Response nextUploads = exchange(HttpMethod.GET, "/photos?uploads&max-uploads=2&key-marker="
                + element(uploads.body(), "NextKeyMarker") + "&upload-id-marker="
                + element(uploads.body(), "NextUploadIdMarker"), "", Map.of());
        assertThat(nextUploads.status()).isEqualTo(200);
        assertThat(nextUploads.body()).contains("<Key>object-c</Key>", "<IsTruncated>false</IsTruncated>")
                .doesNotContain("<Key>object-a</Key>", "<Key>object-b</Key>");
        for (int i = 1; i <= 3; i++) {
            store.uploadPart("photos", "object-a", upload, i, new byte[]{1});
        }
        Response parts = exchange(HttpMethod.GET, "/photos/object-a?uploadId=" + upload + "&max-parts=2", "", Map.of());
        assertThat(parts.status()).isEqualTo(200);
        assertThat(parts.body()).contains("<NextPartNumberMarker>2</NextPartNumberMarker>");
        Response nextParts = exchange(HttpMethod.GET, "/photos/object-a?uploadId=" + upload
                + "&max-parts=2&part-number-marker=" + element(parts.body(), "NextPartNumberMarker"), "", Map.of());
        assertThat(nextParts.status()).isEqualTo(200);
        assertThat(nextParts.body()).contains("<PartNumber>3</PartNumber>", "<IsTruncated>false</IsTruncated>")
                .doesNotContain("<PartNumber>1</PartNumber>", "<PartNumber>2</PartNumber>");
    }

    private void putCopySource() {
        // AWS permits byte-range copy only for source objects larger than 5 MiB.
        byte[] source = new byte[5 * 1024 * 1024 + 1];
        System.arraycopy("0123456789".getBytes(StandardCharsets.UTF_8), 0, source, 0, 10);
        store.putCandy("photos", "source", source, "application/octet-stream", Map.of());
    }

    private String startUpload(String key) {
        assertThat(exchange(HttpMethod.PUT, "/photos", "", Map.of()).status()).isEqualTo(200);
        Response response = exchange(HttpMethod.POST, "/photos/" + key + "?uploads", "", Map.of());
        assertThat(response.status()).isEqualTo(200);
        return element(response.body(), "UploadId");
    }

    private static String partUri(String upload) {
        return "/photos/object?partNumber=1&uploadId=" + upload;
    }

    private void assertNoParts(String upload) {
        assertThat(store.listParts("photos", "object", upload, 0, 1000).parts()).isEmpty();
    }

    private static void assertError(Response response, int status, String code, String message) {
        assertThat(response.status()).isEqualTo(status);
        assertThat(response.body()).contains("<Code>" + code + "</Code>", message);
    }

    private static String element(String xml, String tag) {
        String opening = "<" + tag + ">";
        int start = xml.indexOf(opening);
        assertThat(start).as("XML contains %s", opening).isGreaterThanOrEqualTo(0);
        int end = xml.indexOf("</" + tag + ">", start);
        assertThat(end).isGreaterThan(start);
        return xml.substring(start + opening.length(), end);
    }

    private static Properties properties() {
        Properties props = new Properties();
        props.setProperty("zookeeper.connect", "unused:2181");
        return props;
    }

    private Response exchange(HttpMethod method, String uri, String body, Map<String, String> headers) {
        return exchange(method, uri, body, headers, S3GatewayConfig.fromProperties(properties(), Map.of()));
    }

    private Response exchange(HttpMethod method, String uri, String body, Map<String, String> headers,
                              S3GatewayConfig config) {
        EmbeddedChannel channel = new EmbeddedChannel(new S3Handler(store, config));
        try {
            DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method, uri,
                    Unpooled.wrappedBuffer(body.getBytes(StandardCharsets.UTF_8)));
            headers.forEach(request.headers()::set);
            channel.writeInbound(request);
            FullHttpResponse response = channel.readOutbound();
            assertThat(response).isNotNull();
            try {
                return new Response(response.status().code(), response.content().toString(StandardCharsets.UTF_8));
            } finally {
                response.release();
            }
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    private record Response(int status, String body) {
    }
}
