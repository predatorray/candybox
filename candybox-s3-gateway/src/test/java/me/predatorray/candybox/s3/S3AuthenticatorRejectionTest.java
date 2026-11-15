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
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import me.predatorray.candybox.common.ManualClock;
import me.predatorray.candybox.common.auth.Principal;
import me.predatorray.candybox.common.auth.S3Key;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class S3AuthenticatorRejectionTest {
    private static final long NOW = 1_750_000_000_000L;
    private static final String DATE = SigV4.AMZ_DATE.format(Instant.ofEpochMilli(NOW));
    private static final S3Key KEY =
            new S3Key("AKIAALICE", "alice-secret", Principal.user("alice"));
    private final ManualClock clock = new ManualClock(NOW);
    private final S3Authenticator authenticator = new S3Authenticator(true, true,
            id -> KEY.accessKeyId().equals(id) ? Optional.of(KEY) : Optional.empty(), "us-east-1", clock);

    @Test
    void unmodifiedHeaderAndPresignedRequestsAuthenticate() {
        assertThat(exchange(signed()).status()).isEqualTo(200);
        assertThat(exchange(presigned()).status()).isEqualTo(200);
    }

    @ParameterizedTest
    @ValueSource(strings = {"AWS AKID:sig", "Basic dXNlcjpwYXNz"})
    void unsupportedAuthorizationScheme(String scheme) {
        DefaultFullHttpRequest request = signed();
        request.headers().set("Authorization", scheme);
        reject(request, 400, "InvalidRequest", "Unsupported Authorization scheme");
    }

    @Test
    void malformedAuthorizationHeader() {
        DefaultFullHttpRequest request = signed();
        request.headers().set("Authorization", request.headers().get("Authorization")
                .replaceAll(", Signature=.*", ""));
        reject(request, 400, "InvalidRequest", "Malformed SigV4 Authorization header");
    }

    @Test
    void dateHeaderFallbackAuthenticates() {
        DefaultFullHttpRequest request = request(new byte[0]);
        request.headers().set("Date", DATE);
        sign(request, List.of("host", "date"));
        assertThat(exchange(request).status()).isEqualTo(200);
    }

    @Test
    void missingBothDateHeaders() {
        DefaultFullHttpRequest request = signed();
        request.headers().remove("x-amz-date");
        reject(request, 403, "AccessDenied", "Missing x-amz-date / Date header");
    }

    @Test
    void unsupportedPresignedAlgorithm() {
        DefaultFullHttpRequest request = presigned();
        request.setUri(request.uri().replace(SigV4.ALGORITHM, "AWS4-HMAC-SHA1"));
        reject(request, 400, "InvalidRequest", "Unsupported X-Amz-Algorithm");
    }

    @ParameterizedTest
    @ValueSource(strings = {"Credential", "Date", "Expires", "SignedHeaders", "Signature"})
    void missingPresignedParameter(String parameter) {
        DefaultFullHttpRequest request = presigned();
        request.setUri(request.uri().replaceAll("&X-Amz-" + parameter + "=[^&]*", ""));
        reject(request, 403, "AccessDenied", "Incomplete presigned-URL query parameters");
    }

    @Test
    void tamperedPresignedSignature() {
        DefaultFullHttpRequest request = presigned();
        request.setUri(request.uri().replaceAll("X-Amz-Signature=.*", "X-Amz-Signature=" + "0".repeat(64)));
        reject(request, 403, "SignatureDoesNotMatch", "request signature");
    }

    @Test
    void omittedPayloadHashComputesBodyHashAndRejectsTampering() {
        DefaultFullHttpRequest valid = request(bytes("body"));
        sign(valid, List.of("host", "x-amz-date"));
        assertThat(exchange(valid).status()).isEqualTo(200);
        DefaultFullHttpRequest tampered = request(bytes("body"));
        sign(tampered, List.of("host", "x-amz-date"));
        tampered.content().setByte(0, 'B');
        reject(tampered, 403, "SignatureDoesNotMatch", "request signature");
    }

    @Test
    void wrongScopeService() {
        DefaultFullHttpRequest request = signed();
        request.headers().set("Authorization", request.headers().get("Authorization")
                .replace("/s3/", "/ec2/"));
        reject(request, 400, "InvalidRequest", "Credential scope service must be 's3'");
    }

    @Test
    void wrongScopeRegion() {
        DefaultFullHttpRequest request = signed();
        request.headers().set("Authorization", request.headers().get("Authorization")
                .replace("/us-east-1/", "/eu-west-1/"));
        reject(request, 400, "AuthorizationHeaderMalformed", "does not match this gateway's region");
    }

    @ParameterizedTest
    @ValueSource(strings = {"abc", "0", "604801"})
    void invalidPresignedExpiry(String expiry) {
        DefaultFullHttpRequest request = presigned();
        request.setUri(request.uri().replace("X-Amz-Expires=300", "X-Amz-Expires=" + expiry));
        reject(request, 400, "InvalidRequest", expiry.equals("abc")
                ? "Malformed X-Amz-Expires" : "X-Amz-Expires must be between 1 and 604800 seconds");
    }

    @Test
    void malformedAmzDate() {
        DefaultFullHttpRequest request = signed();
        request.headers().set("x-amz-date", "yesterday");
        reject(request, 400, "InvalidRequest", "Malformed x-amz-date");
    }

    @Test
    void declaredPayloadHashRejectsDifferentBody() {
        DefaultFullHttpRequest request = signed();
        request.setMethod(HttpMethod.PUT);
        request.setUri("/photos/object");
        request.content().writeBytes(bytes("body"));
        request.headers().set("x-amz-content-sha256", SigV4.hex(SigV4.sha256(bytes("other"))));
        sign(request, List.of("host", "x-amz-content-sha256", "x-amz-date"));
        reject(request, 400, "XAmzContentSHA256Mismatch", "does not match what was computed");
    }

    @Test
    void signedStreamingChunkRequiresSignature() {
        DefaultFullHttpRequest request = streaming(SigV4.STREAMING_SIGNED);
        S3Authenticator.S3Auth auth = authenticator.authenticate(request);
        String first = SigV4.chunkSignature(auth.signingKey(), DATE, auth.scope(),
                auth.seedSignature(), bytes("body"));
        String last = SigV4.chunkSignature(auth.signingKey(), DATE, auth.scope(), first, new byte[0]);
        String framed = "4;chunk-signature=" + first
                + "\r\nbody\r\n0;chunk-signature=" + last + "\r\n\r\n";
        assertThat(authenticator.verifiedBody(auth, bytes(framed), "aws-chunked"))
                .isEqualTo(bytes("body"));
        request.content().writeBytes(bytes(framed.replace(";chunk-signature=" + first, "")));
        reject(request, 403, "SignatureDoesNotMatch", "Missing chunk-signature");
    }

    @Test
    void unsignedTrailerCrc32cRejectsMismatch() {
        DefaultFullHttpRequest request = streaming(SigV4.STREAMING_UNSIGNED_TRAILER);
        java.util.zip.CRC32C crc = new java.util.zip.CRC32C();
        crc.update(bytes("body"));
        byte[] checksum = java.nio.ByteBuffer.allocate(4).putInt((int) crc.getValue()).array();
        String valid = java.util.Base64.getEncoder().encodeToString(checksum);
        String framed = "4\r\nbody\r\n0\r\nx-amz-checksum-crc32c: " + valid + "\r\n\r\n";
        S3Authenticator.S3Auth auth = authenticator.authenticate(request);
        assertThat(authenticator.verifiedBody(auth, bytes(framed), "aws-chunked"))
                .isEqualTo(bytes("body"));
        request.content().writeBytes(bytes(framed.replace(valid, "AAAAAA==")));
        reject(request, 400, "InvalidRequest", "x-amz-checksum-crc32c trailer does not match");
    }

    private DefaultFullHttpRequest streaming(String mode) {
        DefaultFullHttpRequest request = request(new byte[0]);
        request.setMethod(HttpMethod.PUT);
        request.setUri("/photos/object");
        request.headers().set("Content-Encoding", "aws-chunked");
        request.headers().set("x-amz-content-sha256", mode);
        sign(request, List.of("host", "x-amz-content-sha256", "x-amz-date"));
        return request;
    }

    private DefaultFullHttpRequest signed() {
        DefaultFullHttpRequest request = request(new byte[0]);
        request.headers().set("x-amz-content-sha256", SigV4.hex(SigV4.sha256(new byte[0])));
        sign(request, List.of("host", "x-amz-content-sha256", "x-amz-date"));
        return request;
    }

    private DefaultFullHttpRequest request(byte[] body) {
        DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1,
                HttpMethod.GET, "/", Unpooled.buffer().writeBytes(body));
        request.headers().set("Host", "127.0.0.1:9711");
        request.headers().set("x-amz-date", DATE);
        return request;
    }

    // Local copy of the S3AuthorizationTest signer, with selectable signed headers for fallback cases.
    private void sign(DefaultFullHttpRequest request, List<String> signedHeaders) {
        if (signedHeaders.contains("date")) {
            request.headers().remove("x-amz-date");
        }
        String payloadHash = request.headers().get("x-amz-content-sha256");
        if (payloadHash == null) {
            byte[] body = new byte[request.content().readableBytes()];
            request.content().getBytes(request.content().readerIndex(), body);
            payloadHash = SigV4.hex(SigV4.sha256(body));
        }
        Map<String, String> headers = new LinkedHashMap<>();
        signedHeaders.forEach(name -> headers.put(name, request.headers().get(name)));
        SigV4.Credential credential = new SigV4.Credential(KEY.accessKeyId(),
                DATE.substring(0, 8), "us-east-1", "s3");
        String path = request.uri().split("\\?", 2)[0];
        String canonicalHash = SigV4.canonicalRequestHash(request.method().name(), path,
                SigV4.rawQueryParams(request.uri()), headers, signedHeaders, payloadHash);
        String signature = SigV4.signature(SigV4.signingKey(KEY.secretKey(), credential),
                SigV4.stringToSign(DATE, credential.scope(), canonicalHash));
        request.headers().set("Authorization", SigV4.ALGORITHM + " Credential="
                + KEY.accessKeyId() + "/" + credential.scope() + ", SignedHeaders="
                + String.join(";", signedHeaders) + ", Signature=" + signature);
    }

    private DefaultFullHttpRequest presigned() {
        DefaultFullHttpRequest request = request(new byte[0]);
        SigV4.Credential credential = new SigV4.Credential(KEY.accessKeyId(),
                DATE.substring(0, 8), "us-east-1", "s3");
        String base = "/?X-Amz-Algorithm=" + SigV4.ALGORITHM + "&X-Amz-Credential="
                + (KEY.accessKeyId() + "/" + credential.scope()).replace("/", "%2F")
                + "&X-Amz-Date=" + DATE + "&X-Amz-Expires=300&X-Amz-SignedHeaders=host";
        String canonicalHash = SigV4.canonicalRequestHash("GET", "/", SigV4.rawQueryParams(base),
                Map.of("host", request.headers().get("Host")), List.of("host"), SigV4.UNSIGNED_PAYLOAD);
        String signature = SigV4.signature(SigV4.signingKey(KEY.secretKey(), credential),
                SigV4.stringToSign(DATE, credential.scope(), canonicalHash));
        request.setUri(base + "&X-Amz-Signature=" + signature);
        return request;
    }

    private void reject(DefaultFullHttpRequest request, int status, String code, String message) {
        Response response = exchange(request);
        assertThat(response.status()).isEqualTo(status);
        assertThat(response.body()).contains("<Code>" + code + "</Code>").contains(message);
    }

    private Response exchange(DefaultFullHttpRequest request) {
        FakeCandyStore store = new FakeCandyStore();
        store.createBox("photos");
        Properties properties = new Properties();
        properties.setProperty("zookeeper.connect", "unused:2181");
        S3GatewayConfig config = S3GatewayConfig.fromProperties(properties, Map.of());
        EmbeddedChannel channel = new EmbeddedChannel(new S3Handler(store, config, authenticator,
                new S3AccessControl(true, store)));
        try {
            channel.writeInbound(request);
            FullHttpResponse response = channel.readOutbound();
            try {
                return new Response(response.status().code(),
                        response.content().toString(StandardCharsets.UTF_8));
            } finally {
                response.release();
            }
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    private record Response(int status, String body) { }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
