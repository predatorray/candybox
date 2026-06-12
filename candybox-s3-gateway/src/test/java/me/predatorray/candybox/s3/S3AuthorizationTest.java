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
import me.predatorray.candybox.common.auth.S3KeyStore;
import org.junit.jupiter.api.Test;

/**
 * SigV4 + ACL enforcement end-to-end through the Netty handler (EmbeddedChannel, FakeCandyStore):
 * signed header requests, presigned URLs, the anonymous-as-AllUsers model with its kill switch,
 * canned ACLs, the bucket/object ACL endpoints, payload-integrity modes, and the standard S3
 * error codes.
 */
class S3AuthorizationTest {

    private static final long NOW_MILLIS = 1_750_000_000_000L; // fixed test clock
    private static final S3Key ALICE_KEY =
            new S3Key("AKIAALICE", "alice-secret", Principal.user("alice"));
    private static final S3Key BOB_KEY = new S3Key("AKIABOB", "bob-secret", Principal.user("bob"));

    private final FakeCandyStore store = new FakeCandyStore();
    private final ManualClock clock = new ManualClock(NOW_MILLIS);
    private final S3KeyStore keys = accessKeyId -> switch (accessKeyId) {
        case "AKIAALICE" -> Optional.of(ALICE_KEY);
        case "AKIABOB" -> Optional.of(BOB_KEY);
        default -> Optional.empty();
    };

    private static S3GatewayConfig config() {
        Properties props = new Properties();
        props.setProperty("zookeeper.connect", "unused:2181");
        return S3GatewayConfig.fromProperties(props, Map.of());
    }

    private EmbeddedChannel channel(boolean allowAnonymous) {
        S3GatewayConfig config = config();
        S3Authenticator authenticator =
                new S3Authenticator(true, allowAnonymous, keys, "us-east-1", clock);
        return new EmbeddedChannel(
                new S3Handler(store, config, authenticator, new S3AccessControl(true, store)));
    }

    // ---- a minimal SigV4 request signer (header form) ---------------------------------------

    private Response exchange(EmbeddedChannel ch, HttpMethod method, String uri, byte[] body,
                              S3Key signAs, Map<String, String> extraHeaders) {
        DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method,
                uri, body == null ? Unpooled.EMPTY_BUFFER : Unpooled.wrappedBuffer(body));
        request.headers().set("Host", "127.0.0.1:9711");
        extraHeaders.forEach((k, v) -> request.headers().set(k, v));
        if (signAs != null) {
            sign(request, method.name(), uri, body, signAs);
        }
        ch.writeInbound(request);
        FullHttpResponse response = ch.readOutbound();
        String responseBody = response.content().toString(StandardCharsets.UTF_8);
        int status = response.status().code();
        response.release();
        return new Response(status, responseBody);
    }

    private record Response(int status, String body) {
    }

    private void sign(DefaultFullHttpRequest request, String method, String uri, byte[] body,
                      S3Key key) {
        String amzDate = SigV4.AMZ_DATE.format(Instant.ofEpochMilli(NOW_MILLIS));
        String payloadHash = SigV4.hex(SigV4.sha256(body == null ? new byte[0] : body));
        request.headers().set("x-amz-date", amzDate);
        request.headers().set("x-amz-content-sha256", payloadHash);

        Map<String, String> signedHeaderValues = new LinkedHashMap<>();
        signedHeaderValues.put("host", request.headers().get("Host"));
        signedHeaderValues.put("x-amz-content-sha256", payloadHash);
        signedHeaderValues.put("x-amz-date", amzDate);
        List<String> signedHeaders = List.of("host", "x-amz-content-sha256", "x-amz-date");

        int q = uri.indexOf('?');
        String rawPath = q < 0 ? uri : uri.substring(0, q);
        SigV4.Credential credential = new SigV4.Credential(key.accessKeyId(),
                amzDate.substring(0, 8), "us-east-1", "s3");
        String canonicalHash = SigV4.canonicalRequestHash(method, rawPath,
                SigV4.rawQueryParams(uri), signedHeaderValues, signedHeaders, payloadHash);
        String signature = SigV4.signature(SigV4.signingKey(key.secretKey(), credential),
                SigV4.stringToSign(amzDate, credential.scope(), canonicalHash));
        request.headers().set("Authorization", SigV4.ALGORITHM
                + " Credential=" + key.accessKeyId() + "/" + credential.scope()
                + ", SignedHeaders=" + String.join(";", signedHeaders)
                + ", Signature=" + signature);
    }

    private Response signedPut(EmbeddedChannel ch, String uri, byte[] body, S3Key as) {
        return exchange(ch, HttpMethod.PUT, uri, body, as, Map.of());
    }

    private Response signedGet(EmbeddedChannel ch, String uri, S3Key as) {
        return exchange(ch, HttpMethod.GET, uri, null, as, Map.of());
    }

    private Response anonymousGet(EmbeddedChannel ch, String uri) {
        return exchange(ch, HttpMethod.GET, uri, null, null, Map.of());
    }

    // ---- tests -------------------------------------------------------------------------------

    @Test
    void signedRequestsAuthenticateAndOwnTheirBuckets() {
        EmbeddedChannel ch = channel(true);
        assertThat(signedPut(ch, "/photos", null, ALICE_KEY).status()).isEqualTo(200);
        assertThat(store.getBoxAcl("photos").orElseThrow().owner())
                .isEqualTo(Principal.user("alice"));
        assertThat(signedPut(ch, "/photos/cat.jpg", "meow".getBytes(StandardCharsets.UTF_8),
                ALICE_KEY).status()).isEqualTo(200);
        assertThat(store.getCandyAcl("photos", "cat.jpg").owner()).isEqualTo("User:alice");
        // The owner reads it back; another authenticated user is denied; anonymous is denied.
        assertThat(signedGet(ch, "/photos/cat.jpg", ALICE_KEY).status()).isEqualTo(200);
        Response bob = signedGet(ch, "/photos/cat.jpg", BOB_KEY);
        assertThat(bob.status()).isEqualTo(403);
        assertThat(bob.body()).contains("AccessDenied");
        assertThat(anonymousGet(ch, "/photos/cat.jpg").status()).isEqualTo(403);
    }

    @Test
    void wrongSecretIsSignatureDoesNotMatchWithStringToSign() {
        EmbeddedChannel ch = channel(true);
        S3Key forged = new S3Key("AKIAALICE", "wrong-secret", Principal.user("alice"));
        Response r = signedPut(ch, "/photos", null, forged);
        assertThat(r.status()).isEqualTo(403);
        assertThat(r.body()).contains("SignatureDoesNotMatch");
        assertThat(r.body()).contains("StringToSign"); // the AWS-style debugging detail
    }

    @Test
    void unknownAccessKeyIsInvalidAccessKeyId() {
        EmbeddedChannel ch = channel(true);
        S3Key unknown = new S3Key("AKIANOBODY", "whatever", Principal.user("nobody"));
        Response r = signedGet(ch, "/", unknown);
        assertThat(r.status()).isEqualTo(403);
        assertThat(r.body()).contains("InvalidAccessKeyId");
    }

    @Test
    void skewedClockIsRequestTimeTooSkewed() {
        EmbeddedChannel ch = channel(true);
        clock.advance(20 * 60 * 1000); // server is 20 minutes ahead of the signed x-amz-date
        Response r = signedGet(ch, "/", ALICE_KEY);
        assertThat(r.status()).isEqualTo(403);
        assertThat(r.body()).contains("RequestTimeTooSkewed");
    }

    @Test
    void anonymousFollowsAclGrantsAndPublicReadOpensObjects() {
        EmbeddedChannel ch = channel(true);
        signedPut(ch, "/site", null, ALICE_KEY);
        // public-read via the canned header at PUT time.
        Response put = exchange(ch, HttpMethod.PUT, "/site/index.html",
                "<html/>".getBytes(StandardCharsets.UTF_8), ALICE_KEY,
                Map.of("x-amz-acl", "public-read"));
        assertThat(put.status()).isEqualTo(200);
        // Anonymous can read the public object, cannot write, cannot read a private one.
        assertThat(anonymousGet(ch, "/site/index.html").status()).isEqualTo(200);
        assertThat(exchange(ch, HttpMethod.PUT, "/site/hack.html",
                "x".getBytes(StandardCharsets.UTF_8), null, Map.of()).status()).isEqualTo(403);
        signedPut(ch, "/site/secret.html", "s".getBytes(StandardCharsets.UTF_8), ALICE_KEY);
        assertThat(anonymousGet(ch, "/site/secret.html").status()).isEqualTo(403);
    }

    @Test
    void theKillSwitchBlocksAllAnonymousAccess() {
        EmbeddedChannel allow = channel(true);
        signedPut(allow, "/pub", null, ALICE_KEY);
        exchange(allow, HttpMethod.PUT, "/pub/o", "v".getBytes(StandardCharsets.UTF_8), ALICE_KEY,
                Map.of("x-amz-acl", "public-read"));
        assertThat(anonymousGet(allow, "/pub/o").status()).isEqualTo(200);
        // Same store, anonymous now hard-blocked despite the public-read grant.
        EmbeddedChannel blocked = channel(false);
        assertThat(anonymousGet(blocked, "/pub/o").status()).isEqualTo(403);
    }

    @Test
    void anonymousListBucketsIsAnEmptyListNotAnError() {
        EmbeddedChannel ch = channel(true);
        signedPut(ch, "/mine", null, ALICE_KEY);
        Response r = anonymousGet(ch, "/");
        assertThat(r.status()).isEqualTo(200);
        assertThat(r.body()).doesNotContain("mine");
        // The owner sees it; another user does not (READ-filtered listing).
        assertThat(signedGet(ch, "/", ALICE_KEY).body()).contains("mine");
        assertThat(signedGet(ch, "/", BOB_KEY).body()).doesNotContain("mine");
    }

    @Test
    void bucketAclEndpointsRoundTripGrants() {
        EmbeddedChannel ch = channel(true);
        signedPut(ch, "/team", null, ALICE_KEY);
        // Grant bob READ+WRITE through the XML body form.
        String policy = """
                <AccessControlPolicy><Owner><ID>User:alice</ID></Owner><AccessControlList>
                <Grant><Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" \
                xsi:type="CanonicalUser"><ID>User:bob</ID></Grantee><Permission>READ</Permission></Grant>
                <Grant><Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" \
                xsi:type="CanonicalUser"><ID>User:bob</ID></Grantee><Permission>WRITE</Permission></Grant>
                </AccessControlList></AccessControlPolicy>""";
        assertThat(signedPut(ch, "/team?acl", policy.getBytes(StandardCharsets.UTF_8), ALICE_KEY)
                .status()).isEqualTo(200);
        // bob can now write and read the ACL document is visible to alice.
        assertThat(signedPut(ch, "/team/bobs.txt", "hi".getBytes(StandardCharsets.UTF_8), BOB_KEY)
                .status()).isEqualTo(200);
        Response acl = signedGet(ch, "/team?acl", ALICE_KEY);
        assertThat(acl.status()).isEqualTo(200);
        assertThat(acl.body()).contains("User:alice").contains("User:bob").contains("READ");
        // bob holds READ+WRITE but not READ_ACP.
        assertThat(signedGet(ch, "/team?acl", BOB_KEY).status()).isEqualTo(403);
    }

    @Test
    void objectAclEndpointFlipsAnObjectPublic() {
        EmbeddedChannel ch = channel(true);
        signedPut(ch, "/flip", null, ALICE_KEY);
        signedPut(ch, "/flip/o", "v".getBytes(StandardCharsets.UTF_8), ALICE_KEY);
        assertThat(anonymousGet(ch, "/flip/o").status()).isEqualTo(403);
        Response setAcl = exchange(ch, HttpMethod.PUT, "/flip/o?acl", null, ALICE_KEY,
                Map.of("x-amz-acl", "public-read"));
        assertThat(setAcl.status()).isEqualTo(200);
        assertThat(anonymousGet(ch, "/flip/o").status()).isEqualTo(200);
        Response acl = signedGet(ch, "/flip/o?acl", ALICE_KEY);
        assertThat(acl.body()).contains("AllUsers").contains("READ").contains("User:alice");
    }

    @Test
    void presignedUrlGrantsTimeBoundedAccess() {
        EmbeddedChannel ch = channel(true);
        signedPut(ch, "/pre", null, ALICE_KEY);
        signedPut(ch, "/pre/o", "v".getBytes(StandardCharsets.UTF_8), ALICE_KEY);

        String uri = presignedUri("GET", "/pre/o", ALICE_KEY, 300);
        Response ok = exchange(ch, HttpMethod.GET, uri, null, null, Map.of());
        assertThat(ok.status()).isEqualTo(200);
        assertThat(ok.body()).isEqualTo("v");

        // Past expiry the same URL is rejected.
        clock.advance(600_000);
        Response expired = exchange(channel(true), HttpMethod.GET, uri, null, null, Map.of());
        assertThat(expired.status()).isEqualTo(403);
    }

    private String presignedUri(String method, String path, S3Key key, long expiresSeconds) {
        String amzDate = SigV4.AMZ_DATE.format(Instant.ofEpochMilli(NOW_MILLIS));
        SigV4.Credential credential = new SigV4.Credential(key.accessKeyId(),
                amzDate.substring(0, 8), "us-east-1", "s3");
        String credentialParam = (key.accessKeyId() + "/" + credential.scope())
                .replace("/", "%2F");
        String base = path + "?X-Amz-Algorithm=AWS4-HMAC-SHA256"
                + "&X-Amz-Credential=" + credentialParam
                + "&X-Amz-Date=" + amzDate
                + "&X-Amz-Expires=" + expiresSeconds
                + "&X-Amz-SignedHeaders=host";
        Map<String, String> signedHeaderValues = Map.of("host", "127.0.0.1:9711");
        String canonicalHash = SigV4.canonicalRequestHash(method, path,
                SigV4.rawQueryParams(base), signedHeaderValues, List.of("host"),
                SigV4.UNSIGNED_PAYLOAD);
        String signature = SigV4.signature(SigV4.signingKey(key.secretKey(), credential),
                SigV4.stringToSign(amzDate, credential.scope(), canonicalHash));
        return base + "&X-Amz-Signature=" + signature;
    }

    @Test
    void signedStreamingChunksAreVerifiedAndTamperingIsRejected() {
        EmbeddedChannel ch = channel(true);
        signedPut(ch, "/chunky", null, ALICE_KEY);

        // Build a STREAMING-AWS4-HMAC-SHA256-PAYLOAD request whose chunk signatures chain from the
        // request signature, then check the stored payload was unframed.
        byte[] payload = "hello streaming world".getBytes(StandardCharsets.UTF_8);
        Response ok = signedChunkedPut(ch, "/chunky/o", payload, false);
        assertThat(ok.status()).isEqualTo(200);
        assertThat(store.getCandy("chunky", "o")).isEqualTo(payload);

        Response tampered = signedChunkedPut(ch, "/chunky/bad", payload, true);
        assertThat(tampered.status()).isEqualTo(403);
        assertThat(tampered.body()).contains("SignatureDoesNotMatch");
    }

    private Response signedChunkedPut(EmbeddedChannel ch, String uri, byte[] payload,
                                      boolean tamper) {
        String amzDate = SigV4.AMZ_DATE.format(Instant.ofEpochMilli(NOW_MILLIS));
        SigV4.Credential credential = new SigV4.Credential(ALICE_KEY.accessKeyId(),
                amzDate.substring(0, 8), "us-east-1", "s3");
        byte[] signingKey = SigV4.signingKey(ALICE_KEY.secretKey(), credential);

        Map<String, String> signedHeaderValues = new LinkedHashMap<>();
        signedHeaderValues.put("host", "127.0.0.1:9711");
        signedHeaderValues.put("x-amz-content-sha256", SigV4.STREAMING_SIGNED);
        signedHeaderValues.put("x-amz-date", amzDate);
        List<String> signedHeaders = List.of("host", "x-amz-content-sha256", "x-amz-date");
        String canonicalHash = SigV4.canonicalRequestHash("PUT", uri, Map.of(),
                signedHeaderValues, signedHeaders, SigV4.STREAMING_SIGNED);
        String seed = SigV4.signature(signingKey,
                SigV4.stringToSign(amzDate, credential.scope(), canonicalHash));

        String chunk1Sig = SigV4.chunkSignature(signingKey, amzDate, credential.scope(), seed,
                payload);
        String finalSig = SigV4.chunkSignature(signingKey, amzDate, credential.scope(), chunk1Sig,
                new byte[0]);
        if (tamper) {
            chunk1Sig = "0" + chunk1Sig.substring(1);
        }
        String framed = Integer.toHexString(payload.length) + ";chunk-signature=" + chunk1Sig
                + "\r\n" + new String(payload, StandardCharsets.ISO_8859_1) + "\r\n"
                + "0;chunk-signature=" + finalSig + "\r\n\r\n";

        DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1,
                HttpMethod.PUT, uri,
                Unpooled.wrappedBuffer(framed.getBytes(StandardCharsets.ISO_8859_1)));
        request.headers().set("Host", "127.0.0.1:9711");
        request.headers().set("x-amz-date", amzDate);
        request.headers().set("x-amz-content-sha256", SigV4.STREAMING_SIGNED);
        request.headers().set("Content-Encoding", "aws-chunked");
        request.headers().set("Authorization", SigV4.ALGORITHM
                + " Credential=" + ALICE_KEY.accessKeyId() + "/" + credential.scope()
                + ", SignedHeaders=" + String.join(";", signedHeaders)
                + ", Signature=" + seed);
        ch.writeInbound(request);
        FullHttpResponse response = ch.readOutbound();
        Response captured = new Response(response.status().code(),
                response.content().toString(StandardCharsets.UTF_8));
        response.release();
        return captured;
    }

    @Test
    void unsignedTrailerPayloadsVerifyTheCrc32Trailer() {
        EmbeddedChannel ch = channel(true);
        signedPut(ch, "/trail", null, ALICE_KEY);
        byte[] payload = "trailer checked".getBytes(StandardCharsets.UTF_8);
        java.util.zip.CRC32 crc = new java.util.zip.CRC32();
        crc.update(payload);
        int v = (int) crc.getValue();
        String crcB64 = java.util.Base64.getEncoder().encodeToString(new byte[] {
                (byte) (v >>> 24), (byte) (v >>> 16), (byte) (v >>> 8), (byte) v});

        assertThat(trailerPut(ch, "/trail/ok", payload, crcB64).status()).isEqualTo(200);
        assertThat(store.getCandy("trail", "ok")).isEqualTo(payload);
        // A corrupted trailer is rejected.
        assertThat(trailerPut(ch, "/trail/bad", payload, "AAAAAA==").status()).isEqualTo(400);
    }

    private Response trailerPut(EmbeddedChannel ch, String uri, byte[] payload, String crc32B64) {
        String framed = Integer.toHexString(payload.length) + "\r\n"
                + new String(payload, StandardCharsets.ISO_8859_1) + "\r\n0\r\n"
                + "x-amz-checksum-crc32: " + crc32B64 + "\r\n\r\n";
        DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1,
                HttpMethod.PUT, uri,
                Unpooled.wrappedBuffer(framed.getBytes(StandardCharsets.ISO_8859_1)));
        request.headers().set("Host", "127.0.0.1:9711");
        request.headers().set("Content-Encoding", "aws-chunked");
        request.headers().set("x-amz-content-sha256", SigV4.STREAMING_UNSIGNED_TRAILER);
        sign(request, "PUT", uri, framed.getBytes(StandardCharsets.ISO_8859_1), ALICE_KEY);
        // sign() overwrote x-amz-content-sha256 with the body hash; restore the streaming mode but
        // re-sign accordingly: simplest is to sign with the literal mode header.
        return resignStreamingAndSend(ch, request, uri);
    }

    /** Re-signs the request with x-amz-content-sha256 = STREAMING-UNSIGNED-PAYLOAD-TRAILER. */
    private Response resignStreamingAndSend(EmbeddedChannel ch, DefaultFullHttpRequest request,
                                            String uri) {
        String amzDate = SigV4.AMZ_DATE.format(Instant.ofEpochMilli(NOW_MILLIS));
        SigV4.Credential credential = new SigV4.Credential(ALICE_KEY.accessKeyId(),
                amzDate.substring(0, 8), "us-east-1", "s3");
        request.headers().set("x-amz-date", amzDate);
        request.headers().set("x-amz-content-sha256", SigV4.STREAMING_UNSIGNED_TRAILER);
        Map<String, String> signedHeaderValues = new LinkedHashMap<>();
        signedHeaderValues.put("host", request.headers().get("Host"));
        signedHeaderValues.put("x-amz-content-sha256", SigV4.STREAMING_UNSIGNED_TRAILER);
        signedHeaderValues.put("x-amz-date", amzDate);
        List<String> signedHeaders = List.of("host", "x-amz-content-sha256", "x-amz-date");
        String canonicalHash = SigV4.canonicalRequestHash("PUT", uri, Map.of(),
                signedHeaderValues, signedHeaders, SigV4.STREAMING_UNSIGNED_TRAILER);
        String signature = SigV4.signature(SigV4.signingKey(ALICE_KEY.secretKey(), credential),
                SigV4.stringToSign(amzDate, credential.scope(), canonicalHash));
        request.headers().set("Authorization", SigV4.ALGORITHM
                + " Credential=" + ALICE_KEY.accessKeyId() + "/" + credential.scope()
                + ", SignedHeaders=" + String.join(";", signedHeaders)
                + ", Signature=" + signature);
        ch.writeInbound(request);
        FullHttpResponse response = ch.readOutbound();
        Response captured = new Response(response.status().code(),
                response.content().toString(StandardCharsets.UTF_8));
        response.release();
        return captured;
    }

    @Test
    void copyRequiresReadOnTheSourceAndTheRequesterOwnsTheCopy() {
        EmbeddedChannel ch = channel(true);
        signedPut(ch, "/srcbox", null, ALICE_KEY);
        signedPut(ch, "/srcbox/private", "v".getBytes(StandardCharsets.UTF_8), ALICE_KEY);
        // bob gets WRITE (not READ) on the bucket: he may create keys but cannot read the source.
        String writeOnly = """
                <AccessControlPolicy><Owner><ID>User:alice</ID></Owner><AccessControlList>
                <Grant><Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" \
                xsi:type="CanonicalUser"><ID>User:bob</ID></Grantee><Permission>WRITE</Permission></Grant>
                </AccessControlList></AccessControlPolicy>""";
        signedPut(ch, "/srcbox?acl", writeOnly.getBytes(StandardCharsets.UTF_8), ALICE_KEY);
        Response denied = exchange(ch, HttpMethod.PUT, "/srcbox/stolen", null, BOB_KEY,
                Map.of("x-amz-copy-source", "/srcbox/private"));
        assertThat(denied.status()).isEqualTo(403);
        // alice copies within her bucket: the copy belongs to her.
        Response ok = exchange(ch, HttpMethod.PUT, "/srcbox/copy", null, ALICE_KEY,
                Map.of("x-amz-copy-source", "/srcbox/private"));
        assertThat(ok.status()).isEqualTo(200);
        assertThat(store.getCandyAcl("srcbox", "copy").owner()).isEqualTo("User:alice");
    }
}
