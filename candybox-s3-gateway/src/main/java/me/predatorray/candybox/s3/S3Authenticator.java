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

import io.netty.handler.codec.http.FullHttpRequest;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import me.predatorray.candybox.common.Clock;
import me.predatorray.candybox.common.auth.Principal;
import me.predatorray.candybox.common.auth.S3Key;
import me.predatorray.candybox.common.auth.S3KeyStore;

/**
 * Authenticates one S3 request: AWS Signature Version 4 in its header form
 * ({@code Authorization: AWS4-HMAC-SHA256 ...}) or presigned-URL form ({@code X-Amz-Algorithm=...}
 * query auth), else the anonymous principal (S3's {@code AllUsers}) when allowed. A request that
 * <em>carries</em> auth material but fails verification is always rejected — it never degrades to
 * anonymous.
 *
 * <p>Verification failures map to the standard S3 errors: {@code InvalidAccessKeyId},
 * {@code SignatureDoesNotMatch} (with the server-side string-to-sign in the error detail, like
 * AWS), {@code RequestTimeTooSkewed} (±15 min), {@code AccessDenied} for blocked anonymous.
 */
final class S3Authenticator {

    private static final Duration MAX_CLOCK_SKEW = Duration.ofMinutes(15);

    private final boolean enabled;
    private final boolean allowAnonymous;
    private final S3KeyStore keys;
    private final String region;
    private final Clock clock;

    S3Authenticator(boolean enabled, boolean allowAnonymous, S3KeyStore keys, String region,
                    Clock clock) {
        this.enabled = enabled;
        this.allowAnonymous = allowAnonymous;
        this.keys = keys;
        this.region = region;
        this.clock = clock;
    }

    /** The outcome: who the caller is, plus what {@code bodyBytes} needs to verify the payload. */
    record S3Auth(Principal principal, String payloadSha256Header, byte[] signingKey,
                  String seedSignature, String amzDate, String scope) {

        static S3Auth anonymous() {
            return new S3Auth(Principal.ANONYMOUS, null, null, null, null, null);
        }

        boolean isAnonymous() {
            return principal.isAnonymous();
        }
    }

    S3Auth authenticate(FullHttpRequest request) {
        if (!enabled) {
            return S3Auth.anonymous();
        }
        String authorization = request.headers().get("Authorization");
        Map<String, List<String>> rawQuery = SigV4.rawQueryParams(request.uri());
        boolean presigned = rawQuery.containsKey("X-Amz-Algorithm");
        if (authorization == null && !presigned) {
            if (!allowAnonymous) {
                throw new S3Exception(S3ErrorCode.ACCESS_DENIED,
                        "Anonymous access is disabled on this gateway");
            }
            return S3Auth.anonymous();
        }
        return presigned ? verifyPresigned(request, rawQuery)
                : verifyHeader(request, authorization, rawQuery);
    }

    private S3Auth verifyHeader(FullHttpRequest request, String authorization,
                                Map<String, List<String>> rawQuery) {
        if (!authorization.startsWith(SigV4.ALGORITHM)) {
            throw new S3Exception(S3ErrorCode.INVALID_REQUEST,
                    "Unsupported Authorization scheme (only " + SigV4.ALGORITHM + ")");
        }
        SigV4.AuthorizationHeader header;
        try {
            header = SigV4.AuthorizationHeader.parse(authorization);
        } catch (IllegalArgumentException e) {
            throw new S3Exception(S3ErrorCode.INVALID_REQUEST, e.getMessage());
        }
        String amzDate = request.headers().get("x-amz-date");
        if (amzDate == null) {
            amzDate = request.headers().get("Date");
        }
        if (amzDate == null) {
            throw new S3Exception(S3ErrorCode.ACCESS_DENIED,
                    "Missing x-amz-date / Date header on a signed request");
        }
        checkSkew(amzDate);
        checkScope(header.credential());
        S3Key key = lookup(header.credential().accessKeyId());

        String payloadSha = request.headers().get("x-amz-content-sha256");
        String payloadHash = payloadHashForCanonicalRequest(request, payloadSha);

        Map<String, String> signedHeaderValues = new LinkedHashMap<>();
        for (String name : header.signedHeaders()) {
            signedHeaderValues.put(name, name.equals("host")
                    ? request.headers().get("Host") : request.headers().get(name));
        }
        String canonicalHash = SigV4.canonicalRequestHash(request.method().name(),
                rawPath(request.uri()), rawQuery, signedHeaderValues, header.signedHeaders(),
                payloadHash);
        String stringToSign = SigV4.stringToSign(amzDate, header.credential().scope(),
                canonicalHash);
        byte[] signingKey = SigV4.signingKey(key.secretKey(), header.credential());
        String expected = SigV4.signature(signingKey, stringToSign);
        if (!SigV4.signatureEquals(expected, header.signature())) {
            throw signatureMismatch(stringToSign);
        }
        return new S3Auth(key.principal(), payloadSha, signingKey, header.signature(), amzDate,
                header.credential().scope());
    }

    private S3Auth verifyPresigned(FullHttpRequest request, Map<String, List<String>> rawQuery) {
        if (!SigV4.ALGORITHM.equals(first(rawQuery, "X-Amz-Algorithm"))) {
            throw new S3Exception(S3ErrorCode.INVALID_REQUEST, "Unsupported X-Amz-Algorithm");
        }
        String credentialRaw = uriDecode(first(rawQuery, "X-Amz-Credential"));
        String amzDate = first(rawQuery, "X-Amz-Date");
        String expiresRaw = first(rawQuery, "X-Amz-Expires");
        String signedHeadersRaw = uriDecode(first(rawQuery, "X-Amz-SignedHeaders"));
        String providedSignature = first(rawQuery, "X-Amz-Signature");
        if (credentialRaw == null || amzDate == null || expiresRaw == null
                || signedHeadersRaw == null || providedSignature == null) {
            throw new S3Exception(S3ErrorCode.ACCESS_DENIED,
                    "Incomplete presigned-URL query parameters");
        }
        SigV4.Credential credential;
        try {
            credential = SigV4.Credential.parse(credentialRaw);
        } catch (IllegalArgumentException e) {
            throw new S3Exception(S3ErrorCode.INVALID_REQUEST, e.getMessage());
        }
        checkScope(credential);
        checkExpiry(amzDate, expiresRaw);
        S3Key key = lookup(credential.accessKeyId());

        // The signature itself is excluded from the canonical query string.
        Map<String, List<String>> canonicalParams = new LinkedHashMap<>(rawQuery);
        canonicalParams.remove("X-Amz-Signature");

        List<String> signedHeaders = List.of(signedHeadersRaw.toLowerCase(Locale.ROOT).split(";"));
        Map<String, String> signedHeaderValues = new LinkedHashMap<>();
        for (String name : signedHeaders) {
            signedHeaderValues.put(name, name.equals("host")
                    ? request.headers().get("Host") : request.headers().get(name));
        }
        String canonicalHash = SigV4.canonicalRequestHash(request.method().name(),
                rawPath(request.uri()), canonicalParams, signedHeaderValues, signedHeaders,
                SigV4.UNSIGNED_PAYLOAD);
        String stringToSign = SigV4.stringToSign(amzDate, credential.scope(), canonicalHash);
        byte[] signingKey = SigV4.signingKey(key.secretKey(), credential);
        String expected = SigV4.signature(signingKey, stringToSign);
        if (!SigV4.signatureEquals(expected, providedSignature)) {
            throw signatureMismatch(stringToSign);
        }
        return new S3Auth(key.principal(), SigV4.UNSIGNED_PAYLOAD, signingKey, expected, amzDate,
                credential.scope());
    }

    /** The hash that goes into the canonical request, per the x-amz-content-sha256 mode. */
    private static String payloadHashForCanonicalRequest(FullHttpRequest request,
                                                         String payloadSha) {
        if (payloadSha == null || payloadSha.isBlank()) {
            // Legacy clients may omit it; hash the body we received.
            byte[] body = new byte[request.content().readableBytes()];
            request.content().getBytes(request.content().readerIndex(), body);
            return SigV4.hex(SigV4.sha256(body));
        }
        return payloadSha.trim();
    }

    private S3Key lookup(String accessKeyId) {
        return keys.s3Key(accessKeyId).orElseThrow(() -> new S3Exception(
                S3ErrorCode.INVALID_ACCESS_KEY_ID,
                "The AWS access key Id you provided does not exist in our records."));
    }

    private void checkScope(SigV4.Credential credential) {
        if (!"s3".equals(credential.service())) {
            throw new S3Exception(S3ErrorCode.INVALID_REQUEST,
                    "Credential scope service must be 's3', got '" + credential.service() + "'");
        }
        if (!region.equals(credential.region())) {
            throw new S3Exception(S3ErrorCode.AUTHORIZATION_REGION_MISMATCH,
                    "Credential scope region '" + credential.region()
                            + "' does not match this gateway's region '" + region + "'");
        }
    }

    private void checkSkew(String amzDate) {
        Instant requestTime = parseAmzDate(amzDate);
        Instant now = Instant.ofEpochMilli(clock.currentTimeMillis());
        if (Duration.between(requestTime, now).abs().compareTo(MAX_CLOCK_SKEW) > 0) {
            throw new S3Exception(S3ErrorCode.REQUEST_TIME_TOO_SKEWED,
                    "The difference between the request time and the server's time is too large.");
        }
    }

    private void checkExpiry(String amzDate, String expiresRaw) {
        Instant signedAt = parseAmzDate(amzDate);
        long expiresSeconds;
        try {
            expiresSeconds = Long.parseLong(expiresRaw);
        } catch (NumberFormatException e) {
            throw new S3Exception(S3ErrorCode.INVALID_REQUEST, "Malformed X-Amz-Expires");
        }
        if (expiresSeconds < 1 || expiresSeconds > Duration.ofDays(7).toSeconds()) {
            throw new S3Exception(S3ErrorCode.INVALID_REQUEST,
                    "X-Amz-Expires must be between 1 and 604800 seconds");
        }
        Instant now = Instant.ofEpochMilli(clock.currentTimeMillis());
        if (now.isAfter(signedAt.plusSeconds(expiresSeconds))) {
            throw new S3Exception(S3ErrorCode.ACCESS_DENIED, "Request has expired");
        }
    }

    private static Instant parseAmzDate(String amzDate) {
        try {
            return Instant.from(SigV4.AMZ_DATE.withZone(ZoneOffset.UTC).parse(amzDate.trim()));
        } catch (DateTimeParseException e) {
            throw new S3Exception(S3ErrorCode.INVALID_REQUEST, "Malformed x-amz-date: " + amzDate);
        }
    }

    private static S3Exception signatureMismatch(String stringToSign) {
        return new S3Exception(S3ErrorCode.SIGNATURE_DOES_NOT_MATCH,
                "The request signature we calculated does not match the signature you provided. "
                        + "StringToSign was: '" + stringToSign.replace("\n", "\\n") + "'");
    }

    private static String first(Map<String, List<String>> params, String name) {
        List<String> values = params.get(name);
        return values == null || values.isEmpty() ? null : values.get(0);
    }

    private static String rawPath(String uri) {
        int q = uri.indexOf('?');
        return q < 0 ? uri : uri.substring(0, q);
    }

    private static String uriDecode(String s) {
        if (s == null) {
            return null;
        }
        return java.net.URLDecoder.decode(s, StandardCharsets.UTF_8);
    }

    /**
     * Verifies the received body against the request's payload mode and returns the raw object
     * bytes: literal sha256 modes are hash-checked; {@code STREAMING-AWS4-HMAC-SHA256-PAYLOAD} is
     * unframed with each chunk signature verified against the chain seeded by the request
     * signature; the {@code -TRAILER} variants are unframed with their {@code x-amz-checksum-*}
     * trailers validated when recognized (CRC32/CRC32C).
     */
    byte[] verifiedBody(S3Auth auth, byte[] received, String contentEncoding) {
        String mode = auth.payloadSha256Header();
        if (auth.isAnonymous() || mode == null || SigV4.UNSIGNED_PAYLOAD.equals(mode)) {
            // No payload integrity to enforce beyond what framing requires.
            if (AwsChunked.isChunked(contentEncoding, mode)) {
                return AwsChunked.decode(received).payload();
            }
            return received;
        }
        switch (mode) {
            case SigV4.STREAMING_SIGNED -> {
                AwsChunked.Decoded decoded = AwsChunked.decode(received);
                verifyChunkSignatures(auth, decoded);
                return decoded.payload();
            }
            case SigV4.STREAMING_SIGNED_TRAILER, SigV4.STREAMING_UNSIGNED_TRAILER -> {
                AwsChunked.Decoded decoded = AwsChunked.decode(received);
                verifyTrailerChecksum(decoded);
                return decoded.payload();
            }
            default -> {
                byte[] body = AwsChunked.isChunked(contentEncoding, mode)
                        ? AwsChunked.decode(received).payload() : received;
                String actual = SigV4.hex(SigV4.sha256(body));
                if (!SigV4.signatureEquals(actual, mode.toLowerCase(Locale.ROOT))) {
                    throw new S3Exception(S3ErrorCode.X_AMZ_CONTENT_SHA256_MISMATCH,
                            "The provided 'x-amz-content-sha256' header does not match what was computed.");
                }
                return body;
            }
        }
    }

    private void verifyChunkSignatures(S3Auth auth, AwsChunked.Decoded decoded) {
        String previous = auth.seedSignature();
        List<AwsChunked.Chunk> chunks = decoded.chunks();
        for (AwsChunked.Chunk chunk : chunks) {
            if (chunk.signature() == null) {
                throw new S3Exception(S3ErrorCode.SIGNATURE_DOES_NOT_MATCH,
                        "Missing chunk-signature in a signed streaming payload");
            }
            String expected = SigV4.chunkSignature(auth.signingKey(), auth.amzDate(), auth.scope(),
                    previous, chunk.data());
            if (!SigV4.signatureEquals(expected, chunk.signature())) {
                throw new S3Exception(S3ErrorCode.SIGNATURE_DOES_NOT_MATCH,
                        "Chunk signature mismatch in streaming payload");
            }
            previous = chunk.signature();
        }
    }

    private void verifyTrailerChecksum(AwsChunked.Decoded decoded) {
        Map<String, String> trailers = decoded.trailers();
        String crc32 = trailers.get("x-amz-checksum-crc32");
        if (crc32 != null) {
            java.util.zip.CRC32 crc = new java.util.zip.CRC32();
            crc.update(decoded.payload());
            if (!crc32.trim().equals(base64IntBE((int) crc.getValue()))) {
                throw new S3Exception(S3ErrorCode.INVALID_REQUEST,
                        "x-amz-checksum-crc32 trailer does not match the payload");
            }
        }
        String crc32c = trailers.get("x-amz-checksum-crc32c");
        if (crc32c != null) {
            java.util.zip.CRC32C crc = new java.util.zip.CRC32C();
            crc.update(decoded.payload());
            if (!crc32c.trim().equals(base64IntBE((int) crc.getValue()))) {
                throw new S3Exception(S3ErrorCode.INVALID_REQUEST,
                        "x-amz-checksum-crc32c trailer does not match the payload");
            }
        }
        // Other checksum algorithms (sha1/sha256) are accepted without verification in v1.
    }

    private static String base64IntBE(int value) {
        byte[] bytes = {(byte) (value >>> 24), (byte) (value >>> 16), (byte) (value >>> 8),
                (byte) value};
        return java.util.Base64.getEncoder().encodeToString(bytes);
    }
}
