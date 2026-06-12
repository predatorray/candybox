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

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * The AWS Signature Version 4 primitives (canonical request, string-to-sign, signing-key
 * derivation, chunk signatures), as specified by the SigV4 documentation. Pure functions over the
 * request pieces — {@link S3Authenticator} owns the protocol-level orchestration (header vs.
 * presigned, payload modes, error mapping).
 */
final class SigV4 {

    static final String ALGORITHM = "AWS4-HMAC-SHA256";
    static final String UNSIGNED_PAYLOAD = "UNSIGNED-PAYLOAD";
    static final String STREAMING_SIGNED = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";
    static final String STREAMING_SIGNED_TRAILER = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER";
    static final String STREAMING_UNSIGNED_TRAILER = "STREAMING-UNSIGNED-PAYLOAD-TRAILER";

    static final DateTimeFormatter AMZ_DATE =
            DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(java.time.ZoneOffset.UTC);

    private SigV4() {
    }

    /** The parsed pieces of an {@code Authorization: AWS4-HMAC-SHA256 ...} header. */
    record Credential(String accessKeyId, String date, String region, String service) {
        String scope() {
            return date + "/" + region + "/" + service + "/aws4_request";
        }

        static Credential parse(String credential) {
            String[] parts = credential.split("/");
            if (parts.length != 5 || !"aws4_request".equals(parts[4])) {
                throw new IllegalArgumentException("Malformed SigV4 credential scope: " + credential);
            }
            return new Credential(parts[0], parts[1], parts[2], parts[3]);
        }
    }

    record AuthorizationHeader(Credential credential, List<String> signedHeaders, String signature) {
        static AuthorizationHeader parse(String header) {
            String rest = header.substring(ALGORITHM.length()).trim();
            String credential = null;
            String signedHeaders = null;
            String signature = null;
            for (String part : rest.split(",")) {
                String p = part.trim();
                if (p.startsWith("Credential=")) {
                    credential = p.substring("Credential=".length());
                } else if (p.startsWith("SignedHeaders=")) {
                    signedHeaders = p.substring("SignedHeaders=".length());
                } else if (p.startsWith("Signature=")) {
                    signature = p.substring("Signature=".length());
                }
            }
            if (credential == null || signedHeaders == null || signature == null) {
                throw new IllegalArgumentException("Malformed SigV4 Authorization header");
            }
            return new AuthorizationHeader(Credential.parse(credential),
                    List.of(signedHeaders.toLowerCase(Locale.ROOT).split(";")), signature);
        }
    }

    /**
     * The canonical request hash over the exact components SigV4 specifies. {@code rawPath} is the
     * undecoded request path ({@code /bucket/key...}); S3 canonicalizes it without re-encoding the
     * already-encoded octets. {@code queryParams} are the raw (still-encoded) name/value pairs
     * <em>excluding</em> {@code X-Amz-Signature}; {@code headers} the lowercase name → value map of
     * the signed headers only.
     */
    static String canonicalRequestHash(String method, String rawPath,
                                       Map<String, List<String>> queryParams,
                                       Map<String, String> headers, List<String> signedHeaders,
                                       String payloadHash) {
        StringBuilder canonical = new StringBuilder();
        canonical.append(method).append('\n');
        canonical.append(canonicalUri(rawPath)).append('\n');
        canonical.append(canonicalQueryString(queryParams)).append('\n');
        for (String name : signedHeaders) {
            String value = headers.get(name);
            canonical.append(name).append(':')
                    .append(value == null ? "" : value.trim().replaceAll("\\s+", " "))
                    .append('\n');
        }
        canonical.append('\n');
        canonical.append(String.join(";", signedHeaders)).append('\n');
        canonical.append(payloadHash);
        return hex(sha256(canonical.toString().getBytes(StandardCharsets.UTF_8)));
    }

    /** Path-style canonical URI: each path segment URI-encoded once (S3 does not double-encode). */
    private static String canonicalUri(String rawPath) {
        if (rawPath == null || rawPath.isEmpty()) {
            return "/";
        }
        return rawPath;
    }

    private static String canonicalQueryString(Map<String, List<String>> queryParams) {
        TreeMap<String, List<String>> sorted = new TreeMap<>();
        for (Map.Entry<String, List<String>> e : queryParams.entrySet()) {
            List<String> values = new ArrayList<>(e.getValue());
            java.util.Collections.sort(values);
            sorted.put(e.getKey(), values);
        }
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, List<String>> e : sorted.entrySet()) {
            for (String value : e.getValue()) {
                if (sb.length() > 0) {
                    sb.append('&');
                }
                sb.append(e.getKey()).append('=').append(value);
            }
        }
        return sb.toString();
    }

    static String stringToSign(String amzDate, String scope, String canonicalRequestHash) {
        return ALGORITHM + "\n" + amzDate + "\n" + scope + "\n" + canonicalRequestHash;
    }

    /** {@code HMAC(HMAC(HMAC(HMAC("AWS4"+secret, date), region), service), "aws4_request")}. */
    static byte[] signingKey(String secretKey, Credential credential) {
        byte[] kDate = hmac(("AWS4" + secretKey).getBytes(StandardCharsets.UTF_8),
                credential.date());
        byte[] kRegion = hmac(kDate, credential.region());
        byte[] kService = hmac(kRegion, credential.service());
        return hmac(kService, "aws4_request");
    }

    static String signature(byte[] signingKey, String stringToSign) {
        return hex(hmac(signingKey, stringToSign));
    }

    /**
     * One {@code aws-chunked} chunk's string-to-sign (the {@code AWS4-HMAC-SHA256-PAYLOAD} chain):
     * each chunk signature covers the previous signature, seeding from the request signature.
     */
    static String chunkSignature(byte[] signingKey, String amzDate, String scope,
                                 String previousSignature, byte[] chunkData) {
        String stringToSign = "AWS4-HMAC-SHA256-PAYLOAD\n" + amzDate + "\n" + scope + "\n"
                + previousSignature + "\n" + hex(sha256(new byte[0])) + "\n"
                + hex(sha256(chunkData));
        return signature(signingKey, stringToSign);
    }

    static byte[] hmac(byte[] key, String data) {
        return hmac(key, data.getBytes(StandardCharsets.UTF_8));
    }

    static byte[] hmac(byte[] key, byte[] data) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(key, "HmacSHA256"));
            return mac.doFinal(data);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("HmacSHA256 unavailable", e);
        }
    }

    static byte[] sha256(byte[] data) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(data);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    static String hex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(Character.forDigit((b >> 4) & 0xF, 16))
                    .append(Character.forDigit(b & 0xF, 16));
        }
        return sb.toString();
    }

    /** Constant-time hex-signature comparison. */
    static boolean signatureEquals(String a, String b) {
        return MessageDigest.isEqual(a.getBytes(StandardCharsets.UTF_8),
                b.getBytes(StandardCharsets.UTF_8));
    }

    /** Splits a raw query string into still-encoded name → values (no decoding — SigV4 needs the
     * on-the-wire octets), normalizing flag params to empty values. */
    static Map<String, List<String>> rawQueryParams(String uri) {
        Map<String, List<String>> params = new LinkedHashMap<>();
        int q = uri.indexOf('?');
        if (q < 0 || q == uri.length() - 1) {
            return params;
        }
        for (String pair : uri.substring(q + 1).split("&")) {
            if (pair.isEmpty()) {
                continue;
            }
            int eq = pair.indexOf('=');
            String name = eq < 0 ? pair : pair.substring(0, eq);
            String value = eq < 0 ? "" : pair.substring(eq + 1);
            params.computeIfAbsent(name, k -> new ArrayList<>()).add(value);
        }
        return params;
    }
}
