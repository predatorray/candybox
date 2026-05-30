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

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.QueryStringDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import me.predatorray.candybox.client.CandyboxClient.CandyInfo;
import me.predatorray.candybox.client.CandyboxClient.Listing;
import me.predatorray.candybox.common.checksum.Crc32c;
import me.predatorray.candybox.s3.S3Router.S3Action;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Netty inbound handler: turns an aggregated {@link FullHttpRequest} into Candybox client calls and
 * writes back an S3-shaped response. Added to the pipeline behind a blocking {@code EventExecutorGroup}
 * (see {@link S3GatewayServer}) so the synchronous {@link CandyStore} calls never block an I/O event
 * loop.
 *
 * <p>v1 buffers whole request/response bodies (the underlying client API is itself buffer-based today),
 * path-style addressing only, anonymous (the {@code Authorization} header is accepted and ignored). See
 * {@code S3_GATEWAY_PLAN.md}.
 */
final class S3Handler extends SimpleChannelInboundHandler<FullHttpRequest> {

    private static final Logger LOG = LoggerFactory.getLogger(S3Handler.class);
    private static final String META_PREFIX = "x-amz-meta-";
    private static final int DEFAULT_MAX_KEYS = 1000;

    private final CandyStore store;
    private final S3GatewayConfig config;

    S3Handler(CandyStore store, S3GatewayConfig config) {
        this.store = store;
        this.config = config;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) {
        String requestId = UUID.randomUUID().toString();
        String method = request.method().name().toUpperCase(Locale.ROOT);
        QueryStringDecoder decoder = new QueryStringDecoder(request.uri());
        PathParts parts = PathParts.parse(decoder.rawPath());
        Set<String> queryNames = new LinkedHashSet<>();
        decoder.parameters().keySet().forEach(k -> queryNames.add(k.toLowerCase(Locale.ROOT)));
        String resource = decoder.rawPath();

        try {
            S3Action action = S3Router.route(method, parts.bucket(), parts.key(), queryNames);
            dispatch(ctx, request, action, parts, decoder, requestId);
        } catch (S3Exception e) {
            sendError(ctx, request, e.error(), e.getMessage(), resource, requestId);
        } catch (RuntimeException e) {
            S3Exception mapped = ErrorMapper.toS3(e);
            if (mapped.error() == S3ErrorCode.INTERNAL_ERROR) {
                LOG.warn("Unexpected error handling {} {} (req {})", method, resource, requestId, e);
            }
            sendError(ctx, request, mapped.error(), mapped.getMessage(), resource, requestId);
        }
    }

    private void dispatch(ChannelHandlerContext ctx, FullHttpRequest request, S3Action action,
                          PathParts parts, QueryStringDecoder decoder, String requestId) {
        switch (action) {
            case LIST_BUCKETS -> sendXml(ctx, request, HttpResponseStatus.OK,
                    S3Xml.listAllMyBuckets(store.listBoxes()), requestId);
            case CREATE_BUCKET -> {
                store.createBox(parts.bucket());
                FullHttpResponse r = empty(HttpResponseStatus.OK);
                r.headers().set(HttpHeaderNames.LOCATION, "/" + parts.bucket());
                send(ctx, request, r, requestId);
            }
            case DELETE_BUCKET -> {
                store.deleteBox(parts.bucket());
                send(ctx, request, empty(HttpResponseStatus.NO_CONTENT), requestId);
            }
            case HEAD_BUCKET -> {
                if (store.headBox(parts.bucket())) {
                    send(ctx, request, empty(HttpResponseStatus.OK), requestId);
                } else {
                    sendError(ctx, request, S3ErrorCode.NO_SUCH_BUCKET, null, "/" + parts.bucket(), requestId);
                }
            }
            case LIST_OBJECTS -> listObjects(ctx, request, parts.bucket(), decoder, requestId);
            case DELETE_OBJECTS -> deleteObjects(ctx, request, parts.bucket(), requestId);
            case GET_BUCKET_LOCATION -> sendXml(ctx, request, HttpResponseStatus.OK,
                    S3Xml.locationConstraint(config.region()), requestId);
            case GET_BUCKET_VERSIONING -> sendXml(ctx, request, HttpResponseStatus.OK,
                    S3Xml.versioningConfiguration(), requestId);
            case GET_BUCKET_ACL -> sendXml(ctx, request, HttpResponseStatus.OK,
                    S3Xml.accessControlPolicy(), requestId);
            case PUT_OBJECT -> putOrCopy(ctx, request, parts, requestId);
            case GET_OBJECT -> getObject(ctx, request, parts, requestId);
            case HEAD_OBJECT -> headObject(ctx, request, parts, requestId);
            case DELETE_OBJECT -> {
                deleteObjectIdempotent(parts.bucket(), parts.key());
                send(ctx, request, empty(HttpResponseStatus.NO_CONTENT), requestId);
            }
            case UNSUPPORTED -> sendError(ctx, request, S3ErrorCode.NOT_IMPLEMENTED, null,
                    decoder.rawPath(), requestId);
            default -> sendError(ctx, request, S3ErrorCode.NOT_IMPLEMENTED, null, decoder.rawPath(), requestId);
        }
    }

    // ---- objects ---------------------------------------------------------------------------

    private void putOrCopy(ChannelHandlerContext ctx, FullHttpRequest request, PathParts parts,
                           String requestId) {
        String copySource = request.headers().get("x-amz-copy-source");
        if (copySource != null && !copySource.isBlank()) {
            copyObject(ctx, request, parts, copySource, requestId);
            return;
        }
        byte[] body = bodyBytes(request);
        if (body.length > config.maxObjectBytes()) {
            throw new S3Exception(S3ErrorCode.ENTITY_TOO_LARGE,
                    "Object exceeds the configured maximum of " + config.maxObjectBytes() + " bytes");
        }
        String contentType = request.headers().get(HttpHeaderNames.CONTENT_TYPE);
        if (contentType == null || contentType.isBlank()) {
            contentType = "application/octet-stream";
        }
        Map<String, String> userMeta = userMetadata(request);
        store.putCandy(parts.bucket(), parts.key(), body, contentType, userMeta);

        FullHttpResponse r = empty(HttpResponseStatus.OK);
        r.headers().set(HttpHeaderNames.ETAG, Etag.of(Crc32c.of(body)));
        send(ctx, request, r, requestId);
    }

    private void copyObject(ChannelHandlerContext ctx, FullHttpRequest request, PathParts parts,
                            String copySource, String requestId) {
        PathParts src = PathParts.parse(stripLeadingSlash(uriDecode(copySource)));
        if (src.bucket() == null || src.key() == null || src.key().isEmpty()) {
            throw new S3Exception(S3ErrorCode.INVALID_ARGUMENT, "Malformed x-amz-copy-source");
        }
        if (!src.bucket().equals(parts.bucket())) {
            // Candybox copyCandy is intra-Box only; cross-bucket copy is deferred (plan §16).
            throw new S3Exception(S3ErrorCode.NOT_IMPLEMENTED, "Cross-bucket copy is not supported");
        }
        CandyInfo info = store.copyCandy(parts.bucket(), src.key(), parts.key());
        sendXml(ctx, request, HttpResponseStatus.OK,
                S3Xml.copyObjectResult(Etag.unquoted(info.crc32c()), info.createdAtMillis()), requestId);
    }

    private void getObject(ChannelHandlerContext ctx, FullHttpRequest request, PathParts parts,
                           String requestId) {
        requireKey(parts);
        // v1: Range is ignored; the whole object is returned with 200 (plan §16).
        CandyInfo info = store.headCandy(parts.bucket(), parts.key());
        byte[] data = store.getCandy(parts.bucket(), parts.key());
        FullHttpResponse r = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK,
                Unpooled.wrappedBuffer(data));
        applyObjectHeaders(r, info);
        send(ctx, request, r, requestId);
    }

    private void headObject(ChannelHandlerContext ctx, FullHttpRequest request, PathParts parts,
                            String requestId) {
        requireKey(parts);
        CandyInfo info = store.headCandy(parts.bucket(), parts.key());
        FullHttpResponse r = empty(HttpResponseStatus.OK);
        applyObjectHeaders(r, info);
        // HEAD carries the headers (incl. Content-Length) but no body.
        r.headers().set(HttpHeaderNames.CONTENT_LENGTH, Long.toString(info.contentLength()));
        send(ctx, request, r, requestId, true);
    }

    private void applyObjectHeaders(FullHttpResponse r, CandyInfo info) {
        String contentType = info.contentType();
        r.headers().set(HttpHeaderNames.CONTENT_TYPE,
                (contentType == null || contentType.isBlank()) ? "application/octet-stream" : contentType);
        r.headers().set(HttpHeaderNames.ETAG, Etag.of(info.crc32c()));
        r.headers().set(HttpHeaderNames.LAST_MODIFIED, httpDate(info.createdAtMillis()));
        r.headers().set(HttpHeaderNames.ACCEPT_RANGES, "bytes");
        if (info.userMetadata() != null) {
            info.userMetadata().forEach((k, v) -> r.headers().set(META_PREFIX + k, v));
        }
    }

    private void deleteObjectIdempotent(String bucket, String key) {
        try {
            store.deleteCandy(bucket, key);
        } catch (RuntimeException e) {
            // DeleteObject is idempotent in S3: a missing key is still a success.
            S3Exception mapped = ErrorMapper.toS3(e);
            if (mapped.error() != S3ErrorCode.NO_SUCH_KEY) {
                throw e;
            }
        }
    }

    // ---- listing ---------------------------------------------------------------------------

    private void listObjects(ChannelHandlerContext ctx, FullHttpRequest request, String bucket,
                             QueryStringDecoder decoder, String requestId) {
        Map<String, List<String>> q = decoder.parameters();
        String prefix = first(q, "prefix", "");
        String delimiter = first(q, "delimiter", null);
        int maxKeys = clampMaxKeys(first(q, "max-keys", null));
        String continuationToken = first(q, "continuation-token", null);
        String startAfterParam = first(q, "start-after", null);
        String startAfter = continuationToken != null ? decodeToken(continuationToken) : startAfterParam;

        Listing listing = store.listCandies(bucket, prefix, startAfter, maxKeys);

        List<S3Xml.Content> contents = new ArrayList<>();
        Set<String> commonPrefixes = new LinkedHashSet<>();
        for (Listing.Entry e : listing.entries()) {
            String key = e.key();
            if (delimiter != null) {
                int from = prefix == null ? 0 : prefix.length();
                int idx = key.indexOf(delimiter, from);
                if (idx >= 0) {
                    commonPrefixes.add(key.substring(0, idx + delimiter.length()));
                    continue;
                }
            }
            // The listing API returns no CRC32C, so list entries carry no ETag in v1 (clients tolerate
            // its absence). A HEAD on the individual key returns the deterministic ETag.
            contents.add(new S3Xml.Content(key, e.contentLength(), e.createdAtMillis(), null));
        }
        String nextToken = listing.isTruncated() ? encodeToken(listing.nextStartAfter()) : null;
        String xml = S3Xml.listBucket(bucket, prefix, delimiter, maxKeys, contents,
                new ArrayList<>(commonPrefixes), continuationToken, nextToken, startAfterParam);
        sendXml(ctx, request, HttpResponseStatus.OK, xml, requestId);
    }

    private void deleteObjects(ChannelHandlerContext ctx, FullHttpRequest request, String bucket,
                               String requestId) {
        S3RequestXml.DeleteRequest req = S3RequestXml.parseDelete(bodyBytes(request));
        List<String> deleted = new ArrayList<>();
        List<String[]> errors = new ArrayList<>();
        for (String key : req.keys()) {
            try {
                deleteObjectIdempotent(bucket, key);
                deleted.add(key);
            } catch (RuntimeException e) {
                S3Exception mapped = ErrorMapper.toS3(e);
                errors.add(new String[]{key, mapped.error().code(), mapped.getMessage()});
            }
        }
        // In quiet mode S3 omits successfully-deleted keys from the response (errors are still listed).
        List<String> reported = req.quiet() ? List.of() : deleted;
        sendXml(ctx, request, HttpResponseStatus.OK, S3Xml.deleteResult(reported, errors), requestId);
    }

    // ---- request parsing helpers -----------------------------------------------------------

    private byte[] bodyBytes(FullHttpRequest request) {
        byte[] raw = new byte[request.content().readableBytes()];
        request.content().getBytes(request.content().readerIndex(), raw);
        String encoding = request.headers().get(HttpHeaderNames.CONTENT_ENCODING);
        String sha256 = request.headers().get("x-amz-content-sha256");
        if (AwsChunked.isChunked(encoding, sha256)) {
            long decodedLen = parseLong(request.headers().get("x-amz-decoded-content-length"), -1);
            return AwsChunked.decode(raw, decodedLen);
        }
        return raw;
    }

    private static Map<String, String> userMetadata(FullHttpRequest request) {
        Map<String, String> meta = new LinkedHashMap<>();
        for (Map.Entry<String, String> h : request.headers()) {
            String name = h.getKey().toLowerCase(Locale.ROOT);
            if (name.startsWith(META_PREFIX) && name.length() > META_PREFIX.length()) {
                meta.put(name.substring(META_PREFIX.length()), h.getValue());
            }
        }
        return meta;
    }

    private static void requireKey(PathParts parts) {
        if (parts.key() == null || parts.key().isEmpty()) {
            throw new S3Exception(S3ErrorCode.METHOD_NOT_ALLOWED, "Missing object key");
        }
    }

    private static int clampMaxKeys(String raw) {
        if (raw == null) {
            return DEFAULT_MAX_KEYS;
        }
        try {
            int v = Integer.parseInt(raw.trim());
            return Math.max(1, Math.min(DEFAULT_MAX_KEYS, v));
        } catch (NumberFormatException e) {
            return DEFAULT_MAX_KEYS;
        }
    }

    private static String first(Map<String, List<String>> q, String key, String dflt) {
        List<String> vs = q.get(key);
        return (vs == null || vs.isEmpty()) ? dflt : vs.get(0);
    }

    private static String encodeToken(String startAfter) {
        return Base64.getUrlEncoder().withoutPadding()
                .encodeToString(startAfter.getBytes(StandardCharsets.UTF_8));
    }

    private static String decodeToken(String token) {
        try {
            return new String(Base64.getUrlDecoder().decode(token), StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            throw new S3Exception(S3ErrorCode.INVALID_ARGUMENT, "Invalid continuation-token");
        }
    }

    private static long parseLong(String s, long dflt) {
        if (s == null) {
            return dflt;
        }
        try {
            return Long.parseLong(s.trim());
        } catch (NumberFormatException e) {
            return dflt;
        }
    }

    private static String stripLeadingSlash(String s) {
        return s.startsWith("/") ? s.substring(1) : s;
    }

    // ---- response writing ------------------------------------------------------------------

    private void sendXml(ChannelHandlerContext ctx, FullHttpRequest request, HttpResponseStatus status,
                         String xml, String requestId) {
        FullHttpResponse r = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status,
                Unpooled.copiedBuffer(xml, StandardCharsets.UTF_8));
        r.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/xml");
        send(ctx, request, r, requestId);
    }

    private void sendError(ChannelHandlerContext ctx, FullHttpRequest request, S3ErrorCode error,
                           String message, String resource, String requestId) {
        String msg = message == null ? error.defaultMessage() : message;
        FullHttpResponse r = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,
                HttpResponseStatus.valueOf(error.httpStatus()),
                Unpooled.copiedBuffer(S3Xml.error(error, msg, resource, requestId), StandardCharsets.UTF_8));
        r.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/xml");
        if (error.retryable()) {
            r.headers().set(HttpHeaderNames.RETRY_AFTER, "1");
        }
        send(ctx, request, r, requestId, isHead(request));
    }

    private FullHttpResponse empty(HttpResponseStatus status) {
        return new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, Unpooled.EMPTY_BUFFER);
    }

    private void send(ChannelHandlerContext ctx, FullHttpRequest request, FullHttpResponse response,
                      String requestId) {
        send(ctx, request, response, requestId, isHead(request));
    }

    /**
     * Writes the response, honoring keep-alive. When {@code headOnly} is true the body buffer is
     * cleared but the (already-set) {@code Content-Length} is preserved, as HTTP requires for HEAD.
     */
    private void send(ChannelHandlerContext ctx, FullHttpRequest request, FullHttpResponse response,
                      String requestId, boolean headOnly) {
        response.headers().set("x-amz-request-id", requestId);
        response.headers().set(HttpHeaderNames.SERVER, "Candybox");
        boolean keepAlive = HttpUtil.isKeepAlive(request);
        if (headOnly) {
            String declared = response.headers().get(HttpHeaderNames.CONTENT_LENGTH, "0");
            response.content().clear();
            response.headers().set(HttpHeaderNames.CONTENT_LENGTH, declared);
        } else {
            response.headers().set(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());
        }
        if (keepAlive) {
            response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
            ctx.writeAndFlush(response);
        } else {
            response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
            ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
        }
    }

    private static boolean isHead(FullHttpRequest request) {
        return "HEAD".equalsIgnoreCase(request.method().name());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOG.warn("Channel exception", cause);
        ctx.close();
    }

    private static String httpDate(long epochMillis) {
        return java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME
                .format(java.time.ZonedDateTime.ofInstant(java.time.Instant.ofEpochMilli(epochMillis),
                        java.time.ZoneOffset.UTC));
    }

    private static String uriDecode(String s) {
        return java.net.URLDecoder.decode(s, StandardCharsets.UTF_8);
    }

    /** Path-style split of a decoded request path into bucket + key (both may be null/empty). */
    record PathParts(String bucket, String key) {
        static PathParts parse(String rawPath) {
            String p = stripLeading(rawPath);
            if (p.isEmpty()) {
                return new PathParts(null, null);
            }
            int slash = p.indexOf('/');
            if (slash < 0) {
                return new PathParts(percentDecode(p), null);
            }
            String bucket = percentDecode(p.substring(0, slash));
            String key = percentDecode(p.substring(slash + 1));
            return new PathParts(bucket, key);
        }

        private static String stripLeading(String s) {
            return s.startsWith("/") ? s.substring(1) : s;
        }
    }

    /**
     * Percent-decodes a path component (UTF-8) WITHOUT treating {@code '+'} as a space — in an S3 key a
     * literal {@code '+'} must survive. Slashes already in the string are preserved.
     */
    static String percentDecode(String s) {
        if (s.indexOf('%') < 0) {
            return s;
        }
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '%' && i + 2 < s.length()) {
                int hi = Character.digit(s.charAt(i + 1), 16);
                int lo = Character.digit(s.charAt(i + 2), 16);
                if (hi >= 0 && lo >= 0) {
                    out.write((hi << 4) + lo);
                    i += 2;
                    continue;
                }
            }
            // Multi-byte chars are emitted as UTF-8 bytes.
            byte[] bytes = String.valueOf(c).getBytes(StandardCharsets.UTF_8);
            out.write(bytes, 0, bytes.length);
        }
        return new String(out.toByteArray(), StandardCharsets.UTF_8);
    }
}
