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
import me.predatorray.candybox.client.CandyboxClient.MultipartListing;
import me.predatorray.candybox.client.CandyboxClient.PartListing;
import me.predatorray.candybox.client.CandyboxClient.PartUploadInfo;
import me.predatorray.candybox.client.CandyboxClient.RangeBytes;
import me.predatorray.candybox.client.CandyboxClient.UploadEntry;
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
            case LIST_MULTIPART_UPLOADS -> listMultipartUploads(ctx, request, parts.bucket(), decoder, requestId);
            case CREATE_MULTIPART_UPLOAD -> createMultipartUpload(ctx, request, parts, requestId);
            case UPLOAD_PART -> uploadPart(ctx, request, parts, decoder, requestId);
            case COMPLETE_MULTIPART_UPLOAD -> completeMultipartUpload(ctx, request, parts, decoder, requestId);
            case ABORT_MULTIPART_UPLOAD -> abortMultipartUpload(ctx, request, parts, decoder, requestId);
            case LIST_PARTS -> listParts(ctx, request, parts, decoder, requestId);
            case LIST_OBJECT_VERSIONS -> listObjectVersions(ctx, request, parts.bucket(), decoder, requestId);
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
        String rangeHeader = request.headers().get(HttpHeaderNames.RANGE);
        if (rangeHeader == null || rangeHeader.isBlank()) {
            // Full object, 200 OK.
            CandyInfo info = store.headCandy(parts.bucket(), parts.key());
            byte[] data = store.getCandy(parts.bucket(), parts.key());
            FullHttpResponse r = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,
                    HttpResponseStatus.OK, Unpooled.wrappedBuffer(data));
            applyObjectHeaders(r, info);
            send(ctx, request, r, requestId);
            return;
        }
        getObjectRange(ctx, request, parts, rangeHeader, requestId);
    }

    /**
     * Handles {@code Range: bytes=…} with a 206 Partial Content response and a {@code Content-Range}
     * header. Multi-range ({@code bytes=A-B,C-D}) is rejected with {@link S3ErrorCode#NOT_IMPLEMENTED}.
     * An unparseable header that doesn't start with {@code bytes=} is ignored per RFC 9110 §14.2 and
     * served as a full 200, matching mainstream S3 behavior.
     */
    private void getObjectRange(ChannelHandlerContext ctx, FullHttpRequest request, PathParts parts,
                                String rangeHeader, String requestId) {
        ParsedRange range = parseRange(rangeHeader);
        if (range == null) {
            // Unparseable / non-bytes unit; per RFC 9110 §14.2 fall back to a full 200.
            CandyInfo info = store.headCandy(parts.bucket(), parts.key());
            byte[] data = store.getCandy(parts.bucket(), parts.key());
            FullHttpResponse r = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,
                    HttpResponseStatus.OK, Unpooled.wrappedBuffer(data));
            applyObjectHeaders(r, info);
            send(ctx, request, r, requestId);
            return;
        }
        RangeBytes slice;
        try {
            slice = store.getCandyRange(parts.bucket(), parts.key(), range.firstByte,
                    range.lastByte);
        } catch (IllegalArgumentException e) {
            // The engine raises IAE for an unsatisfiable range; surface as the S3 416 error code.
            throw new S3Exception(S3ErrorCode.INVALID_RANGE, e.getMessage(), e);
        }
        CandyInfo info = store.headCandy(parts.bucket(), parts.key());
        long total = slice.totalLength();
        long emittedFirst;
        long emittedLast;
        if (range.firstByte < 0) {
            // Suffix range: server clamps to start of object if suffix >= total.
            emittedFirst = Math.max(total - range.lastByte, 0);
            emittedLast = total - 1;
        } else {
            emittedFirst = range.firstByte;
            emittedLast = Math.min(range.lastByte < 0 ? total - 1 : range.lastByte, total - 1);
        }
        FullHttpResponse r = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,
                HttpResponseStatus.PARTIAL_CONTENT, Unpooled.wrappedBuffer(slice.data()));
        applyObjectHeaders(r, info);
        // Reset Content-Length to the slice length (the full-object applyObjectHeaders does not set it
        // explicitly, but Netty's auto-set on the FullHttpResponse uses the body buffer, which is the
        // slice).
        r.headers().set(HttpHeaderNames.CONTENT_RANGE,
                "bytes " + emittedFirst + "-" + emittedLast + "/" + total);
        send(ctx, request, r, requestId);
    }

    /**
     * Parses an HTTP {@code Range} header value, returning the parsed bounds or {@code null} for an
     * unparseable header (RFC fallback to full 200). A multi-range header throws
     * {@link S3ErrorCode#NOT_IMPLEMENTED} — Candybox v1 does not implement
     * {@code multipart/byteranges}.
     */
    private static ParsedRange parseRange(String headerValue) {
        String v = headerValue.trim();
        if (!v.toLowerCase(Locale.ROOT).startsWith("bytes=")) {
            return null;
        }
        String spec = v.substring("bytes=".length()).trim();
        if (spec.indexOf(',') >= 0) {
            throw new S3Exception(S3ErrorCode.NOT_IMPLEMENTED,
                    "Multi-range requests are not supported");
        }
        int dash = spec.indexOf('-');
        if (dash < 0) {
            return null;
        }
        String left = spec.substring(0, dash).trim();
        String right = spec.substring(dash + 1).trim();
        try {
            if (left.isEmpty() && right.isEmpty()) {
                return null;
            }
            if (left.isEmpty()) {
                long n = Long.parseLong(right);
                if (n <= 0) {
                    throw new S3Exception(S3ErrorCode.INVALID_RANGE, "Range " + headerValue
                            + " is not satisfiable");
                }
                return new ParsedRange(-1, n); // suffix
            }
            long first = Long.parseLong(left);
            if (first < 0) {
                throw new S3Exception(S3ErrorCode.INVALID_RANGE, "Range " + headerValue
                        + " is not satisfiable");
            }
            long last = right.isEmpty() ? -1 : Long.parseLong(right);
            if (last >= 0 && last < first) {
                throw new S3Exception(S3ErrorCode.INVALID_RANGE, "Range " + headerValue
                        + " is not satisfiable");
            }
            return new ParsedRange(first, last);
        } catch (NumberFormatException nfe) {
            return null;
        }
    }

    private record ParsedRange(long firstByte, long lastByte) {
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

    // ---- multipart upload ------------------------------------------------------------------

    private void createMultipartUpload(ChannelHandlerContext ctx, FullHttpRequest request,
                                       PathParts parts, String requestId) {
        requireKey(parts);
        String contentType = request.headers().get(HttpHeaderNames.CONTENT_TYPE);
        if (contentType == null || contentType.isBlank()) {
            contentType = "application/octet-stream";
        }
        Map<String, String> userMeta = userMetadata(request);
        String uploadId = store.createMultipartUpload(parts.bucket(), parts.key(), contentType,
                userMeta);
        sendXml(ctx, request, HttpResponseStatus.OK,
                S3Xml.initiateMultipartUploadResult(parts.bucket(), parts.key(), uploadId), requestId);
    }

    private void uploadPart(ChannelHandlerContext ctx, FullHttpRequest request, PathParts parts,
                            QueryStringDecoder decoder, String requestId) {
        requireKey(parts);
        String uploadId = requiredQuery(decoder, "uploadId");
        int partNumber = parseIntQuery(decoder, "partNumber");
        if (partNumber < 1) {
            throw new S3Exception(S3ErrorCode.INVALID_ARGUMENT, "partNumber must be >= 1");
        }
        String copySource = request.headers().get("x-amz-copy-source");
        if (copySource != null && !copySource.isBlank()) {
            uploadPartCopy(ctx, request, parts, uploadId, partNumber, copySource, requestId);
            return;
        }
        byte[] body = bodyBytes(request);
        if (body.length > config.maxObjectBytes()) {
            // Per-part size: tracks the configured single-object cap until streaming lands.
            throw new S3Exception(S3ErrorCode.ENTITY_TOO_LARGE,
                    "Part exceeds the configured maximum of " + config.maxObjectBytes() + " bytes");
        }
        PartUploadInfo info = store.uploadPart(parts.bucket(), parts.key(), uploadId, partNumber,
                body);
        FullHttpResponse r = empty(HttpResponseStatus.OK);
        r.headers().set(HttpHeaderNames.ETAG, Etag.of(info.crc32c()));
        send(ctx, request, r, requestId);
    }

    /**
     * Handles {@code PUT /b/k?partNumber=N&uploadId=…} with an {@code x-amz-copy-source} header
     * (S3 UploadPartCopy). Source is required to be in the same Box; multi-Box copy is rejected
     * with {@link S3ErrorCode#NOT_IMPLEMENTED}, matching the existing {@code CopyObject} rule.
     */
    private void uploadPartCopy(ChannelHandlerContext ctx, FullHttpRequest request, PathParts parts,
                                String uploadId, int partNumber, String copySource, String requestId) {
        PathParts src = PathParts.parse(stripLeadingSlash(uriDecode(copySource)));
        if (src.bucket() == null || src.key() == null || src.key().isEmpty()) {
            throw new S3Exception(S3ErrorCode.INVALID_ARGUMENT, "Malformed x-amz-copy-source");
        }
        if (!src.bucket().equals(parts.bucket())) {
            throw new S3Exception(S3ErrorCode.NOT_IMPLEMENTED, "Cross-bucket UploadPartCopy is not supported");
        }
        String rangeHeader = request.headers().get("x-amz-copy-source-range");
        long firstByte = -1;
        long lastByte = -1;
        if (rangeHeader != null && !rangeHeader.isBlank()) {
            ParsedRange r = parseRange(rangeHeader);
            if (r == null) {
                throw new S3Exception(S3ErrorCode.INVALID_ARGUMENT,
                        "Malformed x-amz-copy-source-range: " + rangeHeader);
            }
            firstByte = r.firstByte;
            lastByte = r.lastByte;
        }
        PartUploadInfo info;
        try {
            info = store.uploadPartCopy(parts.bucket(), parts.key(), uploadId, partNumber,
                    src.key(), firstByte, lastByte);
        } catch (IllegalArgumentException e) {
            throw new S3Exception(S3ErrorCode.INVALID_RANGE, e.getMessage(), e);
        }
        // S3 returns a CopyPartResult XML body for UploadPartCopy.
        long lastModified = System.currentTimeMillis();
        sendXml(ctx, request, HttpResponseStatus.OK,
                S3Xml.copyPartResult(Etag.unquoted(info.crc32c()), lastModified), requestId);
    }

    private void completeMultipartUpload(ChannelHandlerContext ctx, FullHttpRequest request,
                                         PathParts parts, QueryStringDecoder decoder,
                                         String requestId) {
        requireKey(parts);
        String uploadId = requiredQuery(decoder, "uploadId");
        byte[] body = bodyBytes(request);
        S3RequestXml.CompleteMultipartUploadBody parsed = S3RequestXml.parseCompleteMultipart(body);
        List<PartUploadInfo> partList = new ArrayList<>(parsed.parts().size());
        for (S3RequestXml.CompletePart p : parsed.parts()) {
            int crc = Etag.parseCrc32cHex(p.etag());
            partList.add(new PartUploadInfo(p.partNumber(), crc, 0)); // length is server-known
        }
        CandyInfo info = store.completeMultipartUpload(parts.bucket(), parts.key(), uploadId,
                partList);
        String location = "/" + parts.bucket() + "/" + parts.key();
        String etag = multipartEtag(partList.size(), info.crc32c());
        sendXml(ctx, request, HttpResponseStatus.OK,
                S3Xml.completeMultipartUploadResult(parts.bucket(), parts.key(), location, etag),
                requestId);
    }

    /**
     * Multipart ETag in Candybox's CRC32C-hex scheme: {@code <objectCrc>-N}. (S3's spec is
     * {@code MD5(MD5||…)-N} but Candybox already returns CRC32C-hex ETags for single PUTs; we keep
     * the family consistent.)
     */
    private static String multipartEtag(int partCount, int objectCrc32c) {
        return Etag.unquoted(objectCrc32c) + "-" + partCount;
    }

    private void abortMultipartUpload(ChannelHandlerContext ctx, FullHttpRequest request,
                                      PathParts parts, QueryStringDecoder decoder,
                                      String requestId) {
        requireKey(parts);
        String uploadId = requiredQuery(decoder, "uploadId");
        store.abortMultipartUpload(parts.bucket(), parts.key(), uploadId);
        send(ctx, request, empty(HttpResponseStatus.NO_CONTENT), requestId);
    }

    private void listMultipartUploads(ChannelHandlerContext ctx, FullHttpRequest request,
                                      String bucket, QueryStringDecoder decoder, String requestId) {
        Map<String, List<String>> q = decoder.parameters();
        String prefix = first(q, "prefix", "");
        String keyMarker = first(q, "key-marker", null);
        String uploadIdMarker = first(q, "upload-id-marker", null);
        int maxUploads = clampMaxUploads(first(q, "max-uploads", null));

        MultipartListing listing = store.listMultipartUploads(bucket, prefix, keyMarker,
                uploadIdMarker, maxUploads);
        List<S3Xml.UploadRow> rows = new ArrayList<>(listing.uploads().size());
        for (UploadEntry u : listing.uploads()) {
            rows.add(new S3Xml.UploadRow(u.key(), u.uploadId(), u.createdAtMillis()));
        }
        sendXml(ctx, request, HttpResponseStatus.OK,
                S3Xml.listMultipartUploadsResult(bucket, prefix, keyMarker, uploadIdMarker,
                        maxUploads, listing.isTruncated(), listing.nextKeyMarker(),
                        listing.nextUploadIdMarker(), rows), requestId);
    }

    private void listParts(ChannelHandlerContext ctx, FullHttpRequest request, PathParts parts,
                           QueryStringDecoder decoder, String requestId) {
        requireKey(parts);
        String uploadId = requiredQuery(decoder, "uploadId");
        Map<String, List<String>> q = decoder.parameters();
        int partNumberMarker;
        try {
            partNumberMarker = Integer.parseInt(first(q, "part-number-marker", "0"));
        } catch (NumberFormatException e) {
            throw new S3Exception(S3ErrorCode.INVALID_ARGUMENT, "Invalid part-number-marker");
        }
        int maxParts = clampMaxUploads(first(q, "max-parts", null));
        PartListing listing = store.listParts(parts.bucket(), parts.key(), uploadId, partNumberMarker,
                maxParts);
        List<S3Xml.PartRow> rows = new ArrayList<>(listing.parts().size());
        for (PartUploadInfo p : listing.parts()) {
            rows.add(new S3Xml.PartRow(p.partNumber(), Etag.unquoted(p.crc32c()), p.partLength()));
        }
        sendXml(ctx, request, HttpResponseStatus.OK,
                S3Xml.listPartsResult(parts.bucket(), parts.key(), uploadId, partNumberMarker,
                        maxParts, listing.isTruncated(), listing.nextPartNumberMarker(), rows),
                requestId);
    }

    private static int clampMaxUploads(String raw) {
        if (raw == null) {
            return DEFAULT_MAX_KEYS;
        }
        try {
            int n = Integer.parseInt(raw);
            if (n < 1) {
                return DEFAULT_MAX_KEYS;
            }
            return Math.min(n, 1000);
        } catch (NumberFormatException e) {
            throw new S3Exception(S3ErrorCode.INVALID_ARGUMENT, "Invalid max-uploads/max-parts");
        }
    }

    private static String requiredQuery(QueryStringDecoder decoder, String name) {
        List<String> values = decoder.parameters().get(name);
        if (values == null || values.isEmpty() || values.get(0).isEmpty()) {
            throw new S3Exception(S3ErrorCode.INVALID_ARGUMENT, "Missing required query parameter "
                    + name);
        }
        return values.get(0);
    }

    private static int parseIntQuery(QueryStringDecoder decoder, String name) {
        try {
            return Integer.parseInt(requiredQuery(decoder, name));
        } catch (NumberFormatException e) {
            throw new S3Exception(S3ErrorCode.INVALID_ARGUMENT, "Invalid " + name);
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
        // ListObjectsV2 is selected by list-type=2; without it this is the V1 ListObjects API, whose
        // pagination cursor is the (plain, non-opaque) `marker` rather than V2's continuation-token.
        boolean v2 = "2".equals(first(q, "list-type", null));
        String prefix = first(q, "prefix", "");
        String delimiter = first(q, "delimiter", null);
        int maxKeys = clampMaxKeys(first(q, "max-keys", null));

        String continuationToken = first(q, "continuation-token", null);
        String startAfterParam = first(q, "start-after", null);
        String marker = first(q, "marker", null);
        String startAfter = v2
                ? (continuationToken != null ? decodeToken(continuationToken) : startAfterParam)
                : marker;

        Listing listing = store.listCandies(bucket, prefix, startAfter, maxKeys);
        List<S3Xml.Content> contents = new ArrayList<>();
        Set<String> commonPrefixes = new LinkedHashSet<>();
        rollUp(listing, prefix, delimiter, contents, commonPrefixes);

        String xml;
        if (v2) {
            String nextToken = listing.isTruncated() ? encodeToken(listing.nextStartAfter()) : null;
            xml = S3Xml.listBucketV2(bucket, prefix, delimiter, maxKeys, contents,
                    new ArrayList<>(commonPrefixes), continuationToken, nextToken, startAfterParam);
        } else {
            // S3 only returns NextMarker when a delimiter is in play; otherwise the client resumes from
            // the last returned key. Our resume cursor is exactly that last key, so the two agree.
            String nextMarker = (listing.isTruncated() && delimiter != null) ? listing.nextStartAfter() : null;
            xml = S3Xml.listBucketV1(bucket, prefix, delimiter, maxKeys, contents,
                    new ArrayList<>(commonPrefixes), marker, nextMarker, listing.isTruncated());
        }
        sendXml(ctx, request, HttpResponseStatus.OK, xml, requestId);
    }

    /**
     * ListObjectVersions for the unversioned store: lists each object as its sole latest version. The
     * store has no version dimension, so this reuses {@code listCandies} and pages by {@code key-marker}
     * (the {@code version-id-marker} is irrelevant). Mainly here so version-aware clients — e.g. the
     * s3-tests cleanup, which drains a bucket via {@code list_object_versions} — work against us.
     */
    private void listObjectVersions(ChannelHandlerContext ctx, FullHttpRequest request, String bucket,
                                    QueryStringDecoder decoder, String requestId) {
        Map<String, List<String>> q = decoder.parameters();
        String prefix = first(q, "prefix", "");
        String delimiter = first(q, "delimiter", null);
        int maxKeys = clampMaxKeys(first(q, "max-keys", null));
        String keyMarker = first(q, "key-marker", null);

        Listing listing = store.listCandies(bucket, prefix, keyMarker, maxKeys);
        List<S3Xml.Content> versions = new ArrayList<>();
        Set<String> commonPrefixes = new LinkedHashSet<>();
        rollUp(listing, prefix, delimiter, versions, commonPrefixes);

        String nextKeyMarker = listing.isTruncated() ? listing.nextStartAfter() : null;
        String xml = S3Xml.listVersions(bucket, prefix, delimiter, maxKeys, versions,
                new ArrayList<>(commonPrefixes), keyMarker, nextKeyMarker);
        sendXml(ctx, request, HttpResponseStatus.OK, xml, requestId);
    }

    /**
     * Folds a {@link Listing} page into object rows and synthesized common prefixes: a key whose first
     * {@code delimiter} occurrence after {@code prefix} rolls it up into a {@code CommonPrefixes} entry,
     * otherwise it becomes a content/version row. List entries carry no ETag (the listing API returns no
     * CRC32C; a HEAD on the key yields the deterministic ETag).
     */
    private static void rollUp(Listing listing, String prefix, String delimiter,
                               List<S3Xml.Content> rows, Set<String> commonPrefixes) {
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
            rows.add(new S3Xml.Content(key, e.contentLength(), e.createdAtMillis(), null));
        }
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
