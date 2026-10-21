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
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.util.ReferenceCountUtil;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.UUID;

/**
 * Sits ahead of the {@code HttpObjectAggregator} and rejects a request whose declared body (the
 * {@code x-amz-decoded-content-length} of an aws-chunked upload, else {@code Content-Length}) is
 * over {@code limit}, before any of it is buffered. The error goes downstream as a {@link Rejected},
 * so {@link S3Handler} sends it in request order; the connection closes once it is sent and the rest
 * of the body has been drained (a client still sending it then reads the error, not a reset). A
 * client waiting on {@code Expect: 100-continue} sends no body. Nothing after a rejection is read.
 */
final class S3BodyLimitHandler extends ChannelInboundHandlerAdapter {

    private final long limit;
    private boolean discarding;
    private CompletableFuture<Void> bodyDone;

    /** A refused request: its {@code 400} response, and when the client has finished its body. */
    record Rejected(FullHttpResponse response, CompletableFuture<Void> bodyDone) {
    }

    S3BodyLimitHandler(long limit) {
        this.limit = limit;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof HttpRequest request) {
            long declared = declaredLength(request);
            if (!discarding && declared > limit) {
                ReferenceCountUtil.release(msg);
                discarding = true;
                bodyDone = new CompletableFuture<>();
                if (HttpUtil.is100ContinueExpected(request)) {
                    bodyDone.complete(null);
                }
                ctx.fireChannelRead(new Rejected(rejection(request, declared), bodyDone));
                return;
            }
        }
        if (discarding) {
            if (msg instanceof LastHttpContent) {
                bodyDone.complete(null);
            }
            ReferenceCountUtil.release(msg);
            return;
        }
        ctx.fireChannelRead(msg);
    }

    private static long declaredLength(HttpRequest request) {
        String decoded = request.headers().get("x-amz-decoded-content-length");
        String value = decoded != null ? decoded : request.headers().get(HttpHeaderNames.CONTENT_LENGTH);
        if (value == null) {
            return -1;
        }
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    private FullHttpResponse rejection(HttpRequest request, long declared) {
        String requestId = UUID.randomUUID().toString();
        String resource = new QueryStringDecoder(request.uri()).rawPath();
        String message = "Request body of " + declared + " bytes exceeds the maximum of " + limit
                + " bytes for a single PUT or part; use multipart upload";
        FullHttpResponse r = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,
                HttpResponseStatus.valueOf(S3ErrorCode.ENTITY_TOO_LARGE.httpStatus()),
                Unpooled.copiedBuffer(S3Xml.error(S3ErrorCode.ENTITY_TOO_LARGE, message, resource,
                        requestId), StandardCharsets.UTF_8));
        r.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/xml");
        r.headers().set(HttpHeaderNames.CONTENT_LENGTH, r.content().readableBytes());
        r.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
        r.headers().set("x-amz-request-id", requestId);
        r.headers().set(HttpHeaderNames.SERVER, "Candybox");
        return r;
    }
}
