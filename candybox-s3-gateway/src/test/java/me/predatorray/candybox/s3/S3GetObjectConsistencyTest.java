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
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.ReferenceCountUtil;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;
import me.predatorray.candybox.client.CandyboxClient.CandyInfo;
import me.predatorray.candybox.client.CandyboxClient.Listing;
import me.predatorray.candybox.client.CandyboxClient.MultipartListing;
import me.predatorray.candybox.client.CandyboxClient.PartListing;
import me.predatorray.candybox.client.CandyboxClient.PartUploadInfo;
import me.predatorray.candybox.client.CandyboxClient.RangeBytes;
import me.predatorray.candybox.common.checksum.Crc32c;
import me.predatorray.candybox.common.exception.LimitExceededException;
import me.predatorray.candybox.common.exception.ValidationException;
import me.predatorray.candybox.protocol.Message;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class S3GetObjectConsistencyTest {
    private static final int CHUNK_BYTES = 1000;
    private final ChangingStore store = new ChangingStore();
    private final byte[] original = bytes(10_000, 1);

    @BeforeEach
    void setUp() {
        store.createBox("bucket");
        replace(original);
    }

    @Test
    void replacementBeforeFirstSliceRetriesWithFreshHeadersAndBody() {
        byte[] replacement = bytes(9_500, 2);
        store.beforeRange = range -> {
            if (store.rangeCalls == 1) {
                replace(replacement);
            }
        };
        Response response = get(null);
        assertThat(response.status).isEqualTo(200);
        assertThat(response.length).isEqualTo(replacement.length);
        assertThat(response.etag).isEqualTo(Etag.of(Crc32c.of(replacement)));
        assertThat(response.complete).isTrue();
        assertThat(response.body).isEqualTo(replacement);
        assertThat(store.headCalls).isEqualTo(2);
    }

    @Test
    void repeatedReplacementExhaustsThreeAttemptsBeforeReturning503() {
        store.beforeRange = range -> replace(bytes(original.length, store.rangeCalls + 1));
        Response response = get(null);
        assertError(response, 503, "ServiceUnavailable");
        assertThat(response.text()).contains("Object kept changing while being read; retry");
        // The public retry bound is three whole GET attempts, each starting with HEAD.
        assertThat(store.rangeCalls).isEqualTo(3);
        assertThat(store.headCalls).isEqualTo(3);
    }

    @Test
    void deletionBeforeFirstSliceRetriesHeadAndReturns404() {
        store.beforeRange = range -> {
            if (store.rangeCalls == 1) {
                store.deleteCandy("bucket", "object");
            }
        };
        assertError(get(null), 404, "NoSuchKey");
        assertThat(store.rangeCalls).isEqualTo(1);
        assertThat(store.headCalls).isEqualTo(2);
    }

    @Test
    void replacementOnSecondSliceClosesStreamWithoutMixedBytesOrLastContent() {
        store.beforeRange = range -> {
            if (store.rangeCalls == 2) {
                replace(bytes(original.length, 2));
            }
        };
        Response response = get(null);
        assertThat(response.status).isEqualTo(200);
        assertThat(response.full).isFalse();
        assertThat(response.length).isEqualTo(original.length);
        assertThat(response.open).isFalse();
        assertThat(response.complete).isFalse();
        assertThat(response.body.length).isLessThan((int) response.length);
        assertThat(response.body).isEqualTo(Arrays.copyOf(original, CHUNK_BYTES));
        assertThat(store.headCalls).isEqualTo(1);
        assertThat(store.rangeCalls).isEqualTo(2);
    }

    @Test
    void nodeFrameLimitFailureReportsGatewayMisconfiguration() {
        store.failRangeWith = new LimitExceededException("frame too large");
        Response response = get(null);
        assertError(response, 500, "InternalError");
        assertThat(response.text()).contains("max.frame.size.bytes", "must not exceed the nodes");
        assertThat(store.rangeCalls).isEqualTo(1);
    }

    @Test
    void unrelatedSliceFailureReturnsInternalErrorWithoutRetry() {
        store.failRangeWith = new IllegalStateException("node read failed");
        assertError(get(null), 500, "InternalError");
        assertThat(store.rangeCalls).isEqualTo(1);
        assertThat(store.headCalls).isEqualTo(1);
    }

    @ParameterizedTest
    @ValueSource(strings = {"bytes=-0", "bytes=5-2"})
    void unsatisfiableRangesReturn416(String range) {
        assertError(get(range), 416, "InvalidRange");
        assertThat(store.rangeCalls).isZero();
    }

    @ParameterizedTest
    @ValueSource(strings = {"bytes=-", "bytes=5", "bytes=a-b"})
    void malformedRangesAreIgnoredAndReturnFullObject(String range) {
        Response response = get(range);
        assertThat(response.status).isEqualTo(200);
        assertThat(response.complete).isTrue();
        assertThat(response.body).isEqualTo(original);
    }

    @ParameterizedTest
    @ValueSource(strings = {"invalid range", "illegal argument"})
    void transientSliceRangeFailuresRetryTheWholeGet(String failure) {
        store.failRangeWith = failure.equals("invalid range")
                ? new ValidationException("InvalidRange: object shrank")
                : new IllegalArgumentException("object shrank");
        store.beforeRange = range -> {
            if (store.rangeCalls == 2) {
                store.failRangeWith = null;
            }
        };
        Response response = get(null);
        assertThat(response.status).isEqualTo(200);
        assertThat(response.complete).isTrue();
        assertThat(response.body).isEqualTo(original);
        assertThat(store.headCalls).isEqualTo(2);
    }

    @ParameterizedTest
    @ValueSource(strings = {"crc", "total length", "slice length"})
    void inconsistentSliceMetadataOrBodyRetriesTheWholeGet(String mismatch) {
        store.adjustSlice = slice -> {
            if (store.rangeCalls != 1) {
                return slice;
            }
            return new RangeBytes(
                    mismatch.equals("slice length") ? Arrays.copyOf(slice.data(), slice.data().length - 1)
                            : slice.data(),
                    mismatch.equals("total length") ? slice.totalLength() + 1 : slice.totalLength(),
                    slice.sliceLength(), slice.contentType(), slice.userMetadata(),
                    mismatch.equals("crc") ? slice.crc32c() ^ 1 : slice.crc32c());
        };
        Response response = get(null);
        assertThat(response.status).isEqualTo(200);
        assertThat(response.complete).isTrue();
        assertThat(response.body).isEqualTo(original);
        assertThat(store.headCalls).isEqualTo(2);
    }

    private void replace(byte[] data) {
        store.putCandy("bucket", "object", data, "application/octet-stream", Map.of());
    }

    private static void assertError(Response response, int status, String code) {
        assertThat(response.status).isEqualTo(status);
        assertThat(response.complete).isTrue();
        assertThat(response.text()).contains("<Code>" + code + "</Code>");
    }

    private Response get(String range) {
        Properties properties = new Properties();
        properties.setProperty("zookeeper.connect", "unused:2181");
        properties.setProperty("max.frame.size.bytes",
                Integer.toString(Message.INLINE_DATA_OVERHEAD_BYTES + CHUNK_BYTES));
        EmbeddedChannel channel = new EmbeddedChannel(new S3Handler(store,
                S3GatewayConfig.fromProperties(properties, Map.of())));
        try {
            DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1,
                    HttpMethod.GET, "/bucket/object", Unpooled.EMPTY_BUFFER);
            if (range != null) {
                request.headers().set(HttpHeaderNames.RANGE, range);
            }
            channel.writeInbound(request);
            // EmbeddedChannel executes the queued stream continuations deterministically.
            int previousSize;
            int turns = 0;
            do {
                previousSize = channel.outboundMessages().size();
                channel.runPendingTasks();
                assertThat(++turns).as("bounded stream progress").isLessThan(100);
            } while (channel.outboundMessages().size() != previousSize);
            HttpResponse head = channel.readOutbound();
            assertThat(head).isNotNull();
            int status = head.status().code();
            long length = Long.parseLong(head.headers().get(HttpHeaderNames.CONTENT_LENGTH));
            String etag = head.headers().get(HttpHeaderNames.ETAG);
            boolean full = head instanceof FullHttpResponse;
            boolean complete = false;
            ByteArrayOutputStream body = new ByteArrayOutputStream();
            Object message = head;
            do {
                try {
                    if (message instanceof HttpContent content) {
                        byte[] data = new byte[content.content().readableBytes()];
                        content.content().getBytes(content.content().readerIndex(), data);
                        body.writeBytes(data);
                        complete |= content instanceof LastHttpContent;
                    } else {
                        assertThat(message).isSameAs(head);
                    }
                } finally {
                    ReferenceCountUtil.release(message);
                }
            } while ((message = channel.readOutbound()) != null);
            return new Response(status, length, etag, full, complete, channel.isOpen(), body.toByteArray());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    private static byte[] bytes(int length, int seed) {
        byte[] data = new byte[length];
        new Random(seed).nextBytes(data);
        return data;
    }

    private record Response(int status, long length, String etag, boolean full, boolean complete,
                            boolean open, byte[] body) {
        String text() {
            return new String(body, StandardCharsets.UTF_8);
        }
    }

    private static final class ChangingStore implements CandyStore {
        private final FakeCandyStore delegate = new FakeCandyStore();
        Consumer<long[]> beforeRange = range -> { };
        RuntimeException failRangeWith;
        UnaryOperator<RangeBytes> adjustSlice = UnaryOperator.identity();
        int rangeCalls;
        int headCalls;

        @Override
        public RangeBytes getCandyRange(String box, String key, long first, long last) {
            rangeCalls++;
            beforeRange.accept(new long[] {first, last});
            if (failRangeWith != null) {
                throw failRangeWith;
            }
            return adjustSlice.apply(delegate.getCandyRange(box, key, first, last));
        }

        @Override
        public CandyInfo headCandy(String box, String key) {
            headCalls++;
            return delegate.headCandy(box, key);
        }

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
        public void putCandy(String box, String key, byte[] data, String contentType,
                             Map<String, String> userMetadata) {
            delegate.putCandy(box, key, data, contentType, userMetadata);
        }

        @Override
        public void deleteCandy(String box, String key) {
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
        public String createMultipartUpload(String box, String key, String contentType,
                                            Map<String, String> userMetadata) {
            return delegate.createMultipartUpload(box, key, contentType, userMetadata);
        }

        @Override
        public PartUploadInfo uploadPart(String box, String key, String uploadId, int partNumber, byte[] data) {
            return delegate.uploadPart(box, key, uploadId, partNumber, data);
        }

        @Override
        public PartUploadInfo uploadPartCopy(String box, String key, String uploadId, int partNumber,
                                           String srcKey, long firstByte, long lastByte) {
            return delegate.uploadPartCopy(box, key, uploadId, partNumber, srcKey, firstByte, lastByte);
        }

        @Override
        public CandyInfo completeMultipartUpload(String box, String key, String uploadId,
                                               List<PartUploadInfo> parts) {
            return delegate.completeMultipartUpload(box, key, uploadId, parts);
        }

        @Override
        public void abortMultipartUpload(String box, String key, String uploadId) {
            delegate.abortMultipartUpload(box, key, uploadId);
        }

        @Override
        public MultipartListing listMultipartUploads(String box, String prefix, String keyMarker,
                                                   String uploadIdMarker, int maxUploads) {
            return delegate.listMultipartUploads(box, prefix, keyMarker, uploadIdMarker, maxUploads);
        }

        @Override
        public PartListing listParts(String box, String key, String uploadId, int partNumberMarker, int maxParts) {
            return delegate.listParts(box, key, uploadId, partNumberMarker, maxParts);
        }
    }
}
