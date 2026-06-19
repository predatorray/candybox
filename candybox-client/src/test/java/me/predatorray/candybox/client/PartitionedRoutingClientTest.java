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
package me.predatorray.candybox.client;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import me.predatorray.candybox.common.Partitioning;
import me.predatorray.candybox.protocol.Message;
import me.predatorray.candybox.protocol.MessageCodec;
import me.predatorray.candybox.protocol.transport.LoopbackTransport;
import me.predatorray.candybox.protocol.transport.RequestHandler;
import org.junit.jupiter.api.Test;

/**
 * Pins {@link CandyboxClient}'s partition-aware behaviour against a recording two-partition stub
 * node: descriptor caching, scatter-gather list merging (order, truncation, reverse), fanned-out
 * range deletes, merged multipart-upload listings, the cross-partition zero-copy relay for
 * copy/rename, and the (still byte-copy) uploadPartCopy fallback.
 */
class PartitionedRoutingClientTest {

    private static final MessageCodec CODEC = new MessageCodec();
    private static final int PARTITIONS = 2;

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /** A key hashing to the given partition under {@link #PARTITIONS}. */
    private static String keyIn(int partition, String tag) {
        for (int i = 0; i < 10_000; i++) {
            String candidate = tag + "-" + i;
            if (Partitioning.partitionOf(candidate, PARTITIONS) == partition) {
                return candidate;
            }
        }
        throw new AssertionError("no key found for partition " + partition);
    }

    /** Records requests; answers BoxInfo with two partitions and per-partition canned pages. */
    private static final class StubNode implements RequestHandler {
        final AtomicInteger boxInfoCalls = new AtomicInteger();
        final List<Message> requests = new ArrayList<>();
        Map<Integer, Message.ListCandiesResponse> listPages = Map.of();
        Map<Integer, Message.ListMultipartUploadsResponse> uploadPages = Map.of();

        @Override
        public synchronized me.predatorray.candybox.protocol.Frame handle(
                me.predatorray.candybox.protocol.Frame request) {
            Message message = CODEC.decode(request);
            requests.add(message);
            return CODEC.encode(dispatch(message));
        }

        private Message dispatch(Message message) {
            if (message instanceof Message.BoxInfoRequest) {
                boxInfoCalls.incrementAndGet();
                return new Message.BoxInfoResponse(PARTITIONS);
            } else if (message instanceof Message.ListCandiesRequest m) {
                return listPages.get(m.partition());
            } else if (message instanceof Message.ListMultipartUploadsRequest m) {
                return uploadPages.get(m.partition());
            } else if (message instanceof Message.GetCandyRequest) {
                return new Message.CandyDataResponse(7, "text/plain", Map.of("m", "x"), 9,
                        bytes("payload"));
            } else if (message instanceof Message.RangeGetCandyRequest) {
                return new Message.CandyDataResponse(4, 10, "text/plain", Map.of(), 9, bytes("2345"));
            } else if (message instanceof Message.HeadCandyRequest) {
                return new Message.HeadCandyResponse(7, "text/plain", Map.of("m", "x"), 9, 1);
            } else if (message instanceof Message.UploadPartRequest m) {
                return new Message.UploadPartResponse(5, m.data().length);
            } else if (message instanceof Message.UploadPartCopyRequest) {
                return new Message.UploadPartResponse(6, 10);
            } else if (message instanceof Message.CopyCandyRequest
                    || message instanceof Message.RenameCandyRequest) {
                return new Message.HeadCandyResponse(7, "text/plain", Map.of(), 9, 1);
            } else if (message instanceof Message.GetCandyLocatorRequest
                    || message instanceof Message.PrepareRenameRequest) {
                me.predatorray.candybox.common.Part part = new me.predatorray.candybox.common.Part(
                        7, 1 << 20, 9,
                        List.of(new me.predatorray.candybox.common.SegmentRef(1, 0, 0)));
                return new Message.CandyLocatorResponse(List.of(part), "text/plain", Map.of("m", "x"),
                        new me.predatorray.candybox.common.Hlc(1000, 0, 1), 1, "User:alice",
                        List.of());
            } else if (message instanceof Message.ZeroCopyPutRequest) {
                return new Message.HeadCandyResponse(7, "text/plain", Map.of("m", "x"), 9, 1);
            }
            return new Message.OkResponse();
        }

        synchronized <T extends Message> List<T> recorded(Class<T> type) {
            List<T> out = new ArrayList<>();
            for (Message m : requests) {
                if (type.isInstance(m)) {
                    out.add(type.cast(m));
                }
            }
            return out;
        }
    }

    private static Message.ListedCandy row(String key) {
        return new Message.ListedCandy(key, 1, 2);
    }

    @Test
    void listMergesPartitionsInKeyOrderAndCachesTheDescriptor() {
        StubNode node = new StubNode();
        node.listPages = Map.of(
                0, new Message.ListCandiesResponse(List.of(row("a"), row("c")), null),
                1, new Message.ListCandiesResponse(List.of(row("b"), row("d")), null));
        try (CandyboxClient client = new CandyboxClient(new LoopbackTransport(node), "x", 0)) {
            CandyboxClient.Listing page = client.listCandies("box", null, null, 3);
            assertThat(page.entries()).extracting(CandyboxClient.Listing.Entry::key)
                    .containsExactly("a", "b", "c"); // global order; "d" overflows the page
            assertThat(page.nextStartAfter()).isEqualTo("c");

            // Under the page size and no partition truncated: no continuation.
            CandyboxClient.Listing all = client.listCandies("box", null, null, 10);
            assertThat(all.entries()).hasSize(4);
            assertThat(all.isTruncated()).isFalse();

            // The descriptor was fetched exactly once across both listings (it is immutable).
            assertThat(node.boxInfoCalls.get()).isEqualTo(1);
        }
    }

    @Test
    void listHonorsPartitionTruncationAndReverseOrder() {
        StubNode node = new StubNode();
        node.listPages = Map.of(
                0, new Message.ListCandiesResponse(List.of(row("a")), "a"), // truncated partition
                1, new Message.ListCandiesResponse(List.of(row("b")), null));
        try (CandyboxClient client = new CandyboxClient(new LoopbackTransport(node), "x", 0)) {
            CandyboxClient.Listing page = client.listCandies("box", null, null, 10);
            // Fewer rows than maxKeys, but partition 0 had more: the page must carry a cursor.
            assertThat(page.entries()).hasSize(2);
            assertThat(page.nextStartAfter()).isEqualTo("b");
        }

        StubNode reverseNode = new StubNode();
        reverseNode.listPages = Map.of(
                0, new Message.ListCandiesResponse(List.of(row("c"), row("a")), null),
                1, new Message.ListCandiesResponse(List.of(row("d"), row("b")), null));
        try (CandyboxClient client = new CandyboxClient(new LoopbackTransport(reverseNode), "x", 0)) {
            CandyboxClient.Listing page = client.listCandies("box", null, null, null, null, true, 10);
            assertThat(page.entries()).extracting(CandyboxClient.Listing.Entry::key)
                    .containsExactly("d", "c", "b", "a");
        }
    }

    @Test
    void deleteRangeFansOutToEveryPartition() {
        StubNode node = new StubNode();
        try (CandyboxClient client = new CandyboxClient(new LoopbackTransport(node), "x", 0)) {
            client.deleteRangeByPrefix("box", "logs/");
            client.deleteRange("box", "a", "m");
        }
        List<Message.DeleteRangeRequest> sent = node.recorded(Message.DeleteRangeRequest.class);
        assertThat(sent).extracting(Message.DeleteRangeRequest::partition)
                .containsExactly(0, 1, 0, 1);
        assertThat(sent.get(0).prefix()).isEqualTo("logs/");
        assertThat(sent.get(2).startKey()).isEqualTo("a");
    }

    @Test
    void listMultipartUploadsMergesAndPaginatesAcrossPartitions() {
        StubNode node = new StubNode();
        node.uploadPages = Map.of(
                0, new Message.ListMultipartUploadsResponse(
                        List.of(new Message.InProgressUpload("u2", "k1", 1)), null, null),
                1, new Message.ListMultipartUploadsResponse(
                        List.of(new Message.InProgressUpload("u1", "k0", 1)), null, null));
        try (CandyboxClient client = new CandyboxClient(new LoopbackTransport(node), "x", 0)) {
            CandyboxClient.MultipartListing merged =
                    client.listMultipartUploads("box", null, null, null, 10);
            assertThat(merged.uploads()).extracting(CandyboxClient.UploadEntry::key)
                    .containsExactly("k0", "k1"); // merged in key order
            assertThat(merged.isTruncated()).isFalse();

            // A page smaller than the merged rows reports markers from the last returned row.
            CandyboxClient.MultipartListing paged =
                    client.listMultipartUploads("box", null, null, null, 1);
            assertThat(paged.uploads()).hasSize(1);
            assertThat(paged.nextKeyMarker()).isEqualTo("k0");
            assertThat(paged.nextUploadIdMarker()).isEqualTo("u1");
        }
    }

    @Test
    void crossPartitionCopyAndRenameStayZeroCopyViaTheLocatorRelay() {
        String src = keyIn(0, "src");
        String dst = keyIn(1, "dst");
        StubNode node = new StubNode();
        try (CandyboxClient client = new CandyboxClient(new LoopbackTransport(node), "x", 0)) {
            CandyboxClient.CandyInfo copied = client.copyCandy("box", src, dst, "tok");
            assertThat(copied.contentLength()).isEqualTo(7);
            // No byte copy: the source's locator parts were relayed and reused at the destination.
            assertThat(node.recorded(Message.GetCandyRequest.class)).isEmpty();
            assertThat(node.recorded(Message.PutCandyRequest.class)).isEmpty();
            Message.GetCandyLocatorRequest gl =
                    node.recorded(Message.GetCandyLocatorRequest.class).get(0);
            assertThat(gl.key()).isEqualTo(src);
            Message.ZeroCopyPutRequest copyPut = node.recorded(Message.ZeroCopyPutRequest.class).get(0);
            assertThat(copyPut.dstKey()).isEqualTo(dst);
            assertThat(copyPut.idempotencyToken()).isEqualTo("tok");
            assertThat(copyPut.renameToken()).isNull(); // a plain copy carries no rename intent

            client.renameCandy("box", src, dst, "rtok");
            // Rename = prepare-intent on the source, zero-copy put on the destination, finalize delete.
            Message.PrepareRenameRequest pr = node.recorded(Message.PrepareRenameRequest.class).get(0);
            assertThat(pr.srcKey()).isEqualTo(src);
            assertThat(pr.renameToken()).isEqualTo("rtok");
            Message.ZeroCopyPutRequest renamePut =
                    node.recorded(Message.ZeroCopyPutRequest.class).get(1);
            assertThat(renamePut.renameToken()).isEqualTo("rtok");
            assertThat(renamePut.srcKey()).isEqualTo(src);
            Message.CompleteRenameRequest cr =
                    node.recorded(Message.CompleteRenameRequest.class).get(0);
            assertThat(cr.srcKey()).isEqualTo(src);
            assertThat(cr.renameToken()).isEqualTo("rtok");
            // Still no whole-object byte copy anywhere.
            assertThat(node.recorded(Message.PutCandyRequest.class)).isEmpty();
        }
    }

    @Test
    void samePartitionCopyAndRenameStayServerSide() {
        String src = keyIn(0, "zsrc");
        String dst = keyIn(0, "zdst");
        StubNode node = new StubNode();
        try (CandyboxClient client = new CandyboxClient(new LoopbackTransport(node), "x", 0)) {
            client.copyCandy("box", src, dst, null);
            client.renameCandy("box", src, dst, null);
        }
        assertThat(node.recorded(Message.CopyCandyRequest.class)).hasSize(1);
        assertThat(node.recorded(Message.RenameCandyRequest.class)).hasSize(1);
        assertThat(node.recorded(Message.PutCandyRequest.class)).isEmpty();
    }

    @Test
    void uploadPartCopyDegradesToClientSideCopyOnlyAcrossPartitions() {
        String target = keyIn(0, "upc");
        String srcSame = keyIn(0, "same");
        String srcOther = keyIn(1, "other");
        StubNode node = new StubNode();
        try (CandyboxClient client = new CandyboxClient(new LoopbackTransport(node), "x", 0)) {
            // Same partition: the zero-copy server op goes through.
            client.uploadPartCopy("box", target, "u1", 1, srcSame, -1, -1);
            assertThat(node.recorded(Message.UploadPartCopyRequest.class)).hasSize(1);

            // Cross-partition, whole source: GET + UploadPart.
            CandyboxClient.PartUploadInfo whole =
                    client.uploadPartCopy("box", target, "u1", 2, srcOther, -1, -1);
            assertThat(whole.partLength()).isEqualTo(7);
            assertThat(node.recorded(Message.GetCandyRequest.class)).hasSize(1);

            // Cross-partition, ranged: Range GET + UploadPart.
            CandyboxClient.PartUploadInfo ranged =
                    client.uploadPartCopy("box", target, "u1", 3, srcOther, 2, 5);
            assertThat(ranged.partLength()).isEqualTo(4);
            assertThat(node.recorded(Message.RangeGetCandyRequest.class)).hasSize(1);
            assertThat(node.recorded(Message.UploadPartCopyRequest.class)).hasSize(1); // unchanged
        }
    }

    @Test
    void deleteBoxInvalidatesTheCachedDescriptor() {
        StubNode node = new StubNode();
        node.listPages = Map.of(
                0, new Message.ListCandiesResponse(List.of(), null),
                1, new Message.ListCandiesResponse(List.of(), null));
        try (CandyboxClient client = new CandyboxClient(new LoopbackTransport(node), "x", 0)) {
            client.listCandies("box", null, null, 5);
            assertThat(node.boxInfoCalls.get()).isEqualTo(1);
            client.deleteBox("box", true);
            client.listCandies("box", null, null, 5); // re-resolves the descriptor
            assertThat(node.boxInfoCalls.get()).isEqualTo(2);
        }
    }
}
