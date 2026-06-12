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
package me.predatorray.candybox.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import me.predatorray.candybox.bookkeeper.fake.InMemoryLedgerStore;
import me.predatorray.candybox.client.CandyboxClient;
import me.predatorray.candybox.common.BoxName;
import me.predatorray.candybox.common.ManualClock;
import me.predatorray.candybox.common.Partitioning;
import me.predatorray.candybox.common.config.CandyboxConfig;
import me.predatorray.candybox.common.exception.CandyNotFoundException;
import me.predatorray.candybox.coordination.fake.InMemoryCoordinationService;
import me.predatorray.candybox.protocol.Frame;
import me.predatorray.candybox.protocol.FrameCodec;
import me.predatorray.candybox.protocol.transport.Connection;
import me.predatorray.candybox.protocol.transport.RequestHandler;
import me.predatorray.candybox.protocol.transport.Transport;
import me.predatorray.candybox.server.CandyboxNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * End-to-end behaviour of a hash-partitioned Box whose partitions are owned by <em>two different
 * nodes</em>, driven through the cluster-aware {@link CandyboxClient}: keyed routing, scatter-gather
 * listing with pagination, fanned-out range deletes, cross-partition copy/rename byte-copy
 * fallbacks, and multipart uploads. Runs entirely on the in-memory fakes (no BookKeeper/ZooKeeper /
 * sockets) with a port-routing in-JVM transport, so it is fast yet exercises the real codec, request
 * handler, and routing stack.
 */
class PartitionedBoxIT {

    private static final int PARTITIONS = 4;
    private static final String BOX = "parted-box";

    /** An in-JVM {@link Transport} that routes by port to the matching node's request handler. */
    private static final class RoutingTransport implements Transport {
        private final Map<Integer, RequestHandler> handlers;
        private final FrameCodec codec = new FrameCodec();

        RoutingTransport(Map<Integer, RequestHandler> handlers) {
            this.handlers = handlers;
        }

        @Override
        public Connection connect(String host, int port) {
            RequestHandler handler = handlers.get(port);
            return new Connection() {
                @Override
                public Frame call(Frame request) {
                    Frame onWire = codec.decode(codec.encode(request));
                    return codec.decode(codec.encode(handler.handle(onWire)));
                }

                @Override
                public void close() {
                }
            };
        }

        @Override
        public void close() {
        }
    }

    private InMemoryLedgerStore store;
    private InMemoryCoordinationService coordination;
    private CandyboxNode nodeA;
    private CandyboxNode nodeB;
    private CandyboxClient client;

    @BeforeEach
    void setUp() {
        ManualClock clock = new ManualClock(1_000);
        store = new InMemoryLedgerStore();
        coordination = new InMemoryCoordinationService(clock);
        CandyboxConfig config = CandyboxConfig.builder()
                .leaseRenewIntervalMillis(0)
                .multipartMinPartBytes(0)
                .build();
        nodeA = new CandyboxNode(1, config, store, coordination, clock, "127.0.0.1:1");
        nodeB = new CandyboxNode(2, config, store, coordination, clock, "127.0.0.1:2");

        Map<Integer, RequestHandler> handlers = new HashMap<>();
        handlers.put(1, nodeA.requestHandler());
        handlers.put(2, nodeB.requestHandler());
        client = new CandyboxClient(new RoutingTransport(handlers), coordination, config);

        // Create the Box on node 1 (all partitions land there), then hand partitions 2 and 3 over
        // to node 2 so the Box's ownership genuinely spans two nodes.
        client.createBox(BOX, PARTITIONS);
        BoxName box = BoxName.of(BOX);
        nodeA.releasePartition(box, 2);
        nodeA.releasePartition(box, 3);
        nodeB.openPartition(box, 2);
        nodeB.openPartition(box, 3);
    }

    @AfterEach
    void tearDown() {
        client.close();
        nodeA.close();
        nodeB.close();
        store.close();
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /** A key that hashes to the given partition. */
    private static String keyIn(int partition, String tag) {
        for (int i = 0; i < 10_000; i++) {
            String candidate = tag + "-" + i;
            if (Partitioning.partitionOf(candidate, PARTITIONS) == partition) {
                return candidate;
            }
        }
        throw new AssertionError("no key found for partition " + partition);
    }

    @Test
    void keyedOperationsRouteToTheOwningNodeOfEachPartition() {
        for (int p = 0; p < PARTITIONS; p++) {
            String key = keyIn(p, "k");
            client.putCandy(BOX, key, bytes("v" + p), "text/plain", Map.of("p", "" + p), null);
            assertThat(client.getCandy(BOX, key)).isEqualTo(bytes("v" + p));
            assertThat(client.headCandy(BOX, key).userMetadata()).containsEntry("p", "" + p);
        }
        // Each node only holds the keys of its partitions.
        assertThat(nodeA.ownedBoxStats().keySet()).containsExactly(BOX + "/0", BOX + "/1");
        assertThat(nodeB.ownedBoxStats().keySet()).containsExactly(BOX + "/2", BOX + "/3");
    }

    @Test
    void listingMergesPartitionsInGlobalKeyOrderWithPagination() {
        TreeSet<String> expected = new TreeSet<>();
        for (int i = 0; i < 23; i++) {
            String key = "item/" + String.format("%02d", i);
            client.putCandy(BOX, key, bytes("v"), null, Map.of(), null);
            expected.add(key);
        }

        // Forward, paged: walking pages of 7 yields every key exactly once, globally sorted.
        List<String> walked = new ArrayList<>();
        String cursor = null;
        do {
            CandyboxClient.Listing page = client.listCandies(BOX, "item/", cursor, 7);
            for (CandyboxClient.Listing.Entry e : page.entries()) {
                walked.add(e.key());
            }
            cursor = page.nextStartAfter();
        } while (cursor != null);
        assertThat(walked).containsExactlyElementsOf(expected);

        // Reverse listing comes back in descending order.
        CandyboxClient.Listing reversed = client.listCandies(BOX, "item/", null, null, null, true, 100);
        assertThat(reversed.entries()).extracting(CandyboxClient.Listing.Entry::key)
                .containsExactlyElementsOf(expected.descendingSet());
    }

    @Test
    void deleteRangeFansOutToEveryPartition() {
        for (int i = 0; i < 12; i++) {
            client.putCandy(BOX, "logs/" + i, bytes("v"), null, Map.of(), null);
        }
        client.putCandy(BOX, "other/key", bytes("keep"), null, Map.of(), null);

        client.deleteRangeByPrefix(BOX, "logs/");

        assertThat(client.listCandies(BOX, "logs/", null, 100).entries()).isEmpty();
        assertThat(client.getCandy(BOX, "other/key")).isEqualTo(bytes("keep"));
    }

    @Test
    void crossPartitionCopyAndRenameFallBackToByteCopy() {
        String src = keyIn(0, "src");        // owned by node 1
        String dstCopy = keyIn(3, "copy");   // owned by node 2
        String dstMove = keyIn(2, "move");   // owned by node 2
        client.putCandy(BOX, src, bytes("payload"), "text/plain", Map.of("m", "x"), null);

        CandyboxClient.CandyInfo copied = client.copyCandy(BOX, src, dstCopy, null);
        assertThat(copied.contentLength()).isEqualTo(7);
        assertThat(client.getCandy(BOX, dstCopy)).isEqualTo(bytes("payload"));
        assertThat(client.headCandy(BOX, dstCopy).userMetadata()).containsEntry("m", "x");
        assertThat(client.getCandy(BOX, src)).isEqualTo(bytes("payload")); // copy keeps the source

        CandyboxClient.CandyInfo moved = client.renameCandy(BOX, src, dstMove, null);
        assertThat(moved.contentLength()).isEqualTo(7);
        assertThat(client.getCandy(BOX, dstMove)).isEqualTo(bytes("payload"));
        assertThatThrownBy(() -> client.getCandy(BOX, src))
                .isInstanceOf(CandyNotFoundException.class); // rename removed the source
    }

    @Test
    void samePartitionCopyStaysServerSide() {
        String src = keyIn(1, "zsrc");
        String dst = keyIn(1, "zdst");
        client.putCandy(BOX, src, bytes("zero-copy"), null, Map.of(), null);
        client.copyCandy(BOX, src, dst, null);
        assertThat(client.getCandy(BOX, dst)).isEqualTo(bytes("zero-copy"));
    }

    @Test
    void multipartUploadRoutesByTargetKeyAndListsAcrossPartitions() {
        String keyOnB = keyIn(3, "mp"); // the upload lives on node 2's partition
        String uploadId = client.createMultipartUpload(BOX, keyOnB, "text/plain", Map.of());

        CandyboxClient.PartUploadInfo part1 = client.uploadPart(BOX, keyOnB, uploadId, 1,
                bytes("hello "));
        CandyboxClient.PartUploadInfo part2 = client.uploadPart(BOX, keyOnB, uploadId, 2,
                bytes("world"));

        // The merged cross-partition listing surfaces the in-flight upload.
        CandyboxClient.MultipartListing uploads =
                client.listMultipartUploads(BOX, null, null, null, 100);
        assertThat(uploads.uploads()).extracting(CandyboxClient.UploadEntry::uploadId)
                .containsExactly(uploadId);

        client.completeMultipartUpload(BOX, keyOnB, uploadId, List.of(part1, part2), null);
        assertThat(client.getCandy(BOX, keyOnB)).isEqualTo(bytes("hello world"));
    }

    @Test
    void uploadPartCopyAcrossPartitionsDegradesToClientSideCopy() {
        String src = keyIn(0, "upcsrc");   // node 1
        String target = keyIn(2, "upcdst"); // node 2
        client.putCandy(BOX, src, bytes("0123456789"), null, Map.of(), null);

        String uploadId = client.createMultipartUpload(BOX, target, null, Map.of());
        CandyboxClient.PartUploadInfo part = client.uploadPartCopy(BOX, target, uploadId, 1,
                src, 2, 5);
        assertThat(part.partLength()).isEqualTo(4);

        client.completeMultipartUpload(BOX, target, uploadId, List.of(part), null);
        assertThat(client.getCandy(BOX, target)).isEqualTo(bytes("2345"));
    }

    @Test
    void boxInfoHeadBoxAndListBoxesAreClusterWide() {
        // Both nodes answer Box existence/descriptor queries from coordination, so even a node that
        // owns nothing about the Box can serve them (callAny picks the first member).
        assertThat(client.headBox(BOX)).isTrue();
        assertThat(client.headBox("no-such-box")).isFalse();
        assertThat(client.listBoxes()).containsExactly(BOX);
    }
}
