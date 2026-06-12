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
package me.predatorray.candybox.server;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import me.predatorray.candybox.bookkeeper.fake.InMemoryLedgerStore;
import me.predatorray.candybox.common.BoxName;
import me.predatorray.candybox.common.CandyKey;
import me.predatorray.candybox.common.ManualClock;
import me.predatorray.candybox.common.config.CandyboxConfig;
import me.predatorray.candybox.common.exception.BoxAlreadyExistsException;
import me.predatorray.candybox.common.exception.NotOwnerException;
import me.predatorray.candybox.coordination.CandyboxKeys;
import me.predatorray.candybox.coordination.fake.InMemoryCoordinationService;
import me.predatorray.candybox.protocol.Frame;
import me.predatorray.candybox.protocol.Message;
import me.predatorray.candybox.protocol.MessageCodec;
import me.predatorray.candybox.protocol.transport.RequestHandler;
import org.junit.jupiter.api.Test;

class CandyboxNodeTest {

    private final MessageCodec codec = new MessageCodec();

    private Message roundTrip(RequestHandler handler, Message request) {
        Frame response = handler.handle(codec.encode(request));
        return codec.decode(response);
    }

    @Test
    void handlesPutGetDeleteThroughTheProtocol() {
        InMemoryLedgerStore store = new InMemoryLedgerStore();
        try (CandyboxNode node = new CandyboxNode(1, CandyboxConfig.defaults(), store,
                new InMemoryCoordinationService(), new ManualClock(1000))) {
            node.createBox(BoxName.of("my-box"), 1);
            RequestHandler handler = node.requestHandler();

            assertThat(roundTrip(handler, new Message.PutCandyRequest("my-box", "k",
                    null, Map.of("m", "v"), null, "candy".getBytes(StandardCharsets.UTF_8))))
                    .isInstanceOf(Message.OkResponse.class);

            Message getResp = roundTrip(handler, new Message.GetCandyRequest("my-box", "k"));
            assertThat(getResp).isInstanceOf(Message.CandyDataResponse.class);
            assertThat(new String(((Message.CandyDataResponse) getResp).data(), StandardCharsets.UTF_8))
                    .isEqualTo("candy");

            roundTrip(handler, new Message.DeleteCandyRequest("my-box", "k"));
            assertThat(roundTrip(handler, new Message.GetCandyRequest("my-box", "k")))
                    .isInstanceOf(Message.NotFoundResponse.class);
        }
        store.close();
    }

    @Test
    void multipartTtlSweeperAbortsStaleUploadsOlderThanTtl() {
        InMemoryLedgerStore store = new InMemoryLedgerStore();
        ManualClock clock = new ManualClock(1000);
        CandyboxConfig cfg = CandyboxConfig.builder()
                .multipartUploadTtlMillis(60_000) // 60s for the test
                .multipartMinPartBytes(1)
                .build();
        try (CandyboxNode node = new CandyboxNode(1, cfg, store,
                new InMemoryCoordinationService(), clock)) {
            node.createBox(BoxName.of("my-box"), 1);
            RequestHandler handler = node.requestHandler();

            Message create = roundTrip(handler, new Message.CreateMultipartUploadRequest("my-box",
                    "k", null, Map.of()));
            assertThat(create).isInstanceOf(Message.CreateMultipartUploadResponse.class);
            String uploadId = ((Message.CreateMultipartUploadResponse) create).uploadId();

            // Sweep right away: the upload is fresh, no abort.
            assertThat(node.sweepStaleMultipartUploadsOnce()).isZero();

            // Advance the clock past the TTL. Now the sweep aborts it.
            clock.advance(120_000);
            assertThat(node.sweepStaleMultipartUploadsOnce()).isEqualTo(1);
            assertThat(node.sweepStaleMultipartUploadsOnce()).isZero();

            // A subsequent UploadPart against the swept uploadId returns NOT_FOUND (server maps
            // CandyNotFound to NotFoundResponse).
            Message upload = roundTrip(handler, new Message.UploadPartRequest("my-box", "k",
                    uploadId, 1, "x".getBytes(StandardCharsets.UTF_8)));
            assertThat(upload).isInstanceOf(Message.NotFoundResponse.class);
        }
        store.close();
    }

    @Test
    void handlesHeadCandyListBoxesAndHeadBox() {
        InMemoryLedgerStore store = new InMemoryLedgerStore();
        try (CandyboxNode node = new CandyboxNode(1, CandyboxConfig.defaults(), store,
                new InMemoryCoordinationService(), new ManualClock(1000))) {
            node.createBox(BoxName.of("my-box"), 1);
            RequestHandler handler = node.requestHandler();
            roundTrip(handler, new Message.PutCandyRequest("my-box", "k", "text/plain",
                    Map.of("m", "v"), null, "candy".getBytes(StandardCharsets.UTF_8)));

            Message head = roundTrip(handler, new Message.HeadCandyRequest("my-box", "k"));
            assertThat(head).isInstanceOf(Message.HeadCandyResponse.class);
            Message.HeadCandyResponse h = (Message.HeadCandyResponse) head;
            assertThat(h.contentLength()).isEqualTo(5);
            assertThat(h.userMetadata()).containsEntry("m", "v");

            Message boxes = roundTrip(handler, new Message.ListBoxesRequest());
            assertThat(boxes).isInstanceOf(Message.ListBoxesResponse.class);
            assertThat(((Message.ListBoxesResponse) boxes).boxes()).containsExactly("my-box");

            assertThat(roundTrip(handler, new Message.HeadBoxRequest("my-box")))
                    .isInstanceOf(Message.OkResponse.class);
            assertThat(roundTrip(handler, new Message.HeadBoxRequest("ghost-box")))
                    .isInstanceOf(Message.NotFoundResponse.class);
        }
        store.close();
    }

    @Test
    void busyResponseSurfacedUnderWriteStall() {
        CandyboxConfig stall = CandyboxConfig.builder()
                .memtableFlushThresholdBytes(1)
                .l0CompactionTrigger(1)
                .l0StallThreshold(2)
                .build();
        InMemoryLedgerStore store = new InMemoryLedgerStore();
        try (CandyboxNode node = new CandyboxNode(1, stall, store,
                new InMemoryCoordinationService(), new ManualClock(1000))) {
            node.createBox(BoxName.of("my-box"), 1);
            RequestHandler handler = node.requestHandler();

            roundTrip(handler, put("my-box", "k1"));
            roundTrip(handler, put("my-box", "k2"));
            assertThat(roundTrip(handler, put("my-box", "k3"))).isInstanceOf(Message.BusyResponse.class);
        }
        store.close();
    }

    @Test
    void compactionServiceMergesL0Tables() {
        CandyboxConfig cfg = CandyboxConfig.builder()
                .memtableFlushThresholdBytes(1) // each put flushes => one L0 table per key
                .l0CompactionTrigger(3)
                .l0StallThreshold(100)
                .build();
        InMemoryLedgerStore store = new InMemoryLedgerStore();
        ManualClock clock = new ManualClock(1000);
        try (CandyboxNode node = new CandyboxNode(1, cfg, store, new InMemoryCoordinationService(), clock)) {
            node.createBox(BoxName.of("my-box"), 1);
            RequestHandler handler = node.requestHandler();
            for (int i = 0; i < 4; i++) {
                roundTrip(handler, put("my-box", "key-" + i));
            }
            var engine = node.enginePartition(BoxName.of("my-box"), 0);
            assertThat(engine.manifestState().level0().size()).isGreaterThanOrEqualTo(3);

            CompactionService compaction = new CompactionService(store, cfg, clock);
            assertThat(compaction.compactOnce(engine)).isTrue();

            // L0 collapsed and the data moved up to L1; all keys still readable.
            assertThat(engine.manifestState().level0()).isEmpty();
            assertThat(engine.manifestState().level(1)).isNotEmpty();
            for (int i = 0; i < 4; i++) {
                assertThat(engine.getCandy(me.predatorray.candybox.common.CandyKey.of("key-" + i)))
                        .isNotEmpty();
            }
        }
        store.close();
    }

    @Test
    void backgroundCompactionWorkerMergesOwnedBoxes() {
        CandyboxConfig cfg = CandyboxConfig.builder()
                .memtableFlushThresholdBytes(1) // each put flushes => one L0 table per key
                .l0CompactionTrigger(3)
                .l0StallThreshold(100)
                .build();
        InMemoryLedgerStore store = new InMemoryLedgerStore();
        try (CandyboxNode node = new CandyboxNode(1, cfg, store, new InMemoryCoordinationService(),
                new ManualClock(1000))) {
            node.createBox(BoxName.of("my-box"), 1);
            RequestHandler handler = node.requestHandler();
            for (int i = 0; i < 5; i++) {
                roundTrip(handler, put("my-box", "key-" + i));
            }
            assertThat(node.enginePartition(BoxName.of("my-box"), 0).manifestState().level0().size())
                    .isGreaterThanOrEqualTo(3);

            int performed = node.compactOwnedBoxesOnce();
            assertThat(performed).isGreaterThanOrEqualTo(1);
            assertThat(node.enginePartition(BoxName.of("my-box"), 0).manifestState().level0()).isEmpty();
            // Data survives the background compaction.
            for (int i = 0; i < 5; i++) {
                Message resp = roundTrip(handler, new Message.GetCandyRequest("my-box", "key-" + i));
                assertThat(resp).isInstanceOf(Message.CandyDataResponse.class);
            }
        }
        store.close();
    }

    @Test
    void gcDeletesLedgersOfCompactedInputs() {
        CandyboxConfig cfg = CandyboxConfig.builder()
                .memtableFlushThresholdBytes(1)
                .l0CompactionTrigger(3)
                .l0StallThreshold(100)
                .ledgerGcGraceMillis(0) // delete obsolete ledgers immediately
                .build();
        InMemoryLedgerStore store = new InMemoryLedgerStore();
        try (CandyboxNode node = new CandyboxNode(1, cfg, store, new InMemoryCoordinationService(),
                new ManualClock(1000))) {
            node.createBox(BoxName.of("my-box"), 1);
            RequestHandler handler = node.requestHandler();
            for (int i = 0; i < 5; i++) {
                roundTrip(handler, put("my-box", "key-" + i));
            }

            java.util.Set<Long> inputLedgerIds = new java.util.HashSet<>();
            for (var table : node.enginePartition(BoxName.of("my-box"), 0).manifestState().level0()) {
                inputLedgerIds.add(table.ledgerId());
            }
            assertThat(inputLedgerIds.size()).isGreaterThanOrEqualTo(3);
            assertThat(store.listLedgers()).containsAll(inputLedgerIds);

            // Compaction merges L0 into L1 (inputs leave the committed manifest)...
            node.compactOwnedBoxesOnce();
            assertThat(node.enginePartition(BoxName.of("my-box"), 0).manifestState().level0()).isEmpty();
            assertThat(store.listLedgers()).containsAll(inputLedgerIds); // not deleted yet

            // ...then GC physically deletes the obsolete input ledgers (plus rotated WALs).
            int deleted = node.collectGarbageOnce();
            assertThat(deleted).isGreaterThanOrEqualTo(inputLedgerIds.size());
            assertThat(store.listLedgers()).doesNotContainAnyElementsOf(inputLedgerIds);

            // The data is still readable from the merged L1 table.
            for (int i = 0; i < 5; i++) {
                assertThat(roundTrip(handler, new Message.GetCandyRequest("my-box", "key-" + i)))
                        .isInstanceOf(Message.CandyDataResponse.class);
            }
        }
        store.close();
    }

    @Test
    void gcReclaimsOrphanedSyrupsAfterOverwrite() {
        CandyboxConfig cfg = CandyboxConfig.builder()
                .memtableFlushThresholdBytes(1) // each put flushes
                .syrupRolloverBytes(1)          // each Candy lands in its own Syrup
                .l0CompactionTrigger(2)
                .l0StallThreshold(100)
                .ledgerGcGraceMillis(0)
                .build();
        InMemoryLedgerStore store = new InMemoryLedgerStore();
        try (CandyboxNode node = new CandyboxNode(1, cfg, store, new InMemoryCoordinationService(),
                new ManualClock(1000))) {
            node.createBox(BoxName.of("my-box"), 1);
            RequestHandler handler = node.requestHandler();

            // Two versions of the same key => two Syrups, two L0 tables.
            roundTrip(handler, putValue("my-box", "k", "v1"));
            roundTrip(handler, putValue("my-box", "k", "v2"));

            java.util.Set<Long> liveBefore =
                    new java.util.HashSet<>(node.enginePartition(BoxName.of("my-box"), 0).manifestState().liveSyrups());
            assertThat(liveBefore).hasSize(2);

            // Compaction keeps only v2 (S2); the v1 Syrup (S1) becomes unreferenced.
            node.compactOwnedBoxesOnce();
            java.util.Set<Long> referenced =
                    node.enginePartition(BoxName.of("my-box"), 0).manifestState().referencedSyrups();
            assertThat(referenced).hasSize(1);
            java.util.Set<Long> orphan = new java.util.HashSet<>(liveBefore);
            orphan.removeAll(referenced);
            assertThat(orphan).hasSize(1);
            assertThat(store.listLedgers()).containsAll(orphan); // not deleted yet

            // GC reclaims the orphaned Syrup; the referenced one survives and data is intact.
            int deleted = node.collectGarbageOnce();
            assertThat(deleted).isGreaterThanOrEqualTo(1);
            assertThat(store.listLedgers()).doesNotContainAnyElementsOf(orphan);
            assertThat(store.listLedgers()).containsAll(referenced);

            Message get = roundTrip(handler, new Message.GetCandyRequest("my-box", "k"));
            assertThat(new String(((Message.CandyDataResponse) get).data(), StandardCharsets.UTF_8))
                    .isEqualTo("v2");
        }
        store.close();
    }

    @Test
    void gcDeletesRotatedWalLedgers() {
        CandyboxConfig cfg = CandyboxConfig.builder()
                .memtableFlushThresholdBytes(1) // each put flushes => rotates the WAL
                .l0CompactionTrigger(100)       // no compaction, so only WALs are reclaimable
                .l0StallThreshold(200)
                .ledgerGcGraceMillis(0)
                .build();
        InMemoryLedgerStore store = new InMemoryLedgerStore();
        try (CandyboxNode node = new CandyboxNode(1, cfg, store, new InMemoryCoordinationService(),
                new ManualClock(1000))) {
            node.createBox(BoxName.of("my-box"), 1);
            RequestHandler handler = node.requestHandler();
            roundTrip(handler, put("my-box", "a")); // flush => WAL_0 rotated out
            roundTrip(handler, put("my-box", "b")); // flush => WAL_1 rotated out

            // No compaction and all Candies live, so only the two rotated WALs are reclaimable.
            assertThat(node.collectGarbageOnce()).isEqualTo(2);
            // Data still readable from the L0 tables.
            assertThat(roundTrip(handler, new Message.GetCandyRequest("my-box", "a")))
                    .isInstanceOf(Message.CandyDataResponse.class);
        }
        store.close();
    }

    @Test
    void createBoxRollsBackWhenAPartitionCannotBeCreated() {
        InMemoryLedgerStore store = new InMemoryLedgerStore();
        InMemoryCoordinationService coordination = new InMemoryCoordinationService();
        // A leftover manifest pointer for partition 1 makes the second partition's creation fail.
        coordination.create(CandyboxKeys.manifestKey("half-box", 1), new byte[] {0});
        try (CandyboxNode node = new CandyboxNode(1, CandyboxConfig.defaults(), store, coordination,
                new ManualClock(1000))) {
            assertThatThrownBy(() -> node.createBox(BoxName.of("half-box"), 2))
                    .isInstanceOf(BoxAlreadyExistsException.class);
            // The descriptor and the partition that *was* created are rolled back.
            assertThat(node.boxExists(BoxName.of("half-box"))).isFalse();
            assertThat(coordination.get(CandyboxKeys.boxMetaKey("half-box"))).isEmpty();
            assertThat(coordination.get(CandyboxKeys.manifestKey("half-box", 0))).isEmpty();
            assertThat(node.ownedBoxStats()).isEmpty();
        }
        store.close();
    }

    @Test
    void nonForceDeleteBoxFailsWhileAnotherLiveNodeOwnsAPartition() {
        InMemoryLedgerStore store = new InMemoryLedgerStore();
        InMemoryCoordinationService coordination = new InMemoryCoordinationService();
        try (CandyboxNode owner = new CandyboxNode(1, CandyboxConfig.defaults(), store, coordination,
                new ManualClock(1000));
             CandyboxNode other = new CandyboxNode(2, CandyboxConfig.defaults(), store, coordination,
                new ManualClock(1000))) {
            owner.createBox(BoxName.of("held-box"), 2);
            // Node 2 cannot take over partitions whose leases node 1 still holds.
            assertThatThrownBy(() -> other.deleteBox(BoxName.of("held-box"), false))
                    .isInstanceOf(NotOwnerException.class);
            assertThat(owner.boxExists(BoxName.of("held-box"))).isTrue();
        }
        store.close();
    }

    @Test
    void releaseBoxThenOpenBoxReplaysEveryPartition() {
        InMemoryLedgerStore store = new InMemoryLedgerStore();
        InMemoryCoordinationService coordination = new InMemoryCoordinationService();
        try (CandyboxNode node = new CandyboxNode(1, CandyboxConfig.defaults(), store, coordination,
                new ManualClock(1000))) {
            BoxName box = BoxName.of("reopen-box");
            node.createBox(box, 2);
            node.engine(box, "k").putCandy(CandyKey.of("k"),
                    "v".getBytes(StandardCharsets.UTF_8), null, Map.of(), null);

            node.releaseBox(box);
            assertThat(node.ownedBoxStats()).isEmpty();
            assertThat(node.currentOwner(box, 0)).isEmpty(); // released, not just expired

            node.openBox(box);
            assertThat(node.ownedBoxStats()).containsKeys("reopen-box/0", "reopen-box/1");
            assertThat(node.engine(box, "k").getCandy(CandyKey.of("k")))
                    .isEqualTo("v".getBytes(StandardCharsets.UTF_8));
        }
        store.close();
    }

    private static Message put(String box, String key) {
        return putValue(box, key, "v");
    }

    private static Message putValue(String box, String key, String value) {
        return new Message.PutCandyRequest(box, key, null, Map.of(), null,
                value.getBytes(StandardCharsets.UTF_8));
    }
}
