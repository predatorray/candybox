package me.predatorray.candybox.server;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import me.predatorray.candybox.bookkeeper.fake.InMemoryLedgerStore;
import me.predatorray.candybox.common.BoxName;
import me.predatorray.candybox.common.ManualClock;
import me.predatorray.candybox.common.config.CandyboxConfig;
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
            node.createBox(BoxName.of("my-box"));
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
    void handlesHeadCandyListBoxesAndHeadBox() {
        InMemoryLedgerStore store = new InMemoryLedgerStore();
        try (CandyboxNode node = new CandyboxNode(1, CandyboxConfig.defaults(), store,
                new InMemoryCoordinationService(), new ManualClock(1000))) {
            node.createBox(BoxName.of("my-box"));
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
            node.createBox(BoxName.of("my-box"));
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
            node.createBox(BoxName.of("my-box"));
            RequestHandler handler = node.requestHandler();
            for (int i = 0; i < 4; i++) {
                roundTrip(handler, put("my-box", "key-" + i));
            }
            var engine = node.engine(BoxName.of("my-box"));
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
            node.createBox(BoxName.of("my-box"));
            RequestHandler handler = node.requestHandler();
            for (int i = 0; i < 5; i++) {
                roundTrip(handler, put("my-box", "key-" + i));
            }
            assertThat(node.engine(BoxName.of("my-box")).manifestState().level0().size())
                    .isGreaterThanOrEqualTo(3);

            int performed = node.compactOwnedBoxesOnce();
            assertThat(performed).isGreaterThanOrEqualTo(1);
            assertThat(node.engine(BoxName.of("my-box")).manifestState().level0()).isEmpty();
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
            node.createBox(BoxName.of("my-box"));
            RequestHandler handler = node.requestHandler();
            for (int i = 0; i < 5; i++) {
                roundTrip(handler, put("my-box", "key-" + i));
            }

            java.util.Set<Long> inputLedgerIds = new java.util.HashSet<>();
            for (var table : node.engine(BoxName.of("my-box")).manifestState().level0()) {
                inputLedgerIds.add(table.ledgerId());
            }
            assertThat(inputLedgerIds.size()).isGreaterThanOrEqualTo(3);
            assertThat(store.listLedgers()).containsAll(inputLedgerIds);

            // Compaction merges L0 into L1 (inputs leave the committed manifest)...
            node.compactOwnedBoxesOnce();
            assertThat(node.engine(BoxName.of("my-box")).manifestState().level0()).isEmpty();
            assertThat(store.listLedgers()).containsAll(inputLedgerIds); // not deleted yet

            // ...then GC physically deletes the obsolete input ledgers.
            int deleted = node.collectGarbageOnce();
            assertThat(deleted).isEqualTo(inputLedgerIds.size());
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
            node.createBox(BoxName.of("my-box"));
            RequestHandler handler = node.requestHandler();

            // Two versions of the same key => two Syrups, two L0 tables.
            roundTrip(handler, putValue("my-box", "k", "v1"));
            roundTrip(handler, putValue("my-box", "k", "v2"));

            java.util.Set<Long> liveBefore =
                    new java.util.HashSet<>(node.engine(BoxName.of("my-box")).manifestState().liveSyrups());
            assertThat(liveBefore).hasSize(2);

            // Compaction keeps only v2 (S2); the v1 Syrup (S1) becomes unreferenced.
            node.compactOwnedBoxesOnce();
            java.util.Set<Long> referenced =
                    node.engine(BoxName.of("my-box")).manifestState().referencedSyrups();
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

    private static Message put(String box, String key) {
        return putValue(box, key, "v");
    }

    private static Message putValue(String box, String key, String value) {
        return new Message.PutCandyRequest(box, key, null, Map.of(), null,
                value.getBytes(StandardCharsets.UTF_8));
    }
}
