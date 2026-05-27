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

    private static Message put(String box, String key) {
        return new Message.PutCandyRequest(box, key, null, Map.of(), null,
                "v".getBytes(StandardCharsets.UTF_8));
    }
}
