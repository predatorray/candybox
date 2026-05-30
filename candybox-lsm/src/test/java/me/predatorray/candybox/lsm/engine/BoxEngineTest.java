package me.predatorray.candybox.lsm.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import me.predatorray.candybox.bookkeeper.fake.InMemoryLedgerStore;
import me.predatorray.candybox.common.BoxName;
import me.predatorray.candybox.common.CandyKey;
import me.predatorray.candybox.common.ManualClock;
import me.predatorray.candybox.common.config.CandyboxConfig;
import me.predatorray.candybox.common.exception.BusyException;
import me.predatorray.candybox.common.exception.CandyNotFoundException;
import me.predatorray.candybox.common.exception.FencedException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class BoxEngineTest {

    private final InMemoryLedgerStore store = new InMemoryLedgerStore();
    private final BoxName box = BoxName.of("my-box");
    private BoxEngine engine;

    @AfterEach
    void tearDown() {
        if (engine != null) {
            engine.close();
        }
        store.close();
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void putGetDeleteSurvivesFlush() {
        engine = BoxEngine.createNew(box, CandyboxConfig.defaults(), store, 1, new ManualClock(1000), 1L);

        engine.putCandy(CandyKey.of("fruit/apple"), bytes("red"), "text/plain",
                Map.of("color", "red"), null);
        engine.putCandy(CandyKey.of("fruit/banana"), bytes("yellow"), null, Map.of(), null);

        // Read from the memtable.
        assertThat(engine.getCandy(CandyKey.of("fruit/apple"))).isEqualTo(bytes("red"));
        assertThat(engine.headCandy(CandyKey.of("fruit/apple")).userMetadata())
                .containsEntry("color", "red");

        // Flush to L0 and read through the SSTable path.
        engine.flush();
        assertThat(engine.getCandy(CandyKey.of("fruit/apple"))).isEqualTo(bytes("red"));
        assertThat(engine.getCandy(CandyKey.of("fruit/banana"))).isEqualTo(bytes("yellow"));

        // Delete and confirm the tombstone shadows the value across the merged view.
        engine.deleteCandy(CandyKey.of("fruit/apple"));
        assertThatThrownBy(() -> engine.getCandy(CandyKey.of("fruit/apple")))
                .isInstanceOf(CandyNotFoundException.class);
        // The other key is unaffected.
        assertThat(engine.getCandy(CandyKey.of("fruit/banana"))).isEqualTo(bytes("yellow"));
    }

    @Test
    void overwriteReturnsLatestValueAcrossLevels() {
        engine = BoxEngine.createNew(box, CandyboxConfig.defaults(), store, 1, new ManualClock(1000), 1L);
        engine.putCandy(CandyKey.of("k"), bytes("v1"), null, Map.of(), null);
        engine.flush(); // v1 now in an SSTable
        engine.putCandy(CandyKey.of("k"), bytes("v2"), null, Map.of(), null); // v2 in the memtable
        assertThat(engine.getCandy(CandyKey.of("k"))).isEqualTo(bytes("v2"));
    }

    @Test
    void listCandiesHonoursPrefixStartAfterAndPaging() {
        engine = BoxEngine.createNew(box, CandyboxConfig.defaults(), store, 1, new ManualClock(1000), 1L);
        engine.putCandy(CandyKey.of("a/1"), bytes("x"), null, Map.of(), null);
        engine.putCandy(CandyKey.of("a/2"), bytes("x"), null, Map.of(), null);
        engine.putCandy(CandyKey.of("a/3"), bytes("x"), null, Map.of(), null);
        engine.putCandy(CandyKey.of("b/1"), bytes("x"), null, Map.of(), null);
        engine.flush();
        engine.putCandy(CandyKey.of("a/4"), bytes("x"), null, Map.of(), null); // in memtable

        // Prefix filter excludes "b/1".
        ListResult page1 = engine.listCandies("a/", null, 2);
        assertThat(page1.entries()).extracting(e -> e.key().value()).containsExactly("a/1", "a/2");
        assertThat(page1.isTruncated()).isTrue();

        ListResult page2 = engine.listCandies("a/", page1.nextStartAfter(), 100);
        assertThat(page2.entries()).extracting(e -> e.key().value()).containsExactly("a/3", "a/4");
        assertThat(page2.isTruncated()).isFalse();
    }

    @Test
    void boundedRangeScanHonoursHalfOpenWindowAcrossLevels() {
        engine = BoxEngine.createNew(box, CandyboxConfig.defaults(), store, 1, new ManualClock(1000), 1L);
        for (String k : new String[] {"a", "b", "c", "d", "e"}) {
            engine.putCandy(CandyKey.of(k), bytes("x"), null, Map.of(), null);
        }
        engine.flush();
        engine.putCandy(CandyKey.of("c2"), bytes("x"), null, Map.of(), null); // memtable, inside [b,e)

        ListResult result = engine.scanCandies(
                ScanQuery.forwardRange(CandyKey.of("b"), CandyKey.of("e"), 100));
        assertThat(result.entries()).extracting(e -> e.key().value())
                .containsExactly("b", "c", "c2", "d"); // 'a' excluded (< b), 'e' excluded (== end)
    }

    @Test
    void reverseScanListsDescendingAndPaginates() {
        engine = BoxEngine.createNew(box, CandyboxConfig.defaults(), store, 1, new ManualClock(1000), 1L);
        engine.putCandy(CandyKey.of("a/1"), bytes("x"), null, Map.of(), null);
        engine.putCandy(CandyKey.of("a/2"), bytes("x"), null, Map.of(), null);
        engine.putCandy(CandyKey.of("a/3"), bytes("x"), null, Map.of(), null);
        engine.putCandy(CandyKey.of("b/1"), bytes("x"), null, Map.of(), null);
        engine.flush();
        engine.putCandy(CandyKey.of("a/4"), bytes("x"), null, Map.of(), null); // memtable

        ListResult page1 = engine.scanCandies(
                ScanQuery.reverse("a/", null, null, null, 2));
        assertThat(page1.entries()).extracting(e -> e.key().value()).containsExactly("a/4", "a/3");
        assertThat(page1.isTruncated()).isTrue();

        ListResult page2 = engine.scanCandies(
                ScanQuery.reverse("a/", null, null, CandyKey.of(page1.nextStartAfter()), 100));
        assertThat(page2.entries()).extracting(e -> e.key().value()).containsExactly("a/2", "a/1");
        assertThat(page2.isTruncated()).isFalse();
    }

    @Test
    void idempotencyTokenDedupesRetriedPut() {
        engine = BoxEngine.createNew(box, CandyboxConfig.defaults(), store, 1, new ManualClock(1000), 1L);
        CandyMetadata first = engine.putCandy(CandyKey.of("k"), bytes("payload"), null, Map.of(), "tok-1");
        CandyMetadata retry = engine.putCandy(CandyKey.of("k"), bytes("payload"), null, Map.of(), "tok-1");

        // Same HLC returned (no second write happened).
        assertThat(retry.hlc()).isEqualTo(first.hlc());
    }

    @Test
    void writeStallReturnsBusy() {
        CandyboxConfig stallConfig = CandyboxConfig.builder()
                .memtableFlushThresholdBytes(1) // every put flushes immediately
                .l0CompactionTrigger(1)
                .l0StallThreshold(2)
                .build();
        engine = BoxEngine.createNew(box, stallConfig, store, 1, new ManualClock(1000), 1L);

        engine.putCandy(CandyKey.of("k1"), bytes("a"), null, Map.of(), null); // L0 -> 1
        engine.putCandy(CandyKey.of("k2"), bytes("a"), null, Map.of(), null); // L0 -> 2
        assertThatThrownBy(() -> engine.putCandy(CandyKey.of("k3"), bytes("a"), null, Map.of(), null))
                .isInstanceOf(BusyException.class);
    }

    @Test
    void handoverWithRegressedClockDoesNotLoseLatestWrite() {
        // Owner A runs with a wall clock far in the future.
        ManualClock clockA = new ManualClock(10_000);
        BoxEngine ownerA = BoxEngine.createNew(box, CandyboxConfig.defaults(), store, 1, clockA, 1L);
        CandyMetadata v1 = ownerA.putCandy(CandyKey.of("k"), bytes("v1"), null, Map.of(), null);

        long manifestLedgerId = ownerA.manifestLedgerId();

        // Owner B takes over with a badly regressed wall clock.
        ManualClock clockB = new ManualClock(100);
        // Owner B takes over with a strictly higher fencing token than A (1).
        engine = BoxEngine.recover(box, CandyboxConfig.defaults(), store, 2, clockB, manifestLedgerId, 2L);

        CandyMetadata v2 = engine.putCandy(CandyKey.of("k"), bytes("v2"), null, Map.of(), null);

        // Despite B's clock being far behind, its stamp must exceed A's, so v2 wins LWW.
        assertThat(v2.hlc().isAfter(v1.hlc())).isTrue();
        assertThat(engine.getCandy(CandyKey.of("k"))).isEqualTo(bytes("v2"));

        // The old owner is fenced: its WAL and manifest were recover-opened.
        assertThatThrownBy(() -> ownerA.putCandy(CandyKey.of("k2"), bytes("zombie"), null, Map.of(), null))
                .isInstanceOf(FencedException.class);
    }
}
