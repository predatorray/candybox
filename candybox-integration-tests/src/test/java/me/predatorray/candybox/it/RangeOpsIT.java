package me.predatorray.candybox.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import me.predatorray.candybox.bookkeeper.bk.BookKeeperLedgerStore;
import me.predatorray.candybox.common.BoxName;
import me.predatorray.candybox.common.CandyKey;
import me.predatorray.candybox.common.SystemClock;
import me.predatorray.candybox.common.config.CandyboxConfig;
import me.predatorray.candybox.common.exception.CandyNotFoundException;
import me.predatorray.candybox.lsm.engine.BoxEngine;
import me.predatorray.candybox.lsm.engine.ListResult;
import me.predatorray.candybox.lsm.engine.ScanQuery;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * End-to-end tests of the LSM-native operations against real BookKeeper ledgers: bounded and reverse
 * scans over real SSTables, zero-copy rename reading bytes back from real Syrups, deleteRange persisted
 * through a real flush, and recovery of a range tombstone from a real WAL on handover.
 */
class RangeOpsIT {

    private static EmbeddedBookKeeper bookKeeper;
    private BookKeeperLedgerStore store;

    @BeforeAll
    static void startCluster() {
        bookKeeper = new EmbeddedBookKeeper(3);
    }

    @AfterAll
    static void stopCluster() {
        if (bookKeeper != null) {
            bookKeeper.close();
        }
    }

    @BeforeEach
    void openStore() {
        store = new BookKeeperLedgerStore(bookKeeper.clientConfiguration(),
                "candybox".getBytes(StandardCharsets.UTF_8));
    }

    @AfterEach
    void closeStore() {
        store.close();
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void boundedAndReverseScansOverRealSSTables() {
        try (BoxEngine engine = BoxEngine.createNew(BoxName.of("scan-box"), CandyboxConfig.defaults(),
                store, 1, SystemClock.INSTANCE, 1L)) {
            for (String k : new String[] {"a", "b", "c", "d", "e"}) {
                engine.putCandy(CandyKey.of(k), bytes(k), null, Map.of(), null);
            }
            engine.flush(); // real L0 SSTable

            ListResult bounded = engine.scanCandies(
                    ScanQuery.forwardRange(CandyKey.of("b"), CandyKey.of("e"), 100));
            assertThat(bounded.entries()).extracting(e -> e.key().value())
                    .containsExactly("b", "c", "d");

            ListResult reverse = engine.scanCandies(
                    ScanQuery.reverse(null, null, null, null, 100));
            assertThat(reverse.entries()).extracting(e -> e.key().value())
                    .containsExactly("e", "d", "c", "b", "a");
        }
    }

    @Test
    void renameRoundTripsBytesFromRealSyrups() {
        try (BoxEngine engine = BoxEngine.createNew(BoxName.of("rename-box"), CandyboxConfig.defaults(),
                store, 1, SystemClock.INSTANCE, 1L)) {
            engine.putCandy(CandyKey.of("old"), bytes("payload"), "text/plain", Map.of("k", "v"), null);
            engine.flush();

            engine.renameCandy(CandyKey.of("old"), CandyKey.of("new"), null);
            engine.flush(); // exercise the SSTable path for both the new key and the source tombstone

            assertThat(engine.getCandy(CandyKey.of("new"))).isEqualTo(bytes("payload"));
            assertThat(engine.headCandy(CandyKey.of("new")).userMetadata()).containsEntry("k", "v");
            assertThatThrownBy(() -> engine.getCandy(CandyKey.of("old")))
                    .isInstanceOf(CandyNotFoundException.class);
        }
    }

    @Test
    void deleteRangePersistsThroughFlush() {
        try (BoxEngine engine = BoxEngine.createNew(BoxName.of("delrange-box"),
                CandyboxConfig.defaults(), store, 1, SystemClock.INSTANCE, 1L)) {
            engine.putCandy(CandyKey.of("logs/1"), bytes("x"), null, Map.of(), null);
            engine.putCandy(CandyKey.of("logs/2"), bytes("x"), null, Map.of(), null);
            engine.putCandy(CandyKey.of("keep"), bytes("x"), null, Map.of(), null);
            engine.flush();

            engine.deleteRangeByPrefix("logs/");
            engine.flush(); // range tombstone now in a real SSTable

            assertThat(engine.listCandies(null, null, 100).entries())
                    .extracting(e -> e.key().value()).containsExactly("keep");
            assertThatThrownBy(() -> engine.getCandy(CandyKey.of("logs/1")))
                    .isInstanceOf(CandyNotFoundException.class);
        }
    }

    @Test
    void rangeTombstoneIsRecoveredFromTheWalOnHandover() {
        BoxName box = BoxName.of("delrange-recover-box");
        BoxEngine ownerA = BoxEngine.createNew(box, CandyboxConfig.defaults(), store, 1,
                SystemClock.INSTANCE, 1L);
        ownerA.putCandy(CandyKey.of("k1"), bytes("v1"), null, Map.of(), null);
        ownerA.putCandy(CandyKey.of("k2"), bytes("v2"), null, Map.of(), null);
        ownerA.deleteRange(CandyKey.of("k1"), CandyKey.of("k2")); // deletes k1 only; never flushed
        long manifestLedgerId = ownerA.manifestLedgerId();

        try (BoxEngine ownerB = BoxEngine.recover(box, CandyboxConfig.defaults(), store, 2,
                SystemClock.INSTANCE, manifestLedgerId, 2L)) {
            assertThatThrownBy(() -> ownerB.getCandy(CandyKey.of("k1")))
                    .isInstanceOf(CandyNotFoundException.class);
            assertThat(ownerB.getCandy(CandyKey.of("k2"))).isEqualTo(bytes("v2"));
        }
    }
}
