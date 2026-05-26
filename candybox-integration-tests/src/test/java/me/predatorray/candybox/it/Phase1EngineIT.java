package me.predatorray.candybox.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Random;
import me.predatorray.candybox.bookkeeper.bk.BookKeeperLedgerStore;
import me.predatorray.candybox.common.BoxName;
import me.predatorray.candybox.common.CandyKey;
import me.predatorray.candybox.common.SystemClock;
import me.predatorray.candybox.common.config.CandyboxConfig;
import me.predatorray.candybox.common.config.SizeLimits;
import me.predatorray.candybox.common.exception.CandyNotFoundException;
import me.predatorray.candybox.lsm.engine.BoxEngine;
import me.predatorray.candybox.lsm.engine.ListResult;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Phase 1 end-to-end engine tests against real BookKeeper ledgers: put/get/delete/list, large-object
 * chunking across Syrups, and recovery of un-flushed writes from the WAL.
 */
class Phase1EngineIT {

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
    void putGetDeleteListOnRealLedgers() {
        BoxName box = BoxName.of("it-box");
        try (BoxEngine engine = BoxEngine.createNew(box, CandyboxConfig.defaults(), store, 1,
                SystemClock.INSTANCE)) {
            engine.putCandy(CandyKey.of("a/1"), bytes("one"), "text/plain", Map.of("k", "v"), null);
            engine.putCandy(CandyKey.of("a/2"), bytes("two"), null, Map.of(), null);
            engine.putCandy(CandyKey.of("b/1"), bytes("three"), null, Map.of(), null);
            engine.flush(); // push to a real L0 SSTable ledger

            assertThat(engine.getCandy(CandyKey.of("a/1"))).isEqualTo(bytes("one"));
            assertThat(engine.headCandy(CandyKey.of("a/1")).userMetadata()).containsEntry("k", "v");

            ListResult listing = engine.listCandies("a/", null, 10);
            assertThat(listing.entries()).extracting(e -> e.key().value()).containsExactly("a/1", "a/2");

            engine.deleteCandy(CandyKey.of("a/1"));
            assertThatThrownBy(() -> engine.getCandy(CandyKey.of("a/1")))
                    .isInstanceOf(CandyNotFoundException.class);
            assertThat(engine.getCandy(CandyKey.of("a/2"))).isEqualTo(bytes("two"));
        }
    }

    @Test
    void largeObjectIsChunkedAcrossSyrupsAndReassembled() {
        SizeLimits limits = new SizeLimits(64 * 1024, SizeLimits.DEFAULT_MAX_KEY_BYTES,
                SizeLimits.DEFAULT_MAX_METADATA_BYTES, SizeLimits.DEFAULT_MAX_LOCATOR_BYTES, 0);
        CandyboxConfig config = CandyboxConfig.builder()
                .sizeLimits(limits)
                .syrupRolloverBytes(256 * 1024) // force several Syrups for a 1 MiB object
                .build();

        byte[] big = new byte[1024 * 1024];
        new Random(7).nextBytes(big);

        BoxName box = BoxName.of("big-box");
        try (BoxEngine engine = BoxEngine.createNew(box, config, store, 1, SystemClock.INSTANCE)) {
            engine.putCandy(CandyKey.of("large"), big, "application/octet-stream", Map.of(), null);
            assertThat(engine.getCandy(CandyKey.of("large"))).isEqualTo(big);

            engine.flush();
            assertThat(engine.getCandy(CandyKey.of("large"))).isEqualTo(big);
        }
    }

    @Test
    void unflushedWritesAreRecoveredFromTheWal() {
        BoxName box = BoxName.of("recover-box");
        long manifestLedgerId;
        BoxEngine ownerA = BoxEngine.createNew(box, CandyboxConfig.defaults(), store, 1,
                SystemClock.INSTANCE);
        ownerA.putCandy(CandyKey.of("k1"), bytes("v1"), null, Map.of(), null);
        ownerA.putCandy(CandyKey.of("k2"), bytes("v2"), null, Map.of(), null);
        // Deliberately do NOT flush or close: simulate a crash with data only in the WAL.
        manifestLedgerId = ownerA.manifestLedgerId();

        try (BoxEngine ownerB = BoxEngine.recover(box, CandyboxConfig.defaults(), store, 2,
                SystemClock.INSTANCE, manifestLedgerId)) {
            assertThat(ownerB.getCandy(CandyKey.of("k1"))).isEqualTo(bytes("v1"));
            assertThat(ownerB.getCandy(CandyKey.of("k2"))).isEqualTo(bytes("v2"));
        }
    }
}
