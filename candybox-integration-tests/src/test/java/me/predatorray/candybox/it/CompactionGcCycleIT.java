package me.predatorray.candybox.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import me.predatorray.candybox.bookkeeper.bk.BookKeeperLedgerStore;
import me.predatorray.candybox.common.BoxName;
import me.predatorray.candybox.common.CandyKey;
import me.predatorray.candybox.common.SystemClock;
import me.predatorray.candybox.common.config.CandyboxConfig;
import me.predatorray.candybox.coordination.zk.ZooKeeperCoordinationService;
import me.predatorray.candybox.server.CandyboxNode;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * A full compaction-and-GC cycle on real backends (embedded BookKeeper + in-process ZooKeeper): write
 * several Candies (including an overwrite), compact, then GC — and verify that obsolete SSTable, WAL,
 * and orphaned Syrup ledgers are physically deleted from BookKeeper while the live data stays readable.
 */
class CompactionGcCycleIT {

    private static EmbeddedBookKeeper bookKeeper;
    private static TestingServer zookeeper;

    @BeforeAll
    static void start() throws Exception {
        bookKeeper = new EmbeddedBookKeeper(3);
        zookeeper = new TestingServer(true);
    }

    @AfterAll
    static void stop() throws Exception {
        if (zookeeper != null) {
            zookeeper.close();
        }
        if (bookKeeper != null) {
            bookKeeper.close();
        }
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void compactionThenGcReclaimsLedgersOnRealBookKeeper() {
        CandyboxConfig config = CandyboxConfig.builder()
                .memtableFlushThresholdBytes(1) // each put flushes => L0 table + WAL rotation
                .syrupRolloverBytes(1)          // each Candy in its own Syrup
                .l0CompactionTrigger(2)
                .l0StallThreshold(100)
                .ledgerGcGraceMillis(0)         // reclaim immediately
                .leaseRenewIntervalMillis(0)
                .compactionIntervalMillis(0)    // drive compaction/GC manually
                .build();
        BoxName box = BoxName.of("gc-box");

        BookKeeperLedgerStore store = new BookKeeperLedgerStore(bookKeeper.clientConfiguration(),
                bytes("candybox"));
        ZooKeeperCoordinationService coordination =
                new ZooKeeperCoordinationService(zookeeper.getConnectString(), SystemClock.INSTANCE);
        CandyboxNode node = new CandyboxNode(1, config, store, coordination, SystemClock.INSTANCE);
        try {
            node.createBox(box);
            for (int i = 0; i < 4; i++) {
                node.engine(box).putCandy(CandyKey.of("k" + i), bytes("v"), null, Map.of(), null);
            }
            node.engine(box).putCandy(CandyKey.of("k0"), bytes("v-new"), null, Map.of(), null); // overwrite

            int ledgersBefore = store.listLedgers().size();

            node.compactOwnedBoxesOnce(); // merge L0 -> L1 (keeps k0=v-new; old k0 Syrup orphaned)
            int deleted = node.collectGarbageOnce(); // delete obsolete SSTables + orphan Syrup + WALs
            assertThat(deleted).isGreaterThan(0);

            int ledgersAfter = store.listLedgers().size();
            assertThat(ledgersAfter).isLessThan(ledgersBefore);

            // The live data is intact after compaction + GC.
            assertThat(node.engine(box).getCandy(CandyKey.of("k0"))).isEqualTo(bytes("v-new"));
            for (int i = 1; i < 4; i++) {
                assertThat(node.engine(box).getCandy(CandyKey.of("k" + i))).isEqualTo(bytes("v"));
            }
        } finally {
            node.close();
            coordination.close();
            store.close();
        }
    }
}
