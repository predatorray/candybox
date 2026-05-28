package me.predatorray.candybox.lsm.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import me.predatorray.candybox.bookkeeper.fake.InMemoryLedgerStore;
import me.predatorray.candybox.common.BoxName;
import me.predatorray.candybox.common.CandyKey;
import me.predatorray.candybox.common.ManualClock;
import me.predatorray.candybox.common.config.CandyboxConfig;
import me.predatorray.candybox.common.exception.BusyException;
import org.junit.jupiter.api.Test;

/** The engine's operational counters reflect the work it performs. */
class BoxEngineStatsTest {

    private static byte[] bytes(String s) {
        return s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    @Test
    void countersTrackOperations() {
        InMemoryLedgerStore store = new InMemoryLedgerStore();
        try (BoxEngine engine = BoxEngine.createNew(BoxName.of("stats-box"), CandyboxConfig.defaults(),
                store, 1, new ManualClock(1000), 1L)) {
            engine.putCandy(CandyKey.of("a"), bytes("1"), null, Map.of(), null);
            engine.putCandy(CandyKey.of("b"), bytes("2"), null, Map.of(), null);
            engine.getCandy(CandyKey.of("a"));
            engine.headCandy(CandyKey.of("b"));
            engine.listCandies("", null, 10);
            engine.flush();
            engine.deleteCandy(CandyKey.of("a"));

            BoxEngineStats stats = engine.stats();
            assertThat(stats.puts()).isEqualTo(2);
            assertThat(stats.gets()).isEqualTo(1);
            assertThat(stats.heads()).isEqualTo(1);
            assertThat(stats.lists()).isEqualTo(1);
            assertThat(stats.flushes()).isEqualTo(1);
            assertThat(stats.deletes()).isEqualTo(1);
            assertThat(stats.compactions()).isZero();
            assertThat(stats.stallRejections()).isZero();
        }
        store.close();
    }

    @Test
    void stallRejectionsAreCounted() {
        CandyboxConfig config = CandyboxConfig.builder()
                .memtableFlushThresholdBytes(1)
                .l0CompactionTrigger(1)
                .l0StallThreshold(2)
                .build();
        InMemoryLedgerStore store = new InMemoryLedgerStore();
        try (BoxEngine engine = BoxEngine.createNew(BoxName.of("stall-box"), config, store, 1,
                new ManualClock(1000), 1L)) {
            engine.putCandy(CandyKey.of("k1"), bytes("v"), null, Map.of(), null); // L0 -> 1
            engine.putCandy(CandyKey.of("k2"), bytes("v"), null, Map.of(), null); // L0 -> 2
            assertThatThrownBy(() -> engine.putCandy(CandyKey.of("k3"), bytes("v"), null, Map.of(), null))
                    .isInstanceOf(BusyException.class);
            assertThat(engine.stats().stallRejections()).isEqualTo(1);
        }
        store.close();
    }
}
