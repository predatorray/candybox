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
package me.predatorray.candybox.lsm.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import java.util.Optional;
import me.predatorray.candybox.bookkeeper.LedgerConfig;
import me.predatorray.candybox.bookkeeper.fake.InMemoryLedgerStore;
import me.predatorray.candybox.common.BoxName;
import me.predatorray.candybox.common.CandyKey;
import me.predatorray.candybox.common.ManualClock;
import me.predatorray.candybox.common.config.CandyboxConfig;
import me.predatorray.candybox.common.config.LedgerRole;
import me.predatorray.candybox.common.exception.BusyException;
import me.predatorray.candybox.common.exception.FencedException;
import me.predatorray.candybox.common.exception.StorageException;
import me.predatorray.candybox.lsm.compaction.Compactor;
import me.predatorray.candybox.lsm.compaction.CompactionTask;
import me.predatorray.candybox.lsm.compaction.LeveledCompactionStrategy;
import me.predatorray.candybox.lsm.sstable.SSTableWriter;
import org.junit.jupiter.api.Test;

/**
 * Failure-path / fault-injection tests at the engine level, driven by the adversarial fakes
 * ({@code InMemoryLedgerStore} bookie loss, recover-open fencing). These are the tests the fakes were
 * built to make non-vacuous: they assert clean failures and clean recovery, not just happy paths.
 */
class EngineFaultInjectionTest {

    private static final BoxName BOX = BoxName.of("fault-box");

    private static byte[] bytes(String s) {
        return s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    @Test
    void putFailsCleanlyOnAckQuorumLossThenRecovers() {
        InMemoryLedgerStore store = new InMemoryLedgerStore();
        try (BoxEngine engine = BoxEngine.createNew(BOX, CandyboxConfig.defaults(), store, 1,
                new ManualClock(1000), 1L)) {
            // Lose enough bookies that a Syrup ledger cannot even be created.
            store.setAvailableBookies(1);
            assertThatThrownBy(() -> engine.putCandy(CandyKey.of("k"), bytes("v"), null, Map.of(), null))
                    .isInstanceOf(StorageException.class);

            // Bookies return; the engine is still usable and the write now succeeds.
            store.setAvailableBookies(Integer.MAX_VALUE);
            engine.putCandy(CandyKey.of("k"), bytes("v"), null, Map.of(), null);
            assertThat(engine.getCandy(CandyKey.of("k"))).isEqualTo(bytes("v"));
        }
        store.close();
    }

    @Test
    void fencedOwnerCannotFlushAfterHandover() {
        InMemoryLedgerStore store = new InMemoryLedgerStore();
        ManualClock clock = new ManualClock(1000);
        BoxEngine ownerA = BoxEngine.createNew(BOX, CandyboxConfig.defaults(), store, 1, clock, 1L);
        ownerA.putCandy(CandyKey.of("k"), bytes("v1"), null, Map.of(), null); // unflushed: in A's memtable

        // A new owner recovers (fencing A's manifest and WAL).
        try (BoxEngine ownerB = BoxEngine.recover(BOX, CandyboxConfig.defaults(), store, 2, clock,
                ownerA.manifestLedgerId(), 2L)) {
            // A is fenced: it can neither stamp new writes nor flush its memtable into a committed table.
            assertThatThrownBy(() -> ownerA.putCandy(CandyKey.of("k2"), bytes("x"), null, Map.of(), null))
                    .isInstanceOf(FencedException.class);
            assertThatThrownBy(ownerA::flush).isInstanceOf(FencedException.class);
            // B took over and serves the recovered data.
            assertThat(ownerB.getCandy(CandyKey.of("k"))).isEqualTo(bytes("v1"));
        }
        store.close();
    }

    @Test
    void writeStallReturnsBusyThenResumesAfterCompaction() {
        CandyboxConfig config = CandyboxConfig.builder()
                .memtableFlushThresholdBytes(1) // each put flushes -> one L0 table
                .l0CompactionTrigger(2)
                .l0StallThreshold(3)
                .build();
        InMemoryLedgerStore store = new InMemoryLedgerStore();
        ManualClock clock = new ManualClock(1000);
        try (BoxEngine engine = BoxEngine.createNew(BOX, config, store, 1, clock, 1L)) {
            for (int i = 0; i < 3; i++) {
                engine.putCandy(CandyKey.of("k" + i), bytes("v"), null, Map.of(), null);
            }
            // L0 == 3 == stall threshold: the next write is rejected as retriable BUSY.
            assertThatThrownBy(() -> engine.putCandy(CandyKey.of("k3"), bytes("v"), null, Map.of(), null))
                    .isInstanceOf(BusyException.class);

            // Compaction drains L0; the previously-stalled write then succeeds.
            Compactor compactor = new Compactor(store, new SSTableWriter(store, config.bloomBitsPerKey()),
                    LedgerConfig.forRole(LedgerRole.SSTABLE), config.tombstoneGcGraceMillis(), clock);
            Optional<CompactionTask> task =
                    new LeveledCompactionStrategy(config.l0CompactionTrigger()).pickCompaction(engine.manifestState());
            engine.applyCompaction(compactor.compact(task.orElseThrow()).edit());
            assertThat(engine.manifestState().level0()).isEmpty();

            engine.putCandy(CandyKey.of("k3"), bytes("v"), null, Map.of(), null);
            assertThat(engine.getCandy(CandyKey.of("k3"))).isEqualTo(bytes("v"));
        }
        store.close();
    }

    @Test
    void retriedPutWithSameTokenWritesNoSecondSyrup() {
        InMemoryLedgerStore store = new InMemoryLedgerStore();
        try (BoxEngine engine = BoxEngine.createNew(BOX, CandyboxConfig.defaults(), store, 1,
                new ManualClock(1000), 1L)) {
            engine.putCandy(CandyKey.of("k"), bytes("payload"), null, Map.of(), "tok-1");
            int ledgersAfterFirst = store.listLedgers().size();

            // A retried (timed-out) put with the same idempotency token must not write a second Syrup.
            engine.putCandy(CandyKey.of("k"), bytes("payload"), null, Map.of(), "tok-1");
            assertThat(store.listLedgers().size()).isEqualTo(ledgersAfterFirst);
        }
        store.close();
    }
}
