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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import me.predatorray.candybox.bookkeeper.LedgerConfig;
import me.predatorray.candybox.bookkeeper.LedgerStore;
import me.predatorray.candybox.bookkeeper.ReadableLedger;
import me.predatorray.candybox.bookkeeper.WritableLedger;
import me.predatorray.candybox.bookkeeper.fake.InMemoryLedgerStore;
import me.predatorray.candybox.common.BoxName;
import me.predatorray.candybox.common.CandyKey;
import me.predatorray.candybox.common.ManualClock;
import me.predatorray.candybox.common.config.CandyboxConfig;
import me.predatorray.candybox.common.config.LedgerRole;
import me.predatorray.candybox.common.config.QuorumConfig;
import me.predatorray.candybox.coordination.fake.InMemoryCoordinationService;
import me.predatorray.candybox.lsm.engine.BoxEngine;
import org.junit.jupiter.api.Test;

class CompactionServiceTest {

    @Test
    void compactionOutputsUseTheConfiguredSstableQuorumAndBoxMetadata() {
        QuorumConfig sstableQuorum = new QuorumConfig(1, 1, 1);
        CandyboxConfig cfg = CandyboxConfig.builder()
                .memtableFlushThresholdBytes(1)
                .l0CompactionTrigger(2)
                .l0StallThreshold(100)
                .quorum(LedgerRole.SSTABLE, sstableQuorum)
                .build();
        RecordingLedgerStore store = new RecordingLedgerStore(new InMemoryLedgerStore());
        ManualClock clock = new ManualClock(1000);
        try (CandyboxNode node = new CandyboxNode(1, cfg, store, new InMemoryCoordinationService(),
                clock)) {
            node.createBox(BoxName.of("my-box"), 1);
            BoxEngine engine = node.enginePartition(BoxName.of("my-box"), 0);
            for (int i = 0; i < 2; i++) {
                engine.putCandy(CandyKey.of("key-" + i), "v".getBytes(StandardCharsets.UTF_8), null,
                        Map.of(), null);
            }

            store.created.clear();
            assertThat(new CompactionService(store, cfg, clock).compactOnce(engine)).isTrue();

            assertThat(store.created).isNotEmpty().allSatisfy(config -> {
                assertThat(config.quorum()).isEqualTo(sstableQuorum);
                assertThat(new String(config.customMetadata().get("candybox-role"),
                        StandardCharsets.UTF_8)).isEqualTo("SSTABLE");
                assertThat(new String(config.customMetadata().get("candybox-box"),
                        StandardCharsets.UTF_8)).isEqualTo("my-box");
            });
        }
        store.close();
    }

    /** Records the config of every ledger it creates. */
    private static final class RecordingLedgerStore implements LedgerStore {
        private final LedgerStore delegate;
        final List<LedgerConfig> created = new ArrayList<>();

        RecordingLedgerStore(LedgerStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public synchronized WritableLedger createLedger(LedgerConfig config) {
            created.add(config);
            return delegate.createLedger(config);
        }

        @Override
        public ReadableLedger openLedger(long ledgerId) {
            return delegate.openLedger(ledgerId);
        }

        @Override
        public ReadableLedger recoverOpen(long ledgerId) {
            return delegate.recoverOpen(ledgerId);
        }

        @Override
        public void deleteLedger(long ledgerId) {
            delegate.deleteLedger(ledgerId);
        }

        @Override
        public List<Long> listLedgers() {
            return delegate.listLedgers();
        }

        @Override
        public void close() {
            delegate.close();
        }
    }
}
