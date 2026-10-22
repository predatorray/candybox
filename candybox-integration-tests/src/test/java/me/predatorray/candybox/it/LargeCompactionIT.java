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
package me.predatorray.candybox.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import me.predatorray.candybox.bookkeeper.LedgerConfig;
import me.predatorray.candybox.bookkeeper.bk.BookKeeperLedgerStore;
import me.predatorray.candybox.common.CandyKey;
import me.predatorray.candybox.common.CandyLocator;
import me.predatorray.candybox.common.Hlc;
import me.predatorray.candybox.common.Mutation;
import me.predatorray.candybox.common.SegmentRef;
import me.predatorray.candybox.common.SystemClock;
import me.predatorray.candybox.common.config.LedgerRole;
import me.predatorray.candybox.lsm.compaction.CompactionResult;
import me.predatorray.candybox.lsm.compaction.CompactionTask;
import me.predatorray.candybox.lsm.compaction.Compactor;
import me.predatorray.candybox.lsm.sstable.SSTableMeta;
import me.predatorray.candybox.lsm.sstable.SSTableReader;
import me.predatorray.candybox.lsm.sstable.SSTableWriter;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Compacts inputs whose merged bloom filter would exceed BookKeeper's default 5 MiB entry limit if
 * written as one table. A high bits-per-key stands in for the millions of keys a real partition needs
 * to reach that size at the default 10 bits/key.
 */
class LargeCompactionIT {

    private static final int KEYS_PER_INPUT = 25_000;
    private static final int BLOOM_BITS_PER_KEY = 1000; // 125 B/key: 50k merged keys => ~6 MiB bloom

    private static EmbeddedBookKeeper bookKeeper;

    @BeforeAll
    static void start() {
        bookKeeper = new EmbeddedBookKeeper(3);
    }

    @AfterAll
    static void stop() {
        if (bookKeeper != null) {
            bookKeeper.close();
        }
    }

    @Test
    void compactionSplitsOutputSoMetaBlocksFitInOneBookKeeperEntry() {
        BookKeeperLedgerStore store = new BookKeeperLedgerStore(bookKeeper.clientConfiguration(),
                "candybox".getBytes(StandardCharsets.UTF_8));
        try {
            LedgerConfig cfg = LedgerConfig.forRole(LedgerRole.SSTABLE);
            SSTableWriter writer = new SSTableWriter(store, BLOOM_BITS_PER_KEY);
            SSTableMeta even = writer.write(cfg, 0, mutations(0).iterator());
            SSTableMeta odd = writer.write(cfg, 0, mutations(1).iterator());

            Compactor compactor = new Compactor(store, writer, cfg, 0, SystemClock.INSTANCE);
            CompactionResult result = compactor.compact(
                    new CompactionTask(List.of(even, odd), 1, true, List.of()));

            List<SSTableMeta> outputs = result.edit().addedTables();
            assertThat(outputs).hasSizeGreaterThan(1);
            assertThat(outputs.stream().mapToLong(SSTableMeta::entryCount).sum())
                    .isEqualTo(2L * KEYS_PER_INPUT);
            for (int i = 1; i < outputs.size(); i++) {
                assertThat(outputs.get(i - 1).maxKey()).isLessThan(outputs.get(i).minKey());
            }

            int seen = 0;
            for (SSTableMeta out : outputs) {
                try (SSTableReader reader = new SSTableReader(store, out.ledgerId())) {
                    Iterator<Mutation> it = reader.scan(null);
                    while (it.hasNext()) {
                        assertThat(it.next().key()).isEqualTo(key(seen++));
                    }
                    assertThat(reader.get(out.minKey())).isPresent();
                }
            }
            assertThat(seen).isEqualTo(2 * KEYS_PER_INPUT);
        } finally {
            store.close();
        }
    }

    private static List<Mutation> mutations(int parity) {
        List<Mutation> out = new ArrayList<>(KEYS_PER_INPUT);
        for (int i = 0; i < KEYS_PER_INPUT; i++) {
            int k = 2 * i + parity;
            Hlc hlc = new Hlc(1_000 + k, 0, 1);
            out.add(new Mutation(key(k), CandyLocator.singlePart(hlc, 1, 1 << 20,
                    "application/octet-stream", Map.of(), 0, hlc.physicalMillis(),
                    List.of(new SegmentRef(1, k, k)))));
        }
        return out;
    }

    private static CandyKey key(int i) {
        return CandyKey.of(String.format("key-%08d", i));
    }
}
