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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import me.predatorray.candybox.bookkeeper.fake.InMemoryLedgerStore;
import me.predatorray.candybox.common.BoxName;
import me.predatorray.candybox.common.CandyKey;
import me.predatorray.candybox.common.ManualClock;
import me.predatorray.candybox.common.config.CandyboxConfig;
import me.predatorray.candybox.common.serial.BinaryReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BoxEngineReverseScanTest {
    private static final int KEY_COUNT = 3000;
    private final InMemoryLedgerStore store = new InMemoryLedgerStore();
    private final BoxEngine engine = BoxEngine.createNew(BoxName.of("reverse-box"),
            CandyboxConfig.defaults(), store, 1, new ManualClock(1000), 1L);
    private final List<String> descending = IntStream.range(0, KEY_COUNT)
            .mapToObj(BoxEngineReverseScanTest::key).sorted(Comparator.reverseOrder()).toList();
    private int firstBlockCount;
    private int secondBlockCount;

    @BeforeEach
    void populateSSTable() {
        for (int i = 0; i < KEY_COUNT; i++) {
            // Object bytes live outside the SSTable: metadata enlarges the persisted locator.
            engine.putCandy(CandyKey.of(key(i)), new byte[100], null,
                    Map.of("padding", "x".repeat(100)), null);
        }
        engine.flush();
        assertThat(engine.manifestState().tables()).hasSize(1);
        try (var ledger = store.openLedger(engine.manifestState().tables().get(0).ledgerId())) {
            // Data blocks are the first ledger entries. Read their counts to prove this fixture
            // crosses blocks and to select an actual inter-block bound without hardcoded sizes.
            firstBlockCount = new BinaryReader(ledger.read(0).data()).readVarInt();
            secondBlockCount = new BinaryReader(ledger.read(1).data()).readVarInt();
            assertThat(firstBlockCount).isGreaterThan(2);
            assertThat(secondBlockCount).isGreaterThan(2);
            assertThat(firstBlockCount + secondBlockCount).isLessThan(KEY_COUNT);
        }
    }

    @AfterEach
    void close() {
        engine.close();
        store.close();
    }

    @Test
    void unboundedScanReturnsAllKeysDescending() {
        assertThat(keys(scan(null, null, KEY_COUNT + 1))).containsExactlyElementsOf(descending);
    }

    @Test
    void upperBoundInsideBlockStartsAtNextLowerKeyAndCrossesBlocks() {
        // Halfway through block 2 is guaranteed to have a preceding block to traverse.
        int bound = firstBlockCount + secondBlockCount / 2;
        assertBefore(key(bound));
    }

    @Test
    void upperBoundBetweenBlocksReturnsPreviousBlocksLastKey() {
        assertBefore(key(firstBlockCount - 1) + "x");
    }

    @Test
    void upperBoundBeforeFirstBlockReturnsEmpty() {
        assertThat(keys(scan("a", null, KEY_COUNT))).isEmpty();
    }

    @Test
    void cursorPagesReturnEveryKeyExactlyOnceDescending() {
        List<String> actual = new ArrayList<>();
        String cursor = null;
        for (int pageNumber = 0; pageNumber <= KEY_COUNT / 100; pageNumber++) {
            ListResult page = scan(null, cursor, 100);
            assertThat(page.entries()).hasSizeLessThanOrEqualTo(100);
            actual.addAll(keys(page));
            if (!page.isTruncated()) {
                assertThat(actual).containsExactlyElementsOf(descending);
                return;
            }
            assertThat(page.entries()).hasSize(100);
            assertThat(page.nextStartAfter()).isEqualTo(actual.get(actual.size() - 1));
            cursor = page.nextStartAfter();
        }
        throw new AssertionError("Reverse paging did not terminate");
    }

    private void assertBefore(String bound) {
        var result = scan(bound, null, KEY_COUNT + 1);
        assertThat(keys(result)).containsExactlyElementsOf(
                descending.stream().filter(k -> k.compareTo(bound) < 0).toList());
        assertThat(result.isTruncated()).isFalse();
    }

    private ListResult scan(String upperBound, String cursor, int maxKeys) {
        return engine.scanCandies(ScanQuery.reverse(null, null,
                upperBound == null ? null : CandyKey.of(upperBound),
                cursor == null ? null : CandyKey.of(cursor), maxKeys));
    }

    private static List<String> keys(ListResult result) {
        return result.entries().stream().map(entry -> entry.key().value()).toList();
    }

    private static String key(int i) {
        return String.format("k%05d", i);
    }
}
