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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import me.predatorray.candybox.bookkeeper.fake.InMemoryLedgerStore;
import me.predatorray.candybox.common.BoxName;
import me.predatorray.candybox.common.CandyKey;
import me.predatorray.candybox.common.ManualClock;
import me.predatorray.candybox.common.config.CandyboxConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** {@link BoxEngine#scanCandies} with a delimiter: common-prefix roll-up, skip-ahead and paging. */
class BoxEngineDelimiterScanTest {

    private final InMemoryLedgerStore store = new InMemoryLedgerStore();
    private BoxEngine engine;

    @BeforeEach
    void setUp() {
        engine = BoxEngine.createNew(BoxName.of("my-box"), CandyboxConfig.defaults(), store, 1,
                new ManualClock(1000), 1L);
    }

    @AfterEach
    void tearDown() {
        engine.close();
        store.close();
    }

    private void put(String... keys) {
        for (String k : keys) {
            engine.putCandy(CandyKey.of(k), "x".getBytes(StandardCharsets.UTF_8), null, Map.of(), null);
        }
    }

    private void putDirectory(String dir, int count) {
        for (int i = 0; i < count; i++) {
            put(dir + String.format("%04d", i));
        }
    }

    private ListResult forward(String prefix, String cursor, int max) {
        return engine.scanCandies(new ScanQuery(prefix, null, null,
                cursor == null ? null : CandyKey.of(cursor), ScanDirection.FORWARD, max, "/"));
    }

    private ListResult reverse(String prefix, String cursor, int max) {
        return engine.scanCandies(new ScanQuery(prefix, null, null,
                cursor == null ? null : CandyKey.of(cursor), ScanDirection.REVERSE, max, "/"));
    }

    private static List<String> keys(ListResult r) {
        return r.entries().stream().map(e -> e.key().value()).toList();
    }

    @Test
    void rollsUpKeysAcrossMemtableAndSSTables() {
        putDirectory("a/", 50);
        put("b/1", "d/x/y");
        engine.flush();
        putDirectory("a/", 10);
        put("c", "e");

        ListResult r = forward(null, null, 100);
        assertThat(keys(r)).containsExactly("c", "e");
        assertThat(r.commonPrefixes()).containsExactly("a/", "b/", "d/");
        assertThat(r.isTruncated()).isFalse();
    }

    @Test
    void forwardPagesCountPrefixesAndNeverRepeatThem() {
        putDirectory("a/", 2000);
        put("b/1", "c", "d/1", "d/2");
        engine.flush();

        ListResult p1 = forward("", null, 2);
        assertThat(p1.commonPrefixes()).containsExactly("a/", "b/");
        assertThat(p1.entries()).isEmpty();
        assertThat(p1.nextStartAfter()).isEqualTo("b/");

        ListResult p2 = forward("", p1.nextStartAfter(), 2);
        assertThat(keys(p2)).containsExactly("c");
        assertThat(p2.commonPrefixes()).containsExactly("d/");
        assertThat(p2.isTruncated()).isFalse();
    }

    @Test
    void forwardCursorEndingInsideAPrefixReemitsIt() {
        putDirectory("a/", 10);
        put("b");
        ListResult r = forward(null, "a/0005", 10);
        assertThat(r.commonPrefixes()).containsExactly("a/");
        assertThat(keys(r)).containsExactly("b");
    }

    @Test
    void reversePagesRollUpInDescendingOrder() {
        put("a/1", "b", "c/1", "c/2", "d/x/1");
        putDirectory("c/", 500);
        engine.flush();

        List<String> rows = new ArrayList<>();
        String cursor = null;
        int pages = 0;
        do {
            ListResult r = reverse(null, cursor, 2);
            // Within one page, prefixes and keys interleave in descending order.
            rows.addAll(r.commonPrefixes());
            rows.addAll(keys(r));
            cursor = r.nextStartAfter();
            pages++;
        } while (cursor != null);
        assertThat(rows).containsExactlyInAnyOrder("d/", "c/", "b", "a/");
        assertThat(pages).isEqualTo(2);
    }

    @Test
    void prefixNarrowsTheRollUpAndKeepsTheKeyEqualToThePrefix() {
        put("a/", "a/x/1", "a/x/2", "a/y", "b/1");
        ListResult r = forward("a/", null, 10);
        assertThat(keys(r)).containsExactly("a/", "a/y");
        assertThat(r.commonPrefixes()).containsExactly("a/x/");
    }

    @Test
    void deletedKeysDoNotProduceAPrefix() {
        put("a/1", "b/1", "c/1");
        engine.flush();
        engine.deleteCandy(CandyKey.of("a/1"));
        engine.deleteRangeByPrefix("b/");
        ListResult r = forward(null, null, 10);
        assertThat(r.commonPrefixes()).containsExactly("c/");
    }

    @Test
    void multiCharacterDelimiter() {
        put("x--1", "x--2", "y", "z--a--b");
        ListResult r = engine.scanCandies(new ScanQuery(null, null, null, null,
                ScanDirection.FORWARD, 10, "--"));
        assertThat(keys(r)).containsExactly("y");
        assertThat(r.commonPrefixes()).containsExactly("x--", "z--");
    }

    @Test
    void emptyDelimiterMeansNone() {
        put("a/1", "a/2");
        ListResult r = engine.scanCandies(new ScanQuery(null, null, null, null,
                ScanDirection.FORWARD, 10, ""));
        assertThat(keys(r)).containsExactly("a/1", "a/2");
        assertThat(r.commonPrefixes()).isEmpty();
    }
}
