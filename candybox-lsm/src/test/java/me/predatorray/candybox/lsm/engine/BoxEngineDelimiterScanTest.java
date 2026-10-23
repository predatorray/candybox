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
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import me.predatorray.candybox.bookkeeper.LedgerConfig;
import me.predatorray.candybox.bookkeeper.LedgerEntry;
import me.predatorray.candybox.bookkeeper.LedgerStore;
import me.predatorray.candybox.bookkeeper.ReadableLedger;
import me.predatorray.candybox.bookkeeper.WritableLedger;
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

    private final CountingLedgerStore store = new CountingLedgerStore(new InMemoryLedgerStore());
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

        ListResult p1 = reverse(null, null, 2);
        assertThat(p1.commonPrefixes()).containsExactly("d/", "c/");
        assertThat(p1.entries()).isEmpty();
        assertThat(p1.nextStartAfter()).isEqualTo("c/");

        ListResult p2 = reverse(null, p1.nextStartAfter(), 2);
        assertThat(keys(p2)).containsExactly("b");
        assertThat(p2.commonPrefixes()).containsExactly("a/");
        assertThat(p2.isTruncated()).isFalse();
    }

    @Test
    void cursorPrefixSkipsKeysThisPartitionNeverEmitted() {
        // Another partition emitted "a/" as its page's last row; this one resumes past all of "a/".
        putDirectory("a/", 20);
        put("b");
        ListResult r = forward(null, "a/", 10);
        assertThat(r.commonPrefixes()).isEmpty();
        assertThat(keys(r)).containsExactly("b");
    }

    @Test
    void windowEndingInsideAPrefixStopsAtTheWindow() {
        putDirectory("a/", 40);
        put("b/1", "c");
        engine.flush();
        CandyKey start = CandyKey.of("a/0010");
        CandyKey end = CandyKey.of("a/0020");

        ListResult fwd = engine.scanCandies(new ScanQuery(null, start, end, null,
                ScanDirection.FORWARD, 10, "/"));
        assertThat(fwd.commonPrefixes()).containsExactly("a/");
        assertThat(fwd.entries()).isEmpty();
        assertThat(fwd.isTruncated()).isFalse();

        ListResult rev = engine.scanCandies(new ScanQuery(null, CandyKey.of("a/0030"),
                CandyKey.of("c"), null, ScanDirection.REVERSE, 10, "/"));
        assertThat(rev.commonPrefixes()).containsExactly("b/", "a/");
        assertThat(rev.isTruncated()).isFalse();
    }

    @Test
    void manySmallPrefixesAreSteppedOverWithoutReseeking() {
        // 10 SSTables, each holding one key of every directory.
        for (int t = 0; t < 10; t++) {
            for (int d = 0; d < 300; d++) {
                put(String.format("d%04d/%d", d, t));
            }
            engine.flush();
        }
        store.reads.set(0);
        engine.scanCandies(ScanQuery.forward(null, null, 1000));
        long flatReads = store.reads.get();

        store.reads.set(0);
        ListResult r = forward(null, null, 1000);
        assertThat(r.commonPrefixes()).hasSize(300);
        assertThat(store.reads.get()).isLessThanOrEqualTo(flatReads * 2);
    }

    @Test
    void aLargePrefixIsSeekedPastNotWalked() {
        for (int i = 0; i < 20_000; i++) {
            put(String.format("a/%05d", i));
        }
        put("b");
        engine.flush();
        store.reads.set(0);
        engine.scanCandies(ScanQuery.forward(null, null, 20_001)); // walks every block
        long walkReads = store.reads.get();

        store.reads.set(0);
        ListResult r = forward(null, null, 10);
        assertThat(r.commonPrefixes()).containsExactly("a/");
        assertThat(keys(r)).containsExactly("b");
        assertThat(walkReads).isGreaterThan(10);
        assertThat(store.reads.get()).isLessThanOrEqualTo(3);
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

    /** Counts entry reads on ledgers opened for reading (SSTable blocks). */
    private static final class CountingLedgerStore implements LedgerStore {
        final AtomicLong reads = new AtomicLong();
        private final LedgerStore delegate;

        CountingLedgerStore(LedgerStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public WritableLedger createLedger(LedgerConfig config) {
            return delegate.createLedger(config);
        }

        @Override
        public ReadableLedger openLedger(long ledgerId) {
            ReadableLedger ledger = delegate.openLedger(ledgerId);
            return new ReadableLedger() {
                @Override
                public LedgerEntry read(long entryId) {
                    reads.incrementAndGet();
                    return ledger.read(entryId);
                }

                @Override
                public List<LedgerEntry> readRange(long firstEntryId, long lastEntryId) {
                    reads.incrementAndGet();
                    return ledger.readRange(firstEntryId, lastEntryId);
                }

                @Override
                public long ledgerId() {
                    return ledger.ledgerId();
                }

                @Override
                public long lastAddConfirmed() {
                    return ledger.lastAddConfirmed();
                }

                @Override
                public boolean isSealed() {
                    return ledger.isSealed();
                }

                @Override
                public Map<String, byte[]> customMetadata() {
                    return ledger.customMetadata();
                }

                @Override
                public void close() {
                    ledger.close();
                }
            };
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
