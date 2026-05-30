package me.predatorray.candybox.lsm.sstable;

import static me.predatorray.candybox.lsm.TestData.hlc;
import static me.predatorray.candybox.lsm.TestData.putMutation;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import me.predatorray.candybox.bookkeeper.LedgerConfig;
import me.predatorray.candybox.bookkeeper.fake.InMemoryLedgerStore;
import me.predatorray.candybox.common.CandyKey;
import me.predatorray.candybox.common.Hlc;
import me.predatorray.candybox.common.Mutation;
import me.predatorray.candybox.common.RangeTombstone;
import me.predatorray.candybox.common.config.LedgerRole;
import org.junit.jupiter.api.Test;

class SSTableTest {

    private final InMemoryLedgerStore store = new InMemoryLedgerStore();
    private final LedgerConfig config = LedgerConfig.forRole(LedgerRole.SSTABLE);

    private SSTableMeta writeKeys(int n) {
        List<Mutation> sorted = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            sorted.add(putMutation(String.format("key-%05d", i), hlc(i + 1, 0, 1)));
        }
        // Tiny block target to force multiple data blocks and exercise the index.
        return new SSTableWriter(store, 10, 256).write(config, 0, sorted.iterator());
    }

    @Test
    void pointLookupsFindPresentKeysAndMissAbsentOnes() {
        SSTableMeta meta = writeKeys(500);
        try (SSTableReader reader = new SSTableReader(store, meta.ledgerId())) {
            assertThat(reader.entryCount()).isEqualTo(500);
            assertThat(reader.minKey()).isEqualTo(CandyKey.of("key-00000"));
            assertThat(reader.maxKey()).isEqualTo(CandyKey.of("key-00499"));

            assertThat(reader.get(CandyKey.of("key-00000"))).isPresent();
            assertThat(reader.get(CandyKey.of("key-00250"))).isPresent();
            assertThat(reader.get(CandyKey.of("key-00499"))).isPresent();
            assertThat(reader.get(CandyKey.of("key-99999"))).isEmpty();
            assertThat(reader.get(CandyKey.of("absent"))).isEmpty();
        }
    }

    @Test
    void rangeScanReturnsSortedTailFromStartKey() {
        SSTableMeta meta = writeKeys(100);
        try (SSTableReader reader = new SSTableReader(store, meta.ledgerId())) {
            List<String> keys = new ArrayList<>();
            var it = reader.scan(CandyKey.of("key-00098"));
            while (it.hasNext()) {
                keys.add(it.next().key().value());
            }
            assertThat(keys).containsExactly("key-00098", "key-00099");
        }
    }

    @Test
    void reverseScanReturnsDescendingTailFromStartKey() {
        SSTableMeta meta = writeKeys(100); // tiny blocks: reverse must walk across blocks
        try (SSTableReader reader = new SSTableReader(store, meta.ledgerId())) {
            List<String> keys = new ArrayList<>();
            var it = reader.scanReverse(CandyKey.of("key-00002"));
            while (it.hasNext()) {
                keys.add(it.next().key().value());
            }
            assertThat(keys).containsExactly("key-00002", "key-00001", "key-00000");
        }
    }

    @Test
    void reverseFullScanReturnsEverythingDescending() {
        SSTableMeta meta = writeKeys(50);
        try (SSTableReader reader = new SSTableReader(store, meta.ledgerId())) {
            List<String> keys = new ArrayList<>();
            var it = reader.scanReverse(null);
            while (it.hasNext()) {
                keys.add(it.next().key().value());
            }
            assertThat(keys).hasSize(50);
            assertThat(keys.get(0)).isEqualTo("key-00049");
            assertThat(keys.get(49)).isEqualTo("key-00000");
            for (int i = 1; i < keys.size(); i++) {
                assertThat(keys.get(i).compareTo(keys.get(i - 1))).isLessThan(0);
            }
        }
    }

    @Test
    void reverseScanFromKeyBeyondMaxStartsAtLast() {
        SSTableMeta meta = writeKeys(10);
        try (SSTableReader reader = new SSTableReader(store, meta.ledgerId())) {
            var it = reader.scanReverse(CandyKey.of("zzz")); // beyond the table's max key
            assertThat(it.hasNext()).isTrue();
            assertThat(it.next().key().value()).isEqualTo("key-00009");
        }
    }

    @Test
    void persistsAndReadsBackRangeTombstones() {
        List<Mutation> sorted = List.of(putMutation("a", hlc(1, 0, 1)), putMutation("z", hlc(2, 0, 1)));
        List<RangeTombstone> rts = List.of(
                new RangeTombstone(CandyKey.of("b"), CandyKey.of("e"), hlc(10, 0, 1)));
        SSTableMeta meta = new SSTableWriter(store, 10, 256)
                .write(config, 0, sorted.iterator(), rts);

        try (SSTableReader reader = new SSTableReader(store, meta.ledgerId())) {
            assertThat(reader.rangeTombstones()).hasSize(1);
            assertThat(reader.maxRangeTombstoneCovering(CandyKey.of("c"))).isEqualTo(hlc(10, 0, 1));
            assertThat(reader.maxRangeTombstoneCovering(CandyKey.of("e"))).isNull(); // end exclusive
            // Point data is still readable alongside the tombstones.
            assertThat(reader.get(CandyKey.of("a"))).isPresent();
            assertThat(reader.get(CandyKey.of("z"))).isPresent();
        }
    }

    @Test
    void writesRangeOnlyTableWithNoPointData() {
        List<RangeTombstone> rts = List.of(
                new RangeTombstone(CandyKey.of("m"), null, new Hlc(5, 0, 1))); // unbounded end
        SSTableMeta meta = new SSTableWriter(store, 10, 256)
                .write(config, 0, List.<Mutation>of().iterator(), rts);

        assertThat(meta.entryCount()).isZero();
        try (SSTableReader reader = new SSTableReader(store, meta.ledgerId())) {
            assertThat(reader.rangeTombstones()).hasSize(1);
            assertThat(reader.maxRangeTombstoneCovering(CandyKey.of("zzz"))).isEqualTo(new Hlc(5, 0, 1));
            assertThat(reader.get(CandyKey.of("anything"))).isEmpty(); // no point data
            assertThat(reader.scan(null).hasNext()).isFalse();
            assertThat(reader.scanReverse(null).hasNext()).isFalse();
        }
    }

    @Test
    void fullScanReturnsEverythingInOrder() {
        SSTableMeta meta = writeKeys(50);
        try (SSTableReader reader = new SSTableReader(store, meta.ledgerId())) {
            int count = 0;
            String prev = null;
            var it = reader.scan(null);
            while (it.hasNext()) {
                String k = it.next().key().value();
                if (prev != null) {
                    assertThat(k.compareTo(prev)).isGreaterThan(0);
                }
                prev = k;
                count++;
            }
            assertThat(count).isEqualTo(50);
        }
    }
}
