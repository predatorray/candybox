package me.predatorray.candybox.lsm.sstable;

import static me.predatorray.candybox.lsm.TestData.hlc;
import static me.predatorray.candybox.lsm.TestData.putMutation;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import me.predatorray.candybox.bookkeeper.LedgerConfig;
import me.predatorray.candybox.bookkeeper.fake.InMemoryLedgerStore;
import me.predatorray.candybox.common.CandyKey;
import me.predatorray.candybox.common.Mutation;
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
