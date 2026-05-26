package me.predatorray.candybox.common.bloom;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class BloomFilterTest {

    private static byte[] key(int i) {
        return ("candy-key-" + i).getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void neverReportsFalseNegative() {
        List<byte[]> keys = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            keys.add(key(i));
        }
        BloomFilter filter = BloomFilter.build(keys, 10);
        for (byte[] k : keys) {
            assertThat(filter.mightContain(k)).as("present key must test positive").isTrue();
        }
    }

    @Test
    void falsePositiveRateIsReasonable() {
        List<byte[]> keys = new ArrayList<>();
        for (int i = 0; i < 10_000; i++) {
            keys.add(key(i));
        }
        BloomFilter filter = BloomFilter.build(keys, 10);

        int falsePositives = 0;
        int probes = 10_000;
        for (int i = 10_000; i < 10_000 + probes; i++) {
            if (filter.mightContain(key(i))) {
                falsePositives++;
            }
        }
        // 10 bits/key targets ~1%; allow generous slack to avoid flakiness.
        assertThat((double) falsePositives / probes).isLessThan(0.05);
    }

    @Test
    void survivesSerializationRoundTrip() {
        List<byte[]> keys = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            keys.add(key(i));
        }
        BloomFilter original = BloomFilter.build(keys, 12);
        BloomFilter restored = BloomFilter.deserialize(original.serialize());

        assertThat(restored.numBits()).isEqualTo(original.numBits());
        assertThat(restored.numHashes()).isEqualTo(original.numHashes());
        for (byte[] k : keys) {
            assertThat(restored.mightContain(k)).isTrue();
        }
    }
}
