package me.predatorray.candybox.common.checksum;

import java.util.zip.CRC32C;

/**
 * Thin helper over the JDK's {@link CRC32C} (Castagnoli). Used for end-to-end Candy validation across
 * chunk reassembly and per-chunk Syrup entry validation. Ledger-level integrity (WAL, SSTable blocks,
 * manifest entries) relies on BookKeeper's own per-entry digest, so those are not CRC'd again here.
 */
public final class Crc32c {

    private Crc32c() {
    }

    /** CRC32C of an entire array, truncated to the low 32 bits. */
    public static int of(byte[] data) {
        return of(data, 0, data.length);
    }

    /** CRC32C of a slice, truncated to the low 32 bits. */
    public static int of(byte[] data, int offset, int length) {
        CRC32C crc = new CRC32C();
        crc.update(data, offset, length);
        return (int) crc.getValue();
    }

    /** A reusable, incremental accumulator for streaming whole-object checksums. */
    public static final class Accumulator {
        private final CRC32C crc = new CRC32C();

        public void update(byte[] data, int offset, int length) {
            crc.update(data, offset, length);
        }

        public int value() {
            return (int) crc.getValue();
        }
    }
}
