package me.predatorray.candybox.lsm.sstable;

/**
 * Shared constants for the SSTable on-ledger layout. An SSTable ledger is laid out as:
 *
 * <pre>
 *   entry 0 .. B-1   data blocks   (each block = a run of length-prefixed Mutations, key-ascending)
 *   entry B          bloom block   (serialized BloomFilter over all keys)
 *   entry B+1        index block   (per data block: lastKey + its entry id)
 *   entry B+2 (=LAC) footer        (magic, version, bloom/index entry ids, counts, min/max key)
 * </pre>
 *
 * <p>The footer is always the last entry, so a reader finds it at {@code lastAddConfirmed()} and from
 * there locates the index and bloom. One block maps to one ledger entry; block size targets ~64 KiB.
 */
final class SSTableFormat {

    static final int FOOTER_MAGIC = 0x53535442; // "SSTB"
    static final byte FORMAT_VERSION = 1;
    static final int DEFAULT_DATA_BLOCK_TARGET_BYTES = 64 * 1024;

    private SSTableFormat() {
    }
}
