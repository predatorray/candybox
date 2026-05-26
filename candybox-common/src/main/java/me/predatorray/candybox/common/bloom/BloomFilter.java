package me.predatorray.candybox.common.bloom;

import java.util.Collection;
import me.predatorray.candybox.common.exception.SerializationException;
import me.predatorray.candybox.common.serial.BinaryReader;
import me.predatorray.candybox.common.serial.BinaryWriter;

/**
 * A classic Bloom filter used per SSTable to short-circuit point {@code getCandy}/{@code headCandy}
 * lookups (it cannot help {@code listCandies} range scans). Modelled on LevelDB: one base hash plus a
 * rotation-derived delta drives the {@code k} probes (equivalent to double hashing), and the hash is
 * LevelDB's deterministic 32-bit hash so a filter serialized on one node decodes identically on
 * another.
 *
 * <p>Default of 10 bits/key gives roughly a 1% false-positive rate at the derived {@code k≈7}.
 * Immutable once built. Thread-safe for concurrent reads.
 */
public final class BloomFilter {

    private static final byte FORMAT_VERSION = 1;

    private final byte[] bits;
    private final int numBits;
    private final int numHashes;

    private BloomFilter(byte[] bits, int numBits, int numHashes) {
        this.bits = bits;
        this.numBits = numBits;
        this.numHashes = numHashes;
    }

    /**
     * Builds a filter sized for {@code keys} at the given bits-per-key.
     *
     * @param keys        the key set (raw bytes, e.g. UTF-8 CandyKey bytes)
     * @param bitsPerKey  space budget; 10 is the Candybox default
     * @return an immutable filter answering {@link #mightContain(byte[])}
     */
    public static BloomFilter build(Collection<byte[]> keys, int bitsPerKey) {
        if (bitsPerKey < 1) {
            throw new IllegalArgumentException("bitsPerKey must be >= 1");
        }
        int n = Math.max(1, keys.size());
        int numHashes = Math.max(1, Math.min(30, (int) Math.round(bitsPerKey * Math.log(2))));
        long bitCount = (long) n * bitsPerKey;
        int numBits = (int) Math.max(64, Math.min(Integer.MAX_VALUE - 64, bitCount));
        byte[] bits = new byte[(numBits + 7) / 8];
        BloomFilter filter = new BloomFilter(bits, numBits, numHashes);
        for (byte[] key : keys) {
            filter.addInternal(key);
        }
        return filter;
    }

    private void addInternal(byte[] key) {
        int h = hash(key);
        int delta = Integer.rotateRight(h, 17);
        for (int j = 0; j < numHashes; j++) {
            int bitpos = Integer.remainderUnsigned(h, numBits);
            bits[bitpos >>> 3] |= (byte) (1 << (bitpos & 7));
            h += delta;
        }
    }

    /**
     * @return {@code false} means the key is definitely absent; {@code true} means possibly present.
     */
    public boolean mightContain(byte[] key) {
        int h = hash(key);
        int delta = Integer.rotateRight(h, 17);
        for (int j = 0; j < numHashes; j++) {
            int bitpos = Integer.remainderUnsigned(h, numBits);
            if ((bits[bitpos >>> 3] & (1 << (bitpos & 7))) == 0) {
                return false;
            }
            h += delta;
        }
        return true;
    }

    public int numHashes() {
        return numHashes;
    }

    public int numBits() {
        return numBits;
    }

    public byte[] serialize() {
        return new BinaryWriter(bits.length + 16)
                .writeByte(FORMAT_VERSION)
                .writeVarInt(numBits)
                .writeVarInt(numHashes)
                .writeBytes(bits)
                .toByteArray();
    }

    public static BloomFilter deserialize(byte[] data) {
        BinaryReader r = new BinaryReader(data);
        int version = r.readByte();
        if (version != FORMAT_VERSION) {
            throw new SerializationException("Unsupported BloomFilter format version: " + version);
        }
        int numBits = r.readVarInt();
        int numHashes = r.readVarInt();
        byte[] bits = r.readBytes();
        if (bits.length != (numBits + 7) / 8) {
            throw new SerializationException("BloomFilter bit array size mismatch");
        }
        return new BloomFilter(bits, numBits, numHashes);
    }

    /** LevelDB's deterministic 32-bit hash (decode-stable across JVMs and architectures). */
    private static int hash(byte[] data) {
        final int seed = 0xbc9f1d34;
        final int m = 0xc6a4a793;
        int h = seed ^ (data.length * m);
        int i = 0;
        int len = data.length;
        for (; i + 4 <= len; i += 4) {
            int w = (data[i] & 0xFF)
                    | ((data[i + 1] & 0xFF) << 8)
                    | ((data[i + 2] & 0xFF) << 16)
                    | ((data[i + 3] & 0xFF) << 24);
            h += w;
            h *= m;
            h ^= (h >>> 16);
        }
        // Tail bytes (fall-through, matching LevelDB).
        switch (len - i) {
            case 3:
                h += (data[i + 2] & 0xFF) << 16;
                // fall through
            case 2:
                h += (data[i + 1] & 0xFF) << 8;
                // fall through
            case 1:
                h += (data[i] & 0xFF);
                h *= m;
                h ^= (h >>> 24);
                break;
            default:
                break;
        }
        return h;
    }
}
