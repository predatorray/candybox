package me.predatorray.candybox.common.serial;

import me.predatorray.candybox.common.CandyKey;
import me.predatorray.candybox.common.CandyLocator;
import me.predatorray.candybox.common.Mutation;
import me.predatorray.candybox.common.exception.SerializationException;

/**
 * Versioned binary codec for a {@link Mutation} (a CandyKey + its CandyLocator). Used for WAL entries
 * and SSTable data-block records.
 *
 * <pre>
 *   byte   formatVersion (= 1)
 *   bytes  CandyKey UTF-8 (length-prefixed)
 *   bytes  serialized CandyLocator (length-prefixed)
 * </pre>
 */
public final class MutationSerializer {

    public static final byte FORMAT_VERSION = 1;

    private MutationSerializer() {
    }

    public static byte[] serialize(Mutation mutation) {
        return new BinaryWriter(64)
                .writeByte(FORMAT_VERSION)
                .writeBytes(mutation.key().utf8Bytes())
                .writeBytes(CandyLocatorSerializer.serialize(mutation.locator()))
                .toByteArray();
    }

    public static Mutation deserialize(byte[] data) {
        return deserialize(new BinaryReader(data));
    }

    public static Mutation deserialize(BinaryReader r) {
        int version = r.readByte();
        if (version != FORMAT_VERSION) {
            throw new SerializationException("Unsupported Mutation format version: " + version);
        }
        CandyKey key = CandyKey.ofUtf8(r.readBytes());
        CandyLocator locator = CandyLocatorSerializer.deserialize(r.readBytes());
        return new Mutation(key, locator);
    }
}
