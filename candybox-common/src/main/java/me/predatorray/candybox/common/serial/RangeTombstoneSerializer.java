package me.predatorray.candybox.common.serial;

import me.predatorray.candybox.common.CandyKey;
import me.predatorray.candybox.common.Hlc;
import me.predatorray.candybox.common.RangeTombstone;
import me.predatorray.candybox.common.exception.SerializationException;

/**
 * Versioned binary codec for a {@link RangeTombstone}. Used for WAL range-delete entries and the
 * SSTable range-tombstone block. Layout (big-endian, varints where noted):
 *
 * <pre>
 *   byte    formatVersion (= 1)
 *   byte    start present?  [+ bytes(startInclusive UTF-8)]
 *   byte    end present?    [+ bytes(endExclusive UTF-8)]
 *   long    hlc.physicalMillis
 *   varint  hlc.logicalCounter
 *   int     hlc.nodeId
 * </pre>
 */
public final class RangeTombstoneSerializer {

    public static final byte FORMAT_VERSION = 1;

    private RangeTombstoneSerializer() {
    }

    public static byte[] serialize(RangeTombstone rt) {
        BinaryWriter w = new BinaryWriter(48);
        w.writeByte(FORMAT_VERSION);
        writeNullableKey(w, rt.startInclusive());
        writeNullableKey(w, rt.endExclusive());
        Hlc hlc = rt.hlc();
        w.writeLong(hlc.physicalMillis());
        w.writeVarInt(hlc.logicalCounter());
        w.writeInt(hlc.nodeId());
        return w.toByteArray();
    }

    public static RangeTombstone deserialize(byte[] data) {
        return deserialize(new BinaryReader(data));
    }

    public static RangeTombstone deserialize(BinaryReader r) {
        int version = r.readByte();
        if (version != FORMAT_VERSION) {
            throw new SerializationException("Unsupported RangeTombstone format version: " + version);
        }
        CandyKey start = readNullableKey(r);
        CandyKey end = readNullableKey(r);
        Hlc hlc = new Hlc(r.readLong(), r.readVarInt(), r.readInt());
        return new RangeTombstone(start, end, hlc);
    }

    private static void writeNullableKey(BinaryWriter w, CandyKey key) {
        if (key == null) {
            w.writeBoolean(false);
        } else {
            w.writeBoolean(true);
            w.writeBytes(key.utf8Bytes());
        }
    }

    private static CandyKey readNullableKey(BinaryReader r) {
        return r.readBoolean() ? CandyKey.ofUtf8(r.readBytes()) : null;
    }
}
