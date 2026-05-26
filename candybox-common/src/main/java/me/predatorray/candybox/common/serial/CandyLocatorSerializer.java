package me.predatorray.candybox.common.serial;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import me.predatorray.candybox.common.CandyLocator;
import me.predatorray.candybox.common.Hlc;
import me.predatorray.candybox.common.LocatorType;
import me.predatorray.candybox.common.SegmentRef;
import me.predatorray.candybox.common.config.SizeLimits;
import me.predatorray.candybox.common.exception.LimitExceededException;
import me.predatorray.candybox.common.exception.SerializationException;

/**
 * Versioned binary codec for {@link CandyLocator}. Layout (big-endian, varints where noted):
 *
 * <pre>
 *   byte    formatVersion (= 1)
 *   byte    locator type code
 *   long    hlc.physicalMillis
 *   varint  hlc.logicalCounter
 *   int     hlc.nodeId
 *   varlong contentLength
 *   varint  chunkSize
 *   int     crc32c
 *   varlong createdAtMillis
 *   byte    contentType present?  [+ string]
 *   varint  userMetadata entry count   [+ key/value strings]
 *   varint  segment count              [+ {varlong syrupId, varlong firstEntryId, varlong lastEntryId}]
 * </pre>
 *
 * <p>Every encode is checked against the configured {@code maxLocatorBytes} (default 64 KiB) so a
 * locator always fits comfortably in one SSTable data-block entry.
 */
public final class CandyLocatorSerializer {

    public static final byte FORMAT_VERSION = 1;

    private CandyLocatorSerializer() {
    }

    public static byte[] serialize(CandyLocator locator) {
        return serialize(locator, SizeLimits.DEFAULT_MAX_LOCATOR_BYTES);
    }

    public static byte[] serialize(CandyLocator locator, int maxLocatorBytes) {
        BinaryWriter w = new BinaryWriter(64);
        w.writeByte(FORMAT_VERSION);
        w.writeByte(locator.type().code());
        Hlc hlc = locator.hlc();
        w.writeLong(hlc.physicalMillis());
        w.writeVarInt(hlc.logicalCounter());
        w.writeInt(hlc.nodeId());
        w.writeVarLong(locator.contentLength());
        w.writeVarInt(locator.chunkSize());
        w.writeInt(locator.crc32c());
        w.writeVarLong(Math.max(0, locator.createdAtMillis()));

        if (locator.contentType() == null) {
            w.writeBoolean(false);
        } else {
            w.writeBoolean(true);
            w.writeString(locator.contentType());
        }

        Map<String, String> md = locator.userMetadata();
        w.writeVarInt(md.size());
        for (Map.Entry<String, String> e : md.entrySet()) {
            w.writeString(e.getKey());
            w.writeString(e.getValue());
        }

        List<SegmentRef> segments = locator.segments();
        w.writeVarInt(segments.size());
        for (SegmentRef s : segments) {
            w.writeVarLong(s.syrupId());
            w.writeVarLong(s.firstEntryId());
            w.writeVarLong(s.lastEntryId());
        }

        byte[] out = w.toByteArray();
        if (out.length > maxLocatorBytes) {
            throw new LimitExceededException("Serialized CandyLocator is " + out.length
                    + " bytes, exceeds cap of " + maxLocatorBytes);
        }
        return out;
    }

    public static CandyLocator deserialize(byte[] data) {
        return deserialize(new BinaryReader(data));
    }

    public static CandyLocator deserialize(BinaryReader r) {
        int version = r.readByte();
        if (version != FORMAT_VERSION) {
            throw new SerializationException("Unsupported CandyLocator format version: " + version);
        }
        LocatorType type = LocatorType.fromCode(r.readByte());
        Hlc hlc = new Hlc(r.readLong(), r.readVarInt(), r.readInt());
        long contentLength = r.readVarLong();
        int chunkSize = r.readVarInt();
        int crc32c = r.readInt();
        long createdAtMillis = r.readVarLong();

        String contentType = r.readBoolean() ? r.readString() : null;

        int mdCount = r.readVarInt();
        Map<String, String> md = new LinkedHashMap<>(Math.max(4, mdCount * 2));
        for (int i = 0; i < mdCount; i++) {
            String k = r.readString();
            String v = r.readString();
            md.put(k, v);
        }

        int segCount = r.readVarInt();
        List<SegmentRef> segments = new ArrayList<>(segCount);
        for (int i = 0; i < segCount; i++) {
            segments.add(new SegmentRef(r.readVarLong(), r.readVarLong(), r.readVarLong()));
        }

        return new CandyLocator(hlc, type, contentLength, chunkSize, contentType, md, crc32c,
                createdAtMillis, segments);
    }
}
