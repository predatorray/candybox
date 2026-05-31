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
package me.predatorray.candybox.common.serial;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import me.predatorray.candybox.common.CandyLocator;
import me.predatorray.candybox.common.Hlc;
import me.predatorray.candybox.common.LocatorType;
import me.predatorray.candybox.common.Part;
import me.predatorray.candybox.common.SegmentRef;
import me.predatorray.candybox.common.config.SizeLimits;
import me.predatorray.candybox.common.exception.LimitExceededException;
import me.predatorray.candybox.common.exception.SerializationException;

/**
 * Versioned binary codec for {@link CandyLocator}. <b>v2 layout</b> (big-endian; varints where
 * noted):
 *
 * <pre>
 *   byte    formatVersion (= 2)
 *   byte    locator type code (PUT=1, DELETE=2)
 *   long    hlc.physicalMillis
 *   varint  hlc.logicalCounter
 *   int     hlc.nodeId
 *   varlong createdAtMillis
 *   byte    contentType present?      [+ string]
 *   varint  userMetadata entry count  [+ key/value strings]
 *   varint  part count                [+ Part records]
 *     Part record:
 *       varlong partLength
 *       varint  chunkSize
 *       int     crc32c
 *       varint  segment count         [+ {varlong syrupId, varlong firstEntryId, varlong lastEntryId}]
 * </pre>
 *
 * <p>A {@code DELETE} tombstone serializes with {@code part count = 0}. A v1 single-part Candy is the
 * same shape with {@code part count = 1}.
 *
 * <p>Every encode is checked against the configured {@code maxLocatorBytes} (default 256 KiB in v2 —
 * enough room for the ~200 KiB needed to hold an S3-cap 10,000-part multipart object).
 */
public final class CandyLocatorSerializer {

    public static final byte FORMAT_VERSION = 2;

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

        List<Part> parts = locator.parts();
        w.writeVarInt(parts.size());
        for (Part p : parts) {
            w.writeVarLong(p.partLength());
            w.writeVarInt(p.chunkSize());
            w.writeInt(p.crc32c());
            List<SegmentRef> segs = p.segments();
            w.writeVarInt(segs.size());
            for (SegmentRef s : segs) {
                w.writeVarLong(s.syrupId());
                w.writeVarLong(s.firstEntryId());
                w.writeVarLong(s.lastEntryId());
            }
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
        long createdAtMillis = r.readVarLong();

        String contentType = r.readBoolean() ? r.readString() : null;

        int mdCount = r.readVarInt();
        Map<String, String> md = new LinkedHashMap<>(Math.max(4, mdCount * 2));
        for (int i = 0; i < mdCount; i++) {
            String k = r.readString();
            String v = r.readString();
            md.put(k, v);
        }

        int partCount = r.readVarInt();
        List<Part> parts = new ArrayList<>(partCount);
        for (int p = 0; p < partCount; p++) {
            long partLength = r.readVarLong();
            int chunkSize = r.readVarInt();
            int crc32c = r.readInt();
            int segCount = r.readVarInt();
            List<SegmentRef> segments = new ArrayList<>(segCount);
            for (int i = 0; i < segCount; i++) {
                segments.add(new SegmentRef(r.readVarLong(), r.readVarLong(), r.readVarLong()));
            }
            parts.add(new Part(partLength, chunkSize, crc32c, segments));
        }

        return new CandyLocator(hlc, type, contentType, md, createdAtMillis, parts);
    }
}
