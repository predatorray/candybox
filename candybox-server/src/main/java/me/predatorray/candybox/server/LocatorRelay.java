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
package me.predatorray.candybox.server;

import java.util.HashMap;
import java.util.Map;
import me.predatorray.candybox.common.CandyLocator;
import me.predatorray.candybox.common.Hlc;
import me.predatorray.candybox.common.exception.SerializationException;
import me.predatorray.candybox.common.serial.BinaryReader;
import me.predatorray.candybox.common.serial.BinaryWriter;
import me.predatorray.candybox.common.serial.CandyLocatorSerializer;
import me.predatorray.candybox.lsm.engine.CandyMetadata;

/**
 * A source locator snapshot the source partition's owner stashes in coordination for the destination
 * owner of a cross-partition zero-copy put. Only the destination may consume it, and only on behalf
 * of the {@code requester} that created it.
 *
 * @param requester       the principal that resolved the source ({@code Type:name})
 * @param srcKey          the source key
 * @param renameToken     the rename's token when created by a prepare-rename, else null (a copy)
 * @param dstKey          the rename's destination key, else null
 * @param createdAtMillis when the relay was written, used to reap abandoned relays
 * @param locator         the resolved source locator
 * @param consumer        durable single-consumer claim; null while pending
 */
record LocatorRelay(String requester, String srcKey, String renameToken, String dstKey,
                    long createdAtMillis, CandyLocator locator, Consumer consumer) {

    private static final byte FORMAT_VERSION = 2;

    LocatorRelay(String requester, String srcKey, String renameToken, String dstKey,
                 long createdAtMillis, CandyLocator locator) {
        this(requester, srcKey, renameToken, dstKey, createdAtMillis, locator, null);
    }

    record Consumer(int partition, long fencingToken, String key, String incarnation,
                    CandyMetadata committed) { }

    LocatorRelay claim(int partition, long fencingToken, String key, String incarnation) {
        return new LocatorRelay(requester, srcKey, renameToken, dstKey, createdAtMillis, locator,
                new Consumer(partition, fencingToken, key, incarnation, null));
    }

    LocatorRelay committed(CandyMetadata metadata) {
        return new LocatorRelay(requester, srcKey, renameToken, dstKey, createdAtMillis, locator,
                new Consumer(consumer.partition(), consumer.fencingToken(), consumer.key(),
                        consumer.incarnation(), metadata));
    }

    boolean isRename() {
        return renameToken != null;
    }

    byte[] serialize(int maxLocatorBytes) {
        byte[] locatorBytes = CandyLocatorSerializer.serialize(locator, maxLocatorBytes);
        BinaryWriter w = new BinaryWriter(locatorBytes.length + 64);
        w.writeByte(FORMAT_VERSION);
        w.writeString(requester);
        w.writeString(srcKey);
        writeNullable(w, renameToken);
        writeNullable(w, dstKey);
        w.writeVarLong(createdAtMillis);
        w.writeBytes(locatorBytes);
        w.writeBoolean(consumer != null);
        if (consumer != null) {
            w.writeVarInt(consumer.partition());
            w.writeVarLong(consumer.fencingToken());
            w.writeString(consumer.key());
            w.writeString(consumer.incarnation());
            w.writeBoolean(consumer.committed() != null);
            if (consumer.committed() != null) { writeMetadata(w, consumer.committed()); }
        }
        return w.toByteArray();
    }

    static LocatorRelay deserialize(byte[] data) {
        BinaryReader r = new BinaryReader(data);
        int version = r.readByte();
        if (version != 1 && version != FORMAT_VERSION) {
            throw new SerializationException("Unsupported locator relay version " + version);
        }
        LocatorRelay relay = new LocatorRelay(r.readString(), r.readString(), readNullable(r),
                readNullable(r), r.readVarLong(), CandyLocatorSerializer.deserialize(r.readBytes()));
        if (version != 1 && r.readBoolean()) {
            relay = relay.claim(r.readVarInt(), r.readVarLong(), r.readString(), r.readString());
            if (r.readBoolean()) { relay = relay.committed(readMetadata(r)); }
        }
        return relay;
    }

    private static void writeMetadata(BinaryWriter w, CandyMetadata metadata) {
        w.writeLong(metadata.contentLength());
        writeNullable(w, metadata.contentType());
        w.writeVarInt(metadata.userMetadata().size());
        metadata.userMetadata().forEach((key, value) -> { w.writeString(key); w.writeString(value); });
        w.writeInt(metadata.crc32c());
        w.writeLong(metadata.hlc().physicalMillis());
        w.writeVarInt(metadata.hlc().logicalCounter());
        w.writeInt(metadata.hlc().nodeId());
        w.writeLong(metadata.createdAtMillis());
    }

    private static CandyMetadata readMetadata(BinaryReader r) {
        long length = r.readLong();
        String type = readNullable(r);
        int count = r.readVarInt();
        if (count < 0 || count > r.remaining() / 2) {
            throw new SerializationException("Invalid committed relay metadata count");
        }
        Map<String, String> metadata = new HashMap<>();
        for (int i = 0; i < count; i++) { metadata.put(r.readString(), r.readString()); }
        return new CandyMetadata(length, type, Map.copyOf(metadata), r.readInt(),
                new Hlc(r.readLong(), r.readVarInt(), r.readInt()), r.readLong());
    }

    private static void writeNullable(BinaryWriter w, String s) {
        w.writeBoolean(s != null);
        if (s != null) {
            w.writeString(s);
        }
    }

    private static String readNullable(BinaryReader r) {
        return r.readBoolean() ? r.readString() : null;
    }
}
