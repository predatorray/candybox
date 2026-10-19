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

import me.predatorray.candybox.common.CandyLocator;
import me.predatorray.candybox.common.exception.SerializationException;
import me.predatorray.candybox.common.serial.BinaryReader;
import me.predatorray.candybox.common.serial.BinaryWriter;
import me.predatorray.candybox.common.serial.CandyLocatorSerializer;

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
 */
record LocatorRelay(String requester, String srcKey, String renameToken, String dstKey,
                    long createdAtMillis, CandyLocator locator) {

    private static final byte FORMAT_VERSION = 1;

    boolean isRename() {
        return renameToken != null;
    }

    byte[] serialize() {
        byte[] locatorBytes = CandyLocatorSerializer.serialize(locator);
        BinaryWriter w = new BinaryWriter(locatorBytes.length + 64);
        w.writeByte(FORMAT_VERSION);
        w.writeString(requester);
        w.writeString(srcKey);
        writeNullable(w, renameToken);
        writeNullable(w, dstKey);
        w.writeVarLong(createdAtMillis);
        w.writeBytes(locatorBytes);
        return w.toByteArray();
    }

    static LocatorRelay deserialize(byte[] data) {
        BinaryReader r = new BinaryReader(data);
        int version = r.readByte();
        if (version != FORMAT_VERSION) {
            throw new SerializationException("Unsupported locator relay version " + version);
        }
        return new LocatorRelay(r.readString(), r.readString(), readNullable(r), readNullable(r),
                r.readVarLong(), CandyLocatorSerializer.deserialize(r.readBytes()));
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
