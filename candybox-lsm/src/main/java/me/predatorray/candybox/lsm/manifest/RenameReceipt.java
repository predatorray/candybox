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
package me.predatorray.candybox.lsm.manifest;

import me.predatorray.candybox.common.CandyLocator;
import me.predatorray.candybox.common.Hlc;
import me.predatorray.candybox.common.serial.BinaryReader;
import me.predatorray.candybox.common.serial.BinaryWriter;
import me.predatorray.candybox.common.serial.CandyLocatorSerializer;
import me.predatorray.candybox.common.exception.SerializationException;

/** Durable destination decision, committed in the same WAL entry as its PUT. */
public record RenameReceipt(String token, String incarnation, String relayToken, int srcPartition,
                            String srcKey, Hlc srcHlc, String dstKey, CandyLocator result) {
    /** Relay attempts may change; the durable logical transfer identity must not. */
    public boolean sameTransfer(RenameReceipt other) {
        return token.equals(other.token) && incarnation.equals(other.incarnation)
                && srcPartition == other.srcPartition && srcKey.equals(other.srcKey)
                && srcHlc.equals(other.srcHlc) && dstKey.equals(other.dstKey);
    }

    public byte[] serialize() {
        BinaryWriter w = new BinaryWriter(128);
        w.writeByte(1);
        w.writeString(token); w.writeString(incarnation); w.writeString(relayToken);
        w.writeVarInt(srcPartition); w.writeString(srcKey);
        w.writeVarLong(srcHlc.physicalMillis()); w.writeVarInt(srcHlc.logicalCounter());
        w.writeInt(srcHlc.nodeId()); w.writeString(dstKey);
        w.writeBytes(CandyLocatorSerializer.serialize(result));
        return w.toByteArray();
    }

    public static RenameReceipt deserialize(byte[] bytes) {
        BinaryReader r = new BinaryReader(bytes);
        if (r.readByte() != 1) { throw new SerializationException("Unsupported rename receipt"); }
        var receipt = new RenameReceipt(r.readString(), r.readString(), r.readString(),
                r.readVarInt(), r.readString(), new Hlc(r.readVarLong(), r.readVarInt(), r.readInt()),
                r.readString(), CandyLocatorSerializer.deserialize(r.readBytes()));
        if (r.hasRemaining()) { throw new SerializationException("Trailing rename receipt bytes"); }
        return receipt;
    }
}
