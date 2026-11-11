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

import me.predatorray.candybox.common.Hlc;
import me.predatorray.candybox.common.serial.BinaryReader;
import me.predatorray.candybox.common.serial.BinaryWriter;
import me.predatorray.candybox.common.exception.SerializationException;

/** Commit publication followed by the source's durable-delete acknowledgement. */
record RenameMarker(String incarnation, String srcKey, int srcPartition, Hlc srcHlc,
                    String dstKey, boolean completed, boolean committed, boolean reserved) {
    RenameMarker(String incarnation, String srcKey, int srcPartition, Hlc srcHlc,
                 String dstKey, boolean completed, boolean committed) {
        this(incarnation, srcKey, srcPartition, srcHlc, dstKey, completed, committed, committed);
    }
    RenameMarker reserve() {
        return new RenameMarker(incarnation, srcKey, srcPartition, srcHlc, dstKey, completed, committed, true);
    }
    RenameMarker(String incarnation, String srcKey, int srcPartition, Hlc srcHlc,
                 String dstKey, boolean completed) {
        this(incarnation, srcKey, srcPartition, srcHlc, dstKey, completed, true);
    }
    RenameMarker commit() {
        return new RenameMarker(incarnation, srcKey, srcPartition, srcHlc, dstKey, completed, true);
    }
    byte[] serialize() {
        BinaryWriter w = new BinaryWriter(128);
        w.writeByte(0); w.writeByte(3); w.writeString(incarnation); w.writeString(srcKey);
        w.writeVarInt(srcPartition); w.writeVarLong(srcHlc.physicalMillis());
        w.writeVarInt(srcHlc.logicalCounter()); w.writeInt(srcHlc.nodeId());
        w.writeString(dstKey); w.writeBoolean(completed); w.writeBoolean(committed); w.writeBoolean(reserved);
        return w.toByteArray();
    }
    static RenameMarker deserialize(byte[] bytes) {
        BinaryReader r = new BinaryReader(bytes);
        if (r.readByte() != 0) {
            // Original unversioned markers began with a nonempty source-key string.
            r = new BinaryReader(bytes);
            String source = r.readString();
            int partition = r.readVarInt();
            Hlc hlc = new Hlc(r.readVarLong(), r.readVarInt(), r.readInt());
            String destination = r.readString();
            if (r.hasRemaining()) { throw new SerializationException("Trailing legacy rename marker bytes"); }
            return new RenameMarker("", source, partition, hlc, destination, false);
        }
        int version = r.readByte();
        if (version != 1 && version != 2 && version != 3) { throw new SerializationException("Unsupported rename marker"); }
        String incarnation = r.readString(); String source = r.readString(); int partition = r.readVarInt();
        Hlc hlc = new Hlc(r.readVarLong(), r.readVarInt(), r.readInt());
        String destination = r.readString(); boolean completed = r.readBoolean();
        boolean committed = version == 1 || r.readBoolean();
        var marker = new RenameMarker(incarnation, source, partition, hlc, destination, completed,
                committed, version < 3 ? committed : r.readBoolean());
        if (r.hasRemaining()) { throw new SerializationException("Trailing rename marker bytes"); }
        return marker;
    }
    RenameMarker acknowledge() {
        return new RenameMarker(incarnation, srcKey, srcPartition, srcHlc, dstKey, true);
    }
}
