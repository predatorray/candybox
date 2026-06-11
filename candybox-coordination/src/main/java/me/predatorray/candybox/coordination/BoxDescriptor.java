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
package me.predatorray.candybox.coordination;

import me.predatorray.candybox.common.Partitioning;
import me.predatorray.candybox.common.serial.BinaryReader;
import me.predatorray.candybox.common.serial.BinaryWriter;

/**
 * A Box's immutable metadata record, stored at {@link CandyboxKeys#boxMetaKey} when the Box is
 * created. The partition count is fixed for the Box's lifetime — it is the routing source of truth
 * for servers and clients, so changing it would silently re-home every key.
 */
public record BoxDescriptor(int partitionCount) {

    private static final int FORMAT_VERSION = 1;

    public BoxDescriptor {
        if (partitionCount < 1) {
            throw new IllegalArgumentException("partitionCount must be positive: " + partitionCount);
        }
    }

    /** The partition the given key lives in, under this descriptor. */
    public int partitionOf(String key) {
        return Partitioning.partitionOf(key, partitionCount);
    }

    public byte[] encode() {
        return new BinaryWriter(8)
                .writeByte(FORMAT_VERSION)
                .writeVarInt(partitionCount)
                .toByteArray();
    }

    public static BoxDescriptor decode(byte[] data) {
        BinaryReader r = new BinaryReader(data);
        int version = r.readByte();
        if (version != FORMAT_VERSION) {
            throw new CoordinationException("Unsupported BoxDescriptor version: " + version);
        }
        return new BoxDescriptor(r.readVarInt());
    }
}
