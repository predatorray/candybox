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

import java.util.LinkedHashMap;
import java.util.Map;
import me.predatorray.candybox.common.serial.BinaryReader;
import me.predatorray.candybox.common.serial.BinaryWriter;

/**
 * The desired (box, partition) → node assignment table the elected balancer publishes at
 * {@code cluster/assignment}. It is advisory: nodes acquire/release ownership to converge on it, but
 * correctness always rests on the per-partition fenced lease, never on this table.
 */
final class PartitionAssignment {

    private static final int FORMAT_VERSION = 1;

    /** One partition of one Box. */
    record BoxPartition(String box, int partition) implements Comparable<BoxPartition> {
        @Override
        public int compareTo(BoxPartition o) {
            int c = box.compareTo(o.box);
            return c != 0 ? c : Integer.compare(partition, o.partition);
        }
    }

    private final Map<BoxPartition, Integer> targets;

    PartitionAssignment(Map<BoxPartition, Integer> targets) {
        this.targets = new LinkedHashMap<>(targets);
    }

    static PartitionAssignment empty() {
        return new PartitionAssignment(Map.of());
    }

    Map<BoxPartition, Integer> targets() {
        return java.util.Collections.unmodifiableMap(targets);
    }

    /** The node assigned to a partition, or {@code null} if the table does not mention it. */
    Integer targetNode(String box, int partition) {
        return targets.get(new BoxPartition(box, partition));
    }

    byte[] encode() {
        BinaryWriter w = new BinaryWriter(64);
        w.writeByte(FORMAT_VERSION);
        w.writeVarInt(targets.size());
        for (Map.Entry<BoxPartition, Integer> e : targets.entrySet()) {
            w.writeString(e.getKey().box());
            w.writeVarInt(e.getKey().partition());
            w.writeInt(e.getValue());
        }
        return w.toByteArray();
    }

    static PartitionAssignment decode(byte[] data) {
        BinaryReader r = new BinaryReader(data);
        int version = r.readByte();
        if (version != FORMAT_VERSION) {
            throw new IllegalArgumentException("Unsupported PartitionAssignment version: " + version);
        }
        int count = r.readVarInt();
        Map<BoxPartition, Integer> targets = new LinkedHashMap<>(Math.max(4, count * 2));
        for (int i = 0; i < count; i++) {
            targets.put(new BoxPartition(r.readString(), r.readVarInt()), r.readInt());
        }
        return new PartitionAssignment(targets);
    }
}
