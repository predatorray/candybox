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
package me.predatorray.candybox.lsm.wal;

import java.util.ArrayList;
import java.util.List;
import me.predatorray.candybox.common.Mutation;
import me.predatorray.candybox.common.exception.LimitExceededException;
import me.predatorray.candybox.common.exception.SerializationException;
import me.predatorray.candybox.common.serial.BinaryReader;
import me.predatorray.candybox.common.serial.BinaryWriter;
import me.predatorray.candybox.common.serial.MutationSerializer;
import me.predatorray.candybox.common.serial.RangeTombstoneSerializer;

/**
 * Binary codec for a {@link WalEntry}. A leading <em>kind</em> byte tags the record so a mixed log of
 * point mutations and range tombstones replays unambiguously, followed by the kind's own versioned
 * payload (so each record type keeps its independent format version).
 *
 * <pre>
 *   byte  kind (1 = point mutation, 2 = range delete, 3 = atomic batch)
 *   bytes payload (MutationSerializer or RangeTombstoneSerializer)
 *   batch payload: byte version (=1), varint count, repeated bytes(MutationSerializer payload)
 * </pre>
 */
public final class WalEntrySerializer {

    private static final int KIND_MUTATION = 1;
    private static final int KIND_RANGE_DELETE = 2;
    private static final int KIND_BATCH = 3;
    private static final int BATCH_VERSION = 1;
    /** Includes framing; comfortably below the standard BookKeeper 5 MiB frame limit. */
    public static final int MAX_BATCH_BYTES = 1 << 20;

    private WalEntrySerializer() {
    }

    public static byte[] serialize(WalEntry entry) {
        BinaryWriter w = new BinaryWriter(64);
        if (entry instanceof WalEntry.PointMutation pm) {
            w.writeByte(KIND_MUTATION);
            w.writeRaw(MutationSerializer.serialize(pm.mutation()));
        } else if (entry instanceof WalEntry.RangeDelete rd) {
            w.writeByte(KIND_RANGE_DELETE);
            w.writeRaw(RangeTombstoneSerializer.serialize(rd.tombstone()));
        } else if (entry instanceof WalEntry.Batch batch) {
            w.writeByte(KIND_BATCH).writeByte(BATCH_VERSION).writeVarInt(batch.mutations().size());
            for (Mutation mutation : batch.mutations()) {
                byte[] encoded = MutationSerializer.serialize(mutation);
                int prefixBytes = 1;
                for (int remaining = encoded.length; remaining >= 128; remaining >>>= 7) {
                    prefixBytes++;
                }
                if ((long) w.size() + prefixBytes + encoded.length > MAX_BATCH_BYTES) {
                    throw new LimitExceededException("WAL batch exceeds " + MAX_BATCH_BYTES + " bytes");
                }
                w.writeBytes(encoded);
            }
        } else {
            throw new SerializationException("Unknown WAL entry kind: " + entry.getClass());
        }
        return w.toByteArray();
    }

    public static WalEntry deserialize(byte[] data) {
        BinaryReader r = new BinaryReader(data);
        int kind = r.readByte();
        return switch (kind) {
            case KIND_MUTATION -> WalEntry.of(MutationSerializer.deserialize(r));
            case KIND_RANGE_DELETE -> WalEntry.of(RangeTombstoneSerializer.deserialize(r));
            case KIND_BATCH -> deserializeBatch(r, data.length);
            default -> throw new SerializationException("Unknown WAL entry kind: " + kind);
        };
    }

    private static WalEntry deserializeBatch(BinaryReader r, int length) {
        if (length > MAX_BATCH_BYTES) {
            throw new SerializationException("Oversized WAL batch: " + length);
        }
        int version = r.readByte();
        if (version != BATCH_VERSION) {
            throw new SerializationException("Unsupported WAL batch version: " + version);
        }
        int count = r.readVarInt();
        if (count < 1 || count > WalEntry.Batch.MAX_MUTATIONS) {
            throw new SerializationException("Invalid WAL batch mutation count: " + count);
        }
        List<Mutation> mutations = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            BinaryReader member = new BinaryReader(r.readBytes());
            mutations.add(MutationSerializer.deserialize(member));
            if (member.hasRemaining()) {
                throw new SerializationException("Trailing bytes in WAL batch mutation");
            }
        }
        if (r.hasRemaining()) {
            throw new SerializationException("Trailing bytes in WAL batch");
        }
        // No member is exposed to replay until the entire batch has been validated.
        return WalEntry.batch(mutations);
    }
}
