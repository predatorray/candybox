package me.predatorray.candybox.lsm.wal;

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
 *   byte  kind (1 = point mutation, 2 = range delete)
 *   bytes payload (MutationSerializer or RangeTombstoneSerializer)
 * </pre>
 */
public final class WalEntrySerializer {

    private static final int KIND_MUTATION = 1;
    private static final int KIND_RANGE_DELETE = 2;

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
            default -> throw new SerializationException("Unknown WAL entry kind: " + kind);
        };
    }
}
