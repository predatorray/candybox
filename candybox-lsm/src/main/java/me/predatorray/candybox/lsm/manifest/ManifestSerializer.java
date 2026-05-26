package me.predatorray.candybox.lsm.manifest;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import me.predatorray.candybox.common.CandyKey;
import me.predatorray.candybox.common.exception.SerializationException;
import me.predatorray.candybox.common.serial.BinaryReader;
import me.predatorray.candybox.common.serial.BinaryWriter;
import me.predatorray.candybox.lsm.sstable.SSTableMeta;

/**
 * Versioned binary codec for {@link ManifestEdit} records (one per manifest ledger entry) and the
 * {@link SSTableMeta} they contain.
 */
public final class ManifestSerializer {

    public static final byte FORMAT_VERSION = 1;

    private ManifestSerializer() {
    }

    public static byte[] serialize(ManifestEdit edit) {
        BinaryWriter w = new BinaryWriter(128);
        w.writeByte(FORMAT_VERSION);

        w.writeVarInt(edit.addedTables().size());
        for (SSTableMeta t : edit.addedTables()) {
            writeTable(w, t);
        }
        writeLongSet(w, edit.removedTableLedgerIds());
        writeLongSet(w, edit.addedSyrups());
        writeLongSet(w, edit.removedSyrups());

        if (edit.newWalLedgerId() == null) {
            w.writeBoolean(false);
        } else {
            w.writeBoolean(true);
            w.writeVarLong(edit.newWalLedgerId());
        }
        return w.toByteArray();
    }

    public static ManifestEdit deserialize(byte[] data) {
        BinaryReader r = new BinaryReader(data);
        int version = r.readByte();
        if (version != FORMAT_VERSION) {
            throw new SerializationException("Unsupported ManifestEdit version: " + version);
        }
        int tableCount = r.readVarInt();
        List<SSTableMeta> tables = new ArrayList<>(tableCount);
        for (int i = 0; i < tableCount; i++) {
            tables.add(readTable(r));
        }
        Set<Long> removedTables = readLongSet(r);
        Set<Long> addedSyrups = readLongSet(r);
        Set<Long> removedSyrups = readLongSet(r);
        Long newWal = r.readBoolean() ? r.readVarLong() : null;
        return new ManifestEdit(tables, removedTables, addedSyrups, removedSyrups, newWal);
    }

    private static void writeTable(BinaryWriter w, SSTableMeta t) {
        w.writeVarLong(t.ledgerId());
        w.writeVarInt(t.level());
        w.writeBytes(t.minKey().utf8Bytes());
        w.writeBytes(t.maxKey().utf8Bytes());
        w.writeVarLong(t.entryCount());
    }

    private static SSTableMeta readTable(BinaryReader r) {
        long ledgerId = r.readVarLong();
        int level = r.readVarInt();
        CandyKey minKey = CandyKey.ofUtf8(r.readBytes());
        CandyKey maxKey = CandyKey.ofUtf8(r.readBytes());
        long entryCount = r.readVarLong();
        return new SSTableMeta(ledgerId, level, minKey, maxKey, entryCount);
    }

    private static void writeLongSet(BinaryWriter w, Set<Long> set) {
        w.writeVarInt(set.size());
        for (long v : set) {
            w.writeVarLong(v);
        }
    }

    private static Set<Long> readLongSet(BinaryReader r) {
        int count = r.readVarInt();
        Set<Long> set = new LinkedHashSet<>(Math.max(4, count * 2));
        for (int i = 0; i < count; i++) {
            set.add(r.readVarLong());
        }
        return set;
    }
}
