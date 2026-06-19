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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import me.predatorray.candybox.common.CandyKey;
import me.predatorray.candybox.common.Hlc;
import me.predatorray.candybox.common.Part;
import me.predatorray.candybox.common.SegmentRef;
import me.predatorray.candybox.common.exception.SerializationException;
import me.predatorray.candybox.common.serial.BinaryReader;
import me.predatorray.candybox.common.serial.BinaryWriter;
import me.predatorray.candybox.lsm.sstable.SSTableMeta;

/**
 * Versioned binary codec for {@link ManifestEdit} records (one per manifest ledger entry) and the
 * {@link SSTableMeta} they contain.
 *
 * <p><b>v2 layout</b> adds the multipart-upload tracking fields ({@code addedUploads},
 * {@code upsertParts}, {@code removedUploads}) at the end of the v1 record. <b>v3 layout</b> appends
 * the cross-partition rename-intent fields ({@code addedRenameIntents}, {@code removedRenameIntents}).
 * A v2 record (no rename-intent fields) still reads back as an edit with empty intent sets. Older v1
 * records cannot be read back; this is acceptable because the project has no production data to
 * migrate.
 */
public final class ManifestSerializer {

    public static final byte FORMAT_VERSION = 3;

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
        w.writeVarLong(edit.ownerFencingToken());

        // ---- multipart (v2) ------------------------------------------------------------------
        w.writeVarInt(edit.addedUploads().size());
        for (MultipartUploadState u : edit.addedUploads()) {
            writeUpload(w, u);
        }
        w.writeVarInt(edit.upsertParts().size());
        for (ManifestEdit.PartUpsert pu : edit.upsertParts()) {
            w.writeString(pu.uploadId());
            w.writeVarInt(pu.partNumber());
            writePart(w, pu.part());
        }
        w.writeVarInt(edit.removedUploads().size());
        for (String id : edit.removedUploads()) {
            w.writeString(id);
        }

        // ---- cross-partition rename intents (v3) ---------------------------------------------
        w.writeVarInt(edit.addedRenameIntents().size());
        for (RenameIntent intent : edit.addedRenameIntents()) {
            writeRenameIntent(w, intent);
        }
        w.writeVarInt(edit.removedRenameIntents().size());
        for (String token : edit.removedRenameIntents()) {
            w.writeString(token);
        }
        return w.toByteArray();
    }

    public static ManifestEdit deserialize(byte[] data) {
        BinaryReader r = new BinaryReader(data);
        int version = r.readByte();
        if (version != 2 && version != FORMAT_VERSION) {
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
        long ownerFencingToken = r.readVarLong();

        int uploadCount = r.readVarInt();
        List<MultipartUploadState> addedUploads = new ArrayList<>(uploadCount);
        for (int i = 0; i < uploadCount; i++) {
            addedUploads.add(readUpload(r));
        }
        int upsertCount = r.readVarInt();
        List<ManifestEdit.PartUpsert> upserts = new ArrayList<>(upsertCount);
        for (int i = 0; i < upsertCount; i++) {
            String uploadId = r.readString();
            int partNumber = r.readVarInt();
            Part part = readPart(r);
            upserts.add(new ManifestEdit.PartUpsert(uploadId, partNumber, part));
        }
        int removedUploadCount = r.readVarInt();
        Set<String> removedUploads = new LinkedHashSet<>(Math.max(4, removedUploadCount * 2));
        for (int i = 0; i < removedUploadCount; i++) {
            removedUploads.add(r.readString());
        }

        List<RenameIntent> addedIntents = new ArrayList<>();
        Set<String> removedIntents = new LinkedHashSet<>();
        if (version >= 3) {
            int addedIntentCount = r.readVarInt();
            for (int i = 0; i < addedIntentCount; i++) {
                addedIntents.add(readRenameIntent(r));
            }
            int removedIntentCount = r.readVarInt();
            for (int i = 0; i < removedIntentCount; i++) {
                removedIntents.add(r.readString());
            }
        }

        return new ManifestEdit(tables, removedTables, addedSyrups, removedSyrups, newWal,
                addedUploads, upserts, removedUploads, addedIntents, removedIntents,
                ownerFencingToken);
    }

    private static void writeRenameIntent(BinaryWriter w, RenameIntent intent) {
        w.writeString(intent.token());
        w.writeString(intent.srcKey());
        w.writeVarLong(intent.srcHlc().physicalMillis());
        w.writeVarInt(intent.srcHlc().logicalCounter());
        w.writeInt(intent.srcHlc().nodeId());
        w.writeString(intent.dstKey());
        w.writeVarInt(intent.dstPartition());
        w.writeVarLong(Math.max(0, intent.createdAtMillis()));
    }

    private static RenameIntent readRenameIntent(BinaryReader r) {
        String token = r.readString();
        String srcKey = r.readString();
        Hlc srcHlc = new Hlc(r.readVarLong(), r.readVarInt(), r.readInt());
        String dstKey = r.readString();
        int dstPartition = r.readVarInt();
        long createdAtMillis = r.readVarLong();
        return new RenameIntent(token, srcKey, srcHlc, dstKey, dstPartition, createdAtMillis);
    }

    private static void writeTable(BinaryWriter w, SSTableMeta t) {
        w.writeVarLong(t.ledgerId());
        w.writeVarInt(t.level());
        w.writeBytes(t.minKey().utf8Bytes());
        w.writeBytes(t.maxKey().utf8Bytes());
        w.writeVarLong(t.entryCount());
        w.writeVarLong(t.sizeBytes());
        writeLongSet(w, t.referencedSyrups());
    }

    private static SSTableMeta readTable(BinaryReader r) {
        long ledgerId = r.readVarLong();
        int level = r.readVarInt();
        CandyKey minKey = CandyKey.ofUtf8(r.readBytes());
        CandyKey maxKey = CandyKey.ofUtf8(r.readBytes());
        long entryCount = r.readVarLong();
        long sizeBytes = r.readVarLong();
        Set<Long> referencedSyrups = readLongSet(r);
        return new SSTableMeta(ledgerId, level, minKey, maxKey, entryCount, sizeBytes, referencedSyrups);
    }

    private static void writeUpload(BinaryWriter w, MultipartUploadState u) {
        w.writeString(u.uploadId());
        w.writeString(u.key());
        if (u.contentType() == null) {
            w.writeBoolean(false);
        } else {
            w.writeBoolean(true);
            w.writeString(u.contentType());
        }
        Map<String, String> md = u.userMetadata();
        w.writeVarInt(md.size());
        for (Map.Entry<String, String> e : md.entrySet()) {
            w.writeString(e.getKey());
            w.writeString(e.getValue());
        }
        w.writeVarLong(Math.max(0, u.createdAtMillis()));
        // The recorded parts (so the upload survives handover with its already-uploaded parts intact).
        Map<Integer, Part> parts = u.parts();
        w.writeVarInt(parts.size());
        for (Map.Entry<Integer, Part> e : parts.entrySet()) {
            w.writeVarInt(e.getKey());
            writePart(w, e.getValue());
        }
    }

    private static MultipartUploadState readUpload(BinaryReader r) {
        String uploadId = r.readString();
        String key = r.readString();
        String contentType = r.readBoolean() ? r.readString() : null;
        int mdCount = r.readVarInt();
        Map<String, String> md = new LinkedHashMap<>(Math.max(4, mdCount * 2));
        for (int i = 0; i < mdCount; i++) {
            String k = r.readString();
            String v = r.readString();
            md.put(k, v);
        }
        long createdAtMillis = r.readVarLong();
        int partCount = r.readVarInt();
        Map<Integer, Part> parts = new LinkedHashMap<>(Math.max(4, partCount * 2));
        for (int i = 0; i < partCount; i++) {
            int partNumber = r.readVarInt();
            Part part = readPart(r);
            parts.put(partNumber, part);
        }
        return new MultipartUploadState(uploadId, key, contentType, md, createdAtMillis, parts);
    }

    /** Encodes one {@link Part} in the same shape used inside the {@link CandyLocator} v2 record. */
    static void writePart(BinaryWriter w, Part p) {
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

    static Part readPart(BinaryReader r) {
        long partLength = r.readVarLong();
        int chunkSize = r.readVarInt();
        int crc32c = r.readInt();
        int segCount = r.readVarInt();
        List<SegmentRef> segments = new ArrayList<>(segCount);
        for (int i = 0; i < segCount; i++) {
            segments.add(new SegmentRef(r.readVarLong(), r.readVarLong(), r.readVarLong()));
        }
        return new Part(partLength, chunkSize, crc32c, segments);
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
