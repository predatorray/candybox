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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import me.predatorray.candybox.common.Part;
import me.predatorray.candybox.common.SegmentRef;
import me.predatorray.candybox.lsm.sstable.SSTableMeta;

/**
 * An immutable snapshot of a Box's LSM state: the SSTables that exist (with their levels and key
 * ranges), the set of live Syrups, the id of the current WAL ledger, and the in-flight multipart
 * uploads. Produced by replaying the manifest log and advanced by {@link #apply(ManifestEdit)}.
 *
 * <p>One {@code ManifestState} corresponds to one Box's single partition (v1); the structure is kept
 * partition-shaped so key-range sharding can be added later without reworking it.
 */
public final class ManifestState {

    private static final ManifestState EMPTY =
            new ManifestState(List.of(), Set.of(), -1L, Map.of());

    private final List<SSTableMeta> tables;
    private final Set<Long> liveSyrups;
    private final long walLedgerId;
    private final Map<String, MultipartUploadState> multipartUploads;

    private ManifestState(List<SSTableMeta> tables, Set<Long> liveSyrups, long walLedgerId,
                          Map<String, MultipartUploadState> multipartUploads) {
        this.tables = List.copyOf(tables);
        this.liveSyrups = Set.copyOf(liveSyrups);
        this.walLedgerId = walLedgerId;
        this.multipartUploads = Collections.unmodifiableMap(new LinkedHashMap<>(multipartUploads));
    }

    public static ManifestState empty() {
        return EMPTY;
    }

    /** All SSTables across all levels, in insertion order. */
    public List<SSTableMeta> tables() {
        return tables;
    }

    /** SSTables at a specific level. */
    public List<SSTableMeta> level(int level) {
        List<SSTableMeta> out = new ArrayList<>();
        for (SSTableMeta t : tables) {
            if (t.level() == level) {
                out.add(t);
            }
        }
        return out;
    }

    public List<SSTableMeta> level0() {
        return level(0);
    }

    public int maxLevel() {
        int max = 0;
        for (SSTableMeta t : tables) {
            max = Math.max(max, t.level());
        }
        return max;
    }

    public Set<Long> liveSyrups() {
        return liveSyrups;
    }

    /** Syrups currently referenced by some SSTable's locators (union over all tables). */
    public Set<Long> referencedSyrups() {
        Set<Long> referenced = new LinkedHashSet<>();
        for (SSTableMeta t : tables) {
            referenced.addAll(t.referencedSyrups());
        }
        return referenced;
    }

    /** Current WAL ledger id, or {@code -1} if none recorded yet. */
    public long walLedgerId() {
        return walLedgerId;
    }

    /** Snapshot of currently in-flight multipart uploads, keyed by {@code uploadId}. */
    public Map<String, MultipartUploadState> multipartUploads() {
        return multipartUploads;
    }

    /**
     * Syrups referenced by parts of in-flight multipart uploads. These must not be GC'd while the
     * upload is pending — even though no SSTable points at them yet.
     */
    public Set<Long> multipartReferencedSyrups() {
        Set<Long> referenced = new LinkedHashSet<>();
        for (MultipartUploadState upload : multipartUploads.values()) {
            for (Part part : upload.parts().values()) {
                for (SegmentRef seg : part.segments()) {
                    referenced.add(seg.syrupId());
                }
            }
        }
        return referenced;
    }

    /** Returns a new state with {@code edit} applied. */
    public ManifestState apply(ManifestEdit edit) {
        List<SSTableMeta> newTables = new ArrayList<>();
        for (SSTableMeta t : tables) {
            if (!edit.removedTableLedgerIds().contains(t.ledgerId())) {
                newTables.add(t);
            }
        }
        newTables.addAll(edit.addedTables());

        Set<Long> newSyrups = new LinkedHashSet<>(liveSyrups);
        newSyrups.addAll(edit.addedSyrups());
        newSyrups.removeAll(edit.removedSyrups());

        long newWal = edit.newWalLedgerId() == null ? walLedgerId : edit.newWalLedgerId();

        Map<String, MultipartUploadState> newUploads = new LinkedHashMap<>(multipartUploads);
        for (MultipartUploadState u : edit.addedUploads()) {
            newUploads.put(u.uploadId(), u);
        }
        for (ManifestEdit.PartUpsert pu : edit.upsertParts()) {
            MultipartUploadState existing = newUploads.get(pu.uploadId());
            if (existing == null) {
                throw new IllegalStateException("UploadPart for unknown uploadId " + pu.uploadId());
            }
            newUploads.put(pu.uploadId(), existing.withPart(pu.partNumber(), pu.part()));
        }
        for (String dropped : edit.removedUploads()) {
            newUploads.remove(dropped);
        }
        return new ManifestState(newTables, newSyrups, newWal, newUploads);
    }
}
