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

import java.util.List;
import java.util.Set;
import me.predatorray.candybox.common.Part;
import me.predatorray.candybox.lsm.sstable.SSTableMeta;

/**
 * One append-only change to the LSM state: a flush adds an SSTable (and the Syrups it referenced) and
 * may rotate the WAL; a compaction adds the output table and removes the inputs; GC removes obsoleted
 * tables/Syrups. Edits are serialized and appended to the manifest ledger by the Box's single owner.
 *
 * <p>Multipart uploads (v2) also flow through this edit: {@link #addedUploads} introduces a freshly
 * created upload, {@link #upsertParts} adds or replaces a part within an upload (last-write-wins per
 * {@code partNumber}), and {@link #removedUploads} drops an upload entirely (Complete or Abort).
 *
 * <p>Each edit carries the **owner fencing token** of the actor that authored it. The token is
 * normally left {@code 0} by callers and stamped authoritatively by {@link Manifest#apply} with the
 * owning node's token; {@code Manifest} rejects an edit whose token regresses below the highest token
 * it has committed (the manifest-level zombie-commit defense, complementing BookKeeper recover-open).
 *
 * @param addedTables           SSTables to add
 * @param removedTableLedgerIds SSTable ledger ids to remove
 * @param addedSyrups           Syrup ledger ids that became live
 * @param removedSyrups         Syrup ledger ids that became dead
 * @param newWalLedgerId        the new WAL ledger id after a rotation, or {@code null} if unchanged
 * @param addedUploads          new multipart uploads created by this edit (CreateMultipartUpload)
 * @param upsertParts           parts added/replaced under an existing upload (UploadPart)
 * @param removedUploads        upload ids dropped by this edit (CompleteMultipartUpload / Abort)
 * @param ownerFencingToken     fencing token of the authoring owner ({@code 0} = "stamp at apply time")
 */
public record ManifestEdit(
        List<SSTableMeta> addedTables,
        Set<Long> removedTableLedgerIds,
        Set<Long> addedSyrups,
        Set<Long> removedSyrups,
        Long newWalLedgerId,
        List<MultipartUploadState> addedUploads,
        List<PartUpsert> upsertParts,
        Set<String> removedUploads,
        long ownerFencingToken) {

    public ManifestEdit {
        addedTables = List.copyOf(addedTables);
        removedTableLedgerIds = Set.copyOf(removedTableLedgerIds);
        addedSyrups = Set.copyOf(addedSyrups);
        removedSyrups = Set.copyOf(removedSyrups);
        addedUploads = addedUploads == null ? List.of() : List.copyOf(addedUploads);
        upsertParts = upsertParts == null ? List.of() : List.copyOf(upsertParts);
        removedUploads = removedUploads == null ? Set.of() : Set.copyOf(removedUploads);
        if (ownerFencingToken < 0) {
            throw new IllegalArgumentException("ownerFencingToken must be non-negative");
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Convenience: a flush edit adding one table plus its syrups, optionally rotating the WAL. */
    public static ManifestEdit flush(SSTableMeta table, Set<Long> syrups, Long newWalLedgerId) {
        return new ManifestEdit(List.of(table), Set.of(), syrups, Set.of(), newWalLedgerId,
                List.of(), List.of(), Set.of(), 0L);
    }

    /** Returns a copy with the given owner fencing token (used by {@link Manifest#apply}). */
    public ManifestEdit withOwnerFencingToken(long token) {
        return new ManifestEdit(addedTables, removedTableLedgerIds, addedSyrups, removedSyrups,
                newWalLedgerId, addedUploads, upsertParts, removedUploads, token);
    }

    /**
     * One UploadPart-style upsert: replaces (or installs) the {@link Part} bound to
     * {@code (uploadId, partNumber)}. The replaced part's segments (if any) are reclaimable as
     * orphan Syrups in the usual way.
     */
    public record PartUpsert(String uploadId, int partNumber, Part part) {
        public PartUpsert {
            if (uploadId == null || uploadId.isEmpty()) {
                throw new IllegalArgumentException("uploadId is required");
            }
            if (partNumber < 1) {
                throw new IllegalArgumentException("partNumber must be >= 1");
            }
            if (part == null) {
                throw new IllegalArgumentException("part is required");
            }
        }
    }

    public static final class Builder {
        private List<SSTableMeta> addedTables = List.of();
        private Set<Long> removedTableLedgerIds = Set.of();
        private Set<Long> addedSyrups = Set.of();
        private Set<Long> removedSyrups = Set.of();
        private Long newWalLedgerId = null;
        private List<MultipartUploadState> addedUploads = List.of();
        private List<PartUpsert> upsertParts = List.of();
        private Set<String> removedUploads = Set.of();
        private long ownerFencingToken = 0L;

        public Builder addedTables(List<SSTableMeta> v) {
            this.addedTables = v;
            return this;
        }

        public Builder removedTableLedgerIds(Set<Long> v) {
            this.removedTableLedgerIds = v;
            return this;
        }

        public Builder addedSyrups(Set<Long> v) {
            this.addedSyrups = v;
            return this;
        }

        public Builder removedSyrups(Set<Long> v) {
            this.removedSyrups = v;
            return this;
        }

        public Builder newWalLedgerId(Long v) {
            this.newWalLedgerId = v;
            return this;
        }

        public Builder addedUploads(List<MultipartUploadState> v) {
            this.addedUploads = v;
            return this;
        }

        public Builder upsertParts(List<PartUpsert> v) {
            this.upsertParts = v;
            return this;
        }

        public Builder removedUploads(Set<String> v) {
            this.removedUploads = v;
            return this;
        }

        public Builder ownerFencingToken(long v) {
            this.ownerFencingToken = v;
            return this;
        }

        /** Adds a single PartUpsert without disturbing any already-set ones. */
        public Builder addPartUpsert(String uploadId, int partNumber, Part part) {
            java.util.ArrayList<PartUpsert> next = new java.util.ArrayList<>(upsertParts);
            next.add(new PartUpsert(uploadId, partNumber, part));
            this.upsertParts = next;
            return this;
        }

        /** Adds a single added-upload without disturbing any already-set ones. */
        public Builder addUpload(MultipartUploadState upload) {
            java.util.ArrayList<MultipartUploadState> next = new java.util.ArrayList<>(addedUploads);
            next.add(upload);
            this.addedUploads = next;
            return this;
        }

        public ManifestEdit build() {
            return new ManifestEdit(addedTables, removedTableLedgerIds, addedSyrups, removedSyrups,
                    newWalLedgerId, addedUploads, upsertParts, removedUploads, ownerFencingToken);
        }
    }

}
