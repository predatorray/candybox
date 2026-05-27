package me.predatorray.candybox.lsm.manifest;

import java.util.List;
import java.util.Set;
import me.predatorray.candybox.lsm.sstable.SSTableMeta;

/**
 * One append-only change to the LSM state: a flush adds an SSTable (and the Syrups it referenced) and
 * may rotate the WAL; a compaction adds the output table and removes the inputs; GC removes obsoleted
 * tables/Syrups. Edits are serialized and appended to the manifest ledger by the Box's single owner.
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
 * @param ownerFencingToken     fencing token of the authoring owner ({@code 0} = "stamp at apply time")
 */
public record ManifestEdit(
        List<SSTableMeta> addedTables,
        Set<Long> removedTableLedgerIds,
        Set<Long> addedSyrups,
        Set<Long> removedSyrups,
        Long newWalLedgerId,
        long ownerFencingToken) {

    public ManifestEdit {
        addedTables = List.copyOf(addedTables);
        removedTableLedgerIds = Set.copyOf(removedTableLedgerIds);
        addedSyrups = Set.copyOf(addedSyrups);
        removedSyrups = Set.copyOf(removedSyrups);
        if (ownerFencingToken < 0) {
            throw new IllegalArgumentException("ownerFencingToken must be non-negative");
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Convenience: a flush edit adding one table plus its syrups, optionally rotating the WAL. */
    public static ManifestEdit flush(SSTableMeta table, Set<Long> syrups, Long newWalLedgerId) {
        return new ManifestEdit(List.of(table), Set.of(), syrups, Set.of(), newWalLedgerId, 0L);
    }

    /** Returns a copy with the given owner fencing token (used by {@link Manifest#apply}). */
    public ManifestEdit withOwnerFencingToken(long token) {
        return new ManifestEdit(addedTables, removedTableLedgerIds, addedSyrups, removedSyrups,
                newWalLedgerId, token);
    }

    public static final class Builder {
        private List<SSTableMeta> addedTables = List.of();
        private Set<Long> removedTableLedgerIds = Set.of();
        private Set<Long> addedSyrups = Set.of();
        private Set<Long> removedSyrups = Set.of();
        private Long newWalLedgerId = null;
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

        public Builder ownerFencingToken(long v) {
            this.ownerFencingToken = v;
            return this;
        }

        public ManifestEdit build() {
            return new ManifestEdit(addedTables, removedTableLedgerIds, addedSyrups, removedSyrups,
                    newWalLedgerId, ownerFencingToken);
        }
    }
}
