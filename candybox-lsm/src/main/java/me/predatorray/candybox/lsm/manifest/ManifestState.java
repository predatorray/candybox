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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import me.predatorray.candybox.lsm.sstable.SSTableMeta;

/**
 * An immutable snapshot of a Box's LSM state: the SSTables that exist (with their levels and key
 * ranges), the set of live Syrups, and the id of the current WAL ledger. Produced by replaying the
 * manifest log and advanced by {@link #apply(ManifestEdit)}.
 *
 * <p>One {@code ManifestState} corresponds to one Box's single partition (v1); the structure is kept
 * partition-shaped so key-range sharding can be added later without reworking it.
 */
public final class ManifestState {

    private static final ManifestState EMPTY =
            new ManifestState(List.of(), Set.of(), -1L);

    private final List<SSTableMeta> tables;
    private final Set<Long> liveSyrups;
    private final long walLedgerId;

    private ManifestState(List<SSTableMeta> tables, Set<Long> liveSyrups, long walLedgerId) {
        this.tables = List.copyOf(tables);
        this.liveSyrups = Set.copyOf(liveSyrups);
        this.walLedgerId = walLedgerId;
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
        return new ManifestState(newTables, newSyrups, newWal);
    }
}
