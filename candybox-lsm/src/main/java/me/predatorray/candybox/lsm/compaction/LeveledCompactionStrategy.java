package me.predatorray.candybox.lsm.compaction;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import me.predatorray.candybox.common.CandyKey;
import me.predatorray.candybox.lsm.manifest.ManifestState;
import me.predatorray.candybox.lsm.sstable.SSTableMeta;

/**
 * LevelDB-style leveled compaction, the default strategy.
 *
 * <ul>
 *   <li>When L0 reaches the trigger count, all L0 tables (which may overlap each other) plus the
 *       overlapping L1 tables are merged into L1.</li>
 *   <li>When a level L≥1 exceeds its table budget, one of its tables and the overlapping L+1 tables
 *       are merged into L+1.</li>
 * </ul>
 *
 * <p>The {@code bottommost} flag on the produced task is true iff no level above the output level
 * currently holds data, which is the precondition for dropping aged tombstones during the merge.
 *
 * <p>TODO(phase-3): replace the simple per-level table-count budget with LevelDB's byte-size scoring
 * (≈10× growth per level, target file size) and round-robin input selection.
 */
public final class LeveledCompactionStrategy implements CompactionStrategy {

    private final int l0CompactionTrigger;
    private final int maxTablesPerLevel;

    public LeveledCompactionStrategy(int l0CompactionTrigger) {
        this(l0CompactionTrigger, 10);
    }

    public LeveledCompactionStrategy(int l0CompactionTrigger, int maxTablesPerLevel) {
        this.l0CompactionTrigger = l0CompactionTrigger;
        this.maxTablesPerLevel = maxTablesPerLevel;
    }

    @Override
    public Optional<CompactionTask> pickCompaction(ManifestState state) {
        List<SSTableMeta> l0 = state.level0();
        if (l0.size() >= l0CompactionTrigger) {
            return Optional.of(buildTask(l0, state, 1));
        }
        int maxLevel = state.maxLevel();
        for (int level = 1; level <= maxLevel; level++) {
            List<SSTableMeta> tables = state.level(level);
            if (tables.size() > maxTablesPerLevel) {
                return Optional.of(buildTask(List.of(tables.get(0)), state, level + 1));
            }
        }
        return Optional.empty();
    }

    private CompactionTask buildTask(List<SSTableMeta> seedInputs, ManifestState state, int outputLevel) {
        CandyKey min = null;
        CandyKey max = null;
        for (SSTableMeta t : seedInputs) {
            if (min == null || t.minKey().compareTo(min) < 0) {
                min = t.minKey();
            }
            if (max == null || t.maxKey().compareTo(max) > 0) {
                max = t.maxKey();
            }
        }
        List<SSTableMeta> inputs = new ArrayList<>(seedInputs);
        for (SSTableMeta t : state.level(outputLevel)) {
            if (t.overlaps(min, max)) {
                inputs.add(t);
            }
        }
        boolean bottommost = noTablesAboveLevel(state, outputLevel);
        return new CompactionTask(inputs, outputLevel, bottommost);
    }

    private static boolean noTablesAboveLevel(ManifestState state, int level) {
        return state.tables().stream().noneMatch(t -> t.level() > level);
    }
}
