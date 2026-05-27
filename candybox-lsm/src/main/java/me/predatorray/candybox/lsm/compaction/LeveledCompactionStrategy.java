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
 *   <li>Each level L≥1 has a byte budget {@code levelBaseBytes × levelMultiplier^(L-1)}. The level
 *       whose total size most exceeds its budget is compacted: one of its tables and the overlapping
 *       L+1 tables are merged into L+1.</li>
 * </ul>
 *
 * <p>The {@code bottommost} flag on the produced task is true iff no level above the output level
 * currently holds data, which is the precondition for dropping aged tombstones during the merge.
 */
public final class LeveledCompactionStrategy implements CompactionStrategy {

    static final long DEFAULT_LEVEL_BASE_BYTES = 10L << 20; // 10 MiB budget for L1
    static final int DEFAULT_LEVEL_MULTIPLIER = 10;

    private final int l0CompactionTrigger;
    private final long levelBaseBytes;
    private final int levelMultiplier;

    public LeveledCompactionStrategy(int l0CompactionTrigger) {
        this(l0CompactionTrigger, DEFAULT_LEVEL_BASE_BYTES, DEFAULT_LEVEL_MULTIPLIER);
    }

    public LeveledCompactionStrategy(int l0CompactionTrigger, long levelBaseBytes, int levelMultiplier) {
        this.l0CompactionTrigger = l0CompactionTrigger;
        this.levelBaseBytes = levelBaseBytes;
        this.levelMultiplier = Math.max(2, levelMultiplier);
    }

    @Override
    public Optional<CompactionTask> pickCompaction(ManifestState state) {
        // L0 is scored by file count (its tables overlap, so byte size is not meaningful there).
        List<SSTableMeta> l0 = state.level0();
        if (l0.size() >= l0CompactionTrigger) {
            return Optional.of(buildTask(l0, state, 1));
        }

        // Levels >= 1 are scored by total bytes against a growing budget; pick the most over budget.
        int maxLevel = state.maxLevel();
        int bestLevel = -1;
        double bestScore = 1.0; // only act on a level strictly over its budget
        for (int level = 1; level <= maxLevel; level++) {
            double score = (double) levelBytes(state, level) / maxBytesForLevel(level);
            if (score > bestScore) {
                bestScore = score;
                bestLevel = level;
            }
        }
        if (bestLevel < 0) {
            return Optional.empty();
        }
        List<SSTableMeta> tables = state.level(bestLevel);
        return Optional.of(buildTask(List.of(tables.get(0)), state, bestLevel + 1));
    }

    private long maxBytesForLevel(int level) {
        long budget = levelBaseBytes;
        for (int i = 1; i < level; i++) {
            budget *= levelMultiplier;
        }
        return budget;
    }

    private static long levelBytes(ManifestState state, int level) {
        long total = 0;
        for (SSTableMeta t : state.level(level)) {
            total += t.sizeBytes();
        }
        return total;
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
