package me.predatorray.candybox.lsm.compaction;

import java.util.Optional;
import me.predatorray.candybox.lsm.manifest.ManifestState;

/**
 * The pluggable compaction policy SPI. An implementation inspects the current {@link ManifestState}
 * and decides what (if anything) to compact next. Candybox ships {@link LeveledCompactionStrategy} as
 * the default (LevelDB-style), modelled after Cassandra's pluggable strategies.
 *
 * <p>Picking is pure and side-effect-free; execution (merge, write, fenced commit) is the
 * {@link Compactor}'s job, scheduled distributively in Phase 3.
 */
public interface CompactionStrategy {

    /**
     * Chooses the next compaction, if one is warranted.
     *
     * @param state the current LSM state
     * @return a task to run, or empty if nothing should be compacted now
     */
    Optional<CompactionTask> pickCompaction(ManifestState state);
}
