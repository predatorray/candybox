package me.predatorray.candybox.lsm.compaction;

import java.util.List;
import me.predatorray.candybox.lsm.sstable.SSTableMeta;

/**
 * A unit of compaction work: merge {@code inputs} into a single output SSTable at {@code outputLevel}.
 *
 * @param inputs        the SSTables to merge (from one or more levels)
 * @param outputLevel   the level the merged output is written to
 * @param bottommost    whether {@code outputLevel} is the bottommost level holding overlapping data;
 *                      only then may a sufficiently old tombstone be dropped (LevelDB rule)
 */
public record CompactionTask(List<SSTableMeta> inputs, int outputLevel, boolean bottommost) {

    public CompactionTask {
        inputs = List.copyOf(inputs);
        if (inputs.isEmpty()) {
            throw new IllegalArgumentException("a compaction task must have at least one input");
        }
    }
}
