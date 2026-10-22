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
package me.predatorray.candybox.lsm.compaction;

import java.util.List;
import me.predatorray.candybox.lsm.sstable.SSTableMeta;

/**
 * A unit of compaction work: merge {@code inputs} into output SSTables at {@code outputLevel}.
 *
 * @param inputs        the SSTables to merge (from one or more levels)
 * @param outputLevel   the level the merged output is written to
 * @param bottommost    whether {@code outputLevel} is the bottommost level holding overlapping data;
 *                      only then may a sufficiently old tombstone be dropped (LevelDB rule)
 * @param otherTables   the L1+ tables left out of the task; an aged range tombstone overlapping any of
 *                      them is kept, since it may still shadow older points there
 */
public record CompactionTask(List<SSTableMeta> inputs, int outputLevel, boolean bottommost,
                             List<SSTableMeta> otherTables) {

    public CompactionTask {
        inputs = List.copyOf(inputs);
        otherTables = List.copyOf(otherTables);
        if (inputs.isEmpty()) {
            throw new IllegalArgumentException("a compaction task must have at least one input");
        }
    }
}
