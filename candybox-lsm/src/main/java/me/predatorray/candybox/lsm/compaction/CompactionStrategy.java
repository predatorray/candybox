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
