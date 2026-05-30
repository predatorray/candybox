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
import me.predatorray.candybox.lsm.manifest.ManifestEdit;
import me.predatorray.candybox.lsm.sstable.SSTableMeta;

/**
 * The outcome of running a {@link CompactionTask}: the output table (absent if the whole input merged
 * down to nothing — e.g. all tombstones aged out at the bottommost level) and the manifest edit that
 * the Box owner must commit (gated on its fencing token) to swap inputs for the output.
 *
 * <p>The physical deletion of the now-obsolete input ledgers is performed later by reference-counted
 * GC, never here — see DESIGN.md §10 Phase 3.
 *
 * @param output the output table, if any
 * @param edit   the manifest edit removing the inputs and adding the output
 */
public record CompactionResult(Optional<SSTableMeta> output, ManifestEdit edit) {
}
