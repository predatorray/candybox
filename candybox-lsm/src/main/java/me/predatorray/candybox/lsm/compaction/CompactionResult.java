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
