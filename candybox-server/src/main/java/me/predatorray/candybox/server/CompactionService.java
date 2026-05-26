package me.predatorray.candybox.server;

import java.util.Optional;
import me.predatorray.candybox.bookkeeper.LedgerConfig;
import me.predatorray.candybox.bookkeeper.LedgerStore;
import me.predatorray.candybox.common.Clock;
import me.predatorray.candybox.common.config.CandyboxConfig;
import me.predatorray.candybox.common.config.LedgerRole;
import me.predatorray.candybox.lsm.compaction.Compactor;
import me.predatorray.candybox.lsm.compaction.CompactionResult;
import me.predatorray.candybox.lsm.compaction.CompactionStrategy;
import me.predatorray.candybox.lsm.compaction.CompactionTask;
import me.predatorray.candybox.lsm.compaction.LeveledCompactionStrategy;
import me.predatorray.candybox.lsm.engine.BoxEngine;
import me.predatorray.candybox.lsm.sstable.SSTableWriter;

/**
 * Drives compaction for a Box: picks work with the {@link CompactionStrategy}, runs it with the
 * {@link Compactor}, and commits the resulting manifest edit on the engine.
 *
 * <p>This wires the LSM compaction core into the node; the <em>distributed</em> aspects — claiming
 * work via ZooKeeper leader election / task leases and gating the commit on the owner's fencing token
 * — are TODO(phase-3). Today it runs synchronously, in-process, for the local owner.
 */
public final class CompactionService {

    private final CompactionStrategy strategy;
    private final Compactor compactor;

    public CompactionService(LedgerStore ledgerStore, CandyboxConfig config, Clock clock) {
        LedgerConfig sstableConfig = LedgerConfig.forRole(LedgerRole.SSTABLE);
        SSTableWriter writer = new SSTableWriter(ledgerStore, config.bloomBitsPerKey());
        this.strategy = new LeveledCompactionStrategy(config.l0CompactionTrigger());
        this.compactor = new Compactor(ledgerStore, writer, sstableConfig,
                config.tombstoneGcGraceMillis(), clock);
    }

    /**
     * Runs at most one compaction step on the engine.
     *
     * @return {@code true} if a compaction was performed, {@code false} if nothing was due
     */
    public boolean compactOnce(BoxEngine engine) {
        Optional<CompactionTask> task = strategy.pickCompaction(engine.manifestState());
        if (task.isEmpty()) {
            return false;
        }
        CompactionResult result = compactor.compact(task.get());
        engine.applyCompaction(result.edit());
        return true;
    }
}
