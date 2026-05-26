package me.predatorray.candybox.lsm.manifest;

import java.util.ArrayList;
import java.util.List;
import me.predatorray.candybox.bookkeeper.LedgerConfig;
import me.predatorray.candybox.bookkeeper.LedgerStore;
import me.predatorray.candybox.bookkeeper.ReadableLedger;
import me.predatorray.candybox.common.exception.FencedException;
import me.predatorray.candybox.lsm.sstable.SSTableMeta;

/**
 * A Box's manifest: an in-memory {@link ManifestState} kept in lock-step with a durable
 * {@link ManifestLog}. Every {@link #apply(ManifestEdit)} is appended to the log (durable + fenced)
 * before the in-memory state advances, so the two never diverge and a fenced owner cannot mutate
 * committed state.
 *
 * <p>{@link #createNew} boots a brand-new Box; {@link #recover} performs the handover sequence —
 * recover-open and replay the prior manifest ledger, then open a fresh ledger seeded with a
 * self-contained checkpoint of the recovered state.
 */
public final class Manifest implements AutoCloseable {

    private final ManifestLog log;
    private volatile ManifestState state;

    private Manifest(ManifestLog log, ManifestState state) {
        this.log = log;
        this.state = state;
    }

    /** Creates a manifest for a new Box, backed by a fresh empty manifest ledger. */
    public static Manifest createNew(LedgerStore store, LedgerConfig manifestConfig) {
        return new Manifest(ManifestLog.create(store, manifestConfig), ManifestState.empty());
    }

    /**
     * Recovers a Box's manifest on ownership handover: fences and replays the prior ledger, then opens
     * a fresh ledger seeded with a checkpoint of the recovered state.
     *
     * @param store              the ledger store
     * @param manifestConfig     config for the fresh manifest ledger
     * @param priorManifestLedgerId the previous owner's manifest ledger id
     * @return the recovered manifest, ready for writes on the fresh ledger
     */
    public static Manifest recover(LedgerStore store, LedgerConfig manifestConfig,
                                   long priorManifestLedgerId) {
        ReadableLedger prior = store.recoverOpen(priorManifestLedgerId);
        ManifestState recovered = ManifestState.empty();
        try {
            for (ManifestEdit edit : ManifestLog.replay(prior)) {
                recovered = recovered.apply(edit);
            }
        } finally {
            prior.close();
        }

        ManifestLog fresh = ManifestLog.create(store, manifestConfig);
        fresh.append(checkpoint(recovered));
        return new Manifest(fresh, recovered);
    }

    /** Appends an edit durably and advances the in-memory state. */
    public synchronized void apply(ManifestEdit edit) {
        log.append(edit); // throws FencedException if this owner has been fenced
        state = state.apply(edit);
    }

    /** The current LSM state snapshot. */
    public ManifestState current() {
        return state;
    }

    /** The id of the manifest ledger this owner is writing to. */
    public long ledgerId() {
        return log.ledgerId();
    }

    @Override
    public void close() {
        log.close();
    }

    /** Builds a single edit that fully captures {@code state}, used to seed a fresh manifest ledger. */
    private static ManifestEdit checkpoint(ManifestState state) {
        List<SSTableMeta> tables = new ArrayList<>(state.tables());
        Long wal = state.walLedgerId() < 0 ? null : state.walLedgerId();
        return ManifestEdit.builder()
                .addedTables(tables)
                .addedSyrups(state.liveSyrups())
                .newWalLedgerId(wal)
                .build();
    }
}
