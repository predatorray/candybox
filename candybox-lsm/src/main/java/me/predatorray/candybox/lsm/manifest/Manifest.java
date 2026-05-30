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
 * <p>Fencing is enforced at two levels (defense in depth): BookKeeper recover-open is the hard fence
 * on the ledger, and the manifest additionally tracks the highest **owner fencing token** it has
 * committed and rejects any edit (or handover) whose token regresses below it.
 *
 * <p>{@link #createNew} boots a brand-new Box; {@link #recover} performs the handover sequence —
 * recover-open and replay the prior manifest ledger, reject the handover if this owner's token is
 * stale, then open a fresh ledger seeded with a checkpoint of the recovered state.
 */
public final class Manifest implements AutoCloseable {

    private final ManifestLog log;
    private final long ownerFencingToken;
    private volatile ManifestState state;
    private long maxToken;

    private Manifest(ManifestLog log, ManifestState state, long ownerFencingToken, long maxToken) {
        this.log = log;
        this.state = state;
        this.ownerFencingToken = ownerFencingToken;
        this.maxToken = maxToken;
    }

    /** Creates a manifest for a new Box, backed by a fresh empty manifest ledger. */
    public static Manifest createNew(LedgerStore store, LedgerConfig manifestConfig,
                                     long ownerFencingToken) {
        return new Manifest(ManifestLog.create(store, manifestConfig), ManifestState.empty(),
                ownerFencingToken, ownerFencingToken);
    }

    /**
     * Recovers a Box's manifest on ownership handover: fences and replays the prior ledger, then opens
     * a fresh ledger seeded with a checkpoint of the recovered state.
     *
     * @param ownerFencingToken      the recovering owner's fencing token; must not be below the highest
     *                               token already committed, otherwise the handover is stale
     * @throws FencedException if {@code ownerFencingToken} regresses below the recovered max token
     */
    public static Manifest recover(LedgerStore store, LedgerConfig manifestConfig,
                                   long priorManifestLedgerId, long ownerFencingToken) {
        ReadableLedger prior = store.recoverOpen(priorManifestLedgerId);
        ManifestState recovered = ManifestState.empty();
        long maxReplayedToken = 0;
        try {
            for (ManifestEdit edit : ManifestLog.replay(prior)) {
                recovered = recovered.apply(edit);
                maxReplayedToken = Math.max(maxReplayedToken, edit.ownerFencingToken());
            }
        } finally {
            prior.close();
        }
        if (ownerFencingToken < maxReplayedToken) {
            throw new FencedException("Stale handover: owner token " + ownerFencingToken
                    + " is below the committed max token " + maxReplayedToken);
        }

        ManifestLog fresh = ManifestLog.create(store, manifestConfig);
        fresh.append(checkpoint(recovered).withOwnerFencingToken(ownerFencingToken));
        return new Manifest(fresh, recovered, ownerFencingToken, ownerFencingToken);
    }

    /**
     * Appends an edit durably and advances the in-memory state. The edit is stamped with the authoring
     * token (its own if set, else this owner's); a token below the highest committed token is rejected.
     *
     * @throws FencedException if the edit's token regresses, or this owner's ledger has been fenced
     */
    public synchronized void apply(ManifestEdit edit) {
        long token = edit.ownerFencingToken() == 0 ? ownerFencingToken : edit.ownerFencingToken();
        if (token < maxToken) {
            throw new FencedException("Rejecting manifest edit with fencing token " + token
                    + " below committed max " + maxToken);
        }
        ManifestEdit stamped = edit.withOwnerFencingToken(token);
        log.append(stamped); // hard fence: throws FencedException if this ledger was recover-opened
        state = state.apply(stamped);
        maxToken = Math.max(maxToken, token);
    }

    /** The current LSM state snapshot. */
    public ManifestState current() {
        return state;
    }

    /** The id of the manifest ledger this owner is writing to. */
    public long ledgerId() {
        return log.ledgerId();
    }

    /** This owner's fencing token. */
    public long ownerFencingToken() {
        return ownerFencingToken;
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
