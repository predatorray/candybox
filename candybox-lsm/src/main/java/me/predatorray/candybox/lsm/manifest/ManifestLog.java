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
import me.predatorray.candybox.bookkeeper.LedgerEntry;
import me.predatorray.candybox.bookkeeper.LedgerStore;
import me.predatorray.candybox.bookkeeper.ReadableLedger;
import me.predatorray.candybox.bookkeeper.WritableLedger;
import me.predatorray.candybox.common.exception.FencedException;

/**
 * The append-only manifest ledger (Pulsar managed-ledger style): each entry is one serialized
 * {@link ManifestEdit}. A Box's manifest is owned by exactly one node, so there is a single writer.
 *
 * <p>Because a BookKeeper ledger has one writer and cannot be appended once sealed, ownership change
 * means: {@code recover-open} (seal) the prior ledger, {@link #replay(ReadableLedger)} it to rebuild
 * state, then open a <em>fresh</em> ledger. A zombie former owner's {@link #append(ManifestEdit)} then
 * fails with {@link FencedException} — the manifest's safety property.
 */
public final class ManifestLog implements AutoCloseable {

    private final WritableLedger ledger;

    private ManifestLog(WritableLedger ledger) {
        this.ledger = ledger;
    }

    /** Opens a fresh, empty manifest ledger for writing. */
    public static ManifestLog create(LedgerStore store, LedgerConfig config) {
        return new ManifestLog(store.createLedger(config));
    }

    /**
     * Appends an edit.
     *
     * @throws FencedException if this ledger has been fenced by another owner's recovery
     */
    public void append(ManifestEdit edit) {
        ledger.append(ManifestSerializer.serialize(edit));
    }

    public long ledgerId() {
        return ledger.ledgerId();
    }

    @Override
    public void close() {
        ledger.close();
    }

    /** Replays all edits from a (typically recover-opened) manifest ledger, in order. */
    public static List<ManifestEdit> replay(ReadableLedger ledger) {
        List<ManifestEdit> edits = new ArrayList<>();
        long lac = ledger.lastAddConfirmed();
        if (lac >= 0) {
            for (LedgerEntry entry : ledger.readRange(0, lac)) {
                edits.add(ManifestSerializer.deserialize(entry.data()));
            }
        }
        return edits;
    }
}
