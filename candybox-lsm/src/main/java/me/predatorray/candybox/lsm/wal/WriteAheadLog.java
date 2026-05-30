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
package me.predatorray.candybox.lsm.wal;

import java.util.ArrayList;
import java.util.List;
import me.predatorray.candybox.bookkeeper.LedgerConfig;
import me.predatorray.candybox.bookkeeper.LedgerEntry;
import me.predatorray.candybox.bookkeeper.LedgerStore;
import me.predatorray.candybox.bookkeeper.ReadableLedger;
import me.predatorray.candybox.bookkeeper.WritableLedger;
import me.predatorray.candybox.common.Hlc;
import me.predatorray.candybox.common.Mutation;
import me.predatorray.candybox.common.RangeTombstone;

/**
 * The write-ahead log: every mutation is appended here (and ack-quorum durable) before the memtable
 * acknowledges, so an un-flushed memtable can be rebuilt after a crash or ownership handover.
 *
 * <p>One WAL ledger is open for writing at a time. On flush the engine rotates to a fresh WAL ledger;
 * the old one becomes eligible for GC once its mutations are durable in an SSTable.
 *
 * <p>On handover the new owner must {@code recover-open} the prior WAL (fencing it) and replay it via
 * {@link #replay(ReadableLedger)}, then advance its HLC past {@link ReplayResult#maxHlc()} before
 * stamping anything — see {@code BoxEngine}.
 */
public final class WriteAheadLog implements AutoCloseable {

    private final WritableLedger ledger;

    private WriteAheadLog(WritableLedger ledger) {
        this.ledger = ledger;
    }

    /** Creates a fresh WAL ledger. */
    public static WriteAheadLog create(LedgerStore store, LedgerConfig config) {
        return new WriteAheadLog(store.createLedger(config));
    }

    /** Appends a point mutation, blocking until ack-quorum durable. */
    public long append(Mutation mutation) {
        return append(WalEntry.of(mutation));
    }

    /** Appends a range tombstone ({@code deleteRange}), blocking until ack-quorum durable. */
    public long append(RangeTombstone tombstone) {
        return append(WalEntry.of(tombstone));
    }

    /** Appends a WAL entry of either kind, blocking until ack-quorum durable. */
    public long append(WalEntry entry) {
        return ledger.append(WalEntrySerializer.serialize(entry));
    }

    public long ledgerId() {
        return ledger.ledgerId();
    }

    @Override
    public void close() {
        ledger.close();
    }

    /**
     * Reads every entry of a WAL ledger back into {@link WalEntry}s, also reporting the maximum HLC
     * seen across both point mutations and range tombstones — the value the recovering owner must
     * advance its clock beyond before stamping anything new.
     *
     * @param ledger a readable WAL ledger (typically the result of {@code recoverOpen})
     */
    public static ReplayResult replay(ReadableLedger ledger) {
        List<WalEntry> entries = new ArrayList<>();
        Hlc maxHlc = Hlc.MIN;
        long lac = ledger.lastAddConfirmed();
        if (lac >= 0) {
            for (LedgerEntry entry : ledger.readRange(0, lac)) {
                WalEntry e = WalEntrySerializer.deserialize(entry.data());
                entries.add(e);
                if (e.hlc().isAfter(maxHlc)) {
                    maxHlc = e.hlc();
                }
            }
        }
        return new ReplayResult(entries, maxHlc);
    }

    /**
     * The outcome of a WAL replay.
     *
     * @param entries the WAL entries in append order (point mutations and range tombstones)
     * @param maxHlc  the highest HLC recorded ({@link Hlc#MIN} if the WAL was empty)
     */
    public record ReplayResult(List<WalEntry> entries, Hlc maxHlc) {
    }
}
