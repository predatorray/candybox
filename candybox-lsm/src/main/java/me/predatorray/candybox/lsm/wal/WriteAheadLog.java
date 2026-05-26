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
import me.predatorray.candybox.common.serial.MutationSerializer;

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

    /** Appends a mutation, blocking until ack-quorum durable. */
    public long append(Mutation mutation) {
        return ledger.append(MutationSerializer.serialize(mutation));
    }

    public long ledgerId() {
        return ledger.ledgerId();
    }

    @Override
    public void close() {
        ledger.close();
    }

    /**
     * Reads every entry of a WAL ledger back into mutations, also reporting the maximum HLC seen — the
     * value the recovering owner must advance its clock beyond.
     *
     * @param ledger a readable WAL ledger (typically the result of {@code recoverOpen})
     */
    public static ReplayResult replay(ReadableLedger ledger) {
        List<Mutation> mutations = new ArrayList<>();
        Hlc maxHlc = Hlc.MIN;
        long lac = ledger.lastAddConfirmed();
        if (lac >= 0) {
            for (LedgerEntry entry : ledger.readRange(0, lac)) {
                Mutation m = MutationSerializer.deserialize(entry.data());
                mutations.add(m);
                if (m.hlc().isAfter(maxHlc)) {
                    maxHlc = m.hlc();
                }
            }
        }
        return new ReplayResult(mutations, maxHlc);
    }

    /**
     * The outcome of a WAL replay.
     *
     * @param mutations the mutations in append order
     * @param maxHlc    the highest HLC recorded ({@link Hlc#MIN} if the WAL was empty)
     */
    public record ReplayResult(List<Mutation> mutations, Hlc maxHlc) {
    }
}
