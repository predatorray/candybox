package me.predatorray.candybox.lsm.wal;

import me.predatorray.candybox.common.Hlc;
import me.predatorray.candybox.common.Mutation;
import me.predatorray.candybox.common.RangeTombstone;

/**
 * One record in the write-ahead log: either a point {@link Mutation} (PUT or DELETE of a single key)
 * or a {@link RangeTombstone} ({@code deleteRange}). Modeling the WAL as this tagged union keeps the
 * recovery path a single ordered replay and lets the recovering owner observe the maximum HLC across
 * <em>both</em> kinds — essential so a range delete cannot be silently lost on handover (DESIGN §3,§7).
 */
public sealed interface WalEntry permits WalEntry.PointMutation, WalEntry.RangeDelete {

    /** The HLC stamped on this record (the value the recovering owner must advance past). */
    Hlc hlc();

    static WalEntry of(Mutation mutation) {
        return new PointMutation(mutation);
    }

    static WalEntry of(RangeTombstone tombstone) {
        return new RangeDelete(tombstone);
    }

    record PointMutation(Mutation mutation) implements WalEntry {
        @Override
        public Hlc hlc() {
            return mutation.hlc();
        }
    }

    record RangeDelete(RangeTombstone tombstone) implements WalEntry {
        @Override
        public Hlc hlc() {
            return tombstone.hlc();
        }
    }
}
