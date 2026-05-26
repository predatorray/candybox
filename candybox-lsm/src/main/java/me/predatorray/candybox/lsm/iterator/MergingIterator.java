package me.predatorray.candybox.lsm.iterator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import me.predatorray.candybox.common.CandyKey;
import me.predatorray.candybox.common.Mutation;

/**
 * Merges several key-ascending mutation sources (the active memtable, immutable memtables being
 * flushed, and L0 SSTables) into one key-ascending stream, resolving duplicates by last-writer-wins.
 *
 * <p>Each source must be sorted ascending by key with at most one entry per key. When the same key
 * appears in multiple sources, the entry with the highest HLC wins (the LWW rule, with the nodeId
 * tiebreaker) — not the highest source priority, which would be wrong under HLC.
 *
 * <p>When {@code dropTombstones} is set the merged stream omits DELETE winners (the read/list view);
 * compaction passes {@code false} so tombstones survive until the bottommost-level GC rule drops them.
 */
public final class MergingIterator implements Iterator<Mutation> {

    private final List<PeekingIterator<Mutation>> sources;
    private final boolean dropTombstones;
    private Mutation nextOut;
    private boolean computed;

    public MergingIterator(List<Iterator<Mutation>> sources, boolean dropTombstones) {
        this.sources = new ArrayList<>(sources.size());
        for (Iterator<Mutation> s : sources) {
            this.sources.add(new PeekingIterator<>(s));
        }
        this.dropTombstones = dropTombstones;
    }

    @Override
    public boolean hasNext() {
        if (!computed) {
            nextOut = computeNext();
            computed = true;
        }
        return nextOut != null;
    }

    @Override
    public Mutation next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        Mutation result = nextOut;
        nextOut = null;
        computed = false;
        return result;
    }

    private Mutation computeNext() {
        while (true) {
            CandyKey minKey = null;
            for (PeekingIterator<Mutation> it : sources) {
                if (it.hasNext()) {
                    CandyKey k = it.peek().key();
                    if (minKey == null || k.compareTo(minKey) < 0) {
                        minKey = k;
                    }
                }
            }
            if (minKey == null) {
                return null; // all sources exhausted
            }

            Mutation winner = null;
            for (PeekingIterator<Mutation> it : sources) {
                if (it.hasNext() && it.peek().key().equals(minKey)) {
                    Mutation cand = it.next();
                    if (winner == null || cand.hlc().isAfter(winner.hlc())) {
                        winner = cand;
                    }
                }
            }

            if (dropTombstones && winner.isTombstone()) {
                continue; // suppress and move to the next key
            }
            return winner;
        }
    }
}
