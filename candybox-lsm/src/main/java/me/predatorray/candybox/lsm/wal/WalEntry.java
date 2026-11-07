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

import java.util.List;
import me.predatorray.candybox.common.Hlc;
import me.predatorray.candybox.common.Mutation;
import me.predatorray.candybox.common.RangeTombstone;

/**
 * One WAL record: a point {@link Mutation}, an atomic batch of point mutations, or a
 * {@link RangeTombstone}. This tagged union keeps recovery a single ordered replay and lets the
 * recovering owner observe the maximum HLC across every mutation and range delete (DESIGN §3,§7).
 */
public sealed interface WalEntry permits WalEntry.PointMutation, WalEntry.RangeDelete,
        WalEntry.Batch {

    /** The maximum HLC in this record (the value the recovering owner must advance past). */
    Hlc hlc();

    static WalEntry of(Mutation mutation) {
        return new PointMutation(mutation);
    }

    static WalEntry of(RangeTombstone tombstone) {
        return new RangeDelete(tombstone);
    }

    /** One BookKeeper entry: all members become durable together. */
    static WalEntry batch(List<Mutation> mutations) {
        return new Batch(mutations);
    }

    record Batch(List<Mutation> mutations) implements WalEntry {
        public static final int MAX_MUTATIONS = 128;

        public Batch {
            mutations = List.copyOf(mutations);
            if (mutations.isEmpty() || mutations.size() > MAX_MUTATIONS) {
                throw new IllegalArgumentException("WAL batch must contain 1.." + MAX_MUTATIONS
                        + " mutations");
            }
        }

        @Override
        public Hlc hlc() {
            return mutations.stream().map(Mutation::hlc).max(Hlc::compareTo).orElseThrow();
        }
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
