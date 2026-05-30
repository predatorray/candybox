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
