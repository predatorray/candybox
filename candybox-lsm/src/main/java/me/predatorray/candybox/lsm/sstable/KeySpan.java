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
package me.predatorray.candybox.lsm.sstable;

import me.predatorray.candybox.common.CandyKey;
import me.predatorray.candybox.common.RangeTombstone;

/**
 * A contiguous key interval: from {@code start} (inclusive, {@code null} = unbounded) to {@code end}
 * ({@code null} = unbounded; inclusive iff {@code endInclusive}).
 */
public record KeySpan(CandyKey start, CandyKey end, boolean endInclusive) {

    /** The closed interval {@code [min, max]}. */
    public static KeySpan closed(CandyKey min, CandyKey max) {
        return new KeySpan(min, max, true);
    }

    /** The keys a range tombstone covers. */
    public static KeySpan of(RangeTombstone rt) {
        return new KeySpan(rt.startInclusive(), rt.endExclusive(), false);
    }

    public boolean overlaps(KeySpan other) {
        return startsBeforeEndOf(start, other) && startsBeforeEndOf(other.start, this);
    }

    /** The smallest span covering both. */
    public KeySpan union(KeySpan other) {
        CandyKey s = start == null || other.start == null ? null
                : start.compareTo(other.start) <= 0 ? start : other.start;
        if (end == null || other.end == null) {
            return new KeySpan(s, null, false);
        }
        int cmp = end.compareTo(other.end);
        if (cmp == 0) {
            return new KeySpan(s, end, endInclusive || other.endInclusive);
        }
        return cmp > 0 ? new KeySpan(s, end, endInclusive) : new KeySpan(s, other.end, other.endInclusive);
    }

    private static boolean startsBeforeEndOf(CandyKey start, KeySpan span) {
        if (start == null || span.end == null) {
            return true;
        }
        int cmp = start.compareTo(span.end);
        return span.endInclusive ? cmp <= 0 : cmp < 0;
    }
}
