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
package me.predatorray.candybox.common;

/**
 * A range deletion marker: it shadows every key in the half-open interval
 * {@code [startInclusive, endExclusive)} whose own locator HLC is older than {@link #hlc()}. Unlike a
 * point {@link LocatorType#DELETE} tombstone (which targets one key), a range tombstone deletes a whole
 * key span with a single O(1) record — the LSM primitive behind {@code deleteRange}.
 *
 * <p>Both bounds are nullable: a null {@code startInclusive} means "from the start of the keyspace" and
 * a null {@code endExclusive} means "to the end". It carries no Syrup segments, so it never pins data;
 * the bytes it shadows are reclaimed when compaction drops the covered point locators (see DESIGN §9).
 *
 * @param startInclusive inclusive lower bound (nullable = unbounded below)
 * @param endExclusive   exclusive upper bound (nullable = unbounded above)
 * @param hlc            the deletion timestamp (LWW key); only strictly-older keys are shadowed
 */
public record RangeTombstone(CandyKey startInclusive, CandyKey endExclusive, Hlc hlc) {

    public RangeTombstone {
        if (hlc == null) {
            throw new IllegalArgumentException("hlc is required");
        }
        if (startInclusive != null && endExclusive != null
                && startInclusive.compareTo(endExclusive) >= 0) {
            throw new IllegalArgumentException(
                    "range tombstone start must be strictly less than end: "
                            + startInclusive.value() + " >= " + endExclusive.value());
        }
    }

    /** Whether {@code key} falls within {@code [startInclusive, endExclusive)}. */
    public boolean covers(CandyKey key) {
        if (startInclusive != null && key.compareTo(startInclusive) < 0) {
            return false;
        }
        return endExclusive == null || key.compareTo(endExclusive) < 0;
    }
}
