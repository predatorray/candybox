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
 * A Hybrid Logical Clock timestamp: {@code (physicalMillis, logicalCounter, nodeId)}.
 *
 * <p>This is the last-writer-wins (LWW) key for every mutation. Ordering is the natural lexicographic
 * order of the three fields: physical time first, then the logical counter (which disambiguates events
 * within the same millisecond and lets ordering survive a regressing wall clock), then {@code nodeId}
 * as the deterministic final tiebreaker (locked by design — see DESIGN.md decision 4).
 *
 * <p>HLC is chosen over a raw wall clock so a node with a fast clock cannot permanently win LWW or
 * resurrect deleted Candies, and over a global counter so there is no coordination bottleneck.
 *
 * @param physicalMillis the physical component (epoch millis), never trusted alone for ordering
 * @param logicalCounter monotonic counter within a {@code physicalMillis} tick; 32-bit (see DESIGN.md)
 * @param nodeId         the stamping node's id; the LWW tiebreaker
 */
public record Hlc(long physicalMillis, int logicalCounter, int nodeId) implements Comparable<Hlc> {

    /** The smallest possible HLC; useful as a lower bound when scanning. */
    public static final Hlc MIN = new Hlc(Long.MIN_VALUE, 0, Integer.MIN_VALUE);

    public Hlc {
        if (logicalCounter < 0) {
            throw new IllegalArgumentException("logicalCounter must be non-negative: " + logicalCounter);
        }
    }

    @Override
    public int compareTo(Hlc o) {
        int c = Long.compare(physicalMillis, o.physicalMillis);
        if (c != 0) {
            return c;
        }
        c = Integer.compare(logicalCounter, o.logicalCounter);
        if (c != 0) {
            return c;
        }
        return Integer.compare(nodeId, o.nodeId);
    }

    /** Returns whether this timestamp is strictly later than {@code other} under full LWW ordering. */
    public boolean isAfter(Hlc other) {
        return compareTo(other) > 0;
    }
}
