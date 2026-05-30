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
 * Generates monotonically increasing {@link Hlc} timestamps for a single node.
 *
 * <p>The owning node stamps the HLC at write ingest (never the client, so client clock skew is
 * irrelevant). Two operations matter:
 *
 * <ul>
 *   <li>{@link #tick()} — issue the next timestamp for a locally originated write. The result is
 *       strictly greater (under HLC ordering ignoring nodeId) than every timestamp this clock has
 *       previously issued or observed, even if the wall clock has regressed.</li>
 *   <li>{@link #observe(Hlc)} — fold in a timestamp seen during recovery (WAL/manifest replay on
 *       ownership handover). This is the critical step for LWW correctness: the new owner must
 *       advance past the maximum durable HLC <em>before</em> stamping anything, otherwise it could
 *       stamp a genuinely newer write with a lower HLC and the read path would silently drop it or
 *       resurrect a deleted Candy.</li>
 * </ul>
 *
 * <p>This class is thread-safe; {@link #tick()} and {@link #observe(Hlc)} are mutually serialized.
 */
public final class HybridLogicalClock {

    private final int nodeId;
    private final Clock clock;
    private final long maxAcceptableSkewMillis;

    // The (physical, logical) of the most recent timestamp issued or observed. Guarded by {@code this}.
    private long physical;
    private int logical;

    /**
     * @param nodeId                  this node's id, used as the LWW tiebreaker on every stamp
     * @param clock                   the physical clock (untrusted for ordering)
     * @param maxAcceptableSkewMillis reject an observed timestamp whose physical component leads the
     *                                local wall clock by more than this (guards against a remote with a
     *                                wildly fast clock dragging this node's clock far into the future)
     */
    public HybridLogicalClock(int nodeId, Clock clock, long maxAcceptableSkewMillis) {
        if (maxAcceptableSkewMillis < 0) {
            throw new IllegalArgumentException("maxAcceptableSkewMillis must be non-negative");
        }
        this.nodeId = nodeId;
        this.clock = clock;
        this.maxAcceptableSkewMillis = maxAcceptableSkewMillis;
        this.physical = Long.MIN_VALUE;
        this.logical = 0;
    }

    /** This node's id. */
    public int nodeId() {
        return nodeId;
    }

    /**
     * Issues the next timestamp for a locally originated event.
     *
     * @return a fresh {@link Hlc} strictly after all previously issued/observed timestamps
     */
    public synchronized Hlc tick() {
        long pt = clock.currentTimeMillis();
        if (pt > physical) {
            physical = pt;
            logical = 0;
        } else {
            // Wall clock did not advance past our logical position; keep ordering by bumping logical.
            if (logical == Integer.MAX_VALUE) {
                // Astronomically unlikely (≈2.1e9 events in one ms). Spill into the next millisecond.
                physical++;
                logical = 0;
            } else {
                logical++;
            }
        }
        return new Hlc(physical, logical, nodeId);
    }

    /**
     * Folds an observed timestamp into this clock so that the next {@link #tick()} is strictly after
     * it. Safe to call repeatedly during replay.
     *
     * @param observed a timestamp recovered from durable state (WAL/manifest) or a peer
     * @throws IllegalStateException if {@code observed}'s physical component leads the local wall clock
     *                               by more than the configured skew bound
     */
    public synchronized void observe(Hlc observed) {
        long localPhysical = clock.currentTimeMillis();
        if (observed.physicalMillis() - localPhysical > maxAcceptableSkewMillis) {
            throw new IllegalStateException("Observed HLC physical time " + observed.physicalMillis()
                    + " leads local clock " + localPhysical + " by more than the accepted skew of "
                    + maxAcceptableSkewMillis + "ms");
        }
        if (observed.physicalMillis() > physical
                || (observed.physicalMillis() == physical && observed.logicalCounter() > logical)) {
            physical = observed.physicalMillis();
            logical = observed.logicalCounter();
        }
    }

    /** Returns the most recently issued/observed timestamp without advancing, for inspection. */
    public synchronized Hlc peek() {
        return new Hlc(physical, logical, nodeId);
    }
}
