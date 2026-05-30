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
package me.predatorray.candybox.coordination;

/**
 * A time-bounded ownership grant over a named resource (a Box's ownership, a compaction task). The
 * holder is the leader/owner only while {@link #isValid()} and must {@link #renew()} before the TTL
 * elapses.
 *
 * <p>{@link #fencingToken()} is the crux of Candybox's safety: it is monotonically increasing across
 * successive acquisitions of the same resource, so a later owner always has a strictly higher token.
 * Every state-mutating manifest append and every compaction/GC commit carries this token and is
 * rejected if a higher token has since been issued — fencing out a zombie owner whose lease lapsed
 * but who has not yet noticed.
 */
public interface Lease {

    /** The resource this lease grants ownership of. */
    String resource();

    /** The node that holds the lease. */
    int ownerNodeId();

    /** The monotonically increasing fencing token for this acquisition. */
    long fencingToken();

    /** Whether the lease is currently held (not expired, released, or superseded by a newer holder). */
    boolean isValid();

    /**
     * Extends the lease by its TTL from now.
     *
     * @throws LeaseExpiredException if the lease has already been lost and cannot be renewed
     */
    void renew();

    /** Voluntarily relinquishes the lease. Idempotent. */
    void release();
}
