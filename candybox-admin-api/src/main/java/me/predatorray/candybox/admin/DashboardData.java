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
package me.predatorray.candybox.admin;

import java.util.List;

/**
 * The data seam between {@link AdminApiServer}'s HTTP routes and the rest of candybox. Keeps the
 * HTTP layer free of any knowledge of coordination, transport, or the LSM engine — the same shape
 * that {@code candybox-s3-gateway} uses via {@code CandyStore}.
 *
 * <p>In production this is implemented by {@link LiveDashboardData}, which fans out reads against
 * the cluster's {@code CoordinationService} + {@code CandyboxClient}. Tests substitute a
 * {@code FakeDashboardData} that builds the same record shapes from in-memory state, so route
 * behaviour can be asserted without standing up ZooKeeper or BookKeeper.
 *
 * <p>Each method is read-only and is expected to be safe to call from the JDK HttpServer's
 * dispatcher threads. Implementations should not block longer than the {@code /api/*} polling
 * interval (5 s) under healthy conditions — slow reads will starve the small default executor.
 */
public interface DashboardData {

    /** Snapshot of the cluster topology, with owned-box counts per node. */
    ClusterSnapshot cluster();

    /** All currently-known box names plus a one-line summary per box. */
    List<BoxSummary> boxes();

    /** Detailed view for one box, or {@code null} if the box has no known owner / does not exist. */
    BoxSummary box(String name);

    /**
     * Listing of objects in a box, paged by {@code startAfter}.
     *
     * @param name       the box
     * @param prefix     optional key prefix to narrow the listing (nullable / empty for full scan)
     * @param startAfter pagination cursor — pass back {@link CandyListing#nextStartAfter()} from
     *                   the previous page, or {@code null} for the first page
     * @param maxKeys    soft cap on the number of entries returned
     */
    CandyListing candies(String name, String prefix, String startAfter, int maxKeys);

    /**
     * Per-box LSM snapshot. v1 reads what's visible from outside the storage nodes — owner,
     * manifest version, fencing token. Per-node runtime fields (ledger counts, in-flight
     * compactions, GC backlog) require a new per-node JSON endpoint and are surfaced as {@code -1}
     * until that lands.
     */
    List<LsmRow> lsm();

    // ---- mutating ops (post-v1) ----------------------------------------------------------------
    //
    // These mirror the four operations the v1 plan deferred to a future commit. They are exposed
    // through the dashboard with no auth — the deploy assumption is the same trusted-network one
    // we already make for the s3 gateway, where the matching ops are already unauthenticated.
    // Implementations should throw {@link UnsupportedOperationException} when the backing surface
    // is not wired up (the stub mode) so callers can map it to a 503 rather than a silent 200.

    /** Creates a new (empty) Box. Implementations should report "already exists" as a failure. */
    default void createBox(String name) {
        throw new UnsupportedOperationException("createBox is not supported by this backend");
    }

    /**
     * Deletes a Box. {@code force} matches {@code CandyboxClient.deleteBox} semantics — when false
     * the server rejects the call if the Box still has Candies; when true the server is asked to
     * tear them down too.
     */
    default void deleteBox(String name, boolean force) {
        throw new UnsupportedOperationException("deleteBox is not supported by this backend");
    }

    /**
     * Uploads a Candy. {@code contentType} may be {@code null} ("application/octet-stream" is the
     * server-side default).
     */
    default void putCandy(String box, String key, byte[] data, String contentType) {
        throw new UnsupportedOperationException("putCandy is not supported by this backend");
    }

    /** Deletes one Candy. Implementations should treat "missing" as a success (idempotent). */
    default void deleteCandy(String box, String key) {
        throw new UnsupportedOperationException("deleteCandy is not supported by this backend");
    }

    /** One row of the LSM internals view. {@code -1} encodes "not yet exposed by the backend". */
    record LsmRow(String box, String owner, long manifestVersion, long fencingToken,
                  long sstableLedgerCount, long syrupLedgerCount, long walLedgerCount,
                  long inFlightCompactions, long gcBacklog) {

        public static LsmRow coordinationOnly(String box, String owner, long manifestVersion,
                                              long fencingToken) {
            return new LsmRow(box, owner, manifestVersion, fencingToken, -1, -1, -1, -1, -1);
        }
    }

    /**
     * Cluster snapshot. {@code stub} flags the bootstrap state where the admin API is running but
     * not yet wired to a coordination backend; the dashboard surfaces it as a hint.
     */
    record ClusterSnapshot(List<NodeInfo> nodes, int boxCount, List<String> ownerless, boolean stub) {
    }

    /** One row in the cluster overview. */
    record NodeInfo(String nodeId, String address, boolean ready, int ownedBoxCount) {
    }

    /**
     * One row of the box list / details. Optional fields ({@code -1} or {@code null}) mean "the
     * underlying source doesn't expose this yet"; the UI shows an em-dash for them.
     */
    record BoxSummary(String name, String owner, long candyCount, long sizeBytes,
                      long manifestVersion, long fencingToken, String hlc) {

        public static BoxSummary minimal(String name, String owner) {
            return new BoxSummary(name, owner, -1, -1, -1, -1, null);
        }
    }

    /** One page of a candy listing. */
    record CandyListing(List<CandyRow> entries, String nextStartAfter) {
    }

    /** One row of a candy listing. */
    record CandyRow(String key, long contentLength, long createdAtMillis) {
    }
}
