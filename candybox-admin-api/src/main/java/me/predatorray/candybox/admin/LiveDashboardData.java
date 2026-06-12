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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import me.predatorray.candybox.client.BoxClient;
import me.predatorray.candybox.client.CandyboxClient;
import me.predatorray.candybox.coordination.BoxDescriptor;
import me.predatorray.candybox.coordination.CandyboxKeys;
import me.predatorray.candybox.coordination.CoordinationService;
import me.predatorray.candybox.coordination.LeaseInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Production implementation of {@link DashboardData}. Fans out reads across the cluster's
 * {@link CoordinationService} (for membership + lease holders) and a {@link BoxClient} (for box /
 * candy listings). Mirrors the gateway's wiring — see {@code S3GatewayMain} — so the same SPI
 * shapes power both.
 *
 * <p>The {@code BoxClient} seam (implemented in production by {@link CandyboxClient}) lets the
 * unit tests substitute a hand-written fake without standing up a Transport or coordination
 * backend; same precedent as {@code CandyStore} / {@code FakeCandyStore} in
 * {@code candybox-s3-gateway}.
 *
 * <p>All methods are blocking but bounded: the v1 polling cadence (5 s) and small cluster sizes
 * make a fan-out per request acceptable. If a node is unreachable, that single row falls back to
 * "not ready" rather than failing the whole response.
 */
public final class LiveDashboardData implements DashboardData {

    private static final Logger LOG = LoggerFactory.getLogger(LiveDashboardData.class);

    private final CoordinationService coordination;
    private final BoxClient client;

    public LiveDashboardData(CoordinationService coordination, BoxClient client) {
        this.coordination = coordination;
        this.client = client;
    }

    @Override
    public ClusterSnapshot cluster() {
        List<String> boxes = safeListBoxes();
        Map<Integer, Integer> ownedCount = new HashMap<>(); // owned *partitions* per node
        List<String> ownerless = new ArrayList<>(); // boxes with at least one ownerless partition
        for (String box : boxes) {
            boolean allOwned = true;
            for (int p = 0; p < partitionCountOf(box); p++) {
                Optional<LeaseInfo> holder =
                        coordination.leaseHolder(CandyboxKeys.ownerResource(box, p));
                if (holder.isPresent()) {
                    ownedCount.merge(holder.get().ownerNodeId(), 1, Integer::sum);
                } else {
                    allOwned = false;
                }
            }
            if (!allOwned) {
                ownerless.add(box);
            }
        }

        List<NodeInfo> nodes = new ArrayList<>();
        for (Integer nodeId : coordination.members()) {
            String address = coordination.memberInfo(nodeId)
                    .map(b -> new String(b, StandardCharsets.UTF_8))
                    .orElse("");
            // Without a per-node ready probe in v1 we assume "registered ⇒ ready". A future
            // enhancement could hit the node's /healthz on the admin port; intentionally
            // deferred so this commit doesn't introduce a new HTTP fan-out per request.
            nodes.add(new NodeInfo(String.valueOf(nodeId), address, true,
                    ownedCount.getOrDefault(nodeId, 0)));
        }
        return new ClusterSnapshot(nodes, boxes.size(), ownerless, false);
    }

    @Override
    public List<BoxSummary> boxes() {
        List<String> boxes = safeListBoxes();
        List<BoxSummary> rows = new ArrayList<>(boxes.size());
        for (String box : boxes) {
            rows.add(BoxSummary.minimal(box, ownerOf(box)));
        }
        return rows;
    }

    @Override
    public BoxSummary box(String name) {
        // headBox confirms the box has a live owner; without it we'd happily return rows for
        // garbage-collected names that still echo in some node's ListBoxesRequest cache.
        if (!client.headBox(name)) {
            return null;
        }
        return BoxSummary.minimal(name, ownerOf(name));
    }

    @Override
    public List<LsmRow> lsm() {
        List<String> boxes = safeListBoxes();
        List<LsmRow> rows = new ArrayList<>(boxes.size());
        for (String box : boxes) {
            for (int p = 0; p < partitionCountOf(box); p++) {
                Optional<LeaseInfo> holder =
                        coordination.leaseHolder(CandyboxKeys.ownerResource(box, p));
                String owner = holder.map(h -> String.valueOf(h.ownerNodeId())).orElse(null);
                long fencing = holder.map(LeaseInfo::fencingToken).orElse(-1L);
                // TODO(phase-5.5): the manifest pointer's coordination version is a proxy for
                // "manifest revision" — bumps every CAS on the pointer, so it's monotonic per
                // partition. Per-partition runtime fields (ledger counts, compactions, GC) require
                // a new per-node JSON endpoint on HealthServer to fan out against; deferred to keep
                // this commit contained to the admin API + UI.
                long manifestVersion = coordination.get(CandyboxKeys.manifestKey(box, p))
                        .map(v -> v.version())
                        .orElse(-1L);
                rows.add(LsmRow.coordinationOnly(box + "/" + p, owner, manifestVersion, fencing));
            }
        }
        return rows;
    }

    @Override
    public void createBox(String name) {
        client.createBox(name);
    }

    @Override
    public void deleteBox(String name, boolean force) {
        client.deleteBox(name, force);
    }

    @Override
    public void putCandy(String box, String key, byte[] data, String contentType) {
        // The dashboard does no de-dup itself; a null idempotency token lets the server treat each
        // upload as a fresh write — matching how the S3 gateway calls putCandy. User-metadata is
        // empty for now; the v1 dashboard form has no field for it.
        client.putCandy(box, key, data, contentType, java.util.Map.of(), null);
    }

    @Override
    public void deleteCandy(String box, String key) {
        client.deleteCandy(box, key);
    }

    @Override
    public CandyListing candies(String name, String prefix, String startAfter, int maxKeys) {
        int safeMax = (maxKeys <= 0 || maxKeys > 1000) ? 100 : maxKeys;
        CandyboxClient.Listing listing = client.listCandies(name, prefix, startAfter, safeMax);
        List<CandyRow> rows = new ArrayList<>(listing.entries().size());
        for (CandyboxClient.Listing.Entry e : listing.entries()) {
            rows.add(new CandyRow(e.key(), e.contentLength(), e.createdAtMillis()));
        }
        return new CandyListing(rows, listing.nextStartAfter());
    }

    // ---- helpers -------------------------------------------------------------------------------

    private List<String> safeListBoxes() {
        try {
            return client.listBoxes();
        } catch (RuntimeException e) {
            // Whole-cluster outage shouldn't 500 the dashboard — the page still has value showing
            // membership only, so log + degrade.
            LOG.warn("listBoxes failed; returning empty box list: {}", e.toString());
            return List.of();
        }
    }

    /** The Box's partition count from its descriptor, or 0 if the descriptor is missing. */
    private int partitionCountOf(String box) {
        return coordination.get(CandyboxKeys.boxMetaKey(box))
                .map(v -> BoxDescriptor.decode(v.value()).partitionCount())
                .orElse(0);
    }

    /**
     * Summarizes who owns a Box: the single node id when one node owns every partition, a
     * comma-joined list when ownership is spread, {@code null} when no partition has an owner.
     */
    private String ownerOf(String box) {
        java.util.TreeSet<Integer> owners = new java.util.TreeSet<>();
        for (int p = 0; p < partitionCountOf(box); p++) {
            coordination.leaseHolder(CandyboxKeys.ownerResource(box, p))
                    .ifPresent(h -> owners.add(h.ownerNodeId()));
        }
        if (owners.isEmpty()) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        for (Integer owner : owners) {
            if (sb.length() > 0) {
                sb.append(',');
            }
            sb.append(owner);
        }
        return sb.toString();
    }
}
