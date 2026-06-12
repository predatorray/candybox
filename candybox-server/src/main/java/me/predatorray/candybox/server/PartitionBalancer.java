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
package me.predatorray.candybox.server;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import me.predatorray.candybox.common.BoxName;
import me.predatorray.candybox.common.config.CandyboxConfig;
import me.predatorray.candybox.coordination.BoxDescriptor;
import me.predatorray.candybox.coordination.CandyboxKeys;
import me.predatorray.candybox.coordination.CasConflictException;
import me.predatorray.candybox.coordination.CoordinationService;
import me.predatorray.candybox.coordination.Lease;
import me.predatorray.candybox.coordination.LeaseInfo;
import me.predatorray.candybox.coordination.VersionedValue;
import me.predatorray.candybox.server.PartitionAssignment.BoxPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Spreads partition ownership evenly across the cluster. Every node runs a balancing round on a
 * timer; whichever node holds the {@code cluster/balancer} lease acts as the <b>elected
 * coordinator</b> and publishes the desired assignment table, and <b>every</b> node (coordinator or
 * not) then converges on it: releasing partitions assigned elsewhere (with a pre-handover flush) and
 * acquiring partitions assigned to it once their lease is free.
 *
 * <p>The target computation is sticky and rate-limited:
 * <ul>
 *   <li>a live owner keeps its partitions up to the fair-share capacity
 *       (⌈partitions/members⌉) — no gratuitous shuffling;</li>
 *   <li>unowned partitions (a new Box, a dead node's partitions) are failover and are assigned to
 *       the least-loaded members without limit;</li>
 *   <li>at most {@link CandyboxConfig#balancerMaxMovesPerRound()} partitions are taken away from a
 *       <em>live</em> owner per round, so a node join migrates load gradually.</li>
 * </ul>
 *
 * <p>The table is advisory; safety always rests on the per-partition fenced lease. A move converges
 * over polling rounds: the old owner releases in one round, the new owner acquires once it observes
 * the lease free.
 */
final class PartitionBalancer {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionBalancer.class);

    private final CandyboxNode node;
    private final CoordinationService coordination;
    private final CandyboxConfig config;

    PartitionBalancer(CandyboxNode node, CoordinationService coordination, CandyboxConfig config) {
        this.node = node;
        this.coordination = coordination;
        this.config = config;
    }

    /** One balancing round: coordinate (if elected) then converge on the published assignment. */
    void runOnce() {
        try {
            node.sweepDeletedBoxes();
        } catch (RuntimeException e) {
            LOG.warn("Deleted-box sweep failed on node {}", node.nodeId(), e);
        }
        try {
            coordinateIfElected();
        } catch (RuntimeException e) {
            LOG.warn("Balancer coordination round failed on node {}", node.nodeId(), e);
        }
        try {
            applyAssignment();
        } catch (RuntimeException e) {
            LOG.warn("Balancer apply round failed on node {}", node.nodeId(), e);
        }
    }

    // ---- coordinator side --------------------------------------------------------------------

    private void coordinateIfElected() {
        Optional<Lease> lease = coordination.tryAcquireLease(CandyboxKeys.BALANCER_RESOURCE,
                node.nodeId(), config.ownershipLeaseTtlMillis());
        if (lease.isEmpty()) {
            return; // another node coordinates
        }
        List<Integer> members = coordination.members();
        List<BoxPartition> partitions = allPartitions();
        if (members.isEmpty() || partitions.isEmpty()) {
            return;
        }
        PartitionAssignment current = readAssignment().map(v -> PartitionAssignment.decode(v.value()))
                .orElse(PartitionAssignment.empty());
        PartitionAssignment target = computeTarget(partitions, members);
        if (!target.targets().equals(current.targets())) {
            publish(target);
        }
    }

    /** Every partition of every existing Box (descriptor present), in deterministic order. */
    private List<BoxPartition> allPartitions() {
        List<BoxPartition> all = new ArrayList<>();
        for (String boxName : coordination.children(CandyboxKeys.BOXES_ROOT)) {
            // A deleted Box can leave lease znodes behind; only the descriptor makes it real.
            Optional<VersionedValue> meta = coordination.get(CandyboxKeys.boxMetaKey(boxName));
            if (meta.isEmpty()) {
                continue;
            }
            int count = BoxDescriptor.decode(meta.get().value()).partitionCount();
            for (int p = 0; p < count; p++) {
                all.add(new BoxPartition(boxName, p));
            }
        }
        all.sort(BoxPartition::compareTo);
        return all;
    }

    private PartitionAssignment computeTarget(List<BoxPartition> partitions, List<Integer> members) {
        int capacity = (partitions.size() + members.size() - 1) / members.size();
        Map<Integer, Integer> load = new TreeMap<>();
        for (int member : members) {
            load.put(member, 0);
        }

        Map<BoxPartition, Integer> targets = new LinkedHashMap<>();
        List<BoxPartition> unowned = new ArrayList<>();
        Map<BoxPartition, Integer> overflow = new LinkedHashMap<>(); // candidate moves: bp -> holder

        for (BoxPartition bp : partitions) {
            Integer holder = liveHolder(bp);
            if (holder == null || !load.containsKey(holder)) {
                unowned.add(bp); // failover / brand new: not a "move", never rate-limited
            } else if (load.get(holder) < capacity) {
                targets.put(bp, holder); // sticky: keep the live owner within its fair share
                load.merge(holder, 1, Integer::sum);
            } else {
                overflow.put(bp, holder); // live owner above capacity: moving this away counts
            }
        }

        for (BoxPartition bp : unowned) {
            int member = leastLoaded(load);
            targets.put(bp, member);
            load.merge(member, 1, Integer::sum);
        }

        int movesLeft = config.balancerMaxMovesPerRound();
        for (Map.Entry<BoxPartition, Integer> e : overflow.entrySet()) {
            if (movesLeft > 0) {
                int member = leastLoaded(load);
                targets.put(e.getKey(), member);
                load.merge(member, 1, Integer::sum);
                movesLeft--;
            } else {
                // Rate limit reached: the live owner keeps it this round, even above capacity.
                targets.put(e.getKey(), e.getValue());
                load.merge(e.getValue(), 1, Integer::sum);
            }
        }
        return new PartitionAssignment(targets);
    }

    private Integer liveHolder(BoxPartition bp) {
        return coordination.leaseHolder(CandyboxKeys.ownerResource(bp.box(), bp.partition()))
                .map(LeaseInfo::ownerNodeId)
                .orElse(null);
    }

    private static int leastLoaded(Map<Integer, Integer> load) {
        int best = -1;
        int bestLoad = Integer.MAX_VALUE;
        for (Map.Entry<Integer, Integer> e : load.entrySet()) {
            if (e.getValue() < bestLoad) {
                best = e.getKey();
                bestLoad = e.getValue();
            }
        }
        return best;
    }

    private Optional<VersionedValue> readAssignment() {
        return coordination.get(CandyboxKeys.ASSIGNMENT_KEY);
    }

    private void publish(PartitionAssignment assignment) {
        try {
            Optional<VersionedValue> current = readAssignment();
            if (current.isEmpty()) {
                coordination.create(CandyboxKeys.ASSIGNMENT_KEY, assignment.encode());
            } else {
                coordination.compareAndSet(CandyboxKeys.ASSIGNMENT_KEY, assignment.encode(),
                        current.get().version());
            }
        } catch (CasConflictException raced) {
            LOG.debug("Assignment publish lost a race; will retry next round");
        }
    }

    // ---- every-node side ---------------------------------------------------------------------

    private void applyAssignment() {
        Optional<VersionedValue> stored = readAssignment();
        if (stored.isEmpty()) {
            return;
        }
        PartitionAssignment assignment = PartitionAssignment.decode(stored.get().value());
        List<Integer> members = coordination.members();

        // Release first, so our partitions free up for their new owners within this round.
        for (Map.Entry<BoxPartition, Integer> e : assignment.targets().entrySet()) {
            BoxPartition bp = e.getKey();
            int target = e.getValue();
            if (target != node.nodeId() && node.ownsPartition(bp.box(), bp.partition())
                    && members.contains(target)) {
                LOG.info("Node {} handing over box {} partition {} to node {}", node.nodeId(),
                        bp.box(), bp.partition(), target);
                node.releasePartitionForHandover(BoxName.of(bp.box()), bp.partition());
            }
        }
        for (Map.Entry<BoxPartition, Integer> e : assignment.targets().entrySet()) {
            BoxPartition bp = e.getKey();
            if (e.getValue() != node.nodeId() || node.ownsPartition(bp.box(), bp.partition())) {
                continue;
            }
            if (coordination.leaseHolder(CandyboxKeys.ownerResource(bp.box(), bp.partition()))
                    .isPresent()) {
                continue; // previous owner has not released/expired yet; retry next round
            }
            try {
                node.openPartition(BoxName.of(bp.box()), bp.partition());
            } catch (RuntimeException ex) {
                // Lost an acquire race, or the Box vanished concurrently; converge next round.
                LOG.info("Node {} could not take box {} partition {} yet: {}", node.nodeId(),
                        bp.box(), bp.partition(), ex.getMessage());
            }
        }
    }
}
