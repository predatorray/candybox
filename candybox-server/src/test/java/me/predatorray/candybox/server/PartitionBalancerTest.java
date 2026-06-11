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

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import me.predatorray.candybox.bookkeeper.fake.InMemoryLedgerStore;
import me.predatorray.candybox.common.BoxName;
import me.predatorray.candybox.common.CandyKey;
import me.predatorray.candybox.common.ManualClock;
import me.predatorray.candybox.common.Partitioning;
import me.predatorray.candybox.common.config.CandyboxConfig;
import me.predatorray.candybox.coordination.CandyboxKeys;
import me.predatorray.candybox.coordination.LeaseInfo;
import me.predatorray.candybox.coordination.fake.InMemoryCoordinationService;
import org.junit.jupiter.api.Test;

/**
 * Drives the {@link PartitionBalancer} deterministically (the scheduler is disabled; rounds run via
 * {@link CandyboxNode#runBalancerOnce()}): elected-coordinator assignment, even spread, the
 * per-round move rate limit, stickiness once balanced, dead-node failover, and the deleted-Box
 * sweep.
 */
class PartitionBalancerTest {

    private static CandyboxConfig config(int maxMovesPerRound) {
        return CandyboxConfig.builder()
                .leaseRenewIntervalMillis(0)
                .balancerMaxMovesPerRound(maxMovesPerRound)
                .build();
    }

    private static int ownerOf(InMemoryCoordinationService coordination, String box, int partition) {
        return coordination.leaseHolder(CandyboxKeys.ownerResource(box, partition))
                .map(LeaseInfo::ownerNodeId)
                .orElse(-1);
    }

    private static long countOwnedBy(InMemoryCoordinationService coordination, String box,
                                     int partitions, int nodeId) {
        long owned = 0;
        for (int p = 0; p < partitions; p++) {
            if (ownerOf(coordination, box, p) == nodeId) {
                owned++;
            }
        }
        return owned;
    }

    @Test
    void spreadsPartitionsEvenlyAcrossNodesAndStaysSticky() {
        ManualClock clock = new ManualClock(1_000);
        InMemoryLedgerStore store = new InMemoryLedgerStore();
        InMemoryCoordinationService coordination = new InMemoryCoordinationService(clock);
        BoxName box = BoxName.of("spread-box");

        CandyboxNode nodeA = new CandyboxNode(1, config(8), store, coordination, clock);
        CandyboxNode nodeB = new CandyboxNode(2, config(8), store, coordination, clock);
        try {
            nodeA.createBox(box, 4); // all 4 partitions start on node 1
            assertThat(countOwnedBy(coordination, box.value(), 4, 1)).isEqualTo(4);

            // One round on each node: the coordinator (node 1) publishes a 2/2 split and releases
            // its overflow; node 2 then acquires the freed partitions.
            nodeA.runBalancerOnce();
            nodeB.runBalancerOnce();

            assertThat(countOwnedBy(coordination, box.value(), 4, 1)).isEqualTo(2);
            assertThat(countOwnedBy(coordination, box.value(), 4, 2)).isEqualTo(2);

            // Balanced state is sticky: further rounds change nothing.
            int[] before = new int[4];
            for (int p = 0; p < 4; p++) {
                before[p] = ownerOf(coordination, box.value(), p);
            }
            nodeA.runBalancerOnce();
            nodeB.runBalancerOnce();
            for (int p = 0; p < 4; p++) {
                assertThat(ownerOf(coordination, box.value(), p)).isEqualTo(before[p]);
            }

            // The moved partitions still serve their data from the new owner.
            for (int p = 0; p < 4; p++) {
                CandyboxNode owner = ownerOf(coordination, box.value(), p) == 1 ? nodeA : nodeB;
                String key = keyInPartition(p, 4);
                owner.engine(box, key).putCandy(CandyKey.of(key),
                        "v".getBytes(StandardCharsets.UTF_8), null, Map.of(), null);
                assertThat(owner.engine(box, key).getCandy(CandyKey.of(key))).isNotEmpty();
            }
        } finally {
            nodeA.close();
            nodeB.close();
            store.close();
        }
    }

    @Test
    void migrationAwayFromLiveOwnersIsRateLimitedPerRound() {
        ManualClock clock = new ManualClock(1_000);
        InMemoryLedgerStore store = new InMemoryLedgerStore();
        InMemoryCoordinationService coordination = new InMemoryCoordinationService(clock);
        BoxName box = BoxName.of("rate-box");

        CandyboxNode nodeA = new CandyboxNode(1, config(1), store, coordination, clock);
        CandyboxNode nodeB = new CandyboxNode(2, config(1), store, coordination, clock);
        try {
            nodeA.createBox(box, 6);

            // Round 1: capacity is 3, node 1 is 3 over, but only 1 move is allowed per round.
            nodeA.runBalancerOnce();
            nodeB.runBalancerOnce();
            assertThat(countOwnedBy(coordination, box.value(), 6, 2)).isEqualTo(1);

            // Convergence proceeds one partition per round.
            nodeA.runBalancerOnce();
            nodeB.runBalancerOnce();
            assertThat(countOwnedBy(coordination, box.value(), 6, 2)).isEqualTo(2);

            nodeA.runBalancerOnce();
            nodeB.runBalancerOnce();
            assertThat(countOwnedBy(coordination, box.value(), 6, 2)).isEqualTo(3);
            assertThat(countOwnedBy(coordination, box.value(), 6, 1)).isEqualTo(3);
        } finally {
            nodeA.close();
            nodeB.close();
            store.close();
        }
    }

    @Test
    void deadNodesPartitionsFailOverWithoutRateLimit() {
        ManualClock clock = new ManualClock(1_000);
        InMemoryLedgerStore store = new InMemoryLedgerStore();
        InMemoryCoordinationService coordination = new InMemoryCoordinationService(clock);
        BoxName box = BoxName.of("failover-box");

        CandyboxNode nodeA = new CandyboxNode(1, config(1), store, coordination, clock);
        nodeA.createBox(box, 4);
        nodeA.engine(box, "k").putCandy(CandyKey.of("k"), "v".getBytes(StandardCharsets.UTF_8),
                null, Map.of(), null);
        nodeA.close(); // releases all leases and unregisters from membership

        CandyboxNode nodeB = new CandyboxNode(2, config(1), store, coordination, clock);
        try {
            // Failover is not a "move": all 4 unowned partitions reassign in a single round even
            // though the move limit is 1.
            nodeB.runBalancerOnce();
            assertThat(countOwnedBy(coordination, box.value(), 4, 2)).isEqualTo(4);
            // The takeover recovered the prior owner's data.
            assertThat(nodeB.engine(box, "k").getCandy(CandyKey.of("k")))
                    .isEqualTo("v".getBytes(StandardCharsets.UTF_8));
        } finally {
            nodeB.close();
            store.close();
        }
    }

    @Test
    void ownersDropPartitionsOfDeletedBoxes() {
        ManualClock clock = new ManualClock(1_000);
        InMemoryLedgerStore store = new InMemoryLedgerStore();
        InMemoryCoordinationService coordination = new InMemoryCoordinationService(clock);
        BoxName box = BoxName.of("doomed-box");

        CandyboxNode nodeA = new CandyboxNode(1, config(8), store, coordination, clock);
        CandyboxNode nodeB = new CandyboxNode(2, config(8), store, coordination, clock);
        try {
            nodeA.createBox(box, 2);
            // A force-delete from a node that owns nothing: the descriptor goes away; node 1's
            // partitions are cleaned up by its own next balancer round (the sweep).
            nodeB.deleteBox(box, true);
            assertThat(nodeB.boxExists(box)).isFalse();

            nodeA.runBalancerOnce();
            assertThat(nodeA.ownedBoxStats()).isEmpty();
            assertThat(coordination.get(CandyboxKeys.manifestKey(box.value(), 0))).isEmpty();
            assertThat(coordination.get(CandyboxKeys.manifestKey(box.value(), 1))).isEmpty();
        } finally {
            nodeA.close();
            nodeB.close();
            store.close();
        }
    }

    /** Finds a key that hashes to {@code partition} under {@code count} partitions. */
    private static String keyInPartition(int partition, int count) {
        for (int i = 0; i < 10_000; i++) {
            String candidate = "key-" + i;
            if (Partitioning.partitionOf(candidate, count) == partition) {
                return candidate;
            }
        }
        throw new AssertionError("no key found for partition " + partition);
    }
}
