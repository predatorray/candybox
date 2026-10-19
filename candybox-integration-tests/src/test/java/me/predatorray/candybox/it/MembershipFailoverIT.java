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
package me.predatorray.candybox.it;

import static org.assertj.core.api.Assertions.assertThat;

import me.predatorray.candybox.bookkeeper.fake.InMemoryLedgerStore;
import me.predatorray.candybox.common.BoxName;
import me.predatorray.candybox.common.SystemClock;
import me.predatorray.candybox.common.config.CandyboxConfig;
import me.predatorray.candybox.coordination.zk.ZooKeeperCoordinationService;
import me.predatorray.candybox.server.CandyboxNode;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Crash failover end to end on the real {@link ZooKeeperCoordinationService}: a node whose ZooKeeper
 * session ends without {@link CandyboxNode#close()} drops out of membership, and the survivor's
 * balancer takes over all of its partitions.
 */
class MembershipFailoverIT {

    private static TestingServer zookeeper;

    @BeforeAll
    static void start() throws Exception {
        zookeeper = new TestingServer(true);
    }

    @AfterAll
    static void stop() throws Exception {
        if (zookeeper != null) {
            zookeeper.close();
        }
    }

    private static CuratorFramework client() throws InterruptedException {
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(zookeeper.getConnectString())
                .namespace("candybox")
                .retryPolicy(new RetryOneTime(100))
                .sessionTimeoutMs(15_000)
                .connectionTimeoutMs(2_000)
                .build();
        client.start();
        client.blockUntilConnected();
        return client;
    }

    private static int ownedBy(CandyboxNode observer, BoxName box, int partitions, int nodeId) {
        int owned = 0;
        for (int p = 0; p < partitions; p++) {
            if (observer.currentOwner(box, p).orElse(-1) == nodeId) {
                owned++;
            }
        }
        return owned;
    }

    @Test
    void crashedNodesPartitionsFailOverToTheSurvivor() throws Exception {
        CandyboxConfig config = CandyboxConfig.builder()
                .ownershipLeaseTtlMillis(2_000)
                .leaseRenewIntervalMillis(500)
                .balancerMaxMovesPerRound(8)
                .build();
        BoxName box = BoxName.of("crash-box");
        InMemoryLedgerStore store = new InMemoryLedgerStore();
        CuratorFramework client1 = client();
        CuratorFramework client2 = client();
        ZooKeeperCoordinationService coord1 = new ZooKeeperCoordinationService(client1, SystemClock.INSTANCE);
        ZooKeeperCoordinationService coord2 = new ZooKeeperCoordinationService(client2, SystemClock.INSTANCE);
        CandyboxNode node1 = new CandyboxNode(1, config, store, coord1, SystemClock.INSTANCE);
        CandyboxNode node2 = new CandyboxNode(2, config, store, coord2, SystemClock.INSTANCE);
        try {
            node1.createBox(box, 4);
            for (int i = 0; i < 3; i++) {
                node1.runBalancerOnce();
                node2.runBalancerOnce();
            }
            assertThat(ownedBy(node1, box, 4, 2)).isEqualTo(2);

            // Node 2 "crashes": it never closes and can no longer renew. Closing the handle ends the
            // session at once instead of after the timeout; ZooKeeper's expiry gives the same end state.
            client2.getZookeeperClient().getZooKeeper().close();

            long deadline = System.nanoTime() + 30_000_000_000L;
            while (ownedBy(node1, box, 4, 1) < 4 && System.nanoTime() < deadline) {
                node1.runBalancerOnce();
                Thread.sleep(200);
            }
            assertThat(coord1.members()).containsExactly(1);
            assertThat(ownedBy(node1, box, 4, 1)).isEqualTo(4);
        } finally {
            node1.close();
            coord1.close();
            client1.close();
            try {
                node2.close();
            } catch (RuntimeException expected) {
                // its session is gone, so releasing its leases fails
            }
            coord2.close();
            client2.close();
            store.close();
        }
    }
}
