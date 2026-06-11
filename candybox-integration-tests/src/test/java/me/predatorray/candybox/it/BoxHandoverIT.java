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

import java.nio.charset.StandardCharsets;
import java.util.Map;
import me.predatorray.candybox.bookkeeper.bk.BookKeeperLedgerStore;
import me.predatorray.candybox.common.BoxName;
import me.predatorray.candybox.common.CandyKey;
import me.predatorray.candybox.common.SystemClock;
import me.predatorray.candybox.common.config.CandyboxConfig;
import me.predatorray.candybox.coordination.zk.ZooKeeperCoordinationService;
import me.predatorray.candybox.server.CandyboxNode;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * End-to-end Phase 2 WS3 handover on real backends: embedded BookKeeper for ledgers and an in-process
 * ZooKeeper for coordination. Node A creates a Box and writes (un-flushed) data; A relinquishes
 * ownership; Node B takes over by acquiring the lease, recovering the manifest + WAL from the
 * coordination-held pointer, advancing the pointer, and serving the recovered data.
 */
class BoxHandoverIT {

    private static EmbeddedBookKeeper bookKeeper;
    private static TestingServer zookeeper;

    @BeforeAll
    static void start() throws Exception {
        bookKeeper = new EmbeddedBookKeeper(3);
        zookeeper = new TestingServer(true);
    }

    @AfterAll
    static void stop() throws Exception {
        if (zookeeper != null) {
            zookeeper.close();
        }
        if (bookKeeper != null) {
            bookKeeper.close();
        }
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void ownershipMovesBetweenNodesOnRealBackends() {
        CandyboxConfig config = CandyboxConfig.builder().leaseRenewIntervalMillis(0).build();
        BoxName box = BoxName.of("handover-box");

        try (BookKeeperLedgerStore storeA = new BookKeeperLedgerStore(
                bookKeeper.clientConfiguration(),
                bytes("candybox"));
             BookKeeperLedgerStore storeB = new BookKeeperLedgerStore(
                     bookKeeper.clientConfiguration(),
                     bytes("candybox"));
             ZooKeeperCoordinationService coordA =
                     new ZooKeeperCoordinationService(zookeeper.getConnectString(), SystemClock.INSTANCE);
             ZooKeeperCoordinationService coordB =
                     new ZooKeeperCoordinationService(zookeeper.getConnectString(), SystemClock.INSTANCE);
             CandyboxNode nodeA = new CandyboxNode(1, config, storeA, coordA, SystemClock.INSTANCE);
             CandyboxNode nodeB = new CandyboxNode(2, config, storeB, coordB, SystemClock.INSTANCE)
        ) {
            nodeA.createBox(box, 1);
            nodeA.enginePartition(box, 0).putCandy(CandyKey.of("k"), bytes("v1"), "text/plain", Map.of(), null);

            // A relinquishes ownership (releases the lease, closes its engine).
            nodeA.releaseBox(box);

            // B takes over: acquires the lease, recovers the manifest + WAL, advances the pointer.
            nodeB.openBox(box);
            assertThat(nodeB.enginePartition(box, 0).getCandy(CandyKey.of("k"))).isEqualTo(bytes("v1"));
        }
    }
}
