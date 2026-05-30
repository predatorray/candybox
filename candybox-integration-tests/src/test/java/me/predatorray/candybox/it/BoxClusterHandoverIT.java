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

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import me.predatorray.candybox.bookkeeper.bk.BookKeeperLedgerStore;
import me.predatorray.candybox.client.CandyboxClient;
import me.predatorray.candybox.common.BoxName;
import me.predatorray.candybox.common.SystemClock;
import me.predatorray.candybox.common.config.CandyboxConfig;
import me.predatorray.candybox.coordination.zk.ZooKeeperCoordinationService;
import me.predatorray.candybox.protocol.FrameCodec;
import me.predatorray.candybox.protocol.transport.TcpTransport;
import me.predatorray.candybox.protocol.transport.TcpTransportServer;
import me.predatorray.candybox.server.CandyboxNode;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The full Phase 2 path over real TCP, embedded BookKeeper, and in-process ZooKeeper: a cluster-aware
 * client creates a Box and writes through it, the owning node fails over to a second node, and a
 * subsequent client read is transparently re-routed to the new owner (via a {@code MOVED} response)
 * and returns the recovered value.
 */
class BoxClusterHandoverIT {

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

    private static int freePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private ZooKeeperCoordinationService coordination() {
        return new ZooKeeperCoordinationService(zookeeper.getConnectString(), SystemClock.INSTANCE);
    }

    private BookKeeperLedgerStore store() {
        return new BookKeeperLedgerStore(bookKeeper.clientConfiguration(), bytes("candybox"));
    }

    @Test
    void clientReRoutesToTheNewOwnerAfterFailover() {
        CandyboxConfig config = CandyboxConfig.builder().leaseRenewIntervalMillis(0).build();
        BoxName box = BoxName.of("cluster-box");

        int portA = freePort();
        int portB = freePort();
        BookKeeperLedgerStore storeA = store();
        BookKeeperLedgerStore storeB = store();
        ZooKeeperCoordinationService coordA = coordination();
        ZooKeeperCoordinationService coordB = coordination();
        ZooKeeperCoordinationService coordClient = coordination();

        CandyboxNode nodeA = new CandyboxNode(1, config, storeA, coordA, SystemClock.INSTANCE,
                "127.0.0.1:" + portA);
        CandyboxNode nodeB = new CandyboxNode(2, config, storeB, coordB, SystemClock.INSTANCE,
                "127.0.0.1:" + portB);
        TcpTransportServer serverA = new TcpTransportServer(portA, nodeA.requestHandler(), new FrameCodec());
        TcpTransportServer serverB = new TcpTransportServer(portB, nodeB.requestHandler(), new FrameCodec());

        TcpTransport transport = new TcpTransport();
        CandyboxClient client = new CandyboxClient(transport, coordClient, config);
        try {
            // createBox lands on a node (node 1, lowest member) which acquires ownership.
            client.createBox(box.value());
            client.putCandy(box.value(), "k", bytes("v1"), "text/plain", Map.of(), null);
            assertThat(client.getCandy(box.value(), "k")).isEqualTo(bytes("v1"));

            // Fail the current owner over to node 2.
            nodeA.releaseBox(box);
            nodeB.openBox(box);

            // The client still has node 1 cached; node 1 replies MOVED(2), the client re-routes to
            // node 2 and reads the recovered value.
            assertThat(client.getCandy(box.value(), "k")).isEqualTo(bytes("v1"));
        } finally {
            client.close();
            transport.close();
            serverA.close();
            serverB.close();
            nodeA.close();
            nodeB.close();
            coordA.close();
            coordB.close();
            coordClient.close();
            storeA.close();
            storeB.close();
        }
    }
}
