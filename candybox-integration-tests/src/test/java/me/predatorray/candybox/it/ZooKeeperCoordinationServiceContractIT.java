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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import me.predatorray.candybox.common.Clock;
import me.predatorray.candybox.coordination.CoordinationService;
import me.predatorray.candybox.coordination.CoordinationServiceContract;
import me.predatorray.candybox.coordination.zk.ZooKeeperCoordinationService;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

/**
 * Runs the shared {@link CoordinationServiceContract} against the real {@link
 * ZooKeeperCoordinationService} on an in-process ZooKeeper ({@link TestingServer}) — the same suite
 * the in-memory fake passes, proving the fake is a faithful stand-in for ownership/fencing tests.
 *
 * <p>Each contract test gets a Curator client on a unique namespace so state is isolated even though
 * the embedded ZooKeeper is shared across the class.
 */
class ZooKeeperCoordinationServiceContractIT extends CoordinationServiceContract {

    private static TestingServer zookeeper;
    private static final AtomicInteger NAMESPACE_SEQ = new AtomicInteger();
    private static final List<CuratorFramework> CLIENTS = new ArrayList<>();

    @BeforeAll
    static void startZooKeeper() throws Exception {
        zookeeper = new TestingServer(true);
    }

    @AfterAll
    static void stopZooKeeper() throws Exception {
        for (CuratorFramework client : CLIENTS) {
            client.close();
        }
        CLIENTS.clear();
        if (zookeeper != null) {
            zookeeper.close();
        }
    }

    @Override
    protected CoordinationService newService(Clock clock) {
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(zookeeper.getConnectString())
                .namespace("ct-" + NAMESPACE_SEQ.incrementAndGet())
                .retryPolicy(new ExponentialBackoffRetry(100, 3))
                .sessionTimeoutMs(15_000)
                .connectionTimeoutMs(15_000)
                .build();
        client.start();
        try {
            client.blockUntilConnected();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted connecting to embedded ZooKeeper", e);
        }
        CLIENTS.add(client);
        return new ZooKeeperCoordinationService(client, clock);
    }
}
