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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import me.predatorray.candybox.common.Clock;
import me.predatorray.candybox.common.SystemClock;
import me.predatorray.candybox.coordination.CoordinationService;
import me.predatorray.candybox.coordination.CoordinationServiceContract;
import me.predatorray.candybox.coordination.zk.ZooKeeperCoordinationService;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Runs the shared {@link CoordinationServiceContract} against the real {@link
 * ZooKeeperCoordinationService} on an in-process ZooKeeper ({@link TestingServer}) — the same suite
 * the in-memory fake passes, proving the fake is a faithful stand-in for ownership/fencing tests.
 *
 * <p>Each contract test gets a Curator client on a unique namespace so state is isolated even though
 * the embedded ZooKeeper is shared across the class. The extra tests cover ZooKeeper-specific session
 * semantics of membership, observed through a second client on the same namespace.
 */
class ZooKeeperCoordinationServiceContractIT extends CoordinationServiceContract {

    private static TestingServer zookeeper;
    private static final AtomicInteger NAMESPACE_SEQ = new AtomicInteger();
    private static final List<CuratorFramework> CLIENTS = new ArrayList<>();
    private static final Map<ZooKeeperCoordinationService, CuratorFramework> SERVICES = new HashMap<>();

    @BeforeAll
    static void startZooKeeper() throws Exception {
        zookeeper = new TestingServer(true);
    }

    @AfterAll
    static void stopZooKeeper() throws Exception {
        // Close member recipes before their clients, or Curator keeps retrying them on a closed client.
        for (ZooKeeperCoordinationService service : SERVICES.keySet()) {
            service.close();
        }
        SERVICES.clear();
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
        return new ZooKeeperCoordinationService(newClient("ct-" + NAMESPACE_SEQ.incrementAndGet()), clock);
    }

    private static CuratorFramework newClient(String namespace) {
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(zookeeper.getConnectString())
                .namespace(namespace)
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
        return client;
    }

    private static ZooKeeperCoordinationService service(CuratorFramework client) {
        ZooKeeperCoordinationService service = new ZooKeeperCoordinationService(client, SystemClock.INSTANCE);
        SERVICES.put(service, client);
        return service;
    }

    /** Ends the client's session for good and abandons it with its services, as a crashed process would. */
    private static void crash(CuratorFramework client) throws Exception {
        client.getZookeeperClient().getZooKeeper().close();
        CLIENTS.remove(client);
        SERVICES.values().removeIf(client::equals);
    }

    private static long sessionId(CuratorFramework client) throws Exception {
        return client.getZookeeperClient().getZooKeeper().getSessionId();
    }

    private static long memberOwner(CuratorFramework client, int nodeId) throws Exception {
        Stat stat = client.checkExists().forPath("/members/" + nodeId);
        return stat == null ? -1 : stat.getEphemeralOwner();
    }

    private static void await(String what, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + 30_000_000_000L;
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("Timed out waiting until " + what);
            }
            Thread.sleep(50);
        }
    }

    private static BooleanSupplier ownedBy(CuratorFramework observer, int nodeId, CuratorFramework owner) {
        return () -> {
            try {
                return memberOwner(observer, nodeId) == sessionId(owner);
            } catch (Exception e) {
                return false;
            }
        };
    }

    @Test
    void isConnectedFollowsTheServerGoingDownAndComingBack() throws Exception {
        // A private server: stopping the shared one would disturb the other tests.
        try (TestingServer server = new TestingServer(true);
             ZooKeeperCoordinationService service = new ZooKeeperCoordinationService(
                     server.getConnectString(), SystemClock.INSTANCE)) {
            assertThat(service.isConnected()).isTrue();

            server.stop();
            await("the service reports disconnected", () -> !service.isConnected());

            server.restart();
            await("the service reports reconnected", service::isConnected);
        }
    }

    @Test
    void crashedMemberDisappearsWithItsSession() throws Exception {
        String ns = "crash-" + NAMESPACE_SEQ.incrementAndGet();
        CuratorFramework crashing = newClient(ns);
        ZooKeeperCoordinationService member = service(crashing);
        ZooKeeperCoordinationService observer = service(newClient(ns));

        member.registerMember(7, "host-7:70".getBytes(StandardCharsets.UTF_8));
        assertThat(observer.members()).containsExactly(7);

        crash(crashing); // closing the handle ends the session at once; expiry is covered below
        await("the crashed member is gone", () -> observer.members().isEmpty());
    }

    @Test
    void memberIsRecreatedAfterSessionExpiry() throws Exception {
        String ns = "expiry-" + NAMESPACE_SEQ.incrementAndGet();
        CuratorFramework client = newClient(ns);
        CuratorFramework observer = newClient(ns);
        ZooKeeperCoordinationService member = service(client);
        member.registerMember(7, "host-7:70".getBytes(StandardCharsets.UTF_8));
        long firstSession = sessionId(client);
        assertThat(memberOwner(observer, 7)).isEqualTo(firstSession);

        client.getZookeeperClient().getZooKeeper().getTestable().injectSessionExpiration();

        await("the member is re-created under a new session", () -> {
            try {
                long owner = memberOwner(observer, 7);
                return owner > 0 && owner != firstSession && owner == sessionId(client);
            } catch (Exception e) {
                return false;
            }
        });
        assertThat(member.memberInfo(7).map(String::new)).contains("host-7:70");
    }

    @Test
    void restartedMemberTakesOverOnceItsPreviousSessionEnds() throws Exception {
        String ns = "restart-" + NAMESPACE_SEQ.incrementAndGet();
        CuratorFramework previous = newClient(ns);
        CuratorFramework restarted = newClient(ns);
        service(previous)
                .registerMember(7, "old:70".getBytes(StandardCharsets.UTF_8));

        ZooKeeperCoordinationService member = service(restarted);
        member.registerMember(7, "new:70".getBytes(StandardCharsets.UTF_8));
        crash(previous);

        await("the restarted node owns its member znode", ownedBy(restarted, 7, restarted));
        assertThat(member.memberInfo(7).map(String::new)).contains("new:70");
    }

    @Test
    void persistentMemberFromAnOlderVersionBecomesEphemeral() throws Exception {
        String ns = "legacy-" + NAMESPACE_SEQ.incrementAndGet();
        CuratorFramework client = newClient(ns);
        client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT)
                .forPath("/members/7", "old:70".getBytes(StandardCharsets.UTF_8));

        service(client)
                .registerMember(7, "new:70".getBytes(StandardCharsets.UTF_8));

        assertThat(memberOwner(client, 7)).isEqualTo(sessionId(client));
    }
}
