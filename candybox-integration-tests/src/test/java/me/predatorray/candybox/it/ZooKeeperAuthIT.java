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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import me.predatorray.candybox.common.SystemClock;
import me.predatorray.candybox.coordination.zk.ZkAuth;
import me.predatorray.candybox.coordination.zk.ZooKeeperCoordinationService;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * ZooKeeper digest authentication + znode ACLs against a live embedded ZooKeeper: an
 * authenticated {@link ZooKeeperCoordinationService} with {@code aclEnabled} stamps
 * {@code CREATOR_ALL} on what it creates, a same-identity client can keep working with the data,
 * and an unauthenticated client is locked out.
 */
class ZooKeeperAuthIT {

    private static TestingServer zookeeper;

    @BeforeAll
    static void startZooKeeper() throws Exception {
        zookeeper = new TestingServer(true);
    }

    @AfterAll
    static void stopZooKeeper() throws Exception {
        if (zookeeper != null) {
            zookeeper.close();
        }
    }

    @Test
    void digestAuthWithAclsLocksOutOtherIdentities() throws Exception {
        ZkAuth auth = new ZkAuth("digest", "candybox:sekret", true);
        try (ZooKeeperCoordinationService service = new ZooKeeperCoordinationService(
                zookeeper.getConnectString(), SystemClock.INSTANCE, auth)) {
            service.create("acls/secured-box", "owner=User:alice\n".getBytes(StandardCharsets.UTF_8));
            assertThat(service.get("acls/secured-box")).isPresent();

            // The znode carries CREATOR_ALL (a single digest ACL for our identity).
            try (CuratorFramework raw = authedClient("candybox:sekret")) {
                java.util.List<ACL> acls = raw.getACL().forPath("/candybox/acls/secured-box");
                assertThat(acls).hasSize(1);
                assertThat(acls.get(0).getId().getScheme()).isEqualTo("digest");
                assertThat(acls.get(0).getPerms()).isEqualTo(ZooDefs.Perms.ALL);
            }

            // An unauthenticated client can see the node exists but cannot read or modify it ...
            try (CuratorFramework anonymous = anonymousClient()) {
                assertThatThrownBy(() -> anonymous.getData().forPath("/candybox/acls/secured-box"))
                        .isInstanceOf(KeeperException.NoAuthException.class);
                assertThatThrownBy(() -> anonymous.setData()
                        .forPath("/candybox/acls/secured-box", new byte[0]))
                        .isInstanceOf(KeeperException.NoAuthException.class);
            }

            // ... while a wrong digest identity is equally locked out.
            try (CuratorFramework wrongIdentity = authedClient("mallory:guess")) {
                assertThatThrownBy(() -> wrongIdentity.getData()
                        .forPath("/candybox/acls/secured-box"))
                        .isInstanceOf(KeeperException.NoAuthException.class);
            }

            // A second service with the SAME identity interoperates (the shared-identity rule).
            try (ZooKeeperCoordinationService peer = new ZooKeeperCoordinationService(
                    zookeeper.getConnectString(), SystemClock.INSTANCE, auth)) {
                assertThat(peer.get("acls/secured-box")).isPresent();
                long version = peer.get("acls/secured-box").orElseThrow().version();
                peer.compareAndSet("acls/secured-box",
                        "owner=User:bob\n".getBytes(StandardCharsets.UTF_8), version);
            }
        }
    }

    @Test
    void withoutAclsAuthenticatedNodesStayOpen() throws Exception {
        ZkAuth auth = new ZkAuth("digest", "candybox:sekret", false);
        try (ZooKeeperCoordinationService service = new ZooKeeperCoordinationService(
                zookeeper.getConnectString(), SystemClock.INSTANCE, auth)) {
            service.create("open-key", new byte[] {1});
            try (CuratorFramework anonymous = anonymousClient()) {
                assertThat(anonymous.getData().forPath("/candybox/open-key")).containsExactly(1);
            }
        }
    }

    private static CuratorFramework authedClient(String credentials) {
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(zookeeper.getConnectString())
                .retryPolicy(new ExponentialBackoffRetry(100, 3))
                .authorization("digest", credentials.getBytes(StandardCharsets.UTF_8))
                .build();
        client.start();
        return client;
    }

    private static CuratorFramework anonymousClient() {
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(zookeeper.getConnectString())
                .retryPolicy(new ExponentialBackoffRetry(100, 3))
                .build();
        client.start();
        return client;
    }
}
