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
import java.time.Duration;
import java.util.Map;
import me.predatorray.candybox.bookkeeper.bk.BookKeeperLedgerStore;
import me.predatorray.candybox.client.CandyboxClient;
import me.predatorray.candybox.common.*;
import me.predatorray.candybox.common.config.CandyboxConfig;
import me.predatorray.candybox.common.exception.CandyNotFoundException;
import me.predatorray.candybox.common.namespace.NamespaceKeys;
import me.predatorray.candybox.common.namespace.NamespaceOperation;
import me.predatorray.candybox.coordination.zk.ZooKeeperCoordinationService;
import me.predatorray.candybox.protocol.FrameCodec;
import me.predatorray.candybox.protocol.transport.*;
import me.predatorray.candybox.server.CandyboxNode;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.Test;

class NamespaceLifecycleIT {
    @Test void nativeLifecycleSurvivesTcpOwnerHandoverAndRejectsStaleExpiry() throws Exception {
        var clock = new ManualClock(1000);
        var config = CandyboxConfig.builder().routerUnownedRetryMillis(0).leaseRenewIntervalMillis(0).build();
        try (var bookies = new EmbeddedBookKeeper(3); var zk = new TestingServer(true);
             var ledgers = new BookKeeperLedgerStore(bookies.clientConfiguration(), "candybox".getBytes(StandardCharsets.UTF_8));
             var ownerCoord = new ZooKeeperCoordinationService(zk.getConnectString(), clock);
             var nextCoord = new ZooKeeperCoordinationService(zk.getConnectString(), clock);
             var clientCoord = new ZooKeeperCoordinationService(zk.getConnectString(), clock);
             var node = new CandyboxNode(1, config, ledgers, ownerCoord, clock, "127.0.0.1:1");
             var next = new CandyboxNode(2, config, ledgers, nextCoord, clock, "127.0.0.1:2");
             var server = server(node); var nextServer = server(next);
             var transport = new TcpTransport(new FrameCodec(), null, true, 1000, 2000)) {
            ownerCoord.unregisterMember(1); ownerCoord.registerMember(1, ("127.0.0.1:" + server.port()).getBytes(StandardCharsets.UTF_8));
            nextCoord.unregisterMember(2); nextCoord.registerMember(2, ("127.0.0.1:" + nextServer.port()).getBytes(StandardCharsets.UTF_8));
            try (var client = new CandyboxClient(transport, clientCoord, config)) {
                client.createNamespaceBox("shared", 8);
                var created = client.createNamespace("shared", "n1", 100);
                assertThat(created.state().expiresAtMillis()).isEqualTo(1100);
                clock.setTime(1050);
                var put = client.putNamespaceItem("shared", "n1", "你好/../l/", new byte[]{42});
                assertThat(put.state().expiresAtMillis()).isEqualTo(1150);
                clock.setTime(1100);
                assertThat(client.claimNamespaceExpiry("shared", "n1", 1).outcome()).isEqualTo("STALE");
                assertThat(client.getNamespaceItem("shared", "n1", "你好/../l/").data()).containsExactly((byte)42);
                node.releaseBox(BoxName.of("shared")); next.openBox(BoxName.of("shared"));
                assertThat(client.getNamespaceItem("shared", "n1", "你好/../l/").state()).isEqualTo(put.state());
                clock.setTime(1150);
                assertThat(client.claimNamespaceExpiry("shared", "n1", 2).outcome()).isEqualTo("RETIRED");
                assertThat(client.cleanupNamespace("shared", "n1", 1).outcome()).isEqualTo("OK");
                clock.setTime(1050);
                assertThat(client.putNamespaceItem("shared", "n1", "late", new byte[]{1}).outcome()).isEqualTo("EXPIRED");
                assertThat(client.createNamespace("shared", "n1", 100).outcome()).isEqualTo("EXISTS");
                var page = client.namespace("shared", null, NamespaceOperation.ENUMERATE,
                        null, null, 0, 0, 128, null, next.descriptor(BoxName.of("shared")).partitionOf(NamespaceKeys.lifecycle("n1")));
                assertThat(page.keys()).containsExactly(NamespaceKeys.lifecycle("n1"));

                // The old raw application algorithm is a counterexample, not a storage bug.
                client.createBox("raw", 2);
                client.putCandy("raw", "meta/n1", new byte[]{1}, null, Map.of(), null);
                assertThat(client.getCandy("raw", "meta/n1")).containsExactly((byte)1);
                client.putCandy("raw", "data/n1/fresh", new byte[]{42}, null, Map.of(), null);
                client.putCandy("raw", "meta/n1", new byte[]{2}, null, Map.of(), null);
                client.deleteRangeByPrefix("raw", "data/n1/");
                assertThat(client.getCandy("raw", "meta/n1")).containsExactly((byte)2);
                assertThatThrownBy(() -> client.getCandy("raw", "data/n1/fresh"))
                        .isInstanceOf(CandyNotFoundException.class);
            }
        }
    }
    private static TcpTransportServer server(CandyboxNode node) {
        return new TcpTransportServer("127.0.0.1", 0, node.requestHandler(), new FrameCodec(), null, false,
                new TcpTransportServer.Options(1, 2, 2, Duration.ofSeconds(1)));
    }
}
