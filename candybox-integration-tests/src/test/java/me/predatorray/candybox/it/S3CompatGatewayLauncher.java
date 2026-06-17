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

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;
import me.predatorray.candybox.bookkeeper.bk.BookKeeperLedgerStore;
import me.predatorray.candybox.client.CandyboxClient;
import me.predatorray.candybox.common.SystemClock;
import me.predatorray.candybox.common.config.CandyboxConfig;
import me.predatorray.candybox.common.config.LedgerRole;
import me.predatorray.candybox.common.config.QuorumConfig;
import me.predatorray.candybox.coordination.zk.ZooKeeperCoordinationService;
import me.predatorray.candybox.protocol.FrameCodec;
import me.predatorray.candybox.protocol.transport.TcpTransport;
import me.predatorray.candybox.protocol.transport.TcpTransportServer;
import me.predatorray.candybox.s3.S3Gateway;
import me.predatorray.candybox.s3.S3GatewayConfig;
import me.predatorray.candybox.server.CandyboxNode;
import org.apache.curator.test.TestingServer;

/**
 * A throwaway, long-lived launcher that stands up the full Candybox stack in one JVM — embedded
 * BookKeeper, in-process ZooKeeper, a real node, and the Netty S3 gateway — bound to a fixed port so
 * the {@code ceph/s3-tests} compatibility suite ({@code compat/s3-tests/run.sh}) can be pointed at it
 * <em>without Docker</em>. This mirrors what {@code docker-compose.ci.yml} brings up (SigV4 auth + ACL
 * enforcement against the s3tests credentials) and exists only to run a calibration when the Docker
 * Hub image path is unavailable. It is not a unit/integration test; it blocks until killed.
 *
 * <pre>
 *   mvn -q -pl candybox-integration-tests -Dexec.classpathScope=test \
 *       -Dexec.mainClass=me.predatorray.candybox.it.S3CompatGatewayLauncher \
 *       -Dexec.args="9711 /abs/path/to/gateway-credentials.properties" \
 *       org.codehaus.mojo:exec-maven-plugin:3.5.0:java
 * </pre>
 */
public final class S3CompatGatewayLauncher {

    private S3CompatGatewayLauncher() {
    }

    public static void main(String[] args) throws Exception {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 9711;
        String credentialsFile = args.length > 1 ? args[1] : null;

        // One bookie with 1/1/1 quorum and a bounded index-file cache: replication factor is
        // irrelevant to S3-API compatibility, and a single bookie keeps the file-descriptor
        // footprint flat as the suite churns through thousands of ledgers (sandbox RLIMIT_NOFILE
        // is only 4096; the default 3 bookies × one .idx-per-ledger exhausts it mid-suite).
        EmbeddedBookKeeper bookKeeper = new EmbeddedBookKeeper(1, 1024);
        TestingServer zookeeper = new TestingServer(true);

        QuorumConfig single = new QuorumConfig(1, 1, 1);
        CandyboxConfig.Builder configBuilder = CandyboxConfig.builder();
        for (LedgerRole role : LedgerRole.values()) {
            configBuilder.quorum(role, single);
        }
        CandyboxConfig config = configBuilder.build();
        BookKeeperLedgerStore store = new BookKeeperLedgerStore(
                bookKeeper.clientConfiguration(), "candybox".getBytes(StandardCharsets.UTF_8));
        ZooKeeperCoordinationService nodeCoord =
                new ZooKeeperCoordinationService(zookeeper.getConnectString(), SystemClock.INSTANCE);
        int nodePort = 9709;
        CandyboxNode node = new CandyboxNode(
                1, config, store, nodeCoord, SystemClock.INSTANCE, "127.0.0.1:" + nodePort);
        TcpTransportServer transportServer =
                new TcpTransportServer(nodePort, node.requestHandler(), new FrameCodec());

        TcpTransport transport = new TcpTransport();
        ZooKeeperCoordinationService clientCoord =
                new ZooKeeperCoordinationService(zookeeper.getConnectString(), SystemClock.INSTANCE);
        CandyboxClient client = new CandyboxClient(transport, clientCoord, config);

        Properties props = new Properties();
        props.setProperty("zookeeper.connect", zookeeper.getConnectString());
        props.setProperty("s3.bind", "127.0.0.1:" + port);
        // Mirror docker-compose.ci.yml: real SigV4 + ACL enforcement. allow-anonymous keeps its
        // default (true) there, so unsigned requests fall through to the anonymous principal.
        if (credentialsFile != null) {
            props.setProperty("s3.auth.enabled", "true");
            props.setProperty("auth.credentials.file", credentialsFile);
        }
        S3Gateway gateway = new S3Gateway(S3GatewayConfig.fromProperties(props, Map.of()), client);
        gateway.start();

        System.out.println("[launcher] S3 gateway up on 127.0.0.1:" + gateway.port()
                + " (auth=" + (credentialsFile != null) + ")");
        System.out.flush();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            close(gateway);
            close(client);
            close(transport);
            close(transportServer);
            close(node);
            close(nodeCoord);
            close(clientCoord);
            close(store);
            close(zookeeper);
            close(bookKeeper);
        }));

        Thread.currentThread().join();
    }

    private static void close(AutoCloseable c) {
        if (c == null) {
            return;
        }
        try {
            c.close();
        } catch (Exception ignored) {
            // best effort
        }
    }
}
