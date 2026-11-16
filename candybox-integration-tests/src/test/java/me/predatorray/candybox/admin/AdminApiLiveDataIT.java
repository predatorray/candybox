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
package me.predatorray.candybox.admin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import me.predatorray.candybox.bookkeeper.bk.BookKeeperLedgerStore;
import me.predatorray.candybox.client.CandyboxClient;
import me.predatorray.candybox.common.SystemClock;
import me.predatorray.candybox.common.auth.AuthenticationProviders;
import me.predatorray.candybox.common.auth.FileCredentialStore;
import me.predatorray.candybox.common.auth.Passwords;
import me.predatorray.candybox.common.auth.ScramCredential;
import me.predatorray.candybox.common.config.CandyboxConfig;
import me.predatorray.candybox.common.exception.AuthenticationException;
import me.predatorray.candybox.common.tls.PemTls;
import me.predatorray.candybox.coordination.zk.ZooKeeperCoordinationService;
import me.predatorray.candybox.it.EmbeddedBookKeeper;
import me.predatorray.candybox.protocol.FrameCodec;
import me.predatorray.candybox.protocol.auth.AuthenticatingRequestHandler;
import me.predatorray.candybox.protocol.auth.AuthenticatingTransport;
import me.predatorray.candybox.protocol.transport.TcpTransport;
import me.predatorray.candybox.protocol.transport.TcpTransportServer;
import me.predatorray.candybox.server.CandyboxNode;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Lives in the admin package to exercise the composition root without making its seams public. */
class AdminApiLiveDataIT {

    private static final Deque<AutoCloseable> RESOURCES = new ArrayDeque<>();
    private static String plainZk;
    private static String secureZk;
    private static Path ca;

    @BeforeAll
    static void start(@TempDir Path tmp) throws Exception {
        EmbeddedBookKeeper bookKeeper = new EmbeddedBookKeeper(3);
        RESOURCES.push(bookKeeper);
        Path credentials = tmp.resolve("credentials.properties");
        Files.writeString(credentials, "sasl.user.alice=" + Passwords.hash("s3cret") + "\n"
                + "sasl.scram-sha-256.bob="
                + ScramCredential.fromPassword("s3cret").toFileString() + "\n");
        ca = resource("ca.pem");
        plainZk = startNode(bookKeeper, credentials, false);
        secureZk = startNode(bookKeeper, credentials, true);
    }

    private static String startNode(EmbeddedBookKeeper bookKeeper, Path credentials, boolean secure)
            throws Exception {
        TestingServer zk = new TestingServer(true);
        RESOURCES.push(zk);
        BookKeeperLedgerStore store = new BookKeeperLedgerStore(bookKeeper.clientConfiguration(),
                "candybox".getBytes(StandardCharsets.UTF_8));
        RESOURCES.push(store);
        ZooKeeperCoordinationService coordination =
                new ZooKeeperCoordinationService(zk.getConnectString(), SystemClock.INSTANCE);
        RESOURCES.push(coordination);
        int port;
        try (ServerSocket socket = new ServerSocket(0)) {
            port = socket.getLocalPort();
        }
        CandyboxNode node = new CandyboxNode(1,
                CandyboxConfig.builder().leaseRenewIntervalMillis(0).build(), store, coordination,
                SystemClock.INSTANCE, "localhost:" + port);
        RESOURCES.push(node);
        var handler = secure ? new AuthenticatingRequestHandler(node.requestHandler(),
                AuthenticationProviders.forMechanisms(List.of("PLAIN", "SCRAM-SHA-256")),
                new FileCredentialStore(credentials), true) : node.requestHandler();
        TcpTransportServer listener = new TcpTransportServer(port, handler, new FrameCodec(),
                secure ? PemTls.serverContext(resource("server.pem"), resource("server.key"), null)
                        : null,
                false);
        RESOURCES.push(listener);
        return zk.getConnectString();
    }

    @AfterAll
    static void stop() throws Exception {
        // Report cleanup failures while still giving every resource a chance to close.
        Exception failure = null;
        while (!RESOURCES.isEmpty()) {
            try {
                RESOURCES.pop().close();
            } catch (Exception e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    @Test
    void liveDataCreatesAndListsBoxesAndRegistersCleanup() {
        Deque<AutoCloseable> stack = new ArrayDeque<>();
        try {
            DashboardData data = AdminApiMain.buildDataSource(plainZk, stack, Map.of());
            assertThat(data).isInstanceOf(LiveDashboardData.class);
            assertThat(stack).hasSize(3);
            assertThat(stack).satisfiesExactly(
                    client -> assertThat(client).isInstanceOf(CandyboxClient.class),
                    coordination -> assertThat(coordination)
                            .isInstanceOf(ZooKeeperCoordinationService.class),
                    transport -> assertThat(transport).isInstanceOf(TcpTransport.class));
            data.createBox("dash");
            assertThat(data.boxes()).extracting(DashboardData.BoxSummary::name).contains("dash");
        } finally {
            AdminApiMain.closeAll(stack);
        }
        assertThat(stack).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = {"PLAIN", "SCRAM-SHA-256"})
    void forwardsCredentialsMechanismAndTlsSettings(String mechanism) {
        Deque<AutoCloseable> stack = new ArrayDeque<>();
        try {
            DashboardData data = AdminApiMain.buildDataSource(secureZk, stack,
                    securityEnv(mechanism, "s3cret"));
            assertThat(data).isInstanceOf(LiveDashboardData.class);
            assertThat(stack).hasSize(3);
            assertThat(stack.peekLast()).isInstanceOf(AuthenticatingTransport.class);
            String box = mechanism.equals("PLAIN") ? "dash-plain" : "dash-scram";
            data.createBox(box);
            assertThat(data.boxes()).extracting(DashboardData.BoxSummary::name).contains(box);
        } finally {
            AdminApiMain.closeAll(stack);
        }
        assertThat(stack).isEmpty();
    }

    @Test
    void wrongPasswordFailsWithAuthenticationException() {
        Deque<AutoCloseable> stack = new ArrayDeque<>();
        try {
            DashboardData data = AdminApiMain.buildDataSource(secureZk, stack,
                    securityEnv("PLAIN", "wrong"));
            assertThatThrownBy(() -> data.createBox("dash-rejected"))
                    .isInstanceOf(AuthenticationException.class);
        } finally {
            AdminApiMain.closeAll(stack);
        }
        assertThat(stack).isEmpty();
    }

    private static Map<String, String> securityEnv(String mechanism, String password) {
        return Map.of(
                "CANDYBOX_AUTH_CLIENT_USERNAME", mechanism.equals("PLAIN") ? "alice" : "bob",
                "CANDYBOX_AUTH_CLIENT_PASSWORD", password,
                "CANDYBOX_AUTH_CLIENT_MECHANISM", mechanism,
                "CANDYBOX_TLS_ENABLED", "true",
                "CANDYBOX_TLS_CA_PATH", ca.toString());
    }

    private static Path resource(String name) throws Exception {
        return Path.of(AdminApiLiveDataIT.class.getResource("/tls/" + name).toURI());
    }
}
