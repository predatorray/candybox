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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;
import javax.net.ssl.SSLContext;
import me.predatorray.candybox.client.CandyboxClient;
import me.predatorray.candybox.common.auth.Passwords;
import me.predatorray.candybox.common.auth.ScramCredential;
import me.predatorray.candybox.common.exception.AccessDeniedException;
import me.predatorray.candybox.common.exception.AuthenticationException;
import me.predatorray.candybox.common.exception.CandyboxException;
import me.predatorray.candybox.common.tls.PemTls;
import me.predatorray.candybox.it.EmbeddedBookKeeper;
import me.predatorray.candybox.protocol.FrameCodec;
import me.predatorray.candybox.protocol.auth.AuthenticatingTransport;
import me.predatorray.candybox.protocol.transport.TcpTransport;
import me.predatorray.candybox.protocol.transport.Transport;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Boots {@link CandyboxServer#start} with SASL auth and TLS enabled and checks that the security
 * wiring in the composition root is live: authenticator, authorizer, super users and TLS listener.
 */
class CandyboxServerSecurityIT {

    private static final String BOX = "alice-box";

    private static EmbeddedBookKeeper bookKeeper;
    private static TestingServer zookeeper;
    private static CandyboxServer.RunningServer running;
    private static Path ca;

    @BeforeAll
    static void start(@TempDir Path tmp) throws Exception {
        bookKeeper = new EmbeddedBookKeeper(3);
        zookeeper = new TestingServer(true);

        Path tls = Path.of(CandyboxServerSecurityIT.class.getResource("/tls/server.pem").toURI()).getParent();
        ca = tls.resolve("ca.pem");

        Path creds = tmp.resolve("creds");
        Files.writeString(creds, String.join("\n",
                "sasl.user.alice = " + Passwords.hash("alice-pw"),
                "sasl.scram-sha-256.bob = " + ScramCredential.fromPassword("bob-pw").toFileString(),
                "sasl.user.admin = " + Passwords.hash("admin-pw"),
                ""), StandardCharsets.UTF_8);

        Properties props = new Properties();
        props.setProperty("node.id", "1");
        props.setProperty("zookeeper.connect", zookeeper.getConnectString());
        props.setProperty("bookkeeper.metadataServiceUri", bookKeeper.metadataServiceUri());
        props.setProperty("server.bind", "127.0.0.1:0");
        props.setProperty("server.advertised", "127.0.0.1:0");
        props.setProperty("health.port", "0");
        props.setProperty("auth.enabled", "true");
        props.setProperty("auth.sasl.mechanisms", "PLAIN,SCRAM-SHA-256");
        props.setProperty("auth.credentials.file", creds.toString());
        props.setProperty("auth.super.users", "User:admin");
        props.setProperty("tls.enabled", "true");
        props.setProperty("tls.cert.path", tls.resolve("server.pem").toString());
        props.setProperty("tls.key.path", tls.resolve("server.key").toString());
        running = CandyboxServer.start(ServerConfig.fromProperties(props, Map.of()));

        awaitReady();
    }

    @AfterAll
    static void stop() {
        if (running != null) {
            running.close();
        }
        closeQuietly(zookeeper);
        closeQuietly(bookKeeper);
    }

    @Test
    void unauthenticatedClientIsRejected() {
        assertThatThrownBy(() -> {
            try (Transport t = tls();
                 CandyboxClient client = new CandyboxClient(t, "127.0.0.1", running.transportPort())) {
                client.createBox("anon-box");
            }
        }).isInstanceOfAny(AuthenticationException.class, AccessDeniedException.class);
    }

    @Test
    void wrongPasswordIsRejected() {
        assertThatThrownBy(() -> withClient("PLAIN", "alice", "nope", c -> c.createBox("x-box")))
                .isInstanceOf(AuthenticationException.class);
    }

    @Test
    void creatorOwnsBoxOthersAreDeniedAndSuperUserReads() throws Exception {
        byte[] data = "secret".getBytes(StandardCharsets.UTF_8);
        withClient("PLAIN", "alice", "alice-pw", c -> {
            c.createBox(BOX);
            c.putCandy(BOX, "k", data, "text/plain", Map.of(), null);
            assertThat(c.getCandy(BOX, "k")).isEqualTo(data);
        });

        assertThatThrownBy(() -> withClient("SCRAM-SHA-256", "bob", "bob-pw",
                c -> c.getCandy(BOX, "k"))).isInstanceOf(AccessDeniedException.class);

        withClient("PLAIN", "admin", "admin-pw", c -> assertThat(c.getCandy(BOX, "k")).isEqualTo(data));
    }

    @Test
    void plaintextClientCannotTalkToTlsListener() {
        assertThatThrownBy(() -> {
            try (TcpTransport t = new TcpTransport();
                 CandyboxClient client = new CandyboxClient(t, "127.0.0.1", running.transportPort())) {
                client.createBox("plain-box");
            }
        }).isInstanceOf(CandyboxException.class);
    }

    @Test
    void readyzIsOk() throws Exception {
        assertThat(readyzStatus()).isEqualTo(200);
    }

    // ---- helpers ---------------------------------------------------------------------------

    private static Transport tls() {
        SSLContext ctx = PemTls.clientContext(ca, null, null);
        return new TcpTransport(new FrameCodec(), ctx, true);
    }

    private static void withClient(String mechanism, String user, String password,
                                   java.util.function.Consumer<CandyboxClient> body) {
        try (Transport t = new AuthenticatingTransport(tls(), mechanism, user, password);
             CandyboxClient client = new CandyboxClient(t, "127.0.0.1", running.transportPort())) {
            body.accept(client);
        }
    }

    private static int readyzStatus() throws Exception {
        return HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + running.healthPort() + "/readyz"))
                        .GET().build(),
                BodyHandlers.ofString()).statusCode();
    }

    private static void awaitReady() throws Exception {
        for (int i = 0; i < 10; i++) {
            try {
                if (readyzStatus() == 200) {
                    return;
                }
            } catch (Exception notUpYet) {
                // keep polling
            }
            Thread.sleep(250);
        }
        throw new IllegalStateException("Node did not become ready in time");
    }

    private static void closeQuietly(AutoCloseable c) {
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
