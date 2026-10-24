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
package me.predatorray.candybox.client;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import me.predatorray.candybox.bookkeeper.bk.BookKeeperLedgerStore;
import me.predatorray.candybox.common.SystemClock;
import me.predatorray.candybox.common.auth.AuthenticationProviders;
import me.predatorray.candybox.common.auth.FileCredentialStore;
import me.predatorray.candybox.common.config.CandyboxConfig;
import me.predatorray.candybox.common.tls.PemTls;
import me.predatorray.candybox.coordination.zk.ZooKeeperCoordinationService;
import me.predatorray.candybox.it.EmbeddedBookKeeper;
import me.predatorray.candybox.protocol.FrameCodec;
import me.predatorray.candybox.protocol.auth.AuthenticatingRequestHandler;
import me.predatorray.candybox.protocol.transport.TcpTransportServer;
import me.predatorray.candybox.server.CandyboxNode;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Drives {@link CandyboxCli} against a node exposing three listeners: plain, SASL-required, and
 * TLS. Covers the global auth/TLS options and the {@code put}/{@code get} I/O error paths that
 * {@code CandyboxCliTest} cannot reach without a network.
 */
class CandyboxCliSecurityIT {

    private static EmbeddedBookKeeper bookKeeper;
    private static TestingServer zookeeper;
    private static CandyboxNode node;
    private static TcpTransportServer plainServer;
    private static TcpTransportServer saslServer;
    private static TcpTransportServer tlsServer;
    private static ZooKeeperCoordinationService nodeCoord;
    private static BookKeeperLedgerStore store;
    private static String plain;
    private static String sasl;
    private static String tls;
    private static Path ca;

    @BeforeAll
    static void start(@TempDir Path tmp) throws Exception {
        bookKeeper = new EmbeddedBookKeeper(3);
        zookeeper = new TestingServer(true);

        CandyboxConfig config = CandyboxConfig.builder().leaseRenewIntervalMillis(0).build();
        int plainPort = freePort();
        int saslPort = freePort();
        int tlsPort = freePort();
        store = new BookKeeperLedgerStore(bookKeeper.clientConfiguration(),
                "candybox".getBytes(StandardCharsets.UTF_8));
        nodeCoord = new ZooKeeperCoordinationService(zookeeper.getConnectString(), SystemClock.INSTANCE);
        node = new CandyboxNode(1, config, store, nodeCoord, SystemClock.INSTANCE,
                "127.0.0.1:" + plainPort);

        ByteArrayOutputStream creds = new ByteArrayOutputStream();
        PrintStream credsOut = new PrintStream(creds, true, StandardCharsets.UTF_8);
        assertThat(CandyboxCli.run(new String[] {"make-credentials", "alice", "--password", "s3cret"},
                credsOut, credsOut)).isZero();
        Path credFile = tmp.resolve("credentials.properties");
        Files.writeString(credFile, creds.toString(StandardCharsets.UTF_8));

        FrameCodec codec = new FrameCodec();
        plainServer = new TcpTransportServer(plainPort, node.requestHandler(), codec);
        saslServer = new TcpTransportServer(saslPort,
                new AuthenticatingRequestHandler(node.requestHandler(),
                        AuthenticationProviders.forMechanisms(List.of("PLAIN", "SCRAM-SHA-256")),
                        new FileCredentialStore(credFile), true),
                codec);
        tlsServer = new TcpTransportServer(tlsPort, node.requestHandler(), codec,
                PemTls.serverContext(resource("tls/server.pem"), resource("tls/server.key"), null),
                false);
        ca = resource("tls/ca.pem");
        plain = "127.0.0.1:" + plainPort;
        sasl = "127.0.0.1:" + saslPort;
        tls = "localhost:" + tlsPort;
    }

    @AfterAll
    static void stop() {
        closeQuietly(tlsServer);
        closeQuietly(saslServer);
        closeQuietly(plainServer);
        closeQuietly(node);
        closeQuietly(nodeCoord);
        closeQuietly(store);
        if (zookeeper != null) {
            closeQuietly(zookeeper::close);
        }
        if (bookKeeper != null) {
            closeQuietly(bookKeeper::close);
        }
    }

    private final ByteArrayOutputStream out = new ByteArrayOutputStream();
    private final ByteArrayOutputStream err = new ByteArrayOutputStream();

    /** Runs the CLI against {@code server} with the given arguments; returns the exit code. */
    private int cli(String server, String... args) {
        out.reset();
        err.reset();
        String[] full = new String[args.length + 2];
        full[0] = "-s";
        full[1] = server;
        System.arraycopy(args, 0, full, 2, args.length);
        return CandyboxCli.run(full, new PrintStream(out, true, StandardCharsets.UTF_8),
                new PrintStream(err, true, StandardCharsets.UTF_8));
    }

    private String stdout() {
        return out.toString(StandardCharsets.UTF_8);
    }

    private String stderr() {
        return err.toString(StandardCharsets.UTF_8);
    }

    // ---- SASL ------------------------------------------------------------------------------

    @Test
    void plainMechanismIsTheDefault() {
        assertThat(cli(sasl, "-u", "alice", "--password", "s3cret", "list-boxes")).isZero();
    }

    @Test
    void scramSha256() {
        assertThat(cli(sasl, "-u", "alice", "--password", "s3cret", "--mechanism", "SCRAM-SHA-256",
                "list-boxes")).isZero();
    }

    @Test
    void passwordFileIsTrimmed(@TempDir Path tmp) throws IOException {
        Path pw = tmp.resolve("pw");
        Files.writeString(pw, "s3cret\n");
        assertThat(cli(sasl, "-u", "alice", "--password-file", pw.toString(), "list-boxes")).isZero();
    }

    @Test
    void wrongPasswordFails() {
        assertThat(cli(sasl, "-u", "alice", "--password", "wrong", "list-boxes")).isEqualTo(1);
        assertThat(stderr()).startsWith("error:").containsIgnoringCase("authentication");
    }

    @Test
    void missingCredentialsAreRejectedWhenAuthIsRequired() {
        assertThat(cli(sasl, "list-boxes")).isNotZero();
        assertThat(stderr()).startsWith("error:");
    }

    @Test
    void unknownMechanismIsRejected() {
        assertThat(cli(sasl, "-u", "alice", "--password", "s3cret", "--mechanism", "NOPE",
                "list-boxes")).isNotZero();
        assertThat(stderr()).contains("NOPE");
    }

    // ---- TLS -------------------------------------------------------------------------------

    @Test
    void tlsWithCustomCa() {
        assertThat(cli(tls, "--tls", "--tls-ca", ca.toString(), "create-box", "tls-box")).isZero();
        assertThat(cli(tls, "--tls", "--tls-ca", ca.toString(), "list-boxes")).isZero();
        assertThat(stdout()).contains("tls-box");
    }

    @Test
    void tlsWithoutCaFailsBecauseJvmDoesNotTrustTestCa() {
        assertThat(cli(tls, "--tls", "list-boxes")).isNotZero();
    }

    @Test
    void tlsInsecureNoVerifyFlagIsAccepted() {
        assertThat(cli(tls.replace("localhost", "127.0.0.1"), "--tls", "--tls-ca", ca.toString(),
                "--tls-insecure-no-verify", "list-boxes")).isZero();
    }

    // ---- put/get I/O errors ----------------------------------------------------------------

    @Test
    void putMetaWithoutEqualsIsRejected(@TempDir Path tmp) throws IOException {
        Path in = tmp.resolve("in");
        Files.writeString(in, "x");
        assertThat(cli(plain, "put", "io-meta", "k", in.toString(), "--meta", "novalue")).isEqualTo(2);
        assertThat(stderr()).contains("--meta expects key=value, got: novalue");
    }

    @Test
    void putOfMissingInputFileFails(@TempDir Path tmp) {
        assertThat(cli(plain, "create-box", "io-missing-in")).isZero();
        assertThat(cli(plain, "put", "io-missing-in", "k", tmp.resolve("nope").toString())).isEqualTo(1);
        assertThat(stderr()).contains("Failed to read input:");
    }

    @Test
    void getToUnwritableOutputFails(@TempDir Path tmp) throws IOException {
        Path in = tmp.resolve("in");
        Files.writeString(in, "x");
        assertThat(cli(plain, "create-box", "io-no-out")).isZero();
        assertThat(cli(plain, "put", "io-no-out", "k", in.toString())).isZero();
        assertThat(cli(plain, "get", "io-no-out", "k", tmp.resolve("missing-subdir/out.bin").toString()))
                .isEqualTo(1);
        assertThat(stderr()).contains("Failed to write output:");
    }

    // ---- helpers ---------------------------------------------------------------------------

    private static Path resource(String name) throws URISyntaxException {
        return Path.of(CandyboxCliSecurityIT.class.getClassLoader().getResource(name).toURI());
    }

    private static int freePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
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
