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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import me.predatorray.candybox.bookkeeper.bk.BookKeeperLedgerStore;
import me.predatorray.candybox.common.SystemClock;
import me.predatorray.candybox.common.config.CandyboxConfig;
import me.predatorray.candybox.coordination.zk.ZooKeeperCoordinationService;
import me.predatorray.candybox.it.EmbeddedBookKeeper;
import me.predatorray.candybox.protocol.FrameCodec;
import me.predatorray.candybox.protocol.transport.TcpTransportServer;
import me.predatorray.candybox.server.CandyboxNode;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Drives {@link CandyboxCli} (the {@code candybox} command-line front end) against a live node over
 * TCP, embedded BookKeeper, and in-process ZooKeeper. Complements {@code CandyboxCliTest}, which only
 * covers the argument-handling paths that resolve before a network call. Lives in the
 * {@code me.predatorray.candybox.client} package so it can invoke the package-private
 * {@link CandyboxCli#run(String[], PrintStream, PrintStream)} entry point.
 */
class CandyboxCliIT {

    private static EmbeddedBookKeeper bookKeeper;
    private static TestingServer zookeeper;
    private static CandyboxNode node;
    private static TcpTransportServer transportServer;
    private static ZooKeeperCoordinationService nodeCoord;
    private static BookKeeperLedgerStore store;
    private static String server;

    @BeforeAll
    static void start() throws Exception {
        bookKeeper = new EmbeddedBookKeeper(3);
        zookeeper = new TestingServer(true);

        CandyboxConfig config = CandyboxConfig.builder().leaseRenewIntervalMillis(0).build();
        int port = freePort();
        store = new BookKeeperLedgerStore(bookKeeper.clientConfiguration(),
                "candybox".getBytes(StandardCharsets.UTF_8));
        nodeCoord = new ZooKeeperCoordinationService(zookeeper.getConnectString(), SystemClock.INSTANCE);
        node = new CandyboxNode(1, config, store, nodeCoord, SystemClock.INSTANCE, "127.0.0.1:" + port);
        transportServer = new TcpTransportServer(port, node.requestHandler(), new FrameCodec());
        server = "127.0.0.1:" + port;
    }

    @AfterAll
    static void stop() {
        closeQuietly(transportServer);
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

    // ---- the CLI run, with stdout/stderr captured -----------------------------------------

    private final ByteArrayOutputStream out = new ByteArrayOutputStream();
    private final ByteArrayOutputStream err = new ByteArrayOutputStream();

    /** Invokes the CLI with the test server prepended; returns the process exit code. */
    private int cli(String... args) {
        // Reset the capture buffers so stdout()/stderr() reflect only this invocation.
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

    private byte[] stdoutBytes() {
        return out.toByteArray();
    }

    private String stderr() {
        return err.toString(StandardCharsets.UTF_8);
    }

    @Test
    void boxCommands() {
        assertThat(cli("create-box", "cli-boxes")).isZero();
        assertThat(cli("head-box", "cli-boxes")).isZero();
        assertThat(stdout()).contains("exists");
        assertThat(cli("list-boxes")).isZero();
        assertThat(stdout()).contains("cli-boxes");
        assertThat(cli("delete-box", "cli-boxes", "--force")).isZero();
    }

    @Test
    void headBoxAbsentReturnsNonZero() {
        assertThat(cli("head-box", "never-created-box")).isEqualTo(1);
        assertThat(stdout()).contains("absent");
    }

    @Test
    void putGetHeadDelete(@TempDir Path tmp) throws IOException {
        assertThat(cli("create-box", "cli-crud")).isZero();
        Path input = tmp.resolve("in.txt");
        byte[] payload = "command-line candy".getBytes(StandardCharsets.UTF_8);
        Files.write(input, payload);

        assertThat(cli("put", "cli-crud", "obj", input.toString(),
                "--content-type", "text/plain", "--meta", "owner=cli")).isZero();

        // get to stdout returns the exact bytes
        assertThat(cli("get", "cli-crud", "obj")).isZero();
        assertThat(stdoutBytes()).isEqualTo(payload);

        // get to a file
        Path output = tmp.resolve("out.txt");
        assertThat(cli("get", "cli-crud", "obj", output.toString())).isZero();
        assertThat(Files.readAllBytes(output)).isEqualTo(payload);

        // head prints metadata
        assertThat(cli("head", "cli-crud", "obj")).isZero();
        assertThat(stdout())
                .contains("contentLength: " + payload.length)
                .contains("contentType:   text/plain")
                .contains("meta.owner: cli");

        assertThat(cli("delete", "cli-crud", "obj")).isZero();
    }

    @Test
    void copyAndRename(@TempDir Path tmp) throws IOException {
        assertThat(cli("create-box", "cli-copy")).isZero();
        Path input = tmp.resolve("src.bin");
        Files.write(input, "zero-copy".getBytes(StandardCharsets.UTF_8));
        assertThat(cli("put", "cli-copy", "src", input.toString())).isZero();

        assertThat(cli("copy", "cli-copy", "src", "dst")).isZero();
        assertThat(cli("rename", "cli-copy", "src", "moved")).isZero();

        // dst (copy) and moved (rename target) both readable
        assertThat(cli("get", "cli-copy", "dst")).isZero();
        assertThat(stdoutBytes()).isEqualTo("zero-copy".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void listAndDeleteRange(@TempDir Path tmp) throws IOException {
        assertThat(cli("create-box", "cli-list")).isZero();
        Path input = tmp.resolve("v");
        Files.write(input, "x".getBytes(StandardCharsets.UTF_8));
        for (String k : new String[] {"p/1", "p/2", "p/3"}) {
            assertThat(cli("put", "cli-list", k, input.toString())).isZero();
        }

        assertThat(cli("list", "cli-list", "p/", "--max", "2")).isZero();
        String listing = stdout();
        assertThat(listing).contains("p/1").contains("p/2");
        // page of 2 of 3 entries is truncated, so the CLI prints the continuation hint
        assertThat(listing).contains("truncated");

        // positional-prefix range delete removes the whole p/ range
        assertThat(cli("delete-range", "cli-list", "p/")).isZero();
        assertThat(cli("list", "cli-list", "p/")).isZero();
        assertThat(stdout()).doesNotContain("p/1");
    }

    @Test
    void unknownCommandPrintsUsageWithExitCodeTwo() {
        assertThat(cli("frobnicate")).isEqualTo(2);
        assertThat(stderr()).contains("Unknown command: frobnicate");
    }

    // ---- helpers ---------------------------------------------------------------------------

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
