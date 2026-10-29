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
import java.util.List;
import me.predatorray.candybox.bookkeeper.bk.BookKeeperLedgerStore;
import me.predatorray.candybox.common.SystemClock;
import me.predatorray.candybox.common.auth.AuthenticationProviders;
import me.predatorray.candybox.common.auth.Authorizer;
import me.predatorray.candybox.common.auth.BoxAcl;
import me.predatorray.candybox.common.auth.InMemoryCredentialStore;
import me.predatorray.candybox.common.auth.StandardAuthorizer;
import me.predatorray.candybox.common.config.CandyboxConfig;
import me.predatorray.candybox.coordination.CandyboxKeys;
import me.predatorray.candybox.coordination.zk.ZooKeeperCoordinationService;
import me.predatorray.candybox.it.EmbeddedBookKeeper;
import me.predatorray.candybox.protocol.FrameCodec;
import me.predatorray.candybox.protocol.Message;
import me.predatorray.candybox.protocol.MessageCodec;
import me.predatorray.candybox.protocol.auth.AuthenticatingRequestHandler;
import me.predatorray.candybox.protocol.auth.AuthenticatingTransport;
import me.predatorray.candybox.protocol.transport.Connection;
import me.predatorray.candybox.protocol.transport.TcpTransport;
import me.predatorray.candybox.protocol.transport.TcpTransportServer;
import me.predatorray.candybox.server.CandyboxNode;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end tests for {@code candybox acl} (Box and object ACLs) against a live node. The node runs
 * with {@code ALLOW_ALL} for document editing tests; the enforcement test enables the standard
 * authorizer and authenticates two distinct users over SASL.
 */
class CandyboxCliAclIT {

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
        transportServer = new TcpTransportServer(port,
                new AuthenticatingRequestHandler(node.requestHandler(),
                        AuthenticationProviders.forMechanisms(List.of("PLAIN")),
                        new InMemoryCredentialStore().addUser("alice", "alice-pw")
                                .addUser("bob", "bob-pw"), false), new FrameCodec());
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

    private String stderr() {
        return err.toString(StandardCharsets.UTF_8);
    }

    @Test
    void lowercaseGrantsAreEnforcedForAuthenticatedUsers(@TempDir Path tmp) throws IOException {
        node.authorizer(new StandardAuthorizer(List.of(), box ->
                nodeCoord.get(CandyboxKeys.boxAclKey(box)).map(v -> BoxAcl.fromBytes(v.value()))));
        try {
            Path file = tmp.resolve("data");
            Files.writeString(file, "shared bytes");
            assertThat(asUser("alice", "create-box", "acl-enforced")).isZero();
            assertThat(asUser("alice", "put", "acl-enforced", "k", file.toString())).isZero();
            assertThat(asUser("bob", "get", "acl-enforced", "k")).isEqualTo(1);
            assertThat(asUser("alice", "acl", "set", "acl-enforced", "user:alice", "user:bob:READ"))
                    .isZero();
            assertThat(asUser("bob", "get", "acl-enforced", "k")).as(stderr()).isZero();
            assertThat(stdout()).isEqualTo("shared bytes");
            assertThat(asUser("bob", "put", "acl-enforced", "denied", file.toString())).isEqualTo(1);
            assertThat(asUser("alice", "acl", "revoke", "acl-enforced", "USER:bob")).isZero();
            assertThat(asUser("bob", "get", "acl-enforced", "k")).isEqualTo(1);
            assertThat(asUser("alice", "acl", "grant", "acl-enforced", "--object", "k", "user:bob:READ"))
                    .isZero();
            // A fresh CandyboxClient needs Box READ for its BoxInfo lookup. Send GET_CANDY
            // directly to isolate object-only grants from that routing limitation (#189).
            assertThat(readObjectAsBob()).isInstanceOfSatisfying(Message.CandyDataResponse.class,
                    data -> assertThat(data.data())
                            .isEqualTo("shared bytes".getBytes(StandardCharsets.UTF_8)));
            assertThat(asUser("alice", "acl", "revoke", "acl-enforced", "--object", "k", "USER:bob"))
                    .isZero();
            assertThat(readObjectAsBob()).isInstanceOf(Message.AccessDeniedResponse.class);
        } finally {
            node.authorizer(Authorizer.ALLOW_ALL);
        }
    }

    private Message readObjectAsBob() {
        MessageCodec codec = new MessageCodec();
        try (AuthenticatingTransport transport = new AuthenticatingTransport(
                new TcpTransport(), "PLAIN", "bob", "bob-pw");
             Connection connection = transport.connect("127.0.0.1",
                     Integer.parseInt(server.substring(server.lastIndexOf(':') + 1)))) {
            return codec.decode(connection.call(codec.encode(
                    new Message.GetCandyRequest("acl-enforced", "k"))));
        }
    }

    private int asUser(String username, String... args) {
        String[] authenticated = new String[args.length + 4];
        authenticated[0] = "-u";
        authenticated[1] = username;
        authenticated[2] = "--password";
        authenticated[3] = username + "-pw";
        System.arraycopy(args, 0, authenticated, 4, args.length);
        return cli(authenticated);
    }

    @Test
    void getWithoutAclDocument() {
        assertThat(cli("create-box", "acl-none")).isZero();
        assertThat(cli("acl", "get", "acl-none")).isZero();
        assertThat(stdout().trim())
                .isEqualTo("# no ACL document (any authenticated principal has full access)");
    }

    @Test
    void setThenGetListsOwnerAndGrantsInOrder() {
        assertThat(cli("create-box", "acl-set")).isZero();
        assertThat(cli("acl", "set", "acl-set", "user:alice", "AllUsers:READ", "User:bob:READ+WRITE"))
                .isZero();
        assertThat(cli("acl", "get", "acl-set")).isZero();
        assertThat(stdout().lines().toList()).containsExactly(
                "owner=User:alice", "grant=AllUsers:READ", "grant=User:bob:READ+WRITE");
    }

    @Test
    void grantAddsAndReplacesSameGrantee() {
        assertThat(cli("create-box", "acl-grant")).isZero();
        assertThat(cli("acl", "set", "acl-grant", "alice")).isZero();
        assertThat(cli("acl", "grant", "acl-grant", "user:bob:READ")).isZero();
        assertThat(cli("acl", "get", "acl-grant")).isZero();
        assertThat(stdout().lines().toList()).containsExactly("owner=User:alice", "grant=User:bob:READ");

        assertThat(cli("acl", "grant", "acl-grant", "USER:bob:READ+WRITE")).isZero();
        assertThat(cli("acl", "get", "acl-grant")).isZero();
        assertThat(stdout().lines().toList())
                .containsExactly("owner=User:alice", "grant=User:bob:READ+WRITE");
    }

    @Test
    void grantWithoutAclDocumentFails() {
        assertThat(cli("create-box", "acl-grant-none")).isZero();
        assertThat(cli("acl", "grant", "acl-grant-none", "AllUsers:READ")).isEqualTo(2);
        assertThat(stderr()).contains("Box acl-grant-none has no ACL document yet");
    }

    @Test
    void revokeRemovesGrantAndMissingGranteeFails() {
        assertThat(cli("create-box", "acl-revoke")).isZero();
        assertThat(cli("acl", "set", "acl-revoke", "alice", "user:bob:READ", "AllUsers:READ")).isZero();
        assertThat(cli("acl", "revoke", "acl-revoke", "uSeR:bob")).isZero();
        assertThat(cli("acl", "get", "acl-revoke")).isZero();
        assertThat(stdout()).contains("grant=AllUsers:READ").doesNotContain("User:bob");

        assertThat(cli("acl", "revoke", "acl-revoke", "uSeR:bob")).isEqualTo(1);
        assertThat(stderr()).contains("No grant for User:bob");
    }

    @Test
    void aclWithoutSubcommandPrintsUsage() {
        assertThat(cli("acl")).isEqualTo(2);
        assertThat(stderr()).contains("usage: candybox acl get|set|grant|revoke <box> ...");
    }

    @Test
    void unknownSubcommand() {
        assertThat(cli("acl", "frobnicate", "some-box")).isEqualTo(2);
        assertThat(stderr()).contains("Unknown acl subcommand: frobnicate");
        assertThat(stdout()).isEmpty();
        assertThat(cli("acl", "frobnicate", "some-box", "--object", "k")).isEqualTo(2);
        assertThat(stderr()).contains("Unknown acl subcommand: frobnicate");
    }

    @Test
    void getOnMissingBoxFails() {
        assertThat(cli("acl", "get", "acl-missing-box")).isEqualTo(1);
        assertThat(stderr()).startsWith("error:");
    }

    // ---- object ACLs -------------------------------------------------------------------------

    private void putObject(Path tmp, String box, String key, String content) throws IOException {
        assertThat(cli("create-box", box)).isZero();
        Path file = tmp.resolve(box + ".bin");
        Files.writeString(file, content);
        assertThat(cli("put", box, key, file.toString())).isZero();
    }

    @Test
    void objectAclDefaultsToUnowned(@TempDir Path tmp) throws IOException {
        putObject(tmp, "acl-obj-default", "k", "data");
        assertThat(cli("acl", "get", "acl-obj-default", "--object", "k")).isZero();
        assertThat(stdout().lines().toList()).containsExactly("owner=(none)");
    }

    @Test
    void objectAclSetUnowned(@TempDir Path tmp) throws IOException {
        putObject(tmp, "acl-obj-unowned", "k", "data");
        assertThat(cli("acl", "set", "acl-obj-unowned", "--object", "k", "-", "AllUsers:READ")).isZero();
        assertThat(cli("acl", "get", "acl-obj-unowned", "--object", "k")).isZero();
        assertThat(stdout().lines().toList()).containsExactly("owner=(none)", "grant=AllUsers:READ");
    }

    @Test
    void objectAclSetOwner(@TempDir Path tmp) throws IOException {
        putObject(tmp, "acl-obj-owner", "k", "data");
        assertThat(cli("acl", "set", "acl-obj-owner", "--object", "k", "user:alice", "AllUsers:READ"))
                .isZero();
        assertThat(cli("acl", "get", "acl-obj-owner", "--object", "k")).isZero();
        assertThat(stdout().lines().toList())
                .containsExactly("owner=User:alice", "grant=AllUsers:READ");
    }

    @Test
    void objectAclGrantReplacesAndRevokes(@TempDir Path tmp) throws IOException {
        putObject(tmp, "acl-obj-edit", "k", "data");
        assertThat(cli("acl", "grant", "acl-obj-edit", "--object", "k", "user:bob:READ")).isZero();
        assertThat(cli("acl", "grant", "acl-obj-edit", "--object", "k", "USER:bob:READ+WRITE")).isZero();
        assertThat(cli("acl", "get", "acl-obj-edit", "--object", "k")).isZero();
        assertThat(stdout().lines().toList())
                .containsExactly("owner=(none)", "grant=User:bob:READ+WRITE");

        assertThat(cli("acl", "revoke", "acl-obj-edit", "--object", "k", "uSeR:bob")).isZero();
        assertThat(cli("acl", "get", "acl-obj-edit", "--object", "k")).isZero();
        assertThat(stdout().lines().toList()).containsExactly("owner=(none)");

        assertThat(cli("acl", "revoke", "acl-obj-edit", "--object", "k", "uSeR:bob")).isEqualTo(1);
        assertThat(stderr()).contains("No grant for User:bob");
    }

    @Test
    void objectOptionMayAppearAnywhereAfterBox(@TempDir Path tmp) throws IOException {
        putObject(tmp, "acl-obj-order", "k", "data");
        assertThat(cli("acl", "grant", "acl-obj-order", "User:bob:READ", "--object", "k")).isZero();
        assertThat(cli("acl", "grant", "acl-obj-order", "--object", "k", "User:carol:READ")).isZero();
        assertThat(cli("acl", "get", "acl-obj-order", "--object", "k")).isZero();
        assertThat(stdout().lines().toList())
                .containsExactly("owner=(none)", "grant=User:bob:READ", "grant=User:carol:READ");
    }

    @Test
    void objectAclChangesDoNotTouchBytes(@TempDir Path tmp) throws IOException {
        putObject(tmp, "acl-obj-bytes", "k", "original bytes");
        assertThat(cli("acl", "set", "acl-obj-bytes", "--object", "k", "alice", "AllUsers:READ")).isZero();
        assertThat(cli("acl", "grant", "acl-obj-bytes", "--object", "k", "user:bob:READ")).isZero();
        assertThat(cli("acl", "revoke", "acl-obj-bytes", "--object", "k", "uSeR:bob")).isZero();
        assertThat(cli("get", "acl-obj-bytes", "k")).isZero();
        assertThat(stdout()).isEqualTo("original bytes");
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
