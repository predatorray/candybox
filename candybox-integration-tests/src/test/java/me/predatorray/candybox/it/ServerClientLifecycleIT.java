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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import me.predatorray.candybox.bookkeeper.bk.BookKeeperLedgerStore;
import me.predatorray.candybox.client.CandyboxClient;
import me.predatorray.candybox.common.BoxName;
import me.predatorray.candybox.common.SystemClock;
import me.predatorray.candybox.common.config.CandyboxConfig;
import me.predatorray.candybox.common.exception.CandyNotFoundException;
import me.predatorray.candybox.common.exception.CandyboxException;
import me.predatorray.candybox.coordination.zk.ZooKeeperCoordinationService;
import me.predatorray.candybox.protocol.FrameCodec;
import me.predatorray.candybox.protocol.transport.TcpTransport;
import me.predatorray.candybox.protocol.transport.TcpTransportServer;
import me.predatorray.candybox.server.CandyboxNode;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Drives the entire {@link CandyboxClient} surface against a real {@link CandyboxNode} over TCP,
 * embedded BookKeeper, and in-process ZooKeeper. This is the end-to-end path that exercises the
 * server-side {@code NodeRequestHandler} dispatch and the client's response decoding for every
 * operation: Box lifecycle, single-object CRUD, range GET, copy/rename, range delete, listing
 * (forward/reverse/paged), and the full multipart-upload flow. Single-node, single owner, so every
 * request routes straight to the node that created the Box.
 */
class ServerClientLifecycleIT {

    private static EmbeddedBookKeeper bookKeeper;
    private static TestingServer zookeeper;

    private static CandyboxNode node;
    private static TcpTransportServer transportServer;
    private static TcpTransport transport;
    private static ZooKeeperCoordinationService nodeCoord;
    private static ZooKeeperCoordinationService clientCoord;
    private static BookKeeperLedgerStore store;
    private static CandyboxClient client;

    @BeforeAll
    static void start() throws Exception {
        bookKeeper = new EmbeddedBookKeeper(3);
        zookeeper = new TestingServer(true);

        // Drop the multipart minimum part size so the multipart tests can use tiny in-memory parts
        // (the 5 MiB S3 default only matters for the non-final part; correctness is the same).
        CandyboxConfig config = CandyboxConfig.builder()
                .leaseRenewIntervalMillis(0)
                .multipartMinPartBytes(0)
                .partitionsPerBoxDefault(2) // small but real partition spread over embedded BK
                .build();
        int port = freePort();
        store = new BookKeeperLedgerStore(bookKeeper.clientConfiguration(), bytes("candybox"));
        nodeCoord = new ZooKeeperCoordinationService(zookeeper.getConnectString(), SystemClock.INSTANCE);
        node = new CandyboxNode(1, config, store, nodeCoord, SystemClock.INSTANCE, "127.0.0.1:" + port);
        transportServer = new TcpTransportServer(port, node.requestHandler(), new FrameCodec());

        transport = new TcpTransport();
        clientCoord = new ZooKeeperCoordinationService(zookeeper.getConnectString(), SystemClock.INSTANCE);
        client = new CandyboxClient(transport, clientCoord, config);
    }

    @AfterAll
    static void stop() {
        closeQuietly(client);
        closeQuietly(transport);
        closeQuietly(transportServer);
        closeQuietly(node);
        closeQuietly(nodeCoord);
        closeQuietly(clientCoord);
        closeQuietly(store);
        if (zookeeper != null) {
            closeQuietly(zookeeper::close);
        }
        if (bookKeeper != null) {
            closeQuietly(bookKeeper::close);
        }
    }

    @Test
    void boxLifecycle() {
        String box = "lifecycle-box";
        assertThat(client.headBox(box)).isFalse();
        client.createBox(box);
        assertThat(client.headBox(box)).isTrue();
        assertThat(client.listBoxes()).contains(box);

        client.deleteBox(box, true);
        assertThat(client.headBox(box)).isFalse();
    }

    @Test
    void candyCrudAndHead() {
        String box = "crud-box";
        client.createBox(box);
        byte[] data = bytes("hello candybox");
        client.putCandy(box, "k/1", data, "text/plain", Map.of("owner", "alice"), "idem-1");

        assertThat(client.getCandy(box, "k/1")).isEqualTo(data);

        CandyboxClient.CandyInfo info = client.headCandy(box, "k/1");
        assertThat(info.contentLength()).isEqualTo(data.length);
        assertThat(info.contentType()).isEqualTo("text/plain");
        assertThat(info.userMetadata()).containsEntry("owner", "alice");

        // Streaming get convenience writes the same bytes.
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        client.getCandy(box, "k/1", out);
        assertThat(out.toByteArray()).isEqualTo(data);

        client.deleteCandy(box, "k/1");
        assertThatThrownBy(() -> client.getCandy(box, "k/1"))
                .isInstanceOf(CandyNotFoundException.class);
    }

    @Test
    void rangeGet() {
        String box = "range-box";
        client.createBox(box);
        byte[] data = bytes("0123456789");
        client.putCandy(box, "blob", data, "application/octet-stream", Map.of(), null);

        // Explicit inclusive window [2,5] -> "2345".
        CandyboxClient.RangeBytes window = client.getCandyRange(box, "blob", 2, 5);
        assertThat(new String(window.data(), StandardCharsets.UTF_8)).isEqualTo("2345");
        assertThat(window.totalLength()).isEqualTo(10);
        assertThat(window.sliceLength()).isEqualTo(4);

        // From offset to end (lastByte < 0) -> "6789".
        CandyboxClient.RangeBytes toEnd = client.getCandyRange(box, "blob", 6, -1);
        assertThat(new String(toEnd.data(), StandardCharsets.UTF_8)).isEqualTo("6789");

        // Suffix range (firstByte < 0) -> last 3 bytes "789".
        CandyboxClient.RangeBytes suffix = client.getCandyRange(box, "blob", -1, 3);
        assertThat(new String(suffix.data(), StandardCharsets.UTF_8)).isEqualTo("789");
    }

    @Test
    void copyAndRename() {
        String box = "copy-box";
        client.createBox(box);
        byte[] data = bytes("payload");
        client.putCandy(box, "src", data, "text/plain", Map.of(), null);

        CandyboxClient.CandyInfo copied = client.copyCandy(box, "src", "dst", null);
        assertThat(copied.contentLength()).isEqualTo(data.length);
        assertThat(client.getCandy(box, "dst")).isEqualTo(data);
        // Source still present after a copy.
        assertThat(client.getCandy(box, "src")).isEqualTo(data);

        client.renameCandy(box, "src", "moved", null);
        assertThat(client.getCandy(box, "moved")).isEqualTo(data);
        // Source gone after a rename.
        assertThatThrownBy(() -> client.getCandy(box, "src"))
                .isInstanceOf(CandyNotFoundException.class);
    }

    @Test
    void rangeDeleteByPrefixAndWindow() {
        String box = "delete-box";
        client.createBox(box);
        for (String k : List.of("logs/a", "logs/b", "data/x", "data/y", "data/z")) {
            client.putCandy(box, k, bytes(k), "text/plain", Map.of(), null);
        }

        client.deleteRangeByPrefix(box, "logs/");
        assertThat(keysIn(box, "logs/")).isEmpty();

        // Delete the half-open window [data/x, data/z): removes x and y, keeps z.
        client.deleteRange(box, "data/x", "data/z");
        assertThat(keysIn(box, "data/")).containsExactly("data/z");
    }

    @Test
    void listForwardReverseRangeAndPaging() {
        String box = "list-box";
        client.createBox(box);
        for (String k : List.of("k1", "k2", "k3", "k4", "k5")) {
            client.putCandy(box, k, bytes(k), "text/plain", Map.of(), null);
        }

        // Forward, full listing.
        CandyboxClient.Listing all = client.listCandies(box, "k", null, 100);
        assertThat(all.entries().stream().map(CandyboxClient.Listing.Entry::key))
                .containsExactly("k1", "k2", "k3", "k4", "k5");

        // Forward, paged: first page of 2, then continue from nextStartAfter.
        CandyboxClient.Listing page1 = client.listCandies(box, "k", null, 2);
        assertThat(page1.entries()).hasSize(2);
        assertThat(page1.isTruncated()).isTrue();
        CandyboxClient.Listing page2 = client.listCandies(box, "k", page1.nextStartAfter(), 2);
        assertThat(page2.entries().stream().map(CandyboxClient.Listing.Entry::key))
                .containsExactly("k3", "k4");

        // Reverse over the whole range.
        CandyboxClient.Listing reverse = client.listCandies(box, null, null, null, null, true, 100);
        assertThat(reverse.entries().stream().map(CandyboxClient.Listing.Entry::key))
                .containsExactly("k5", "k4", "k3", "k2", "k1");

        // Half-open window [k2, k4).
        CandyboxClient.Listing windowed = client.listCandies(box, null, "k2", "k4", null, false, 100);
        assertThat(windowed.entries().stream().map(CandyboxClient.Listing.Entry::key))
                .containsExactly("k2", "k3");
    }

    @Test
    void multipartUploadCompletes() {
        String box = "mpart-box";
        client.createBox(box);
        String uploadId = client.createMultipartUpload(box, "big", "text/plain", Map.of("k", "v"));
        assertThat(uploadId).isNotBlank();

        CandyboxClient.PartUploadInfo p1 = client.uploadPart(box, "big", uploadId, 1, bytes("hello "));
        CandyboxClient.PartUploadInfo p2 = client.uploadPart(box, "big", uploadId, 2, bytes("world"));

        CandyboxClient.PartListing parts = client.listParts(box, "big", uploadId, 0, 100);
        assertThat(parts.parts()).hasSize(2);

        CandyboxClient.CandyInfo done = client.completeMultipartUpload(box, "big", uploadId,
                List.of(p1, p2), null);
        assertThat(done.contentLength()).isEqualTo("hello world".length());
        assertThat(client.getCandy(box, "big")).isEqualTo(bytes("hello world"));
    }

    @Test
    void multipartListsThenAborts() {
        String box = "abort-box";
        client.createBox(box);
        String uploadId = client.createMultipartUpload(box, "pending", null, Map.of());

        CandyboxClient.MultipartListing inflight = client.listMultipartUploads(box, null, null, null, 100);
        assertThat(inflight.uploads().stream().map(CandyboxClient.UploadEntry::uploadId))
                .contains(uploadId);

        client.abortMultipartUpload(box, "pending", uploadId);
        // Abort is idempotent: a second abort is a no-op.
        client.abortMultipartUpload(box, "pending", uploadId);

        CandyboxClient.MultipartListing afterAbort =
                client.listMultipartUploads(box, null, null, null, 100);
        assertThat(afterAbort.uploads().stream().map(CandyboxClient.UploadEntry::uploadId))
                .doesNotContain(uploadId);
    }

    @Test
    void multipartUploadPartCopyFromExistingObject() {
        String box = "mpcopy-box";
        client.createBox(box);
        byte[] source = bytes("abcdefghij");
        client.putCandy(box, "source", source, "application/octet-stream", Map.of(), null);

        String uploadId = client.createMultipartUpload(box, "assembled", null, Map.of());
        // Copy the whole source as part 1, and a byte window [0,4] ("abcde") as part 2.
        CandyboxClient.PartUploadInfo whole =
                client.uploadPartCopy(box, "assembled", uploadId, 1, "source", -1, -1);
        CandyboxClient.PartUploadInfo slice =
                client.uploadPartCopy(box, "assembled", uploadId, 2, "source", 0, 4);

        CandyboxClient.CandyInfo done = client.completeMultipartUpload(box, "assembled", uploadId,
                List.of(whole, slice), null);
        assertThat(done.contentLength()).isEqualTo(source.length + 5);
        assertThat(client.getCandy(box, "assembled")).isEqualTo(bytes("abcdefghijabcde"));
    }

    @Test
    void headBoxIsFalseForUnknownBoxAndMissingKeyGetFails() {
        assertThat(client.headBox("ghost-box")).isFalse();
        assertThatThrownBy(() -> client.getCandy("ghost-box", "k"))
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    void serverErrorsSurfaceToTheClient() {
        String box = "error-box";
        client.createBox(box);

        // Re-creating an existing Box is a server-side error mapped to an ErrorResponse.
        assertThatThrownBy(() -> client.createBox(box))
                .isInstanceOf(CandyboxException.class);

        // An out-of-bounds range GET is rejected as an InvalidRange validation error.
        client.putCandy(box, "small", bytes("0123456789"), "text/plain", Map.of(), null);
        assertThatThrownBy(() -> client.getCandyRange(box, "small", 100, 200))
                .isInstanceOf(CandyboxException.class);
    }

    @Test
    void deleteBoxRequiresForceWhenNonEmptyThenSucceeds() {
        String box = "nonempty-box";
        client.createBox(box);
        client.putCandy(box, "k", bytes("v"), "text/plain", Map.of(), null);

        // A non-empty Box cannot be deleted without force.
        assertThatThrownBy(() -> client.deleteBox(box, false))
                .isInstanceOf(CandyboxException.class);
        assertThat(client.headBox(box)).isTrue();

        // Forced delete removes it.
        client.deleteBox(box, true);
        assertThat(client.headBox(box)).isFalse();
    }

    @Test
    void boxOwnershipReleaseAndReopenReplaysState() {
        String box = "handover-box";
        client.createBox(box);
        client.putCandy(box, "durable", bytes("survives-handover"), "text/plain", Map.of(), null);

        BoxName boxName = BoxName.of(box);
        // Hand the Box off this node and re-acquire it: the open path fences prior owners and replays
        // the WAL, so the previously written value must still be readable.
        node.releaseBox(boxName);
        node.openBox(boxName);

        assertThat(node.currentOwner(boxName, 0)).contains(node.nodeId());
        assertThat(client.getCandy(box, "durable")).isEqualTo(bytes("survives-handover"));
    }

    @Test
    void nodeMaintenanceOperationsRunOverOwnedBoxes() {
        // Drive enough churn through a Box that the maintenance passes have real work to do.
        String box = "maint-box";
        client.createBox(box);
        for (int i = 0; i < 8; i++) {
            client.putCandy(box, "k" + i, bytes("v" + i), "text/plain", Map.of(), null);
        }
        client.deleteCandy(box, "k0");
        // An in-flight multipart upload gives the stale-upload sweep something to consider.
        client.createMultipartUpload(box, "pending", null, Map.of());

        BoxName boxName = BoxName.of(box);
        assertThat(node.boxExists(boxName)).isTrue();
        assertThat(node.currentOwner(boxName, 0)).contains(node.nodeId());
        assertThat(node.listBoxes()).contains(box);
        assertThat(node.ownedBoxStats()).containsKeys(box + "/0", box + "/1");

        // The three periodic maintenance passes should run without error and report a non-negative
        // count of units processed.
        assertThat(node.compactOwnedBoxesOnce()).isGreaterThanOrEqualTo(0);
        assertThat(node.collectGarbageOnce()).isGreaterThanOrEqualTo(0);
        assertThat(node.sweepStaleMultipartUploadsOnce()).isGreaterThanOrEqualTo(0);
    }

    // ---- helpers ---------------------------------------------------------------------------

    private static List<String> keysIn(String box, String prefix) {
        return client.listCandies(box, prefix, null, 100).entries().stream()
                .map(CandyboxClient.Listing.Entry::key)
                .toList();
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
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
