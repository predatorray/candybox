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

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import me.predatorray.candybox.bookkeeper.fake.InMemoryLedgerStore;
import me.predatorray.candybox.common.BoxName;
import me.predatorray.candybox.common.ManualClock;
import me.predatorray.candybox.common.config.CandyboxConfig;
import me.predatorray.candybox.coordination.fake.InMemoryCoordinationService;
import me.predatorray.candybox.protocol.Frame;
import me.predatorray.candybox.protocol.Message;
import me.predatorray.candybox.protocol.MessageCodec;
import me.predatorray.candybox.protocol.Opcode;
import me.predatorray.candybox.protocol.transport.RequestHandler;
import org.junit.jupiter.api.Test;

/**
 * Exercises the server-side request dispatcher {@link NodeRequestHandler} directly over the protocol
 * codec, driving a real {@link CandyboxNode} backed by the in-memory {@code LedgerStore} /
 * {@code CoordinationService} fakes (no BookKeeper, ZooKeeper or sockets). Focuses on the dispatch
 * branches and error mappings that the socket-level integration tests do not deterministically reach:
 * {@code MOVED} re-routing, not-found, malformed frames, validation errors and the multipart listing
 * operations.
 */
class NodeRequestHandlerTest {

    private static final MessageCodec CODEC = new MessageCodec();

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static Message roundTrip(RequestHandler handler, Message request) {
        Frame response = handler.handle(CODEC.encode(request));
        return CODEC.decode(response);
    }

    /** Multipart minimum dropped to 0 so small in-memory parts complete. */
    private static CandyboxConfig config() {
        return CandyboxConfig.builder().multipartMinPartBytes(0).build();
    }

    @Test
    void malformedFrameMapsToProtocolError() {
        try (CandyboxNode node = new CandyboxNode(1, config(), new InMemoryLedgerStore(),
                new InMemoryCoordinationService(), new ManualClock(1000))) {
            RequestHandler handler = node.requestHandler();
            // A frame whose body the codec cannot decode (a PUT opcode with a truncated payload) must
            // come back as an ErrorResponse, not blow up the connection.
            Frame garbage = new Frame(Opcode.PUT_CANDY, new byte[] {1, 2, 3, 4});
            Message response = CODEC.decode(handler.handle(garbage));
            assertThat(response).isInstanceOf(Message.ErrorResponse.class);
            assertThat(((Message.ErrorResponse) response).errorType()).isEqualTo("ProtocolError");
        }
    }

    @Test
    void requestForUnknownBoxIsNotFound() {
        try (CandyboxNode node = new CandyboxNode(1, config(), new InMemoryLedgerStore(),
                new InMemoryCoordinationService(), new ManualClock(1000))) {
            RequestHandler handler = node.requestHandler();
            // No node owns "absent-box": a Box-routed request resolves to NotFound (no other owner).
            assertThat(roundTrip(handler, new Message.GetCandyRequest("absent-box", "k")))
                    .isInstanceOf(Message.NotFoundResponse.class);
            assertThat(roundTrip(handler, new Message.HeadBoxRequest("absent-box")))
                    .isInstanceOf(Message.NotFoundResponse.class);
        }
    }

    @Test
    void requestForBoxOwnedElsewhereIsRedirectedWithMoved() {
        // Two nodes sharing one coordination service and store: node 1 owns the Box, so a request to
        // node 2 must come back as MOVED(1) — the re-route signal the client follows.
        InMemoryLedgerStore store = new InMemoryLedgerStore();
        InMemoryCoordinationService coordination = new InMemoryCoordinationService();
        try (CandyboxNode owner = new CandyboxNode(1, config(), store, coordination, new ManualClock(1000));
             CandyboxNode other = new CandyboxNode(2, config(), store, coordination, new ManualClock(1000))) {
            owner.createBox(BoxName.of("shared-box"));

            Message response = roundTrip(other.requestHandler(),
                    new Message.GetCandyRequest("shared-box", "k"));
            assertThat(response).isInstanceOf(Message.MovedResponse.class);
            assertThat(((Message.MovedResponse) response).ownerNodeId()).isEqualTo(1);
        }
        store.close();
    }

    @Test
    void duplicateCreateBoxAndNonEmptyDeleteMapToErrorResponses() {
        try (CandyboxNode node = new CandyboxNode(1, config(), new InMemoryLedgerStore(),
                new InMemoryCoordinationService(), new ManualClock(1000))) {
            RequestHandler handler = node.requestHandler();
            assertThat(roundTrip(handler, new Message.CreateBoxRequest("dup-box", 1)))
                    .isInstanceOf(Message.OkResponse.class);
            // Re-create is a typed CandyboxException -> ErrorResponse.
            assertThat(roundTrip(handler, new Message.CreateBoxRequest("dup-box", 1)))
                    .isInstanceOf(Message.ErrorResponse.class);

            roundTrip(handler, new Message.PutCandyRequest("dup-box", "k", null, Map.of(), null,
                    bytes("v")));
            // Non-empty Box without force -> ErrorResponse (BoxNotEmpty).
            assertThat(roundTrip(handler, new Message.DeleteBoxRequest("dup-box", false)))
                    .isInstanceOf(Message.ErrorResponse.class);
            // With force -> OK.
            assertThat(roundTrip(handler, new Message.DeleteBoxRequest("dup-box", true)))
                    .isInstanceOf(Message.OkResponse.class);
        }
    }

    @Test
    void invalidRangeGetMapsToErrorResponse() {
        try (CandyboxNode node = new CandyboxNode(1, config(), new InMemoryLedgerStore(),
                new InMemoryCoordinationService(), new ManualClock(1000))) {
            RequestHandler handler = node.requestHandler();
            roundTrip(handler, new Message.CreateBoxRequest("range-box", 1));
            roundTrip(handler, new Message.PutCandyRequest("range-box", "k", null, Map.of(), null,
                    bytes("0123456789")));

            Message response = roundTrip(handler,
                    new Message.RangeGetCandyRequest("range-box", "k", 100, 200));
            assertThat(response).isInstanceOf(Message.ErrorResponse.class);
        }
    }

    @Test
    void listingAndMaintenanceCandyOperationsDispatch() {
        try (CandyboxNode node = new CandyboxNode(1, config(), new InMemoryLedgerStore(),
                new InMemoryCoordinationService(), new ManualClock(1000))) {
            RequestHandler handler = node.requestHandler();
            roundTrip(handler, new Message.CreateBoxRequest("ops-box", 1));
            for (String k : List.of("a", "b", "c")) {
                roundTrip(handler, new Message.PutCandyRequest("ops-box", k, null, Map.of(), null,
                        bytes(k)));
            }

            // ListBoxes (cluster-wide-ish: this node's owned Boxes).
            Message boxes = roundTrip(handler, new Message.ListBoxesRequest());
            assertThat(((Message.ListBoxesResponse) boxes).boxes()).contains("ops-box");

            // Reverse, ranged listing exercises the toScanQuery direction/bounds mapping.
            Message listed = roundTrip(handler, new Message.ListCandiesRequest("ops-box", 0, null, null,
                    100, "a", "c", true));
            assertThat(listed).isInstanceOf(Message.ListCandiesResponse.class);

            // Copy then rename round-trips through HeadCandyResponse.
            assertThat(roundTrip(handler, new Message.CopyCandyRequest("ops-box", "a", "a-copy", null)))
                    .isInstanceOf(Message.HeadCandyResponse.class);
            assertThat(roundTrip(handler, new Message.RenameCandyRequest("ops-box", "b", "b-moved", null)))
                    .isInstanceOf(Message.HeadCandyResponse.class);

            // Range delete (window form) returns OK.
            assertThat(roundTrip(handler, new Message.DeleteRangeRequest("ops-box", 0, null, "a", "c")))
                    .isInstanceOf(Message.OkResponse.class);
        }
    }

    @Test
    void multipartDispatchIncludingListings() {
        try (CandyboxNode node = new CandyboxNode(1, config(), new InMemoryLedgerStore(),
                new InMemoryCoordinationService(), new ManualClock(1000))) {
            RequestHandler handler = node.requestHandler();
            roundTrip(handler, new Message.CreateBoxRequest("mp-box", 1));

            Message created = roundTrip(handler, new Message.CreateMultipartUploadRequest(
                    "mp-box", "obj", "text/plain", Map.of()));
            String uploadId = ((Message.CreateMultipartUploadResponse) created).uploadId();

            Message up1 = roundTrip(handler,
                    new Message.UploadPartRequest("mp-box", "obj", uploadId, 1, bytes("hello ")));
            Message up2 = roundTrip(handler,
                    new Message.UploadPartRequest("mp-box", "obj", uploadId, 2, bytes("world")));
            int crc1 = ((Message.UploadPartResponse) up1).crc32c();
            int crc2 = ((Message.UploadPartResponse) up2).crc32c();

            // ListParts and ListMultipartUploads dispatch.
            Message parts = roundTrip(handler,
                    new Message.ListPartsRequest("mp-box", "obj", uploadId, 0, 100));
            assertThat(((Message.ListPartsResponse) parts).parts()).hasSize(2);

            Message uploads = roundTrip(handler, new Message.ListMultipartUploadsRequest(
                    "mp-box", 0, null, null, null, 100));
            assertThat(((Message.ListMultipartUploadsResponse) uploads).uploads()).hasSize(1);

            // Complete and read back the assembled object.
            Message done = roundTrip(handler, new Message.CompleteMultipartUploadRequest("mp-box", "obj",
                    uploadId, List.of(new Message.CompletedPart(1, crc1), new Message.CompletedPart(2, crc2)),
                    null));
            assertThat(done).isInstanceOf(Message.HeadCandyResponse.class);
            Message get = roundTrip(handler, new Message.GetCandyRequest("mp-box", "obj"));
            assertThat(new String(((Message.CandyDataResponse) get).data(), StandardCharsets.UTF_8))
                    .isEqualTo("hello world");

            // A fresh upload that we then abort exercises the abort dispatch path.
            Message created2 = roundTrip(handler, new Message.CreateMultipartUploadRequest(
                    "mp-box", "obj2", null, Map.of()));
            String upload2 = ((Message.CreateMultipartUploadResponse) created2).uploadId();
            assertThat(roundTrip(handler, new Message.AbortMultipartUploadRequest("mp-box", "obj2", upload2)))
                    .isInstanceOf(Message.OkResponse.class);
        }
    }
}
