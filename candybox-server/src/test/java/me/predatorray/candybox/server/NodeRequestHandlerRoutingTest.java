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
import java.util.stream.Stream;
import me.predatorray.candybox.bookkeeper.fake.InMemoryLedgerStore;
import me.predatorray.candybox.common.BoxName;
import me.predatorray.candybox.common.CandyKey;
import me.predatorray.candybox.common.Hlc;
import me.predatorray.candybox.common.ManualClock;
import me.predatorray.candybox.common.config.CandyboxConfig;
import me.predatorray.candybox.coordination.BoxDescriptor;
import me.predatorray.candybox.coordination.fake.InMemoryCoordinationService;
import me.predatorray.candybox.protocol.Message;
import me.predatorray.candybox.protocol.MessageCodec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * {@link NodeRequestHandler} routing of cross-partition and ACL requests to a non-owner, plus Box-ACL
 * and UploadPartCopy validation. Two nodes share one ledger store and coordination service.
 */
class NodeRequestHandlerRoutingTest {

    private static final String BOX = "routed-box";

    private final MessageCodec codec = new MessageCodec();
    private InMemoryLedgerStore store;
    private CandyboxNode node1;
    private CandyboxNode node2;

    @BeforeEach
    void setUp() {
        ManualClock clock = new ManualClock(1_000);
        store = new InMemoryLedgerStore();
        InMemoryCoordinationService coordination = new InMemoryCoordinationService(clock);
        CandyboxConfig config = CandyboxConfig.builder().leaseRenewIntervalMillis(0).build();
        node1 = new CandyboxNode(1, config, store, coordination, clock);
        node2 = new CandyboxNode(2, config, store, coordination, clock);
    }

    @AfterEach
    void tearDown() {
        node1.close();
        node2.close();
        store.close();
    }

    private Message send(CandyboxNode node, Message request) {
        return codec.decode(node.requestHandler().handle(codec.encode(request)));
    }

    static Stream<Message> keyedRequests() {
        return Stream.of(
                new Message.GetCandyLocatorRequest(BOX, "k"),
                new Message.PrepareRenameRequest(BOX, "k", "k2", 0, "token"),
                new Message.ZeroCopyPutRequest(BOX, "k", 0, "relay", null, List.of(), null),
                new Message.CompleteRenameRequest(BOX, "k", 0, "token", new Hlc(1, 0, 1)),
                new Message.GetCandyAclRequest(BOX, "k"),
                new Message.SetCandyAclRequest(BOX, "k", null, List.of()));
    }

    @ParameterizedTest
    @MethodSource("keyedRequests")
    void nonOwnerRedirectsKeyedRequestToOwner(Message request) {
        node1.createBox(BoxName.of(BOX), 1);

        Message reply = send(node2, request);

        assertThat(reply).isInstanceOf(Message.MovedResponse.class);
        assertThat(((Message.MovedResponse) reply).ownerNodeId()).isEqualTo(1);
    }

    @Test
    void renameRoutesBySourceKeyAndZeroCopyPutByDestinationKey() {
        BoxName box = BoxName.of(BOX);
        node1.createBox(box, 4);
        BoxDescriptor descriptor = node1.descriptor(box);
        String srcKey = "src";
        int srcPartition = descriptor.partitionOf(srcKey);
        String dstKey = null;
        for (int i = 0; dstKey == null; i++) {
            if (descriptor.partitionOf("dst-" + i) != srcPartition) {
                dstKey = "dst-" + i;
            }
        }
        int dstPartition = descriptor.partitionOf(dstKey);

        // Node 1 keeps only the source partition; node 2 takes the destination partition.
        for (int p = 0; p < descriptor.partitionCount(); p++) {
            if (p != srcPartition) {
                node1.releasePartition(box, p);
            }
        }
        node2.openPartition(box, dstPartition);

        Message prepare = send(node2, new Message.PrepareRenameRequest(
                BOX, srcKey, dstKey, dstPartition, "token"));
        assertThat(prepare).isEqualTo(new Message.MovedResponse(1));

        Message complete = send(node2, new Message.CompleteRenameRequest(
                BOX, srcKey, srcPartition, "token", new Hlc(1, 0, 1)));
        assertThat(complete).isEqualTo(new Message.MovedResponse(1));

        Message zeroCopy = send(node1, new Message.ZeroCopyPutRequest(
                BOX, dstKey, srcPartition, "relay", null, List.of(), null));
        assertThat(zeroCopy).isEqualTo(new Message.MovedResponse(2));
    }

    @Test
    void setBoxAclOnMissingBoxIsNotFound() {
        node1.createBox(BoxName.of(BOX), 1);

        assertThat(send(node1, new Message.SetBoxAclRequest("no-such-box", "user:alice", List.of())))
                .isInstanceOf(Message.NotFoundResponse.class);
    }

    @Test
    void setBoxAclRejectsUnparsableGrantOrOwner() {
        node1.createBox(BoxName.of(BOX), 1);

        assertInvalidAcl(send(node1, new Message.SetBoxAclRequest(BOX, "user:alice", List.of("not-a-grant"))));
        assertInvalidAcl(send(node1, new Message.SetBoxAclRequest(BOX, "", List.of())));
    }

    private static void assertInvalidAcl(Message reply) {
        assertThat(reply).isInstanceOf(Message.ErrorResponse.class);
        Message.ErrorResponse error = (Message.ErrorResponse) reply;
        assertThat(error.errorType()).isEqualTo("ValidationException");
        assertThat(error.message()).startsWith("Invalid ACL:");
    }

    @Test
    void boxAclRoundTrips() {
        node1.createBox(BoxName.of(BOX), 1);
        String grant = "user:bob:READ";

        assertThat(send(node1, new Message.SetBoxAclRequest(BOX, "user:alice", List.of(grant))))
                .isInstanceOf(Message.OkResponse.class);

        Message reply = send(node1, new Message.GetBoxAclRequest(BOX));
        assertThat(reply).isInstanceOf(Message.BoxAclResponse.class);
        Message.BoxAclResponse acl = (Message.BoxAclResponse) reply;
        assertThat(acl.owner()).isEqualTo("User:alice");
        assertThat(acl.grants()).containsExactly("User:bob:READ");
    }

    @Test
    void uploadPartCopyWithUnsatisfiableRangeIsInvalidRange() {
        node1.createBox(BoxName.of(BOX), 1);
        node1.enginePartition(BoxName.of(BOX), 0).putCandy(CandyKey.of("src"),
                "0123456789".getBytes(StandardCharsets.UTF_8), null, Map.of(), null);
        Message created = send(node1, new Message.CreateMultipartUploadRequest(BOX, "dst", null, Map.of()));
        String uploadId = ((Message.CreateMultipartUploadResponse) created).uploadId();

        assertInvalidRange(send(node1, new Message.UploadPartCopyRequest(BOX, "dst", uploadId, 1, "src", 20, 30)));
        assertInvalidRange(send(node1, new Message.UploadPartCopyRequest(BOX, "dst", uploadId, 1, "src", 5, 2)));
    }

    private static void assertInvalidRange(Message reply) {
        assertThat(reply).isInstanceOf(Message.ErrorResponse.class);
        Message.ErrorResponse error = (Message.ErrorResponse) reply;
        assertThat(error.errorType()).isEqualTo("ValidationException");
        assertThat(error.message()).startsWith("InvalidRange:");
    }
}
