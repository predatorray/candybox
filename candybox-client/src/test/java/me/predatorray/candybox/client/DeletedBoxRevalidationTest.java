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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import me.predatorray.candybox.common.ManualClock;
import me.predatorray.candybox.common.config.SizeLimits;
import me.predatorray.candybox.common.exception.BoxNotFoundException;
import me.predatorray.candybox.common.exception.BusyException;
import me.predatorray.candybox.common.exception.CandyboxException;
import me.predatorray.candybox.common.exception.NotOwnerException;
import me.predatorray.candybox.coordination.CandyboxKeys;
import me.predatorray.candybox.coordination.Lease;
import me.predatorray.candybox.coordination.fake.InMemoryCoordinationService;
import me.predatorray.candybox.protocol.Frame;
import me.predatorray.candybox.protocol.Message;
import me.predatorray.candybox.protocol.MessageCodec;
import me.predatorray.candybox.protocol.transport.LoopbackTransport;
import me.predatorray.candybox.protocol.transport.RequestHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A Box deleted by another client must surface as {@link BoxNotFoundException} even though this
 * client still caches its partition count, while a merely unowned partition of a live Box stays a
 * retryable {@link NotOwnerException}.
 */
class DeletedBoxRevalidationTest {

    private static final MessageCodec CODEC = new MessageCodec();
    private static final long TTL = 5_000;

    /** A node whose Box can be "deleted" and whose list/delete-range answers can be overridden. */
    private static final class StubNode implements RequestHandler {
        volatile boolean boxExists = true;
        volatile Message partitionResponse;

        @Override
        public Frame handle(Frame request) {
            Message message = CODEC.decode(request);
            return CODEC.encode(dispatch(message));
        }

        private Message dispatch(Message message) {
            if (message instanceof Message.BoxInfoRequest) {
                return boxExists ? new Message.BoxInfoResponse(2) : new Message.NotFoundResponse();
            }
            if (partitionResponse != null) {
                return partitionResponse;
            }
            if (message instanceof Message.ListCandiesRequest) {
                return new Message.ListCandiesResponse(List.of(), null);
            } else if (message instanceof Message.ListMultipartUploadsRequest) {
                return new Message.ListMultipartUploadsResponse(List.of(), null, null);
            } else if (message instanceof Message.GetCandyRequest) {
                return new Message.CandyDataResponse(1, "text/plain", Map.of(), 0, new byte[] {1});
            }
            return new Message.OkResponse();
        }
    }

    private final StubNode node = new StubNode();
    private final InMemoryCoordinationService coordination = new InMemoryCoordinationService();
    private final ManualClock clock = new ManualClock(1_000);
    private final List<Lease> leases = new ArrayList<>();
    private CandyboxClient client;

    @BeforeEach
    void setUp() {
        coordination.registerMember(1, "node:1".getBytes(StandardCharsets.UTF_8));
        for (int p = 0; p < 2; p++) {
            leases.add(coordination.tryAcquireLease(CandyboxKeys.ownerResource("box", p), 1, 60_000)
                    .orElseThrow());
        }
        client = new CandyboxClient(
                new ClusterRouter(new LoopbackTransport(node), coordination, TTL, clock),
                SizeLimits.defaults());
        client.listCandies("box", null, null, 10); // caches the partition count and both owners
    }

    @AfterEach
    void tearDown() {
        client.close();
    }

    /** What another client's deleteBox leaves behind: no descriptor and no partition owners. */
    private void deleteBoxElsewhere() {
        node.boxExists = false;
        leases.forEach(Lease::release);
    }

    @Test
    void ownerlessPartitionsOfADeletedBoxSurfaceAsBoxNotFound() {
        deleteBoxElsewhere();
        clock.advance(TTL + 1); // the router's cached owners expire

        assertThatThrownBy(() -> client.listCandies("box", null, null, 10))
                .isInstanceOf(BoxNotFoundException.class);
        assertThatThrownBy(() -> client.getCandy("box", "k"))
                .isInstanceOf(BoxNotFoundException.class);
        assertThatThrownBy(() -> client.listMultipartUploads("box", null, null, null, 10))
                .isInstanceOf(BoxNotFoundException.class);
        assertThatThrownBy(() -> client.deleteRangeByPrefix("box", ""))
                .isInstanceOf(BoxNotFoundException.class);
    }

    @Test
    void anUnownedPartitionOfALiveBoxStaysRetryable() {
        leases.forEach(Lease::release);
        clock.advance(TTL + 1);

        assertThatThrownBy(() -> client.listCandies("box", null, null, 10))
                .isInstanceOf(NotOwnerException.class);
        assertThatThrownBy(() -> client.getCandy("box", "k"))
                .isInstanceOf(NotOwnerException.class);
    }

    @Test
    void fanOutNotFoundFromAStaleOwnerSurfacesAsBoxNotFound() {
        deleteBoxElsewhere();
        // The router still holds the old owner's address, which answers NOT_FOUND.
        node.partitionResponse = new Message.NotFoundResponse();

        assertThatThrownBy(() -> client.listCandies("box", null, null, 10))
                .isInstanceOf(BoxNotFoundException.class);
        assertThatThrownBy(() -> client.listMultipartUploads("box", null, null, null, 10))
                .isInstanceOf(BoxNotFoundException.class);
        assertThatThrownBy(() -> client.deleteRange("box", "a", "z"))
                .isInstanceOf(BoxNotFoundException.class);
    }

    @Test
    void fanOutNotFoundForALiveBoxKeepsTheOriginalError() {
        node.partitionResponse = new Message.NotFoundResponse();

        assertThatThrownBy(() -> client.listCandies("box", null, null, 10))
                .isExactlyInstanceOf(CandyboxException.class);
        assertThatThrownBy(() -> client.listMultipartUploads("box", null, null, null, 10))
                .isExactlyInstanceOf(CandyboxException.class);
        assertThatThrownBy(() -> client.deleteRangeByPrefix("box", "p/"))
                .isExactlyInstanceOf(CandyboxException.class);
        assertThatThrownBy(() -> client.deleteRange("box", "a", "z"))
                .isExactlyInstanceOf(CandyboxException.class);

        node.partitionResponse = new Message.BusyResponse(10); // not a NOT_FOUND: no revalidation
        assertThatThrownBy(() -> client.listCandies("box", null, null, 10))
                .isInstanceOf(BusyException.class);
    }

    @Test
    void aFailedRevalidationKeepsTheOriginalError() {
        leases.forEach(Lease::release);
        coordination.unregisterMember(1); // the BoxInfo lookup has nowhere to go
        clock.advance(TTL + 1);

        assertThatThrownBy(() -> client.listCandies("box", null, null, 10))
                .isInstanceOf(NotOwnerException.class)
                .satisfies(e -> assertThat(e.getSuppressed()).hasSize(1));
    }

    @Test
    void recoversOnceTheBoxIsUsableAgain() {
        leases.forEach(Lease::release);
        clock.advance(TTL + 1);
        assertThatThrownBy(() -> client.listCandies("box", null, null, 10))
                .isInstanceOf(NotOwnerException.class);

        coordination.tryAcquireLease(CandyboxKeys.ownerResource("box", 0), 1, 60_000);
        coordination.tryAcquireLease(CandyboxKeys.ownerResource("box", 1), 1, 60_000);
        assertThat(client.listCandies("box", null, null, 10).entries()).isEmpty();
    }
}
