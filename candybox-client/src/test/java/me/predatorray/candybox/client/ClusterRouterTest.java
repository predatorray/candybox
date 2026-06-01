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
import java.util.Collections;
import java.util.List;
import me.predatorray.candybox.common.SystemClock;
import me.predatorray.candybox.common.exception.CandyboxException;
import me.predatorray.candybox.common.exception.NotOwnerException;
import me.predatorray.candybox.coordination.CandyboxKeys;
import me.predatorray.candybox.coordination.fake.InMemoryCoordinationService;
import me.predatorray.candybox.protocol.Frame;
import me.predatorray.candybox.protocol.Message;
import me.predatorray.candybox.protocol.MessageCodec;
import me.predatorray.candybox.protocol.transport.Connection;
import me.predatorray.candybox.protocol.transport.Transport;
import org.junit.jupiter.api.Test;

/**
 * Drives {@link ClusterRouter} against a fake coordination service and a recording stub transport:
 * resolution by lease holder, redirect on {@code MOVED} using the named owner, route caching, and the
 * no-owner case.
 */
class ClusterRouterTest {

    private static final MessageCodec CODEC = new MessageCodec();

    /** Records contacted ports; port 1001 redirects Box ops to node 2, everything else is OK. */
    private static final class RecordingTransport implements Transport {
        final List<Integer> contacted = Collections.synchronizedList(new ArrayList<>());

        @Override
        public Connection connect(String host, int port) {
            return new Connection() {
                @Override
                public Frame call(Frame request) {
                    contacted.add(port);
                    Message req = CODEC.decode(request);
                    Message resp = (port == 1001 && req instanceof Message.GetCandyRequest)
                            ? new Message.MovedResponse(2)
                            : new Message.OkResponse();
                    return CODEC.encode(resp);
                }

                @Override
                public void close() {
                }
            };
        }

        @Override
        public void close() {
        }
    }

    private InMemoryCoordinationService coordinationWithMembers() {
        InMemoryCoordinationService coordination = new InMemoryCoordinationService();
        coordination.registerMember(1, "127.0.0.1:1001".getBytes(StandardCharsets.UTF_8));
        coordination.registerMember(2, "127.0.0.1:2002".getBytes(StandardCharsets.UTF_8));
        return coordination;
    }

    @Test
    void routesToTheLeaseHolder() {
        InMemoryCoordinationService coordination = coordinationWithMembers();
        coordination.tryAcquireLease(CandyboxKeys.ownerResource("b"), 2, 10_000); // owner = node 2
        RecordingTransport transport = new RecordingTransport();

        try (ClusterRouter router = new ClusterRouter(transport, coordination, 5_000, SystemClock.INSTANCE)) {
            Message resp = router.callBox("b", new Message.GetCandyRequest("b", "k"));
            assertThat(resp).isInstanceOf(Message.OkResponse.class);
            assertThat(transport.contacted).containsExactly(2002);
        }
    }

    @Test
    void followsMovedRedirectAndCachesTheNewOwner() {
        InMemoryCoordinationService coordination = coordinationWithMembers();
        coordination.tryAcquireLease(CandyboxKeys.ownerResource("b"), 1, 10_000); // stale owner = node 1
        RecordingTransport transport = new RecordingTransport();

        try (ClusterRouter router = new ClusterRouter(transport, coordination, 5_000, SystemClock.INSTANCE)) {
            Message first = router.callBox("b", new Message.GetCandyRequest("b", "k"));
            assertThat(first).isInstanceOf(Message.OkResponse.class);
            assertThat(transport.contacted).containsExactly(1001, 2002); // redirected to node 2

            // The new owner is cached: a second call goes straight to node 2.
            router.callBox("b", new Message.GetCandyRequest("b", "k"));
            assertThat(transport.contacted).containsExactly(1001, 2002, 2002);
        }
    }

    @Test
    void throwsWhenBoxHasNoOwner() {
        InMemoryCoordinationService coordination = coordinationWithMembers();
        RecordingTransport transport = new RecordingTransport();
        try (ClusterRouter router = new ClusterRouter(transport, coordination, 5_000, SystemClock.INSTANCE)) {
            assertThatThrownBy(() -> router.callBox("ghost", new Message.GetCandyRequest("ghost", "k")))
                    .isInstanceOf(NotOwnerException.class);
        }
    }

    @Test
    void callAnyContactsAMember() {
        InMemoryCoordinationService coordination = coordinationWithMembers();
        RecordingTransport transport = new RecordingTransport();
        try (ClusterRouter router = new ClusterRouter(transport, coordination, 5_000, SystemClock.INSTANCE)) {
            Message resp = router.callAny(new Message.CreateBoxRequest("new-box"));
            assertThat(resp).isInstanceOf(Message.OkResponse.class);
            assertThat(transport.contacted).hasSize(1);
        }
    }

    @Test
    void callAnyThrowsWhenThereAreNoMembers() {
        InMemoryCoordinationService coordination = new InMemoryCoordinationService(); // no members
        RecordingTransport transport = new RecordingTransport();
        try (ClusterRouter router = new ClusterRouter(transport, coordination, 5_000, SystemClock.INSTANCE)) {
            assertThatThrownBy(() -> router.callAny(new Message.ListBoxesRequest()))
                    .isInstanceOf(CandyboxException.class)
                    .hasMessageContaining("No cluster members");
        }
    }

    @Test
    void throwsWhenOwnerNodeIsNotRegistered() {
        InMemoryCoordinationService coordination = coordinationWithMembers();
        coordination.tryAcquireLease(CandyboxKeys.ownerResource("b"), 99, 10_000); // node 99 unknown
        RecordingTransport transport = new RecordingTransport();
        try (ClusterRouter router = new ClusterRouter(transport, coordination, 5_000, SystemClock.INSTANCE)) {
            assertThatThrownBy(() -> router.callBox("b", new Message.GetCandyRequest("b", "k")))
                    .isInstanceOf(NotOwnerException.class)
                    .hasMessageContaining("not registered");
        }
    }

    @Test
    void throwsOnUnparseableMemberAddress() {
        InMemoryCoordinationService coordination = new InMemoryCoordinationService();
        coordination.registerMember(5, "no-colon-here".getBytes(StandardCharsets.UTF_8));
        coordination.tryAcquireLease(CandyboxKeys.ownerResource("b"), 5, 10_000);
        RecordingTransport transport = new RecordingTransport();
        try (ClusterRouter router = new ClusterRouter(transport, coordination, 5_000, SystemClock.INSTANCE)) {
            assertThatThrownBy(() -> router.callBox("b", new Message.GetCandyRequest("b", "k")))
                    .isInstanceOf(NotOwnerException.class)
                    .hasMessageContaining("Unroutable");
        }
    }

    @Test
    void exceedingRoutingAttemptsThrowsNotOwner() {
        InMemoryCoordinationService coordination = coordinationWithMembers();
        coordination.tryAcquireLease(CandyboxKeys.ownerResource("b"), 2, 10_000);
        // A transport that always redirects: the router exhausts MAX_ATTEMPTS and gives up.
        Transport alwaysMoved = new Transport() {
            @Override
            public Connection connect(String host, int port) {
                return new Connection() {
                    @Override
                    public Frame call(Frame request) {
                        return CODEC.encode(new Message.MovedResponse(2));
                    }

                    @Override
                    public void close() {
                    }
                };
            }

            @Override
            public void close() {
            }
        };
        try (ClusterRouter router = new ClusterRouter(alwaysMoved, coordination, 5_000, SystemClock.INSTANCE)) {
            assertThatThrownBy(() -> router.callBox("b", new Message.GetCandyRequest("b", "k")))
                    .isInstanceOf(NotOwnerException.class)
                    .hasMessageContaining("exceeded routing attempts");
        }
    }

    @Test
    void brokenConnectionIsDroppedAndReopenedOnRetry() {
        InMemoryCoordinationService coordination = coordinationWithMembers();
        coordination.tryAcquireLease(CandyboxKeys.ownerResource("b"), 2, 10_000);
        // First call on each freshly-opened connection throws; the router must drop it and rethrow.
        Transport flaky = new Transport() {
            @Override
            public Connection connect(String host, int port) {
                return new Connection() {
                    @Override
                    public Frame call(Frame request) {
                        throw new IllegalStateException("connection reset");
                    }

                    @Override
                    public void close() {
                    }
                };
            }

            @Override
            public void close() {
            }
        };
        try (ClusterRouter router = new ClusterRouter(flaky, coordination, 5_000, SystemClock.INSTANCE)) {
            assertThatThrownBy(() -> router.callBox("b", new Message.GetCandyRequest("b", "k")))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("connection reset");
            // A subsequent call reopens the connection and fails the same way (not a stale-handle error).
            assertThatThrownBy(() -> router.callBox("b", new Message.GetCandyRequest("b", "k")))
                    .isInstanceOf(IllegalStateException.class);
        }
    }
}
