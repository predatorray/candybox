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
import java.util.concurrent.atomic.AtomicInteger;
import me.predatorray.candybox.common.ManualClock;
import me.predatorray.candybox.common.SystemClock;
import me.predatorray.candybox.common.exception.CandyboxException;
import me.predatorray.candybox.common.exception.NotOwnerException;
import me.predatorray.candybox.coordination.CandyboxKeys;
import me.predatorray.candybox.coordination.fake.InMemoryCoordinationService;
import me.predatorray.candybox.protocol.Frame;
import me.predatorray.candybox.protocol.Message;
import me.predatorray.candybox.protocol.MessageCodec;
import me.predatorray.candybox.protocol.ProtocolException;
import me.predatorray.candybox.protocol.transport.Connection;
import me.predatorray.candybox.protocol.transport.Transport;
import org.junit.jupiter.api.Test;

/**
 * Drives {@link ClusterRouter} against a fake coordination service and a recording stub transport:
 * resolution by lease holder, redirect on {@code MOVED} using the named owner, route caching, and the
 * no-owner (handover gap) retry.
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
        coordination.tryAcquireLease(CandyboxKeys.ownerResource("b", 0), 2, 10_000); // owner = node 2
        RecordingTransport transport = new RecordingTransport();

        try (ClusterRouter router = new ClusterRouter(transport, coordination, 5_000, SystemClock.INSTANCE)) {
            Message resp = router.callPartition("b", 0, new Message.GetCandyRequest("b", "k"));
            assertThat(resp).isInstanceOf(Message.OkResponse.class);
            assertThat(transport.contacted).containsExactly(2002);
        }
    }

    @Test
    void followsMovedRedirectAndCachesTheNewOwner() {
        InMemoryCoordinationService coordination = coordinationWithMembers();
        coordination.tryAcquireLease(CandyboxKeys.ownerResource("b", 0), 1, 10_000); // stale owner = node 1
        RecordingTransport transport = new RecordingTransport();

        try (ClusterRouter router = new ClusterRouter(transport, coordination, 5_000, SystemClock.INSTANCE)) {
            Message first = router.callPartition("b", 0, new Message.GetCandyRequest("b", "k"));
            assertThat(first).isInstanceOf(Message.OkResponse.class);
            assertThat(transport.contacted).containsExactly(1001, 2002); // redirected to node 2

            // The new owner is cached: a second call goes straight to node 2.
            router.callPartition("b", 0, new Message.GetCandyRequest("b", "k"));
            assertThat(transport.contacted).containsExactly(1001, 2002, 2002);
        }
    }

    @Test
    void throwsWhenBoxHasNoOwner() {
        InMemoryCoordinationService coordination = coordinationWithMembers();
        RecordingTransport transport = new RecordingTransport();
        try (ClusterRouter router = new ClusterRouter(transport, coordination, 5_000, SystemClock.INSTANCE)) {
            assertThatThrownBy(() -> router.callPartition("ghost", 0, new Message.GetCandyRequest("ghost", "k")))
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
        coordination.tryAcquireLease(CandyboxKeys.ownerResource("b", 0), 99, 10_000); // node 99 unknown
        RecordingTransport transport = new RecordingTransport();
        try (ClusterRouter router = new ClusterRouter(transport, coordination, 5_000, SystemClock.INSTANCE)) {
            assertThatThrownBy(() -> router.callPartition("b", 0, new Message.GetCandyRequest("b", "k")))
                    .isInstanceOf(NotOwnerException.class)
                    .hasMessageContaining("not registered");
        }
    }

    @Test
    void throwsOnUnparseableMemberAddress() {
        InMemoryCoordinationService coordination = new InMemoryCoordinationService();
        coordination.registerMember(5, "no-colon-here".getBytes(StandardCharsets.UTF_8));
        coordination.tryAcquireLease(CandyboxKeys.ownerResource("b", 0), 5, 10_000);
        RecordingTransport transport = new RecordingTransport();
        try (ClusterRouter router = new ClusterRouter(transport, coordination, 5_000, SystemClock.INSTANCE)) {
            assertThatThrownBy(() -> router.callPartition("b", 0, new Message.GetCandyRequest("b", "k")))
                    .isInstanceOf(NotOwnerException.class)
                    .hasMessageContaining("Unroutable");
        }
    }

    @Test
    void exceedingRoutingAttemptsThrowsNotOwner() {
        InMemoryCoordinationService coordination = coordinationWithMembers();
        coordination.tryAcquireLease(CandyboxKeys.ownerResource("b", 0), 2, 10_000);
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
            assertThatThrownBy(() -> router.callPartition("b", 0, new Message.GetCandyRequest("b", "k")))
                    .isInstanceOf(NotOwnerException.class)
                    .hasMessageContaining("exceeded routing attempts");
        }
    }

    @Test
    void brokenConnectionIsDroppedAndReopenedOnRetry() {
        InMemoryCoordinationService coordination = coordinationWithMembers();
        coordination.tryAcquireLease(CandyboxKeys.ownerResource("b", 0), 2, 10_000);
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
            assertThatThrownBy(() -> router.callPartition("b", 0, new Message.GetCandyRequest("b", "k")))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("connection reset");
            // A subsequent call reopens the connection and fails the same way (not a stale-handle error).
            assertThatThrownBy(() -> router.callPartition("b", 0, new Message.GetCandyRequest("b", "k")))
                    .isInstanceOf(IllegalStateException.class);
        }
    }

    /** A transport that cannot reach {@code deadPort}; every other port answers OK. */
    private static final class PartlyDeadTransport implements Transport {
        final int deadPort;
        final List<Integer> contacted = Collections.synchronizedList(new ArrayList<>());

        PartlyDeadTransport(int deadPort) {
            this.deadPort = deadPort;
        }

        @Override
        public Connection connect(String host, int port) {
            contacted.add(port);
            if (port == deadPort) {
                throw new IllegalStateException("connection refused");
            }
            return new Connection() {
                @Override
                public Frame call(Frame request) {
                    return CODEC.encode(new Message.OkResponse());
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

    @Test
    void callAnyFailsOverWhenAMemberIsUnreachable() {
        InMemoryCoordinationService coordination = coordinationWithMembers();
        PartlyDeadTransport transport = new PartlyDeadTransport(1001); // the lowest-id member is down
        try (ClusterRouter router = new ClusterRouter(transport, coordination, 5_000, SystemClock.INSTANCE)) {
            for (int i = 0; i < 4; i++) {
                assertThat(router.callAny(new Message.ListBoxesRequest())).isInstanceOf(Message.OkResponse.class);
            }
            assertThat(transport.contacted).contains(1001, 2002);
        }
    }

    @Test
    void callAnySpreadsAcrossMembers() {
        InMemoryCoordinationService coordination = coordinationWithMembers();
        RecordingTransport transport = new RecordingTransport();
        try (ClusterRouter router = new ClusterRouter(transport, coordination, 5_000, SystemClock.INSTANCE)) {
            router.callAny(new Message.ListBoxesRequest());
            router.callAny(new Message.ListBoxesRequest());
            assertThat(transport.contacted).containsExactlyInAnyOrder(1001, 2002);
        }
    }

    @Test
    void callAnyThrowsWhenNoMemberIsReachable() {
        InMemoryCoordinationService coordination = new InMemoryCoordinationService();
        coordination.registerMember(1, "127.0.0.1:1001".getBytes(StandardCharsets.UTF_8));
        PartlyDeadTransport transport = new PartlyDeadTransport(1001);
        try (ClusterRouter router = new ClusterRouter(transport, coordination, 5_000, SystemClock.INSTANCE)) {
            assertThatThrownBy(() -> router.callAny(new Message.ListBoxesRequest()))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("connection refused");
        }
    }

    /** A sleeper that advances {@code clock} and records each requested sleep. */
    private static ClusterRouter.Sleeper advancing(ManualClock clock, List<Long> sleeps, Runnable onSleep) {
        return millis -> {
            sleeps.add(millis);
            clock.advance(millis);
            onSleep.run();
        };
    }

    @Test
    void waitsForALeaseHolderToAppearWithinTheRetryBudget() {
        ManualClock clock = new ManualClock(1_000);
        InMemoryCoordinationService coordination = new InMemoryCoordinationService(clock);
        coordination.registerMember(2, "127.0.0.1:2002".getBytes(StandardCharsets.UTF_8));
        coordination.create(CandyboxKeys.boxMetaKey("b"), new byte[0]);
        RecordingTransport transport = new RecordingTransport();
        List<Long> sleeps = new ArrayList<>();
        Runnable acquireOnThirdSleep = () -> {
            if (sleeps.size() == 3) {
                coordination.tryAcquireLease(CandyboxKeys.ownerResource("b", 0), 2, 10_000);
            }
        };
        try (ClusterRouter router = new ClusterRouter(transport, coordination, 5_000, clock, 15_000,
                advancing(clock, sleeps, acquireOnThirdSleep))) {
            Message resp = router.callPartition("b", 0, new Message.GetCandyRequest("b", "k"));
            assertThat(resp).isInstanceOf(Message.OkResponse.class);
            assertThat(transport.contacted).containsExactly(2002);
            assertThat(sleeps).containsExactly(50L, 100L, 200L); // exponential backoff
        }
    }

    @Test
    void retriesAnUnavailableAnswerHonoringItsRetryAfter() {
        ManualClock clock = new ManualClock(1_000);
        InMemoryCoordinationService coordination = new InMemoryCoordinationService(clock);
        coordination.registerMember(2, "127.0.0.1:2002".getBytes(StandardCharsets.UTF_8));
        coordination.tryAcquireLease(CandyboxKeys.ownerResource("b", 0), 2, 60_000);
        AtomicInteger calls = new AtomicInteger();
        Transport unavailableOnce = new Transport() {
            @Override
            public Connection connect(String host, int port) {
                return new Connection() {
                    @Override
                    public Frame call(Frame request) {
                        return CODEC.encode(calls.getAndIncrement() == 0
                                ? new Message.UnavailableResponse(300)
                                : new Message.OkResponse());
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
        List<Long> sleeps = new ArrayList<>();
        try (ClusterRouter router = new ClusterRouter(unavailableOnce, coordination, 5_000, clock, 15_000,
                advancing(clock, sleeps, () -> { }))) {
            Message resp = router.callPartition("b", 0, new Message.GetCandyRequest("b", "k"));
            assertThat(resp).isInstanceOf(Message.OkResponse.class);
            assertThat(calls).hasValue(2);
            assertThat(sleeps).containsExactly(300L);
        }
    }

    @Test
    void givesUpWithNotOwnerOnceTheRetryBudgetIsSpent() {
        ManualClock clock = new ManualClock(1_000);
        InMemoryCoordinationService coordination = new InMemoryCoordinationService(clock);
        coordination.create(CandyboxKeys.boxMetaKey("b"), new byte[0]);
        RecordingTransport transport = new RecordingTransport();
        List<Long> sleeps = new ArrayList<>();
        try (ClusterRouter router = new ClusterRouter(transport, coordination, 5_000, clock, 2_000,
                advancing(clock, sleeps, () -> { }))) {
            assertThatThrownBy(() -> router.callPartition("b", 0, new Message.GetCandyRequest("b", "k")))
                    .isInstanceOf(NotOwnerException.class)
                    .hasMessageContaining("has no current owner");
            assertThat(sleeps.stream().mapToLong(Long::longValue).sum()).isEqualTo(2_000);
            assertThat(transport.contacted).isEmpty();
        }
    }

    @Test
    void doesNotWaitForAnOwnerOfADeletedBox() {
        ManualClock clock = new ManualClock(1_000);
        InMemoryCoordinationService coordination = new InMemoryCoordinationService(clock);
        RecordingTransport transport = new RecordingTransport();
        List<Long> sleeps = new ArrayList<>();
        try (ClusterRouter router = new ClusterRouter(transport, coordination, 5_000, clock, 15_000,
                advancing(clock, sleeps, () -> { }))) {
            assertThatThrownBy(() -> router.callPartition("gone", 0, new Message.GetCandyRequest("gone", "k")))
                    .isInstanceOf(NotOwnerException.class)
                    .hasMessageContaining("has no current owner");
            assertThat(sleeps).isEmpty();
            assertThat(transport.contacted).isEmpty();
        }
    }

    /** Answers every call from {@code responses} in turn (the last one repeats), recording ports. */
    private static final class ScriptedTransport implements Transport {
        final List<Integer> contacted = Collections.synchronizedList(new ArrayList<>());
        final List<Integer> refusedPorts = new ArrayList<>();
        final List<Integer> failingCallPorts = new ArrayList<>();
        final List<Integer> rejectingCallPorts = new ArrayList<>();
        Throwable connectFailureCause = new java.net.ConnectException("Connection refused");
        private final List<Message> responses;
        private int next;

        ScriptedTransport(Message... responses) {
            this.responses = List.of(responses);
        }

        @Override
        public Connection connect(String host, int port) {
            if (refusedPorts.contains(port)) {
                throw new ProtocolException("Failed to connect to " + host + ":" + port,
                        connectFailureCause);
            }
            return new Connection() {
                @Override
                public Frame call(Frame request) {
                    contacted.add(port);
                    if (failingCallPorts.contains(port)) {
                        throw new ProtocolException("RPC failed", new java.io.EOFException());
                    }
                    if (rejectingCallPorts.contains(port)) {
                        throw new ProtocolException("Frame payload 20000000 exceeds max 16777216");
                    }
                    Message response = responses.get(Math.min(next++, responses.size() - 1));
                    return CODEC.encode(response);
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

    @Test
    void followsMovedToARecoveringOwnerThatIsBrieflyUnavailable() {
        ManualClock clock = new ManualClock(1_000);
        InMemoryCoordinationService coordination = coordinationWithMembers();
        coordination.tryAcquireLease(CandyboxKeys.ownerResource("b", 0), 1, 60_000);
        ScriptedTransport transport = new ScriptedTransport(new Message.MovedResponse(2),
                new Message.UnavailableResponse(200), new Message.OkResponse());
        List<Long> sleeps = new ArrayList<>();
        try (ClusterRouter router = new ClusterRouter(transport, coordination, 5_000, clock, 15_000,
                advancing(clock, sleeps, () -> { }))) {
            Message resp = router.callPartition("b", 0, new Message.GetCandyRequest("b", "k"));
            assertThat(resp).isInstanceOf(Message.OkResponse.class);
            // The stale lease still names node 1 after the retry; it re-routes to node 2 again.
            assertThat(transport.contacted).containsExactly(1001, 2002, 1001);
            assertThat(sleeps).containsExactly(200L);
        }
    }

    @Test
    void waitsOutALeaseHeldByAnUnregisteredOwner() {
        ManualClock clock = new ManualClock(1_000);
        InMemoryCoordinationService coordination = new InMemoryCoordinationService(clock);
        coordination.registerMember(2, "127.0.0.1:2002".getBytes(StandardCharsets.UTF_8));
        coordination.create(CandyboxKeys.boxMetaKey("b"), new byte[0]);
        // Node 1 crashed: its member entry is gone but its lease runs for another second.
        coordination.tryAcquireLease(CandyboxKeys.ownerResource("b", 0), 1, 1_000);
        RecordingTransport transport = new RecordingTransport();
        List<Long> sleeps = new ArrayList<>();
        Runnable takeOverOnceLapsed = () -> coordination.tryAcquireLease(
                CandyboxKeys.ownerResource("b", 0), 2, 10_000);
        try (ClusterRouter router = new ClusterRouter(transport, coordination, 5_000, clock, 15_000,
                advancing(clock, sleeps, takeOverOnceLapsed))) {
            Message resp = router.callPartition("b", 0, new Message.GetCandyRequest("b", "k"));
            assertThat(resp).isInstanceOf(Message.OkResponse.class);
            assertThat(transport.contacted).containsExactly(2002);
            assertThat(sleeps.stream().mapToLong(Long::longValue).sum()).isGreaterThanOrEqualTo(1_000);
        }
    }

    @Test
    void retriesARefusedConnectionToACrashedOwnerUntilTheNewOwnerTakesOver() {
        ManualClock clock = new ManualClock(1_000);
        InMemoryCoordinationService coordination = new InMemoryCoordinationService(clock);
        coordination.registerMember(1, "127.0.0.1:1001".getBytes(StandardCharsets.UTF_8));
        coordination.registerMember(2, "127.0.0.1:2002".getBytes(StandardCharsets.UTF_8));
        coordination.create(CandyboxKeys.boxMetaKey("b"), new byte[0]);
        coordination.tryAcquireLease(CandyboxKeys.ownerResource("b", 0), 1, 1_000);
        ScriptedTransport transport = new ScriptedTransport(new Message.OkResponse());
        transport.refusedPorts.add(1001); // node 1 is down but still holds its lease
        List<Long> sleeps = new ArrayList<>();
        Runnable takeOverOnceLapsed = () -> coordination.tryAcquireLease(
                CandyboxKeys.ownerResource("b", 0), 2, 10_000);
        try (ClusterRouter router = new ClusterRouter(transport, coordination, 5_000, clock, 15_000,
                advancing(clock, sleeps, takeOverOnceLapsed))) {
            Message resp = router.callPartition("b", 0, new Message.PutCandyRequest(
                    "b", "k", null, java.util.Map.of(), null, new byte[] {1}));
            assertThat(resp).isInstanceOf(Message.OkResponse.class);
            assertThat(transport.contacted).containsExactly(2002); // the write was sent once
        }
    }

    @Test
    void anOwnerThatStaysUnreachableFailsWithNotOwnerCarryingTheConnectError() {
        ManualClock clock = new ManualClock(1_000);
        InMemoryCoordinationService coordination = coordinationWithMembers();
        coordination.create(CandyboxKeys.boxMetaKey("b"), new byte[0]);
        coordination.tryAcquireLease(CandyboxKeys.ownerResource("b", 0), 1, 60_000);
        ScriptedTransport transport = new ScriptedTransport(new Message.OkResponse());
        transport.refusedPorts.add(1001);
        List<Long> sleeps = new ArrayList<>();
        try (ClusterRouter router = new ClusterRouter(transport, coordination, 5_000, clock, 2_000,
                advancing(clock, sleeps, () -> { }))) {
            assertThatThrownBy(() -> router.callPartition("b", 0, new Message.GetCandyRequest("b", "k")))
                    .isInstanceOf(NotOwnerException.class)
                    .hasMessageContaining("unreachable")
                    .satisfies(e -> assertThat(e.getSuppressed())
                            .hasAtLeastOneElementOfType(ProtocolException.class));
            assertThat(sleeps.stream().mapToLong(Long::longValue).sum()).isEqualTo(2_000);
        }
    }

    @Test
    void aMidCallFailureOnAPooledConnectionIsNotOwnerAndDropsTheRoute() {
        ManualClock clock = new ManualClock(1_000);
        InMemoryCoordinationService coordination = new InMemoryCoordinationService(clock);
        coordination.registerMember(1, "127.0.0.1:1001".getBytes(StandardCharsets.UTF_8));
        coordination.registerMember(2, "127.0.0.1:2002".getBytes(StandardCharsets.UTF_8));
        coordination.create(CandyboxKeys.boxMetaKey("b"), new byte[0]);
        coordination.tryAcquireLease(CandyboxKeys.ownerResource("b", 0), 1, 1_000);
        ScriptedTransport transport = new ScriptedTransport(new Message.OkResponse());
        try (ClusterRouter router = new ClusterRouter(transport, coordination, 60_000, clock, 15_000,
                advancing(clock, new ArrayList<>(), () -> { }))) {
            assertThat(router.callPartition("b", 0, new Message.GetCandyRequest("b", "k")))
                    .isInstanceOf(Message.OkResponse.class); // pools a connection to node 1

            transport.failingCallPorts.add(1001); // node 1 crashes under the pooled connection
            assertThatThrownBy(() -> router.callPartition("b", 0, new Message.PutCandyRequest(
                    "b", "k", null, java.util.Map.of(), null, new byte[] {1})))
                    .isInstanceOf(NotOwnerException.class)
                    .hasCauseInstanceOf(ProtocolException.class);

            // The cached route was dropped, so the next call re-resolves to the new owner.
            clock.advance(1_000);
            coordination.tryAcquireLease(CandyboxKeys.ownerResource("b", 0), 2, 10_000);
            assertThat(router.callPartition("b", 0, new Message.GetCandyRequest("b", "k")))
                    .isInstanceOf(Message.OkResponse.class);
            assertThat(transport.contacted).containsExactly(1001, 1001, 2002);
        }
    }

    @Test
    void waitsOutAMovedToAnUnregisteredOwner() {
        ManualClock clock = new ManualClock(1_000);
        InMemoryCoordinationService coordination = new InMemoryCoordinationService(clock);
        coordination.registerMember(1, "127.0.0.1:1001".getBytes(StandardCharsets.UTF_8));
        coordination.create(CandyboxKeys.boxMetaKey("b"), new byte[0]);
        coordination.tryAcquireLease(CandyboxKeys.ownerResource("b", 0), 1, 60_000);
        ScriptedTransport transport = new ScriptedTransport(new Message.MovedResponse(2),
                new Message.OkResponse());
        List<Long> sleeps = new ArrayList<>();
        Runnable registerNode2 = () -> coordination.registerMember(2,
                "127.0.0.1:2002".getBytes(StandardCharsets.UTF_8));
        try (ClusterRouter router = new ClusterRouter(transport, coordination, 5_000, clock, 15_000,
                advancing(clock, sleeps, registerNode2))) {
            Message resp = router.callPartition("b", 0, new Message.GetCandyRequest("b", "k"));
            assertThat(resp).isInstanceOf(Message.OkResponse.class);
            assertThat(sleeps).hasSize(1);
            assertThat(transport.contacted).containsExactly(1001, 1001);
        }
    }

    @Test
    void aConnectFailureThatIsNotARefusalFailsFast() {
        ManualClock clock = new ManualClock(1_000);
        InMemoryCoordinationService coordination = coordinationWithMembers();
        coordination.create(CandyboxKeys.boxMetaKey("b"), new byte[0]);
        coordination.tryAcquireLease(CandyboxKeys.ownerResource("b", 0), 1, 60_000);
        ScriptedTransport transport = new ScriptedTransport(new Message.OkResponse());
        transport.refusedPorts.add(1001);
        transport.connectFailureCause = new javax.net.ssl.SSLHandshakeException("bad certificate");
        List<Long> sleeps = new ArrayList<>();
        try (ClusterRouter router = new ClusterRouter(transport, coordination, 5_000, clock, 15_000,
                advancing(clock, sleeps, () -> { }))) {
            assertThatThrownBy(() -> router.callPartition("b", 0, new Message.GetCandyRequest("b", "k")))
                    .isInstanceOf(ProtocolException.class)
                    .hasCauseInstanceOf(javax.net.ssl.SSLHandshakeException.class);
            assertThat(sleeps).isEmpty();
        }
    }

    @Test
    void aNonIoProtocolErrorFromACallPropagatesAndKeepsTheRoute() {
        ManualClock clock = new ManualClock(1_000);
        InMemoryCoordinationService coordination = coordinationWithMembers();
        coordination.create(CandyboxKeys.boxMetaKey("b"), new byte[0]);
        coordination.tryAcquireLease(CandyboxKeys.ownerResource("b", 0), 1, 60_000);
        ScriptedTransport transport = new ScriptedTransport(new Message.OkResponse());
        transport.rejectingCallPorts.add(1001); // e.g. a request over the frame cap
        try (ClusterRouter router = new ClusterRouter(transport, coordination, 60_000, clock, 15_000,
                advancing(clock, new ArrayList<>(), () -> { }))) {
            assertThatThrownBy(() -> router.callPartition("b", 0, new Message.GetCandyRequest("b", "k")))
                    .isExactlyInstanceOf(ProtocolException.class)
                    .hasMessageContaining("exceeds max");

            // The route is still cached: with node 1 deregistered, a re-resolve would find no owner.
            coordination.unregisterMember(1);
            transport.rejectingCallPorts.clear();
            assertThat(router.callPartition("b", 0, new Message.GetCandyRequest("b", "k")))
                    .isInstanceOf(Message.OkResponse.class);
            assertThat(transport.contacted).containsExactly(1001, 1001);
        }
    }
}
