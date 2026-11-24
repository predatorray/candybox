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
import static org.assertj.core.api.Assertions.catchThrowable;

import java.net.NoRouteToHostException;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.IntFunction;
import javax.net.ssl.SSLHandshakeException;
import me.predatorray.candybox.common.ManualClock;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ClusterRouterFailureTest {

    private static final MessageCodec CODEC = new MessageCodec();
    private static final Message REQUEST = new Message.GetCandyRequest("b", "k");
    private static final String OWNER = CandyboxKeys.ownerResource("b", 0);
    private final ManualClock clock = new ManualClock(1_000);
    private final InMemoryCoordinationService coordination = new InMemoryCoordinationService(clock);
    private final List<Long> sleeps = new ArrayList<>();

    private void seedOwner(long ttlMillis) {
        coordination.registerMember(1, "host:1001".getBytes(StandardCharsets.UTF_8));
        coordination.registerMember(2, "host:2002".getBytes(StandardCharsets.UTF_8));
        coordination.create(CandyboxKeys.boxMetaKey("b"), new byte[0]);
        assertThat(coordination.tryAcquireLease(OWNER, 1, ttlMillis)).isPresent();
    }

    private ClusterRouter.Sleeper advancing(Runnable onSleep) {
        return millis -> {
            sleeps.add(millis);
            clock.advance(millis);
            onSleep.run();
        };
    }

    @Test
    void retriesConnectTimeoutUntilNewOwnerTakesOver() {
        assertHandover(new SocketTimeoutException());
    }

    @Test
    void retriesUnroutableOwnerUntilNewOwnerTakesOver() {
        assertHandover(new NoRouteToHostException());
    }

    private void assertHandover(Exception cause) {
        seedOwner(1);
        ScriptedConnection healthy = new ScriptedConnection(null, false);
        RecordingTransport transport = new RecordingTransport(port -> {
            if (port == 1001) {
                throw new ProtocolException("connect failed", cause);
            }
            return healthy;
        });
        try (ClusterRouter router = new ClusterRouter(transport, coordination, 5_000, clock, 2_000,
                advancing(() -> assertThat(coordination.tryAcquireLease(OWNER, 2, 60_000)).isPresent()))) {
            assertThat(router.callPartition("b", 0, REQUEST)).isInstanceOf(Message.OkResponse.class);
            assertThat(sleeps).hasSize(1);
            assertThat(transport.ports).containsExactly(1001, 2002);
            assertThat(healthy.calls).isEqualTo(1);
        }
    }

    @Test
    void connectTimeoutExhaustsRetryBudgetAndPreservesFailure() {
        seedOwner(60_000);
        ProtocolException connectFailure = new ProtocolException("connect failed", new SocketTimeoutException());
        RecordingTransport transport = new RecordingTransport(port -> { throw connectFailure; });
        long budget = 225;
        try (ClusterRouter router = new ClusterRouter(transport, coordination, 5_000, clock, budget,
                advancing(() -> { }))) {
            assertThatThrownBy(() -> router.callPartition("b", 0, REQUEST))
                    .isInstanceOf(NotOwnerException.class)
                    .hasMessageContaining("unreachable")
                    .satisfies(e -> assertThat(e.getSuppressed()).containsExactly(connectFailure));
            assertThat(sleeps).isNotEmpty().allSatisfy(millis -> assertThat(millis).isPositive());
            assertThat(sleeps.stream().mapToLong(Long::longValue).sum()).isEqualTo(budget);
            assertThat(transport.ports).hasSize(sleeps.size() + 1).containsOnly(1001);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void nonNetworkConnectFailurePropagatesWithoutRetry(boolean withTlsCause) {
        seedOwner(60_000);
        ProtocolException failure = withTlsCause
                ? new ProtocolException("TLS handshake failed", new SSLHandshakeException("bad certificate"))
                : new ProtocolException("TLS handshake failed");
        RecordingTransport transport = new RecordingTransport(port -> { throw failure; });
        try (ClusterRouter router = new ClusterRouter(transport, coordination, 5_000, clock, 2_000,
                advancing(() -> { }))) {
            assertThatThrownBy(() -> router.callPartition("b", 0, REQUEST)).isSameAs(failure);
            assertThat(sleeps).isEmpty();
            assertThat(transport.ports).containsExactly(1001);
        }
    }

    @Test
    void interruptWhileWaitingRestoresInterruptFlag() {
        seedOwner(60_000);
        RecordingTransport transport = new RecordingTransport(port -> {
            throw new ProtocolException("connect failed", new SocketTimeoutException());
        });
        try (ClusterRouter router = new ClusterRouter(transport, coordination, 5_000, clock, 2_000,
                millis -> {
                    sleeps.add(millis);
                    throw new InterruptedException();
                })) {
            Throwable failure = catchThrowable(() -> router.callPartition("b", 0, REQUEST));
            boolean interrupted = Thread.interrupted();
            assertThat(failure).isInstanceOf(NotOwnerException.class)
                    .hasMessageContaining("interrupted waiting for an owner");
            assertThat(interrupted).isTrue();
            assertThat(sleeps).hasSize(1);
            assertThat(transport.ports).containsExactly(1001);
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void callAnyThrowsFirstFailureWithOtherMembersSuppressed() {
        seedOwner(60_000);
        coordination.registerMember(3, "host:3003".getBytes(StandardCharsets.UTF_8));
        List<IllegalStateException> failures = new ArrayList<>();
        RecordingTransport transport = new RecordingTransport(port -> {
            IllegalStateException failure = new IllegalStateException("node-" + (port / 1001) + " down");
            failures.add(failure);
            throw failure;
        });
        try (ClusterRouter router = new ClusterRouter(transport, coordination, 5_000, clock)) {
            Throwable failure = catchThrowable(() -> router.callAny(new Message.ListBoxesRequest()));
            assertThat(transport.ports).containsExactlyInAnyOrder(1001, 2002, 3003);
            assertThat(failure).isSameAs(failures.get(0));
            assertThat(failure.getSuppressed()).containsExactlyInAnyOrder(failures.get(1), failures.get(2));
            assertThat(failures).extracting(Throwable::getMessage)
                    .containsExactlyInAnyOrder("node-1 down", "node-2 down", "node-3 down");
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void brokenConnectionIsReplacedEvenWhenCloseFails(boolean closeFails) {
        seedOwner(60_000);
        IllegalStateException reset = new IllegalStateException("reset");
        ScriptedConnection broken = new ScriptedConnection(reset, closeFails);
        ScriptedConnection healthy = new ScriptedConnection(null, false);
        List<Connection> connections = new ArrayList<>(List.of(broken, healthy));
        RecordingTransport transport = new RecordingTransport(port -> connections.remove(0));
        try (ClusterRouter router = new ClusterRouter(transport, coordination, 5_000, clock, 2_000,
                advancing(() -> { }))) {
            assertThatThrownBy(() -> router.callPartition("b", 0, REQUEST)).isSameAs(reset);
            assertThat(transport.ports).containsExactly(1001);
            assertThat(broken.closes).isEqualTo(1);
            assertThat(router.callPartition("b", 0, REQUEST)).isInstanceOf(Message.OkResponse.class);
            assertThat(router.callPartition("b", 0, REQUEST)).isInstanceOf(Message.OkResponse.class);
            assertThat(transport.ports).containsExactly(1001, 1001);
            assertThat(broken.calls).isEqualTo(1);
            assertThat(healthy.calls).isEqualTo(2);
            assertThat(sleeps).isEmpty();
        }
    }

    private static final class RecordingTransport implements Transport {
        final List<Integer> ports = new ArrayList<>();
        private final IntFunction<Connection> connect;

        RecordingTransport(IntFunction<Connection> connect) {
            this.connect = connect;
        }

        @Override
        public Connection connect(String host, int port) {
            ports.add(port);
            return connect.apply(port);
        }

        @Override
        public void close() {
        }
    }

    private static final class ScriptedConnection implements Connection {
        private final RuntimeException callFailure;
        private final boolean closeFails;
        int calls;
        int closes;

        ScriptedConnection(RuntimeException callFailure, boolean closeFails) {
            this.callFailure = callFailure;
            this.closeFails = closeFails;
        }

        @Override
        public Frame call(Frame request) {
            calls++;
            if (callFailure != null) {
                throw callFailure;
            }
            return CODEC.encode(new Message.OkResponse());
        }

        @Override
        public void close() {
            closes++;
            if (closeFails) {
                throw new IllegalStateException("close failed");
            }
        }
    }
}
