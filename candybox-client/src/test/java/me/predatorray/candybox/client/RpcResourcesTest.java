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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import me.predatorray.candybox.common.exception.BusyException;
import me.predatorray.candybox.protocol.Frame;
import me.predatorray.candybox.protocol.ProtocolException;
import me.predatorray.candybox.protocol.transport.Connection;
import me.predatorray.candybox.protocol.transport.Transport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(10)
class RpcResourcesTest {
    private static final class BlockingTransport implements Transport {
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final AtomicInteger opened = new AtomicInteger();
        final AtomicInteger closed = new AtomicInteger();

        @Override
        public Connection connect(String host, int port) {
            int index = opened.incrementAndGet();
            return new Connection() {
                @Override
                public Frame call(Frame request) {
                    if (index == 1) {
                        entered.countDown();
                        try {
                            release.await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new ProtocolException("interrupted", e);
                        }
                    }
                    return request;
                }
                @Override
                public void close() {
                    closed.incrementAndGet();
                    if (index == 1) { release.countDown(); }
                }
            };
        }
        @Override
        public void close() { }
    }

    private static Object send(RpcResources resources) {
        return resources.call(() -> resources.send("host", 1, c -> c.call(null)));
    }

    @Test
    void stalledBorrowerDoesNotBlockAnotherSlot() throws Exception {
        BlockingTransport transport = new BlockingTransport();
        var callers = Executors.newCachedThreadPool();
        try (RpcResources resources = new RpcResources(transport, 2, 8, 100, 5_000)) {
            var stalled = callers.submit(() -> send(resources));
            assertThat(transport.entered.await(2, TimeUnit.SECONDS)).isTrue();
            callers.submit(() -> send(resources)).get(2, TimeUnit.SECONDS);
            assertThat(transport.opened).hasValue(2);
            assertThat(stalled.isDone()).isFalse();
            transport.release.countDown();
            stalled.get(2, TimeUnit.SECONDS);
        } finally {
            callers.shutdownNow();
        }
    }

    @Test
    void acquisitionWaitersExpireWithoutClosingBorrowerOrLeakingSlots() throws Exception {
        BlockingTransport transport = new BlockingTransport();
        var callers = Executors.newCachedThreadPool();
        try (RpcResources resources = new RpcResources(transport, 1, 8, 100, 5_000)) {
            var stalled = callers.submit(() -> send(resources));
            assertThat(transport.entered.await(2, TimeUnit.SECONDS)).isTrue();
            var waiter1 = callers.submit(() -> assertThatThrownBy(() -> send(resources))
                    .isInstanceOf(ProtocolException.class).hasMessageContaining("acquisition"));
            var waiter2 = callers.submit(() -> assertThatThrownBy(() -> send(resources))
                    .isInstanceOf(ProtocolException.class).hasMessageContaining("acquisition"));
            waiter1.get(2, TimeUnit.SECONDS);
            waiter2.get(2, TimeUnit.SECONDS);
            assertThat(transport.closed).hasValue(0);
            assertThat(stalled.isDone()).isFalse();
            transport.release.countDown();
            stalled.get(2, TimeUnit.SECONDS);
            send(resources);
            assertThat(transport.opened).hasValue(1);
        } finally {
            callers.shutdownNow();
        }
    }

    @Test
    void overallDeadlineCancelsOwnedCallAndConnectionIsReplaced() {
        BlockingTransport transport = new BlockingTransport();
        try (RpcResources resources = new RpcResources(transport, 1, 8, 100, 200)) {
            assertThatThrownBy(() -> send(resources)).isInstanceOf(ProtocolException.class)
                    .hasMessageContaining("deadline");
            // Cancellation closes the socket and wakes the worker; wait for its slot to be returned.
            assertThat(transport.closed.get()).isGreaterThanOrEqualTo(1);
            send(resources);
            assertThat(transport.opened).hasValue(2);
        }
    }

    @Test
    void admissionRejectsExcessCallsWithoutAnUnboundedQueue() throws Exception {
        BlockingTransport transport = new BlockingTransport();
        var callers = Executors.newCachedThreadPool();
        try (RpcResources resources = new RpcResources(transport, 1, 1, 100, 5_000)) {
            var stalled = callers.submit(() -> send(resources));
            assertThat(transport.entered.await(2, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> send(resources)).isInstanceOf(BusyException.class);
            assertThat(transport.opened).hasValue(1);
            transport.release.countDown();
            stalled.get(2, TimeUnit.SECONDS);
        } finally {
            callers.shutdownNow();
        }
    }

    @Test
    void shutdownWakesBorrowerAndWaiterAndRejectsNewCalls() throws Exception {
        BlockingTransport transport = new BlockingTransport();
        var callers = Executors.newCachedThreadPool();
        RpcResources resources = new RpcResources(transport, 1, 8, 5_000, 5_000);
        try {
            var stalled = callers.submit(() -> {
                try { send(resources); } catch (ProtocolException ignored) { }
            });
            assertThat(transport.entered.await(2, TimeUnit.SECONDS)).isTrue();
            var waiter = callers.submit(() -> {
                try { send(resources); } catch (ProtocolException ignored) { }
            });
            resources.close();
            stalled.get(2, TimeUnit.SECONDS);
            waiter.get(2, TimeUnit.SECONDS);
            assertThatThrownBy(() -> send(resources)).isInstanceOf(ProtocolException.class)
                    .hasMessageContaining("closed");
            assertThat(transport.closed.get()).isPositive();
        } finally {
            resources.close();
            callers.shutdownNow();
        }
    }

    @Test
    void interruptionCancelsOnlyTheInterruptedCaller() throws Exception {
        BlockingTransport transport = new BlockingTransport();
        try (RpcResources resources = new RpcResources(transport, 1, 8, 5_000, 5_000)) {
            CountDownLatch interrupted = new CountDownLatch(1);
            Thread caller = new Thread(() -> {
                try { send(resources); } catch (ProtocolException e) {
                    if (Thread.currentThread().isInterrupted()) { interrupted.countDown(); }
                }
            });
            caller.start();
            assertThat(transport.entered.await(2, TimeUnit.SECONDS)).isTrue();
            caller.interrupt();
            assertThat(interrupted.await(2, TimeUnit.SECONDS)).isTrue();
            caller.join(2_000);
            assertThat(transport.closed.get()).isPositive();
        }
    }

    @Test
    void lateConnectionSetupIsClosedAfterDeadline() throws Exception {
        CountDownLatch setup = new CountDownLatch(1);
        CountDownLatch finish = new CountDownLatch(1);
        CountDownLatch closed = new CountDownLatch(1);
        Transport transport = new Transport() {
            public Connection connect(String host, int port) {
                setup.countDown();
                boolean done = false;
                while (!done) {
                    try { finish.await(); done = true; } catch (InterruptedException ignored) { }
                }
                return new Connection() {
                    public Frame call(Frame frame) { throw new AssertionError("cancelled setup dispatched"); }
                    public void close() { closed.countDown(); }
                };
            }
            public void close() { }
        };
        try (RpcResources resources = new RpcResources(transport, 1, 2, 100, 200)) {
            assertThatThrownBy(() -> send(resources)).isInstanceOf(ProtocolException.class)
                    .hasMessageContaining("deadline");
            assertThat(setup.getCount()).isZero();
            finish.countDown();
            assertThat(closed.await(2, TimeUnit.SECONDS)).isTrue();
        } finally { finish.countDown(); }
    }
    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void deadlineClosesRealSocketDuringBlockedReadOrWrite(boolean largeWrite) throws Exception {
        var peers = Executors.newSingleThreadExecutor();
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch accepted = new CountDownLatch(1);
        try (java.net.ServerSocket server = new java.net.ServerSocket(0);
             var tcp = new me.predatorray.candybox.protocol.transport.TcpTransport(
                     new me.predatorray.candybox.protocol.FrameCodec(), null, true, 1_000, 0);
             RpcResources resources = new RpcResources(tcp, 1, 2, 100, 300)) {
            var peer = peers.submit(() -> {
                try (var socket = server.accept()) {
                    socket.setReceiveBufferSize(1_024);
                    accepted.countDown();
                    release.await(); // Never read the request or send a response.
                }
                return null;
            });
            Frame request = new Frame(me.predatorray.candybox.protocol.Opcode.LIST_BOXES,
                    new byte[largeWrite ? 16 * 1024 * 1024 : 0]);
            assertThatThrownBy(() -> resources.call(() -> resources.send("127.0.0.1",
                    server.getLocalPort(), c -> c.call(request))))
                    .isInstanceOf(ProtocolException.class).hasMessageContaining("deadline");
            assertThat(accepted.getCount()).isZero();
            release.countDown();
            peer.get(2, TimeUnit.SECONDS);
        } finally {
            release.countDown();
            peers.shutdownNow();
        }
    }

    @Test
    void eachPoolSlotCompletesItsOwnSaslHandshake() throws Exception {
        var codec = new me.predatorray.candybox.protocol.MessageCodec();
        var base = new BlockingTransport();
        AtomicInteger handshakes = new AtomicInteger();
        Transport saslServer = new Transport() {
            public Connection connect(String host, int port) {
                Connection data = base.connect(host, port);
                return new Connection() {
                    boolean authenticated;
                    public Frame call(Frame frame) {
                        var message = codec.decode(frame);
                        if (message instanceof me.predatorray.candybox.protocol.Message.SaslHandshakeRequest) {
                            handshakes.incrementAndGet();
                            return codec.encode(new me.predatorray.candybox.protocol.Message
                                    .SaslHandshakeResponse(true, java.util.List.of("PLAIN")));
                        }
                        if (message instanceof me.predatorray.candybox.protocol.Message.SaslAuthenticateRequest) {
                            authenticated = true;
                            return codec.encode(new me.predatorray.candybox.protocol.Message
                                    .SaslAuthenticateResponse(true, new byte[0]));
                        }
                        assertThat(authenticated).isTrue();
                        return data.call(frame);
                    }
                    public void close() { data.close(); }
                };
            }
            public void close() { }
        };
        var callers = Executors.newCachedThreadPool();
        try (var auth = new me.predatorray.candybox.protocol.auth.AuthenticatingTransport(
                saslServer, "PLAIN", "user", "password");
             RpcResources resources = new RpcResources(auth, 2, 8, 100, 5_000)) {
            Frame request = codec.encode(new me.predatorray.candybox.protocol.Message.ListBoxesRequest());
            java.util.concurrent.Callable<Frame> send = () -> resources.call(
                    () -> resources.send("host", 1, c -> c.call(request)));
            var stalled = callers.submit(send);
            assertThat(base.entered.await(2, TimeUnit.SECONDS)).isTrue();
            callers.submit(send).get(2, TimeUnit.SECONDS);
            assertThat(handshakes).hasValue(2);
            base.release.countDown();
            stalled.get(2, TimeUnit.SECONDS);
        } finally { callers.shutdownNow(); }
    }

}
