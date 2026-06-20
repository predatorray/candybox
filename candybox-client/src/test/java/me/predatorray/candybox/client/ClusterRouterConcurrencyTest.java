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

import java.nio.charset.StandardCharsets;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import me.predatorray.candybox.common.SystemClock;
import me.predatorray.candybox.coordination.CandyboxKeys;
import me.predatorray.candybox.coordination.fake.InMemoryCoordinationService;
import me.predatorray.candybox.protocol.Frame;
import me.predatorray.candybox.protocol.Message;
import me.predatorray.candybox.protocol.MessageCodec;
import me.predatorray.candybox.protocol.transport.Connection;
import me.predatorray.candybox.protocol.transport.Transport;
import org.junit.jupiter.api.Test;

/**
 * Guards the fix for the blocking-{@code computeIfAbsent} defect: {@link ClusterRouter} must open
 * connections <em>outside</em> the connection map's locks. If {@code connect()} ran inside
 * {@link java.util.concurrent.ConcurrentMap#computeIfAbsent}, two callers racing on the same cold
 * address could never have their connects in flight at the same time (the second blocks on the bin
 * lock the first holds), and the barrier below would never trip.
 */
class ClusterRouterConcurrencyTest {

    private static final MessageCodec CODEC = new MessageCodec();

    @Test
    void connectsForTheSameAddressAreNotSerializedByThePoolLock() throws Exception {
        InMemoryCoordinationService coordination = new InMemoryCoordinationService();
        coordination.registerMember(2, "127.0.0.1:2002".getBytes(StandardCharsets.UTF_8));
        coordination.tryAcquireLease(CandyboxKeys.ownerResource("b", 0), 2, 60_000);

        // connect() parks on a 2-party barrier: it can only complete if BOTH callers are inside
        // connect() simultaneously — i.e. neither is blocked behind a map lock held by the other.
        CyclicBarrier bothConnecting = new CyclicBarrier(2);
        AtomicInteger connectStarts = new AtomicInteger();
        AtomicInteger closes = new AtomicInteger();

        Transport barrierTransport = new Transport() {
            @Override
            public Connection connect(String host, int port) {
                connectStarts.incrementAndGet();
                try {
                    bothConnecting.await(5, TimeUnit.SECONDS);
                } catch (TimeoutException | BrokenBarrierException e) {
                    throw new AssertionError("connects were serialized — connect() ran under a lock", e);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
                return new Connection() {
                    @Override
                    public Frame call(Frame request) {
                        return CODEC.encode(new Message.OkResponse());
                    }

                    @Override
                    public void close() {
                        closes.incrementAndGet();
                    }
                };
            }

            @Override
            public void close() {
            }
        };

        try (ClusterRouter router = new ClusterRouter(barrierTransport, coordination, 60_000,
                SystemClock.INSTANCE)) {
            CyclicBarrier start = new CyclicBarrier(2);
            Runnable call = () -> {
                try {
                    start.await();
                } catch (InterruptedException | BrokenBarrierException e) {
                    throw new RuntimeException(e);
                }
                Message resp = router.callPartition("b", 0, new Message.GetCandyRequest("b", "k"));
                assertThat(resp).isInstanceOf(Message.OkResponse.class);
            };
            Thread a = new Thread(call, "router-a");
            Thread b = new Thread(call, "router-b");
            a.start();
            b.start();
            a.join(10_000);
            b.join(10_000);
            assertThat(a.isAlive()).as("thread a finished").isFalse();
            assertThat(b.isAlive()).as("thread b finished").isFalse();

            // Both callers connected concurrently (the barrier tripped), and exactly one surplus
            // connection — the loser of the putIfAbsent race — was closed.
            assertThat(connectStarts).hasValue(2);
            assertThat(closes).hasValue(1);

            // The pooled connection is reused on a later call: no new connect.
            router.callPartition("b", 0, new Message.GetCandyRequest("b", "k"));
            assertThat(connectStarts).hasValue(2);
        }
    }
}
