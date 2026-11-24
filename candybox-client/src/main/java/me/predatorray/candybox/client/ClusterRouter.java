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

import java.io.IOException;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import me.predatorray.candybox.common.Clock;
import me.predatorray.candybox.common.concurrent.TtlCache;
import me.predatorray.candybox.common.exception.CandyboxException;
import me.predatorray.candybox.common.exception.NotOwnerException;
import me.predatorray.candybox.coordination.CandyboxKeys;
import me.predatorray.candybox.coordination.CoordinationService;
import me.predatorray.candybox.coordination.LeaseInfo;
import me.predatorray.candybox.protocol.Frame;
import me.predatorray.candybox.protocol.Message;
import me.predatorray.candybox.protocol.MessageCodec;
import me.predatorray.candybox.protocol.ProtocolException;
import me.predatorray.candybox.protocol.transport.Transport;

/**
 * A cluster-aware {@link Router}: it resolves a (Box, partition) to its owning node via the
 * per-partition coordination lease, maps the owner's node id to its advertised {@code host:port} via
 * membership, connects there, and re-routes on a {@code MOVED} response (using the named owner). A
 * partition without a reachable owner (no lease holder, a holder that is no longer a member or
 * refuses connections, or an {@code UNAVAILABLE} answer) is a handover gap: it is re-resolved with
 * exponential backoff for up to {@code unownedRetryMillis}, then fails with
 * {@link NotOwnerException}.
 * Partition→address resolutions are cached with a TTL and invalidated on redirect; a bounded connection pool is
 * kept per node address. Requests that any node can serve rotate across members and fail over to the
 * next member when one is unreachable.
 */
final class ClusterRouter implements Router {

    private static final int MAX_ATTEMPTS = 4;
    /** First client-side backoff; a larger {@code UNAVAILABLE} retry hint from the server wins. */
    private static final long INITIAL_BACKOFF_MILLIS = 50;
    private static final long MAX_BACKOFF_MILLIS = 1_000;

    /** Blocks the calling thread; injectable so tests can advance a manual clock instead. */
    @FunctionalInterface
    interface Sleeper {
        void sleep(long millis) throws InterruptedException;
    }

    private final CoordinationService coordination;
    private final MessageCodec codec = new MessageCodec();
    private final Clock clock;
    private final Sleeper sleeper;
    private final long unownedRetryMillis;

    private final TtlCache<String, NodeAddress> partitionCache;
    private final RpcResources resources;
    private final AtomicInteger nextMember = new AtomicInteger();

    /** A router that fails at once on an unowned partition (no handover-gap retry). */
    ClusterRouter(Transport transport, CoordinationService coordination, long cacheTtlMillis,
                  Clock clock) {
        this(transport, coordination, cacheTtlMillis, clock, 0, Thread::sleep);
    }

    ClusterRouter(Transport transport, CoordinationService coordination, long cacheTtlMillis,
                  Clock clock, long unownedRetryMillis, Sleeper sleeper) {
        this(transport, coordination, cacheTtlMillis, clock, unownedRetryMillis, sleeper,
                4, 64, 5_000, 120_000);
    }

    ClusterRouter(Transport transport, CoordinationService coordination, long cacheTtlMillis,
                  Clock clock, long unownedRetryMillis, Sleeper sleeper, int connectionsPerNode,
                  int maxCalls, long acquireMillis, long requestMillis) {
        this.resources = new RpcResources(transport, connectionsPerNode, maxCalls,
                acquireMillis, requestMillis);
        this.coordination = coordination;
        this.partitionCache = new TtlCache<>(clock, cacheTtlMillis);
        this.clock = clock;
        this.sleeper = sleeper;
        this.unownedRetryMillis = unownedRetryMillis;
    }

    @Override
    public Message callPartition(String box, int partition, Message request) {
        return resources.call(() -> routePartition(box, partition, request));
    }

    private Message routePartition(String box, int partition, Message request) {
        String cacheKey = box + "#" + partition;
        long deadline = clock.currentTimeMillis() + unownedRetryMillis;
        long backoff = INITIAL_BACKOFF_MILLIS;
        while (true) {
            OwnerGap gap;
            long retryAfter = 0;
            try {
                Message response = sendToOwner(box, partition, cacheKey, request);
                if (!(response instanceof Message.UnavailableResponse unavailable)) {
                    return response;
                }
                // The node checked the Box still exists, so skip that check below.
                gap = new OwnerGap("has no current owner", null, true);
                retryAfter = unavailable.retryAfterMillis();
            } catch (OwnerGap g) {
                gap = g;
            }
            partitionCache.invalidate(cacheKey);
            long remaining = deadline - clock.currentTimeMillis();
            // A deleted Box has no owner either; don't wait out the budget for it.
            if (remaining <= 0 || (!gap.boxConfirmed && !boxExists(box))) {
                NotOwnerException failure = new NotOwnerException("box " + box + " partition "
                        + partition + " " + gap.getMessage());
                if (gap.getCause() != null) {
                    failure.addSuppressed(gap.getCause());
                }
                throw failure;
            }
            try {
                sleeper.sleep(Math.min(Math.max(backoff, retryAfter), remaining));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new NotOwnerException("box " + box + " partition " + partition
                        + ": interrupted waiting for an owner");
            }
            backoff = Math.min(backoff * 2, MAX_BACKOFF_MILLIS);
        }
    }

    /**
     * Sends to the partition's owner, following {@code MOVED}. Throws {@link OwnerGap} when the owner
     * (or a redirect target) is unregistered or refuses connections: nothing was sent, so it is safe
     * to retry. An I/O failure mid-call is not retried (a write may have applied) but surfaces as a
     * retriable {@link NotOwnerException}.
     */
    private Message sendToOwner(String box, int partition, String cacheKey, Message request) {
        NodeAddress address = resolveOwner(box, partition, cacheKey);
        for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
            Message response;
            try {
                response = send(address, request);
            } catch (RpcResources.SetupFailure e) {
                throw connectFailure(address, e.failure());
            } catch (ProtocolException e) {
                if (!(e.getCause() instanceof IOException)) {
                    throw e; // a frame-limit or decode error is not an ownership problem
                }
                partitionCache.invalidate(cacheKey);
                NotOwnerException failure = new NotOwnerException("box " + box + " partition "
                        + partition + ": request to " + address.key() + " failed");
                failure.initCause(e);
                throw failure;
            }
            if (response instanceof Message.MovedResponse moved) {
                address = memberAddress(moved.ownerNodeId());
                partitionCache.put(cacheKey, address);
                continue;
            }
            return response;
        }
        throw new NotOwnerException("box " + box + " partition " + partition
                + ": exceeded routing attempts");
    }

    @Override
    public Message callAny(Message request) {
        return resources.call(() -> routeAny(request));
    }

    private Message routeAny(Message request) {
        List<Integer> members = coordination.members();
        if (members.isEmpty()) {
            throw new CandyboxException("No cluster members to route to");
        }
        int start = Math.floorMod(nextMember.getAndIncrement(), members.size());
        RuntimeException failure = null;
        for (int i = 0; i < members.size(); i++) {
            int nodeId = members.get((start + i) % members.size());
            try {
                NodeAddress address;
                try {
                    address = addressOfNode(nodeId);
                } catch (RuntimeException e) {
                    // Membership may change after enumeration. Nothing has been dispatched.
                    throw new RpcResources.SetupFailure(e);
                }
                return send(address, request);
            } catch (RpcResources.SetupFailure setup) {
                RuntimeException e = setup.failure();
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        throw failure;
    }

    private boolean boxExists(String box) {
        return coordination.get(CandyboxKeys.boxMetaKey(box)).isPresent();
    }

    /** The partition owner's address, or throws {@link OwnerGap} if there is none right now. */
    private NodeAddress resolveOwner(String box, int partition, String cacheKey) {
        NodeAddress cached = partitionCache.getIfFresh(cacheKey).orElse(null);
        if (cached != null) {
            return cached;
        }
        LeaseInfo holder = coordination.leaseHolder(CandyboxKeys.ownerResource(box, partition))
                .orElseThrow(() -> new OwnerGap("has no current owner", null, false));
        NodeAddress address = memberAddress(holder.ownerNodeId());
        partitionCache.put(cacheKey, address);
        return address;
    }

    /** A partition owner's address; an unregistered owner (it crashed) is an {@link OwnerGap}. */
    private NodeAddress memberAddress(int nodeId) {
        byte[] info = coordination.memberInfo(nodeId)
                .orElseThrow(() -> new OwnerGap("owner node " + nodeId + " is not registered", null,
                        false));
        return NodeAddress.parse(new String(info, StandardCharsets.UTF_8));
    }

    /**
     * Opens (or reuses) a pooled connection. A refused, unroutable or timed-out connect (a crashed
     * owner whose lease has not lapsed yet) is an {@link OwnerGap}; anything else, such as a TLS or
     * SASL failure, propagates.
     */
    private RuntimeException connectFailure(NodeAddress address, RuntimeException e) {
        if (e instanceof ProtocolException && (e.getCause() instanceof ConnectException
                || e.getCause() instanceof NoRouteToHostException
                || e.getCause() instanceof SocketTimeoutException)) {
            return new OwnerGap("owner at " + address.key() + " is unreachable", e, false);
        }
        return e;
    }

    private NodeAddress addressOfNode(int nodeId) {
        byte[] info = coordination.memberInfo(nodeId)
                .orElseThrow(() -> new NotOwnerException("node " + nodeId + " is not registered"));
        return NodeAddress.parse(new String(info, StandardCharsets.UTF_8));
    }

    private Message send(NodeAddress address, Message request) {
        return resources.send(address.host(), address.port(), connection -> {
            Frame response = connection.call(codec.encode(request));
            return codec.decode(response);
        });
    }

    @Override
    public void close() {
        resources.close();
    }

    /** No reachable owner right now; {@code boxConfirmed} if a node answered that the Box exists. */
    private static final class OwnerGap extends RuntimeException {
        final boolean boxConfirmed;

        OwnerGap(String reason, Throwable cause, boolean boxConfirmed) {
            super(reason, cause, false, false);
            this.boxConfirmed = boxConfirmed;
        }
    }

    private record NodeAddress(String host, int port) {
        String key() {
            return host + ":" + port;
        }

        static NodeAddress parse(String hostPort) {
            int colon = hostPort.lastIndexOf(':');
            if (colon < 0) {
                throw new NotOwnerException("Unroutable member address: " + hostPort);
            }
            return new NodeAddress(hostPort.substring(0, colon),
                    Integer.parseInt(hostPort.substring(colon + 1)));
        }
    }
}
