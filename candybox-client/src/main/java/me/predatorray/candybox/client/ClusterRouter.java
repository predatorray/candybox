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

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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
import me.predatorray.candybox.protocol.transport.Connection;
import me.predatorray.candybox.protocol.transport.Transport;

/**
 * A cluster-aware {@link Router}: it resolves a (Box, partition) to its owning node via the
 * per-partition coordination lease, maps the owner's node id to its advertised {@code host:port} via
 * membership, connects there, and re-routes on a {@code MOVED} response (using the named owner). A
 * partition without a reachable owner (no lease holder, a holder that is no longer a member or
 * refuses connections, or an {@code UNAVAILABLE} answer) is a handover gap: it is re-resolved with
 * exponential backoff for up to {@code unownedRetryMillis}, then fails with
 * {@link NotOwnerException}.
 * Partition→address resolutions are cached with a TTL and invalidated on redirect; one connection is
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

    private final Transport transport;
    private final CoordinationService coordination;
    private final MessageCodec codec = new MessageCodec();
    private final Clock clock;
    private final Sleeper sleeper;
    private final long unownedRetryMillis;

    private final TtlCache<String, NodeAddress> partitionCache;
    private final ConcurrentMap<String, Connection> connections = new ConcurrentHashMap<>();
    private final AtomicInteger nextMember = new AtomicInteger();

    /** A router that fails at once on an unowned partition (no handover-gap retry). */
    ClusterRouter(Transport transport, CoordinationService coordination, long cacheTtlMillis,
                  Clock clock) {
        this(transport, coordination, cacheTtlMillis, clock, 0, Thread::sleep);
    }

    ClusterRouter(Transport transport, CoordinationService coordination, long cacheTtlMillis,
                  Clock clock, long unownedRetryMillis, Sleeper sleeper) {
        this.transport = transport;
        this.coordination = coordination;
        this.partitionCache = new TtlCache<>(clock, cacheTtlMillis);
        this.clock = clock;
        this.sleeper = sleeper;
        this.unownedRetryMillis = unownedRetryMillis;
    }

    @Override
    public Message callPartition(String box, int partition, Message request) {
        String cacheKey = box + "#" + partition;
        long deadline = clock.currentTimeMillis() + unownedRetryMillis;
        long backoff = INITIAL_BACKOFF_MILLIS;
        while (true) {
            Route route = resolveOwner(box, partition, cacheKey);
            String gap = route.gap();
            RuntimeException connectFailure = null;
            boolean unavailableAnswer = false;
            long retryAfter = 0;
            if (route.address() != null) {
                // Connecting sends nothing, so a refused connection (e.g. a crashed owner whose lease
                // has not lapsed yet) is safe to retry even for writes.
                connectFailure = tryConnect(route.address());
                if (connectFailure == null) {
                    Message response = sendFollowingMoves(box, partition, cacheKey, route.address(),
                            request);
                    if (!(response instanceof Message.UnavailableResponse unavailable)) {
                        return response;
                    }
                    unavailableAnswer = true; // the node checked the Box still exists
                    retryAfter = unavailable.retryAfterMillis();
                    gap = "has no current owner";
                } else {
                    gap = "owner at " + route.address().key() + " is unreachable";
                }
                partitionCache.invalidate(cacheKey);
            }
            long remaining = deadline - clock.currentTimeMillis();
            // A deleted Box has no owner either; don't wait out the budget for it.
            if (remaining <= 0 || (!unavailableAnswer && !boxExists(box))) {
                NotOwnerException failure = new NotOwnerException("box " + box + " partition "
                        + partition + " " + gap);
                if (connectFailure != null) {
                    failure.addSuppressed(connectFailure);
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

    private Message sendFollowingMoves(String box, int partition, String cacheKey,
                                       NodeAddress address, Message request) {
        for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
            Message response = send(address, request);
            if (response instanceof Message.MovedResponse moved) {
                address = addressOfNode(moved.ownerNodeId());
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
        List<Integer> members = coordination.members();
        if (members.isEmpty()) {
            throw new CandyboxException("No cluster members to route to");
        }
        int start = Math.floorMod(nextMember.getAndIncrement(), members.size());
        RuntimeException failure = null;
        for (int i = 0; i < members.size(); i++) {
            int nodeId = members.get((start + i) % members.size());
            try {
                return send(addressOfNode(nodeId), request);
            } catch (RuntimeException e) {
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

    /** The partition owner's address, or why there is none right now. */
    private Route resolveOwner(String box, int partition, String cacheKey) {
        NodeAddress cached = partitionCache.getIfFresh(cacheKey).orElse(null);
        if (cached != null) {
            return new Route(cached, null);
        }
        LeaseInfo holder = coordination.leaseHolder(CandyboxKeys.ownerResource(box, partition))
                .orElse(null);
        if (holder == null) {
            return new Route(null, "has no current owner");
        }
        byte[] info = coordination.memberInfo(holder.ownerNodeId()).orElse(null);
        if (info == null) {
            // The holder's session is gone (it crashed) but its lease has not lapsed yet.
            return new Route(null, "owner node " + holder.ownerNodeId() + " is not registered");
        }
        NodeAddress address = NodeAddress.parse(new String(info, StandardCharsets.UTF_8));
        partitionCache.put(cacheKey, address);
        return new Route(address, null);
    }

    /** Opens (or reuses) a pooled connection; returns the failure if the node refuses it. */
    private RuntimeException tryConnect(NodeAddress address) {
        try {
            acquireConnection(address);
            return null;
        } catch (ProtocolException e) {
            return e;
        }
    }

    private NodeAddress addressOfNode(int nodeId) {
        byte[] info = coordination.memberInfo(nodeId)
                .orElseThrow(() -> new NotOwnerException("node " + nodeId + " is not registered"));
        return NodeAddress.parse(new String(info, StandardCharsets.UTF_8));
    }

    private Message send(NodeAddress address, Message request) {
        Connection connection = acquireConnection(address);
        try {
            Frame response = connection.call(codec.encode(request));
            return codec.decode(response);
        } catch (RuntimeException e) {
            // Drop a broken connection so it is reopened on the next attempt.
            Connection broken = connections.remove(address.key());
            if (broken != null) {
                try {
                    broken.close();
                } catch (RuntimeException ignored) {
                    // best effort
                }
            }
            throw e;
        }
    }

    /**
     * Returns a pooled connection to {@code address}, opening one on a miss. The blocking
     * {@link Transport#connect} runs <em>outside</em> the connection map's locks: doing it inside
     * {@link ConcurrentMap#computeIfAbsent} would hold a bin lock across network I/O (stalling
     * unrelated keys that hash to the same bin) and is forbidden from updating the map reentrantly.
     * The cost of this lock-free approach is that two callers racing on a cold address may both
     * connect; the loser's surplus connection is closed and the winner's is shared.
     */
    private Connection acquireConnection(NodeAddress address) {
        String key = address.key();
        Connection existing = connections.get(key);
        if (existing != null) {
            return existing;
        }
        Connection fresh = transport.connect(address.host(), address.port());
        Connection raced = connections.putIfAbsent(key, fresh);
        if (raced != null) {
            try {
                fresh.close();
            } catch (RuntimeException ignored) {
                // best effort: discard the connection that lost the race
            }
            return raced;
        }
        return fresh;
    }

    @Override
    public void close() {
        for (Connection connection : connections.values()) {
            try {
                connection.close();
            } catch (RuntimeException ignored) {
                // best effort
            }
        }
        connections.clear();
    }

    private record Route(NodeAddress address, String gap) {
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
