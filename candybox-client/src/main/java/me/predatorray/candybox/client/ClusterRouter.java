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
import me.predatorray.candybox.common.Clock;
import me.predatorray.candybox.common.exception.CandyboxException;
import me.predatorray.candybox.common.exception.NotOwnerException;
import me.predatorray.candybox.coordination.CandyboxKeys;
import me.predatorray.candybox.coordination.CoordinationService;
import me.predatorray.candybox.coordination.LeaseInfo;
import me.predatorray.candybox.protocol.Frame;
import me.predatorray.candybox.protocol.Message;
import me.predatorray.candybox.protocol.MessageCodec;
import me.predatorray.candybox.protocol.transport.Connection;
import me.predatorray.candybox.protocol.transport.Transport;

/**
 * A cluster-aware {@link Router}: it resolves a (Box, partition) to its owning node via the
 * per-partition coordination lease, maps the owner's node id to its advertised {@code host:port} via
 * membership, connects there, and re-routes on a {@code MOVED} response (using the named owner).
 * Partition→address resolutions are cached with a TTL and invalidated on redirect; one connection is
 * kept per node address.
 */
final class ClusterRouter implements Router {

    private static final int MAX_ATTEMPTS = 4;

    private final Transport transport;
    private final CoordinationService coordination;
    private final long cacheTtlMillis;
    private final Clock clock;
    private final MessageCodec codec = new MessageCodec();

    private final ConcurrentMap<String, CachedAddress> partitionCache = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Connection> connections = new ConcurrentHashMap<>();

    ClusterRouter(Transport transport, CoordinationService coordination, long cacheTtlMillis,
                  Clock clock) {
        this.transport = transport;
        this.coordination = coordination;
        this.cacheTtlMillis = cacheTtlMillis;
        this.clock = clock;
    }

    @Override
    public Message callPartition(String box, int partition, Message request) {
        String cacheKey = box + "#" + partition;
        NodeAddress address = resolveOwner(box, partition, cacheKey);
        for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
            Message response = send(address, request);
            if (response instanceof Message.MovedResponse moved) {
                address = addressOfNode(moved.ownerNodeId());
                partitionCache.put(cacheKey,
                        new CachedAddress(address, clock.currentTimeMillis() + cacheTtlMillis));
                continue;
            }
            return response;
        }
        throw new NotOwnerException("box " + box + " partition " + partition
                + ": exceeded routing attempts");
    }

    @Override
    public Message callAny(Message request) {
        return send(anyMember(), request);
    }

    private NodeAddress resolveOwner(String box, int partition, String cacheKey) {
        CachedAddress cached = partitionCache.get(cacheKey);
        if (cached != null && clock.currentTimeMillis() < cached.expiry()) {
            return cached.address();
        }
        LeaseInfo holder = coordination.leaseHolder(CandyboxKeys.ownerResource(box, partition))
                .orElseThrow(() -> new NotOwnerException("box " + box + " partition " + partition
                        + " has no current owner"));
        NodeAddress address = addressOfNode(holder.ownerNodeId());
        partitionCache.put(cacheKey,
                new CachedAddress(address, clock.currentTimeMillis() + cacheTtlMillis));
        return address;
    }

    private NodeAddress addressOfNode(int nodeId) {
        byte[] info = coordination.memberInfo(nodeId)
                .orElseThrow(() -> new NotOwnerException("node " + nodeId + " is not registered"));
        return NodeAddress.parse(new String(info, StandardCharsets.UTF_8));
    }

    private NodeAddress anyMember() {
        List<Integer> members = coordination.members();
        if (members.isEmpty()) {
            throw new CandyboxException("No cluster members to route to");
        }
        return addressOfNode(members.get(0));
    }

    private Message send(NodeAddress address, Message request) {
        Connection connection = connections.computeIfAbsent(address.key(),
                k -> transport.connect(address.host(), address.port()));
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

    private record CachedAddress(NodeAddress address, long expiry) {
    }
}
