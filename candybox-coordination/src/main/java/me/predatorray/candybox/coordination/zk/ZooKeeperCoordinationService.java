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
package me.predatorray.candybox.coordination.zk;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import me.predatorray.candybox.common.Clock;
import me.predatorray.candybox.common.SystemClock;
import me.predatorray.candybox.common.serial.BinaryReader;
import me.predatorray.candybox.common.serial.BinaryWriter;
import me.predatorray.candybox.coordination.CasConflictException;
import me.predatorray.candybox.coordination.CoordinationException;
import me.predatorray.candybox.coordination.CoordinationService;
import me.predatorray.candybox.coordination.Lease;
import me.predatorray.candybox.coordination.LeaseExpiredException;
import me.predatorray.candybox.coordination.LeaseInfo;
import me.predatorray.candybox.coordination.VersionedValue;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

/**
 * ZooKeeper-backed {@link CoordinationService} (Apache Curator). It is the production counterpart of
 * {@link me.predatorray.candybox.coordination.fake.InMemoryCoordinationService} and passes the same
 * {@code CoordinationServiceContract}.
 *
 * <p>Mapping:
 * <ul>
 *   <li><b>Versioned KV</b> → a znode per key; ZooKeeper's {@code dataVersion} is the CAS version, and
 *       {@code setData(path, data, expectedVersion)} is the compare-and-set.</li>
 *   <li><b>Leases</b> → a persistent znode per resource storing {@code {owner, token, expiry,
 *       released}}. Acquisition/renewal is a read + version-checked write; the fencing
 *       <b>token is a monotonic counter</b> in that record, bumped on every fresh acquisition.
 *       Expiry is driven by the injected {@link Clock} (so it is identical to the fake and testable),
 *       and safety rests on the fencing token, not on session liveness — a fenced owner is detected
 *       because a strictly higher token exists.</li>
 *   <li><b>Membership</b> → child znodes under {@code /members}.</li>
 * </ul>
 *
 * <p>All paths are scoped under the {@code candybox} Curator namespace. Thread-safe.
 */
public final class ZooKeeperCoordinationService implements CoordinationService {

    private static final String MEMBERS_BASE = "/members";

    private final CuratorFramework client;
    private final Clock clock;
    private final boolean ownsClient;

    /** Builds and owns a Curator client connected to {@code connectString}. */
    public ZooKeeperCoordinationService(String connectString, Clock clock) {
        this(buildClient(connectString), clock, true);
        try {
            client.blockUntilConnected();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CoordinationException("Interrupted connecting to ZooKeeper", e);
        }
    }

    /** Builds over an externally managed, already-started Curator client (the caller closes it). */
    public ZooKeeperCoordinationService(CuratorFramework client, Clock clock) {
        this(client, clock, false);
    }

    private ZooKeeperCoordinationService(CuratorFramework client, Clock clock, boolean ownsClient) {
        this.client = client;
        this.clock = clock == null ? SystemClock.INSTANCE : clock;
        this.ownsClient = ownsClient;
    }

    private static CuratorFramework buildClient(String connectString) {
        CuratorFramework c = CuratorFrameworkFactory.builder()
                .connectString(connectString)
                .namespace("candybox")
                .retryPolicy(new ExponentialBackoffRetry(100, 3))
                .sessionTimeoutMs(15_000)
                .connectionTimeoutMs(15_000)
                .build();
        c.start();
        return c;
    }

    private static String path(String key) {
        return key.startsWith("/") ? key : "/" + key;
    }

    // ---- versioned key-value ---------------------------------------------------------------

    @Override
    public Optional<VersionedValue> get(String key) {
        Stat stat = new Stat();
        try {
            byte[] data = client.getData().storingStatIn(stat).forPath(path(key));
            return Optional.of(new VersionedValue(stat.getVersion(), data));
        } catch (KeeperException.NoNodeException e) {
            return Optional.empty();
        } catch (Exception e) {
            throw wrap("get", key, e);
        }
    }

    @Override
    public long create(String key, byte[] value) {
        try {
            client.create().creatingParentsIfNeeded().forPath(path(key), value);
            return 0;
        } catch (KeeperException.NodeExistsException e) {
            throw new CasConflictException(key, -1, currentVersion(key));
        } catch (Exception e) {
            throw wrap("create", key, e);
        }
    }

    @Override
    public long compareAndSet(String key, byte[] value, long expectedVersion) {
        try {
            if (expectedVersion < 0) {
                // "expected absent" — succeed iff we can create it.
                client.create().creatingParentsIfNeeded().forPath(path(key), value);
                return 0;
            }
            Stat stat = client.setData().withVersion((int) expectedVersion).forPath(path(key), value);
            return stat.getVersion();
        } catch (KeeperException.NodeExistsException | KeeperException.BadVersionException
                 | KeeperException.NoNodeException e) {
            throw new CasConflictException(key, expectedVersion, currentVersion(key));
        } catch (Exception e) {
            throw wrap("compareAndSet", key, e);
        }
    }

    @Override
    public void delete(String key, long expectedVersion) {
        try {
            client.delete().withVersion((int) expectedVersion).forPath(path(key));
        } catch (KeeperException.NoNodeException | KeeperException.BadVersionException e) {
            throw new CasConflictException(key, expectedVersion, currentVersion(key));
        } catch (Exception e) {
            throw wrap("delete", key, e);
        }
    }

    private long currentVersion(String key) {
        Stat stat = new Stat();
        try {
            client.getData().storingStatIn(stat).forPath(path(key));
            return stat.getVersion();
        } catch (Exception e) {
            return -1;
        }
    }

    // ---- leases ----------------------------------------------------------------------------

    @Override
    public Optional<Lease> tryAcquireLease(String resource, int nodeId, long ttlMillis) {
        String p = path(resource);
        Stat stat = new Stat();
        LeaseRecord current = readLease(p, stat);
        long now = clock.currentTimeMillis();
        boolean free = current == null || current.released || now >= current.expiry;

        if (!free) {
            if (current.owner == nodeId) {
                // Idempotent re-acquire by the holder: renew, keep the same token.
                LeaseRecord renewed = new LeaseRecord(nodeId, current.token, now + ttlMillis, false);
                return writeLease(p, renewed, stat.getVersion(), false)
                        ? Optional.of(new ZkLease(resource, nodeId, current.token, ttlMillis))
                        : Optional.empty();
            }
            return Optional.empty();
        }

        long token = current == null ? 1 : current.token + 1; // strictly increasing across acquisitions
        LeaseRecord fresh = new LeaseRecord(nodeId, token, now + ttlMillis, false);
        boolean ok = current == null
                ? createLease(p, fresh)
                : writeLease(p, fresh, stat.getVersion(), false);
        return ok ? Optional.of(new ZkLease(resource, nodeId, token, ttlMillis)) : Optional.empty();
    }

    @Override
    public Optional<LeaseInfo> leaseHolder(String resource) {
        LeaseRecord cur = readLease(path(resource), new Stat());
        long now = clock.currentTimeMillis();
        if (cur == null || cur.released || now >= cur.expiry) {
            return Optional.empty();
        }
        return Optional.of(new LeaseInfo(cur.owner, cur.token));
    }

    private LeaseRecord readLease(String p, Stat stat) {
        try {
            byte[] data = client.getData().storingStatIn(stat).forPath(p);
            return LeaseRecord.decode(data);
        } catch (KeeperException.NoNodeException e) {
            return null;
        } catch (Exception e) {
            throw wrap("readLease", p, e);
        }
    }

    private boolean createLease(String p, LeaseRecord record) {
        try {
            client.create().creatingParentsIfNeeded().forPath(p, record.encode());
            return true;
        } catch (KeeperException.NodeExistsException e) {
            return false; // lost the race
        } catch (Exception e) {
            throw wrap("createLease", p, e);
        }
    }

    private boolean writeLease(String p, LeaseRecord record, int expectedVersion, boolean ignoreConflict) {
        try {
            client.setData().withVersion(expectedVersion).forPath(p, record.encode());
            return true;
        } catch (KeeperException.BadVersionException | KeeperException.NoNodeException e) {
            if (ignoreConflict) {
                return false;
            }
            return false; // lost the race
        } catch (Exception e) {
            throw wrap("writeLease", p, e);
        }
    }

    // ---- membership ------------------------------------------------------------------------

    @Override
    public void registerMember(int nodeId, byte[] info) {
        try {
            client.create().orSetData().creatingParentsIfNeeded()
                    .forPath(MEMBERS_BASE + "/" + nodeId, info);
        } catch (Exception e) {
            throw wrap("registerMember", Integer.toString(nodeId), e);
        }
    }

    @Override
    public void unregisterMember(int nodeId) {
        try {
            client.delete().forPath(MEMBERS_BASE + "/" + nodeId);
        } catch (KeeperException.NoNodeException ignored) {
            // already gone
        } catch (Exception e) {
            throw wrap("unregisterMember", Integer.toString(nodeId), e);
        }
    }

    @Override
    public List<Integer> members() {
        try {
            List<String> children = client.getChildren().forPath(MEMBERS_BASE);
            List<Integer> ids = new ArrayList<>(children.size());
            for (String c : children) {
                ids.add(Integer.parseInt(c));
            }
            ids.sort(Integer::compareTo);
            return ids;
        } catch (KeeperException.NoNodeException e) {
            return List.of();
        } catch (Exception e) {
            throw wrap("members", MEMBERS_BASE, e);
        }
    }

    @Override
    public Optional<byte[]> memberInfo(int nodeId) {
        try {
            return Optional.of(client.getData().forPath(MEMBERS_BASE + "/" + nodeId));
        } catch (KeeperException.NoNodeException e) {
            return Optional.empty();
        } catch (Exception e) {
            throw wrap("memberInfo", Integer.toString(nodeId), e);
        }
    }

    @Override
    public void close() {
        if (ownsClient) {
            client.close();
        }
    }

    private static CoordinationException wrap(String op, String key, Exception e) {
        if (e instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        }
        if (e instanceof CoordinationException ce) {
            return ce;
        }
        return new CoordinationException("ZooKeeper " + op + " failed for '" + key + "'", e);
    }

    // ---- lease record + handle -------------------------------------------------------------

    private record LeaseRecord(int owner, long token, long expiry, boolean released) {
        byte[] encode() {
            return new BinaryWriter(24)
                    .writeInt(owner)
                    .writeLong(token)
                    .writeLong(expiry)
                    .writeBoolean(released)
                    .toByteArray();
        }

        static LeaseRecord decode(byte[] data) {
            BinaryReader r = new BinaryReader(data);
            return new LeaseRecord(r.readInt(), r.readLong(), r.readLong(), r.readBoolean());
        }
    }

    private final class ZkLease implements Lease {
        private final String resource;
        private final int ownerNodeId;
        private final long token;
        private final long ttlMillis;

        ZkLease(String resource, int ownerNodeId, long token, long ttlMillis) {
            this.resource = resource;
            this.ownerNodeId = ownerNodeId;
            this.token = token;
            this.ttlMillis = ttlMillis;
        }

        @Override
        public String resource() {
            return resource;
        }

        @Override
        public int ownerNodeId() {
            return ownerNodeId;
        }

        @Override
        public long fencingToken() {
            return token;
        }

        @Override
        public boolean isValid() {
            String p = path(resource);
            Stat stat = new Stat();
            LeaseRecord cur = readLease(p, stat);
            return cur != null && cur.token == token && !cur.released
                    && clock.currentTimeMillis() < cur.expiry;
        }

        @Override
        public void renew() {
            String p = path(resource);
            Stat stat = new Stat();
            LeaseRecord cur = readLease(p, stat);
            long now = clock.currentTimeMillis();
            if (cur == null || cur.token != token || cur.released || now >= cur.expiry) {
                throw new LeaseExpiredException(resource, token);
            }
            LeaseRecord renewed = new LeaseRecord(ownerNodeId, token, now + ttlMillis, false);
            if (!writeLease(p, renewed, stat.getVersion(), true)) {
                throw new LeaseExpiredException(resource, token);
            }
        }

        @Override
        public void release() {
            String p = path(resource);
            Stat stat = new Stat();
            LeaseRecord cur = readLease(p, stat);
            if (cur != null && cur.token == token && !cur.released) {
                writeLease(p, new LeaseRecord(ownerNodeId, token, cur.expiry, true), stat.getVersion(),
                        true);
            }
        }
    }
}
