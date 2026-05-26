package me.predatorray.candybox.coordination.fake;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import me.predatorray.candybox.common.Clock;
import me.predatorray.candybox.common.SystemClock;
import me.predatorray.candybox.coordination.CasConflictException;
import me.predatorray.candybox.coordination.CoordinationService;
import me.predatorray.candybox.coordination.Lease;
import me.predatorray.candybox.coordination.LeaseExpiredException;
import me.predatorray.candybox.coordination.VersionedValue;

/**
 * In-memory {@link CoordinationService} fake that models the adversarial semantics the fencing and
 * handover tests depend on:
 *
 * <ul>
 *   <li><b>Lease expiry</b> driven by an injected {@link Clock}; once the TTL elapses the lease is
 *       lost and {@link Lease#renew()} fails.</li>
 *   <li><b>Supersession / fencing</b>: when a new owner acquires a lapsed lease, its fencing token is
 *       strictly higher and the previous holder's lease immediately reports invalid.</li>
 *   <li><b>CAS conflicts</b>: versioned writes fail with {@link CasConflictException} on a version
 *       mismatch, so the manifest-pointer race is real, not papered over.</li>
 * </ul>
 *
 * <p>Thread-safe via coarse synchronization on the service instance.
 */
public final class InMemoryCoordinationService implements CoordinationService {

    private final Clock clock;
    private final ConcurrentMap<String, byte[]> members = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Entry> kv = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, LeaseState> leases = new ConcurrentHashMap<>();

    public InMemoryCoordinationService() {
        this(SystemClock.INSTANCE);
    }

    public InMemoryCoordinationService(Clock clock) {
        this.clock = clock;
    }

    // ---- versioned key-value ---------------------------------------------------------------

    @Override
    public synchronized Optional<VersionedValue> get(String key) {
        Entry e = kv.get(key);
        return e == null ? Optional.empty() : Optional.of(new VersionedValue(e.version, e.value.clone()));
    }

    @Override
    public synchronized long create(String key, byte[] value) {
        Entry existing = kv.get(key);
        if (existing != null) {
            throw new CasConflictException(key, -1, existing.version);
        }
        kv.put(key, new Entry(0, value.clone()));
        return 0;
    }

    @Override
    public synchronized long compareAndSet(String key, byte[] value, long expectedVersion) {
        Entry e = kv.get(key);
        long actual = e == null ? -1 : e.version;
        if (actual != expectedVersion) {
            throw new CasConflictException(key, expectedVersion, actual);
        }
        long newVersion = expectedVersion + 1;
        kv.put(key, new Entry(newVersion, value.clone()));
        return newVersion;
    }

    @Override
    public synchronized void delete(String key, long expectedVersion) {
        Entry e = kv.get(key);
        long actual = e == null ? -1 : e.version;
        if (actual != expectedVersion) {
            throw new CasConflictException(key, expectedVersion, actual);
        }
        kv.remove(key);
    }

    // ---- leases ----------------------------------------------------------------------------

    @Override
    public synchronized Optional<Lease> tryAcquireLease(String resource, int nodeId, long ttlMillis) {
        long now = clock.currentTimeMillis();
        LeaseState st = leases.get(resource);
        boolean free = st == null || st.released || now >= st.expiry;
        if (!free) {
            if (st.ownerNodeId == nodeId) {
                // Idempotent re-acquire by the current holder: renew, keep the same token.
                st.expiry = now + ttlMillis;
                return Optional.of(new LeaseHandle(st));
            }
            return Optional.empty();
        }
        long token = st == null ? 1 : st.token + 1; // strictly increasing across acquisitions
        LeaseState fresh = new LeaseState(resource, nodeId, token, now + ttlMillis, ttlMillis);
        leases.put(resource, fresh);
        return Optional.of(new LeaseHandle(fresh));
    }

    // ---- membership ------------------------------------------------------------------------

    @Override
    public void registerMember(int nodeId, byte[] info) {
        members.put(Integer.toString(nodeId), info.clone());
    }

    @Override
    public void unregisterMember(int nodeId) {
        members.remove(Integer.toString(nodeId));
    }

    @Override
    public List<Integer> members() {
        List<Integer> ids = new ArrayList<>();
        for (String k : members.keySet()) {
            ids.add(Integer.parseInt(k));
        }
        ids.sort(Integer::compareTo);
        return ids;
    }

    @Override
    public void close() {
        // No resources to release in the fake.
    }

    private boolean isCurrentAndLive(LeaseState st) {
        return leases.get(st.resource) == st && !st.released && clock.currentTimeMillis() < st.expiry;
    }

    private static final class Entry {
        final long version;
        final byte[] value;

        Entry(long version, byte[] value) {
            this.version = version;
            this.value = value;
        }
    }

    private static final class LeaseState {
        final String resource;
        final int ownerNodeId;
        final long token;
        final long ttlMillis;
        volatile long expiry;
        volatile boolean released;

        LeaseState(String resource, int ownerNodeId, long token, long expiry, long ttlMillis) {
            this.resource = resource;
            this.ownerNodeId = ownerNodeId;
            this.token = token;
            this.expiry = expiry;
            this.ttlMillis = ttlMillis;
        }
    }

    private final class LeaseHandle implements Lease {
        private final LeaseState state;

        LeaseHandle(LeaseState state) {
            this.state = state;
        }

        @Override
        public String resource() {
            return state.resource;
        }

        @Override
        public int ownerNodeId() {
            return state.ownerNodeId;
        }

        @Override
        public long fencingToken() {
            return state.token;
        }

        @Override
        public boolean isValid() {
            synchronized (InMemoryCoordinationService.this) {
                return isCurrentAndLive(state);
            }
        }

        @Override
        public void renew() {
            synchronized (InMemoryCoordinationService.this) {
                if (!isCurrentAndLive(state)) {
                    throw new LeaseExpiredException(state.resource, state.token);
                }
                state.expiry = clock.currentTimeMillis() + state.ttlMillis;
            }
        }

        @Override
        public void release() {
            synchronized (InMemoryCoordinationService.this) {
                if (leases.get(state.resource) == state) {
                    state.released = true;
                }
            }
        }
    }
}
