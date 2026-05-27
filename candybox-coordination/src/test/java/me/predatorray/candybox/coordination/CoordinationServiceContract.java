package me.predatorray.candybox.coordination;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import me.predatorray.candybox.common.Clock;
import me.predatorray.candybox.common.ManualClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The shared {@link CoordinationService} behavioural contract. Both the in-memory fake and the real
 * ZooKeeper-backed service must pass this identical suite — that equivalence is what lets the fast
 * fake-based unit tests stand in for the ownership/fencing scenarios that never touch real ZooKeeper.
 *
 * <p>Lease TTL is driven by an injected {@link ManualClock}, so expiry is deterministic and identical
 * across both implementations (no real sleeping, no session timing).
 */
public abstract class CoordinationServiceContract {

    private ManualClock clock;
    private CoordinationService service;

    /** Produces a fresh, isolated service bound to {@code clock}. */
    protected abstract CoordinationService newService(Clock clock);

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @BeforeEach
    void setUpService() {
        clock = new ManualClock(1_000);
        service = newService(clock);
    }

    @AfterEach
    void tearDownService() {
        if (service != null) {
            service.close();
        }
    }

    @Test
    void createGetAndCompareAndSet() {
        long v0 = service.create("manifest", b("ledger-1"));
        assertThat(v0).isEqualTo(0);

        VersionedValue read = service.get("manifest").orElseThrow();
        assertThat(new String(read.value(), StandardCharsets.UTF_8)).isEqualTo("ledger-1");
        assertThat(read.version()).isEqualTo(0);

        long v1 = service.compareAndSet("manifest", b("ledger-2"), 0);
        assertThat(v1).isEqualTo(1);
        assertThat(service.get("manifest").orElseThrow().version()).isEqualTo(1);
    }

    @Test
    void getAbsentKeyIsEmpty() {
        assertThat(service.get("nope")).isEmpty();
    }

    @Test
    void createRejectsDuplicate() {
        service.create("k", b("v"));
        assertThatThrownBy(() -> service.create("k", b("v2"))).isInstanceOf(CasConflictException.class);
    }

    @Test
    void compareAndSetRejectsStaleVersion() {
        service.create("k", b("v"));
        service.compareAndSet("k", b("v2"), 0); // -> version 1
        assertThatThrownBy(() -> service.compareAndSet("k", b("v3"), 0))
                .isInstanceOf(CasConflictException.class);
    }

    @Test
    void deleteRejectsStaleVersionAndRemovesOnMatch() {
        service.create("k", b("v"));
        assertThatThrownBy(() -> service.delete("k", 99)).isInstanceOf(CasConflictException.class);
        service.delete("k", 0);
        assertThat(service.get("k")).isEmpty();
    }

    @Test
    void leaseBlocksOthersThenExpiresAndTokenIncreases() {
        Lease ownerA = service.tryAcquireLease("owner", 1, 5_000).orElseThrow();
        assertThat(ownerA.isValid()).isTrue();
        assertThat(ownerA.fencingToken()).isEqualTo(1);

        // A different node cannot steal a valid lease.
        assertThat(service.tryAcquireLease("owner", 2, 5_000)).isEmpty();

        // Let A's lease lapse.
        clock.advance(6_000);
        assertThat(ownerA.isValid()).isFalse();
        assertThatThrownBy(ownerA::renew).isInstanceOf(LeaseExpiredException.class);

        // Node 2 takes over with a strictly higher fencing token; A stays fenced.
        Lease ownerB = service.tryAcquireLease("owner", 2, 5_000).orElseThrow();
        assertThat(ownerB.fencingToken()).isGreaterThan(ownerA.fencingToken());
        assertThat(ownerA.isValid()).isFalse();
        assertThat(ownerB.isValid()).isTrue();
    }

    @Test
    void renewKeepsLeaseAlive() {
        Lease lease = service.tryAcquireLease("r", 1, 1_000).orElseThrow();
        clock.advance(900);
        lease.renew();              // expiry now 1900
        clock.advance(900);         // 2800 total; still valid
        assertThat(lease.isValid()).isTrue();
        assertThat(service.tryAcquireLease("r", 2, 1_000)).isEmpty();
    }

    @Test
    void releasedLeaseCanBeReacquiredImmediatelyWithHigherToken() {
        Lease a = service.tryAcquireLease("r", 1, 10_000).orElseThrow();
        a.release();
        assertThat(a.isValid()).isFalse();

        Lease b = service.tryAcquireLease("r", 2, 10_000).orElseThrow();
        assertThat(b.fencingToken()).isGreaterThan(a.fencingToken());
    }

    @Test
    void membershipRegistersAndLists() {
        service.registerMember(3, b("host-3"));
        service.registerMember(1, b("host-1"));
        assertThat(service.members()).containsExactly(1, 3);
        service.unregisterMember(1);
        assertThat(service.members()).containsExactly(3);
    }
}
