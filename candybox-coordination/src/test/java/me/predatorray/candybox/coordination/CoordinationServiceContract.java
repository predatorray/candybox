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
    void publicationRequiresLiveCurrentEpochAndExpectedPointerVersion() {
        Lease first = service.tryAcquireLease("boxes/leased/owner", 1, 100).orElseThrow();
        assertThat(service.compareAndSetWithLease("boxes/leased/manifest", b("one"), -1,
                first.resource(), first.fencingToken())).isZero();
        assertThatThrownBy(() -> service.compareAndSetWithLease("boxes/leased/manifest", b("bad"), -1,
                first.resource(), first.fencingToken())).isInstanceOf(CasConflictException.class);
        assertThat(service.compareAndSetWithLease("boxes/leased/manifest", b("two"), 0,
                first.resource(), first.fencingToken())).isEqualTo(1);
        clock.advance(101);
        assertThatThrownBy(() -> service.compareAndSetWithLease("boxes/leased/manifest", b("expired"), 1,
                first.resource(), first.fencingToken())).isInstanceOf(LeaseExpiredException.class);
        Lease second = service.tryAcquireLease(first.resource(), 2, 100).orElseThrow();
        assertThatThrownBy(() -> service.compareAndSetWithLease("boxes/leased/manifest", b("stale"), 1,
                first.resource(), first.fencingToken())).isInstanceOf(LeaseExpiredException.class);
        assertThat(service.get("boxes/leased/manifest").orElseThrow().value()).isEqualTo(b("two"));
        assertThat(service.compareAndSetWithLease("boxes/leased/manifest", b("winner"), 1,
                second.resource(), second.fencingToken())).isEqualTo(2);
        second.release();
        assertThatThrownBy(() -> service.compareAndSetWithLease("boxes/leased/manifest", b("released"), 2,
                second.resource(), second.fencingToken())).isInstanceOf(LeaseExpiredException.class);
        assertThat(service.get("boxes/leased/manifest").orElseThrow().value()).isEqualTo(b("winner"));
    }

    @Test
    void connectedUntilClosed() {
        assertThat(service.isConnected()).isTrue();
        service.close();
        assertThat(service.isConnected()).isFalse();
        service = null;
    }

    @Test
    void createGetAndCompareAndSet() {
        long v0 = service.create("manifest", b("ledger-1"));
        assertThat(v0).isEqualTo(0);

        VersionedValue read = service.get("manifest").orElseThrow();
        assertThat(new String(read.value(), StandardCharsets.UTF_8)).isEqualTo("ledger-1");
        assertThat(read.version()).isEqualTo(0);
        assertThat(read.modifiedAtMillis()).isGreaterThanOrEqualTo(0);
        assertThat(service.get("manifest").orElseThrow().modifiedAtMillis())
                .isEqualTo(read.modifiedAtMillis());

        long v1 = service.compareAndSet("manifest", b("ledger-2"), 0);
        assertThat(v1).isEqualTo(1);
        assertThat(service.get("manifest").orElseThrow().version()).isEqualTo(1);
        assertThat(service.get("manifest").orElseThrow().modifiedAtMillis())
                .isGreaterThanOrEqualTo(read.modifiedAtMillis());
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
    void compareAndSetWithExpectedAbsentCreatesOnlyIfMissing() {
        assertThat(service.compareAndSet("k", b("v"), -1)).isZero();
        VersionedValue created = service.get("k").orElseThrow();
        assertThat(created.value()).isEqualTo(b("v"));
        assertThat(created.version()).isZero();

        assertThatThrownBy(() -> service.compareAndSet("k", b("v2"), -1))
                .isInstanceOf(CasConflictException.class);
        VersionedValue unchanged = service.get("k").orElseThrow();
        assertThat(unchanged.value()).isEqualTo(b("v"));
        assertThat(unchanged.version()).isZero();
    }

    @Test
    void deleteRejectsStaleVersionAndRemovesOnMatch() {
        service.create("k", b("v"));
        assertThatThrownBy(() -> service.delete("k", 99)).isInstanceOf(CasConflictException.class);
        service.delete("k", 0);
        assertThat(service.get("k")).isEmpty();
    }

    @Test
    void childrenListsDirectChildNamesAcrossKeysAndLeases() {
        assertThat(service.children("boxes")).isEmpty();

        service.create("boxes/alpha/meta", b("d"));
        service.create("boxes/alpha/partitions/0/manifest", b("m"));
        service.create("boxes/beta/meta", b("d"));
        service.tryAcquireLease("boxes/gamma/partitions/0/owner", 1, 5_000).orElseThrow();

        // Direct child names only, deduplicated, sorted — and lease resources count as paths too.
        assertThat(service.children("boxes")).containsExactly("alpha", "beta", "gamma");
        assertThat(service.children("boxes/alpha")).containsExactly("meta", "partitions");
        assertThat(service.children("boxes/alpha/partitions")).containsExactly("0");
        assertThat(service.children("elsewhere")).isEmpty();
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
    void staleHoldersReleaseAfterTakeoverDoesNotFreeTheNewHolder() {
        Lease a = service.tryAcquireLease("r", 1, 5_000).orElseThrow();
        clock.advance(6_000);
        Lease b = service.tryAcquireLease("r", 2, 5_000).orElseThrow();

        a.release();

        assertThat(b.isValid()).isTrue();
        LeaseInfo holder = service.leaseHolder("r").orElseThrow();
        assertThat(holder.ownerNodeId()).isEqualTo(2);
        assertThat(holder.fencingToken()).isEqualTo(b.fencingToken());
        assertThat(service.tryAcquireLease("r", 3, 5_000)).isEmpty();
    }

    @Test
    void renewAfterReleaseThrows() {
        Lease lease = service.tryAcquireLease("r", 1, 5_000).orElseThrow();
        lease.release();

        assertThatThrownBy(lease::renew).isInstanceOf(LeaseExpiredException.class);
        assertThat(lease.isValid()).isFalse();
    }

    @Test
    void staleHolderCannotRenewOrValidateAfterTakeover() {
        Lease a = service.tryAcquireLease("r", 1, 5_000).orElseThrow();
        clock.advance(6_000);
        Lease b = service.tryAcquireLease("r", 2, 5_000).orElseThrow();
        // Attempt the stale renewal later, so an accidental extension is observable.
        clock.advance(1_000);

        assertThatThrownBy(a::renew).isInstanceOf(LeaseExpiredException.class);
        assertThat(a.isValid()).isFalse();
        clock.advance(3_999);
        assertThat(b.isValid()).isTrue();
        clock.advance(1);
        assertThat(b.isValid()).isFalse();
        assertThat(service.leaseHolder("r")).isEmpty();
    }

    @Test
    void holderReacquireKeepsTokenAndExtendsExpiry() {
        Lease original = service.tryAcquireLease("r", 1, 5_000).orElseThrow();
        clock.advance(4_000);
        Lease reacquired = service.tryAcquireLease("r", 1, 5_000).orElseThrow();
        assertThat(reacquired.fencingToken()).isEqualTo(original.fencingToken());

        clock.advance(4_000);
        assertThat(reacquired.isValid()).isTrue();
        LeaseInfo holder = service.leaseHolder("r").orElseThrow();
        assertThat(holder.ownerNodeId()).isEqualTo(1);
        assertThat(holder.fencingToken()).isEqualTo(original.fencingToken());
    }

    @Test
    void releaseIsIdempotent() {
        Lease lease = service.tryAcquireLease("r", 1, 5_000).orElseThrow();
        lease.release();
        lease.release();

        Lease successor = service.tryAcquireLease("r", 2, 5_000).orElseThrow();
        assertThat(successor.fencingToken()).isGreaterThan(lease.fencingToken());
        assertThat(successor.isValid()).isTrue();
    }

    @Test
    void membershipRegistersListsAndExposesInfo() {
        service.registerMember(3, b("host-3:30"));
        service.registerMember(1, b("host-1:10"));
        assertThat(service.members()).containsExactly(1, 3);
        assertThat(service.memberInfo(1).map(String::new)).contains("host-1:10");
        assertThat(service.memberInfo(2)).isEmpty();
        service.unregisterMember(1);
        assertThat(service.members()).containsExactly(3);
        assertThat(service.memberInfo(1)).isEmpty();
    }

    @Test
    void reRegisteringAMemberRefreshesItsInfo() {
        service.registerMember(1, b("host-1:10"));
        service.registerMember(1, b("host-1:11"));
        assertThat(service.members()).containsExactly(1);
        assertThat(service.memberInfo(1).map(String::new)).contains("host-1:11");
    }

    @Test
    void unregisteringAnUnknownMemberIsANoOp() {
        assertThat(service.members()).isEmpty();
        service.unregisterMember(42);
        assertThat(service.members()).isEmpty();

        service.registerMember(1, b("host-1:10"));
        service.unregisterMember(42);
        assertThat(service.members()).containsExactly(1);
        assertThat(service.memberInfo(1)).contains(b("host-1:10"));
    }

    @Test
    void leaseHolderReportsCurrentOwnerAndClearsOnExpiry() {
        assertThat(service.leaseHolder("owner")).isEmpty();

        Lease lease = service.tryAcquireLease("owner", 5, 5_000).orElseThrow();
        LeaseInfo holder = service.leaseHolder("owner").orElseThrow();
        assertThat(holder.ownerNodeId()).isEqualTo(5);
        assertThat(holder.fencingToken()).isEqualTo(lease.fencingToken());

        clock.advance(6_000);
        assertThat(service.leaseHolder("owner")).isEmpty();
    }
}
