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
