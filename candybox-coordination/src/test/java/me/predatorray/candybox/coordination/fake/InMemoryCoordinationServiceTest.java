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
package me.predatorray.candybox.coordination.fake;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import me.predatorray.candybox.common.ManualClock;
import me.predatorray.candybox.coordination.CasConflictException;
import me.predatorray.candybox.coordination.Lease;
import me.predatorray.candybox.coordination.LeaseExpiredException;
import me.predatorray.candybox.coordination.VersionedValue;
import org.junit.jupiter.api.Test;

class InMemoryCoordinationServiceTest {

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void createGetAndCompareAndSet() {
        InMemoryCoordinationService cs = new InMemoryCoordinationService();
        long v0 = cs.create("box/b/manifest-ptr", b("ledger-1"));
        assertThat(v0).isEqualTo(0);

        VersionedValue read = cs.get("box/b/manifest-ptr").orElseThrow();
        assertThat(new String(read.value(), StandardCharsets.UTF_8)).isEqualTo("ledger-1");
        assertThat(read.version()).isEqualTo(0);

        long v1 = cs.compareAndSet("box/b/manifest-ptr", b("ledger-2"), 0);
        assertThat(v1).isEqualTo(1);
    }

    @Test
    void createRejectsDuplicateAndCasRejectsStaleVersion() {
        InMemoryCoordinationService cs = new InMemoryCoordinationService();
        cs.create("k", b("v"));
        assertThatThrownBy(() -> cs.create("k", b("v2"))).isInstanceOf(CasConflictException.class);

        cs.compareAndSet("k", b("v2"), 0); // now at version 1
        // A second writer still believing it is version 0 loses the race.
        assertThatThrownBy(() -> cs.compareAndSet("k", b("v3"), 0))
                .isInstanceOf(CasConflictException.class);
    }

    @Test
    void leaseExpiresAndFencingTokenIncreasesAcrossOwners() {
        ManualClock clock = new ManualClock(1000);
        InMemoryCoordinationService cs = new InMemoryCoordinationService(clock);

        Lease ownerA = cs.tryAcquireLease("box/b/owner", 1, 5_000).orElseThrow();
        assertThat(ownerA.isValid()).isTrue();
        assertThat(ownerA.fencingToken()).isEqualTo(1);

        // A different node cannot steal a valid lease.
        assertThat(cs.tryAcquireLease("box/b/owner", 2, 5_000)).isEmpty();

        // Let A's lease lapse.
        clock.advance(6_000);
        assertThat(ownerA.isValid()).isFalse();
        assertThatThrownBy(ownerA::renew).isInstanceOf(LeaseExpiredException.class);

        // Node 2 takes over with a strictly higher fencing token — A is now fenced.
        Lease ownerB = cs.tryAcquireLease("box/b/owner", 2, 5_000).orElseThrow();
        assertThat(ownerB.fencingToken()).isEqualTo(2);
        assertThat(ownerB.fencingToken()).isGreaterThan(ownerA.fencingToken());
        assertThat(ownerA.isValid()).isFalse();
    }

    @Test
    void renewKeepsLeaseAlive() {
        ManualClock clock = new ManualClock(0);
        InMemoryCoordinationService cs = new InMemoryCoordinationService(clock);
        Lease lease = cs.tryAcquireLease("r", 1, 1_000).orElseThrow();

        clock.advance(900);
        lease.renew();
        clock.advance(900); // 1800 total, but renewed at 900 so expiry is 1900
        assertThat(lease.isValid()).isTrue();
        assertThat(cs.tryAcquireLease("r", 2, 1_000)).isEmpty();
    }

    @Test
    void membershipRegistersAndLists() {
        InMemoryCoordinationService cs = new InMemoryCoordinationService();
        cs.registerMember(3, b("host-3"));
        cs.registerMember(1, b("host-1"));
        assertThat(cs.members()).containsExactly(1, 3);
        cs.unregisterMember(1);
        assertThat(cs.members()).containsExactly(3);
    }

    @Test
    void releasedLeaseCanBeReacquiredImmediately() {
        ManualClock clock = new ManualClock(0);
        InMemoryCoordinationService cs = new InMemoryCoordinationService(clock);
        Lease a = cs.tryAcquireLease("r", 1, 10_000).orElseThrow();
        a.release();
        assertThat(a.isValid()).isFalse();

        Optional<Lease> b = cs.tryAcquireLease("r", 2, 10_000);
        assertThat(b).isPresent();
        assertThat(b.get().fencingToken()).isGreaterThan(a.fencingToken());
    }
}
