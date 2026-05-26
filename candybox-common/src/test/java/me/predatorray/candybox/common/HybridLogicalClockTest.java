package me.predatorray.candybox.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class HybridLogicalClockTest {

    @Test
    void ticksAreStrictlyMonotonicWithinSameMilli() {
        ManualClock clock = new ManualClock(1000);
        HybridLogicalClock hlc = new HybridLogicalClock(7, clock, 60_000);

        Hlc a = hlc.tick();
        Hlc b = hlc.tick();
        Hlc c = hlc.tick();

        assertThat(a.physicalMillis()).isEqualTo(1000);
        assertThat(a.logicalCounter()).isEqualTo(0);
        assertThat(b.logicalCounter()).isEqualTo(1);
        assertThat(c.logicalCounter()).isEqualTo(2);
        assertThat(b).isGreaterThan(a);
        assertThat(c).isGreaterThan(b);
        assertThat(a.nodeId()).isEqualTo(7);
    }

    @Test
    void advancingWallClockResetsLogicalCounter() {
        ManualClock clock = new ManualClock(1000);
        HybridLogicalClock hlc = new HybridLogicalClock(1, clock, 60_000);

        hlc.tick();
        clock.setTime(2000);
        Hlc next = hlc.tick();

        assertThat(next.physicalMillis()).isEqualTo(2000);
        assertThat(next.logicalCounter()).isEqualTo(0);
    }

    @Test
    void regressingWallClockStillProducesMonotonicTimestamps() {
        ManualClock clock = new ManualClock(5000);
        HybridLogicalClock hlc = new HybridLogicalClock(1, clock, Long.MAX_VALUE);

        Hlc before = hlc.tick();
        clock.setTime(10); // wall clock jumps backwards
        Hlc after = hlc.tick();

        assertThat(after).isGreaterThan(before);
        assertThat(after.physicalMillis()).isEqualTo(5000);
        assertThat(after.logicalCounter()).isEqualTo(1);
    }

    @Test
    void observeAdvancesSoNextTickStrictlyExceedsObserved() {
        ManualClock clock = new ManualClock(100);
        HybridLogicalClock hlc = new HybridLogicalClock(2, clock, Long.MAX_VALUE);

        // Simulate a value recovered from a prior owner's WAL whose physical time is far ahead.
        Hlc observed = new Hlc(9_000, 42, 99);
        hlc.observe(observed);
        Hlc next = hlc.tick();

        assertThat(next).isGreaterThan(observed);
        assertThat(next.physicalMillis()).isEqualTo(9_000);
        assertThat(next.logicalCounter()).isEqualTo(43);
    }

    @Test
    void observeRejectsTimestampBeyondSkewBound() {
        ManualClock clock = new ManualClock(1000);
        HybridLogicalClock hlc = new HybridLogicalClock(1, clock, 5_000);

        assertThatThrownBy(() -> hlc.observe(new Hlc(1_000_000, 0, 0)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("skew");
    }
}
