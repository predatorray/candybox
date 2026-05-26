package me.predatorray.candybox.common;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class HlcTest {

    @Test
    void ordersByPhysicalThenLogicalThenNodeId() {
        Hlc base = new Hlc(100, 5, 1);
        assertThat(new Hlc(101, 0, 0)).isGreaterThan(base);   // physical dominates
        assertThat(new Hlc(100, 6, 0)).isGreaterThan(base);   // logical breaks physical tie
        assertThat(new Hlc(100, 5, 2)).isGreaterThan(base);   // nodeId is the final tiebreaker
        assertThat(new Hlc(100, 5, 1)).isEqualByComparingTo(base);
    }

    @Test
    void nodeIdTiebreakIsDeterministic() {
        Hlc lowNode = new Hlc(100, 5, 1);
        Hlc highNode = new Hlc(100, 5, 9);
        assertThat(highNode.isAfter(lowNode)).isTrue();
        assertThat(lowNode.isAfter(highNode)).isFalse();
    }
}
