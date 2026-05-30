package me.predatorray.candybox.common.serial;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import me.predatorray.candybox.common.CandyKey;
import me.predatorray.candybox.common.Hlc;
import me.predatorray.candybox.common.RangeTombstone;
import org.junit.jupiter.api.Test;

class RangeTombstoneSerializerTest {

    @Test
    void roundTripsBoundedRange() {
        RangeTombstone rt = new RangeTombstone(CandyKey.of("a/"), CandyKey.of("a0"), new Hlc(99, 2, 7));
        RangeTombstone decoded = RangeTombstoneSerializer.deserialize(
                RangeTombstoneSerializer.serialize(rt));
        assertThat(decoded).isEqualTo(rt);
    }

    @Test
    void roundTripsUnboundedEnds() {
        RangeTombstone fromStart = new RangeTombstone(null, CandyKey.of("m"), new Hlc(1, 0, 0));
        RangeTombstone toEnd = new RangeTombstone(CandyKey.of("m"), null, new Hlc(2, 0, 0));
        RangeTombstone whole = new RangeTombstone(null, null, new Hlc(3, 0, 0));

        for (RangeTombstone rt : new RangeTombstone[] {fromStart, toEnd, whole}) {
            assertThat(RangeTombstoneSerializer.deserialize(RangeTombstoneSerializer.serialize(rt)))
                    .isEqualTo(rt);
        }
    }

    @Test
    void rejectsEmptyOrInvertedRange() {
        assertThatThrownBy(() -> new RangeTombstone(CandyKey.of("b"), CandyKey.of("a"), new Hlc(1, 0, 0)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RangeTombstone(CandyKey.of("a"), CandyKey.of("a"), new Hlc(1, 0, 0)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void coversRespectsHalfOpenInterval() {
        RangeTombstone rt = new RangeTombstone(CandyKey.of("b"), CandyKey.of("d"), new Hlc(1, 0, 0));
        assertThat(rt.covers(CandyKey.of("b"))).isTrue();  // start inclusive
        assertThat(rt.covers(CandyKey.of("c"))).isTrue();
        assertThat(rt.covers(CandyKey.of("d"))).isFalse(); // end exclusive
        assertThat(rt.covers(CandyKey.of("a"))).isFalse();
    }
}
