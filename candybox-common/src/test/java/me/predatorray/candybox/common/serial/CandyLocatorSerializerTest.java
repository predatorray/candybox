package me.predatorray.candybox.common.serial;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import me.predatorray.candybox.common.CandyLocator;
import me.predatorray.candybox.common.Hlc;
import me.predatorray.candybox.common.LocatorType;
import me.predatorray.candybox.common.SegmentRef;
import me.predatorray.candybox.common.exception.LimitExceededException;
import org.junit.jupiter.api.Test;

class CandyLocatorSerializerTest {

    @Test
    void roundTripsPutLocator() {
        CandyLocator locator = new CandyLocator(
                new Hlc(123456789L, 3, 5),
                LocatorType.PUT,
                2_500_000L,
                1 << 20,
                "application/octet-stream",
                Map.of("author", "alice", "tag", "v1"),
                0x1234abcd,
                999L,
                List.of(new SegmentRef(10, 0, 2), new SegmentRef(11, 0, 0)));

        byte[] encoded = CandyLocatorSerializer.serialize(locator);
        CandyLocator decoded = CandyLocatorSerializer.deserialize(encoded);

        assertThat(decoded).isEqualTo(locator);
    }

    @Test
    void roundTripsTombstone() {
        CandyLocator tombstone = CandyLocator.tombstone(new Hlc(1, 0, 0), 42L);
        byte[] encoded = CandyLocatorSerializer.serialize(tombstone);
        CandyLocator decoded = CandyLocatorSerializer.deserialize(encoded);

        assertThat(decoded.isTombstone()).isTrue();
        assertThat(decoded.segments()).isEmpty();
        assertThat(decoded).isEqualTo(tombstone);
    }

    @Test
    void enforcesMaxLocatorSize() {
        // Build a large metadata map that overflows a tiny cap.
        StringBuilder big = new StringBuilder();
        for (int i = 0; i < 500; i++) {
            big.append("xxxxxxxx");
        }
        CandyLocator locator = new CandyLocator(new Hlc(1, 0, 0), LocatorType.PUT, 1, 1,
                null, Map.of("k", big.toString()), 0, 0, List.of(new SegmentRef(1, 0, 0)));

        assertThatThrownBy(() -> CandyLocatorSerializer.serialize(locator, 256))
                .isInstanceOf(LimitExceededException.class);
    }
}
