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
package me.predatorray.candybox.common.serial;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import me.predatorray.candybox.common.CandyLocator;
import me.predatorray.candybox.common.Hlc;
import me.predatorray.candybox.common.LocatorType;
import me.predatorray.candybox.common.Part;
import me.predatorray.candybox.common.SegmentRef;
import me.predatorray.candybox.common.exception.LimitExceededException;
import org.junit.jupiter.api.Test;

class CandyLocatorSerializerTest {

    @Test
    void roundTripsSinglePartPutLocator() {
        CandyLocator locator = CandyLocator.singlePart(
                new Hlc(123456789L, 3, 5),
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
        assertThat(decoded.parts()).hasSize(1);
        assertThat(decoded.contentLength()).isEqualTo(2_500_000L);
        assertThat(decoded.segments()).hasSize(2);
    }

    @Test
    void roundTripsMultipartPutLocator() {
        Part p1 = new Part(5L << 20, 1 << 20, 0xaaaaaaaa, List.of(new SegmentRef(20, 0, 4)));
        Part p2 = new Part(2L << 20, 1 << 20, 0xbbbbbbbb, List.of(new SegmentRef(21, 0, 1)));
        Part p3 = new Part(123L, 1 << 20, 0xcccccccc, List.of(new SegmentRef(21, 2, 2)));
        CandyLocator locator = new CandyLocator(new Hlc(42, 7, 1), LocatorType.PUT,
                "image/png", Map.of("x", "y"), 1000L, List.of(p1, p2, p3));

        byte[] encoded = CandyLocatorSerializer.serialize(locator);
        CandyLocator decoded = CandyLocatorSerializer.deserialize(encoded);

        assertThat(decoded).isEqualTo(locator);
        assertThat(decoded.parts()).hasSize(3);
        assertThat(decoded.contentLength()).isEqualTo((5L << 20) + (2L << 20) + 123L);
        assertThat(decoded.segments()).hasSize(3); // flattened across parts
    }

    @Test
    void roundTripsTombstone() {
        CandyLocator tombstone = CandyLocator.tombstone(new Hlc(1, 0, 0), 42L);
        byte[] encoded = CandyLocatorSerializer.serialize(tombstone);
        CandyLocator decoded = CandyLocatorSerializer.deserialize(encoded);

        assertThat(decoded.isTombstone()).isTrue();
        assertThat(decoded.parts()).isEmpty();
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
        CandyLocator locator = CandyLocator.singlePart(new Hlc(1, 0, 0), 1, 1,
                null, Map.of("k", big.toString()), 0, 0, List.of(new SegmentRef(1, 0, 0)));

        assertThatThrownBy(() -> CandyLocatorSerializer.serialize(locator, 256))
                .isInstanceOf(LimitExceededException.class);
    }
}
