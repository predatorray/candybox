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
package me.predatorray.candybox.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.junit.jupiter.api.Test;

class PartTest {

    private static SegmentRef seg(long syrupId, long first, long last) {
        return new SegmentRef(syrupId, first, last);
    }

    @Test
    void entryCountSumsAcrossSegments() {
        Part part = new Part(10, 4, 0x1234,
                List.of(seg(1, 0, 2), seg(2, 0, 1))); // 3 + 2 = 5 entries
        assertThat(part.entryCount()).isEqualTo(5);
    }

    @Test
    void nullSegmentsBecomeEmptyForAZeroLengthPart() {
        Part empty = new Part(0, 4, 0, null);
        assertThat(empty.segments()).isEmpty();
        assertThat(empty.entryCount()).isZero();
    }

    @Test
    void segmentsAreDefensivelyCopiedAndImmutable() {
        var source = new java.util.ArrayList<>(List.of(seg(1, 0, 0)));
        Part part = new Part(5, 4, 0, source);
        source.add(seg(9, 0, 0));
        assertThat(part.segments()).hasSize(1);
        assertThatThrownBy(() -> part.segments().add(seg(2, 0, 0)))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void rejectsNegativePartLength() {
        assertThatThrownBy(() -> new Part(-1, 4, 0, List.of(seg(1, 0, 0))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("partLength");
    }

    @Test
    void rejectsNonPositiveChunkSize() {
        assertThatThrownBy(() -> new Part(1, 0, 0, List.of(seg(1, 0, 0))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("chunkSize");
    }

    @Test
    void rejectsNonEmptyPartWithoutSegments() {
        assertThatThrownBy(() -> new Part(5, 4, 0, List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least one segment");
    }
}
