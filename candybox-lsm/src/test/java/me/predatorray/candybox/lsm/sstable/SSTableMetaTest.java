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
package me.predatorray.candybox.lsm.sstable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashSet;
import java.util.Set;
import me.predatorray.candybox.common.CandyKey;
import org.junit.jupiter.api.Test;

class SSTableMetaTest {

    private static CandyKey key(String s) {
        return CandyKey.of(s);
    }

    /** A table covering ["m".."t"] with the given referenced syrups. */
    private static SSTableMeta table(Set<Long> syrups) {
        return new SSTableMeta(1L, 0, key("m"), key("t"), 5, 1024, syrups);
    }

    @Test
    void nullReferencedSyrupsBecomesEmptySet() {
        assertThat(table(null).referencedSyrups()).isEmpty();
    }

    @Test
    void referencedSyrupsIsDefensivelyCopiedAndImmutable() {
        HashSet<Long> source = new HashSet<>(Set.of(7L, 8L));
        SSTableMeta meta = table(source);

        // Mutating the source after construction must not leak into the table.
        source.add(9L);
        assertThat(meta.referencedSyrups()).containsExactlyInAnyOrder(7L, 8L);

        // And the exposed set must be unmodifiable.
        assertThatThrownBy(() -> meta.referencedSyrups().add(99L))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void overlapsTreatsNullBoundsAsUnbounded() {
        SSTableMeta meta = table(Set.of());
        assertThat(meta.overlaps(null, null)).isTrue();
        assertThat(meta.overlaps(null, key("a"))).isFalse(); // upper bound below minKey
        assertThat(meta.overlaps(key("z"), null)).isFalse(); // lower bound above maxKey
    }

    @Test
    void overlapsIsInclusiveAtTheEndpoints() {
        SSTableMeta meta = table(Set.of());
        assertThat(meta.overlaps(key("t"), key("z"))).isTrue();  // touches maxKey
        assertThat(meta.overlaps(key("a"), key("m"))).isTrue();  // touches minKey
    }

    @Test
    void overlapsFalseWhenRangeIsEntirelyBelowOrAbove() {
        SSTableMeta meta = table(Set.of());
        assertThat(meta.overlaps(key("a"), key("l"))).isFalse();
        assertThat(meta.overlaps(key("u"), key("z"))).isFalse();
    }

    @Test
    void mayContainIsInclusiveWithinRangeAndFalseOutside() {
        SSTableMeta meta = table(Set.of());
        assertThat(meta.mayContain(key("m"))).isTrue();   // == minKey
        assertThat(meta.mayContain(key("p"))).isTrue();   // interior
        assertThat(meta.mayContain(key("t"))).isTrue();   // == maxKey
        assertThat(meta.mayContain(key("a"))).isFalse();  // below
        assertThat(meta.mayContain(key("z"))).isFalse();  // above
    }
}
