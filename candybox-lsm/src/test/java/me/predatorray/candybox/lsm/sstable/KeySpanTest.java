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

import me.predatorray.candybox.common.CandyKey;
import org.junit.jupiter.api.Test;

class KeySpanTest {

    private static CandyKey k(String s) {
        return CandyKey.of(s);
    }

    private static KeySpan halfOpen(String start, String end) {
        return new KeySpan(start == null ? null : k(start), end == null ? null : k(end), false);
    }

    @Test
    void exclusiveEndDoesNotReachItsBound() {
        assertThat(KeySpan.closed(k("a"), k("a")).overlaps(halfOpen("x", "a"))).isFalse();
        assertThat(halfOpen("x", "a").overlaps(KeySpan.closed(k("a"), k("a")))).isFalse();
        assertThat(halfOpen("a", "b").overlaps(halfOpen("b", "c"))).isFalse();
    }

    @Test
    void inclusiveBoundsTouchingOverlap() {
        assertThat(KeySpan.closed(k("a"), k("a")).overlaps(halfOpen("a", "b"))).isTrue();
        assertThat(KeySpan.closed(k("a"), k("b")).overlaps(KeySpan.closed(k("b"), k("c")))).isTrue();
    }

    @Test
    void unboundedSidesOverlapEverythingInThatDirection() {
        assertThat(halfOpen(null, "c").overlaps(KeySpan.closed(k("a"), k("a")))).isTrue();
        assertThat(halfOpen(null, "c").overlaps(KeySpan.closed(k("c"), k("d")))).isFalse();
        assertThat(halfOpen("m", null).overlaps(KeySpan.closed(k("z"), k("z")))).isTrue();
        assertThat(halfOpen("m", null).overlaps(halfOpen(null, "m"))).isFalse();
        assertThat(halfOpen(null, null).overlaps(halfOpen("q", "r"))).isTrue();
    }

    @Test
    void unionKeepsTheWidestBounds() {
        assertThat(halfOpen("b", "d").union(KeySpan.closed(k("a"), k("c"))))
                .isEqualTo(halfOpen("a", "d"));
        assertThat(halfOpen("a", "d").union(KeySpan.closed(k("b"), k("d"))))
                .isEqualTo(KeySpan.closed(k("a"), k("d")));
        assertThat(KeySpan.closed(k("a"), k("d")).union(halfOpen("b", "d")))
                .isEqualTo(KeySpan.closed(k("a"), k("d")));
        assertThat(halfOpen("c", "d").union(halfOpen(null, "a"))).isEqualTo(halfOpen(null, "d"));
        assertThat(KeySpan.closed(k("a"), k("z")).union(halfOpen("m", null)))
                .isEqualTo(halfOpen("a", null));
    }
}
