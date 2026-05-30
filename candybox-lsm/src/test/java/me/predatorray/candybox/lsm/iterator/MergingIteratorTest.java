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
package me.predatorray.candybox.lsm.iterator;

import static me.predatorray.candybox.lsm.TestData.hlc;
import static me.predatorray.candybox.lsm.TestData.putMutation;
import static me.predatorray.candybox.lsm.TestData.tombstone;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import me.predatorray.candybox.common.Mutation;
import me.predatorray.candybox.lsm.engine.ScanDirection;
import org.junit.jupiter.api.Test;

class MergingIteratorTest {

    private static List<Mutation> drain(Iterator<Mutation> it) {
        List<Mutation> out = new ArrayList<>();
        while (it.hasNext()) {
            out.add(it.next());
        }
        return out;
    }

    @Test
    void picksHighestHlcAcrossSourcesNotSourceOrder() {
        // Source 0 ("older SSTable") has the newest write for "k"; source 1 ("memtable") an older one.
        List<Mutation> older = List.of(putMutation("a", hlc(1, 0, 1)), putMutation("k", hlc(500, 0, 1)));
        List<Mutation> newer = List.of(putMutation("k", hlc(10, 0, 1)), putMutation("z", hlc(1, 0, 1)));

        MergingIterator it = new MergingIterator(
                List.of(older.iterator(), newer.iterator()), false);
        List<Mutation> merged = drain(it);

        assertThat(merged).extracting(m -> m.key().value()).containsExactly("a", "k", "z");
        Mutation k = merged.stream().filter(m -> m.key().value().equals("k")).findFirst().orElseThrow();
        assertThat(k.hlc()).isEqualTo(hlc(500, 0, 1)); // highest HLC won
    }

    @Test
    void suppressesTombstoneWinnersWhenRequested() {
        List<Mutation> s1 = List.of(putMutation("a", hlc(1, 0, 1)), putMutation("b", hlc(1, 0, 1)));
        List<Mutation> s2 = List.of(tombstone("b", hlc(5, 0, 1)), putMutation("c", hlc(1, 0, 1)));

        List<Mutation> withDrop = drain(new MergingIterator(
                List.of(s1.iterator(), s2.iterator()), true));
        assertThat(withDrop).extracting(m -> m.key().value()).containsExactly("a", "c");

        List<Mutation> keepTombstones = drain(new MergingIterator(
                List.of(s1.iterator(), s2.iterator()), false));
        assertThat(keepTombstones).extracting(m -> m.key().value()).containsExactly("a", "b", "c");
    }

    @Test
    void reverseMergesDescendingAndStillPicksHighestHlc() {
        // Both sources are descending; "k" appears in both with the newer write in source 0.
        List<Mutation> s0 = List.of(putMutation("k", hlc(500, 0, 1)), putMutation("a", hlc(1, 0, 1)));
        List<Mutation> s1 = List.of(putMutation("z", hlc(1, 0, 1)), putMutation("k", hlc(10, 0, 1)));

        List<Mutation> merged = drain(new MergingIterator(
                List.of(s0.iterator(), s1.iterator()), false, ScanDirection.REVERSE));

        assertThat(merged).extracting(m -> m.key().value()).containsExactly("z", "k", "a");
        Mutation k = merged.stream().filter(m -> m.key().value().equals("k")).findFirst().orElseThrow();
        assertThat(k.hlc()).isEqualTo(hlc(500, 0, 1)); // highest HLC won, direction notwithstanding
    }
}
