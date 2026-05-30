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
package me.predatorray.candybox.lsm.memtable;

import static me.predatorray.candybox.lsm.TestData.hlc;
import static me.predatorray.candybox.lsm.TestData.putMutation;
import static me.predatorray.candybox.lsm.TestData.tombstone;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import me.predatorray.candybox.common.CandyKey;
import me.predatorray.candybox.common.Mutation;
import me.predatorray.candybox.common.RangeTombstone;
import org.junit.jupiter.api.Test;

class MemtableTest {

    @Test
    void higherHlcWinsRegardlessOfInsertionOrder() {
        Memtable m = new Memtable();
        m.put(putMutation("k", hlc(100, 0, 1)));
        // A later-arriving but older-HLC write must not overwrite.
        boolean won = m.put(putMutation("k", hlc(50, 0, 1)));
        assertThat(won).isFalse();
        assertThat(m.get(me.predatorray.candybox.common.CandyKey.of("k")).orElseThrow().hlc())
                .isEqualTo(hlc(100, 0, 1));

        // A strictly-newer write wins.
        assertThat(m.put(putMutation("k", hlc(150, 0, 1)))).isTrue();
        assertThat(m.get(me.predatorray.candybox.common.CandyKey.of("k")).orElseThrow().hlc())
                .isEqualTo(hlc(150, 0, 1));
    }

    @Test
    void nodeIdBreaksEqualPhysicalAndLogical() {
        Memtable m = new Memtable();
        m.put(putMutation("k", hlc(100, 5, 1)));
        boolean wonHigherNode = m.put(putMutation("k", hlc(100, 5, 9)));
        assertThat(wonHigherNode).isTrue();
        boolean wonLowerNode = m.put(putMutation("k", hlc(100, 5, 2)));
        assertThat(wonLowerNode).isFalse();
    }

    @Test
    void iteratesInAscendingKeyOrder() {
        Memtable m = new Memtable();
        m.put(putMutation("banana", hlc(1, 0, 1)));
        m.put(putMutation("apple", hlc(1, 0, 1)));
        m.put(putMutation("cherry", hlc(1, 0, 1)));

        List<String> keys = new ArrayList<>();
        var it = m.iterator();
        while (it.hasNext()) {
            keys.add(it.next().key().value());
        }
        assertThat(keys).containsExactly("apple", "banana", "cherry");
    }

    @Test
    void descendingIteratorWalksKeysInReverse() {
        Memtable m = new Memtable();
        m.put(putMutation("banana", hlc(1, 0, 1)));
        m.put(putMutation("apple", hlc(1, 0, 1)));
        m.put(putMutation("cherry", hlc(1, 0, 1)));

        List<String> all = new ArrayList<>();
        var it = m.descendingIterator();
        while (it.hasNext()) {
            all.add(it.next().key().value());
        }
        assertThat(all).containsExactly("cherry", "banana", "apple");

        List<String> bounded = new ArrayList<>();
        var bit = m.descendingIterator(me.predatorray.candybox.common.CandyKey.of("banana"));
        while (bit.hasNext()) {
            bounded.add(bit.next().key().value());
        }
        assertThat(bounded).containsExactly("banana", "apple"); // <= start, descending
    }

    @Test
    void rangeTombstoneCoveringReportsHighestHlc() {
        Memtable m = new Memtable();
        m.delete(new RangeTombstone(CandyKey.of("b"), CandyKey.of("e"), hlc(10, 0, 1)));
        m.delete(new RangeTombstone(CandyKey.of("a"), CandyKey.of("c"), hlc(20, 0, 1))); // overlaps at b

        assertThat(m.maxRangeTombstoneCovering(CandyKey.of("a"))).isEqualTo(hlc(20, 0, 1));
        assertThat(m.maxRangeTombstoneCovering(CandyKey.of("b"))).isEqualTo(hlc(20, 0, 1)); // higher wins
        assertThat(m.maxRangeTombstoneCovering(CandyKey.of("d"))).isEqualTo(hlc(10, 0, 1));
        assertThat(m.maxRangeTombstoneCovering(CandyKey.of("z"))).isNull(); // uncovered
        assertThat(m.isEmpty()).isFalse(); // range tombstones count toward flush
    }

    @Test
    void tombstoneIsStoredAndResolvableByReader() {
        Memtable m = new Memtable();
        m.put(putMutation("k", hlc(1, 0, 1)));
        m.put(tombstone("k", hlc(2, 0, 1)));
        Mutation latest = new Mutation(me.predatorray.candybox.common.CandyKey.of("k"),
                m.get(me.predatorray.candybox.common.CandyKey.of("k")).orElseThrow());
        assertThat(latest.isTombstone()).isTrue();
    }
}
