package me.predatorray.candybox.lsm.memtable;

import static me.predatorray.candybox.lsm.TestData.hlc;
import static me.predatorray.candybox.lsm.TestData.putMutation;
import static me.predatorray.candybox.lsm.TestData.tombstone;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import me.predatorray.candybox.common.Mutation;
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
    void tombstoneIsStoredAndResolvableByReader() {
        Memtable m = new Memtable();
        m.put(putMutation("k", hlc(1, 0, 1)));
        m.put(tombstone("k", hlc(2, 0, 1)));
        Mutation latest = new Mutation(me.predatorray.candybox.common.CandyKey.of("k"),
                m.get(me.predatorray.candybox.common.CandyKey.of("k")).orElseThrow());
        assertThat(latest.isTombstone()).isTrue();
    }
}
