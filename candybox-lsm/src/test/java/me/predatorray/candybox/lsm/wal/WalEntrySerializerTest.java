package me.predatorray.candybox.lsm.wal;

import static me.predatorray.candybox.lsm.TestData.hlc;
import static me.predatorray.candybox.lsm.TestData.putMutation;
import static org.assertj.core.api.Assertions.assertThat;

import me.predatorray.candybox.common.CandyKey;
import me.predatorray.candybox.common.Mutation;
import me.predatorray.candybox.common.RangeTombstone;
import org.junit.jupiter.api.Test;

class WalEntrySerializerTest {

    @Test
    void roundTripsPointMutation() {
        Mutation m = putMutation("k", hlc(10, 1, 2));
        WalEntry decoded = WalEntrySerializer.deserialize(
                WalEntrySerializer.serialize(WalEntry.of(m)));
        assertThat(decoded).isInstanceOf(WalEntry.PointMutation.class);
        assertThat(((WalEntry.PointMutation) decoded).mutation()).isEqualTo(m);
        assertThat(decoded.hlc()).isEqualTo(hlc(10, 1, 2));
    }

    @Test
    void roundTripsRangeDelete() {
        RangeTombstone rt = new RangeTombstone(CandyKey.of("a/"), CandyKey.of("a0"), hlc(20, 0, 1));
        WalEntry decoded = WalEntrySerializer.deserialize(
                WalEntrySerializer.serialize(WalEntry.of(rt)));
        assertThat(decoded).isInstanceOf(WalEntry.RangeDelete.class);
        assertThat(((WalEntry.RangeDelete) decoded).tombstone()).isEqualTo(rt);
        assertThat(decoded.hlc()).isEqualTo(hlc(20, 0, 1));
    }
}
