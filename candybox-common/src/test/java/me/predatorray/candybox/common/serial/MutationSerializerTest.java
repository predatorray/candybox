package me.predatorray.candybox.common.serial;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import me.predatorray.candybox.common.CandyKey;
import me.predatorray.candybox.common.CandyLocator;
import me.predatorray.candybox.common.Hlc;
import me.predatorray.candybox.common.LocatorType;
import me.predatorray.candybox.common.Mutation;
import me.predatorray.candybox.common.SegmentRef;
import org.junit.jupiter.api.Test;

class MutationSerializerTest {

    @Test
    void roundTripsPutAndTombstoneMutations() {
        Mutation put = new Mutation(CandyKey.of("path/to/candy"),
                new CandyLocator(new Hlc(10, 1, 2), LocatorType.PUT, 42, 1024, "text/plain",
                        Map.of("k", "v"), 7, 5, List.of(new SegmentRef(3, 0, 0))));
        Mutation delete = new Mutation(CandyKey.of("path/to/candy"),
                CandyLocator.tombstone(new Hlc(20, 0, 2), 6));

        assertThat(MutationSerializer.deserialize(MutationSerializer.serialize(put))).isEqualTo(put);
        assertThat(MutationSerializer.deserialize(MutationSerializer.serialize(delete))).isEqualTo(delete);
    }
}
