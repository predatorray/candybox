package me.predatorray.candybox.lsm;

import java.util.List;
import java.util.Map;
import me.predatorray.candybox.common.CandyKey;
import me.predatorray.candybox.common.CandyLocator;
import me.predatorray.candybox.common.Hlc;
import me.predatorray.candybox.common.LocatorType;
import me.predatorray.candybox.common.Mutation;
import me.predatorray.candybox.common.SegmentRef;

/** Small builders shared by the LSM tests. */
public final class TestData {

    private TestData() {
    }

    public static Hlc hlc(long physical, int logical, int node) {
        return new Hlc(physical, logical, node);
    }

    public static CandyLocator put(Hlc hlc, long syrupId, long contentLength) {
        return new CandyLocator(hlc, LocatorType.PUT, contentLength, 1 << 20, "application/octet-stream",
                Map.of(), 0, hlc.physicalMillis(), List.of(new SegmentRef(syrupId, 0, 0)));
    }

    public static Mutation putMutation(String key, Hlc hlc) {
        return new Mutation(CandyKey.of(key), put(hlc, 1, 10));
    }

    public static Mutation tombstone(String key, Hlc hlc) {
        return new Mutation(CandyKey.of(key), CandyLocator.tombstone(hlc, hlc.physicalMillis()));
    }
}
