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
